"""
DAG: статистика медийных кампаний AdMetrica за период → BigQuery + S3.

## Структура

```
get_dates ─────────┐
                   ├─→ day[<дата>]: collect ─┬─→ upload_s3
ensure_gcs_bucket ─┘                         └─→ load_bq

day ─→ dictionary: params ─┬─→ upload_gcs → load_bq
                           └─→ upload_s3

day, dictionary → cleanup
```

`day` — mapped task group: `get_dates` разворачивает период в список дат от свежих в
прошлое, и каждый день едет отдельным map index целиком — сбор, выгрузка в S3 и
загрузка в BigQuery. Падение дня останавливает только его собственную цепочку:
остальные дни того же прогона выгружаются и грузятся как обычно. `collect` стоит с
`max_active_tis_per_dag=1`, чтобы запросы к API шли последовательно; `max_active_runs=1`
не даёт двум прогонам писать один и тот же ключ S3 и одну и ту же партицию BigQuery.

Направления внутри дня параллельны и независимы: S3 и BigQuery — две таски от одного
`collect`, и отказ одной оставляет вторую доехать до своей витрины. Повторяется тогда
только упавшее направление: собранные файлы дня лежат на месте, пока `cleanup` не
дождётся обоих.

## Требования к развёртыванию

Всем задачам прогона нужна одна и та же файловая система по пути `BASE_DIR`. `collect`
пишет туда файлы дня, а `upload_s3`, `load_bq` и `cleanup` — отдельные экземпляры
задач, которые эти файлы читают, тогда как привязки к воркеру внутри task group Airflow
не обещает. На Celery или Kubernetes с локальными дисками воркеров выгрузки падают на
отсутствующем файле, а собранные файлы остаются на том воркере, который их написал, —
`cleanup` их не видит. Раскладка держится либо на однохостовом `LocalExecutor`, либо на
общем томе (NFS, PVC с `ReadWriteMany`), примонтированном в `BASE_DIR` на каждом
воркере, который берёт задачи этого DAG.

## Формат результата оператора

Оператор возвращает список записей `{kind, date, path, advertiser_id, campaign_id}`:
`kind="stats"` — файл одной кампании за день, и таких записей в дне столько, сколько
кампаний дали строки; `kind="dict"` — снапшот справочника кампаний, у него
`campaign_id=None`, потому что он описывает кабинет целиком. `advertiser_id` берётся
из записи — рекламодатель назван в коннекшене, и DAG узнаёт его только отсюда.

`day.upload_s3` и `day.load_bq` перебирают записи `kind="stats"` своего дня и увозят
каждую по её собственному адресу — в S3 своим ключом, в BigQuery своей таблицей через
свой промежуточный объект GCS. Кампания, не отдавшая за день ни строки, файла не пишет,
и везти за неё нечего; день, в котором таких записей не оказалось вовсе, поднимает
`AirflowSkipException` в обеих тасках, и оба направления пропускаются вместе с ним.

Снапшот справочника один на прогон, а пишет его каждый день, в один и тот же файл.
Грузится он один раз — группой `dictionary` после дней: `dictionary.params` берёт
первую запись `kind="dict"` из результатов дней. Правило `trigger_rule="all_done"`
пускает справочник и тогда, когда какой-то день упал; прогон, в котором справочник
не собрался ни одним днём, эту группу пропускает.

## Раскладка

Локально файлы прогона лежат под `{BASE_DIR}/{dag_id}-{digest}/{run_id}-{digest}/{advertiser_id}/…`
и удаляются задачей `cleanup`. DAG в пути потому, что `run_id` уникален только внутри
своего DAG, а рекламодателей обслуживают несколько DAG'ов на общем `BASE_DIR`. Тем же
парным адресом живут промежуточные объекты в GCS.

Локально и в GCS день статистики — каталог, а файл внутри назван кампанией:
`…/{advertiser_id}/stats/{дата}/{campaign_id}.json`. Справочник этого уровня не
получает: он снимок кабинета целиком и адресуется одной только датой снимка.

В S3 ни DAG, ни прогон в ключ не входят; перетирается день одной кампании:

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/_campaign_id=1234/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

`_campaign_id` стоит последним уровнем иерархии: датой отбирают диапазон, кампанией
сужают внутри него.

В BigQuery статистика и справочник живут в разных таблицах со своими схемами, а
статистика — ещё и в таблице на кампанию: `stats_{advertiser_id}_{campaign_id}`.
Гранула перезаписи нужна двумерная, день и кампания, а BigQuery партиционирует по
одному полю, поэтому второе измерение уходит в имя таблицы. Партиция адресуется
декоратором `table$YYYYMMDD` — по `date` для статистики и по `snapshot_date` для
справочника, так что `WRITE_TRUNCATE` перетирает один день одной кампании, а не
таблицу целиком.

Гранула перезаписи в обеих витринах равна грануле выгрузки, поэтому выгрузка части
кампаний не может стереть данные остальных за те же дни — ни ключом S3, ни партицией
BigQuery.

Схемы заданы явно, и `autodetect` выключен вслух в обеих загрузках: и в
`GCSToBigQueryOperator`, которым грузится справочник, и в конфигурации job'а, которым
грузится статистика. По умолчанию схема определяется сама, а определялась бы она по
вложенным полям начала файла и теряла бы те, что встречаются дальше.

Витрина читается wildcard-таблицами: `stats_*` — все рекламодатели, `stats_{advertiser_id}_*`
— один. Цена такого чтения принята сознательно: нет кэша результатов и нет BI Engine, а
схемы всех таблиц обязаны совпадать до типов и партиционирования. Вью, чьё имя попадает
под `stats_*`, ломает wildcard-запрос целиком, даже с условием на `_TABLE_SUFFIX`, —
вью следующего слоя держите в другом датасете либо называйте не на `stats_`.

Промежуточный бакет GCS может быть общим: правило жизненного цикла, которое DAG на
него вешает, ограничено префиксом `GCS_PREFIX` и встаёт рядом с теми правилами, что
на бакете уже есть.

## Формат коннекшена

Один Airflow connection типа HTTP = один рекламодатель:

```
password: <OAuth-токен без префикса "OAuth ">
extra:    {"advertiser_id": 17004}
```

Диагностика запросов включается отдельным коннекшеном Loki: без `loki_conn_id`
клиент не конструируется.

## Перезапуск отдельного дня

Clear нужного map index группы `day` вместе с задачами ниже перевыкачивает день и
перетирает файлы и партиции тех кампаний, которые он собрал заново. Дни независимы,
поэтому повтор одного не трогает ни файлы, ни партиции остальных, а кампания, не давшая
за этот день строк, остаётся в обеих витринах с прежними цифрами. Одно направление
повторяется clear'ом одной таски: `upload_s3` и `load_bq` читают файлы дня сами по себе.

`cleanup` стоит с `trigger_rule="none_failed"`: при `all_done` он сносил бы локальные
файлы и после окончательного отказа загрузки, и обычный ручной clear упавшей загрузки
повторить её уже не смог бы — день пришлось бы выкачивать из API заново. Правило
`none_failed`, а не `all_success`, потому что пропуск — штатный исход: день без файлов
статистики пропускает оба своих направления, а прогон без справочника — всю группу
`dictionary`.
"""

import os
import shutil
from collections.abc import Iterable
from datetime import date, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import check_date
from airflow_provider_yandex_admetrica.operators.stats import (
    DICT_CAMPAIGNS_PARTS,
    STATS_PARTS,
    YandexAdmetricaStatsOperator,
    id_segment,
)

# ── Конфигурация ──────────────────────────────────────────────────────────────

# Имя DAG. Оператор кладёт файлы прогона под него, поэтому DAG и его таски
# адресуют один и тот же каталог, а переименование остаётся одной правкой.
DAG_ID            = "admetrica_to_bq_and_s3"

ADMETRICA_CONN_ID = "yandex_admetrica_default"
LOKI_CONN_ID      = "loki_default"
# Корень локальной раскладки. Один и тот же путь на каждом воркере, который берёт
# задачи этого DAG, и одна и та же файловая система за ним: см. «Требования к
# развёртыванию».
BASE_DIR          = "/tmp/yandex_admetrica"

DIMENSIONS = ["am:e:placement", "am:e:deviceType"]
METRICS    = ["am:e:renders", "am:e:clicks", "am:e:ctr"]

GCP_CONN_ID    = "google_cloud_default"
GCS_BUCKET     = "my-gcs-bucket"
GCS_PREFIX     = "yandex_admetrica/staging"
BQ_PROJECT     = "my-gcp-project"
BQ_DATASET     = "yandex_admetrica"
BQ_STATS_TABLE = "stats"
BQ_DICT_TABLE  = "campaigns"

S3_CONN_ID = "aws_default"
S3_BUCKET  = "my-s3-bucket"
S3_PREFIX  = "raw/placements/display/yandex_admetrica"

MAX_ACTIVE_TASKS = 5

# Партиция статистики адресуется по дню отчёта, партиция справочника — по дню снапшота.
STATS_PARTITION_FIELD = "date"
DICT_PARTITION_FIELD  = "snapshot_date"

# Префикс, под которым живут промежуточные объекты, и правило, которое их удаляет.
# Условие `matchesPrefix` — это то, чем правило ограничено местом этого DAG: без него
# оно адресовало бы каждый объект бакета, а бакет может быть общим.
STAGING_PREFIX = f"{GCS_PREFIX}/"
STAGING_LIFECYCLE_RULE = {
    "action": {"type": "Delete"},
    "condition": {"age": 1, "matchesPrefix": [STAGING_PREFIX]},
}

# Служебные поля записи статистики плоские и типизированные, переменная часть —
# два объекта JSON: набор полей внутри них задаётся запросом и ответом API, и новое
# поле в ответе не требует правки схемы.
BQ_STATS_SCHEMA = [
    {"name": "date",          "type": "DATE",    "mode": "NULLABLE"},
    {"name": "advertiser_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "campaign_id",   "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "dimensions",    "type": "JSON",    "mode": "NULLABLE"},
    {"name": "metrics",       "type": "JSON",    "mode": "NULLABLE"},
]

# Справочник кампаний плоский: поля management API плюс дата снапшота, которой
# адресуется партиция.
BQ_DICT_SCHEMA = [
    {"name": "snapshot_date",   "type": "DATE",    "mode": "NULLABLE"},
    {"name": "campaign_id",     "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "name",            "type": "STRING",  "mode": "NULLABLE"},
    {"name": "status",          "type": "STRING",  "mode": "NULLABLE"},
    {"name": "date_start",      "type": "STRING",  "mode": "NULLABLE"},
    {"name": "date_end",        "type": "STRING",  "mode": "NULLABLE"},
    {"name": "advertiser_id",   "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "advertiser_name", "type": "STRING",  "mode": "NULLABLE"},
]

# Сегменты под рекламодателем, разводящие статистику и справочник в ключах S3 и
# GCS. Взяты у оператора, поэтому объект в облаке лежит по той же раскладке, что
# и файл, из которого он загружен.
KEY_PARTS = {"stats": STATS_PARTS, "dict": DICT_CAMPAIGNS_PARTS}


# ── Чистые функции, которыми пользуются таски ─────────────────────────────────


def run_dir(run_id: str) -> str:
    """Вернуть каталог прогона, в который собирает файлы оператор.

    Собирается тем же `id_segment`, что и в операторе, поэтому уборка и загрузка
    адресуют ровно тот каталог, куда велась запись. Пара из DAG и прогона:
    `run_id` уникален в пределах своего DAG, а рекламодателей обслуживают
    несколько DAG'ов, и на общем `BASE_DIR` одного `run_id` для имени каталога
    не хватает.
    """
    return os.path.join(BASE_DIR, id_segment(DAG_ID), id_segment(run_id))


def build_dates(date_from: str, date_to: str) -> list[str]:
    """Вернуть даты периода включительно по обеим границам, от свежих в прошлое.

    Порядок задаёт очерёдность map index: свежий день доезжает до витрины первым,
    а хвост периода догружается следом.

    Обе границы приходят из параметров прогона и проверяются `check_date`
    провайдера: его отказ называет значение типом и длиной, а разбор стандартной
    библиотеки процитировал бы его целиком в трейсбеке таски, куда набранное в
    поле параметра попадать не должно.
    """
    check_date(date_from)
    check_date(date_to)
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    if end < start:
        raise ValueError(f"date_to {date_to} раньше date_from {date_from}")
    return [(end - timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]


def find_record(records: Iterable[dict] | None, kind: str) -> dict | None:
    """Вернуть первую запись вида `kind` или `None`, если её нет."""
    for record in records or []:
        if record["kind"] == kind:
            return record
    return None


def select_records(records: Iterable[dict] | None, kind: str) -> list[dict]:
    """Вернуть все записи вида `kind` в порядке, в котором их отдал оператор.

    Статистика дня приходит записью на кампанию, и каждому направлению нужны все
    до одной: взять одну — значит молча оставить остальные кампании дня без
    выгрузки. Порядок — тот же, в котором кабинет перечисляет кампании.
    """
    return [record for record in records or [] if record["kind"] == kind]


def dictionary_record(mapped_results: Iterable[list[dict]] | None) -> dict | None:
    """Вернуть снапшот справочника прогона из результатов дней.

    Снапшот один на прогон: путь и дата у него одни для всех дней, поэтому годится
    запись любого дня, который до него дошёл, а первая — это самый свежий день
    периода.
    """
    for day_results in mapped_results or []:
        record = find_record(day_results, "dict")
        if record is not None:
            return record
    return None


def gcs_object(record: dict, run_id: str) -> str:
    """Вернуть промежуточный объект GCS для записи.

    DAG и прогон входят в ключ: объект живёт до конца прогона и удаляется
    правилом жизненного цикла бакета, поэтому два прогона не должны делить один
    ключ — а бакет общий, и `run_id` одинаков у DAG'ов на общем расписании.

    У статистики день — каталог, а объект назван кампанией: файлы двух кампаний
    одного дня иначе легли бы на один адрес и затёрли бы друг друга ещё до того,
    как доехали до BigQuery. Справочник назван датой снимка: он один на прогон.
    """
    parts = "/".join(KEY_PARTS[record["kind"]])
    name = (
        f"{record['date']}/{record['campaign_id']}"
        if record["kind"] == "stats"
        else record["date"]
    )
    return (
        f"{GCS_PREFIX}/{id_segment(DAG_ID)}/{id_segment(run_id)}"
        f"/{record['advertiser_id']}/{parts}/{name}.json"
    )


def s3_key(record: dict) -> str:
    """Вернуть ключ S3 с hive-партициями для записи.

    `run_id` в ключ не входит: история не хранится, адрес перетирается.

    У статистики последним уровнем иерархии идёт `_campaign_id`: датой отбирают
    диапазон, кампанией сужают внутри него. Он же и делает адрес ровно таким,
    какова гранула выгрузки, — перезалив части кампаний не достаёт до данных
    остальных за те же дни. Справочник этого уровня не получает: он снимок
    кабинета целиком, и адресуется одной только датой снимка.
    """
    day = record["date"]
    year, month, date_of_month = day.split("-")
    parts = "/".join(KEY_PARTS[record["kind"]])
    campaign = f"/_campaign_id={record['campaign_id']}" if record["kind"] == "stats" else ""
    return (
        f"{S3_PREFIX}/{record['advertiser_id']}/{parts}"
        f"/_year={year}/_month={month}/_day={date_of_month}"
        f"/_date={day.replace('-', '')}{campaign}/{day}.json"
    )


def bq_table(record: dict, table: str) -> str:
    """Вернуть партицию таблицы, адресованную декоратором даты записи."""
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table}${record['date'].replace('-', '')}"


def stats_table_id(record: dict) -> str:
    """Вернуть партицию таблицы кампании — голым идентификатором, без квалификации.

    Гранула перезаписи нужна двумерная, день и кампания, а BigQuery партиционирует
    по одному полю: второе измерение уходит в имя таблицы, и `WRITE_TRUNCATE`
    достаёт ровно до одного дня одной кампании — перезалив части кампаний не
    трогает партиции остальных.

    Идентификатор голый: проект и датасет конфигурация `insert_job` называет
    отдельными полями `projectId` и `datasetId`.
    """
    day = record["date"].replace("-", "")
    return f"{BQ_STATS_TABLE}_{record['advertiser_id']}_{record['campaign_id']}${day}"


def load_params(record: dict, run_id: str, table: str) -> dict:
    """Вернуть адреса, которыми грузится один файл: откуда, куда и в какую партицию."""
    return {
        "src": record["path"],
        "gcs_object": gcs_object(record, run_id),
        "s3_key": s3_key(record),
        "bq_table": bq_table(record, table),
    }


def staging_rule_present(rules: Iterable[dict]) -> bool:
    """Сказать, стоит ли на бакете правило удаления промежуточного префикса.

    Правило узнаётся по действию и по префиксу, а не по полному совпадению: возраст
    и прочие условия — дело владельца бакета, а сверка целиком заводила бы второе
    такое же правило при первом же расхождении.
    """
    for rule in rules:
        action = rule.get("action") or {}
        condition = rule.get("condition") or {}
        prefixes = condition.get("matchesPrefix") or []
        if action.get("type") == "Delete" and STAGING_PREFIX in prefixes:
            return True
    return False


# ── default_args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":             "analytics",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── DAG ───────────────────────────────────────────────────────────────────────


@dag(
    dag_id=DAG_ID,
    doc_md=__doc__,
    schedule=None,
    start_date=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "date_from": Param(
            (date.today() - timedelta(days=30)).isoformat(),
            type="string",
            description="Начальная дата (включительно), YYYY-MM-DD",
        ),
        "date_to": Param(
            (date.today() - timedelta(days=1)).isoformat(),
            type="string",
            description="Конечная дата (включительно), YYYY-MM-DD",
        ),
    },
    tags=["yandex", "admetrica", "bigquery", "s3"],
)
def admetrica_to_bq_and_s3():

    @task
    def get_dates(**context) -> list[str]:
        params = context["params"]
        return build_dates(params["date_from"], params["date_to"])

    @task
    def ensure_gcs_bucket() -> None:
        """Создать бакет, если его нет, и повесить правило на промежуточный префикс.

        Правила, которые на бакете уже стоят, остаются на месте: новое встаёт рядом
        с ними и адресует только префикс этого DAG. Повторный прогон, нашедший своё
        правило, бакет не патчит.
        """
        client = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        # `lookup_bucket` и `create_bucket` отдают бакет с загруженными метаданными,
        # и `lifecycle_rules` читает то, что на бакете стоит на самом деле: правила
        # живут в метаданных, а без них список вышел бы пустым и запись стёрла бы их.
        bucket = client.lookup_bucket(GCS_BUCKET) or client.create_bucket(GCS_BUCKET)
        rules = [dict(rule) for rule in bucket.lifecycle_rules]
        if staging_rule_present(rules):
            return
        bucket.lifecycle_rules = [*rules, STAGING_LIFECYCLE_RULE]
        bucket.patch()

    @task
    def day_upload(records: list[dict]) -> None:
        """Увезти в S3 все файлы статистики дня — по файлу на кампанию.

        Цикл, а не оператор на запись: число кампаний за день известно только
        после сбора, а маппинг внутри mapped-группы Airflow не поддерживает.
        Выгрузки идут последовательно, и первая же неудача роняет таску, оставляя
        clear этого map index как повтор дня целиком.

        Направление своё: таска читает файлы дня сама и ни к чему в BigQuery не
        привязана, поэтому отказ той стороны её не касается.

        День, за который ни одна кампания не отдала строк, файлов не пишет, и
        везти нечего: такой день пропускается вместе с задачами ниже.
        """
        stats = select_records(records, "stats")
        if not stats:
            raise AirflowSkipException("за этот день файлов статистики нет")
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        for record in stats:
            hook.load_file(
                filename=record["path"],
                key=s3_key(record),
                bucket_name=S3_BUCKET,
                replace=True,
            )

    @task
    def day_load(records: list[dict], **context) -> None:
        """Загрузить в BigQuery все файлы статистики дня — по файлу на кампанию.

        Цикл, а не оператор на запись: число кампаний за день известно только
        после сбора, а маппинг внутри mapped-группы Airflow не поддерживает.
        Каждая кампания едет своим промежуточным объектом GCS в свою таблицу;
        загрузки идут последовательно, и первая же неудача роняет таску, оставляя
        clear этого map index как повтор дня целиком.

        Направление своё: таска читает файлы дня сама и ни к чему в S3 не
        привязана, поэтому отказ той стороны её не касается.

        `job_id` не задаётся: `insert_job` подмешивает в него микросекунды,
        поэтому повтор не упирается в 409, а `WRITE_TRUNCATE` в партицию делает
        его безобидным — кампания перезаписывается тем, что собрано заново.

        День, за который ни одна кампания не отдала строк, файлов не пишет, и
        грузить нечего: такой день пропускается вместе с задачами ниже.
        """
        stats = select_records(records, "stats")
        if not stats:
            raise AirflowSkipException("за этот день файлов статистики нет")
        gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
        bigquery = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        for record in stats:
            object_name = gcs_object(record, context["run_id"])
            gcs.upload(
                bucket_name=GCS_BUCKET,
                object_name=object_name,
                filename=record["path"],
            )
            bigquery.insert_job(
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{GCS_BUCKET}/{object_name}"],
                        "destinationTable": {
                            "projectId": BQ_PROJECT,
                            "datasetId": BQ_DATASET,
                            "tableId": stats_table_id(record),
                        },
                        "schema": {"fields": BQ_STATS_SCHEMA},
                        "autodetect": False,
                        "sourceFormat": "NEWLINE_DELIMITED_JSON",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "createDisposition": "CREATE_IF_NEEDED",
                        "timePartitioning": {"type": "DAY", "field": STATS_PARTITION_FIELD},
                    }
                }
            )

    @task(multiple_outputs=True, trigger_rule="all_done")
    def dictionary_params(mapped_results, **context) -> dict:
        """Вернуть адреса загрузки снапшота справочника прогона.

        `all_done`: справочник — результат прогона, а не дня, и упавший день не
        повод его не грузить. Прогон, в котором справочник не собрался ни одним
        днём, пропускает загрузку целиком.
        """
        record = dictionary_record(mapped_results)
        if record is None:
            raise AirflowSkipException("в этом прогоне справочник не собран")
        return load_params(record, context["run_id"], BQ_DICT_TABLE)

    def upload_to_s3(task_id: str, params) -> LocalFilesystemToS3Operator:
        """Вернуть выгрузку файла в S3 по адресам из *params*."""
        return LocalFilesystemToS3Operator(
            task_id=task_id,
            aws_conn_id=S3_CONN_ID,
            dest_bucket=S3_BUCKET,
            filename=params["src"],
            dest_key=params["s3_key"],
            replace=True,
        )

    def upload_to_gcs(task_id: str, params) -> LocalFilesystemToGCSOperator:
        """Вернуть выгрузку файла в промежуточный бакет GCS по адресам из *params*."""
        return LocalFilesystemToGCSOperator(
            task_id=task_id,
            gcp_conn_id=GCP_CONN_ID,
            bucket=GCS_BUCKET,
            src=params["src"],
            dst=params["gcs_object"],
        )

    def load_to_bq(task_id: str, params, schema: list[dict], field: str) -> GCSToBigQueryOperator:
        """Вернуть загрузку объекта GCS в партицию BigQuery по адресам из *params*."""
        return GCSToBigQueryOperator(
            task_id=task_id,
            gcp_conn_id=GCP_CONN_ID,
            bucket=GCS_BUCKET,
            source_objects=[params["gcs_object"]],
            destination_project_dataset_table=params["bq_table"],
            schema_fields=schema,
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning={"type": "DAY", "field": field},
        )

    @task_group(group_id="day")
    def per_day(day: str):
        """Собрать день и увезти его файлы в S3 и в BigQuery.

        Всё, что делается с днём, лежит внутри группы, поэтому map index — это день
        целиком: упавший день останавливает только свою цепочку, а clear его map
        index с задачами ниже перевыкачивает и перезаписывает только этот день.

        Направления внутри дня — две таски от одного `collect`, параллельные и
        независимые: каждая читает файлы дня сама, и отказ одной оставляет второй
        доехать до своей витрины. Повторяется тогда одна упавшая таска.
        """
        records = YandexAdmetricaStatsOperator(
            task_id="collect",
            admetrica_conn_id=ADMETRICA_CONN_ID,
            loki_conn_id=LOKI_CONN_ID,
            date=day,
            dimensions=DIMENSIONS,
            metrics=METRICS,
            base_dir=BASE_DIR,
            collect_dictionaries=True,
            max_active_tis_per_dag=1,
        ).output
        day_upload.override(task_id="upload_s3")(records)
        day_load.override(task_id="load_bq")(records)
        return records

    @task_group(group_id="dictionary")
    def per_run_dictionary(mapped_results) -> None:
        """Увезти снапшот справочника прогона в S3 и в BigQuery — один раз за прогон."""
        params = dictionary_params.override(task_id="params")(mapped_results)

        upload_to_gcs("upload_gcs", params) >> load_to_bq(
            "load_bq", params, BQ_DICT_SCHEMA, DICT_PARTITION_FIELD
        )
        upload_to_s3("upload_s3", params)

    @task(trigger_rule="none_failed")
    def cleanup(**context) -> None:
        directory = run_dir(context["run_id"])
        if not os.path.isdir(directory):
            return
        shutil.rmtree(directory)

    dates = get_dates()
    bucket_ready = ensure_gcs_bucket()

    day_results = per_day.expand(day=dates)
    # Группа целиком, а не одна её таска: `expand` отдаёт ссылку на результат дня, а
    # зависимости прогона строятся от группы — бакет готов до первого сбора, а
    # `cleanup` ждёт каждую загрузку каждого дня.
    days = day_results.operator.task_group
    dictionary = per_run_dictionary(day_results)

    bucket_ready >> days
    [days, dictionary] >> cleanup()


admetrica_to_bq_and_s3()
