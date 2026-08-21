"""
DAG: статистика медийных кампаний AdMetrica за период → BigQuery + S3.

## Структура

```
get_dates ─────────┐
                   ├─→ day[<дата>]: collect → params ─┬─→ upload_gcs → load_bq
ensure_gcs_bucket ─┘                                  └─→ upload_s3

day ─→ dictionary: params ─┬─→ upload_gcs → load_bq
                           └─→ upload_s3

day, dictionary → cleanup
```

`day` — mapped task group: `get_dates` разворачивает период в список дат от свежих в
прошлое, и каждый день едет отдельным map index целиком — сбор, обе выгрузки и
загрузка в BigQuery. Падение дня останавливает только его собственную цепочку:
остальные дни того же прогона выгружаются и грузятся как обычно. `collect` стоит с
`max_active_tis_per_dag=1`, чтобы запросы к API шли последовательно; `max_active_runs=1`
не даёт двум прогонам писать один и тот же ключ S3 и одну и ту же партицию BigQuery.

## Требования к развёртыванию

Всем задачам прогона нужна одна и та же файловая система по пути `BASE_DIR`. `collect`
пишет туда файл дня, а `upload_gcs`, `upload_s3` и `cleanup` — отдельные экземпляры
задач, которые этот файл читают, тогда как привязки к воркеру внутри task group Airflow
не обещает. На Celery или Kubernetes с локальными дисками воркеров выгрузки падают на
отсутствующем файле, а собранные файлы остаются на том воркере, который их написал, —
`cleanup` их не видит. Раскладка держится либо на однохостовом `LocalExecutor`, либо на
общем томе (NFS, PVC с `ReadWriteMany`), примонтированном в `BASE_DIR` на каждом
воркере, который берёт задачи этого DAG.

## Формат результата оператора

Оператор возвращает список записей `{kind, date, path, advertiser_id}`: `kind="stats"`
для файла статистики дня и `kind="dict"` для снапшота справочника кампаний.
`advertiser_id` берётся из записи — рекламодатель назван в коннекшене, и DAG узнаёт
его только отсюда.

`day.params` собирает адреса загрузок из записи `kind="stats"` своего дня. День, за
который API не отдал ни строки, файла не пишет: `params` такого дня поднимает
`AirflowSkipException`, и обе выгрузки с загрузкой пропускаются вместе с ним.

Снапшот справочника один на прогон, а пишет его каждый день, в один и тот же файл.
Грузится он один раз — группой `dictionary` после дней: `dictionary.params` берёт
первую запись `kind="dict"` из результатов дней. Правило `trigger_rule="all_done"`
пускает справочник и тогда, когда какой-то день упал; прогон, в котором справочник
не собрался ни одним днём, эту группу пропускает.

## Раскладка

Локально файлы прогона лежат под `{BASE_DIR}/{safe_run_id}/{advertiser_id}/…` и
удаляются задачей `cleanup`.

В S3 `run_id` в ключ не входит, день перетирается:

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

В BigQuery статистика и справочник живут в разных таблицах со своими схемами.
Схемы заданы явно, а `autodetect=False` сказано вслух: по умолчанию оператор GCS →
BigQuery определяет схему сам, а определял бы он вложенные поля по началу файла и
терял бы те, что встречаются дальше. Партиция адресуется декоратором `table$YYYYMMDD`
— по `date` для статистики и по `snapshot_date` для справочника, так что
`WRITE_TRUNCATE` перетирает один день, а не таблицу целиком.

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

Clear нужного map index группы `day` вместе с задачами ниже перевыкачивает день,
перетирает его файл в S3 и его партицию в BigQuery. Дни независимы, поэтому повтор
одного не трогает ни файлы, ни партиции остальных.

`cleanup` стоит с `trigger_rule="none_failed"`: при `all_done` он сносил бы локальные
файлы и после окончательного отказа загрузки, и обычный ручной clear упавшей загрузки
повторить её уже не смог бы — день пришлось бы выкачивать из API заново. Правило
`none_failed`, а не `all_success`, потому что пропуск — штатный исход: день без строк
пропускает свои выгрузки, а прогон без справочника — всю группу `dictionary`.
"""

import os
import re
import shutil
from collections.abc import Iterable
from datetime import date, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import check_date
from airflow_provider_yandex_admetrica.operators.stats import (
    DICT_CAMPAIGNS_PARTS,
    STATS_PARTS,
    YandexAdmetricaStatsOperator,
)

# ── Конфигурация ──────────────────────────────────────────────────────────────

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


def safe_id(run_id: str) -> str:
    """Вернуть `run_id`, пригодный для имени каталога."""
    return re.sub(r"[^\w-]", "_", run_id or "")


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

    `run_id` в ключ входит: объект живёт до конца прогона и удаляется правилом
    жизненного цикла бакета, поэтому два прогона не должны делить один ключ.
    """
    parts = "/".join(KEY_PARTS[record["kind"]])
    return f"{GCS_PREFIX}/{safe_id(run_id)}/{record['advertiser_id']}/{parts}/{record['date']}.json"


def s3_key(record: dict) -> str:
    """Вернуть ключ S3 с hive-партициями для записи.

    `run_id` в ключ не входит: день перетирается, история не хранится.
    """
    day = record["date"]
    year, month, date_of_month = day.split("-")
    parts = "/".join(KEY_PARTS[record["kind"]])
    return (
        f"{S3_PREFIX}/{record['advertiser_id']}/{parts}"
        f"/_year={year}/_month={month}/_day={date_of_month}"
        f"/_date={day.replace('-', '')}/{day}.json"
    )


def bq_table(record: dict, table: str) -> str:
    """Вернуть партицию таблицы, адресованную декоратором даты записи."""
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table}${record['date'].replace('-', '')}"


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
    dag_id="admetrica_to_bq_and_s3",
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

    @task(multiple_outputs=True)
    def day_params(records: list[dict], **context) -> dict:
        """Вернуть адреса загрузок дня, собранные из его записи статистики.

        День, за который API не отдал ни строки, файла не пишет, и грузить нечего:
        такой день пропускается вместе с задачами ниже.
        """
        record = find_record(records, "stats")
        if record is None:
            raise AirflowSkipException("за этот день файла статистики нет")
        return load_params(record, context["run_id"], BQ_STATS_TABLE)

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
        """Собрать день и увезти его файл в S3 и в BigQuery.

        Всё, что делается с днём, лежит внутри группы, поэтому map index — это день
        целиком: упавший день останавливает только свою цепочку, а clear его map
        index с задачами ниже перевыкачивает и перезаписывает только этот день.
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
        params = day_params.override(task_id="params")(records)

        upload_to_gcs("upload_gcs", params) >> load_to_bq(
            "load_bq", params, BQ_STATS_SCHEMA, STATS_PARTITION_FIELD
        )
        upload_to_s3("upload_s3", params)
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
        run_dir = os.path.join(BASE_DIR, safe_id(context["run_id"]))
        if not os.path.isdir(run_dir):
            return
        shutil.rmtree(run_dir)

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
