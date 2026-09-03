"""
DAG: статистика медийных кампаний AdMetrica за период → S3.

Пример, а не боевой DAG: он показывает, как связать оператор провайдера с
витриной, и рассчитан на то, что его скопируют и подгонят под себя.

## Структура

```
get_dates ─→ day[<дата>]: collect → upload_s3

day ─→ dictionary: params → upload_s3

day, dictionary → cleanup
```

`day` — mapped task group: `get_dates` разворачивает период в список дат от свежих в
прошлое, и каждый день едет отдельным map index целиком — сбор и выгрузка. Падение
дня останавливает только его собственную цепочку: остальные дни того же прогона
выгружаются как обычно. `collect` стоит с `max_active_tis_per_dag=1`, чтобы запросы к
API шли последовательно; `max_active_runs=1` не даёт двум прогонам писать один и тот
же ключ S3.

## Требования к развёртыванию

Всем задачам прогона нужна одна и та же файловая система по пути `BASE_DIR`. `collect`
пишет туда файлы дня, а `upload_s3` и `cleanup` — отдельные экземпляры задач, которые
эти файлы читают, тогда как привязки к воркеру внутри task group Airflow не обещает.
На Celery или Kubernetes с локальными дисками воркеров выгрузка падает на отсутствующем
файле, а собранные файлы остаются на том воркере, который их написал, — `cleanup` их не
видит. Раскладка держится либо на однохостовом `LocalExecutor`, либо на общем томе
(NFS, PVC с `ReadWriteMany`), примонтированном в `BASE_DIR` на каждом воркере, который
берёт задачи этого DAG.

## Формат результата оператора

Оператор возвращает список записей `{kind, date, path, advertiser_id, campaign_id}`:
`kind="stats"` — файл одной кампании за день, и таких записей в дне столько, сколько
кампаний дали строки; `kind="dict"` — снапшот справочника кампаний, у него
`campaign_id=None`, потому что он описывает кабинет целиком. `advertiser_id` берётся
из записи — рекламодатель назван в коннекшене, и DAG узнаёт его только отсюда.

`day.upload_s3` перебирает записи `kind="stats"` своего дня и увозит каждую по её
собственному ключу. Кампания, не отдавшая за день ни строки, файла не пишет, и грузить
за неё нечего; день, в котором таких записей не оказалось вовсе, поднимает
`AirflowSkipException`, и выгрузка пропускается вместе с ним.

Снапшот справочника один на прогон, а пишет его каждый день, в один и тот же файл.
Выгружается он один раз — группой `dictionary` после дней: `dictionary.params` берёт
первую запись `kind="dict"` из результатов дней. Правило `trigger_rule="all_done"`
пускает справочник и тогда, когда какой-то день упал; прогон, в котором справочник
не собрался ни одним днём, эту группу пропускает.

## Раскладка

Локально файлы прогона лежат под `{BASE_DIR}/{dag_id}-{digest}/{run_id}-{digest}/{advertiser_id}/…`
и удаляются задачей `cleanup`. DAG в пути потому, что `run_id` уникален только внутри
своего DAG, а рекламодателей обслуживают несколько DAG'ов на общем `BASE_DIR`.

Локально день статистики — каталог, а файл внутри назван кампанией:
`…/{advertiser_id}/stats/{дата}/{campaign_id}.json`.

В S3 ни DAG, ни прогон в ключ не входят; перетирается день одной кампании:

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/_campaign_id=1234/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

`_campaign_id` стоит последним уровнем иерархии: датой отбирают диапазон, кампанией
сужают внутри него. Гранула перезаписи равна грануле выгрузки, поэтому выгрузка части
кампаний не может стереть данные остальных за те же дни. Справочник кампанией не
адресуется: он снимок кабинета целиком, и переписывается целиком.

Бакет, в который этот DAG выгружает впервые, под `{S3_PREFIX}/{advertiser_id}/stats/`
должен быть пуст: объект, лежащий по ключу дня без уровня `_campaign_id=`, читатель
префикса возьмёт наравне с файлами кампаний и посчитает строки дня дважды. Такие ключи
снимают или уводят в архивный префикс до первого прогона, а внешнюю таблицу или
краулер переопределяют под ключ партиции `_campaign_id`.

Цена такой грануле — число объектов и число вызовов S3. День стоит по объекту на
каждую кампанию, давшую строки, и `day.upload_s3` увозит их последовательно, одной
таской: рекламодатель на 70 кампаний — это 70 выгрузок в дне и около 2100 за
тридцатидневный период. Считайте это, подбирая `execution_timeout`, и сужайте
период, если день не укладывается.

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
перетирает в S3 файлы тех кампаний, которые он собрал заново. Дни независимы, поэтому
повтор одного не трогает файлы остальных, а кампания, не давшая за этот день строк,
остаётся в витрине с прежними цифрами.

День выгружается по кампании за раз и целым в S3 не появляется: отказ на середине
оставляет в бакете свежие файлы кампаний, до которых цикл дошёл, и прежние — у
остальных, и в самих данных этой разницы не видно. Повтор увозит день целиком заново,
поэтому успевшие кампании оплачиваются вторично, а `replace=True` кладёт каждый файл
на его собственный адрес, и после успешного повтора день в витрине сходится.

`cleanup` стоит с `trigger_rule="none_failed"`: при `all_done` он сносил бы локальные
файлы и после окончательного отказа выгрузки, и обычный ручной clear упавшей выгрузки
повторить её уже не смог бы — день пришлось бы выкачивать из API заново. Правило
`none_failed`, а не `all_success`, потому что пропуск — штатный исход: день без файлов
статистики пропускает свою выгрузку, а прогон без справочника — всю группу
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
DAG_ID            = "admetrica_to_s3"

ADMETRICA_CONN_ID = "yandex_admetrica_default"
LOKI_CONN_ID      = "loki_default"
# Корень локальной раскладки. Один и тот же путь на каждом воркере, который берёт
# задачи этого DAG, и одна и та же файловая система за ним: см. «Требования к
# развёртыванию».
BASE_DIR          = "/tmp/yandex_admetrica"

DIMENSIONS = ["am:e:placement", "am:e:deviceType"]
METRICS    = ["am:e:renders", "am:e:clicks", "am:e:ctr"]

S3_CONN_ID = "aws_default"
S3_BUCKET  = "my-s3-bucket"
S3_PREFIX  = "raw/placements/display/yandex_admetrica"

MAX_ACTIVE_TASKS = 5

# Сегменты под рекламодателем, разводящие статистику и справочник в ключах S3.
# Взяты у оператора, поэтому объект в облаке лежит по той же раскладке, что и
# файл, из которого он загружен.
KEY_PARTS = {"stats": STATS_PARTS, "dict": DICT_CAMPAIGNS_PARTS}


# ── Чистые функции, которыми пользуются таски ─────────────────────────────────


def run_dir(run_id: str) -> str:
    """Вернуть каталог прогона, в который собирает файлы оператор.

    Собирается тем же `id_segment`, что и в операторе, поэтому уборка адресует
    ровно тот каталог, куда велась запись. Пара из DAG и прогона: `run_id`
    уникален в пределах своего DAG, а рекламодателей обслуживают несколько
    DAG'ов, и на общем `BASE_DIR` одного `run_id` для имени каталога не хватает.
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


def select_records(records: Iterable[dict] | None, kind: str) -> list[dict]:
    """Вернуть все записи вида `kind` в порядке, в котором их отдал оператор.

    Статистика дня приходит записью на кампанию, и выгрузке нужны все до одной:
    взять одну — значит молча оставить остальные кампании дня незагруженными.
    Порядок — тот же, в котором кабинет перечисляет кампании.
    """
    return [record for record in records or [] if record["kind"] == kind]


def dictionary_record(mapped_results: Iterable[list[dict]] | None) -> dict | None:
    """Вернуть снапшот справочника прогона из результатов дней.

    Снапшот один на прогон: путь и дата у него одни для всех дней, поэтому годится
    запись любого дня, который до него дошёл, а первая — это самый свежий день
    периода.

    Обход останавливается на первой же найденной записи, поэтому из результата
    прогона читается один день, а не все: день приходит записью на кампанию, и
    период из тридцати дней по три сотни кампаний — это тысячи записей, из
    которых нужна одна.
    """
    for day_results in mapped_results or []:
        for record in day_results or []:
            if record["kind"] == "dict":
                return record
    return None


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


def dictionary_load_params(record: dict) -> dict:
    """Вернуть адреса, которыми выгружается снапшот справочника: откуда и куда."""
    return {
        "src": record["path"],
        "s3_key": s3_key(record),
    }


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
    tags=["yandex", "admetrica", "s3"],
)
def admetrica_to_s3():

    @task
    def get_dates(**context) -> list[str]:
        params = context["params"]
        return build_dates(params["date_from"], params["date_to"])

    @task
    def day_upload(records: list[dict]) -> None:
        """Увезти в S3 все файлы статистики дня — по файлу на кампанию.

        Цикл, а не оператор на запись: число кампаний за день известно только
        после сбора, а маппинг внутри mapped-группы Airflow не поддерживает.
        Выгрузки идут последовательно, и первая же неудача роняет таску, оставляя
        clear этого map index как повтор дня целиком.

        День, за который ни одна кампания не отдала строк, файлов не пишет, и
        грузить нечего: такой день пропускается вместе с задачами ниже.
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

    @task(multiple_outputs=True, trigger_rule="all_done")
    def dictionary_params(mapped_results) -> dict:
        """Вернуть адреса выгрузки снапшота справочника прогона.

        `all_done`: справочник — результат прогона, а не дня, и упавший день не
        повод его не грузить. Прогон, в котором справочник не собрался ни одним
        днём, пропускает выгрузку целиком.
        """
        record = dictionary_record(mapped_results)
        if record is None:
            raise AirflowSkipException("в этом прогоне справочник не собран")
        return dictionary_load_params(record)

    @task_group(group_id="day")
    def per_day(day: str):
        """Собрать день и увезти его файлы в S3.

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
        day_upload.override(task_id="upload_s3")(records)
        return records

    @task_group(group_id="dictionary")
    def per_run_dictionary(mapped_results) -> None:
        """Увезти снапшот справочника прогона в S3 — один раз за прогон.

        Запись одна на прогон, её адрес известен до запуска задачи, поэтому
        выгрузка остаётся декларативным оператором переноса.
        """
        params = dictionary_params.override(task_id="params")(mapped_results)

        LocalFilesystemToS3Operator(
            task_id="upload_s3",
            aws_conn_id=S3_CONN_ID,
            dest_bucket=S3_BUCKET,
            filename=params["src"],
            dest_key=params["s3_key"],
            replace=True,
        )

    @task(trigger_rule="none_failed")
    def cleanup(**context) -> None:
        directory = run_dir(context["run_id"])
        if not os.path.isdir(directory):
            return
        shutil.rmtree(directory)

    dates = get_dates()

    day_results = per_day.expand(day=dates)
    # Группа целиком, а не одна её таска: `expand` отдаёт ссылку на результат дня, а
    # зависимости прогона строятся от группы — `cleanup` ждёт каждую выгрузку
    # каждого дня.
    days = day_results.operator.task_group
    dictionary = per_run_dictionary(day_results)

    [days, dictionary] >> cleanup()


admetrica_to_s3()
