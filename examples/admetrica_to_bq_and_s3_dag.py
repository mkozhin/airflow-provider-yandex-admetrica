"""
DAG: статистика медийных кампаний AdMetrica за период → BigQuery + S3.

## Структура

```
get_dates ─────────┐
                   ├─→ collect.expand(date=dates) → flatten ─┬─→ make_s3_params  → upload_s3 ──────────────┐
ensure_gcs_bucket ─┘                                          │                                            ├─→ cleanup
                                                              └─→ make_gcs_params → upload_gcs ─┬─→ load_bq_stats ─┤
                                                                                                └─→ load_bq_dict ──┘
```

`collect` — mapped-таска: `get_dates` разворачивает период в список дат от свежих в
прошлое, и каждый день едет отдельным map index. День падает сам по себе, остальные
дни прогона это не трогает, а повтор одного дня — это clear его map index.
`max_active_tis_per_dag=1` пускает дни по одному, чтобы запросы к API шли
последовательно; `max_active_runs=1` не даёт двум прогонам писать один и тот же
ключ S3 и одну и ту же партицию BigQuery.

## Формат результата оператора

Оператор возвращает список записей `{kind, date, path, advertiser_id}`: `kind="stats"`
для файла статистики дня и `kind="dict"` для снапшота справочника кампаний.
`advertiser_id` берётся из записи — рекламодатель назван в коннекшене, и DAG узнаёт
его только отсюда.

`collect.output` у mapped-таски приходит списком списков, по элементу на map index,
поэтому `flatten` разворачивает его в плоский список записей. Снапшот справочника
один на прогон, а оператор отрабатывает на каждый день, поэтому запись `kind="dict"`
приходит из каждого map index и перед формированием параметров загрузки
дедуплицируется — иначе один и тот же файл грузился бы в один и тот же ключ столько
раз, сколько в периоде дней.

## Раскладка

Локально файлы прогона лежат под `{BASE_DIR}/{safe_run_id}/{advertiser_id}/…` и
удаляются задачей `cleanup`.

В S3 `run_id` в ключ не входит, день перетирается:

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

В BigQuery статистика и справочник живут в разных таблицах со своими схемами.
Схемы заданы явно: `autodetect` определял бы вложенные поля по началу файла и терял
бы те, что встречаются дальше. Партиция адресуется декоратором `table$YYYYMMDD` — по
`date` для статистики и по `snapshot_date` для справочника, так что `WRITE_TRUNCATE`
перетирает один день, а не таблицу целиком.

## Формат коннекшена

Один Airflow connection типа HTTP = один рекламодатель:

```
password: <OAuth-токен без префикса "OAuth ">
extra:    {"advertiser_id": 17004}
```

Диагностика запросов включается отдельным коннекшеном Loki: без `loki_conn_id`
клиент не конструируется.

## Перезапуск отдельного дня

Clear нужного map index `collect` перевыкачивает день и перетирает его файл в S3 и
партицию в BigQuery. `cleanup` стоит с `trigger_rule="all_success"`: при `all_done` он
сносил бы локальные файлы и после окончательного отказа загрузки, и обычный ручной
clear упавшей загрузки повторить её уже не смог бы — день пришлось бы выкачивать из
API заново.
"""

import os
import re
import shutil
from datetime import date, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow_provider_yandex_admetrica.operators.stats import YandexAdmetricaStatsOperator

# ── Конфигурация ──────────────────────────────────────────────────────────────

ADMETRICA_CONN_ID = "yandex_admetrica_default"
LOKI_CONN_ID      = "loki_default"
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

# Сегменты ключа под рекламодателем, разводящие статистику и справочник.
S3_PARTS = {"stats": ("stats",), "dict": ("dict", "campaigns")}


# ── Чистые функции, которыми пользуются таски ─────────────────────────────────


def safe_id(run_id: str) -> str:
    """Вернуть `run_id`, пригодный для имени каталога."""
    return re.sub(r"[^\w-]", "_", run_id or "")


def build_dates(date_from: str, date_to: str) -> list[str]:
    """Вернуть даты периода включительно по обеим границам, от свежих в прошлое.

    Порядок задаёт очерёдность map index: свежий день доезжает до витрины первым,
    а хвост периода догружается следом.
    """
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    if end < start:
        raise ValueError(f"date_to {date_to} раньше date_from {date_from}")
    return [(end - timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]


def flatten_results(mapped_results: list[list[dict]]) -> list[dict]:
    """Развернуть результат mapped-таски в плоский список записей.

    У mapped-таски `output` — список результатов по map index, а результат одного
    map index сам список записей, поэтому запись достаётся из двух уровней.
    """
    return [record for day_result in mapped_results or [] for record in day_result or []]


def dedupe_records(records: list[dict]) -> list[dict]:
    """Оставить по одной записи на файл, сохранив порядок первого появления.

    Снапшот справочника один на прогон, а появляется в результате каждого дня:
    без этого один файл грузился бы в один ключ столько раз, сколько в периоде дней.
    """
    seen: set[str] = set()
    unique: list[dict] = []
    for record in records:
        if record["path"] in seen:
            continue
        seen.add(record["path"])
        unique.append(record)
    return unique


def gcs_object(record: dict, run_id: str) -> str:
    """Вернуть промежуточный объект GCS для записи.

    `run_id` в ключ входит: объект живёт до конца прогона и удаляется правилом
    жизненного цикла бакета, поэтому два прогона не должны делить один ключ.
    """
    parts = "/".join(S3_PARTS[record["kind"]])
    return f"{GCS_PREFIX}/{safe_id(run_id)}/{record['advertiser_id']}/{parts}/{record['date']}.json"


def s3_key(record: dict) -> str:
    """Вернуть ключ S3 с hive-партициями для записи.

    `run_id` в ключ не входит: день перетирается, история не хранится.
    """
    day = record["date"]
    year, month, date_of_month = day.split("-")
    parts = "/".join(S3_PARTS[record["kind"]])
    return (
        f"{S3_PREFIX}/{record['advertiser_id']}/{parts}"
        f"/_year={year}/_month={month}/_day={date_of_month}"
        f"/_date={day.replace('-', '')}/{day}.json"
    )


def bq_table(record: dict, table: str) -> str:
    """Вернуть партицию таблицы, адресованную декоратором даты записи."""
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table}${record['date'].replace('-', '')}"


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
        client = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        bucket = client.bucket(GCS_BUCKET)
        if not bucket.exists():
            bucket = client.create_bucket(GCS_BUCKET)
        bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 1}}]
        bucket.patch()

    @task
    def flatten(mapped_results: list[list[dict]]) -> list[dict]:
        return dedupe_records(flatten_results(mapped_results))

    @task
    def make_gcs_params(records: list[dict], **context) -> list[dict]:
        run_id = context["run_id"]
        return [{"src": r["path"], "dst": gcs_object(r, run_id)} for r in records]

    @task
    def make_bq_params(records: list[dict], kind: str, table: str, **context) -> list[dict]:
        run_id = context["run_id"]
        return [
            {
                "source_objects": [gcs_object(r, run_id)],
                "destination_project_dataset_table": bq_table(r, table),
            }
            for r in records
            if r["kind"] == kind
        ]

    @task
    def make_s3_params(records: list[dict]) -> list[dict]:
        return [{"filename": r["path"], "dest_key": s3_key(r)} for r in records]

    @task(trigger_rule="all_success")
    def cleanup(**context) -> None:
        run_dir = os.path.join(BASE_DIR, safe_id(context["run_id"]))
        if not os.path.isdir(run_dir):
            return
        shutil.rmtree(run_dir)

    dates = get_dates()
    bucket_ready = ensure_gcs_bucket()

    collect = YandexAdmetricaStatsOperator.partial(
        task_id="collect",
        admetrica_conn_id=ADMETRICA_CONN_ID,
        loki_conn_id=LOKI_CONN_ID,
        dimensions=DIMENSIONS,
        metrics=METRICS,
        base_dir=BASE_DIR,
        collect_dictionaries=True,
        max_active_tis_per_dag=1,
    ).expand(date=dates)

    upload_gcs = LocalFilesystemToGCSOperator.partial(
        task_id="upload_gcs",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
    )

    load_bq_stats = GCSToBigQueryOperator.partial(
        task_id="load_bq_stats",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        schema_fields=BQ_STATS_SCHEMA,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY", "field": STATS_PARTITION_FIELD},
    )

    load_bq_dict = GCSToBigQueryOperator.partial(
        task_id="load_bq_dict",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        schema_fields=BQ_DICT_SCHEMA,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY", "field": DICT_PARTITION_FIELD},
    )

    upload_s3 = LocalFilesystemToS3Operator.partial(
        task_id="upload_s3",
        aws_conn_id=S3_CONN_ID,
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    records = flatten(collect.output)

    gcs_params = make_gcs_params(records)
    stats_bq_params = make_bq_params.override(task_id="make_bq_params_stats")(
        records, kind="stats", table=BQ_STATS_TABLE
    )
    dict_bq_params = make_bq_params.override(task_id="make_bq_params_dict")(
        records, kind="dict", table=BQ_DICT_TABLE
    )
    s3_params = make_s3_params(records)

    gcs_done = upload_gcs.expand_kwargs(gcs_params)
    stats_loaded = load_bq_stats.expand_kwargs(stats_bq_params)
    dict_loaded = load_bq_dict.expand_kwargs(dict_bq_params)
    s3_done = upload_s3.expand_kwargs(s3_params)

    bucket_ready >> collect
    gcs_done >> [stats_loaded, dict_loaded]
    [stats_loaded, dict_loaded, s3_done] >> cleanup()


admetrica_to_bq_and_s3()
