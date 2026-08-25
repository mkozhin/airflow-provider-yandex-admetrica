"""
DAG: статистика медийных кампаний AdMetrica за период → S3.

Пример, а не боевой DAG: он показывает, как связать оператор провайдера с
витриной, и рассчитан на то, что его скопируют и подгонят под себя.

Рядом лежит `admetrica_to_bigquery_dag.py` — тот же сбор, но выгрузка в
BigQuery. DAG'и независимы: каждый ходит в API сам за себя, поэтому запуск обоих
за один период удваивает число запросов к AdMetrica. Возьмите тот, который нужен,
либо допишите вторую выгрузку в один DAG.

## Структура

```
get_dates ─→ day[<дата>]: collect → params → upload_s3

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
пишет туда файл дня, а `upload_s3` и `cleanup` — отдельные экземпляры задач, которые
этот файл читают, тогда как привязки к воркеру внутри task group Airflow не обещает.
На Celery или Kubernetes с локальными дисками воркеров выгрузка падает на отсутствующем
файле, а собранные файлы остаются на том воркере, который их написал, — `cleanup` их не
видит. Раскладка держится либо на однохостовом `LocalExecutor`, либо на общем томе
(NFS, PVC с `ReadWriteMany`), примонтированном в `BASE_DIR` на каждом воркере, который
берёт задачи этого DAG.

## Формат результата оператора

Оператор возвращает список записей `{kind, date, path, advertiser_id}`: `kind="stats"`
для файла статистики дня и `kind="dict"` для снапшота справочника кампаний.
`advertiser_id` берётся из записи — рекламодатель назван в коннекшене, и DAG узнаёт
его только отсюда.

`day.params` собирает адрес выгрузки из записи `kind="stats"` своего дня. День, за
который API не отдал ни строки, файла не пишет: `params` такого дня поднимает
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

В S3 ни DAG, ни прогон в ключ не входят, день перетирается:

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

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
перетирает его файл в S3. Дни независимы, поэтому повтор одного не трогает файлы
остальных.

`cleanup` стоит с `trigger_rule="none_failed"`: при `all_done` он сносил бы локальные
файлы и после окончательного отказа выгрузки, и обычный ручной clear упавшей выгрузки
повторить её уже не смог бы — день пришлось бы выкачивать из API заново. Правило
`none_failed`, а не `all_success`, потому что пропуск — штатный исход: день без строк
пропускает свою выгрузку, а прогон без справочника — всю группу `dictionary`.
"""

import os
import shutil
from collections.abc import Iterable
from datetime import date, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
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


def load_params(record: dict) -> dict:
    """Вернуть адреса, которыми выгружается один файл: откуда и куда."""
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

    @task(multiple_outputs=True)
    def day_params(records: list[dict], **context) -> dict:
        """Вернуть адреса выгрузки дня, собранные из его записи статистики.

        День, за который API не отдал ни строки, файла не пишет, и грузить нечего:
        такой день пропускается вместе с задачами ниже.
        """
        record = find_record(records, "stats")
        if record is None:
            raise AirflowSkipException("за этот день файла статистики нет")
        return load_params(record)

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
        return load_params(record)

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

    @task_group(group_id="day")
    def per_day(day: str):
        """Собрать день и увезти его файл в S3.

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

        upload_to_s3("upload_s3", params)
        return records

    @task_group(group_id="dictionary")
    def per_run_dictionary(mapped_results) -> None:
        """Увезти снапшот справочника прогона в S3 — один раз за прогон."""
        params = dictionary_params.override(task_id="params")(mapped_results)

        upload_to_s3("upload_s3", params)

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
