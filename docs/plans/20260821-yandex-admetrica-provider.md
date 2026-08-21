# Airflow-провайдер для API отчётов Яндекс Метрики для медийной рекламы (AdMetrica)

## Overview

Провайдер `airflow-provider-yandex-admetrica` выгружает статистику медийных рекламных
кампаний из API отчётов Яндекс Метрики для медийной рекламы в локальные файлы, откуда
DAG забирает их в S3 и BigQuery.

Решаемая задача: статистика медийных кампаний доступна только через веб-интерфейс
AdMetrica либо через API отчётов, требующий OAuth-авторизации, обхода кампаний и
подневных запросов. Провайдер закрывает это одним оператором, который за один день
собирает статистику по всем кампаниям рекламодателя и справочник кампаний.

Интеграция с существующим ландшафтом: пакет повторяет устройство соседних провайдеров
(`airflow-provider-avito`, `airflow-provider-cian`, `airflow-provider-yandex-realty`) —
структура пакета, hive-партиционирование в S3, диагностика запросов в Loki, публикация
на PyPI по тегу.

## Context (from discovery)

- **Репозиторий**: `/home/mgcom/mkozhin/airflow-provider-yandex-admetrica`, один коммит
  `57d5cab Initial commit`, ветка `main`. Из файлов присутствует только `LICENSE` (MIT).
- **Образцы**: `/home/mgcom/mkozhin/airflow-provider-avito` — `hooks/avito.py` (1534
  строки, HTTP-слой с ретраями и диагностикой), `hooks/loki.py` (232 строки, переносится
  целиком), `operators/calls.py`, `examples/bq_and_s3_multi_account_dag.py`,
  `tests/conftest.py` (глобальный патч `time.sleep`), `tests/test_loki.py` (613 строк),
  `CONTEXT.md`, `pyproject.toml`, `.github/workflows/publish.yml`.
- **Документация API** — `https://yandex.ru/dev/admetrica/doc/ru/`; любая её страница
  доступна в markdown добавлением `.md` к адресу, чем и снимались исходники. Нужные
  разделы: `openapi/report/data` (метод `/v1/stat/data`), `openapi/report/bytime`,
  `openapi/management/Kampanii/getAllCampaigns`, `openapi/management/Kampanii/getCampaigns`,
  `openapi/management/Kampanii/getCampaign`, `authorization`, `reports-intro`, `param`,
  `attrandmetr/dim_all`, `attributes/events/{placements,audience,geo,browser,os,technology}`,
  `metrics/events/{basic,conversion,ecommerce,finance,video}`.
- **Конвенции соседей**: имя дистрибутива `airflow-provider-<name>` (не
  `apache-airflow-provider-`), `README.md` + `README_RU.md`, `CONTEXT.md` с доменными
  терминами и швами, `setuptools-scm` с `version_file`, entry point `provider_info`.

## Development Approach

- **testing approach**: Regular (код, затем тесты в рамках той же задачи)
- завершать каждую задачу полностью перед переходом к следующей
- делать небольшие сфокусированные изменения
- **CRITICAL: каждая задача ОБЯЗАНА включать новые/обновлённые тесты** для кода этой задачи
  - тесты не опциональны — это обязательная часть чеклиста
  - unit-тесты на новые функции и методы
  - unit-тесты на изменённые функции и методы
  - новые тест-кейсы на новые ветви кода
  - обновление существующих тестов при изменении поведения
  - тесты покрывают и успешные, и ошибочные сценарии
- **CRITICAL: все тесты обязаны проходить до начала следующей задачи** — без исключений
- **CRITICAL: обновлять этот файл плана при изменении объёма работ**
- запускать тесты после каждого изменения
- сохранять обратную совместимость

## Testing Strategy

- **unit-тесты**: обязательны для каждой задачи (см. Development Approach)
- **e2e-тесты**: в проекте отсутствует UI, e2e-набора нет; их роль выполняет тест импорта
  примера DAG и тест `get_provider_info()`
- сеть в тестах не используется: `requests` подменяется через `unittest.mock`,
  `time.sleep` патчится глобальной фикстурой в `tests/conftest.py`
- команда запуска: `pytest tests/ -v`

## Progress Tracking

- отмечать выполненное `[x]` сразу после завершения
- новые обнаруженные задачи добавлять с префиксом ➕
- проблемы и блокеры помечать префиксом ⚠️
- обновлять план, если реализация отклоняется от исходного объёма
- держать план в соответствии с фактически выполненной работой

## Solution Overview

**Единица коннекшена — рекламодатель.** Один Airflow connection типа HTTP = один
`advertiser_id`. Токен OAuth хранится в `conn.password` без префикса `OAuth `,
`conn.extra` содержит `{"advertiser_id": 17004}`. Мульти-аккаунтной формы нет,
модуля `accounts.py` нет, сканирования коннекшенов нет — `conn_id` указывается в операторе.

**Единица оператора — один день.** Оператор получает одну дату и собирает за неё
статистику по всем кампаниям рекламодателя. Период разворачивается в DAG задачей
`get_dates` и подаётся через `collect.expand(date=dates)`: каждый день — отдельный
map index, падение одного дня не трогает остальные, ручной перезапуск = clear этого
map index. Даты идут от свежих в прошлое.

**Подневные запросы вместо `bytime`.** У метода `/v1/stat/data/bytime` нет `limit`/`offset`,
вместо них `top_keys` с потолком 30 строк — для выгрузки с группировками он непригоден.
Используется `/v1/stat/data` с `date1=date2=<день>`: дата становится колонкой, которую
проставляет провайдер, выгрузка полная, семплирование на однодневной выборке менее вероятно.

**Разрез по кампаниям — запросом на кампанию.** Группировки по кампании в API нет, а
`ids=1,2,3` суммирует кампании. Единственный способ сохранить разрез — отдельный запрос
на каждую кампанию.

**Переменная часть записи — вложенный JSON.** Служебные поля (`date`, `advertiser_id`,
`campaign_id`) плоские и типизированные, а `dimensions` и `metrics` — вложенные объекты.
Набор полей внутри них задаётся запросом и ответом API и не фиксирован: документация
объявляет объект значения группировки словарём с произвольными строковыми ключами, где
описанием гарантирован только `name`. Вложенная форма переживает и появление нового поля,
и его отсутствие в части строк, не требуя ни выравнивания записей, ни изменения схемы
таблицы.

**Факты и справочники раздельно.** В записях статистики только `campaign_id`; название
кампании живёт в отдельном словаре, потому что добывается отдельным запросом к management
API. Значения группировок, приходящие внутри строки отчёта, остаются в статистике как
есть — их не выносят в словарь.

**История не хранится.** День перетирается: `run_id` в путь S3 не входит, поля
`snapshot_ts` нет. `run_id` используется только в локальном `base_dir` для изоляции
параллельных прогонов.

**Диагностика запросов в Loki** переносится из avito целиком: транспорт, маскирование
токена, ограниченный слепок тела ответа, таблица уровней и форма события. Совпадает
устройство и общая часть полей; предметная часть схемы своя — вместо `offset`/`date_time_from`
кампании и дни AdMetrica, плюс поля семплирования. Метка `service` тоже своя, так что
запросы Grafana, отбирающие по ней, новый провайдер увидят только после добавления его
метки, а панели по общим полям (`level`, `outcome`, `http_status`, `duration_ms`,
`attempt`) работают без правок.

### Ключевые решения и их обоснование

| Решение | Обоснование |
|---|---|
| `/v1/stat/data`, не `bytime` | у `bytime` потолок `top_keys` = 30 строк, нет `limit`/`offset` |
| Запрос на кампанию | группировки по кампании в API нет, `ids` суммирует |
| Все статусы кампаний | архивная кампания крутилась в прошлом; фильтр `status=active` потерял бы её статистику при перезаливке |
| Кампании не фильтруются по `date_start`/`date_end` | справочные даты кампании не гарантируют отсутствия показов вне их, а пропуск запроса означал бы потерю данных |
| Единственный формат — JSONL | набор колонок не выводится из запроса и может различаться между строками одного дня |
| `dimensions` и `metrics` вложенными объектами | новое поле в ответе API не меняет схему таблицы и не требует выравнивания записей |
| `include_undefined=true` по умолчанию | иначе API выкидывает строки с неопределённой первой группировкой и сумма не сходится с `totals` |
| `accuracy="full"` по умолчанию | иначе цифры плывут между прогонами; параметр оператора позволяет вернуть семплирование |
| Падение при расхождении с `total_rows` | молчаливая потеря части страниц недопустима |
| `total_rows` не служит условием останова при `total_rows_rounded=true` | округлённое значение непригодно и как критерий завершения: совпадение счётчиков оборвало бы выгрузку на хвосте |
| Ключ строки отчёта — значения группировок в пределах одной кампании | отчёт агрегирован по группировкам, поэтому внутри одного ответа комбинация уникальна; между кампаниями она повторяется штатно (одна площадка крутится у нескольких), так что множество ключей живёт в пределах кампании |
| День без строк не создаёт файла | как в avito/cian; следствие — прежний файл в S3 остаётся, это документируется |
| `getAllCampaigns`, а не advertiser-scoped метод | только он отдаёт `total` для завершения пагинации и поле `advertiser_name` |

## Technical Details

### API

Хост `https://api.media.metrika.yandex.net`. Заголовок `Authorization: OAuth <token>`.
Доступ к API отчётов требует тарифа Метрика Про.

Используются два GET-метода:

**`GET /v1/management/campaigns`** — список кампаний рекламодателя.
Параметры: `advertiser_id`, `limit`, `offset`, `filter`, `from`, `to`, `sort`, `reversed`,
`status`. Значения по умолчанию для `limit`/`offset` документация этого метода не называет,
поэтому оба параметра передаются явно; `offset` — число пропускаемых строк, то есть
отсчёт идёт с 0. Фильтр по статусу не применяется.
Ответ: `{"campaigns": [{"campaign_id", "name", "date_start", "date_end", "advertiser_id",
"advertiser_name", "status", "days_left", "renders", "conversions", "conversion", "cost",
"permission"}], "total": N, "overall_total": M}`.

**`GET /v1/stat/data`** — статистика.
Параметры: `ids` (обязателен, идентификаторы кампаний), `metrics` (обязателен),
`dimensions`, `date1`, `date2`, `filters`, `limit` (дефолт 100, потолок 100 000),
`offset` (дефолт **1**), `accuracy`, `include_undefined`, `sort`, `timezone`, `lang`,
`preset`, `pretty`, `callback`.
Ответ: `{"query": {...}, "data": [{"dimensions": [{"name": ..., "id": ...}], "metrics": [числа]}],
"total_rows", "total_rows_rounded", "sampled", "sample_share", "sample_size", "sample_space",
"data_lag", "contains_sensitive_data", "totals", "min", "max"}`.

Форма значения группировки: спецификация описывает его как объект с произвольными
строковыми ключами (`additionalProperties: string`), а текст описания добавляет, что
`name` присутствует обязательно, а дополнительные поля — например `id` — присутствовать
могут. Единственный пример в документации вырожденный (`"dimensions": [{}]`), поэтому
фактический набор полей и его постоянство между строками проверяются на живом API.

Документированные лимиты: ≤20 метрик, ≤10 группировок, `limit` ≤ 100 000, фильтр —
≤10 уникальных группировок и метрик, ≤20 отдельных условий, ≤10 000 символов строки,
≤100 значений в одном условии.

Минимальная дата отчёта задаётся не общим лимитом, а полем `since` у каждой группировки
и метрики: у большинства это `2019-01-01`, у категорий интересов (`am:e:interest2d1..3`) —
`2018-09-01`.

**Не документировано вовсе**: квоты и RPS, формат ошибок (в спецификации описан только
ответ 200), допустимые значения `accuracy`, наличие `Retry-After`. Обработка отказов —
собственная конвенция провайдера.

### Формат записи статистики

Служебные поля плоские, переменная часть — два вложенных объекта:

```json
{"date": "2026-08-20", "advertiser_id": 17004, "campaign_id": 123456,
 "dimensions": {"placement": {"name": "Главная страница", "id": 55},
                "device_type": {"name": "mobile"}},
 "metrics": {"renders": 12345, "clicks": 67, "ctr": 0.54}}
```

Правила формирования ключей:

- `date` проставляет провайдер — API её не возвращает.
- Ключ внутри `dimensions` — имя группировки без префикса `am:e:`, переведённое в
  snake_case: `am:e:placement` → `placement`, `am:e:deviceType` → `device_type`,
  `am:e:operatingSystemRoot` → `operating_system_root`, `am:e:interest2d1` →
  `interest2d1`. camelCase среди группировок преобладает, поэтому правило то же, что для
  метрик, и применяется одной и той же функцией нормализации имени.
  Значение — объект ровно с теми полями, что пришли от API; ничего не отбрасывается и
  ничего не добавляется, имена полей внутри объекта не трогаются.
- Ключ внутри `metrics` — имя метрики без префикса `am:e:`, переведённое в snake_case:
  `am:e:renders` → `renders`, `am:e:videoCompletePercent` → `video_complete_percent`.
- Параметризованные имена приводятся к тому же виду: `am:e:goal12345Reaches` →
  `goal12345_reaches`. Когда параметр передаётся отдельным полем запроса, имя приходит с
  угловыми скобками (`am:e:goal<goal_id>Reaches`, `am:e:ecommerce<currency>Revenue`) — в
  ключ подставляется фактическое значение параметра из `extra_params`.
- Порядок ключей детерминирован: служебные, затем `dimensions` в порядке параметра
  `dimensions`, затем `metrics` в порядке параметра `metrics`.

Формат файла — JSONL, по объекту на строку. Смена набора `dimensions`/`metrics` между
прогонами меняет форму записей начиная с файлов, записанных после смены; ранее выгруженные
файлы остаются как есть, и приведение истории к новому набору — ручная операция
(перезаливка нужного периода либо удаление и повторная выгрузка).

### Формат записи словаря кампаний

Плоский объект: `snapshot_date` (дата выгрузки, проставляет оператор — он же именует
этой датой файл и заполняет `date` в возвращаемой записи) плюс поля
management API без переименований — `campaign_id`, `name`, `status`, `date_start`,
`date_end`, `advertiser_id`, `advertiser_name`.

`snapshot_date` нужен потому, что снапшоты копятся по партициям дат: без колонки с датой
партицию нечем адресовать при загрузке и нечем отличить версии справочника при чтении.

### Раскладка файлов

Локально (`run_id` изолирует параллельные прогоны):

```
{base_dir}/{safe_run_id}/{advertiser_id}/stats/{date}.json
{base_dir}/{safe_run_id}/{advertiser_id}/dict/campaigns/{snapshot_date}.json
```

В S3 (`run_id` отсутствует, день перетирается):

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

Словарь кампаний — снапшот на **дату выгрузки**, а не на обрабатываемый день: management
API отдаёт только текущее состояние справочника.

### Схема таблиц BigQuery

Таблица статистики: `date` (DATE, поле партиционирования), `advertiser_id` (INTEGER),
`campaign_id` (INTEGER), `dimensions` (JSON), `metrics` (JSON). Схема задаётся явно —
`autodetect` определял бы вложенные поля по началу файла и терял бы те, что встречаются
дальше.

Таблица словаря кампаний: `snapshot_date` (DATE, поле партиционирования) плюс плоская
схема по полям management API. Партиция адресуется декоратором `table$YYYYMMDD` по
`snapshot_date`, как в avito, поэтому `WRITE_TRUNCATE` перетирает снапшот того же дня и
не трогает предыдущие.

### Обработка отказов

| Ситуация | Поведение |
|---|---|
| 429, 5xx | повтор с backoff 1/2/4 с, всего 4 попытки; `Retry-After` соблюдается, если пришёл |
| 401 | падение сразу: токен долгоживущий, механизма обновления нет |
| 400 и прочие 4xx | падение сразу, текст ошибки достаётся из тела |
| сетевая ошибка | повтор наравне с 5xx |
| расхождение с `total_rows` при `total_rows_rounded=false` | `AirflowException` |
| расхождение при `total_rows_rounded=true` | WARNING в лог |
| `sampled=true` | WARNING с `sample_share` |
| `contains_sensitive_data=true` | WARNING: часть соц-дем строк скрыта API |
| `data_lag` | INFO |

Каждая неудачная попытка оставляет в логе таски одну строку; `Retrying in N s`
появляется только там, где пауза действительно будет.

### Схема события диагностики

Событие формируется на каждый HTTP-запрос к обоим эндпоинтам и содержит поля avito-схемы,
адаптированные под AdMetrica: `schema_version`, `outcome`, `level`, `sent_at`, `endpoint`,
`advertiser_id`, `campaign_id`, `date`, `offset`, `attempt`, `max_attempts`,
`request_method`, `request_url`, `request_params`, `request_headers` (**единственное поле
события, куда попадает токен: он живёт в заголовке `Authorization: OAuth <token>` и
маскируется перед записью в событие**),
`http_status`, `duration_ms`, `rows_count`, `rows_shape_ok`, `payload_kind`, `total_rows`,
`total_rows_rounded`, `sampled`, `sample_share`, `sample_size`, `sample_space`,
`contains_sensitive_data`, `data_lag`, `error_code`, `error_message`, `exception_type`,
`exception_message`, `rate_limit_limit`, `rate_limit_remaining`, `response_body`.
Тело ответа прикладывается только к событиям уровня выше `info`.

Через маскирование проходит **всё**, что покидает процесс: `request_headers`,
`response_body`, `error_message` и текст исключения. Структурированное описание ошибки —
такой же канал наружу, как сырое тело: сервер или промежуточный узел вправе отразить
заголовок `Authorization` внутри JSON-сообщения, и без маскирования токен уйдёт в событие,
в лог таски и в текст исключения.

## What Goes Where

- **Implementation Steps** (`[ ]`): всё, что делается внутри репозитория — код, тесты, документация.
- **Post-Completion** (без чекбоксов): действия во внешних системах — проверка на живом
  API, настройка OIDC на PyPI, создание коннекшенов в Airflow.

## Implementation Steps

### Task 1: Каркас пакета и метаданные дистрибутива

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `README.md`
- Create: `CHANGELOG.md`
- Create: `airflow_provider_yandex_admetrica/__init__.py`
- Create: `airflow_provider_yandex_admetrica/hooks/__init__.py`
- Create: `airflow_provider_yandex_admetrica/operators/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/test_provider_info.py`
- Modify: `LICENSE`

- [ ] проверить существующий `LICENSE` (MIT из initial commit), при необходимости обновить год и правообладателя
- [ ] создать `pyproject.toml`: имя `airflow-provider-yandex-admetrica`, `dynamic = ["version"]`, `description`, `readme = "README.md"`, `requires-python = ">=3.10"`, `license`, `authors`, `keywords`, `classifiers` с `Framework :: Apache Airflow :: Provider`
- [ ] дописать в `pyproject.toml` зависимости `apache-airflow>=2.9.1,<3.0` и `requests>=2.28`, `[project.urls]` (Homepage, Documentation, Repository, Changelog, Issues)
- [ ] дописать `[project.optional-dependencies] dev = ["pytest>=7.0", "apache-airflow-providers-amazon", "apache-airflow-providers-google"]` — оба провайдера нужны тесту импорта примера DAG (Task 13) и в рантайме самого провайдера не используются
- [ ] дописать `[project.entry-points."apache_airflow_provider"] provider_info = "airflow_provider_yandex_admetrica:get_provider_info"`, `[tool.setuptools.packages.find] include`, `[tool.setuptools_scm] version_file`, `[tool.pytest.ini_options] testpaths = ["tests"]`, `pythonpath = ["."]`
- [ ] создать `README.md` заготовкой с названием и одной строкой описания, чтобы `readme` из `pyproject.toml` разрешался
- [ ] создать `.gitignore` по образцу avito, обязательно с `airflow_provider_yandex_admetrica/_version.py`
- [ ] создать `CHANGELOG.md` с разделом `[Unreleased]`
- [ ] создать `__init__.py` с `get_provider_info()`: `package-name`, `name` = "Yandex AdMetrica", описание, `versions` из `_version.__version__`, `integrations` со ссылкой на `https://yandex.ru/dev/admetrica/doc/ru/`, `operators` и `hooks` с python-модулями
- [ ] создать пустые `hooks/__init__.py`, `operators/__init__.py`, `tests/__init__.py`
- [ ] создать `tests/conftest.py` с автофикстурой, патчащей `time.sleep` (по образцу avito)
- [ ] написать `tests/test_provider_info.py`: ключи присутствуют, типы значений верны, `versions` берётся из `_version.__version__`, `package-name` совпадает с именем дистрибутива, списки `operators`/`hooks` непусты. Тест **не импортирует** объявленные модули: они создаются в задачах 3 и 11, а гейт «тесты обязаны пройти» стоит уже здесь. Импортируемость объявленных модулей проверяется в задаче 18, когда они существуют — тот же объём проверки, что в `airflow-provider-avito/tests/test_provider_info.py`
- [ ] выполнить `pip install -e ".[dev]"` и убедиться, что `_version.py` сгенерирован
- [ ] запустить тесты — обязаны пройти до перехода к задаче 2

### Task 2: Перенос LokiClient

**Files:**
- Create: `airflow_provider_yandex_admetrica/hooks/loki.py`
- Create: `tests/test_loki.py`

- [ ] перенести `LokiClient` и `_build_target` из `airflow-provider-avito/airflow_provider_avito/hooks/loki.py`, заменив метку `_SERVICE` на `airflow-provider-yandex-admetrica`
- [ ] сохранить поведение: circuit breaker после первой ошибки, отказ отправлять Basic Auth по не-HTTPS, отсутствие влияния на управляющий поток вызывающего, пропуск `BaseException` наружу
- [ ] перенести `tests/test_loki.py` из avito, адаптировав импорты и ожидаемую метку сервиса
- [ ] дополнить тесты кейсом на метку `service` в payload
- [ ] запустить тесты — обязаны пройти до перехода к задаче 3

### Task 3: Маскирование секретов и ограничение текста

**Files:**
- Create: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_diagnostics.py`

- [ ] перенести из avito функции ограничения текста: `_truncate`, `_one_line`, `_bounded_header`, константы `_TEXT_LIMIT`, `_HEADER_LIMIT`, `_BODY_LIMIT`, `_TRUNCATED_SUFFIX`, `_WHITESPACE_RUN_RE`
- [ ] перенести маскирование токена: `_mask_token`, `_redact`, `_strip_token`, `_drop_cut_token`, константы `_TOKEN_REDACTED`, `_TOKEN_HEAD`, `_TOKEN_TAIL`, `_TOKEN_MIN_LENGTH`
- [ ] перенести чтение тела ответа: `_bounded_body`, `_declared_charset`, `_decoder_position`, `_CHARSET_RE`, `_ASSUMED_ENCODING`, `_DECODER_MESSAGES`
- [ ] адаптировать маскирование под AdMetrica: токен приходит в заголовке `Authorization: OAuth <token>` и не должен попадать ни в событие, ни в лог таски ни в каком виде
- [ ] завести единую точку маскирования, через которую проходит каждый текст, покидающий процесс: заголовки запроса, тело ответа, сообщение об ошибке и текст исключения
- [ ] написать тесты: обрезка по границе лимита, схлопывание пробелов и управляющих символов, маскирование короткого и длинного токена, вырезание токена из тела, тело в нечитаемой кодировке, тело без `charset`, замаскированный заголовок `Authorization`, токен, отражённый сервером внутри JSON-сообщения об ошибке
- [ ] запустить тесты — обязаны пройти до перехода к задаче 4

### Task 4: Схема диагностического события

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Modify: `tests/test_diagnostics.py`

- [ ] реализовать `_new_event(...)` со всем набором полей схемы (см. «Схема события диагностики» в Technical Details); поля, которые попытка ещё не определила, инициализируются `None`, так что набор ключей события постоянен
- [ ] перенести описание отказа: `_find_error`, `_summarize_error`, `_describe_error`, `_describe_error_code`, `_describe_unreadable_body`, константу `_OUTCOME_UNKNOWN`; кодов ошибок AdMetrica не документирует, поэтому `error_code` остаётся `None` до проверки на живом API
- [ ] пропускать `error_message` и текст исключения через маскирование из задачи 3: сообщение сервера — такой же канал наружу, как тело ответа
- [ ] реализовать `_event_level(event)` — таблицу уровней, определяющую и severity, и то, покидает ли тело ответа процесс
- [ ] реализовать `_stamp_duration`, `_record_exception` (только тип исключения), `_record_rate_limit`, `_stamp_response_error`
- [ ] реализовать `_classify_payload(event, data)` — аналог `_classify_ok_body` avito: описывает тело HTTP-200 (`rows_count`, `rows_shape_ok`, `payload_kind`, `outcome`) и возвращает пригодность тела к чтению; вызывающий решает по возвращаемому значению, а не по полю события
- [ ] реализовать `_emit_event(loki, event, resp, token)`: проставляет уровень, прикладывает `response_body` для уровней выше `info`, пропускает событие при сработавшем circuit breaker
- [ ] написать тесты: полнота набора ключей события, таблица уровней, тело прикладывается только выше `info`, `_record_exception` сохраняет первый тип, событие не отправляется при выключенном клиенте, разбор тела без объекта ошибки, `request_headers` события не содержит токена ни при одном уровне, `error_message` с отражённым токеном уходит замаскированным
- [ ] запустить тесты — обязаны пройти до перехода к задаче 5

### Task 5: Класс хука и разбор коннекшена

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_connection.py`

- [ ] объявить класс `AdmetricaHook(BaseHook)` с атрибутами `conn_name_attr = "admetrica_conn_id"`, `default_conn_name = "yandex_admetrica_default"`, `conn_type = "http"`, `hook_name = "Yandex AdMetrica"`
- [ ] реализовать `__init__(self, *, admetrica_conn_id=default_conn_name, loki: LokiClient | None = None, request_delay=..., limit=...)` с ленивыми полями для соединения и списка кампаний
- [ ] реализовать датакласс `AdvertiserConfig` с полем `advertiser_id: int`
- [ ] реализовать `parse_connection(extra: dict) -> AdvertiserConfig | None` — единая точка знания о формате `extra`; best-effort, не бросает исключений, логирует WARNING на битое значение
- [ ] реализовать резолюцию учётных данных: токен из `conn.password` без префикса `OAuth `, `advertiser_id` из `extra`; отсутствие любого из них — `AirflowException` с внятным текстом
- [ ] написать тесты: корректный extra, `advertiser_id` строкой, отсутствующий `advertiser_id`, нечисловое значение, пустой extra, пустой пароль, пароль с ошибочно сохранённым префиксом `OAuth `, значения атрибутов класса
- [ ] запустить тесты — обязаны пройти до перехода к задаче 6

### Task 6: HTTP-слой с ретраями

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_hook_http.py`

- [ ] реализовать `_request_page(url, params, event_fields) -> dict` — один запрос с ретраями, ничего не знающий о цикле по дням и кампаниям
- [ ] реализовать политику отказов: 429 и 5xx (`_RETRY_STATUSES`) и сетевые ошибки — повтор с backoff `[1, 2, 4]`, всего 4 попытки, с соблюдением `Retry-After`; 401 — падение сразу; 400 и прочие 4xx — падение сразу с текстом из тела
- [ ] реализовать `_log_attempt` и `_attempt_reason` — одна строка в логе таски на каждую неудачную попытку, включая последнюю; `Retrying in N s` только там, где пауза действительно будет
- [ ] встроить формирование и отправку диагностического события на каждую попытку
- [ ] реализовать паузу `request_delay` между запросами
- [ ] написать тесты: 429 ретраится и исчерпывает попытки, 5xx ретраится, сетевая ошибка ретраится, 401 не ретраится, 400 не ретраится и несёт текст ошибки, соблюдение `Retry-After`, ровно одна строка лога на попытку, событие уходит в Loki на каждую попытку
- [ ] запустить тесты — обязаны пройти до перехода к задаче 7

### Task 7: Получение списка кампаний

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_campaigns.py`

- [ ] реализовать `AdmetricaHook.get_campaigns()` — `GET /v1/management/campaigns?advertiser_id=…` с пагинацией; `limit` и `offset` передаются явно, отсчёт `offset` идёт с 0
- [ ] не передавать параметр `status`: архивные кампании нужны наравне с активными
- [ ] цикл пагинации до сбора `total` либо до короткой страницы; поле `campaigns` не список — `AirflowException`
- [ ] сверять число собранных кампаний с `total` и падать `AirflowException` при расхождении — короткая страница раньше времени означает потерю целых кампаний со всей их статистикой, и молчаливая потеря здесь так же недопустима, как в статистике
- [ ] кэшировать список кампаний на время жизни хука: статистика и словарь в одном прогоне обходятся одним обращением к management API
- [ ] возвращать записи словаря с полями `campaign_id`, `name`, `status`, `date_start`, `date_end`, `advertiser_id`, `advertiser_name` — ровно как отдал API. `snapshot_date` хук не проставляет: владелец этого поля один, и это оператор, который той же датой именует файл и заполняет `date` в возвращаемой записи. Так же устроен `snapshot_ts` в `airflow-provider-yandex-realty` — его ставит оператор, хук отдаёт записи API как есть
- [ ] написать тесты: одна страница, несколько страниц, `limit`/`offset` присутствуют в каждом запросе, отсчёт с 0, пустой список кампаний, отсутствующий ключ `campaigns`, короткая страница при незакрытом `total` роняет таск, архивные кампании присутствуют в результате, повторный вызов не делает второго запроса
- [ ] запустить тесты — обязаны пройти до перехода к задаче 8

### Task 8: Преобразование строки отчёта в запись

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_map_row.py`

- [ ] реализовать `_normalize_name(name, extra_params)` — снятие префикса `am:e:`, перевод в snake_case, подстановка фактического значения параметра вместо `<goal_id>` / `<currency>`; одна функция для группировок и метрик, потому что правило именования у них общее
- [ ] реализовать `_map_row(raw_row, date, advertiser_id, campaign_id, dimensions, metrics, extra_params)` — чистая функция без сети
- [ ] складывать значения группировок в объект `dimensions` под базовыми именами, сохраняя пришедшие поля без изменений; метрики — в объект `metrics` по нормализованным именам
- [ ] сохранять детерминированный порядок ключей: служебные, `dimensions` в порядке параметра, `metrics` в порядке параметра
- [ ] обрабатывать вырожденные случаи: значение группировки не объект, отсутствующее поле `name`, число метрик не совпадает с числом запрошенных
- [ ] написать тесты: группировка с `id` и без него, группировка с дополнительными полями, две строки с разным набором полей группировки, snake_case для camelCase-группировок `am:e:deviceType` → `device_type` и `am:e:operatingSystemRoot` → `operating_system_root`, `am:e:interest2d1` остаётся `interest2d1`, snake_case для метрики `videoCompletePercent`, поля внутри объекта группировки не переименовываются, `am:e:goal<goal_id>Reaches` с `extra_params={"goal_id": 12345}`, `am:e:goal12345Reaches` без параметров, пустой `dimensions`, рассинхрон длины массива метрик
- [ ] запустить тесты — обязаны пройти до перехода к задаче 9

### Task 9: Сбор статистики за день

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_stats.py`

- [ ] реализовать `AdmetricaHook.get_stats(date, dimensions, metrics, ...)` — цикл по кампаниям из кэшированного списка, для каждой `GET /v1/stat/data?ids={campaign_id}&date1=date2={date}`
- [ ] реализовать пагинацию внутри кампании: `limit` (дефолт 10 000), **offset начинается с 1**
- [ ] реализовать условие останова по флагу округления: при `total_rows_rounded=false` — по сбору `total_rows` либо по короткой странице; при `total_rows_rounded=true` — **только** по короткой странице, потому что округлённое значение как критерий останова оборвало бы выгрузку на совпадении счётчиков (10 437 строк при `limit=10000` и округлённом вниз `total_rows=10000` дают полную первую страницу и потерянный хвост)
- [ ] задавать `sort` по всем запрошенным группировкам: отчёт агрегирован по ним, комбинация их значений уникальна, поэтому такой порядок полный и воспроизводимый между запросами; сортировка по метрике полного порядка не даёт (`renders=1` у длинного хвоста площадок)
- [ ] отслеживать ключи собранных строк **в пределах одной кампании**, обнуляя множество на каждой следующей: уникальна комбинация значений группировок внутри одного отчёта, а не по всему рекламодателю — одна площадка откручивается в нескольких кампаниях, и общий на всех набор ключей ронял бы выгрузку на штатных данных
- [ ] задать проекцию ключа явно: для каждой группировки берётся `id`, если он пришёл, иначе `name`; кортеж объектов нехешируем, а проекция на один только `name` даёт ложный дубль на двух площадках с одинаковым названием и разными `id`
- [ ] падать `AirflowException` при повторе ключа между страницами одной кампании: сверка по количеству дубль от пропуска не отличает. При пустом `dimensions` отчёт состоит из одной строки, пагинации нет и проверка не применяется
- [ ] реализовать сверку полноты: при `total_rows_rounded=false` и несовпадении числа собранных строк с `total_rows` — `AirflowException`; при взведённом флаге округления — WARNING
- [ ] проверять документированные лимиты до запроса: ≤20 метрик, ≤10 группировок — `ValueError` с внятным текстом
- [ ] передавать `accuracy`, `include_undefined`, `filters`, `timezone`, `lang` и `extra_params` (для `goal_id`/`currency`) в запрос
- [ ] отклонять `ValueError` ключи `extra_params`, которыми владеет хук — `ids`, `date1`, `date2`, `metrics`, `dimensions`, `limit`, `offset`, `sort`, `accuracy`, `include_undefined`, `filters`, `timezone`, `lang`: молчаливое переопределение `date1` запросило бы другую дату, а `_map_row` проштамповал бы строки датой оператора; `accuracy` и `include_undefined` сняли бы ровно те дефолты, что стоят против плавающих и урезанных цифр, причём сверка полноты прошла бы, потому что `total_rows` согласован с урезанной выборкой. Все пять последних доступны отдельными параметрами оператора, так что маршрут через `extra_params` для них не нужен
- [ ] задать приоритет слияния явно: параметры хука выигрывают всегда, `extra_params` только добавляет ключи, которых в запросе ещё нет
- [ ] логировать `sampled` и `sample_share`, а также `contains_sensitive_data` как WARNING, `data_lag` как INFO; переносить эти поля в диагностическое событие
- [ ] написать тесты: одна страница, несколько страниц, offset начинается с 1, `sort` содержит все запрошенные группировки, повтор ключа строки между страницами одной кампании роняет таск, одинаковая комбинация группировок в двух разных кампаниях таск не роняет, две площадки с одним названием и разными `id` дублем не считаются, расхождение с `total_rows` роняет таск, полная страница при округлённом `total_rows` не останавливает цикл, округлённый `total_rows` при расхождении только предупреждает, `accuracy` и `include_undefined` доходят до запроса с заданными значениями, `filters`/`timezone`/`lang` доходят до запроса, зарезервированный ключ в `extra_params` роняет `get_stats` с `ValueError`, незарезервированный ключ доходит до запроса, превышение лимита метрик и группировок, кампания без данных, пустой `dimensions`, `sampled=true` даёт WARNING, `contains_sensitive_data=true` даёт WARNING, список кампаний запрашивается один раз
- [ ] запустить тесты — обязаны пройти до перехода к задаче 10

### Task 10: Проверка соединения и кэш подключения

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Create: `tests/test_hook_meta.py`

- [ ] реализовать `test_connection() -> tuple[bool, str]` через запрос списка кампаний рекламодателя
- [ ] реализовать ленивое кэширование соединения: `get_connection` вызывается ровно один раз за жизненный цикл хука независимо от числа запросов
- [ ] написать тесты: успешная проверка, неуспешная проверка с текстом ошибки, `get_connection` вызывается один раз на множество запросов
- [ ] запустить тесты — обязаны пройти до перехода к задаче 11

### Task 11: Оператор — выгрузка статистики за день

**Files:**
- Create: `airflow_provider_yandex_admetrica/operators/stats.py`
- Create: `tests/test_operator.py`

- [ ] создать `YandexAdmetricaStatsOperator(BaseOperator)` с параметрами `admetrica_conn_id`, `date`, `dimensions`, `metrics`, `filters`, `accuracy="full"`, `include_undefined=True`, `limit`, `request_delay`, `timezone`, `lang`, `extra_params`, `base_dir`, `collect_dictionaries=True`, `loki_conn_id=None`
- [ ] задать `template_fields` (включая `date`, `admetrica_conn_id`, `loki_conn_id`) и `ui_color`
- [ ] реализовать `_build_path` — локальные пути с `safe_run_id`, санитизация по правилу `re.sub(r"[^\w-]", "_", ...)`
- [ ] реализовать `_write` — JSONL, по объекту на строку, `ensure_ascii=False`
- [ ] реализовать `_build_loki_client` — диагностика включается только при заданном `loki_conn_id`
- [ ] реализовать `execute`: сбор статистики за день, запись файла, возврат списка `{kind: "stats", date, path, advertiser_id}`; при нуле строк файл не создаётся и в результат ничего не добавляется
- [ ] включить `advertiser_id` в каждую возвращаемую запись: DAG строит по нему пути S3 и имена таблиц, а взять его больше неоткуда — параметров рекламодателя у DAG нет, `conn.extra` читается только внутри хука, и хардкод константы в DAG дал бы второй источник правды, расходящийся с коннекшеном молча
- [ ] написать тесты: путь формируется верно, JSONL пишется построчно, порядок ключей в записи детерминирован, строки с разным набором полей группировки пишутся без потерь, день без строк не создаёт файла, `advertiser_id` присутствует в каждой возвращаемой записи и совпадает с тем, что в коннекшене, параметры оператора доходят до хука, диагностика не конструируется при пустом `loki_conn_id`
- [ ] запустить тесты — обязаны пройти до перехода к задаче 12

### Task 12: Оператор — выгрузка словаря кампаний

**Files:**
- Modify: `airflow_provider_yandex_admetrica/operators/stats.py`
- Modify: `tests/test_operator.py`

- [ ] дополнить `execute` выгрузкой словаря кампаний при `collect_dictionaries=True`
- [ ] определять дату снапшота как дату выполнения выгрузки, а не как обрабатываемый день
- [ ] писать словарь в `{base_dir}/{safe_run_id}/{advertiser_id}/dict/campaigns/{snapshot_date}.json` и добавлять в результат запись `{kind: "dict", date: snapshot_date, path, advertiser_id}`
- [ ] сохранять поля словаря без переименований и проставлять `snapshot_date` в каждую запись файла — единственное место, где это поле появляется; та же дата именует файл и заполняется в `date` возвращаемой записи, поэтому колонка, путь и декоратор партиции BigQuery всегда согласованы
- [ ] написать тесты: словарь пишется в партицию даты выгрузки, `snapshot_date` присутствует в каждой записи файла и равен дате в имени файла и в поле `date` результата, хук поля не проставляет, `collect_dictionaries=False` его не создаёт, результат содержит обе записи `kind` с `advertiser_id`, пустой список кампаний не создаёт файла, повторный запуск пишет тот же путь
- [ ] запустить тесты — обязаны пройти до перехода к задаче 13

### Task 13: Пример DAG

**Files:**
- Create: `examples/admetrica_to_bq_and_s3_dag.py`
- Create: `tests/test_example_dag.py`

- [ ] написать DAG с параметрами `date_from` / `date_to` и задачей `get_dates`, возвращающей список дат по убыванию (от свежих в прошлое)
- [ ] подключить `collect.expand(date=dates)` с `max_active_tis_per_dag=1`, чтобы дни шли последовательно
- [ ] задать `max_active_runs=1`: `max_active_tis_per_dag` упорядочивает только экземпляры `collect` внутри прогона, а два одновременных прогона одного рекламодателя за тот же день пишут в один и тот же ключ S3 и в одну партицию BigQuery, и какой из них останется — вопрос порядка завершения
- [ ] добавить задачу, разворачивающую результат mapped-таски: `collect.output` приходит списком списков, по элементу на map index, и нуждается во flatten до плоского списка записей
- [ ] дедуплицировать записи `kind="dict"` при формировании параметров загрузки: снапшот словаря один на прогон, а появляется в результате каждого map index
- [ ] добавить `make_s3_params`, разводящий записи по `kind`: статистика в `{S3_PREFIX}/{advertiser_id}/stats/_year=…`, словарь в `{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=…`; `advertiser_id` берётся из самой записи, а не из константы DAG
- [ ] добавить загрузку в S3 через `LocalFilesystemToS3Operator` с `replace=True`
- [ ] описать константу схемы BigQuery для статистики: `date` (DATE), `advertiser_id` (INTEGER), `campaign_id` (INTEGER), `dimensions` (JSON), `metrics` (JSON), партиционирование по `date`
- [ ] описать константу схемы для словаря кампаний: `snapshot_date` (DATE), `campaign_id` (INTEGER), `name`, `status`, `date_start`, `date_end`, `advertiser_id` (INTEGER), `advertiser_name`; партиционирование по `snapshot_date`, отдельная таблица
- [ ] добавить загрузку в BigQuery через GCS по образцу avito, с явными схемами и без `autodetect`; партиция адресуется декоратором `table$YYYYMMDD` — по `date` для статистики, по `snapshot_date` для словаря, чтобы `WRITE_TRUNCATE` перетирал одну партицию, а не таблицу целиком
- [ ] проверить, что зависимости из Task 1 покрывают импорты примера: `apache-airflow-providers-amazon` и `apache-airflow-providers-google` нужны тесту импорта и в CI
- [ ] добавить `cleanup`, удаляющий локальную папку прогона, с `trigger_rule="all_success"`: при `all_done` он сносит файлы и после окончательного отказа загрузки, и обычный ручной `clear` упавшей задачи повторить её уже не сможет — данные придётся выкачивать из API заново. Оставленная после сбоя папка стоит дискового места, потерянный день стоит перезапроса
- [ ] снабдить DAG docstring-ом с описанием структуры, формата коннекшена и поведения при перезапуске отдельного дня
- [ ] написать `tests/test_example_dag.py`: DAG импортируется без ошибок, содержит ожидаемые задачи, `get_dates` возвращает даты по убыванию и включительно по границам, flatten разворачивает результаты нескольких map index, дедупликация оставляет один словарь, `max_active_runs=1` и `trigger_rule` у `cleanup` заданы как описано
- [ ] запустить тесты — обязаны пройти до перехода к задаче 14

### Task 14: README

**Files:**
- Modify: `README.md`
- Create: `README_RU.md`

- [ ] описать установку, требования (Python ≥3.10, Airflow ≥2.9.1 <3.0) и **необходимость тарифа Метрика Про** для доступа к API отчётов
- [ ] описать настройку коннекшена: тип HTTP, токен в `password` без префикса `OAuth `, `extra` с `advertiser_id`; отдельно — коннекшн Loki для диагностики
- [ ] описать быстрый старт с примером оператора и всеми параметрами
- [ ] описать формат записи: служебные поля плоские, `dimensions` и `metrics` вложенными объектами, правила именования ключей, пример записи, схема таблиц BigQuery
- [ ] описать раскладку файлов локально и в S3, hive-партиции, отсутствие `run_id` в ключе и перетирание дня
- [ ] явно описать поведение при дне без данных: файл не создаётся, прежний файл в S3 остаётся нетронутым
- [ ] описать поведение при смене набора `dimensions`/`metrics`: новые файлы пишутся в новой форме, ранее выгруженные остаются как есть, приведение истории — ручная перезаливка периода
- [ ] описать отказы, ретраи и вид строк в логе таски; отдельно указать, что квоты и формат ошибок API не документированы, а значения `accuracy` в документации Яндекса не перечислены
- [ ] описать диагностику в Loki: схему полей события, маскирование токена, поведение circuit breaker
- [ ] дать ссылки на документацию AdMetrica: `https://yandex.ru/dev/admetrica/doc/ru/` и раздел группировок и метрик `https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all`
- [ ] написать `README.md` (EN) и `README_RU.md` (RU) с одинаковой структурой
- [ ] проверить, что примеры кода из README запускаются как есть
- [ ] запустить тесты — обязаны пройти до перехода к задаче 15

### Task 15: Справочник группировок и метрик

**Files:**
- Create: `docs/metrics-and-dimensions.md`

- [ ] скачать первоисточники заново на момент выполнения задачи — они живут по постоянным URL, и каждая страница доступна в markdown добавлением `.md` к адресу (например `https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all.md`):
  - сводный список: `https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all`
  - группировки: `attributes/events/placements`, `attributes/events/audience`, `attributes/events/geo`, `attributes/events/browser`, `attributes/events/os`, `attributes/events/technology`
  - метрики: `metrics/events/basic`, `metrics/events/conversion`, `metrics/events/ecommerce`, `metrics/events/finance`, `metrics/events/video`
  - параметризация: `https://yandex.ru/dev/admetrica/doc/ru/param`
- [ ] перенести таблицы всех группировок: имя вида `am:e:placement`, русское название, описание, поддерживаемые операторы фильтрации, значение `since`
- [ ] сгруппировать группировки по разделам: размещения, аудитория, гео, браузер, ОС, технологии
- [ ] перенести таблицы всех метрик: базовые, конверсионные, ecommerce, финансовые, видео — имя, русское название, описание, тип, возможность фильтрации, значение `since`
- [ ] привести рядом с каждым именем ключ, под которым оно попадёт в запись: `am:e:deviceType` → `device_type`, `am:e:videoCompletePercent` → `video_complete_percent` — это и есть то, что аналитик пишет в `JSON_VALUE`
- [ ] описать параметризацию: `goal_id` и `currency`, оба способа задания (в самом выражении и отдельным параметром запроса), связь с параметром оператора `extra_params` и список ключей, которые `extra_params` не принимает
- [ ] описать документированные лимиты запроса: 20 метрик, 10 группировок, `limit` до 100 000, ограничения фильтра, разные значения `since` у группировок
- [ ] указать, как эти имена передаются в параметры `dimensions` и `metrics` оператора, и привести готовые наборы для типовых отчётов
- [ ] дать URL первоисточника рядом с каждым разделом
- [ ] сверить перенесённые таблицы со скачанными страницами на полноту
- [ ] дать ссылку на `docs/metrics-and-dimensions.md` из обоих README
- [ ] запустить тесты — обязаны пройти до перехода к задаче 16

### Task 16: CONTEXT.md

**Files:**
- Create: `CONTEXT.md`

- [ ] описать доменные термины: рекламодатель, кампания, группировка, метрика, словарь, снапшот словаря, служебные поля
- [ ] описать архитектурные швы: `parse_connection`, `_map_row`, `_request_page`, `_build_path`, `_new_event`/`_emit_event`
- [ ] описать правило санитизации идентификаторов и правила именования ключей в `dimensions`/`metrics`
- [ ] описать политику работы с секретами: где живёт токен, где он маскируется, какие каналы (лог таски и события Loki) его никогда не получают
- [ ] описать таблицу политик отказов по call-site
- [ ] описать принятые решения и их причины: отказ от `bytime`, запрос на каждую кампанию отдельно, выбор `getAllCampaigns` ради `total` и `advertiser_name`, вложенный JSON вместо плоских колонок, отсутствие фильтра кампаний по `date_start`/`date_end`, JSONL как единственный формат, ключ строки отчёта как кортеж значений группировок, останов пагинации по флагу округления
- [ ] запустить тесты — обязаны пройти до перехода к задаче 17

### Task 17: CI и публикация на PyPI

**Files:**
- Create: `.github/workflows/publish.yml`

- [ ] создать workflow, срабатывающий на push тега `v*`
- [ ] job `test`: матрица Python 3.10/3.11/3.12, `fetch-depth: 0`, установка `pip install -e ".[dev]"`, запуск `pytest tests/ -v`
- [ ] job `publish`: `needs: test`, `environment: pypi`, `permissions: id-token: write`, `fetch-depth: 0`, сборка `python -m build`, публикация через `pypa/gh-action-pypi-publish@release/v1`
- [ ] проверить сборку локально: `python -m build` создаёт `.whl` и `.tar.gz`
- [ ] проверить, что `get_provider_info()` из собранного пакета возвращает корректный dict
- [ ] запустить тесты — обязаны пройти до перехода к задаче 18

### Task 18: Проверка критериев приёмки

- [ ] проверить, что все требования из Overview и Solution Overview реализованы
- [ ] проверить граничные случаи: пустой день, пустой список кампаний, единственная кампания, кампания в архиве, расхождение `total_rows`, недоступный Loki, строки с разным набором полей группировки
- [ ] проверить, что токен не попадает ни в лог таски, ни в события Loki ни при одном сценарии отказа
- [ ] запустить полный набор тестов: `pytest tests/ -v`
- [ ] проверить, что `pip install -e .` и `python -m build` отрабатывают без ошибок
- [ ] проверить `python -c "from airflow_provider_yandex_admetrica import get_provider_info; print(get_provider_info())"`
- [ ] проверить, что каждый модуль, объявленный в `get_provider_info()` в списках `operators` и `hooks`, импортируется — к этому моменту все они существуют

### Task 19: [Final] Обновление документации

- [ ] зафиксировать в `CHANGELOG.md` версию первого релиза
- [ ] сверить README с фактическим набором параметров оператора
- [ ] запустить полный набор тестов после правок документации
- [ ] перенести этот план в `docs/plans/completed/`

## Post-Completion

*Действия, требующие внешних систем — без чекбоксов, справочно*

**Проверка на живом API:**
- создать приложение на `https://oauth.yandex.ru/client/new` с доступами `mediametrika:read`
  и `mediametrika:write` (инструкция Яндекса требует обоих; провайдеру достаточно чтения)
  и получить OAuth-токен
- убедиться, что у аккаунта есть тариф Метрика Про — без него API отчётов недоступен
- проверить фактический набор полей в объектах значений группировок и то, одинаков ли он
  между строками одного ответа: спецификация описывает объект как словарь с произвольными
  ключами и примера с заполненными полями не содержит
- проверить, принимает ли API значение `accuracy=full` (в документации допустимые значения
  не перечислены); при отказе скорректировать значение по умолчанию
- измерить реальные квоты и RPS: документация о них молчит, значение `request_delay` по
  умолчанию подбирается по результатам первого прогона
- проверить фактический формат тела ошибки (в спецификации описан только ответ 200) и при
  необходимости уточнить извлечение `error_code` / `error_message`
- проверить стабильность порядка строк при пагинации с заданным `sort`
- оценить время прогона на крупном рекламодателе (около 70 кампаний × число дней) и
  подобрать `execution_timeout` в DAG

**Настройка внешних систем:**
- создать коннекшены Airflow: по одному на рекламодателя плюс коннекшн Loki
- настроить OIDC Trusted Publisher на pypi.org (Owner, Repository, Workflow `publish.yml`,
  Environment `pypi`) **до** первого push тега
- создать environment `pypi` в настройках репозитория GitHub
- проверить, что существующие дашборды Grafana видят события нового провайдера по метке `service`
