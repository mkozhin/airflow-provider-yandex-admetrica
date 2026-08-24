# Changelog

## [0.1.0] - 2026-08-24

### Added

- `AdmetricaHook` — коннекшн на рекламодателя (OAuth-токен в `conn.password`, `advertiser_id` в `conn.extra`), запрос на кампанию к `/v1/stat/data`, пагинация с проверкой полноты по `total_rows`, список кампаний из `/v1/management/campaigns` с кэшем на время жизни хука, ретраи 429/5xx и сетевых отказов с backoff и `Retry-After`, `test_connection()`
- `YandexAdmetricaStatsOperator` — выгрузка статистики за один день по всем кампаниям рекламодателя и снапшота словаря кампаний в JSONL; служебные поля плоские, `dimensions` и `metrics` — вложенные объекты
- Диагностика HTTP-запросов в Loki: событие на каждую попытку, маскирование OAuth-токена во всех каналах наружу — заголовках, теле ответа, сообщении об ошибке и тексте исключения
- Пример DAG `examples/admetrica_to_bq_and_s3_dag.py` — разворот периода в mapped task group по дню на map index, загрузка в S3 с hive-партиционированием и в BigQuery по декоратору партиции
- Справочник группировок и метрик `docs/metrics-and-dimensions.md` с ключами, под которыми имена попадают в записи
- README на английском и русском, `CONTEXT.md` с доменными терминами и архитектурными швами
- GitHub Actions workflow публикации на PyPI по пушу тега `v*`
