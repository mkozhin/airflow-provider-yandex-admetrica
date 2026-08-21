# Группировки и метрики AdMetrica

Справочник имён, которые идут в параметры `dimensions` и `metrics` оператора
`YandexAdmetricaStatsOperator`, и ключей, под которыми эти имена появляются в
записях выгрузки.

Названия и описания приведены так, как их формулирует документация API отчётов
Яндекс Метрики для медийной рекламы. URL первоисточника указан рядом с каждым
разделом, полный список — в разделе [Первоисточники](#первоисточники).

## Как читать таблицы

- **Имя** — то, что пишется в списке `dimensions` или `metrics`.
- **Ключ в записи** — ключ внутри объекта `dimensions` или `metrics` записи
  JSONL. Это то, что аналитик пишет в `JSON_VALUE(dimensions, '$.device_type')`.
- **Название** — как группировка или метрика называется в отчёте.
- **Описание** — подробное описание из документации; прочерк стоит там, где
  документация описания не даёт.
- **Операторы фильтрации** — операторы, которые группировка принимает в
  параметре `filters`; расшифровка — в разделе
  [Операторы фильтрации](#операторы-фильтрации).
- **Тип** — тип значения метрики: `int`, `double`, `percents`, `currency`.
- **Фильтрация** — принимает ли метрика фильтрацию.
- **Минимальная дата** — самая ранняя дата, за которую отчёт с этим именем можно
  построить.

**Ключ получается из имени по одному правилу**, общему для группировок и метрик:
снимается префикс `am:e:`, параметр в угловых скобках заменяется своим значением,
оставшийся camelCase переводится в snake_case. `am:e:placement` — это
`placement`, `am:e:deviceType` — `device_type`, `am:e:videoCompletePercent` —
`video_complete_percent`, а `am:e:interest2d1`, в котором заглавных букв нет, —
`interest2d1`.

## Как имена попадают в оператор

```python
YandexAdmetricaStatsOperator(
    task_id="collect",
    admetrica_conn_id="yandex_admetrica_default",
    date="{{ params.date }}",
    dimensions=["am:e:placement", "am:e:deviceType"],
    metrics=["am:e:renders", "am:e:clicks", "am:e:ctr"],
    base_dir="/tmp/yandex_admetrica",
)
```

Такой запрос даёт записи вида:

```json
{"date": "2026-08-20", "advertiser_id": 17004, "campaign_id": 123456,
 "dimensions": {"placement": {"name": "Главная страница", "id": 55},
                "device_type": {"name": "mobile"}},
 "metrics": {"renders": 12345, "clicks": 67, "ctr": 0.54}}
```

Порядок ключей внутри `dimensions` и `metrics` повторяет порядок списков, так что
файлы одного и того же запроса сравнимы построчно.

Список группировок может быть и пустым: отчёт без группировок считает суммарный
результат, то есть одну строку на кампанию.

## Группировки

### Размещения

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/attributes/events/placements>

| Имя | Ключ в записи | Название | Описание | Операторы фильтрации | Минимальная дата |
|---|---|---|---|---|---|
| `am:e:placement` | `placement` | Размещение | Название размещения | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:site` | `site` | Площадка | Название площадки | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:creative` | `creative` | Креатив | Название креатива | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:domain` | `domain` | Домен | Доме́нное имя | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |
| `am:e:advType` | `adv_type` | Тип рекламы | Два типа рекламы: баннерная или видеореклама | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |

### Аудитория

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/attributes/events/audience>

| Имя | Ключ в записи | Название | Описание | Операторы фильтрации | Минимальная дата |
|---|---|---|---|---|---|
| `am:e:gender` | `gender` | Пол | — | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |
| `am:e:ageInterval` | `age_interval` | Возраст | — | `!.`, `!=`, `<`, `<=`, `=.`, `==`, `>`, `>=` | 2019-01-01 |
| `am:e:interest2d1` | `interest2d1` | Категория интересов, ур. 1 | Категория коммерческих интересов. Работает по множеству Интересы 2.0. | `!.`, `!=`, `<`, `<=`, `=.`, `==`, `>`, `>=` | 2018-09-01 |
| `am:e:interest2d2` | `interest2d2` | Категория интересов, ур. 2 | Категория коммерческих интересов. Работает по множеству Интересы 2.0. | `!.`, `!=`, `<`, `<=`, `=.`, `==`, `>`, `>=` | 2018-09-01 |
| `am:e:interest2d3` | `interest2d3` | Категория интересов, ур. 3 | Категория коммерческих интересов. Работает по множеству Интересы 2.0. | `!.`, `!=`, `<`, `<=`, `=.`, `==`, `>`, `>=` | 2018-09-01 |
| `am:e:income` | `income` | Доход | — | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |

### География

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/attributes/events/geo>

| Имя | Ключ в записи | Название | Описание | Операторы фильтрации | Минимальная дата |
|---|---|---|---|---|---|
| `am:e:regionContinent` | `region_continent` | Континент | — | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:regionCountry` | `region_country` | Страна | — | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:regionDistrict` | `region_district` | Округ | — | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:regionArea` | `region_area` | Область | — | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:regionCity` | `region_city` | Город | — | `!.`, `!=`, `=.`, `==` | 2019-01-01 |
| `am:e:regionCitySize` | `region_city_size` | Размер города | Размер города по населению. | `!.`, `!=`, `<`, `<=`, `=.`, `==`, `>`, `>=` | 2019-01-01 |

### Браузер

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/attributes/events/browser>

| Имя | Ключ в записи | Название | Описание | Операторы фильтрации | Минимальная дата |
|---|---|---|---|---|---|
| `am:e:browser` | `browser` | Браузер | — | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |

### Операционные системы

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/attributes/events/os>

| Имя | Ключ в записи | Название | Описание | Операторы фильтрации | Минимальная дата |
|---|---|---|---|---|---|
| `am:e:operatingSystemRoot` | `operating_system_root` | Группа операционных систем | — | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |
| `am:e:operatingSystem` | `operating_system` | Операционная система | Название операционной системы | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |

### Технологии

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/attributes/events/technology>

| Имя | Ключ в записи | Название | Описание | Операторы фильтрации | Минимальная дата |
|---|---|---|---|---|---|
| `am:e:deviceType` | `device_type` | Тип устройства | Тип устройства, с которого было взаимодействие с рекламными материалами. Возможные значения: `desktop`, `mobile`, `tablet`, `tv` | `!*`, `!.`, `!=`, `!@`, `!~`, `<`, `<=`, `=*`, `=.`, `==`, `=@`, `=~`, `>`, `>=` | 2019-01-01 |

## Метрики

### Базовые метрики

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/metrics/events/basic>

| Имя | Ключ в записи | Название | Описание | Тип | Фильтрация | Минимальная дата |
|---|---|---|---|---|---|---|
| `am:e:renders` | `renders` | Показы | Количество показов. | `int` | есть | 2019-01-01 |
| `am:e:renderFrequency` | `render_frequency` | Частота показов | Среднее количество показов | `double` | есть | 2019-01-01 |
| `am:e:clicks` | `clicks` | Клики | Количество кликов. | `int` | есть | 2019-01-01 |
| `am:e:ctr` | `ctr` | CTR | Отношение числа кликов по рекламе к числу ее показов | `percents` | есть | 2019-01-01 |
| `am:e:users` | `users` | Охват | Количество уникальных пользователей | `int` | есть | 2019-01-01 |

### Целевые метрики

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/metrics/events/conversion>

Ключи в таблице показаны для `goal_id` = 12345.

| Имя | Ключ в записи | Название | Описание | Тип | Фильтрация | Минимальная дата |
|---|---|---|---|---|---|---|
| `am:e:goal<goal_id>Conversion` | `goal12345_conversion` | Конверсия | Доля целевой конверсии | `percents` | есть | 2019-01-01 |
| `am:e:goal<goal_id>ConversionPostView` | `goal12345_conversion_post_view` | Конверсия по показам | Доля целевой конверсии по показам | `percents` | есть | 2019-01-01 |
| `am:e:goal<goal_id>ConversionPostClick` | `goal12345_conversion_post_click` | Конверсия по кликам | Доля целевой конверсии по кликам | `percents` | есть | 2019-01-01 |
| `am:e:goal<goal_id>Reaches` | `goal12345_reaches` | Количество конверсий | Количество выполнений целевого условия по всем показам | `int` | есть | 2019-01-01 |
| `am:e:goal<goal_id>ReachesPostView` | `goal12345_reaches_post_view` | Количество конверсий, атрибуцированных к показам | Количество конверсий по указанной цели, атрибуцированных к показам рекламы | `int` | есть | 2019-01-01 |
| `am:e:goal<goal_id>ReachesPostClick` | `goal12345_reaches_post_click` | Количество конверсий, атрибуцированных к кликам | Количество конверсий по указанной цели, атрибуцированных к кликам по рекламе | `int` | есть | 2019-01-01 |

### Электронная коммерция

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/metrics/events/ecommerce>

Ключи в таблице показаны для `currency` = `RUB`.

| Имя | Ключ в записи | Название | Описание | Тип | Фильтрация | Минимальная дата |
|---|---|---|---|---|---|---|
| `am:e:ecommerce<currency>Revenue` | `ecommerce_rub_revenue` | Доход | Суммарный доход. Работает по множеству `Покупки`. | `currency` | есть | 2019-01-01 |
| `am:e:ecommerce<currency>RevenuePostView` | `ecommerce_rub_revenue_post_view` | Доход по показам | Суммарный доход по всем показам. Работает по множеству `Покупки`. | `currency` | есть | 2019-01-01 |
| `am:e:ecommerce<currency>RevenuePostClick` | `ecommerce_rub_revenue_post_click` | Доход по кликам | Суммарный доход по всем кликам. Работает по множеству `Покупки`. | `currency` | есть | 2019-01-01 |

### Финансовые показатели

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/metrics/events/finance>

Ключи параметризованных имён показаны для `goal_id` = 12345.

| Имя | Ключ в записи | Название | Описание | Тип | Фильтрация | Минимальная дата |
|---|---|---|---|---|---|---|
| `am:e:cpm` | `cpm` | CPM | Цена тысячи просмотров рекламы | `double` | есть | 2019-01-01 |
| `am:e:cpmu` | `cpmu` | CPMU | — | `double` | есть | 2019-01-01 |
| `am:e:cpc` | `cpc` | CPC | Цена одного перехода пользователя по рекламе | `double` | есть | 2019-01-01 |
| `am:e:cpa<goal_id>` | `cpa12345` | Стоимость конверсии | Средняя стоимость конверсии | `double` | есть | 2019-01-01 |
| `am:e:cpa<goal_id>PostView` | `cpa12345_post_view` | Стоимость конверсии по показам | Средняя стоимость конверсии по показам | `double` | есть | 2019-01-01 |
| `am:e:cpa<goal_id>PostClick` | `cpa12345_post_click` | Стоимость конверсии по кликам | Средняя стоимость конверсии по кликам | `double` | есть | 2019-01-01 |

### Видеореклама

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/metrics/events/video>

| Имя | Ключ в записи | Название | Описание | Тип | Фильтрация | Минимальная дата |
|---|---|---|---|---|---|---|
| `am:e:videoRenders` | `video_renders` | Показы видео | Количество показов видео | `int` | есть | 2019-01-01 |
| `am:e:videoMeasurableRendersPercent` | `video_measurable_renders_percent` | Доля показанных видео | — | `percents` | есть | 2019-01-01 |
| `am:e:videoVisibility` | `video_visibility` | Видимость | Процент видимых показов рекламы | `percents` | есть | 2019-01-01 |
| `am:e:videoStarts` | `video_starts` | Старты видео | Количество стартов видео | `int` | есть | 2019-01-01 |
| `am:e:videoStartsVisible` | `video_starts_visible` | Просмотры видео в видимой части экрана | — | `int` | есть | 2019-01-01 |
| `am:e:videoFirstQuartiles` | `video_first_quartiles` | Просмотры видео до 25% | — | `int` | есть | 2019-01-01 |
| `am:e:videoFirstQuartilesVisible` | `video_first_quartiles_visible` | Просмотры видео до 25% в видимой части экрана | — | `int` | есть | 2019-01-01 |
| `am:e:videoFirstQuartilesPercent` | `video_first_quartiles_percent` | Доля просмотров видео до 25% | — | `percents` | есть | 2019-01-01 |
| `am:e:videoFirstQuartilesVisiblePercent` | `video_first_quartiles_visible_percent` | Доля просмотров видео до 25% в видимой части экрана | — | `percents` | есть | 2019-01-01 |
| `am:e:videoSecondQuartiles` | `video_second_quartiles` | Просмотры видео до 50% | — | `int` | есть | 2019-01-01 |
| `am:e:videoSecondQuartilesVisible` | `video_second_quartiles_visible` | Просмотры видео до 50% в видимой части экрана | — | `int` | есть | 2019-01-01 |
| `am:e:videoSecondQuartilesPercent` | `video_second_quartiles_percent` | Доля просмотров видео до 50% | — | `percents` | есть | 2019-01-01 |
| `am:e:videoSecondQuartilesVisiblePercent` | `video_second_quartiles_visible_percent` | Доля просмотров видео до 50% в видимой части экрана | — | `percents` | есть | 2019-01-01 |
| `am:e:videoThirdQuartiles` | `video_third_quartiles` | Просмотры видео до 75% | — | `int` | есть | 2019-01-01 |
| `am:e:videoThirdQuartilesVisible` | `video_third_quartiles_visible` | Просмотры видео до 75% в видимой части экрана | — | `int` | есть | 2019-01-01 |
| `am:e:videoThirdQuartilesPercent` | `video_third_quartiles_percent` | Доля просмотров видео до 75% | — | `percents` | есть | 2019-01-01 |
| `am:e:videoThirdQuartilesVisiblePercent` | `video_third_quartiles_visible_percent` | Доля просмотров видео до 75% в видимой части экрана | — | `percents` | есть | 2019-01-01 |
| `am:e:videoComplete` | `video_complete` | Просмотры видео до конца | — | `int` | есть | 2019-01-01 |
| `am:e:videoCompleteVisible` | `video_complete_visible` | Просмотры видео до конца в видимой части экрана | — | `int` | есть | 2019-01-01 |
| `am:e:videoCompletePercent` | `video_complete_percent` | Доля просмотров видео до конца | — | `percents` | есть | 2019-01-01 |
| `am:e:videoCompleteVisiblePercent` | `video_complete_visible_percent` | Доля просмотров видео до конца в видимой части экрана | — | `percents` | есть | 2019-01-01 |

## Параметризация

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/param>

Часть имён параметризована: параметр записывается в угловых скобках прямо внутри
имени. Параметров два:

| Параметр | Название | Описание | Значение по умолчанию |
|---|---|---|---|
| `goal_id` | Цель | Идентификатор цели. | — |
| `currency` | Валюта | Некоторые группировки позволяют настраивать валюту. Возможные значения: `RUB`, `USD`, `EUR`, `YND`. См. также [ISO-4217](https://ru.wikipedia.org/wiki/ISO_4217). | Зависит от настроек счётчика |

Задать параметр можно двумя способами, и оператор поддерживает оба:

```python
# значение вписано в само имя
metrics=["am:e:goal12345Reaches"]

# значение задано отдельным параметром запроса и действует на все имена
metrics=["am:e:goal<goal_id>Reaches"]
extra_params={"goal_id": 12345}
```

Оба написания дают один и тот же ключ в записи — `goal12345_reaches`, — поэтому
отчёт, переписанный с одного написания на другое, продолжает наполнять ту же
колонку. Оба способа можно использовать в одном запросе одновременно.

Если параметр не задан ни одним из способов, имя уходит в API как есть, а ключ
сохраняет имя параметра на месте значения (`am:e:goal<goal_id>Reaches` даёт
`goalgoal_id_reaches`), и провайдер пишет об этом WARNING. Собственное имя цели в
ключе видно сразу, тогда как молчаливое выбрасывание параметра слило бы все цели
аккаунта в одну колонку, и заметить это можно было бы только по завышенным
числам.

### Ключи, которых `extra_params` не принимает

`extra_params` добавляет в запрос имена, которых в нём ещё нет, и не
переопределяет ни одного: параметры оператора выигрывают всегда. Тринадцать имён
запрещены совсем — `ids`, `date1`, `date2`, `metrics`, `dimensions`, `limit`,
`offset`, `sort`, `accuracy`, `include_undefined`, `filters`, `timezone`, `lang`.
Любое из них поднимает `ValueError` до первого запроса. Каждое из них — это либо
сам задаваемый вопрос, либо ответ на то, как он задаётся, а последние пять
доступны отдельными параметрами оператора, так что маршрут через `extra_params`
для них не нужен.

## Операторы фильтрации

Расшифровка операторов из колонки «Операторы фильтрации» таблиц группировок:

| Оператор | Значение |
|---|---|
| `==` | равно |
| `!=` | не равно |
| `=.` | встречается среди значений. Вы можете указать в запросе до 100 значений |
| `!.` | не встречается среди значений |
| `=@` | является подстрокой |
| `!@` | не является подстрокой |
| `=~` | попадает под регулярное выражение |
| `!~` | не попадает под регулярное выражение |
| `=*` | равно, с возможностью поиска по `*` |
| `!*` | не равно, с возможностью поиска по `*` |
| `<` | меньше |
| `<=` | меньше либо равно |
| `>` | больше |
| `>=` | больше либо равно |

## Лимиты запроса

Первоисточник: <https://yandex.ru/dev/admetrica/doc/ru/openapi/report/data>

| Что | Лимит |
|---|---|
| Метрик в запросе | 20 |
| Группировок в запросе | 10 |
| `limit` — строк на странице | 100 000 (по умолчанию у API — 100) |
| `offset` — индекс первой строки выборки | отсчёт с 1 |
| Уникальных группировок и метрик в фильтре | 10 |
| Отдельных фильтров | 20 |
| Длина строки фильтра | 10 000 символов |
| Значений в одном условии фильтрации | 100 |

Первые два лимита оператор проверяет до того, как уйдёт первый запрос: список,
который вышел за границу, поднимает `ValueError` с указанием, какой это список и
на сколько он длиннее допустимого. Оператор запрашивает по 10 000 строк на
страницу — это `limit` по умолчанию.

Общей минимальной даты у отчётов нет: она своя у каждого имени и указана в
колонке «Минимальная дата». У большинства это `2019-01-01`, у категорий интересов
(`am:e:interest2d1`, `am:e:interest2d2`, `am:e:interest2d3`) — `2018-09-01`.

## Готовые наборы для типовых отчётов

| Отчёт | `dimensions` | `metrics` |
|---|---|---|
| Площадки и креативы | `am:e:placement`, `am:e:site`, `am:e:creative` | `am:e:renders`, `am:e:clicks`, `am:e:ctr`, `am:e:users` |
| Соц-дем | `am:e:gender`, `am:e:ageInterval`, `am:e:income` | `am:e:renders`, `am:e:users`, `am:e:clicks` |
| География | `am:e:regionCountry`, `am:e:regionArea`, `am:e:regionCity` | `am:e:renders`, `am:e:clicks`, `am:e:ctr` |
| Технологии | `am:e:deviceType`, `am:e:operatingSystemRoot`, `am:e:browser` | `am:e:renders`, `am:e:clicks`, `am:e:ctr` |
| Экономика размещения | `am:e:placement` | `am:e:renders`, `am:e:clicks`, `am:e:ctr`, `am:e:cpm`, `am:e:cpc` |
| Видео | `am:e:placement`, `am:e:creative` | `am:e:videoRenders`, `am:e:videoStarts`, `am:e:videoFirstQuartiles`, `am:e:videoSecondQuartiles`, `am:e:videoThirdQuartiles`, `am:e:videoComplete`, `am:e:videoCompletePercent` |
| Конверсии по цели | `am:e:placement` | `am:e:renders`, `am:e:clicks`, `am:e:goal<goal_id>Reaches`, `am:e:goal<goal_id>Conversion`, `am:e:cpa<goal_id>` |

Отчёт по цели требует значения параметра — оно передаётся `extra_params`:

```python
YandexAdmetricaStatsOperator(
    task_id="collect",
    admetrica_conn_id="yandex_admetrica_default",
    date="{{ params.date }}",
    dimensions=["am:e:placement"],
    metrics=[
        "am:e:renders",
        "am:e:clicks",
        "am:e:goal<goal_id>Reaches",
        "am:e:goal<goal_id>Conversion",
        "am:e:cpa<goal_id>",
    ],
    extra_params={"goal_id": 12345},
    base_dir="/tmp/yandex_admetrica",
)
```

Набор группировок и метрик задаёт форму записи, поэтому его смена меняет файлы,
записанные после неё; выгруженные раньше остаются в прежней форме. Приведение
истории к новому набору — ручная перезаливка нужного периода.

## Первоисточники

- Сводный список группировок и метрик: <https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all>
- Группировки: [размещения](https://yandex.ru/dev/admetrica/doc/ru/attributes/events/placements),
  [аудитория](https://yandex.ru/dev/admetrica/doc/ru/attributes/events/audience),
  [гео](https://yandex.ru/dev/admetrica/doc/ru/attributes/events/geo),
  [браузер](https://yandex.ru/dev/admetrica/doc/ru/attributes/events/browser),
  [ОС](https://yandex.ru/dev/admetrica/doc/ru/attributes/events/os),
  [технологии](https://yandex.ru/dev/admetrica/doc/ru/attributes/events/technology)
- Метрики: [базовые](https://yandex.ru/dev/admetrica/doc/ru/metrics/events/basic),
  [целевые](https://yandex.ru/dev/admetrica/doc/ru/metrics/events/conversion),
  [ecommerce](https://yandex.ru/dev/admetrica/doc/ru/metrics/events/ecommerce),
  [финансовые](https://yandex.ru/dev/admetrica/doc/ru/metrics/events/finance),
  [видео](https://yandex.ru/dev/admetrica/doc/ru/metrics/events/video)
- Параметризация: <https://yandex.ru/dev/admetrica/doc/ru/param>
- Метод `/v1/stat/data`: <https://yandex.ru/dev/admetrica/doc/ru/openapi/report/data>
- Введение в API отчётов: <https://yandex.ru/dev/admetrica/doc/ru/reports-intro>

Любая страница документации доступна в markdown добавлением `.md` к адресу.
