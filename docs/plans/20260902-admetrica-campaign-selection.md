# Отбор кампаний при сборе статистики AdMetrica

## Overview

`AdmetricaHook.get_stats()` обходит весь список кампаний рекламодателя, делая по
одному запросу на кампанию на каждый выгружаемый день. Сто кампаний за месяц —
три тысячи запросов, из которых актуальны единицы: остальные кампании давно в
архиве и новых цифр не дают. Лимиты AdMetrica тратятся впустую.

План сужает обход статистики до нужного подмножества кампаний:

- по умолчанию — только активные (`status == "active"`);
- `campaign_scope="all"` возвращает прежний полный обход и статус не смотрит
  вовсе;
- `campaign_ids` / `campaign_names` задают конкретные кампании и перекрывают
  `campaign_scope` — так перезаливается архивная кампания;
- все параметры шаблонные, поэтому значение доезжает из `params` DAG при ручном
  запуске: по расписанию собираются активные, руками — что угодно.

Словарь кампаний это не затрагивает: `get_campaigns()` по-прежнему тянет полный
список, он же уезжает в `dict/campaigns` целиком. Отбор режет только обход
статистики.

Изменение меняет поведение по умолчанию. Совместимость не поддерживается:
провайдер нигде не применяется, переходных ветвей и раздела «как вернуть
прежнее поведение» не нужно.

## Context (from discovery)

- код: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py` (3123
  строки, `get_campaigns` со строки 2553, `get_stats` со строки 2722,
  `_campaign_record` на 1616), `operators/stats.py` (356 строк);
- тесты, утверждающие текущее поведение: `tests/test_stats.py`,
  `tests/test_operator.py`, `tests/test_campaigns.py`, `tests/test_hook_meta.py`
  и — важнее прочих — `tests/test_token_never_leaves.py`, где кэш списка
  кампаний семь раз сеется вручную **без поля `status`** (строки 99, 306, 324,
  340, 371, 382, 393). Со сменой умолчания отбор не найдёт в них ни одной
  кампании и весь файл станет красным;
- `tests/test_readme.py:124` сверяет таблицу параметров обоих README с
  `inspect.signature(YandexAdmetricaStatsOperator)` — новый параметр без строки
  в таблице роняет набор;
- документация: `README.md`, `README_RU.md`, `CONTEXT.md`, `CHANGELOG.md`;
- примеры: три DAG в `examples/`, покрытые `tests/test_example_s3_dag.py`,
  `tests/test_example_bigquery_dag.py` и `tests/test_example_dag.py`
  (последний — про `admetrica_to_bq_and_s3_dag`);
- паттерны: развёрнутые английские докстроки, объясняющие *почему*; проверки
  конфигурации (`check_date`, `_check_report_limits`, `_check_extra_params`) —
  `ValueError` до первого запроса; чужой текст в сообщениях проходит
  `_scrub` / `_truncate` / `_one_line` / `_quoted_parameter` — все они живут в
  хуке и работают против живого токена, которого до построения хука ещё нет;
  `template_fields`
  объявлены кортежем; в примерах DAG параметры объявлены через
  `airflow.models.param.Param`, а `template_undefined` у DAG строгий;
- зависимости: `get_stats()` внутри вызывает `self.get_campaigns()` — это
  единственное место, где решается, по каким кампаниям идёт обход;
- тесты запускаются `.venv/bin/pytest` (`testpaths = ["tests"]`,
  `pythonpath = ["."]`).

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change
- **комментарии и докстроки — на английском, в тоне существующего кода;
  комментарий описывает настоящее, никогда прошлое: ни «раньше фильтра не
  было», ни «теперь вместо», ни «это больше не нужно»**
- README на двух языках держатся синхронными

## Testing Strategy

- **unit tests**: обязательны в каждой задаче (см. Development Approach)
- **e2e tests**: в проекте нет UI и e2e-тестов; их роль играют тесты примеров
  DAG (`tests/test_example_*_dag.py`), которые парсят DAG и проверяют состав
  тасок и аргументы операторов — они обновляются в той же задаче, что и примеры
- команда: `.venv/bin/pytest`
- параметр появляется в сигнатуре оператора и в таблице README **в одной
  задаче**: `tests/test_readme.py` сверяет одно с другим, и разнесение по
  задачам сделало бы промежуточный прогон заведомо красным

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

Отбор — самостоятельное понятие, поэтому у него отдельный модуль
`airflow_provider_yandex_admetrica/campaign_selection.py` с одним
объектом-значением `CampaignSelection`. Он знает три вещи и ничего сверх: как
превратить значение из `params` в отбор (`parse`), какие кампании списка ему
соответствуют (`matching`) и что из явно названного в списке отсутствует
(`missing`).

Разделение обязанностей:

- **`campaign_selection.py`** — единственное место, где строка становится
  значением, и единственное место, где записано правило отбора. Чистые функции,
  тестируются без HTTP и без Airflow.
- **хук** — принимает готовый `CampaignSelection` в `get_stats()` и обходит
  `selection.matching(self.get_campaigns())`. `get_campaigns()` не меняется:
  пагинация, кэш списка и сверка с `total` остаются какими были.
- **оператор** — держит четыре шаблонных поля, разбирает их в `execute()` до
  построения хука, после получения списка кампаний применяет политику
  `on_missing_campaign`, пишет строку в лог и отдаёт отбор хуку.

Ключевые решения и их мотивы:

- **`campaign_scope` — строка, а не `bool`.** Шаблон `{{ params.x }}`
  рендерится в строку, и `"False"` истинна: булев флаг молча включался бы, а
  заметно это было бы по счёту запросов, не по ошибке. У строки такой ловушки
  нет — опечатка `"activ"` падает по имени.
- **Явный список перекрывает `campaign_scope` целиком.** Назвали кампанию —
  значит хотите именно её, архивную в том числе, и вспоминать про второй
  параметр не нужно.
- **Ненайденная кампания по умолчанию не роняет таску.** Удалённая в интерфейсе
  кампания пропадает из списка кабинета, и запросить её цифры невозможно в
  принципе; падение из-за этого красило бы расписание, в коде которого ничего
  не сломалось. `on_missing_campaign="fail"` включает падение там, где список
  набирается руками и опечатка дороже.
- **Пустой отбор по `campaign_scope` — не ошибка.** Кабинет без активных
  кампаний законен; это WARNING, никогда не падение. Но WARNING обязан быть
  диагностическим: словарь значений `status` в API нигде не документирован, и
  пустой отбор — единственное место, где незнакомое значение статуса себя
  проявит. Поэтому строка называет, сколько кампаний в списке и какие статусы в
  нём встретились.
- **Словарь остаётся полным снимком кабинета.** По нему потом и видно, какая
  кампания пропущена и почему, и когда она исчезла из кабинета.

## Technical Details

### `CampaignSelection`

```python
from __future__ import annotations   # -> CampaignSelection и frozenset[int] на 3.10

SCOPE_ACTIVE = "active"
SCOPE_ALL = "all"
ACTIVE_STATUS = "active"

@dataclass(frozen=True)
class CampaignSelection:
    scope: str = SCOPE_ACTIVE
    ids: frozenset[int] = frozenset()
    names: frozenset[str] = frozenset()

    @classmethod
    def parse(cls, *, scope=SCOPE_ACTIVE, ids=None, names=None) -> CampaignSelection: ...

    @property
    def is_explicit(self) -> bool: ...          # bool(self.ids or self.names)

    def matching(self, campaigns: Sequence[dict]) -> list[dict]: ...
    def missing(self, campaigns: Sequence[dict]) -> tuple[frozenset[int], frozenset[str]]: ...
```

### Правила разбора (`parse`)

| Что пришло | Как читается |
|---|---|
| `None` | не задано |
| `list` / `tuple` / `set` | элементы как есть |
| строка, пустая после `strip()` | не задано (сюда попадает отрендеренный пустой `{{ params.ids }}`) |
| строка, начинающаяся с `[` | `ast.literal_eval`; результат обязан быть списком/кортежем |
| прочая строка | split по запятой, `strip()` каждого элемента, пустые отбрасываются |

Разбор скобки идёт через `ast.literal_eval`, а не через `json.loads`, потому что
Jinja рендерит список в python-repr: `Param(type="array")` со значением
`["Летняя кампания"]` приезжает строкой `['Летняя кампания']` — одинарные
кавычки, для JSON это синтаксическая ошибка. `literal_eval` читает и такую
запись, и настоящий JSON с двойными кавычками, и `[123, 534, 234]`, при этом
исполняет не код, а только литералы.

Дальше: `ids` → положительный `int` (`"12a"`, `0`, `-1` — `ValueError`,
описывающий значение по правилу ниже); `names` → строки со `strip()`, и пустые
после него отбрасываются наравне с пустыми элементами после split: иначе
`campaign_names=[""]` и отрендеренное `"['']"` дают непустой явный отбор, который
перекрывает `campaign_scope` и не совпадает ни с одной кампанией, — день ушёл бы
без единого запроса. `scope` → `strip()` и `lower()`, допустимы только
`"active"` и `"all"`.

Значения `campaign_ids` / `campaign_names` — произвольный текст снаружи, и отказ
их **не цитирует**: он описывает значение по типу и длине, как это делает
`check_date`. Разбор идёт до построения хука, то есть до того, как токен вообще
прочитан, поэтому маскирующие помощники хука здесь неприменимы — им нечего
вычищать, — а `campaign_ids`, в который по ошибке проставлено значение
OAuth-токена, ушёл бы в traceback Airflow целиком. Пример:

```
campaign_ids holds a str of 64 characters, which is not a campaign id. A
campaign is asked for by a positive whole number, so the export stops here
rather than walking a selection that names nothing.
```

Короткое значение, состоящее из букв и цифр ASCII длиной до восьми символов,
цитируется: `"12a"` — это опечатка, а не учётка, и без неё сообщение не
показывает, что именно не прочиталось. Границу проводит одна приватная функция
модуля; помощники хука (`_one_line`, `_truncate`, `_quoted_parameter`) сюда не
импортируются — Task 3 импортирует `CampaignSelection` в обратную сторону, и
пара импортов замкнула бы цикл.

### Правило отбора (`matching`)

1. `is_explicit` → кампании, чей `campaign_id` есть в `ids` **или** чьё имя есть
   в `names`; `scope` не смотрится;
2. иначе `scope == SCOPE_ALL` → весь список, статус не читается вовсе;
   `scope == SCOPE_ACTIVE` → статус равен `ACTIVE_STATUS` после `strip()` и
   `lower()`.

Порядок исходного списка сохраняется. Кампания, совпавшая и по id, и по имени,
попадает в результат один раз — дубликат удвоил бы строки дня. Имена
сравниваются целиком, с учётом регистра, со `strip()` с обеих сторон. Одно имя
может совпасть с несколькими кампаниями — берутся все.

`name` и `status` читаются через `.get()` и участвуют в сравнении только будучи
строками: `_campaign_record` кладёт эти поля как есть, `row.get(field)`, и
валидируется у записи один `campaign_id`. Кампания без имени должна не совпасть
ни с чем, а не уронить обход `AttributeError`.

Совпадение имени живёт в одной приватной функции — «эта кампания названа этим
отбором», — и `matching` и `missing` спрашивают её обе. Разойдись они, кампания
с хвостовым пробелом в имени попала бы в выгрузку по `matching` и одновременно
числилась ненайденной по `missing`: WARNING про кампанию, которая как раз
собирается, а при `on_missing_campaign="fail"` — упавшая таска на полностью
корректном отборе.

### Политика `on_missing_campaign`

`missing()` возвращает то из `ids` и `names`, чего в списке кабинета нет — id и
имена раздельно, потому что сообщение их различает.

- `"warn"` — WARNING, перечисляющий ненайденное, обход по найденным;
- `"fail"` — `ValueError` с тем же перечислением. `ValueError`, а не
  `AirflowException`: это отказ по значению, названному снаружи, и таблица
  политик в `CONTEXT.md` относит такие к `ValueError`.

Сверка возможна только после `get_campaigns()`, то есть после первого HTTP-обхода
management API. Разбор значений от неё отделён: он идёт до построения хука, и
кривой `campaign_scope` не стоит ни одного запроса.

`execute()` берёт список **безусловно**, независимо от `collect_dictionaries`:
сегодня список запрашивается либо внутри `get_stats`, либо в `_export_campaigns`,
а последний работает только при включённом словаре — привязка политики к нему
означала бы, что при `collect_dictionaries=False` ни политика
`on_missing_campaign`, ни строка о пропущенных, ни WARNING о пустом отборе не
появляются никогда. Второго HTTP-обхода это не стоит: список кэширован на время
таски.

Пустой результат при `is_explicit == False` — отдельный случай: WARNING всегда,
падения нет никогда, и строка называет размер списка и встреченные в нём
статусы.

### Строка в логе

```
Collecting statistics for 3 of 127 campaigns (scope=active, 124 skipped).
Collecting statistics for 2 of 127 campaigns (explicit selection; campaign_scope="active" not applied).
No campaign of the 127 listed is active (statuses seen: active, archived); no statistics requested.
```

Число пропущенных называется всегда: тихо укоротившаяся выгрузка — главный риск
этого изменения, и лог обязан его показывать.

### Что не меняется

Возвращаемое значение оператора (`ExportRecord`), раскладка файлов, диагностика
в Loki (её события описывают HTTP-попытки, а отбор — не запрос; событий просто
станет меньше), `get_campaigns()` и все проверки полноты списка.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): код, тесты, документация,
  примеры DAG
- **Post-Completion** (no checkboxes): проверка на живом рекламодателе, снятие
  фактического словаря статусов, тег релиза

## Implementation Steps

### Task 1: Модуль `campaign_selection.py` — разбор значений из `params`

**Files:**
- Create: `airflow_provider_yandex_admetrica/campaign_selection.py`
- Create: `tests/test_campaign_selection.py`

- [ ] создать модуль с `from __future__ import annotations`, константами
      `SCOPE_ACTIVE`, `SCOPE_ALL`, `ACTIVE_STATUS` и frozen-dataclass
      `CampaignSelection` (`scope`, `ids`, `names`) с докстрокой, объясняющей,
      почему `scope` — строка, а не флаг
- [ ] реализовать приватный разбор списка: `None`, `list`/`tuple`/`set`, пустая
      строка, `ast.literal_eval` по ведущему `[` (с объяснением, почему не
      `json.loads`), строка через запятую
- [ ] реализовать `CampaignSelection.parse` — приведение `ids` к положительным
      `int`, `names` к строкам со `strip()` с отбрасыванием пустых, `scope` к
      `"active"`/`"all"` через `strip().lower()`; все отказы `ValueError`
- [ ] написать приватную функцию описания отвергнутого значения: короткое
      значение из букв и цифр ASCII цитируется, всё остальное описывается по
      типу и длине. Помощники хука (`_one_line`, `_truncate`,
      `_quoted_parameter`) сюда **не импортируются**: Task 3 импортирует
      `CampaignSelection` в хук, и пара импортов замкнула бы цикл на модуле, чьи
      помощники определены ниже блока импортов, — пакет перестал бы
      импортироваться целиком; к тому же на этапе разбора токена ещё нет
- [ ] добавить свойство `is_explicit`
- [ ] написать тесты разбора: нативные список/кортеж/множество, строка через
      запятую с пробелами, `[123, 534, 234]`, python-repr со строками в
      одинарных кавычках, JSON с двойными, пустая строка, `None`, регистр
      `scope`, пустой элемент в нативном списке и в списке в скобках
- [ ] написать тесты отказов: `"12a"`, `0`, `-1`, битая скобка, скобка со
      словарём вместо списка, `scope="activ"`; отдельным тестом — что длинное
      значение в сообщение не попадает, а описывается типом и длиной
- [ ] запустить `.venv/bin/pytest tests/test_campaign_selection.py` — должно
      пройти

### Task 2: Правило отбора — `matching` и `missing`

**Files:**
- Modify: `airflow_provider_yandex_admetrica/campaign_selection.py`
- Modify: `tests/test_campaign_selection.py`

- [ ] реализовать `matching(campaigns)`: явный список перекрывает `scope`,
      объединение id и имён по ИЛИ, сохранение порядка, без дубликатов; `scope
      == "all"` статус не читает вовсе
- [ ] читать `name` и `status` через `.get()` и сравнивать только строки —
      кампания без имени или без статуса не совпадает ни с чем и не роняет
      обход
- [ ] вынести совпадение имени в одну приватную функцию — `strip()` с обеих
      сторон, регистр значим, нестроковое имя не совпадает — и спрашивать её и
      из `matching`, и из `missing`
- [ ] реализовать `missing(campaigns)` — не найденные в списке кабинета явно
      названные id и имена, раздельно, по той же функции совпадения
- [ ] докстроки: почему явный список побеждает, почему кампания, совпавшая
      дважды, обходится один раз, и почему статус сравнивается после
      `strip().lower()`
- [ ] написать тесты `matching`: `scope="active"` отсекает архив, `scope="all"`
      берёт всё (в том числе запись без поля `status`), отбор по id, по имени,
      объединение, перекрытие `scope`, совпадение имени с несколькими
      кампаниями, `strip()` вокруг имени, чувствительность к регистру имени,
      нечувствительность к регистру статуса, сохранение порядка, отсутствие
      дубликата, кампания без имени, кампания без статуса
- [ ] написать тесты `missing`: ничего не потеряно, потеряна часть, потеряно
      всё, и — зеркально тестам `matching` — имя с пробелами по краям и имя,
      отличающееся регистром: реализация прямым сравнением на первом из них
      объявила бы ненайденной кампанию, которую `matching` как раз собирает
- [ ] запустить `.venv/bin/pytest tests/test_campaign_selection.py` — должно
      пройти

### Task 3: Хук — `get_stats(selection=...)`

**Files:**
- Modify: `airflow_provider_yandex_admetrica/hooks/yandex_admetrica.py`
- Modify: `tests/test_stats.py`
- Modify: `tests/test_token_never_leaves.py`
- Modify: `tests/test_hook_meta.py`

- [ ] добавить в `get_stats()` параметр `selection: CampaignSelection =
      CampaignSelection()` — экземпляр frozen-датакласса как значение по
      умолчанию, чтобы «по умолчанию активные» было записано в одном месте, а не
      продублировано веткой `None`
- [ ] заменить обход `self.get_campaigns()` на
      `selection.matching(self.get_campaigns())`, не трогая пагинацию, кэш
      списка и сверку с `total`
- [ ] переписать первую строку докстроки `get_stats`
      (`hooks/yandex_admetrica.py:2735`, «Return one day of statistics for every
      campaign of the advertiser») — после изменения она описывает поведение,
      которого у кода нет, — и дополнить абзацем о том, что обход идёт по
      отбору, а полный список остаётся за `get_campaigns()` со сверкой полноты и
      словарём
- [ ] обновить докстроку `get_campaigns` там, где она говорит о запрашиваемых
      статусах: параметр `status` в запрос по-прежнему не уходит, список
      остаётся полным, а сужение живёт в отборе статистики
- [ ] проставить `status` в семи местах `tests/test_token_never_leaves.py`, где
      кэш кампаний сеется вручную (строки 99, 306, 324, 340, 371, 382, 393), —
      иначе отбор по умолчанию не найдёт ни одной кампании и к отчётному
      эндпоинту не уйдёт ни одного запроса
- [ ] написать тесты: запросы уходят только по отобранным кампаниям (счётчик
      вызовов и переданные `ids`), умолчание хука — только активные,
      `scope="all"` обходит весь список, пустой отбор не даёт ни одного запроса
- [ ] запустить `.venv/bin/pytest tests/test_stats.py tests/test_campaigns.py
      tests/test_token_never_leaves.py tests/test_hook_meta.py` — должно пройти

### Task 4: Оператор — четыре параметра, разбор и политика

**Files:**
- Modify: `airflow_provider_yandex_admetrica/operators/stats.py`
- Modify: `README.md`
- Modify: `README_RU.md`
- Modify: `tests/test_operator.py`

- [ ] добавить параметры `campaign_scope="active"`, `campaign_ids=None`,
      `campaign_names=None`, `on_missing_campaign="warn"` и включить все четыре
      в `template_fields`
- [ ] разбирать их в `execute()` через `CampaignSelection.parse` рядом с
      `check_date(self.date)` — до построения хука, чтобы ошибка конфигурации не
      стоила ни одного запроса; `on_missing_campaign` проверять там же на
      принадлежность `{"warn", "fail"}`
- [ ] вызвать `campaigns = hook.get_campaigns()` в `execute()` **безусловно**,
      независимо от `collect_dictionaries` (список кэширован, второго обхода не
      будет), и применить политику: `missing()` при `"fail"` даёт `ValueError`,
      при `"warn"` — WARNING, перечисляющий ненайденное; всё это до
      `hook.get_stats(...)`
- [ ] написать строку INFO о числе отобранных и пропущенных кампаний в обеих
      формах (по `scope` и по явному списку); пустой отбор без явного списка —
      WARNING, называющий размер списка и встреченные статусы
- [ ] передать отбор в `hook.get_stats`; переписать первую строку докстроки
      класса (`operators/stats.py:98`, «Collect one day of statistics for every
      campaign of one advertiser») и дополнить абзацем об отборе: что режется,
      что нет и почему словарь остаётся полным
- [ ] добавить четыре параметра в таблицу параметров оператора **обоих** README
      (после `collect_dictionaries`) и обновить строку о том, какие поля
      шаблонные — `tests/test_readme.py:124` сверяет таблицу с сигнатурой, и
      без этого прогон красный
- [ ] переписать `tests/test_operator.py:533` — `run.get_campaigns.assert_not_called()`
      в `test_switched_off_it_writes_no_dictionary` становится «вызван один раз,
      файл словаря не написан». Ассерция зелена только потому, что `_Run` патчит
      и `get_stats`, и `get_campaigns`; поставить вызов под
      `if self.collect_dictionaries`, чтобы её сохранить, — значит молча
      отключить сторож укоротившейся выгрузки
- [ ] написать тест: политика `on_missing_campaign` и строка отбора работают при
      `collect_dictionaries=False`
- [ ] написать тесты: умолчание доносит до `get_stats` отбор «только активные»
      (в `tests/test_operator.py` хук замокан, поэтому проверяется доехавший
      `CampaignSelection`, а не состав HTTP-запросов — это уже покрыто Task 3);
      `campaign_scope="all"`; отбор по id и по имени; словарь остался полным при
      суженном отборе; `on_missing_campaign="warn"` не роняет таску и пишет
      WARNING; `"fail"` роняет; неизвестное значение `on_missing_campaign` —
      `ValueError` до первого запроса; шаблонные поля рендерятся (строка
      `"all"`, строка с запятыми и `[1, 2]` в `campaign_ids`)
- [ ] запустить `.venv/bin/pytest` — весь набор должен пройти

### Task 5: Примеры DAG

**Files:**
- Modify: `examples/admetrica_to_s3_dag.py`
- Modify: `examples/admetrica_to_bigquery_dag.py`
- Modify: `examples/admetrica_to_bq_and_s3_dag.py`
- Modify: `tests/test_example_s3_dag.py`
- Modify: `tests/test_example_bigquery_dag.py`
- Modify: `tests/test_example_dag.py`

- [ ] добавить в `params` каждого DAG `campaign_scope` через `Param` с
      `enum=["active", "all"]` (выпадающий список в форме запуска) и **значением
      по умолчанию `"all"`**, и `campaign_ids` строкой с описанием формата
      «через запятую, пусто — по `campaign_scope`»
- [ ] обосновать это умолчание в `doc_md`: у примеров `schedule=None` и период
      по умолчанию «последние 30 дней», а загрузка в BigQuery идёт
      `write_disposition="WRITE_TRUNCATE"` по дневной партиции
      (`admetrica_to_bigquery_dag.py:400`, закреплено `tests/test_example_dag.py:135`),
      то есть штатный прогон примера — перезалив тридцати уже загруженных дней.
      С `"active"` кампания, ушедшая в архив за месяц, выпала бы из отбора, её
      строк в новом файле не было бы, а `WRITE_TRUNCATE` перетёр бы партицию
      целиком — уже загруженные данные удалились бы. У самого оператора
      умолчание остаётся `"active"`: там за период отвечает расписание
- [ ] передать оба в `YandexAdmetricaStatsOperator` через `{{ params.… }}`
- [ ] дополнить `doc_md` каждого DAG абзацем о том, что даёт ручной запуск с
      этими параметрами, и предупреждением: `campaign_scope="active"` в форме
      уместен на узком свежем окне, а не на перезаливке прошлого периода
- [ ] обновить тесты примеров: параметры объявлены в `params` и доехали до
      аргументов оператора
- [ ] запустить `.venv/bin/pytest` — весь набор должен пройти

### Task 6: Документация

**Files:**
- Modify: `README.md`
- Modify: `README_RU.md`
- Modify: `CONTEXT.md`
- Modify: `CHANGELOG.md`

- [ ] переписать пункт «Все статусы кампаний» / «Every campaign status»
      (`README_RU.md:116`, `README.md:116`) под новое решение с его мотивом:
      сто кампаний за месяц — три тысячи запросов, из которых актуальны
      единицы; архивная перезаливается точечно по id
- [ ] выправить остальные места README, утверждающие полный обход: строка 70
      («собирает статистику по всем кампаниям рекламодателя»), абзац о
      планировании длинной таски на строке 83 (арифметика «70 кампаний ≈ 70
      запросов»), «One request per campaign» на строке 115
- [ ] описать в обоих README форму значения из `params` (строка через запятую,
      `[1, 2]`, пустая строка как «не задано»), почему `campaign_scope` —
      строка, а не флаг, и добавить абзац про перезаливку прошлого периода:
      повторный прогон старого дня при умолчании соберёт только сегодняшних
      активных — для перезаливки ставьте `campaign_scope="all"`
- [ ] сниппет DAG в README показывает и блок `params={...}`, а не только
      `{{ params.campaign_scope }}`: у DAG строгий `template_undefined`, и
      скопированный без объявления параметра шаблон упал бы при рендере
- [ ] переписать в `CONTEXT.md` раздел «Кампании не фильтруются ни по статусу,
      ни по датам» (строка 443): часть про `date_start`/`date_end` остаётся в
      силе, часть про статус заменяется решением об отборе; поправить тот же
      тезис в разделе `get_campaigns()` (строка 118) и упоминание обхода списка
      в разделе `get_stats(...)` (строка 135) и в разделе оператора (строка 278)
- [ ] добавить в `CONTEXT.md` секцию нового шва `CampaignSelection` в «Key
      seams» (строка 60) и три строки в таблицу «Error policies» (строка 379):
      нераспознанный `scope`/`id` → `ValueError` до конструирования хука;
      ненайденные кампании → WARNING либо `ValueError` по
      `on_missing_campaign`; пустой отбор по `scope` → WARNING, никогда падение
- [ ] добавить запись `0.3.0` в `CHANGELOG.md` — смена поведения по умолчанию и
      четыре новых параметра
- [ ] запустить `.venv/bin/pytest` — весь набор должен пройти

### Task 7: Verify acceptance criteria

- [ ] сбор по умолчанию идёт только по активным кампаниям
- [ ] `campaign_scope="all"` возвращает полный обход и статус не читает
- [ ] `campaign_ids` / `campaign_names` перекрывают `campaign_scope` и
      объединяются по ИЛИ
- [ ] значение доезжает из `params` при ручном запуске в каждом примере DAG
- [ ] словарь `dict/campaigns` остаётся полным при любом отборе
- [ ] обе политики `on_missing_campaign` ведут себя как описано, а пустой отбор
      по `campaign_scope` никогда не роняет таску и называет встреченные статусы
- [ ] нераспознанное значение параметра падает до первого HTTP-запроса, и его
      текст не цитирует длинное значение; ненайденная кампания при `"fail"` — до
      первого запроса статистики
- [ ] политика и строка отбора работают при `collect_dictionaries=False`
- [ ] умолчание `campaign_scope` в примерах — `"all"`, у оператора — `"active"`
- [ ] запустить полный набор: `.venv/bin/pytest`

### Task 8: [Final] Update documentation

- [ ] проверить, что README на двух языках синхронны
- [ ] проверить, что новый шов и три политики отказа записаны в `CONTEXT.md` —
      носитель паттернов и швов в этом проекте
- [ ] перенести план в `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- снять фактические значения `status` на живом рекламодателе и записать их в
  `CONTEXT.md`: сейчас словарь статусов нигде не зафиксирован, в коде и тестах
  встречаются только `active` и `archived`. Если в кабинете найдётся
  промежуточный статус у крутящейся кампании — правило `scope="active"`
  придётся расширить, и WARNING о пустом отборе покажет это первым
- прогон на живом рекламодателе: сравнить число HTTP-запросов до и после при
  одинаковом периоде — это и есть измерение экономии лимитов
- ручной запуск с `campaign_ids` архивной кампании: убедиться, что её день
  выгружается, несмотря на `campaign_scope="active"` по умолчанию
- проверить, что форма запуска в Airflow UI показывает `campaign_scope`
  выпадающим списком

**External system updates**:
- тег `0.3.0` для публикации на PyPI (версия берётся из тега через
  setuptools-scm)
