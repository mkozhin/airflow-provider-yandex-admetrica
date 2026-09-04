# airflow-provider-yandex-admetrica

Apache Airflow provider for the [Yandex Metrica for Display Advertising (AdMetrica) report API](https://yandex.ru/dev/admetrica/doc/ru/) — collect display campaign statistics.

[Русская версия](README_RU.md)

---

*Powered by [Claude Code](https://claude.ai/code)*

---

## Installation

```bash
pip install airflow-provider-yandex-admetrica
```

Requires Python 3.10+ and Airflow 2: `apache-airflow>=2.9.1,<3.0`.

**A Metrica Pro plan is required.** The report API is part of it, and an account without the plan is refused access to `/v1/stat/data` however valid its token. Create the OAuth application at [oauth.yandex.ru](https://oauth.yandex.ru/client/new) with the `mediametrika:read` and `mediametrika:write` permissions — Yandex's own instructions ask for both, while this provider only ever reads.

## Connection

**One connection is one advertiser.** The token and the advertiser it works for live together, so the advertiser a task exports is named in one place and everything — the records, the S3 keys, the table names — reads it from there.

Create an Airflow connection of type **HTTP** with `conn_id = yandex_admetrica_default` (or any name you pass to the operator):

| Airflow UI field | Value |
|---|---|
| **Password** | The OAuth token, **without** the `OAuth ` scheme in front of it. The provider spells the scheme itself when it builds `Authorization: OAuth {token}`. A password written with the scheme is accepted too — it is stripped, so the outgoing header never doubles it |
| **Extra** | `{"advertiser_id": 17004}` |

`advertiser_id` may be written as a number or as a string: `extra` is JSON typed by hand, and `17004` and `"17004"` name the same advertiser. It has to be a whole number above zero — a flag, a fraction or a zero names no advertiser, and the task fails with a message saying so.

No other field of the connection is read. **Host** is ignored: the API host is fixed at `https://api.media.metrika.yandex.net`.

`AdmetricaHook.test_connection()` answers whether a connection works: it asks for the advertiser's campaign list, which exercises at once everything an export depends on — the token is accepted, the advertiser is real, and the account may read the API — and returns `(True, "Connected to AdMetrica as advertiser 17004: 42 campaigns are readable.")`, or `(False, <the failure, with the token masked>)`. It is a helper to call from a DAG or a shell, not the Airflow UI's **Test** button: the provider registers no connection type of its own, so a connection of type HTTP is tested by the HTTP provider's own hook.

Diagnostics use a second connection, described under [Loki connection](#loki-connection).

## Quick start

```python
from airflow.decorators import dag
from airflow.models.param import Param
from airflow_provider_yandex_admetrica.operators.stats import YandexAdmetricaStatsOperator


@dag(
    schedule=None,
    start_date=None,
    catchup=False,
    params={
        "date": Param("2026-08-20", type="string"),
        "campaign_scope": Param("active", type="string", enum=["active", "all"]),
    },
)
def admetrica_one_day():
    YandexAdmetricaStatsOperator(
        task_id="collect",
        admetrica_conn_id="yandex_admetrica_default",
        date="{{ params.date }}",
        dimensions=["am:e:placement", "am:e:deviceType"],
        metrics=["am:e:renders", "am:e:clicks", "am:e:ctr"],
        base_dir="/tmp/yandex_admetrica",
        campaign_scope="{{ params.campaign_scope }}",
    )


admetrica_one_day()
```

**A task is a day.** The operator takes one date and collects the campaigns of the advertiser its selection names for it — the active ones by default, see [Choosing the campaigns](#choosing-the-campaigns). A period is expanded by the DAG, which hands each day to a map index of its own: one day failing leaves the others alone, and re-running it is a clear of that map index. The example DAG puts the whole way of a day into that map index — both uploads and the BigQuery load included — so every day loads on its own as well; see [Examples](#examples).

The operator writes JSONL files and returns a `list[dict]`, one entry per file written:

```python
[
    {"kind": "stats", "date": "2026-08-20", "path": "/tmp/…/stats/2026-08-20/123456.json",
     "advertiser_id": 17004, "campaign_id": 123456},
    {"kind": "stats", "date": "2026-08-20", "path": "/tmp/…/stats/2026-08-20/123457.json",
     "advertiser_id": 17004, "campaign_id": 123457},
    {"kind": "dict",  "date": "2026-08-21", "path": "/tmp/…/dict/campaigns/2026-08-21.json",
     "advertiser_id": 17004, "campaign_id": None},
]
```

**A file is a day of one campaign.** The rows of a day are split by the campaign they belong to and written a file each, so the day carries as many `kind="stats"` entries as it has campaigns with rows. The dictionary is one snapshot per run whatever the number of campaigns.

`advertiser_id` travels with every entry because the tasks downstream build the S3 key and the table name from it and have nowhere else to read it: the advertiser is named in the connection, which only the hook opens. `campaign_id` names the campaign whose rows the file holds, and it is what lets an address reach one campaign of one day. The dictionary describes the cabinet whole and belongs to no single campaign, so its entry carries `None` — the key is spelled out on both kinds, so a DAG walking a mixed list and reading `record["campaign_id"]` never meets a `KeyError`.

**Plan for a long task.** A day is one request per selected campaign, plus one page more for every full page a campaign fills, plus the walk over the campaign list, which is fetched whole whatever the selection. An advertiser with 70 campaigns of which 12 are active therefore costs about 12 requests for a day of ordinary volume under the default `campaign_scope="active"`, and about 70 under `campaign_scope="all"`, each spaced by `request_delay` and bounded by a 30 s timeout. A storm of retries adds up to 7 s of backoff to any one of them when the ladder decides the wait — but a server that sends `Retry-After` names the wait itself, and each of the three rungs then costs up to 300 s, so one request can hold the task for 15 minutes. A 400 the API words as `query_error` walks a ladder of its own: 65 s of pauses plus four requests, each bounded by the same 30 s timeout, so such a request holds the task for at most 185 s. A refusal that clears on a repeat is the case to size for: it costs the rung plus one further request, some 15 s where the second attempt brings the page back, and an endpoint refusing a third of the requests of a 300-campaign day therefore adds tens of minutes to it rather than seconds. Walking the ladder to its end costs nearer 105 s at the 10 s the refusal empirically takes, and happens once in a run, because the attempt after it is the one that ends the day. Size `execution_timeout` for the number of days a run exports — the example DAG allows two hours.

### Operator parameters

| Parameter | Default | Meaning |
|---|---|---|
| `admetrica_conn_id` | `"yandex_admetrica_default"` | The connection naming the advertiser and holding the token |
| `date` | — | The day to export, `YYYY-MM-DD`. Required |
| `dimensions` | — | Groupings, e.g. `["am:e:placement", "am:e:deviceType"]`. Required; may be empty, and the report is then a single row per campaign. At most 10 |
| `metrics` | — | Metrics, e.g. `["am:e:renders", "am:e:clicks"]`. Required. At most 20 |
| `filters` | `None` | A filter expression passed to the API as `filters`. Left out of the request when unset |
| `accuracy` | `"full"` | Sampling accuracy. `"full"` asks for the whole selection, which is what keeps the numbers from drifting between runs; pass another value to trade accuracy for speed |
| `include_undefined` | `True` | Keeps the rows whose first grouping is undefined. With it off the API drops them and the sum no longer agrees with `totals`. `None` leaves the parameter out of the request, so whatever the API defaults to decides |
| `limit` | `10000` | Rows per page of statistics. The API allows up to 100 000 |
| `request_delay` | `0.2` | Seconds of quiet between two requests. AdMetrica publishes neither a quota nor a rate, so this is a conservative pace to raise or lower once a real advertiser has been measured |
| `timezone` | `None` | Passed to the API as `timezone`. Left out of the request when unset |
| `lang` | `None` | Passed to the API as `lang`; it decides the language the API words a grouping's `name` in. Left out of the request when unset |
| `extra_params` | `None` | Extra query parameters, and the place a parameterised name's value goes: `{"goal_id": 12345}`, `{"currency": "RUB"}`. Adds names the request does not already carry and overrides none — see [Reserved parameters](#reserved-parameters) |
| `base_dir` | `"/tmp/yandex_admetrica"` | Root of the local layout |
| `collect_dictionaries` | `True` | Also export the campaign dictionary |
| `campaign_scope` | `"active"` | Which campaigns the day is walked over: `"active"` keeps the ones the cabinet calls active, `"all"` walks the whole list and does not read the status at all. A string rather than a flag, because a rendered `{{ params.x }}` is the string `"False"` and every non-empty string is true |
| `campaign_ids` | `None` | Campaigns asked for by id, as a list or as one string: `[123, 534]`, `"123, 534"`. Empty means none were named. Overrides `campaign_scope` outright, so an archived campaign is re-collected by naming it |
| `campaign_names` | `None` | Campaigns asked for by name, as a list or as one comma-separated string. Joined with `campaign_ids` by OR, and overriding `campaign_scope` the same way. Names are compared whole, case included, surrounding space dropped |
| `on_missing_campaign` | `"warn"` | What a named campaign the cabinet does not list is answered with: `"warn"` names it in the log and collects the rest, `"fail"` refuses the task with `ValueError` |
| `loki_conn_id` | `None` | Connection for [request diagnostics](#request-diagnostics-in-loki-loki_conn_id). Without it nothing is constructed and nothing is sent |

`date`, `admetrica_conn_id`, `loki_conn_id`, `base_dir`, `campaign_scope`, `campaign_ids`, `campaign_names` and `on_missing_campaign` are template fields. They render as text, including in a DAG declared with `render_template_as_native_obj=True`: that setting finishes a render by reading the rendered characters as a Python literal, which turns `1_2` typed into `campaign_ids` into the number twelve and `None` typed into `campaign_names` into no name at all, and the operator renders its own fields as the characters that were typed instead.

The request is checked before it goes out, so a report configured wrongly costs nothing. `ValueError` answers an empty `metrics`, more than 20 metrics, more than 10 dimensions and a `limit` outside 1…100 000, naming what is wrong. `date` is held to `YYYY-MM-DD` by the hook itself, so a DAG calling `get_stats` directly is answered the same way: it is the day the API is asked for, the day stamped onto every record and the day that names the directory the files land in, and it arrives rendered from a template.

#### Choosing the campaigns

`campaign_scope`, `campaign_ids` and `campaign_names` decide which campaigns of the cabinet a day is walked over. The two lists are joined by OR and override `campaign_scope` outright: naming a campaign is asking for that campaign, an archived one included.

All three are template fields, so the value reaches them from `params` of a manual run. A collection is taken as it is — a list, a tuple, a set, a `range` of ids — and a string is read like this:

- empty, or blank once trimmed — nothing was named, and `campaign_scope` decides. This is what a `{{ params.campaign_ids }}` left blank in the run form renders to;
- opening `[`, `{` or `(` — a container written out: `[123, 534]`, `['Summer campaign']`, `{123, 534}` and `('Summer campaign',)` alike, which is the shape Jinja renders a `Param(type="array")` and a tuple in, as well as the JSON one with double quotes. A mapping is refused: it names no campaign;
- anything else — comma-separated, surrounding space dropped: `"123, 534"`, `"Summer campaign, Winter campaign"`.

A bracket opens a campaign name as often as it opens a list — a cabinet tags a campaign with its locale, its market and its year that way — so in `campaign_names` the rule is written on the quote: text opening a bracket straight onto a quote is a container written out and has to parse as one, and every other bracketed line is a name. `"[MSK] Summer [2026]"`, `"[Test]"`, `"[Promo] Autumn]"` and the comma-separated line `"[EN] Summer, [RU] Winter [2026]"` are names, brackets and all. `"['Summer', 'Winter'"` is a `ValueError` rather than the two junk names `"['Summer'"` and `"'Winter'"`, which would override `campaign_scope` and then match nothing. `campaign_ids` is digits and has no bracketed spelling of its own, so a bracket there opens a container however it is written, and text that does not read as one is a `ValueError` — including `"[0999, 1000]"`, which Python reads as no literal at all because of the leading zero. A name holding a comma is written in the quoted form — `['Summer, and autumn']` — since the comma-separated form has no way to say it. Each refusal spells out the form that works for the parameter it refuses: quoted names for `campaign_names`, digits for `campaign_ids`.

An id is ASCII digits: Python reads `1_2` and the Arabic-Indic `١٢` as twelve, and a mistyped id that becomes a different existing campaign would be collected in silence, so both are refused instead. The digits have to be the number's own spelling as well — `"+999"` and `"000999"` are refused although `int` reads either as `999`. An id is repeated in a message as the number written out, so admitting that spelling alone keeps what a message repeats identical to what was typed, and a value typed into `campaign_ids` reaches the masking gate in the shape the gate can recognise it by.

An id has to be a whole number above zero, and a name is compared whole, case included. An unreadable value is `ValueError` before the first request, and the message describes it by type and length rather than quoting it: the field holds whatever the run form was filled in with, and the refusal is read from a task log and a traceback. One shape is quoted back — ASCII digits, optionally signed, at most eight characters written out with the sign counted among them — because a length says least about exactly that shape: `"0"` and `"-1"` are refused for how much they are, not for how they are written, and a message calling them a two-character string names nothing to go and fix. What the bound buys is how much of a value can travel: eight characters are short of the account numbers, cards and long numeric tokens a run form can be filled in with. A short numeric secret does fit — a PIN, a one-time code, a numeric password — and is quoted like the id it resembles, which is the price this quoting is worth naming. Anything else, `"12a"` and `"hunter2"` alike, is named by type and length.

`campaign_scope` is a string rather than a flag because a rendered `{{ params.x }}` is a string, and `"False"` is a non-empty string and therefore true: a boolean would switch itself on silently, visible only in the count of requests. A misspelt scope fails by name instead, and so does a blank one: unlike the two lists, `campaign_scope` has no empty reading — a `ValueError` names the two words it accepts. Declare its `Param` with `enum=["active", "all"]` and the run form offers them.

Calling the hook directly selects the same way: `get_stats` takes a `selection` argument holding a `CampaignSelection` from `airflow_provider_yandex_admetrica.campaign_selection`, and it defaults to the active campaigns as the operator does. `hook.get_stats(date, dimensions, metrics, selection=CampaignSelection(scope="all"))` is the walk over the whole list, and `CampaignSelection(ids=frozenset({123}))` the walk over one campaign.

**Re-collecting a past period.** The default keeps the campaigns that are active now, while the day being collected is a past one, so a re-run of an old day under the default brings back only today's active campaigns. Pass `campaign_scope="all"` to walk that day as it was, or name the campaigns in `campaign_ids`.

**The default on a schedule.** The status is read at the moment of the run and the day collected is an earlier one, so a campaign that ended with day D and was archived before the run of D+1 does not pass the selection: its last and fullest day is not collected. The log does not single this out — under `"active"` skipped campaigns are the ordinary case, not a signal. The window of the loss is the lag of the schedule, and the way back is a manual run naming that campaign in `campaign_ids`.

A named campaign the cabinet does not list is answered by `on_missing_campaign`: `"warn"` names it in the log and collects the rest, `"fail"` refuses the task with `ValueError` before a single day of statistics is asked for. A selection by `campaign_scope` that matches nothing is never a failure — a cabinet with no active campaign is legitimate — but it is a WARNING naming how many campaigns the list holds and which statuses were met in it. Ids, names and statuses reach the log and the `ValueError` through the hook's masking gate, so a value typed into `campaign_ids` or `campaign_names` that turns out to be the token of the connection is quoted as `<token>`. The line counts the ids and the names it did not find as well as naming them, so it stays worth reading when the gate hides a value. The gate knows the one credential this provider holds and no other: a PIN or a one-time code typed into `campaign_ids` has the shape of an id and is quoted as one.

**The word for a running campaign is not documented.** The API describes no vocabulary for `status`, so `campaign_scope="active"` compares that field with the string `"active"`, case and surrounding space aside — and the whole of the default rests on that one comparison. A cabinet wording a running campaign otherwise matches nothing under it and exports an empty day while the task stays green, since an empty scope is a warning by design. That WARNING is where it shows: it names the statuses it did meet and gives this reading before the other one, an advertiser whose campaigns are all over. Read that line on an advertiser's first scheduled run, compare the statuses it names with what the interface shows, and collect the day with `campaign_scope="all"` until they agree.

The selection narrows the statistics and nothing else: `dict/campaigns` stays the whole snapshot of the cabinet whatever is selected, and it is what later shows which campaign was left out and when it left the cabinet.

#### Reserved parameters

`extra_params` may not carry `ids`, `date1`, `date2`, `metrics`, `dimensions`, `preset`, `limit`, `offset`, `sort`, `accuracy`, `include_undefined`, `filters`, `timezone` and `lang`; passing one raises `ValueError` before anything is requested. Each of them is either the question being asked or an answer to how it is asked, and a silent override would be invisible in the data: another `date1` would fetch another day while the records still carry the operator's date, and `accuracy` or `include_undefined` would drop the defaults that stand against drifting and truncated numbers — with the completeness check still passing, because `total_rows` agrees with the truncated selection. `preset` lets the API define the report's own metrics and dimensions, while values are paired by position against the names that were requested, so the numbers would land under the wrong keys. `filters`, `timezone`, `lang`, `accuracy` and `include_undefined` have parameters of their own on the operator, so nothing needs this route to reach them.

### How a day is collected

- **One request per selected campaign.** The API offers no grouping by campaign and sums the campaigns named in `ids` together, so a request per campaign is the only way the split survives. The campaign list is fetched whole once per task instance — the hook lives for one `execute`, so a run spread over one map index per day walks the list once per day — and serves the selection, the statistics and the dictionary of that day. A campaign the answer names no usable `campaign_id` for fails the export: `ids` is required and names one campaign, so such a campaign is one whose rows nothing can ask for, and a day written without them would be short without saying so.
- **The status filter is the provider's own.** No `status` parameter goes out to the API: the list comes back as the whole cabinet, which is what the dictionary is a snapshot of and what a named campaign is looked for in. The narrowing happens over that list, in the provider, and `campaign_scope` is what sets it: a hundred campaigns over a month are three thousand requests for the numbers of the handful still running, so a day is walked over the active campaigns by default, and an archived one is re-collected by naming its id. Campaigns are not filtered by `date_start`/`date_end` at all — those are the campaign's declared dates, not a promise about where impressions are.
- **`date1 = date2 = <day>`.** The report carries no date of its own, so the day is asked for one at a time and stamped onto every record by the provider. A one-day selection is also small enough for sampling to be unlikely.
- **`sort` names every requested grouping.** The API documents nothing about the order rows come back in, so naming the groupings is this provider's own insurance rather than a consequence of anything the API promises: the report is aggregated by them, so their combination is a total order over the rows, where sorting by a metric leaves a long tail of placements sharing `renders=1` for the API to arrange as it likes. The insurance is backed rather than taken on faith — a grouping combination seen twice inside one campaign fails the day, and the collected rows are counted against `total_rows` — so an order that turned out unstable is a day that stops rather than a file quietly written short. The gap is a rounded total: under `total_rows_rounded` the count mismatch softens to a warning, watched by the repeat detector and by a warning of its own whenever the declared total changes from one page to the next.
- **Pagination is checked, not trusted.** Rows are identified by what their groupings matched, and a row seen twice while walking one campaign fails the day: pages that overlap are pages that also skip, and counting rows cannot tell the two apart. The same combination in two different campaigns is ordinary — one placement runs in several — so the set of seen rows is reset for each campaign.
- **The walk stops on a short page, never on a total.** A campaign-day is read until a page comes back shorter than the one asked for, which is the report's own word that its rows have run out. A declared total is a number about the report rather than the report itself, and one that is low by a page would end a day with its tail left behind: 10 437 rows declared as 10 000 against `limit=10000` fill the first page exactly and match the total. The price is one further request whenever a campaign-day ends exactly on a page boundary. The campaign list is walked by the same rule.
- **Completeness is checked against `total_rows`.** A count that does not match an exact total fails the day with an `AirflowException`: rows lost between pages are lost silently, and what is not in the file is discovered weeks later. When the answer sets `total_rows_rounded`, the same difference is only a warning — the number is an approximation by the API's own word, and the collected rows are the ones written.
- **A completeness signal that cannot be read fails the day.** Every page has to carry a whole-number `total_rows`, a `total_rows_rounded` that is a boolean whenever the field is there at all (a `null` is a flag with no reading, while a page carrying no such field declares an exact total), and — while the totals are exact — the same total as the pages before it. A missing total leaves the day nothing to be checked against, a `"false"` read as truthy would disown the exact check altogether, and two different exact totals leave two answers to how long the report is. An export that cannot read its completeness signal cannot claim completeness, so it fails instead. The campaign list is held to the same rule for its own `total`.
- **Caveats reach the task log.** `sampled` is a WARNING carrying `sample_share`, `sample_size` and `sample_space`; `contains_sensitive_data` is a WARNING saying the day is short by whatever the API withheld; `data_lag` is an INFO.

## Output records

### Statistics

The service fields are flat and typed; the variable half is two nested objects:

```json
{"date": "2026-08-20", "advertiser_id": 17004, "campaign_id": 123456,
 "dimensions": {"placement": {"name": "Главная страница", "id": 55},
                "device_type": {"name": "mobile"}},
 "metrics": {"renders": 12345, "clicks": 67, "ctr": 0.54}}
```

Nesting is what makes a new field in the answer a change to a JSON value rather than to a table schema, and what lets two rows of the same day carry different fields without either being padded out to match the other. The API describes a grouping's value as an object with arbitrary string keys of which only `name` is guaranteed, so the set of fields inside it is the answer's to choose.

| Key | Built from |
|---|---|
| `date` | Stamped by the provider: the report has no date in it, so the day the request was made for is the day the row belongs to |
| `advertiser_id` | The connection's `extra` |
| `campaign_id` | The campaign the request was scoped to |
| `dimensions` | One key per requested grouping, in the order they were requested. The value is the object the API returned, with **exactly** the fields it arrived with — nothing is dropped, nothing is added, and the field names inside it are not touched |
| `metrics` | One key per requested metric, in the order they were requested, holding the number the answer returned |

**One key per requested name — and two names can want one key.** The two spellings of a parameterised name normalise to the same key, so asking for `am:e:goal12345Reaches` and `am:e:goal<goal_id>Reaches` with `extra_params={"goal_id": 12345}` in one request writes `goal12345_reaches` once: the record keeps the later value, a WARNING names both by their position in the request, and one of the two requested metrics is not in the file. Ask for either spelling, never both. The names themselves never reach the log: a name is the caller's own text and can hold anything, credentials included.

**Key naming.** The shared `am:e:` prefix comes off, a parameter spelled into the name is replaced by its value, and the camelCase that is left becomes snake_case. One rule serves groupings and metrics alike, because the API names both the same way:

| Requested name | Record key |
|---|---|
| `am:e:placement` | `placement` |
| `am:e:deviceType` | `device_type` |
| `am:e:operatingSystemRoot` | `operating_system_root` |
| `am:e:interest2d1` | `interest2d1` |
| `am:e:renders` | `renders` |
| `am:e:videoCompletePercent` | `video_complete_percent` |
| `am:e:goal12345Reaches` | `goal12345_reaches` |
| `am:e:goal<goal_id>Reaches` with `extra_params={"goal_id": 12345}` | `goal12345_reaches` |

A parameterised name reaches the API in two spellings — the value written into the name, or a placeholder in the name and the value in a field of its own — and substituting the value makes the record key the same for both, so a report rewritten from one spelling to the other keeps writing the column it already did. A placeholder no parameter answers keeps the parameter's name in its place and logs a WARNING naming the name by its length: dropping the placeholder instead would merge every goal of the account into one column, and the merge would only be visible as numbers that are too large.

This key is a public contract: it is what an analyst writes in `JSON_VALUE(dimensions, '$.device_type')`.

Key order is fixed — the service fields, then the groupings in the order they were requested, then the metrics in theirs — so files written from the same request are byte-comparable and a re-export is reviewable as a diff.

A row carrying another number of values than were asked for fails the day. The answer names none of its values, so position is all that ties a number to its metric, and a row of another length cannot be read at all: some keys would be left empty or some numbers dropped, while the row counts towards `total_rows` exactly like a whole one — so the completeness check would pass and the file would look whole. An empty value is written through as it arrived. A metric that divides or prices — `am:e:ctr`, `am:e:cpm`, `am:e:cpc`, `am:e:cpa<goal_id>`, the `video*Percent` family, the `am:e:ecommerce<currency>Revenue` family — is empty wherever its denominator or its cost is, so a report asking for those alone answers in rows of empty numbers; an empty **grouping** value is what `include_undefined=True` asks for. Each reaches the file as JSON `null`, which BigQuery reads as NULL.

### Campaign dictionary

The statistics carry only `campaign_id`; the campaign's name lives in a dictionary of its own, because it comes from a different endpoint. The dictionary is flat, its fields worded exactly as the management API words them:

```json
{"snapshot_date": "2026-08-21", "campaign_id": 123456, "name": "Летняя кампания",
 "status": "active", "date_start": "2026-06-01", "date_end": "2026-08-31",
 "advertiser_id": 17004, "advertiser_name": "ООО Ромашка"}
```

`snapshot_date` is the day the **export ran**, not the day it reports on: the management API answers with the state the campaigns are in right now, and there is no way to ask it about a past day. It is the operator that stamps it, names the file with the same date and reports it as the `date` of the result, so the column of a row, the key it is loaded from and the partition it is loaded into always name one day. The date is taken from the DAG run's `start_date` rather than from the clock, so a run whose days are spread over map indices produces one snapshot even when it crosses midnight.

Every day of a run exports the dictionary, so the same file appears in the result of every map index. There is no reason to load it once per day of the period: take the `kind="dict"` entry of any day that reported one and load the snapshot once per run, as the example DAG does.

The path is the same for every map index too, because the snapshot date is, so map indices running at once write one file. The write is atomic — the lines go to a temporary file that is moved onto the path in one step — so what an upload finds is always one whole snapshot rather than the truncated middle of two. Serialising the mapped task with `max_active_tis_per_dag=1`, as the example DAG does, or leaving `collect_dictionaries=True` on a single index, avoids the repeated work as well.

The measures the answer carries beside these fields — spend, impressions, days left, conversions — describe the campaign at the moment of the request rather than the campaign itself, and a measure belongs to the statistics table. They are not written.

### BigQuery schema

Both schemas are declared explicitly. `autodetect` would read the nested fields off the beginning of a file and lose the ones that appear further down.

Statistics, partitioned by `date`:

| Column | Type |
|---|---|
| `date` | DATE |
| `advertiser_id` | INTEGER |
| `campaign_id` | INTEGER |
| `dimensions` | JSON |
| `metrics` | JSON |

Campaign dictionary, partitioned by `snapshot_date`: `snapshot_date` (DATE), `campaign_id` (INTEGER), `name`, `status`, `date_start`, `date_end` (STRING), `advertiser_id` (INTEGER), `advertiser_name` (STRING).

Statistics go to a table per campaign, `stats_{advertiser_id}_{campaign_id}`, and the dictionary to a single `campaigns`. The grain that has to be overwritten is two-dimensional — a day and a campaign — while BigQuery partitions by one field, so the second dimension goes into the table name. Keeping it there is what makes a load idempotent on its own: `WRITE_TRUNCATE` into a partition decorator rewrites one day of one campaign in a single statement, where a `DELETE` followed by an `INSERT` in one shared table would open a window in which the old rows are gone and the new ones have not arrived yet.

Addressing the partition with a `table$YYYYMMDD` decorator — `date` for statistics, `snapshot_date` for the dictionary — lets `WRITE_TRUNCATE` overwrite one day of one campaign and leave the rest of the table alone.

A decorator addresses a partition of a table that already exists, so the DAG creates both tables itself — empty, with the same schema and the same partitioning — before it loads into them: the day's load creates a campaign's table, and the `dictionary.create_table` task creates the dictionary's. The dataset has to exist beforehand and the connection needs the right to create tables in it.

The mart is read through wildcard tables: `stats_*` is every advertiser, `stats_{advertiser_id}_*` is one, and `_TABLE_SUFFIX` tells the tables apart inside a query. The price of that is worth knowing before choosing the readers: a wildcard query has no result cache, so a repeat of the same query is paid for again, it is not served by BI Engine, and the schemas of all matched tables must agree down to their types and partitioning. The schema of five columns is stable by construction — a new grouping or metric lives inside the `dimensions` and `metrics` JSON and changes nothing — so the readers this suits are the next layer of a pipeline rather than dashboards.

A query references at most 1,000 tables after the wildcard is expanded, and a table per campaign makes that ceiling reachable: five advertisers of two hundred campaigns is already it. `stats_*` therefore fits a dataset holding few campaigns, while the address that keeps working is `stats_{advertiser_id}_*`, united across advertisers where the whole cabinet is needed at once.

**A view whose name falls under `stats_*` breaks such a query outright**, even one carrying a condition on `_TABLE_SUFFIX`. Keep the views of the next layer in another dataset, or name them something other than `stats_`. Table prefixes themselves do not collide: `stats_1234_5` does not fall under `stats_123_*`, because what follows `123` is a `4` and not an `_`.

## File layout

**The grain of an overwrite is the grain of an export: a day of one campaign.** Every address a file reaches — the local path, the S3 key, the object in GCS, the table in BigQuery — is built from the campaign it holds, so a re-export rewrites exactly what it collected and cannot reach the data of a campaign it never asked for. Collecting part of a cabinet is safe by construction rather than by care.

The grain has a price on the write side. A day costs one local file, one S3 object, one staging object in GCS and one BigQuery load job **per campaign that has rows**, and the dataset gains a table per campaign per advertiser: an advertiser of 70 campaigns is 70 uploads and 70 load jobs a day, some 2100 of each over a 30-day period. The example DAGs walk them sequentially inside one task per day and per direction, waiting for every load job, so size `execution_timeout` for a whole day rather than one job, and narrow the period when a day does not fit. The task owns the job it waits on: it names the job before submitting it and, killed or timed out, cancels it by that name rather than leaving it to outlive the task and race the next attempt's `WRITE_TRUNCATE` for the same partition. Three cases still leave a job without an owner — a worker lost outright runs no cancellation at all; BigQuery can refuse the cancellation, which the task logs; and a cancellation it accepts is not instant, the hook waiting about a minute for the job to end and returning either way — and such a job is cancelled by hand. A day is therefore not atomic in the mart either: a failure on campaign N leaves the campaigns before it refreshed and the rest holding their previous export, and the retry redoes the whole day.

Locally, the run id isolates two runs exporting the same day from each other:

```
{base_dir}/{dag_segment}/{run_segment}/{advertiser_id}/stats/{date}/{campaign_id}.json
{base_dir}/{dag_segment}/{run_segment}/{advertiser_id}/dict/campaigns/{snapshot_date}.json
```

The day of statistics names a directory and the campaign names the file inside it. The dictionary gets no such level: it is a snapshot of the whole cabinet, addressed by the date of the snapshot alone.

`dag_segment` and `run_segment` name the DAG and the run: the identifier with every character outside `[\w-]` — letters, digits, underscore and hyphen — replaced by an underscore, so a run id carrying a timestamp with colons and a plus sign still names a directory on every filesystem, followed by a hyphen and the first eight characters of its SHA-1. The digest is what keeps two identifiers apart after the substitution has made them look alike, as `manual:a` and `manual/a` do. The readable part is cut to what 255 bytes leave after the digest, so the segment names a directory on every filesystem: Airflow accepts a `dag_id` and a `run_id` of up to 250 characters, and a character outside ASCII takes more than one byte. The cut costs nothing, since the digest is taken from the whole identifier.

It takes the DAG as well as the run because Airflow holds a run id unique inside its DAG and nothing wider. One connection names one advertiser, so several advertisers are served by several DAGs, and two of them on the same schedule are handed the same `scheduled__<logical_date>`: on a shared `base_dir` a run directory named by the run alone would be one directory they both collect into, and the example DAG's `cleanup` deletes the whole of it.

The day is sanitised by the substitution alone, so nothing rendered into `date` can address a file outside `base_dir`. The campaign takes no such treatment and needs none: it reaches the path from the campaign list, where it has already been read as a positive whole number, so the only source of that segment is trusted by construction.

In S3 the run id is absent and a day of one campaign is overwritten — no history is kept:

```
{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20/_date=20260820/_campaign_id=123456/2026-08-20.json
{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json
```

`_campaign_id` is the last level of the hierarchy, because a date is what a reader selects a range by and a campaign is what narrows it inside that range. The key is written in hive style throughout, so the campaign continues a convention the other levels already follow. The dictionary gets no such partition.

A DAG loading into BigQuery stages the files in GCS on the way, under the DAG and the run — the object lives for the length of a run, and a shared bucket must not hand two runs one name:

```
{GCS_PREFIX}/{dag_segment}/{run_segment}/{advertiser_id}/stats/{date}/{campaign_id}.json
{GCS_PREFIX}/{dag_segment}/{run_segment}/{advertiser_id}/dict/campaigns/{snapshot_date}.json
```

The campaign names the staging object as well, so two campaigns of one day do not overwrite each other on the way to BigQuery.

The files are JSONL — one JSON object per line, UTF-8, written with `ensure_ascii=False` so placement and campaign names stay readable in the file itself. JSONL is the only format offered: the set of columns does not follow from the request and can differ between two rows of the same day.

### A campaign with no rows

A campaign the API returns no rows for on a day **writes no file** and adds nothing to the operator's result; a day no campaign has rows for adds no `kind="stats"` entry at all, and the example DAGs skip the uploads of such a day. The task stays green — a campaign with no impressions on a day is the ordinary state of most of an advertiser's campaigns.

The consequence is worth spelling out: **whatever S3 and BigQuery already hold for that campaign and that day stays exactly as it was.** Those are the last figures known for it, so leaving them is the right reading of silence rather than a leftover. Re-exporting a day a campaign has since gone quiet on does not clear it, and clearing it is a manual operation on the bucket and on the BigQuery partition.

The same holds for the dictionary: an advertiser whose campaign list comes back empty writes no dictionary file. Otherwise the dictionary is exported even on a day with no statistics — the campaign list does not depend on impressions.

### Changing the set of dimensions and metrics

The two parameters decide the shape of a record, so changing them changes the files written **from that point on**. Files exported earlier stay as they are, under the keys they were written with.

Nothing reconciles the two automatically, and nothing needs to: `dimensions` and `metrics` are JSON columns, so a record with a new key loads into the existing table without a schema change. A query naming a key that older files do not carry answers NULL for them rather than failing.

Bringing history to the new shape is a manual operation — re-export the period you need, or delete it and export it again.

## Failures, retries and the task log

| Situation | Behaviour |
|---|---|
| 429, and any 5xx | Retried along a 1 / 2 / 4 s backoff, four attempts to a request. A `Retry-After` header outranks the ladder in both of its spellings — seconds and an HTTP date — capped at 300 s; a longer wait would hold a task slot for the whole of it, and failing the day costs less than that |
| A request the network did not carry | Retried on the same ladder |
| 401 or 403 | Raises at once. The token is long-lived and nothing here refreshes it, so the attempt after it would be refused the same way. Either status says the same thing about a credential the API will not accept |
| 400 whose body names `error_type: query_error` | Retried along a 5 / 15 / 45 s backoff of its own, four attempts to a request. The request is sound and the moment was not, and the ladder spans a minute because the condition behind such a refusal drifts on that scale. `Retry-After` is left unread here: the header names a window a server is keeping, while this refusal is the endpoint running long |
| 400 with any other `error_type`, and any other 4xx | Raises at once, with the server's own words for it read out of the body. The request itself is what is being refused, and a repeat brings back the same answer |
| An HTTP 200 no rows could be read out of | Raises, naming what the body held instead. **A zero is never green when it came from a failure** — only a well-formed answer, the empty one included, is handed on |
| A row carrying another number of grouping or metric values than were asked for | `AirflowException` naming the campaign, the day and both counts: position is all that ties a value to its name, and such a row counts towards `total_rows` like a whole one |
| Rows collected disagree with an exact `total_rows` | `AirflowException` |
| The same disagreement with `total_rows_rounded` set | WARNING; the collected rows are the ones written. A rounded total that changes between pages is a WARNING of its own, once per walk: the same report rounds to the same number at every offset, so a changed one is a changed report |
| A page declaring no whole-number `total_rows`, or a `total_rows_rounded` that is present and not a boolean | `AirflowException`: a completeness signal that cannot be read leaves the day nothing to be checked against, and a short one would pass unnoticed |
| Two pages of one campaign-day declaring different exact totals | `AirflowException`: the number the rows are checked against changed under the walk |
| Campaigns collected disagree with the declared `total` | `AirflowException`: a campaign missing from the list takes all of its statistics with it |
| A page of the campaign list declaring no whole-number `total`, or two pages declaring different totals | `AirflowException`, for the reason the same answers fail a campaign-day |
| A campaign whose `campaign_id` is not a positive whole number | `AirflowException` naming the campaign. Statistics are asked for one campaign at a time and named by that id, so such a campaign is one whose rows no request can ask for — and a day written without them would look complete |
| The campaign list repeating a `campaign_id` | `AirflowException` naming the campaign. A list that repeats a campaign is a list the offset is not moving through, so the pages after it are pages of a walk that has stopped advancing |
| A walk that keeps answering with full pages | `AirflowException` after the walk's page budget. The budget is ten million rows for one campaign-day and a million campaigns for one advertiser's list, divided by the rows a page of that walk asks for: the statistics walk asks for `limit`, so at `limit=10000` it is allowed 1000 pages and at `limit=100` it is allowed 100 000, while the campaign list asks for a fixed 1000 and is allowed 1000 pages. A page small enough that the ceiling would take more than 100 000 requests to reach runs out of requests instead |
| `sampled` | WARNING carrying `sample_share`, `sample_size` and `sample_space` |
| `contains_sensitive_data` | WARNING: part of the rows was withheld by the API |
| `data_lag` | INFO |

A single request is given 30 s before it counts as one the network did not carry.

**Every unsuccessful attempt leaves one line in the task log**, the last one included, so a minute of waiting reads as a chronicle rather than as a silence. A page that came back on an attempt past the first adds one INFO line naming that attempt — `recovered on attempt 3/4` — so a run of warnings carries its own ending; a page that arrived on the first attempt says nothing at all, so a healthy export writes no lines about its attempts. The one exception is an attempt the task itself was stopped in — a `BaseException` that is not an `Exception`, such as an execution timeout or a SIGTERM: it writes no line and pushes no event, deliberately, because the reason for it is already in the Airflow task log and a push would hold the stop for its own length.

```
AdMetrica stat campaign_id=123456 date=2026-08-20 offset=1: attempt 1/4 failed — HTTP 429. Retrying in 1 s
AdMetrica stat campaign_id=123456 date=2026-08-20 offset=1: attempt 2/4 failed — HTTP 502. Retrying in 2 s
AdMetrica stat campaign_id=123456 date=2026-08-20 offset=1: attempt 3/4 failed — no response, ConnectionError. Retrying in 4 s
AdMetrica stat campaign_id=123456 date=2026-08-20 offset=1: attempt 4/4 failed — HTTP 500, code 42: internal error
AdMetrica campaigns offset=0: failed, not retryable — HTTP 200, no readable rows (payload_kind=rows_absent)
```

`Retrying in N s` appears only where a pause really follows, which is how the final attempt reads as final. A refusal no ladder applies to reads `failed, not retryable` in place of the fraction, because a fraction there would promise attempts the request will never be given. The line carries parsed fields only — the HTTP status, the refusal the body named, the label saying what stood in place of rows, the position a JSON document broke at, the type of a network failure — so one attempt stays one line whatever the server wrote. The raw answer belongs to the other channel: it travels in the diagnostic event, if one is configured, never in the task log.

### What the API does not document

- **Quotas and request rate.** The documentation names neither, which is why `request_delay` defaults to a conservative 0.2 s and is a parameter rather than a constant.
- **The order rows come back in.** Nothing in the specification says that a page-to-page order exists at all, which is why `sort` names every grouping and why the walk audits itself: a row seen twice inside one campaign fails the day, and the collected rows are counted against `total_rows`.
- **The shape of an error.** The specification describes the answer to a successful request and nothing else. The provider looks for a refusal in three shapes — `{"error": {…}}`, `{"errors": [{…}]}` and a top-level `{"code": …, "message": …}` — and reports the `code`, the `error_type` and the `message` it finds. The plural spelling is described at its first element, since an event carries one refusal; `error_code` therefore stays empty for a body that puts the code beside the list rather than inside it, and `error_type` is the part that says what kind of refusal it is.
- **What a `query_error` really objects to.** The 400 worded `Query is too complicated. Please reduce the date interval or sampling.` is transient rather than a verdict on the request: one campaign answered the same day with the same parameters in 641 ms and ran into the refusal eleven minutes later, and every variant of that request — fewer groupings, fewer metrics, another `accuracy` — was answered too. What decides the outcome is how long the endpoint takes at that moment, and the refusal arrives at around ten seconds. That is why this one refusal is repeated and its wording is not taken at face value.
- **Whether `Retry-After` is ever sent.** On the 1 / 2 / 4 s ladder it is honoured wherever it arrives and the rung stands in when it does not; the `query_error` ladder waits out its own rungs and reads no header.
- **The values `accuracy` accepts.** `"full"` is the default here because the alternative is numbers that drift between runs; the documentation lists no vocabulary to check it against.
- **The fields inside a grouping's value.** Only `name` is guaranteed, and the single example in the specification is empty. That is exactly why the value travels as the object it arrived as.

## Request diagnostics in Loki (`loki_conn_id`)

Optional, off by default. With `loki_conn_id` set, the operator emits one diagnostic event per HTTP attempt against **both** endpoints — the campaign list and the statistics — to a [Loki](https://grafana.com/docs/loki/latest/) instance. An event describes how the attempt went (severity, outcome, timing, HTTP status, the shape of the raw answer, what the report said about its own numbers), the request as it went out, and — for every attempt whose answer was not intelligible — the raw response body, so a past run can be explained afterwards in Grafana. **Read [Content policy](#content-policy) before turning this on: on an anomalous answer the response body travels as it came, and the body is treated as arbitrary sensitive data.**

Turning diagnostics on does not change the export: the same files, the same operator return value, the same exceptions with the same types and messages. A Loki outage cannot fail the task — the first push failure logs one WARNING and disables diagnostics for the rest of that task instance. The one cost is wall-clock: the push is synchronous, with a 2 s connect timeout and a 3 s read timeout, so an unresponsive Loki holds an attempt for about 5 s — once, before diagnostics switch themselves off. The read half bounds the quiet between received bytes rather than the whole exchange, so a Loki answering in a slow dribble can hold an attempt longer than that; only the response status is used, and the body is never downloaded. What diagnostics never absorb is the task being stopped: an `execution_timeout` firing or a SIGTERM arriving interrupts the task there and then, and the attempt it cut short goes unreported rather than holding the stop for the length of a push.

A run that fails before the first request — a connection Airflow cannot find, an `extra` naming no advertiser, an empty password — sends nothing, so the absence of events for a `dag_run` is not evidence about it: it reads the same as diagnostics being off or Loki being unreachable.

```python
YandexAdmetricaStatsOperator(
    task_id="collect",
    admetrica_conn_id="yandex_admetrica_default",
    loki_conn_id="loki_default",   # optional; without it nothing is sent
    date="{{ params.date }}",
    dimensions=["am:e:placement"],
    metrics=["am:e:renders"],
)
```

Outside an operator, the same client can be handed to the hook directly:

```python
from airflow_provider_yandex_admetrica.hooks.loki import LokiClient
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import AdmetricaHook

hook = AdmetricaHook(
    admetrica_conn_id="yandex_admetrica_default",
    loki=LokiClient(conn_id="loki_default", context={"dag_id": "adhoc"}),
)
rows = hook.get_stats("2026-08-20", ["am:e:placement"], ["am:e:renders"])
```

### Loki connection

Create an Airflow connection with `conn_type = http`:

| Airflow UI field | Meaning |
|---|---|
| **Host** | Loki base URL, either with an explicit scheme (`https://loki.example.ru`, port allowed: `https://loki.example.ru:3100`) or a bare host (`loki.example.ru`) paired with **Schema**. An IPv6 address goes in brackets: `[::1]`, `http://[::1]:3100` |
| **Schema** | `https` or `http`. Required when **Host** carries no scheme |
| **Port** | Optional (e.g. `3100`), used only when **Host** carries neither a scheme nor a port of its own |
| **Login** / **Password** | Optional Basic Auth. Set both or neither |

The push path `/loki/api/v1/push` is appended automatically; a trailing slash on **Host** is fine, and a **Host** that already ends in the push path is taken as is. `Host = https://loki.example.ru` alone and `Host = loki.example.ru` plus `Schema = https` are equivalent.

Credentials belong in **Login**/**Password**, never in the URL: a **Host** carrying userinfo (`https://user:token@loki.example.ru`, the form Grafana Cloud publishes) is rejected with a WARNING, as are a query string and a fragment.

The scheme is never guessed. A bare **Host** with an empty **Schema** is a broken connection: diagnostics are disabled with a WARNING naming the fix, rather than silently defaulting to `http`. The same happens for an empty **Host** and for any scheme other than http/https.

Basic Auth requires HTTPS: with **Login** set and a non-HTTPS URL, nothing is sent. Half-filled credentials (**Login** without **Password**, or the reverse) count as a misconfiguration and disable diagnostics too.

Multi-tenant Loki is not supported — no `X-Scope-OrgID` header is sent. The target must be single-tenant or sit behind a gateway that stamps the tenant itself.

A push counts as delivered only on HTTP 204, the status Loki answers with. Anything else — a `200` from a reverse proxy, a redirect (redirects are not followed) — is a failure: one WARNING, and diagnostics are off for the rest of that task instance.

Each entry carries a single stream label, `service="airflow-provider-yandex-admetrica"`, so label cardinality stays constant. Everything else lives in the JSON log line and is queried with LogQL over the parsed body:

```logql
{service="airflow-provider-yandex-admetrica"} | json | outcome != "success"
```

That label is this provider's own, so a Grafana query selecting by `service` sees these events only once the label is added to it; panels built on the fields every provider shares — `level`, `outcome`, `http_status`, `duration_ms`, `attempt` — work unchanged.

Because that label is the same for every task, all tasks write into one stream. On a Loki that rejects out-of-order writes, concurrent tasks can therefore have a push refused with a 4xx, which disables diagnostics for that task.

### Event fields

| Field | Description |
|---|---|
| `schema_version` | Event format version, currently `2` |
| `dag_id`, `task_id`, `dag_run_id`, `try_number`, `map_index` | Correlation with the Airflow task instance (`map_index` is `-1` when not mapped). These five are stamped by the Loki client at push time; the other fields come from the request itself |
| `outcome` | How the attempt ended — see the table below |
| `level` | Severity of the attempt: `info`, `warn` or `error` — see [Severity](#severity-level) below |
| `sent_at` | UTC ISO 8601 timestamp taken just before the request is sent |
| `endpoint` | `campaigns` or `stat` — which of the two APIs was asked |
| `advertiser_id` | The advertiser the connection names |
| `campaign_id`, `date` | The campaign and the day the request was scoped to. Empty for a request to the campaign list, which is scoped to neither |
| `offset` | The page being asked for. It counts rows already skipped on the campaign list, starting at 0, and rows themselves on the statistics endpoint, starting at 1 — that is the API's own numbering, and a walk starting at 0 there would ask for the first row twice |
| `attempt`, `max_attempts` | Retry counters for one request: `attempt` counts from 1 up to `max_attempts` as 429s, 5xx answers, network failures and a 400 the API names `query_error` are retried out of one budget |
| `request_method`, `request_url` | `"GET"` and the endpoint's address |
| `request_headers` | The headers the provider sets — `Authorization` (masked, see below) and `Accept` |
| `request_params` | The query as it went out: `ids`, `date1`, `date2`, `metrics`, `dimensions`, `limit`, `offset`, `sort` and the rest. Bounded parameter by parameter: at most 24 of them are described, a name at 40 characters and a text value at 300, each cut marked with `…`. A query carrying more than 24 parameters gets a `<params truncated>` key saying how many were left out. A name is a way out of the process like any other, so it passes the same masking gate as a value, and two names the bound reduces to the same text are told apart by the position of the parameter |
| `duration_ms` | Wall-clock duration of the HTTP attempt |
| `http_status` | Response status, `null` when the request never got one |
| `rows_count` | Number of rows the raw answer carried, `null` when no list of rows was recognised |
| `rows_shape_ok` | Whether the answer held a list of objects under its rows key (`campaigns` or `data`) |
| `payload_kind` | Which shape the body turned out to have: `dict` (the rows key is there, holding the list it promises or an empty value of another type), `rows_absent` (no rows key at all), `rows_non_list` (the rows key holds a non-empty value that is not a list), `non_dict` (the body itself is not a dict) |
| `total_rows`, `total_rows_rounded` | What the report declared about its own size, and whether that number is an approximation |
| `sampled`, `sample_share`, `sample_size`, `sample_space` | What the report said about sampling |
| `contains_sensitive_data` | Whether the API withheld part of the rows |
| `data_lag` | How far behind the data is, as the report declared it |
| `error_code`, `error_type`, `error_message` | `code`, `error_type` and `message` of the refusal the answer carried, in whichever of the three shapes it came: `{"error": {…}}`, `{"errors": [{…}]}` or a top-level `{"code": …, "message": …}`. `error_type` is the API's own name for the kind of refusal — `query_error`, `invalid_token` — and it is read from the refusal object itself, which is where this API states it; `error_code` stays empty for such an answer, because the `code` sits at the top level of the body rather than inside the refusal, and the refusal is what is described. The type and the message are flattened onto one line, bounded to 300 characters and passed through the same masking gate as everything else that leaves the process. Filled for an HTTP 200 whose rows could not be read and for any non-200 whose body names an error |
| `exception_type`, `exception_message` | Type of the exception that ended the attempt; the message is filled only for a JSON parse error reported by the standard decoder, from a fixed vocabulary |
| `rate_limit_limit`, `rate_limit_remaining` | `X-RateLimit-*` headers, collected on HTTP 429. AdMetrica documents no headers of the kind, so these are the conventional spellings read in case the API sends them. A header value is text the server wrote, so it passes the same masking point as everything else leaving the process |
| `response_body` | Raw response text, bounded and with the live token cut out — see [Raw response body](#raw-response-body) below |

The key set of an event is constant: a field the attempt never determined is present and `null`. `rows_count`, `rows_shape_ok` and `payload_kind` stay `null` for any attempt that never produced a parsed HTTP-200 body, and the report's own fields — `total_rows` through `data_lag` — are filled only from an answer that carried them.

#### The request as it went out

`request_method`, `request_url`, `request_headers` and `request_params` together are a template of the request, not a literal transcript of the wire. Two things separate them:

- **`Authorization` carries a mask**, `"OAuth y0__xC…9f2a"` — the `OAuth ` scheme kept, the token reduced to its first six and last four characters, joined by `…`. A token shorter than twenty characters — twice what the mask shows — is replaced whole by `***`, so the mask never spells out most of the value. The value is rebuilt from the scheme and the mask rather than copied and edited, so the raw header never enters an event at all. The mask is enough to tell one token from another; replaying the request means substituting a live one.
- **Only the headers the provider sets are listed.** The ones `requests` adds for the connection — `User-Agent`, `Accept-Encoding`, `Connection` — are not in the event: they do not change what the request means.

`request_headers` and `request_params` are nested objects. In LogQL, `| json` flattens nesting with an underscore, which is how these fields are queried:

```logql
{service="airflow-provider-yandex-admetrica"} | json | level = "error"
{service="airflow-provider-yandex-admetrica"} | json | endpoint = "stat" and sampled = "true"
{service="airflow-provider-yandex-admetrica"} | json | line_format "{{.request_params_ids}} {{.response_body}}"
```

### `outcome` values

| Value | Meaning |
|---|---|
| `success` | HTTP 200 with a well-formed answer, including an empty one — "this campaign had no impressions on this day" is a valid answer |
| `empty_shape` | HTTP 200 in which no list of row objects was recognised — the attempt raises, whatever `payload_kind` says about it |
| `auth_error` | HTTP 401 or 403 — raises at once, since nothing here refreshes the token |
| `retryable_error` | HTTP 429, any 5xx, or a 400 whose body names `error_type: query_error` — retried and, on the last attempt, raised. 429 and 5xx walk the 1 / 2 / 4 s ladder, and a `Retry-After` the answer named is honoured in place of its rung, capped at 300 s; the repeatable 400 walks a 5 / 15 / 45 s ladder of its own and reads no `Retry-After`. The rung is picked by the number of the attempt about to be spent rather than by the kind of the refusal, so a repeatable 400 arriving third waits the third rung. The outcome names the policy, and `http_status` with `error_type` name the fact, which is how a dashboard tells the two apart |
| `http_error` | Any other non-200 status — raises at once |
| `network_error` | The request never completed (timeout, DNS, TLS, proxy) — retried like a 5xx |
| `invalid_json` | HTTP 200 whose body could not be parsed |
| `unexpected_error` | A body that is valid JSON but not an object (`["a", "b"]`) — the attempt raises, naming `payload_kind=non_dict` — and, as a safety net, an attempt that ended some other way |

The whole 5xx range is retried rather than the familiar four: a proxy in front of the API answers with codes of its own choosing, and every one of them says the request never reached the logic that would refuse it on its merits.

`campaign_id`, `date` and `offset` say how much a failure cost. A statistics request failing at `offset = 1` broke before anything was collected for that campaign; further on, pagination broke in the middle, and the rows gathered before it go down with the task instead of into a partial file. Either way the task ends red, which is the signal to alert on: every failure ends the run, so the red task is the alert and a file on disk means a complete export of that day.

### Severity (`level`)

| `level` | When | Meaning |
|---|---|---|
| `info` | `success` | The answer is intelligible — whether or not it carried rows |
| `warn` | `retryable_error` or `network_error` with an attempt still left | A situation that may fix itself; a repeat is ahead of it |
| `error` | `retryable_error` or `network_error` on the last attempt; `auth_error`; `empty_shape`; `http_error`; `invalid_json`; `unexpected_error` | The answer is unintelligible, access was refused, or the request never completed |

`level` answers "is the answer intelligible, and is there still hope", not "did the task fail". A successful answer carrying no rows is routine — a campaign with no impressions on a day is the ordinary state of most of an advertiser's campaigns — so it stays at `info`, and the caveats a successful answer can carry, sampling and withheld rows, travel as fields of their own and as warnings in the task log. An `auth_error` is an `error` on first sight: the token is long-lived and nothing here refreshes it, so the attempt after it would be refused the same way.

**This table is also the body export policy.** The same `level` decides both the severity shown in Grafana and whether the raw body leaves the process (see below). Moving a row here changes what content is shipped, not just how alerts are coloured.

### Raw response body

`response_body` holds the response text, bounded to 32768 characters (fixed in the provider — there is no connection or operator setting for it). A longer body is cut to that budget and ends with `…[truncated]`. The text is read with the charset the server named in `Content-Type`, and as UTF-8 when it named none or named a codec Python does not know. Bytes that do not decode are replaced rather than dropping the body: a body no one can read is worth less here than one read on a good assumption.

| Situation | `response_body` |
|---|---|
| `level = "info"` | `null` — diagnostics deliberately do not read the body |
| Any other level, with a response whose bytes could be read | The response text, bounded, token cut out |
| `network_error` — there is no response | `null` |
| A response exists but its bytes could not be read, or spell the token out beyond the reach of a search for it | `null` |
| Diagnostics off (`loki_conn_id` unset) or the task being stopped | `null` — the body is not read and nothing is pushed |

The key is always present; `null` in it is not a promise that the level was `info`.

In a healthy run every event is `info`, so no body travels at all. Volume grows on failing runs — and on retries, since a `retryable_error` or a `network_error` with an attempt left is a `warn`: **every** unsuccessful try ships its body, four of them when a storm exhausts a request's attempts.

One event is one Loki line, and Loki refuses a line longer than `limits_config.max_line_size` — 256 KB by default. With every bounded field at its budget, a body of control characters and parameters of emoji — the widest each gets once JSON has escaped it — a line measures about 215 KB, so the default leaves room. An installation that lowered that limit — Grafana Cloud, a tuned self-hosted Loki — answers such a push with a non-204, and that first refusal disables diagnostics for the rest of that task instance, exactly as any other push failure does. Check `limits_config.max_line_size` on your instance before turning this on.

### Content policy

The raw response body leaves the process whenever the answer was not intelligible — that is, at every `level` other than `info` (see the tables above). **Treat it as arbitrary sensitive data.**

- **A body without recognised rows is not a body without sensitive content.** A response can carry socio-demographic breakdowns, internal identifiers or secrets inside an error object while holding no list of rows at all — and that body ships whole.
- **Known edge:** an anomalous outcome alongside recognised rows ships a body containing the report itself.
- **The token guarantee covers every channel.** The OAuth token is masked in `request_headers`, cut out of `response_body`, and cut out of `error_type`, `error_message` and the text of every exception this module raises — a server or a proxy is free to quote the `Authorization` header back inside a JSON `message`, and a structured description of an error is as much a way out as a raw body. Text the token survives — an answer that spells it out with something standing between its characters, as UTF-16 read as UTF-8 does — is dropped whole rather than shipped: the value outranks the diagnostic. A failure of an unforeseen kind — one neither this module nor the network layer worded — leaves the request path reworded the same way: its type names it and its text passes the same gate. The guarantee covers the task log too, whose lines are built from the same masked fields, and the traceback printed with a failure: an exception this module raises carries no original exception along as its cause or context, because an attached exception prints its own unmasked words underneath.
- **Somebody else's text leaves as one visible line.** Every text this provider writes out — a server's error message, a campaign name typed into a run form, a status the cabinet wrote — is flattened first: every run of whitespace, control characters and invisible formatting characters becomes a single space. That covers the line break that would turn one attempt into two lines of the log, the bidirectional override that would reorder a message on the screen and let a campaign name forge the counts and quotation marks around it, and the zero-width character that would leave the token legible to a reader while hiding it from the search that cuts it out.
- **The structured fields are a structure, not a boundary.** `error_code`/`error_type`/`error_message` and `exception_message` are narrow, queryable summaries; they describe the failure, they do not bound what the event discloses, because the body travels alongside.
- **There is no setting that keeps diagnostics on and bodies out.** The level table decides what travels; the only way to stop bodies from leaving is to leave `loki_conn_id` unset, which turns the whole feature off.
- Response headers other than the two `X-RateLimit-*` are not copied.
- Retention and access follow Loki: bodies live as long as the instance keeps them, and everyone holding the shared Loki credentials can read them.

How the structured fields are built:

- From a refusal only `code` (a value whose type is exactly `int`), `error_type` (a value whose type is exactly `str`, masked and truncated the same way as the message) and `message` (a value whose type is exactly `str`, flattened onto one line, masked and truncated to 300 characters) are taken. A value of an unexpected type is described by its type — `<non-dict error: list>`, `<non-str error_type: int>`, `<non-str message: dict>` — rather than serialised, so nested keys such as `details` or `trace` are never summarised into the event.
- `exception_message` is filled only for `invalid_json`, and only when the standard JSON decoder reported the failure. It is rebuilt from the exception's own attributes rather than from its rendered text, and the wording is chosen from a fixed vocabulary of the decoder's own literals — `Expecting value`, `Expecting ',' delimiter`, `Expecting ':' delimiter`, `Expecting property name enclosed in double quotes`, `Extra data`, `Unterminated string starting at`, `Invalid control character at`, `Invalid \escape`, `Invalid \uXXXX escape` — followed by the position counted in the document: `Expecting value: line 1 column 1 (char 0)`. Anything the decoder words differently is reported as `<other decoder message>` with the same position, because some decoder messages are formatted around a character taken from the document. A parse failure of any other origin records `exception_type` alone, as does every other outcome.
- Of the response headers, only the two `X-RateLimit-*` are copied, and only when their type is exactly `str`, truncated to 32 characters. A value of any other type is described by its type (`<non-str header: int>`), so no unknown object is ever rendered into the event.
- Truncation bounds length, not content.

## Documentation

- [AdMetrica API](https://yandex.ru/dev/admetrica/doc/ru/) — the API this provider speaks
- [Groupings and metrics](https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all) — the `am:e:…` names that go into `dimensions` and `metrics`
- [`docs/metrics-and-dimensions.md`](docs/metrics-and-dimensions.md) — every grouping and metric beside the record key it writes, the filter operators it accepts and the earliest date it answers for
- [Authorization](https://yandex.ru/dev/admetrica/doc/ru/authorization) — obtaining the OAuth token

## Examples

Three production examples live in [`examples/`](examples/), one per destination. All three expand a period into a mapped task group, one map index per day: the collection of a day and everything done with it live inside the index, so a failed day never holds the others back and re-running a day is a clear of its map index with the tasks below it. The dictionary snapshot is loaded once per run, by a group of its own after the days. All three expose `campaign_scope` and `campaign_ids` as run parameters, and all three default `campaign_scope` to `"all"` where the operator itself defaults to `"active"`: an example runs on `schedule=None` over the last thirty days, which is a backfill of a past period, and a past period is walked as it was.

- [`admetrica_to_s3_dag.py`](examples/admetrica_to_s3_dag.py) — S3 alone. The day's task walks its `kind="stats"` entries and uploads each under a key of its own; the dictionary goes up declaratively through `LocalFilesystemToS3Operator`
- [`admetrica_to_bigquery_dag.py`](examples/admetrica_to_bigquery_dag.py) — BigQuery alone, staged through GCS. The day's task creates the campaign's table when it is missing and loads its partition; the dictionary travels through `LocalFilesystemToGCSOperator` and `GCSToBigQueryOperator`
- [`admetrica_to_bq_and_s3_dag.py`](examples/admetrica_to_bq_and_s3_dag.py) — both at once. The two directions are parallel tasks off one `collect`, independent of each other, so a refusal on one side still lets the other reach its mart

They need more than this provider:

- `apache-airflow-providers-amazon` for the S3 destination and `apache-airflow-providers-google>=14.0.0` for the BigQuery one, which this package installs only under its `dev` extra — a deployment running a DAG installs them itself. The floor on the google provider is what the tables are created by: `BigQueryHook.create_table` for a campaign's table and `BigQueryCreateTableOperator` for the dictionary's, and 14.0.0 is the earliest release on PyPI carrying them, while the constraint set of Airflow 2.9.1 pins 10.17.0, so a deployment on that set installs the google provider above its own constraints or the first load into a fresh dataset fails on a table that is not there
- connections `yandex_admetrica_default` and `loki_default`, plus `aws_default` for the S3 destination and `google_cloud_default` for the BigQuery one
- a filesystem shared by every worker that runs the DAG's tasks, mounted at `BASE_DIR` on each of them. `collect` writes the day's files there and the uploads and `cleanup` are separate task instances that read them, and Airflow promises no worker affinity inside a task group: under Celery or Kubernetes with worker-local disks the uploads fail on a missing file and the collected files stay behind on the worker that wrote them. A single-machine `LocalExecutor`, or a shared volume mounted at `BASE_DIR`, is what makes the layout hold
- a GCS staging bucket for the two examples that load into BigQuery, which the DAG's first task creates when it is missing: the load reads from GCS, not from the worker's disk, so every file goes to the bucket first. What clears it afterwards is a one-day delete lifecycle rule the DAG adds beside the rules the bucket already carries, scoped by a `matchesPrefix` condition — the bucket may be a shared one, and the rule addresses this DAG's prefix and nothing else
- a BigQuery dataset that already exists, for the same two: the campaign's table is created by the day's task, and the connection needs the right to create tables in the dataset

## License

MIT
