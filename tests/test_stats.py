"""Tests for a day of statistics: what is asked, what is kept, what fails it."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _CAMPAIGNS_LIMIT,
    _ENDPOINT_URLS,
    _MAX_DIMENSIONS,
    _MAX_CAMPAIGNS,
    _MAX_LIMIT,
    _MAX_METRICS,
    _MAX_PAGES,
    _MAX_ROWS,
    _RESERVED_PARAMS,
    _STAT_OFFSET_BASE,
    AdmetricaHook,
    _page_budget,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

ADVERTISER_ID = 17004

DATE = "2026-08-20"

DIMENSIONS = ["am:e:placement", "am:e:deviceType"]
METRICS = ["am:e:renders", "am:e:clicks"]

_HOOK_LOGGER = "airflow_provider_yandex_admetrica.hooks.yandex_admetrica"


class _Sink:
    """A Loki stand-in that records what it was handed."""

    def __init__(self) -> None:
        self.enabled = True
        self.pushed: list[dict] = []

    def push(self, event: dict) -> None:
        self.pushed.append(dict(event))


def _hook(**kwargs) -> AdmetricaHook:
    """A hook whose connection is a working one, paced at nothing."""
    conn = Connection(
        conn_id="admetrica",
        conn_type="http",
        password=TOKEN,
        extra=json.dumps({"advertiser_id": ADVERTISER_ID}),
    )
    hook = AdmetricaHook(admetrica_conn_id="admetrica", request_delay=0, **kwargs)
    hook.get_connection = MagicMock(return_value=conn)
    return hook


def _response(payload: object) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = payload
    resp.content = json.dumps(payload, ensure_ascii=False).encode()
    resp.headers = {}
    return resp


def _row(*dimensions: object, metrics: tuple = (1, 2)) -> dict:
    """One row of a report: the grouping values it matched and its numbers."""
    return {"dimensions": list(dimensions), "metrics": list(metrics)}


def _placement(name: str, placement_id: object = None) -> dict:
    value = {"name": name}
    if placement_id is not None:
        value["id"] = placement_id
    return value


def _page(rows: list, total: object = None, rounded: object = None, **extra) -> dict:
    """A statistics answer, with only the fields the case is about."""
    body: dict = {"data": rows}
    if total is not None:
        body["total_rows"] = total
    if rounded is not None:
        body["total_rows_rounded"] = rounded
    body.update(extra)
    return body


def _campaign(campaign_id: int) -> dict:
    return {
        "campaign_id": campaign_id,
        "name": f"Campaign {campaign_id}",
        "status": "active",
        "date_start": "2026-01-01",
        "date_end": "2026-12-31",
        "advertiser_id": ADVERTISER_ID,
        "advertiser_name": "Advertiser",
    }


def _api(campaigns: list[dict], pages: list[dict]):
    """A ``requests.get`` stand-in answering both endpoints.

    The campaign list is answered from *campaigns* however often it is asked
    for; every statistics request takes the next of *pages*, so a test spells
    out the walk it expects and a request too many fails loudly.
    """
    remaining = list(pages)

    def get(url, **kwargs):
        if url == _ENDPOINT_URLS["campaigns"]:
            return _response({"campaigns": campaigns, "total": len(campaigns)})
        return _response(remaining.pop(0))

    return get


def _stat_params(mock_get: MagicMock) -> list[dict]:
    """The query parameters of each statistics request, in order."""
    return [
        call.kwargs["params"]
        for call in mock_get.call_args_list
        if call.args[0] == _ENDPOINT_URLS["stat"]
    ]


def _said(caplog) -> list[str]:
    """What the hook itself wrote, apart from anything else the run logged."""
    return [r.getMessage() for r in caplog.records if r.name == _HOOK_LOGGER]


def _collect(hook: AdmetricaHook, campaigns: list[dict], pages: list[dict], **kwargs):
    """Run one day through *hook* and return the records and the request mock."""
    with patch("requests.get", side_effect=_api(campaigns, pages)) as mock_get:
        records = hook.get_stats(DATE, DIMENSIONS, METRICS, **kwargs)
    return records, mock_get


# ---------------------------------------------------------------------------
# What comes back
# ---------------------------------------------------------------------------


class TestOnePage:
    def test_a_single_page_is_the_whole_day(self):
        rows = [_row(_placement("Главная", 55), {"name": "mobile"})]
        records, mock_get = _collect(_hook(), [_campaign(1)], [_page(rows, total=1)])
        assert records == [
            {
                "date": DATE,
                "advertiser_id": ADVERTISER_ID,
                "campaign_id": 1,
                "dimensions": {
                    "placement": {"name": "Главная", "id": 55},
                    "device_type": {"name": "mobile"},
                },
                "metrics": {"renders": 1, "clicks": 2},
            }
        ]
        assert len(_stat_params(mock_get)) == 1

    def test_the_statistics_endpoint_is_the_one_asked(self):
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page([], 0)])) as mock_get:
            _hook().get_stats(DATE, DIMENSIONS, METRICS)
        assert mock_get.call_args.args[0] == _ENDPOINT_URLS["stat"]

    def test_every_campaign_is_asked_for_on_its_own(self):
        pages = [_page([_row(_placement("A", 1), {"name": "mobile"})], total=1) for _ in range(3)]
        campaigns = [_campaign(1), _campaign(2), _campaign(3)]
        records, mock_get = _collect(_hook(), campaigns, pages)
        assert [p["ids"] for p in _stat_params(mock_get)] == [1, 2, 3]
        assert [r["campaign_id"] for r in records] == [1, 2, 3]

    def test_a_campaign_without_data_contributes_nothing(self):
        campaigns = [_campaign(1), _campaign(2)]
        pages = [_page([], total=0), _page([_row(_placement("A", 1), {"name": "mobile"})], total=1)]
        records, _ = _collect(_hook(), campaigns, pages)
        assert [r["campaign_id"] for r in records] == [2]

    def test_a_report_of_ratios_alone_comes_back_whole(self):
        """A ratio without a denominator is empty, and a day of them is a day."""
        rows = [_row(metrics=(None, None))]
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page(rows, total=1)])):
            records = _hook().get_stats(DATE, [], ["am:e:ctr", "am:e:cpm"])
        assert [r["metrics"] for r in records] == [{"ctr": None, "cpm": None}]

    def test_a_day_of_an_advertiser_without_campaigns_is_empty(self):
        records, mock_get = _collect(_hook(), [], [])
        assert records == []
        assert _stat_params(mock_get) == []

    def test_the_day_asked_for_is_the_day_stamped(self):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        records, mock_get = _collect(_hook(), [_campaign(1)], [_page(rows, total=1)])
        (sent,) = _stat_params(mock_get)
        assert sent["date1"] == sent["date2"] == DATE
        assert records[0]["date"] == DATE


# ---------------------------------------------------------------------------
# What goes out
# ---------------------------------------------------------------------------


class TestRequest:
    def test_the_offset_starts_at_one(self):
        _, mock_get = _collect(_hook(), [_campaign(1)], [_page([], total=0)])
        assert _stat_params(mock_get)[0]["offset"] == _STAT_OFFSET_BASE == 1

    def test_the_sort_names_every_grouping_asked_for(self):
        _, mock_get = _collect(_hook(), [_campaign(1)], [_page([], total=0)])
        sent = _stat_params(mock_get)[0]
        assert sent["sort"] == ",".join(DIMENSIONS)
        assert sent["dimensions"] == ",".join(DIMENSIONS)
        assert sent["metrics"] == ",".join(METRICS)

    def test_the_defaults_that_guard_the_numbers_go_out(self):
        _, mock_get = _collect(_hook(), [_campaign(1)], [_page([], total=0)])
        sent = _stat_params(mock_get)[0]
        assert sent["accuracy"] == "full"
        assert sent["include_undefined"] == "true"

    def test_accuracy_and_include_undefined_go_out_as_given(self):
        _, mock_get = _collect(
            _hook(),
            [_campaign(1)],
            [_page([], total=0)],
            accuracy="0.1",
            include_undefined=False,
        )
        sent = _stat_params(mock_get)[0]
        assert sent["accuracy"] == "0.1"
        assert sent["include_undefined"] == "false"

    @pytest.mark.parametrize(("value", "sent"), [(True, "true"), (False, "false")])
    def test_include_undefined_goes_out_in_the_apis_spelling(self, value, sent):
        _, mock_get = _collect(
            _hook(),
            [_campaign(1)],
            [_page([], total=0)],
            include_undefined=value,
        )
        assert _stat_params(mock_get)[0]["include_undefined"] == sent

    def test_include_undefined_of_none_is_left_out_of_the_query(self):
        _, mock_get = _collect(
            _hook(),
            [_campaign(1)],
            [_page([], total=0)],
            include_undefined=None,
        )
        assert "include_undefined" not in _stat_params(mock_get)[0]

    def test_filters_timezone_and_lang_go_out(self):
        _, mock_get = _collect(
            _hook(),
            [_campaign(1)],
            [_page([], total=0)],
            filters="am:e:deviceType=='mobile'",
            timezone="+03:00",
            lang="ru",
        )
        sent = _stat_params(mock_get)[0]
        assert sent["filters"] == "am:e:deviceType=='mobile'"
        assert sent["timezone"] == "+03:00"
        assert sent["lang"] == "ru"

    def test_an_unset_parameter_is_left_out_of_the_query(self):
        _, mock_get = _collect(_hook(), [_campaign(1)], [_page([], total=0)])
        sent = _stat_params(mock_get)[0]
        assert "filters" not in sent
        assert "timezone" not in sent
        assert "lang" not in sent

    def test_the_page_size_is_the_hooks_limit(self):
        _, mock_get = _collect(_hook(limit=250), [_campaign(1)], [_page([], total=0)])
        assert _stat_params(mock_get)[0]["limit"] == 250

    def test_without_groupings_neither_dimensions_nor_sort_go_out(self):
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page([_row()], 1)])) as m:
            records = _hook().get_stats(DATE, [], METRICS)
        sent = _stat_params(m)[0]
        assert "dimensions" not in sent
        assert "sort" not in sent
        assert records[0]["dimensions"] == {}


class TestExtraParams:
    @pytest.mark.parametrize("name", sorted(_RESERVED_PARAMS))
    def test_a_reserved_key_is_refused_before_any_request(self, name):
        with patch("requests.get") as mock_get:
            with pytest.raises(ValueError, match=name):
                _hook().get_stats(DATE, DIMENSIONS, METRICS, extra_params={name: "x"})
        assert mock_get.call_count == 0

    def test_it_adds_names_and_overrides_none(self):
        """One-directional merge: a name the query already carries stays as it is."""
        _, mock_get = _collect(
            _hook(),
            [_campaign(1)],
            [_page([], total=0)],
            filters="am:e:placement=='A'",
            extra_params={"pretty": True},
        )
        sent = _stat_params(mock_get)[0]
        assert sent["pretty"] is True
        assert sent["filters"] == "am:e:placement=='A'"
        assert sent["date1"] == DATE
        assert sent["metrics"] == ",".join(METRICS)

    def test_a_key_of_its_own_reaches_the_query(self):
        _, mock_get = _collect(
            _hook(),
            [_campaign(1)],
            [_page([], total=0)],
            extra_params={"goal_id": 12345},
        )
        assert _stat_params(mock_get)[0]["goal_id"] == 12345

    def test_a_parameter_spelled_into_a_name_reaches_the_record_key(self):
        rows = [_row(_placement("A", 1), {"name": "mobile"}, metrics=(7,))]
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page(rows, 1)])):
            records = _hook().get_stats(
                DATE,
                DIMENSIONS,
                ["am:e:goal<goal_id>Reaches"],
                extra_params={"goal_id": 12345},
            )
        assert records[0]["metrics"] == {"goal12345_reaches": 7}


class TestDocumentedLimits:
    def test_too_many_metrics_are_refused_before_any_request(self):
        metrics = [f"am:e:metric{i}" for i in range(_MAX_METRICS + 1)]
        with patch("requests.get") as mock_get:
            with pytest.raises(ValueError, match=str(_MAX_METRICS)):
                _hook().get_stats(DATE, DIMENSIONS, metrics)
        assert mock_get.call_count == 0

    def test_too_many_dimensions_are_refused_before_any_request(self):
        dimensions = [f"am:e:dim{i}" for i in range(_MAX_DIMENSIONS + 1)]
        with patch("requests.get") as mock_get:
            with pytest.raises(ValueError, match=str(_MAX_DIMENSIONS)):
                _hook().get_stats(DATE, dimensions, METRICS)
        assert mock_get.call_count == 0

    def test_a_request_at_the_ceiling_is_allowed(self):
        dimensions = [f"am:e:dim{i}" for i in range(_MAX_DIMENSIONS)]
        metrics = [f"am:e:metric{i}" for i in range(_MAX_METRICS)]
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page([], 0)])):
            assert _hook().get_stats(DATE, dimensions, metrics) == []

    def test_a_report_without_metrics_is_refused_before_any_request(self):
        """`metrics` is required; an empty one goes out as `metrics=` and 400s."""
        with patch("requests.get") as mock_get:
            with pytest.raises(ValueError, match="at least one metric"):
                _hook().get_stats(DATE, DIMENSIONS, [])
        assert mock_get.call_count == 0

    @pytest.mark.parametrize(
        "limit",
        [0, -1, _MAX_LIMIT + 1, 200000, 1.5, "10000", True, None],
        ids=["zero", "negative", "over", "far_over", "fraction", "text", "flag", "none"],
    )
    def test_a_limit_outside_the_documented_range_is_refused(self, limit):
        """At or below zero the API falls back to 100 and no page is ever short."""
        with patch("requests.get") as mock_get:
            with pytest.raises(ValueError, match="limit must be"):
                _hook(limit=limit).get_stats(DATE, DIMENSIONS, METRICS)
        assert mock_get.call_count == 0

    @pytest.mark.parametrize("limit", [1, 10000, _MAX_LIMIT])
    def test_a_limit_inside_it_is_allowed(self, limit):
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page([], 0)])):
            assert _hook(limit=limit).get_stats(DATE, DIMENSIONS, METRICS) == []


class TestTheDayAsked:
    """The hook holds its own boundary: a day is a day whoever calls it."""

    @pytest.mark.parametrize(
        "date",
        ["20260820", "2026-8-20", "2026-08-20 00:00:00", "not-a-day", "", None, 20260820],
        ids=["compact", "unpadded", "timestamp", "words", "empty", "none", "number"],
    )
    def test_a_day_that_is_not_a_day_is_refused_before_any_request(self, date):
        with patch("requests.get") as mock_get:
            with pytest.raises(ValueError, match="date must be a day"):
                _hook().get_stats(date, DIMENSIONS, METRICS)
        assert mock_get.call_count == 0

    def test_the_refusal_names_the_day_by_shape_and_never_quotes_it(self):
        with patch("requests.get"):
            with pytest.raises(ValueError) as excinfo:
                _hook().get_stats(TOKEN, DIMENSIONS, METRICS)
        assert TOKEN not in str(excinfo.value)
        assert f"{len(TOKEN)} character(s)" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Walking a campaign
# ---------------------------------------------------------------------------


class TestPagination:
    def test_several_pages_are_collected_in_order(self):
        first = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        second = [_row(_placement("C", 3), {"name": "mobile"})]
        pages = [_page(first, total=3, rounded=False), _page(second, total=3, rounded=False)]
        records, mock_get = _collect(_hook(limit=2), [_campaign(1)], pages)
        assert [r["dimensions"]["placement"]["id"] for r in records] == [1, 2, 3]
        assert len(_stat_params(mock_get)) == 2

    def test_the_offset_counts_the_rows_already_read(self):
        first = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        second = [_row(_placement("C", 3), {"name": "mobile"})]
        pages = [_page(first, total=3, rounded=False), _page(second, total=3, rounded=False)]
        _, mock_get = _collect(_hook(limit=2), [_campaign(1)], pages)
        assert [p["offset"] for p in _stat_params(mock_get)] == [1, 3]

    def test_a_reached_total_on_a_full_page_still_asks_for_the_page_after_it(self):
        """The rows run out when the report says so, not when its count is met."""
        rows = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        pages = [_page(rows, total=2, rounded=False), _page([], total=2, rounded=False)]
        records, mock_get = _collect(_hook(limit=2), [_campaign(1)], pages)
        assert len(records) == 2
        assert [p["offset"] for p in _stat_params(mock_get)] == [1, 3]

    def test_a_total_a_later_page_raises_never_ends_the_walk_early(self):
        """A count that was one page stale would leave the tail behind."""
        first = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        second = [_row(_placement("C", 3), {"name": "mobile"})]
        pages = [_page(first, total=2, rounded=False), _page(second, total=3, rounded=False)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="on an earlier page"):
                _hook(limit=2).get_stats(DATE, DIMENSIONS, METRICS)

    def test_a_rounded_total_never_stops_the_walk(self):
        first = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        second = [_row(_placement("C", 3), {"name": "mobile"})]
        pages = [_page(first, total=2, rounded=True), _page(second, total=2, rounded=True)]
        records, mock_get = _collect(_hook(limit=2), [_campaign(1)], pages)
        assert len(records) == 3
        assert len(_stat_params(mock_get)) == 2

    def test_a_rounding_flag_seen_once_disarms_the_total_for_the_walk(self):
        first = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        second = [
            _row(_placement("C", 3), {"name": "mobile"}),
            _row(_placement("D", 4), {"name": "mobile"}),
        ]
        third = [_row(_placement("E", 5), {"name": "mobile"})]
        pages = [
            _page(first, total=4, rounded=True),
            _page(second, total=4, rounded=False),
            _page(third, total=4, rounded=False),
        ]
        records, mock_get = _collect(_hook(limit=2), [_campaign(1)], pages)
        assert len(records) == 5
        assert len(_stat_params(mock_get)) == 3

    def test_each_campaign_is_walked_from_the_first_row_again(self):
        rows = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        pages = [
            _page(rows, total=2, rounded=False),
            _page([], total=2, rounded=False),
            _page(rows, total=2, rounded=False),
            _page([], total=2, rounded=False),
        ]
        _, mock_get = _collect(_hook(limit=2), [_campaign(1), _campaign(2)], pages)
        assert [p["offset"] for p in _stat_params(mock_get)] == [1, 3, 1, 3]


# ---------------------------------------------------------------------------
# What fails a day
# ---------------------------------------------------------------------------


class TestRowIdentity:
    def test_a_row_repeated_between_pages_of_one_campaign_fails_the_day(self):
        repeated = _row(_placement("A", 1), {"name": "mobile"})
        first = [repeated, _row(_placement("B", 2), {"name": "mobile"})]
        pages = [_page(first, total=3, rounded=False), _page([repeated], total=3, rounded=False)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="already carried"):
                _hook(limit=2).get_stats(DATE, DIMENSIONS, METRICS)

    def test_the_same_groupings_in_two_campaigns_are_two_rows(self):
        rows = [_row(_placement("Главная", 55), {"name": "mobile"})]
        pages = [_page(rows, total=1), _page(rows, total=1)]
        records, _ = _collect(_hook(), [_campaign(1), _campaign(2)], pages)
        assert [r["campaign_id"] for r in records] == [1, 2]

    def test_two_placements_sharing_a_name_are_two_rows(self):
        rows = [
            _row(_placement("Главная", 55), {"name": "mobile"}),
            _row(_placement("Главная", 56), {"name": "mobile"}),
        ]
        records, _ = _collect(_hook(), [_campaign(1)], [_page(rows, total=2)])
        assert [r["dimensions"]["placement"]["id"] for r in records] == [55, 56]

    def test_two_placements_sharing_a_name_and_bringing_no_id_are_a_repeat(self):
        rows = [_row(_placement("Главная"), {"name": "mobile"})]
        pages = [_page(rows, total=2, rounded=False), _page(rows, total=2, rounded=False)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="already carried"):
                _hook(limit=1).get_stats(DATE, DIMENSIONS, METRICS)

    def test_two_names_sharing_an_empty_id_are_two_rows(self):
        """Only `name` is guaranteed, so an `id` holding null tells nothing apart."""
        first = _row({"id": None, "name": "Главная"}, {"name": "mobile"})
        second = _row({"id": None, "name": "Раздел"}, {"name": "mobile"})
        pages = [
            _page([first], total=2, rounded=False),
            _page([second], total=2, rounded=False),
            _page([], total=2, rounded=False),
        ]
        records, _ = _collect(_hook(limit=1), [_campaign(1)], pages)
        assert [r["dimensions"]["placement"]["name"] for r in records] == ["Главная", "Раздел"]

    def test_one_name_under_an_empty_id_repeated_between_pages_is_a_repeat(self):
        rows = [_row({"id": None, "name": "Главная"}, {"name": "mobile"})]
        pages = [_page(rows, total=2, rounded=False), _page(rows, total=2, rounded=False)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="already carried"):
                _hook(limit=1).get_stats(DATE, DIMENSIONS, METRICS)

    def test_two_rows_whose_values_are_scalars_are_two_rows(self):
        """The API can send a scalar where an object is documented."""
        first = _row("Главная", "mobile")
        second = _row("Раздел", "desktop")
        pages = [
            _page([first], total=2, rounded=False),
            _page([second], total=2, rounded=False),
            _page([], total=2, rounded=False),
        ]
        records, _ = _collect(_hook(limit=1), [_campaign(1)], pages)
        assert len(records) == 2

    def test_a_scalar_repeated_between_pages_is_still_a_repeat(self):
        rows = [_row("Главная", "mobile")]
        pages = [_page(rows, total=2, rounded=False), _page(rows, total=2, rounded=False)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="already carried"):
                _hook(limit=1).get_stats(DATE, DIMENSIONS, METRICS)

    def test_two_values_carrying_neither_id_nor_name_are_two_rows(self):
        """A degenerate value is read whole; projecting it onto a constant would
        call every such row a repeat of the first."""
        first = _row({"segment": "A"}, {"name": "mobile"})
        second = _row({"segment": "B"}, {"name": "mobile"})
        pages = [
            _page([first], total=2, rounded=False),
            _page([second], total=2, rounded=False),
            _page([], total=2, rounded=False),
        ]
        records, _ = _collect(_hook(limit=1), [_campaign(1)], pages)
        assert len(records) == 2

    def test_without_groupings_the_single_row_is_never_checked(self):
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page([_row()], 1)])):
            records = _hook().get_stats(DATE, [], METRICS)
        assert len(records) == 1


class TestCompleteness:
    def test_an_exact_total_that_does_not_match_fails_the_day(self):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with patch(
            "requests.get", side_effect=_api([_campaign(1)], [_page(rows, total=5, rounded=False)])
        ):
            with pytest.raises(AirflowException) as excinfo:
                _hook().get_stats(DATE, DIMENSIONS, METRICS)
        message = str(excinfo.value)
        assert "1 rows" in message
        assert "5" in message

    def test_a_rounded_total_that_does_not_match_only_warns(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            records, _ = _collect(
                _hook(), [_campaign(1)], [_page(rows, total=5, rounded=True)]
            )
        assert len(records) == 1
        assert any("total_rows_rounded" in r.getMessage() for r in caplog.records)

    def test_an_answer_without_a_readable_total_fails_the_day(self):
        """Rows nothing can be checked against are rows nothing may trust."""
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page(rows)])):
            with pytest.raises(AirflowException, match="no readable total_rows"):
                _hook().get_stats(DATE, DIMENSIONS, METRICS)

    def test_a_total_that_is_not_a_number_is_no_total_at_all(self):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page(rows, total="5")])):
            with pytest.raises(AirflowException, match="no readable total_rows"):
                _hook().get_stats(DATE, DIMENSIONS, METRICS)

    @pytest.mark.parametrize("flag", ["false", "true", 0, 1, "", []])
    def test_a_rounding_flag_that_is_not_a_boolean_fails_the_day(self, flag):
        """A flag read as truthy would disown the exact check and pass a short day."""
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        pages = [_page(rows, total=5, rounded=flag)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="not a boolean"):
                _hook().get_stats(DATE, DIMENSIONS, METRICS)

    def test_a_rounding_flag_that_is_null_fails_the_day(self):
        """A field holding `null` is one the answer carried and nothing can read."""
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        pages = [_page(rows, total=5, total_rows_rounded=None)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="not a boolean"):
                _hook().get_stats(DATE, DIMENSIONS, METRICS)

    def test_a_missing_rounding_flag_leaves_the_exact_check_armed(self):
        """An answer that says nothing about rounding declares an exact total."""
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with patch("requests.get", side_effect=_api([_campaign(1)], [_page(rows, total=5)])):
            with pytest.raises(AirflowException, match="1 rows"):
                _hook().get_stats(DATE, DIMENSIONS, METRICS)

    def test_two_exact_totals_that_disagree_fail_the_day(self):
        first = [_row(_placement("A", 1), {"name": "mobile"})]
        second = [_row(_placement("B", 2), {"name": "mobile"})]
        pages = [_page(first, total=2, rounded=False), _page(second, total=7, rounded=False)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)):
            with pytest.raises(AirflowException, match="on an earlier page"):
                _hook(limit=1).get_stats(DATE, DIMENSIONS, METRICS)

    def test_rounded_totals_that_disagree_are_two_approximations(self):
        """Numbers the report calls approximate are free to differ between pages."""
        first = [_row(_placement("A", 1), {"name": "mobile"})]
        second = [_row(_placement("B", 2), {"name": "mobile"})]
        pages = [
            _page(first, total=2, rounded=True),
            _page(second, total=7, rounded=True),
            _page([], total=7, rounded=True),
        ]
        records, _ = _collect(_hook(limit=1), [_campaign(1)], pages)
        assert len(records) == 2

    def test_a_matching_total_says_nothing(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            _collect(_hook(), [_campaign(1)], [_page(rows, total=1, rounded=False)])
        assert _said(caplog) == []


# ---------------------------------------------------------------------------
# What the answer says about its own numbers
# ---------------------------------------------------------------------------


class TestCaveats:
    def test_a_sampled_answer_warns_with_its_share(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        page = _page(rows, total=1, sampled=True, sample_share=0.1, sample_size=10)
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            _collect(_hook(), [_campaign(1)], [page])
        assert any("0.1" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)

    def test_withheld_rows_warn(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        page = _page(rows, total=1, contains_sensitive_data=True)
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            _collect(_hook(), [_campaign(1)], [page])
        assert any("sensitive" in r.getMessage() for r in caplog.records)

    def test_a_data_lag_is_information(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with caplog.at_level(logging.INFO, logger=_HOOK_LOGGER):
            _collect(_hook(), [_campaign(1)], [_page(rows, total=1, data_lag=3600)])
        lines = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert any("3600" in line for line in lines)

    def test_a_quiet_answer_says_nothing(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        page = _page(
            rows, total=1, rounded=False, sampled=False, contains_sensitive_data=False, data_lag=0
        )
        with caplog.at_level(logging.INFO, logger=_HOOK_LOGGER):
            _collect(_hook(), [_campaign(1)], [page])
        assert _said(caplog) == []

    def test_the_caveats_travel_with_the_event_describing_the_request(self):
        sink = _Sink()
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        page = _page(
            rows,
            total=1,
            rounded=False,
            sampled=True,
            sample_share=0.1,
            sample_size=10,
            sample_space=100,
            contains_sensitive_data=True,
            data_lag=3600,
        )
        _collect(_hook(loki=sink), [_campaign(1)], [page])
        event = sink.pushed[-1]
        assert event["endpoint"] == "stat"
        assert event["campaign_id"] == 1
        assert event["date"] == DATE
        assert event["offset"] == 1
        assert event["total_rows"] == 1
        assert event["total_rows_rounded"] is False
        assert event["sampled"] is True
        assert event["sample_share"] == 0.1
        assert event["sample_size"] == 10
        assert event["sample_space"] == 100
        assert event["contains_sensitive_data"] is True
        assert event["data_lag"] == 3600

    def test_a_caveat_the_answer_left_out_stays_empty_in_the_event(self):
        sink = _Sink()
        _collect(_hook(loki=sink), [_campaign(1)], [_page([], total=0)])
        event = sink.pushed[-1]
        assert event["total_rows"] == 0
        assert event["sampled"] is None
        assert event["data_lag"] is None

    def test_the_campaign_list_declares_no_caveats(self):
        sink = _Sink()
        _collect(_hook(loki=sink), [_campaign(1)], [_page([], total=0)])
        (campaigns_event,) = [e for e in sink.pushed if e["endpoint"] == "campaigns"]
        assert campaigns_event["total_rows"] is None
        assert campaigns_event["sampled"] is None


# ---------------------------------------------------------------------------
# The campaign list is read once
# ---------------------------------------------------------------------------


class TestCampaignList:
    def test_the_list_is_fetched_once_for_the_whole_day(self):
        campaigns = [_campaign(1), _campaign(2)]
        pages = [_page([], total=0), _page([], total=0)]
        with patch("requests.get", side_effect=_api(campaigns, pages)) as mock_get:
            _hook().get_stats(DATE, DIMENSIONS, METRICS)
        list_calls = [c for c in mock_get.call_args_list if c.args[0] == _ENDPOINT_URLS["campaigns"]]
        assert len(list_calls) == 1

    def test_a_second_day_reuses_the_same_list(self):
        hook = _hook()
        pages = [_page([], total=0), _page([], total=0)]
        with patch("requests.get", side_effect=_api([_campaign(1)], pages)) as mock_get:
            hook.get_stats(DATE, DIMENSIONS, METRICS)
            hook.get_stats("2026-08-19", DIMENSIONS, METRICS)
        list_calls = [c for c in mock_get.call_args_list if c.args[0] == _ENDPOINT_URLS["campaigns"]]
        assert len(list_calls) == 1
        assert len(_stat_params(mock_get)) == 2


# ---------------------------------------------------------------------------
# A walk that does not converge
# ---------------------------------------------------------------------------


class TestTheWalkIsBounded:
    """An endpoint that ignores the offset must cost a task a bounded amount."""

    def test_the_budget_is_the_rows_a_walk_may_collect(self):
        """The same amount of data whatever page size it is read in."""
        assert _page_budget(10000, _MAX_ROWS) * 10000 == _MAX_ROWS
        assert _page_budget(1000, _MAX_ROWS) * 1000 == _MAX_ROWS
        assert _page_budget(_MAX_ROWS, _MAX_ROWS) == 1

    def test_a_page_too_small_to_reach_the_ceiling_runs_out_of_requests(self):
        """A row ceiling says nothing about how long a walk may take."""
        assert _page_budget(1, _MAX_ROWS) == _MAX_PAGES
        assert _page_budget(0, _MAX_ROWS) == _MAX_PAGES

    def test_a_ceiling_that_does_not_divide_evenly_is_reached_and_not_missed(self):
        assert _page_budget(3, 10) == 4

    def test_full_pages_of_new_rows_stop_at_the_page_budget(self):
        hook = _hook(limit=1)
        budget = 25

        def endless(url, **kwargs):
            if url == _ENDPOINT_URLS["campaigns"]:
                return _response({"campaigns": [_campaign(1)], "total": 1})
            # A new row every time: the seen-key set never fires and no page is
            # ever short, so nothing but the cap ends this walk.
            endless.seen += 1
            return _response(
                _page(
                    [_row(_placement("A", endless.seen), {"name": "mobile"})],
                    total=_MAX_ROWS,
                    rounded=True,
                )
            )

        endless.seen = 0

        with patch(f"{_HOOK_LOGGER}._MAX_PAGES", budget):
            with patch("requests.get", side_effect=endless) as mock_get:
                with pytest.raises(AirflowException, match="full pages"):
                    hook.get_stats(DATE, DIMENSIONS, METRICS)

        assert len(_stat_params(mock_get)) == budget

    def test_a_campaign_list_that_never_ends_stops_at_the_page_budget(self):
        hook = _hook()
        counter = {"n": 0}

        def endless(url, **kwargs):
            counter["n"] += 1
            start = counter["n"] * _CAMPAIGNS_LIMIT
            return _response(
                {
                    "campaigns": [_campaign(start + i) for i in range(_CAMPAIGNS_LIMIT)],
                    "total": _MAX_CAMPAIGNS,
                }
            )

        with patch("requests.get", side_effect=endless) as mock_get:
            with pytest.raises(AirflowException, match="full pages"):
                hook.get_campaigns()

        assert mock_get.call_count == _page_budget(_CAMPAIGNS_LIMIT, _MAX_CAMPAIGNS)

    def test_a_list_that_repeats_a_campaign_fails_at_once(self):
        """A repeated id says the offset is not moving through the list."""
        hook = _hook()
        page = [_campaign(i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]

        body = {"campaigns": page, "total": 2 * _CAMPAIGNS_LIMIT}
        with patch("requests.get", return_value=_response(body)) as mock_get:
            with pytest.raises(AirflowException, match="twice"):
                hook.get_campaigns()

        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# A campaign the answer named no usable id for
# ---------------------------------------------------------------------------


class TestCampaignsWithoutAnId:
    """Such a campaign fails the export rather than quietly leaving it short."""

    @pytest.mark.parametrize(
        "raw",
        [
            {},
            {"campaign_id": None},
            {"campaign_id": 123456.0},
            {"campaign_id": "12-34"},
            {"campaign_id": 0},
        ],
        ids=["absent", "null", "fractional", "not a number", "zero"],
    )
    def test_a_campaign_whose_id_cannot_be_read_fails_the_day(self, raw):
        campaigns = [{"name": "No usable id", **raw}, _campaign(2)]

        with pytest.raises(AirflowException, match="not a positive whole number"):
            _collect(_hook(), campaigns, [_page([], 0)])

    def test_the_failure_names_the_campaign_it_could_not_read(self):
        campaigns = [{"name": "Autumn brand", "campaign_id": 123456.0}]

        with pytest.raises(AirflowException) as excinfo:
            _collect(_hook(), campaigns, [_page([], 0)])

        assert "float" in str(excinfo.value)
        assert "Autumn brand" in str(excinfo.value)

    def test_the_day_is_not_asked_for_one_campaign_at_a_time_instead(self):
        """The list fails, so no statistics request goes out for the rest of it."""
        campaigns = [{"name": "No usable id"}, _campaign(2)]

        with patch("requests.get", side_effect=_api(campaigns, [])) as mock_get:
            with pytest.raises(AirflowException, match="not a positive whole number"):
                _hook().get_stats(DATE, DIMENSIONS, METRICS)

        assert _stat_params(mock_get) == []

    @pytest.mark.parametrize("raw", ["7", 7], ids=["text", "number"])
    def test_an_id_written_either_way_names_the_same_campaign(self, raw):
        campaigns = [{**_campaign(7), "campaign_id": raw}]
        _, mock_get = _collect(_hook(), campaigns, [_page([], 0)])
        assert _stat_params(mock_get)[0]["ids"] == 7


class TestWhatTheRequestIsWarnedAboutOnce:
    """A caveat about the request is written once, not once per row it reaches."""

    def _rows(self, count: int, metrics: tuple = (1,)) -> list[dict]:
        return [
            _row(_placement(f"P{n}", n), {"name": "mobile"}, metrics=metrics)
            for n in range(count)
        ]

    def test_an_unanswered_placeholder_is_one_warning_for_the_whole_day(self, caplog):
        """The names and the parameters are the request's, identical for every row."""
        rows = self._rows(5)
        pages = [_page(rows, total=5), _page(rows, total=5)]
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=_api([_campaign(1), _campaign(2)], pages)):
                _hook().get_stats(DATE, DIMENSIONS, ["am:e:goal<goal_id>Reaches"])

        assert len([line for line in _said(caplog) if "does not carry" in line]) == 1

    def test_two_names_writing_one_key_are_one_warning_for_the_whole_day(self, caplog):
        rows = self._rows(5, metrics=(1, 2))
        pages = [_page(rows, total=5), _page(rows, total=5)]
        metrics = ["am:e:goal12345Reaches", "am:e:goal<goal_id>Reaches"]
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=_api([_campaign(1), _campaign(2)], pages)):
                _hook().get_stats(
                    DATE, DIMENSIONS, metrics, extra_params={"goal_id": 12345}
                )

        assert len([line for line in _said(caplog) if "already writes" in line]) == 1
