"""Tests for a day of statistics: what is asked, what is kept, what fails it."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _ENDPOINT_URLS,
    _MAX_DIMENSIONS,
    _MAX_METRICS,
    _RESERVED_PARAMS,
    _STAT_OFFSET_BASE,
    AdmetricaHook,
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

    def test_a_closed_total_stops_the_walk_on_a_full_page(self):
        rows = [
            _row(_placement("A", 1), {"name": "mobile"}),
            _row(_placement("B", 2), {"name": "mobile"}),
        ]
        records, mock_get = _collect(
            _hook(limit=2), [_campaign(1)], [_page(rows, total=2, rounded=False)]
        )
        assert len(records) == 2
        assert len(_stat_params(mock_get)) == 1

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
            _page(rows, total=2, rounded=False),
        ]
        _, mock_get = _collect(_hook(limit=2), [_campaign(1), _campaign(2)], pages)
        assert [p["offset"] for p in _stat_params(mock_get)] == [1, 1]


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

    def test_an_answer_without_a_readable_total_warns_and_goes_on(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            records, _ = _collect(_hook(), [_campaign(1)], [_page(rows)])
        assert len(records) == 1
        assert any("unverified" in r.getMessage() for r in caplog.records)

    def test_a_total_that_is_not_a_number_is_no_total_at_all(self, caplog):
        rows = [_row(_placement("A", 1), {"name": "mobile"})]
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            records, _ = _collect(_hook(), [_campaign(1)], [_page(rows, total="5")])
        assert len(records) == 1
        assert any("unverified" in r.getMessage() for r in caplog.records)

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
