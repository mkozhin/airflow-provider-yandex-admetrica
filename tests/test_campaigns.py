"""Tests for the campaign list: what is asked for, what is kept, when it fails."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _CAMPAIGN_FIELDS,
    _CAMPAIGNS_LIMIT,
    _ENDPOINT_URLS,
    AdmetricaHook,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

ADVERTISER_ID = 17004


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


def _campaign(campaign_id: int, **overrides) -> dict:
    """A campaign as the management API returns it, measures included."""
    row = {
        "campaign_id": campaign_id,
        "name": f"Campaign {campaign_id}",
        "date_start": "2026-01-01",
        "date_end": "2026-12-31",
        "advertiser_id": ADVERTISER_ID,
        "advertiser_name": "Advertiser",
        "status": "active",
        "days_left": 100,
        "renders": 12345,
        "conversions": 7,
        "conversion": 0,
        "cost": 1000.5,
        "permission": "edit",
    }
    row.update(overrides)
    return row


def _response(payload: object, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.content = json.dumps(payload, ensure_ascii=False).encode()
    resp.headers = {}
    return resp


def _page(rows: list[dict], total: object = None) -> MagicMock:
    body: dict = {"campaigns": rows}
    if total is not None:
        body["total"] = total
    return _response(body)


def _params(mock_get: MagicMock) -> list[dict]:
    """The query parameters of each request, in order."""
    return [call.kwargs["params"] for call in mock_get.call_args_list]


# ---------------------------------------------------------------------------
# What comes back
# ---------------------------------------------------------------------------


class TestOnePage:
    def test_a_single_page_is_the_whole_list(self):
        hook = _hook()
        rows = [_campaign(1), _campaign(2)]
        with patch("requests.get", return_value=_page(rows, total=2)) as mock_get:
            campaigns = hook.get_campaigns()
        assert [c["campaign_id"] for c in campaigns] == [1, 2]
        assert mock_get.call_count == 1

    def test_the_campaigns_endpoint_is_the_one_asked(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=1)) as mock_get:
            hook.get_campaigns()
        assert mock_get.call_args.args[0] == _ENDPOINT_URLS["campaigns"]

    def test_an_advertiser_without_campaigns_gives_an_empty_list(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([], total=0)):
            assert hook.get_campaigns() == []

    def test_the_record_holds_the_named_fields_in_order(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=1)):
            (record,) = hook.get_campaigns()
        assert tuple(record) == _CAMPAIGN_FIELDS

    def test_values_arrive_as_the_api_worded_them(self):
        hook = _hook()
        row = _campaign(1, name="Летняя кампания", status="archived", date_end="2026-03-01")
        with patch("requests.get", return_value=_page([row], total=1)):
            (record,) = hook.get_campaigns()
        assert record == {
            "campaign_id": 1,
            "name": "Летняя кампания",
            "status": "archived",
            "date_start": "2026-01-01",
            "date_end": "2026-03-01",
            "advertiser_id": ADVERTISER_ID,
            "advertiser_name": "Advertiser",
        }

    def test_a_field_the_answer_left_out_is_present_and_empty(self):
        hook = _hook()
        row = _campaign(1)
        del row["advertiser_name"]
        with patch("requests.get", return_value=_page([row], total=1)):
            (record,) = hook.get_campaigns()
        assert tuple(record) == _CAMPAIGN_FIELDS
        assert record["advertiser_name"] is None

    def test_the_hook_stamps_no_snapshot_date(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=1)):
            (record,) = hook.get_campaigns()
        assert "snapshot_date" not in record


class TestEveryStatusIsAsked:
    def test_no_status_filter_goes_out(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=1)) as mock_get:
            hook.get_campaigns()
        assert "status" not in mock_get.call_args.kwargs["params"]

    def test_an_archived_campaign_is_in_the_result(self):
        hook = _hook()
        rows = [_campaign(1), _campaign(2, status="archived")]
        with patch("requests.get", return_value=_page(rows, total=2)):
            campaigns = hook.get_campaigns()
        assert [c["status"] for c in campaigns] == ["active", "archived"]


# ---------------------------------------------------------------------------
# Walking the list
# ---------------------------------------------------------------------------


class TestPagination:
    def test_several_pages_are_collected_in_order(self):
        hook = _hook()
        first = [_campaign(i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]
        second = [_campaign(_CAMPAIGNS_LIMIT + 1), _campaign(_CAMPAIGNS_LIMIT + 2)]
        total = len(first) + len(second)
        pages = [_page(first, total=total), _page(second, total=total)]
        with patch("requests.get", side_effect=pages) as mock_get:
            campaigns = hook.get_campaigns()
        assert mock_get.call_count == 2
        assert [c["campaign_id"] for c in campaigns] == list(range(1, total + 1))

    def test_limit_and_offset_go_out_with_every_request(self):
        hook = _hook()
        first = [_campaign(i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]
        second = [_campaign(_CAMPAIGNS_LIMIT + 1)]
        total = len(first) + len(second)
        pages = [_page(first, total=total), _page(second, total=total)]
        with patch("requests.get", side_effect=pages) as mock_get:
            hook.get_campaigns()
        sent = _params(mock_get)
        assert [p["limit"] for p in sent] == [_CAMPAIGNS_LIMIT, _CAMPAIGNS_LIMIT]
        assert [p["offset"] for p in sent] == [0, _CAMPAIGNS_LIMIT]
        assert {p["advertiser_id"] for p in sent} == {ADVERTISER_ID}

    def test_the_offset_counts_the_rows_skipped_and_starts_at_zero(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=1)) as mock_get:
            hook.get_campaigns()
        assert mock_get.call_args.kwargs["params"]["offset"] == 0

    def test_a_reached_total_on_a_full_page_still_asks_for_the_page_after_it(self):
        """The list ends where the answer runs short, not where its count is met."""
        hook = _hook()
        rows = [_campaign(i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]
        pages = [_page(rows, total=_CAMPAIGNS_LIMIT), _page([], total=_CAMPAIGNS_LIMIT)]
        with patch("requests.get", side_effect=pages) as mock_get:
            campaigns = hook.get_campaigns()
        assert mock_get.call_count == 2
        assert len(campaigns) == _CAMPAIGNS_LIMIT


# ---------------------------------------------------------------------------
# What fails
# ---------------------------------------------------------------------------


class TestCompleteness:
    def test_a_short_page_before_the_total_is_closed_fails_the_task(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=5)):
            with pytest.raises(AirflowException) as excinfo:
                hook.get_campaigns()
        message = str(excinfo.value)
        assert "1 campaigns" in message
        assert "5" in message

    def test_an_answer_without_a_readable_total_fails_the_task(self):
        """A list nothing can be checked against is a list nothing may trust."""
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)])):
            with pytest.raises(AirflowException, match="no readable total"):
                hook.get_campaigns()

    def test_a_total_that_is_not_a_number_is_no_total_at_all(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total="5")):
            with pytest.raises(AirflowException, match="no readable total"):
                hook.get_campaigns()

    def test_a_total_that_changes_between_pages_fails_the_task(self):
        """Two answers about the list's length, one of which would stop the walk."""
        first = [_campaign(i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]
        second = [_campaign(_CAMPAIGNS_LIMIT + 1)]
        pages = [_page(first, total=_CAMPAIGNS_LIMIT), _page(second, total=_CAMPAIGNS_LIMIT + 1)]
        with patch("requests.get", side_effect=pages):
            with pytest.raises(AirflowException, match="on an earlier page"):
                _hook().get_campaigns()

    def test_a_growing_total_is_read_before_the_walk_stops_on_the_stale_one(self):
        """The page that changes the count is a full one, so the walk reaches it."""
        first = [_campaign(i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]
        second = [_campaign(_CAMPAIGNS_LIMIT + i) for i in range(1, _CAMPAIGNS_LIMIT + 1)]
        pages = [
            _page(first, total=_CAMPAIGNS_LIMIT),
            _page(second, total=2 * _CAMPAIGNS_LIMIT),
        ]
        with patch("requests.get", side_effect=pages) as mock_get:
            with pytest.raises(AirflowException, match="on an earlier page"):
                _hook().get_campaigns()
        assert mock_get.call_count == 2


class TestUnreadableAnswer:
    def test_a_body_without_the_campaigns_key_fails_the_task(self):
        hook = _hook()
        with patch("requests.get", return_value=_response({"total": 2})):
            with pytest.raises(AirflowException) as excinfo:
                hook.get_campaigns()
        assert "campaigns" in str(excinfo.value)

    def test_a_campaigns_value_that_is_not_a_list_fails_the_task(self):
        hook = _hook()
        with patch("requests.get", return_value=_response({"campaigns": "нет", "total": 1})):
            with pytest.raises(AirflowException):
                hook.get_campaigns()

    def test_a_failed_fetch_leaves_nothing_cached(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=5)):
            with pytest.raises(AirflowException):
                hook.get_campaigns()
        rows = [_campaign(1), _campaign(2), _campaign(3), _campaign(4), _campaign(5)]
        with patch("requests.get", return_value=_page(rows, total=5)) as mock_get:
            assert len(hook.get_campaigns()) == 5
        assert mock_get.call_count == 1

    def test_a_connection_without_an_advertiser_fails_before_the_request(self):
        conn = Connection(conn_id="admetrica", conn_type="http", password=TOKEN, extra="{}")
        hook = AdmetricaHook(admetrica_conn_id="admetrica", request_delay=0)
        hook.get_connection = MagicMock(return_value=conn)
        with patch("requests.get") as mock_get:
            with pytest.raises(AirflowException, match="advertiser"):
                hook.get_campaigns()
        assert mock_get.call_count == 0


# ---------------------------------------------------------------------------
# The list is fetched once
# ---------------------------------------------------------------------------


class TestCaching:
    def test_a_second_call_makes_no_second_request(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([_campaign(1)], total=1)) as mock_get:
            first = hook.get_campaigns()
            second = hook.get_campaigns()
        assert mock_get.call_count == 1
        assert first == second

    def test_an_empty_list_is_cached_too(self):
        hook = _hook()
        with patch("requests.get", return_value=_page([], total=0)) as mock_get:
            hook.get_campaigns()
            assert hook.get_campaigns() == []
        assert mock_get.call_count == 1
