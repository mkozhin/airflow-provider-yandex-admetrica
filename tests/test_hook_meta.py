"""Tests for the connection check and for how often the connection is read."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import requests
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _ENDPOINT_URLS,
    _TOKEN_REDACTED,
    AdmetricaHook,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

ADVERTISER_ID = 17004

DATE = "2026-08-20"

DIMENSIONS = ["am:e:placement"]
METRICS = ["am:e:renders"]


def _connection(**overrides) -> Connection:
    fields = {
        "conn_id": "admetrica",
        "conn_type": "http",
        "password": TOKEN,
        "extra": json.dumps({"advertiser_id": ADVERTISER_ID}),
    }
    fields.update(overrides)
    return Connection(**fields)


def _hook(conn: Connection | None = None, **kwargs) -> AdmetricaHook:
    """A hook whose connection is the given one, paced at nothing."""
    hook = AdmetricaHook(admetrica_conn_id="admetrica", request_delay=0, **kwargs)
    hook.get_connection = MagicMock(return_value=conn if conn is not None else _connection())
    return hook


def _response(payload: object, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.content = json.dumps(payload, ensure_ascii=False).encode()
    resp.headers = {}
    return resp


def _campaign(campaign_id: int) -> dict:
    return {
        "campaign_id": campaign_id,
        "name": f"Campaign {campaign_id}",
        "date_start": "2026-01-01",
        "date_end": "2026-12-31",
        "advertiser_id": ADVERTISER_ID,
        "advertiser_name": "Advertiser",
        "status": "active",
    }


def _campaign_page(rows: list[dict]) -> MagicMock:
    return _response({"campaigns": rows, "total": len(rows)})


def _stat_page(rows: list[dict]) -> MagicMock:
    return _response({"data": rows, "total_rows": len(rows), "total_rows_rounded": False})


def _row(name: str) -> dict:
    return {"dimensions": [{"name": name, "id": 1}], "metrics": [1]}


# ---------------------------------------------------------------------------
# The Test Connection button
# ---------------------------------------------------------------------------


class TestConnectionCheckSucceeds:
    def test_a_working_connection_answers_true(self):
        hook = _hook()
        with patch("requests.get", return_value=_campaign_page([_campaign(1)])):
            ok, message = hook.test_connection()
        assert ok is True
        assert isinstance(message, str)

    def test_the_message_names_the_advertiser_and_the_campaigns(self):
        hook = _hook()
        page = _campaign_page([_campaign(1), _campaign(2)])
        with patch("requests.get", return_value=page):
            _, message = hook.test_connection()
        assert str(ADVERTISER_ID) in message
        assert "2" in message

    def test_the_check_costs_one_request_to_the_campaigns_endpoint(self):
        hook = _hook()
        with patch("requests.get", return_value=_campaign_page([_campaign(1)])) as mock_get:
            hook.test_connection()
        assert mock_get.call_count == 1
        assert mock_get.call_args.args[0] == _ENDPOINT_URLS["campaigns"]

    def test_an_advertiser_without_campaigns_still_answers_true(self):
        hook = _hook()
        with patch("requests.get", return_value=_campaign_page([])):
            ok, _ = hook.test_connection()
        assert ok is True


class TestConnectionCheckFails:
    def test_a_refused_token_answers_false_with_the_reason(self):
        hook = _hook()
        body = {"errors": [{"error_type": "invalid_token", "message": "Invalid oauth token"}]}
        with patch("requests.get", return_value=_response(body, status=401)):
            ok, message = hook.test_connection()
        assert ok is False
        assert "401" in message

    def test_an_extra_without_an_advertiser_answers_false_before_any_request(self):
        hook = _hook(_connection(extra="{}"))
        with patch("requests.get") as mock_get:
            ok, message = hook.test_connection()
        assert ok is False
        assert "advertiser_id" in message
        assert mock_get.call_count == 0

    def test_a_connection_without_a_password_answers_false(self):
        hook = _hook(_connection(password=None))
        with patch("requests.get") as mock_get:
            ok, message = hook.test_connection()
        assert ok is False
        assert "token" in message.lower()
        assert mock_get.call_count == 0

    def test_a_connection_airflow_cannot_find_answers_false(self):
        hook = AdmetricaHook(admetrica_conn_id="missing", request_delay=0)
        hook.get_connection = MagicMock(side_effect=AirflowException("The conn_id is not defined"))
        ok, message = hook.test_connection()
        assert ok is False
        assert "not defined" in message

    def test_a_network_failure_answers_false_rather_than_raising(self):
        hook = _hook()
        with patch("requests.get", side_effect=requests.ConnectionError("dial tcp: refused")):
            ok, message = hook.test_connection()
        assert ok is False
        assert message

    def test_a_short_campaign_page_answers_false_with_the_count(self):
        hook = _hook()
        page = _response({"campaigns": [_campaign(1)], "total": 5})
        with patch("requests.get", return_value=page):
            ok, message = hook.test_connection()
        assert ok is False
        assert "5" in message


class TestConnectionCheckKeepsTheToken:
    def test_a_reflected_token_never_reaches_the_button(self):
        hook = _hook()
        body = {"errors": [{"error_type": "invalid_token", "message": f"OAuth {TOKEN} refused"}]}
        with patch("requests.get", return_value=_response(body, status=401)):
            ok, message = hook.test_connection()
        assert ok is False
        assert TOKEN not in message
        assert _TOKEN_REDACTED in message

    def test_a_token_in_the_text_of_a_network_failure_is_cut_out(self):
        hook = _hook()
        failure = requests.ConnectionError(f"proxy https://user:{TOKEN}@proxy.local refused")
        with patch("requests.get", side_effect=failure):
            _, message = hook.test_connection()
        assert TOKEN not in message

    def test_a_failure_that_spells_the_token_out_leaves_only_the_type(self):
        hook = _hook()
        spelled = "\x00".join(TOKEN)
        with patch("requests.get", side_effect=requests.ConnectionError(spelled)):
            ok, message = hook.test_connection()
        assert ok is False
        assert TOKEN not in message
        assert message.endswith("<ConnectionError>")


# ---------------------------------------------------------------------------
# How often the connection is read
# ---------------------------------------------------------------------------


class TestConnectionIsReadOnce:
    def test_a_day_of_many_requests_reads_the_connection_once(self):
        hook = _hook()
        pages = [
            _campaign_page([_campaign(1), _campaign(2)]),
            _stat_page([_row("A")]),
            _stat_page([_row("B")]),
        ]
        with patch("requests.get", side_effect=pages) as mock_get:
            hook.get_stats(DATE, DIMENSIONS, METRICS)
        assert mock_get.call_count == 3
        assert hook.get_connection.call_count == 1

    def test_repeated_reads_of_the_advertiser_ask_airflow_once(self):
        hook = _hook()
        assert hook.advertiser_id == ADVERTISER_ID
        assert hook.advertiser_id == ADVERTISER_ID
        assert hook.get_connection.call_count == 1

    def test_the_check_and_the_export_that_follows_share_one_read(self):
        hook = _hook()
        pages = [_campaign_page([_campaign(1)]), _stat_page([_row("A")])]
        with patch("requests.get", side_effect=pages):
            hook.test_connection()
            hook.get_stats(DATE, DIMENSIONS, METRICS)
        assert hook.get_connection.call_count == 1

    def test_a_connection_airflow_cannot_find_is_asked_for_once(self):
        hook = AdmetricaHook(admetrica_conn_id="missing", request_delay=0)
        hook.get_connection = MagicMock(side_effect=AirflowException("The conn_id is not defined"))
        hook.test_connection()
        assert hook.get_connection.call_count == 1
