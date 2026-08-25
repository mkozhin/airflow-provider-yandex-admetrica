"""Tests for the one request path: what it repeats, what it refuses, what it tells."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import MagicMock, patch

import pytest
import requests
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _BACKOFF_DELAYS,
    _ENDPOINT_URLS,
    _OUTCOME_UNKNOWN,
    _QUERY_ERROR_DELAYS,
    _REQUEST_TIMEOUT,
    _RETRY_AFTER_HEADER,
    _RETRY_AFTER_MAX,
    _TOKEN_REDACTED,
    _TOKEN_SCHEME,
    AdmetricaHook,
    _attempt_reason,
    _retry_after,
    _seconds_until,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

_HOOK_LOGGER = "airflow_provider_yandex_admetrica.hooks.yandex_admetrica"

#: The locators a statistics request names itself by; every test that reaches
#: the request path passes them, so the lines and the events carry a full target.
STAT_FIELDS = {
    "advertiser_id": 17004,
    "campaign_id": 123456,
    "date": "2026-08-20",
    "offset": 1,
}

STAT_PARAMS = {"ids": 123456, "date1": "2026-08-20", "date2": "2026-08-20", "limit": 10000}


class _Sink:
    """A Loki stand-in that records what it was handed."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.pushed: list[dict] = []

    def push(self, event: dict) -> None:
        self.pushed.append(dict(event))


def _hook(*, request_delay: float = 0, loki: _Sink | None = None, **kwargs) -> AdmetricaHook:
    """A hook whose connection is a working one, paced at nothing by default."""
    conn = Connection(
        conn_id="admetrica",
        conn_type="http",
        password=TOKEN,
        extra=json.dumps({"advertiser_id": 17004}),
    )
    hook = AdmetricaHook(
        admetrica_conn_id="admetrica", request_delay=request_delay, loki=loki, **kwargs
    )
    hook.get_connection = MagicMock(return_value=conn)
    return hook


def _response(status: int, payload: object | None = None, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    body = {} if payload is None else payload
    resp.json.return_value = body
    text = json.dumps(body, ensure_ascii=False)
    resp.content = text.encode()
    resp.headers = headers or {}
    return resp


def _ok(rows: list | None = None, **extra) -> MagicMock:
    """A well-formed statistics answer."""
    return _response(200, {"data": rows if rows is not None else [], **extra})


def _delays(mock_sleep: MagicMock) -> list:
    """The delay each ``time.sleep`` call was given, in order."""
    return [call.args[0] for call in mock_sleep.call_args_list]


def _lines(caplog) -> list[str]:
    """The lines unsuccessful attempts left in the task log, in order."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == _HOOK_LOGGER and record.levelno == logging.WARNING
    ]


def _call(hook: AdmetricaHook, endpoint: str = "stat", params: dict | None = None) -> dict:
    fields = STAT_FIELDS if endpoint == "stat" else {"advertiser_id": 17004, "offset": 0}
    return hook._request_page(
        endpoint, STAT_PARAMS if params is None else params, dict(fields)
    )


# ---------------------------------------------------------------------------
# The answer that comes back
# ---------------------------------------------------------------------------


class TestSuccessfulAnswer:
    def test_a_well_formed_body_comes_back_whole(self):
        hook = _hook()
        page = {"data": [{"dimensions": [{"name": "Main"}], "metrics": [1]}], "total_rows": 1}
        with patch("requests.get", return_value=_response(200, page)):
            assert _call(hook) == page

    def test_a_campaign_without_impressions_is_an_answer_like_any_other(self):
        hook = _hook()
        with patch("requests.get", return_value=_ok([])) as get:
            assert _call(hook) == {"data": []}
        assert get.call_count == 1

    def test_the_request_goes_out_with_the_token_the_scheme_and_the_parameters(self):
        hook = _hook()
        with patch("requests.get", return_value=_ok()) as get:
            _call(hook)
        assert get.call_args.args == (_ENDPOINT_URLS["stat"],)
        assert get.call_args.kwargs["params"] == STAT_PARAMS
        assert get.call_args.kwargs["timeout"] == _REQUEST_TIMEOUT
        assert get.call_args.kwargs["headers"]["Authorization"] == f"{_TOKEN_SCHEME} {TOKEN}"

    def test_the_endpoint_label_decides_the_address(self):
        hook = _hook()
        with patch("requests.get", return_value=_response(200, {"campaigns": []})) as get:
            _call(hook, "campaigns", {"advertiser_id": 17004, "limit": 100, "offset": 0})
        assert get.call_args.args == (_ENDPOINT_URLS["campaigns"],)

    def test_a_successful_attempt_leaves_no_line_in_the_task_log(self, caplog):
        hook = _hook()
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=_ok()):
                _call(hook)
        assert _lines(caplog) == []


# ---------------------------------------------------------------------------
# Answers a repeat can fix
# ---------------------------------------------------------------------------


class TestTheLadders:
    def test_both_ladders_are_of_one_length(self):
        """The attempt number is counted off one ladder and indexes either."""
        assert len(_QUERY_ERROR_DELAYS) == len(_BACKOFF_DELAYS)


class TestRetries:
    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 599])
    def test_a_retryable_status_gives_way_to_the_page_that_follows_it(self, status):
        hook = _hook()
        with patch("requests.get", side_effect=[_response(status), _ok()]) as get:
            assert _call(hook) == {"data": []}
        assert get.call_count == 2

    def test_a_429_is_repeated_until_the_attempts_run_out(self):
        hook = _hook()
        with patch("requests.get", return_value=_response(429)) as get:
            with patch("time.sleep") as sleep:
                with pytest.raises(AirflowException, match="returned 429"):
                    _call(hook)
        assert get.call_count == len(_BACKOFF_DELAYS) + 1
        # No pause after the final attempt — it raises instead.
        assert _delays(sleep) == _BACKOFF_DELAYS

    def test_a_5xx_walks_the_whole_ladder(self):
        hook = _hook()
        fails = [_response(503)] * len(_BACKOFF_DELAYS)
        with patch("requests.get", side_effect=fails + [_ok()]):
            with patch("time.sleep") as sleep:
                assert _call(hook) == {"data": []}
        assert _delays(sleep) == _BACKOFF_DELAYS

    def test_a_network_failure_is_repeated_like_a_5xx(self):
        hook = _hook()
        answers = [requests.ConnectionError("no route"), _ok()]
        with patch("requests.get", side_effect=answers) as get:
            with patch("time.sleep") as sleep:
                assert _call(hook) == {"data": []}
        assert get.call_count == 2
        assert _delays(sleep) == [_BACKOFF_DELAYS[0]]

    def test_a_network_failure_that_never_lets_up_ends_the_task(self):
        hook = _hook()
        with patch("requests.get", side_effect=requests.Timeout("timed out")) as get:
            with pytest.raises(AirflowException, match="failed after 4 attempts: timed out"):
                _call(hook)
        assert get.call_count == len(_BACKOFF_DELAYS) + 1

    def test_the_text_of_a_network_failure_carries_no_token(self, caplog):
        """A proxy URL in the failure's own text is a way out the gate closes."""
        hook = _hook()
        failure = requests.ConnectionError(f"proxy https://user:{TOKEN}@proxy.local refused")
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=failure):
                with pytest.raises(AirflowException) as excinfo:
                    _call(hook)
        assert TOKEN not in str(excinfo.value)
        assert _TOKEN_REDACTED in str(excinfo.value)
        assert TOKEN not in "\n".join(_lines(caplog))


# ---------------------------------------------------------------------------
# Retry-After
# ---------------------------------------------------------------------------


class TestRetryAfter:
    def test_a_wait_in_seconds_replaces_the_rung(self):
        hook = _hook()
        refused = _response(429, headers={_RETRY_AFTER_HEADER: "7"})
        with patch("requests.get", side_effect=[refused, _ok()]):
            with patch("time.sleep") as sleep:
                _call(hook)
        assert _delays(sleep) == [7]

    def test_a_wait_written_as_a_date_is_read_too(self):
        hook = _hook()
        moment = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
        refused = _response(503, headers={_RETRY_AFTER_HEADER: moment})
        with patch("requests.get", side_effect=[refused, _ok()]):
            with patch("time.sleep") as sleep:
                _call(hook)
        (delay,) = _delays(sleep)
        assert 0 < delay <= 30

    def test_a_wait_longer_than_the_cap_is_cut_to_it(self):
        hook = _hook()
        refused = _response(429, headers={_RETRY_AFTER_HEADER: "86400"})
        with patch("requests.get", side_effect=[refused, _ok()]):
            with patch("time.sleep") as sleep:
                _call(hook)
        assert _delays(sleep) == [_RETRY_AFTER_MAX]

    @pytest.mark.parametrize("value", ["", "  ", "soon", "0", "-5", "Tue, 99 Zzz 2026"])
    def test_a_header_naming_no_usable_wait_leaves_the_ladder_alone(self, value):
        hook = _hook()
        refused = _response(503, headers={_RETRY_AFTER_HEADER: value})
        with patch("requests.get", side_effect=[refused, _ok()]):
            with patch("time.sleep") as sleep:
                _call(hook)
        assert _delays(sleep) == [_BACKOFF_DELAYS[0]]

    def test_an_answer_without_the_header_asks_for_nothing(self):
        assert _retry_after(_response(429)) is None

    def test_a_response_that_answers_with_code_of_its_own_asks_for_nothing(self):
        hostile = MagicMock()
        type(hostile).headers = property(lambda self: (_ for _ in ()).throw(RuntimeError("nope")))
        assert _retry_after(hostile) is None

    def test_a_date_already_past_is_no_wait_at_all(self):
        moment = format_datetime(datetime.now(timezone.utc) - timedelta(hours=1), usegmt=True)
        assert _seconds_until(moment) < 0

    def test_a_date_without_a_zone_is_read_as_gmt(self):
        ahead = datetime.now(timezone.utc) + timedelta(seconds=30)
        naive = ahead.strftime("%a, %d %b %Y %H:%M:%S")
        seconds = _seconds_until(naive)
        assert 0 < seconds <= 30


# ---------------------------------------------------------------------------
# Answers a repeat cannot fix
# ---------------------------------------------------------------------------


class TestRefusals:
    def test_a_401_fails_at_once(self):
        hook = _hook()
        with patch("requests.get", return_value=_response(401)) as get:
            with patch("time.sleep") as sleep:
                with pytest.raises(AirflowException, match="401 Unauthorized") as excinfo:
                    _call(hook)
        assert get.call_count == 1
        sleep.assert_not_called()
        assert "nothing here refreshes it" in str(excinfo.value)
        assert "'admetrica'" in str(excinfo.value)

    def test_a_400_fails_at_once_and_carries_the_servers_words(self):
        hook = _hook()
        body = {"error": {"code": 400, "message": "dimension am:e:nope is unknown"}}
        with patch("requests.get", return_value=_response(400, body)) as get:
            with pytest.raises(AirflowException) as excinfo:
                _call(hook)
        assert get.call_count == 1
        assert "returned 400" in str(excinfo.value)
        assert "code 400: dimension am:e:nope is unknown" in str(excinfo.value)

    @pytest.mark.parametrize("status", [403, 404, 409, 422])
    def test_every_other_4xx_fails_at_once_too(self, status):
        hook = _hook()
        with patch("requests.get", return_value=_response(status)) as get:
            with pytest.raises(AirflowException, match=f"returned {status}"):
                _call(hook)
        assert get.call_count == 1

    def test_a_body_naming_no_error_is_described_by_its_status_alone(self):
        hook = _hook()
        with patch("requests.get", return_value=_response(400, {"data": []})):
            with pytest.raises(AirflowException) as excinfo:
                _call(hook)
        assert str(excinfo.value).endswith(_ENDPOINT_URLS["stat"])

    def test_a_200_that_is_not_json_fails_at_once(self):
        hook = _hook()
        resp = _response(200)
        resp.json.side_effect = json.JSONDecodeError("Expecting value", "<html>", 0)
        with patch("requests.get", return_value=resp) as get:
            with pytest.raises(AirflowException, match="is not JSON"):
                _call(hook)
        assert get.call_count == 1

    @pytest.mark.parametrize(
        ("payload", "kind"),
        [
            ({"total_rows": 0}, "rows_absent"),
            ({"data": "nope"}, "rows_non_list"),
            ({"data": ["nope"]}, "dict"),
            ([], "non_dict"),
        ],
    )
    def test_a_200_with_no_readable_rows_fails_at_once(self, payload, kind):
        hook = _hook()
        with patch("requests.get", return_value=_response(200, payload)) as get:
            with pytest.raises(AirflowException, match=f"payload_kind={kind}"):
                _call(hook)
        assert get.call_count == 1

    def test_a_200_carrying_a_refusal_instead_of_rows_says_what_it_carried(self):
        hook = _hook()
        body = {"errors": [{"code": 429, "message": "quota exceeded"}]}
        with patch("requests.get", return_value=_response(200, body)):
            with pytest.raises(AirflowException) as excinfo:
                _call(hook)
        assert "no readable rows" in str(excinfo.value)
        assert "code 429: quota exceeded" in str(excinfo.value)

    def test_a_refusal_echoing_the_token_is_told_without_it(self, caplog):
        hook = _hook()
        body = {"error": {"code": 401, "message": f"header was 'OAuth {TOKEN}'"}}
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=_response(401, body)):
                with pytest.raises(AirflowException) as excinfo:
                    _call(hook)
        assert TOKEN not in str(excinfo.value)
        assert _TOKEN_REDACTED in str(excinfo.value)
        assert TOKEN not in "\n".join(_lines(caplog))


# ---------------------------------------------------------------------------
# The chronicle in the task log
# ---------------------------------------------------------------------------


class TestAttemptLines:
    def test_every_unsuccessful_attempt_leaves_exactly_one_line(self, caplog):
        hook = _hook()
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=_response(503)):
                with pytest.raises(AirflowException):
                    _call(hook)
        lines = _lines(caplog)
        assert len(lines) == len(_BACKOFF_DELAYS) + 1
        for number, line in enumerate(lines, start=1):
            assert f"attempt {number}/4 failed" in line

    def test_a_pause_is_promised_only_where_one_follows(self, caplog):
        hook = _hook()
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=_response(503)):
                with pytest.raises(AirflowException):
                    _call(hook)
        lines = _lines(caplog)
        assert [line for line in lines if "Retrying in" in line] == lines[:-1]
        assert "Retrying in" not in lines[-1]

    def test_the_promised_pause_is_the_one_actually_taken(self, caplog):
        hook = _hook()
        refused = _response(429, headers={_RETRY_AFTER_HEADER: "7"})
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=[refused, _ok()]):
                with patch("time.sleep") as sleep:
                    _call(hook)
        assert "Retrying in 7" in _lines(caplog)[0]
        assert _delays(sleep) == [7]

    def test_the_line_names_the_request_it_was_made_for(self, caplog):
        hook = _hook()
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=[_response(503), _ok()]):
                _call(hook)
        (line,) = _lines(caplog)
        assert line.startswith(
            "AdMetrica stat campaign_id=123456 date=2026-08-20 offset=1: attempt 1/4 failed — "
        )
        assert line.endswith("HTTP 503. Retrying in 1 s")

    def test_a_campaign_list_line_claims_no_day_and_no_campaign(self, caplog):
        hook = _hook()
        answers = [_response(503), _response(200, {"campaigns": []})]
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=answers):
                _call(hook, "campaigns", {"advertiser_id": 17004, "limit": 100, "offset": 0})
        (line,) = _lines(caplog)
        assert line.startswith("AdMetrica campaigns offset=0: ")
        assert "date=" not in line
        assert "campaign_id=" not in line

    def test_a_network_failure_reads_as_no_response_and_names_its_type(self):
        event = {
            "http_status": None,
            "outcome": "network_error",
            "error_code": None,
            "error_type": None,
            "error_message": None,
            "payload_kind": None,
            "exception_type": "ConnectionError",
            "exception_message": None,
        }
        assert _attempt_reason(event) == "no response, ConnectionError"

    def test_a_broken_document_reads_as_the_position_it_broke_at(self):
        event = {
            "http_status": 200,
            "outcome": "invalid_json",
            "error_code": None,
            "error_type": None,
            "error_message": None,
            "payload_kind": None,
            "exception_type": "JSONDecodeError",
            "exception_message": "Expecting value: line 1 column 1 (char 0)",
        }
        reason = _attempt_reason(event)
        assert reason == "HTTP 200, invalid JSON (Expecting value: line 1 column 1 (char 0))"


# ---------------------------------------------------------------------------
# The event every attempt leaves behind
# ---------------------------------------------------------------------------


class TestDiagnostics:
    def test_every_attempt_pushes_exactly_one_event(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=_response(503)):
            with pytest.raises(AirflowException):
                _call(hook)
        assert len(sink.pushed) == len(_BACKOFF_DELAYS) + 1
        assert [event["attempt"] for event in sink.pushed] == [1, 2, 3, 4]
        assert [event["outcome"] for event in sink.pushed] == ["retryable_error"] * 4

    def test_a_repeat_still_to_come_is_a_warning_and_the_last_one_an_error(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=_response(503)):
            with pytest.raises(AirflowException):
                _call(hook)
        assert [event["level"] for event in sink.pushed] == ["warn", "warn", "warn", "error"]

    def test_a_successful_attempt_keeps_its_body_inside_the_process(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=_ok()):
            _call(hook)
        (event,) = sink.pushed
        assert event["outcome"] == "success"
        assert event["level"] == "info"
        assert event["response_body"] is None
        assert event["http_status"] == 200
        assert event["duration_ms"] is not None

    def test_a_failure_travels_with_its_body(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        body = {"error": {"code": 400, "message": "bad"}}
        with patch("requests.get", return_value=_response(400, body)):
            with pytest.raises(AirflowException):
                _call(hook)
        (event,) = sink.pushed
        assert event["outcome"] == "http_error"
        assert event["error_code"] == 400
        assert event["error_message"] == "bad"
        assert "bad" in event["response_body"]

    def test_the_event_names_the_request_it_describes(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=_ok()):
            _call(hook)
        (event,) = sink.pushed
        assert event["endpoint"] == "stat"
        assert event["request_url"] == _ENDPOINT_URLS["stat"]
        assert event["request_method"] == "GET"
        assert event["request_params"] == STAT_PARAMS
        assert event["advertiser_id"] == 17004
        assert event["campaign_id"] == 123456
        assert event["date"] == "2026-08-20"
        assert event["offset"] == 1

    def test_a_rate_limited_answer_carries_what_it_said_about_the_limit(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        headers = {"X-RateLimit-Limit": "100", "X-RateLimit-Remaining": "0"}
        with patch("requests.get", side_effect=[_response(429, headers=headers), _ok()]):
            _call(hook)
        assert sink.pushed[0]["rate_limit_limit"] == "100"
        assert sink.pushed[0]["rate_limit_remaining"] == "0"

    @pytest.mark.parametrize(
        "make_answer",
        [
            lambda: _response(401, {"error": {"code": 401, "message": f"OAuth {TOKEN}"}}),
            lambda: _response(500, {"message": f"upstream sent OAuth {TOKEN}"}),
            lambda: _response(200, {"data": "nope", "hint": f"OAuth {TOKEN}"}),
        ],
        ids=["auth_error", "retryable_error", "empty_shape"],
    )
    def test_no_pushed_event_of_any_failure_spells_the_token_out(self, make_answer):
        """The answer is built here: a shared mock accumulates calls between tests."""
        sink = _Sink()
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=make_answer()):
            with pytest.raises(AirflowException):
                _call(hook)
        assert sink.pushed
        for event in sink.pushed:
            assert TOKEN not in json.dumps(event, ensure_ascii=False, default=str)
            assert event["request_headers"]["Authorization"].startswith(_TOKEN_SCHEME)

    def test_a_sink_whose_breaker_has_tripped_is_handed_nothing(self):
        sink = _Sink(enabled=False)
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=_ok()):
            _call(hook)
        assert sink.pushed == []

    def test_a_sink_that_raises_never_reaches_the_caller(self):
        sink = _Sink()
        sink.push = MagicMock(side_effect=RuntimeError("loki is down"))
        hook = _hook(loki=sink)
        with patch("requests.get", return_value=_ok()):
            assert _call(hook) == {"data": []}

    def test_without_a_sink_no_event_is_built_at_all(self):
        hook = _hook()
        with patch("requests.get", return_value=_ok()):
            with patch(
                "airflow_provider_yandex_admetrica.hooks.yandex_admetrica._emit_event"
            ) as emit:
                _call(hook)
        emit.assert_not_called()


# ---------------------------------------------------------------------------
# The pace between requests
# ---------------------------------------------------------------------------


class TestPace:
    def test_the_first_request_of_a_task_waits_for_nothing(self):
        hook = _hook(request_delay=0.5)
        with patch("requests.get", return_value=_ok()):
            with patch("time.sleep") as sleep:
                _call(hook)
        sleep.assert_not_called()

    def test_every_request_after_it_waits_out_the_delay(self):
        hook = _hook(request_delay=0.5)
        with patch("requests.get", return_value=_ok()):
            with patch("time.sleep") as sleep:
                _call(hook)
                _call(hook)
                _call(hook)
        assert _delays(sleep) == [0.5, 0.5]

    def test_a_repeat_waits_out_its_rung_and_nothing_more(self):
        hook = _hook(request_delay=0.5)
        with patch("requests.get", side_effect=[_response(503), _ok()]):
            with patch("time.sleep") as sleep:
                _call(hook)
        assert _delays(sleep) == [_BACKOFF_DELAYS[0]]

    def test_a_pace_of_nothing_costs_no_call_at_all(self):
        hook = _hook(request_delay=0)
        with patch("requests.get", return_value=_ok()):
            with patch("time.sleep") as sleep:
                _call(hook)
                _call(hook)
        sleep.assert_not_called()


# ---------------------------------------------------------------------------
# The safety net around an attempt
# ---------------------------------------------------------------------------


class TestAnAttemptThatFailedInAnUnforeseenWay:
    """Whatever escapes the branches is classified, and nothing is left "unknown"."""

    def test_an_unexpected_exception_is_pushed_as_such_and_reworded(self):
        sink = _Sink()
        hook = _hook(loki=sink)
        boom = AttributeError("a response object of an unforeseen shape")

        with patch("requests.get", side_effect=boom):
            with pytest.raises(AirflowException) as excinfo:
                _call(hook)

        assert "AttributeError" in str(excinfo.value)
        assert "a response object of an unforeseen shape" in str(excinfo.value)
        (event,) = sink.pushed
        assert event["outcome"] == "unexpected_error"
        assert event["exception_type"] == "AttributeError"
        assert event["level"] == "error"

    def test_an_unexpected_exception_is_raised_with_nothing_attached(self):
        """The original would print its own text into the task log, past the gate."""
        hook = _hook()

        with patch("requests.get", side_effect=AttributeError(f"OAuth {TOKEN}")):
            with pytest.raises(AirflowException) as excinfo:
                _call(hook)

        assert excinfo.value.__cause__ is None
        assert excinfo.value.__suppress_context__ is True
        assert TOKEN not in str(excinfo.value)
        assert _TOKEN_REDACTED in str(excinfo.value)

    def test_a_timeout_of_the_task_travels_unchanged(self):
        """A stop Airflow itself raised is the caller's own signal, not a failure."""
        hook = _hook()
        stop = AirflowTaskTimeout("Timeout, PID: 4242")

        with patch("requests.get", side_effect=stop):
            with pytest.raises(AirflowTaskTimeout) as excinfo:
                _call(hook)

        assert excinfo.value is stop

    def test_an_interrupted_attempt_is_neither_logged_nor_pushed(self, caplog):
        """A stop of the task itself must not be held for the length of a push."""
        sink = _Sink()
        hook = _hook(loki=sink)
        stop = KeyboardInterrupt()

        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", side_effect=stop):
                with pytest.raises(KeyboardInterrupt) as excinfo:
                    _call(hook)

        assert excinfo.value is stop
        assert sink.pushed == []
        assert [r for r in caplog.records if r.name == _HOOK_LOGGER] == []

    def test_the_placeholder_outcome_never_reaches_the_sink(self):
        sink = _Sink()
        hook = _hook(loki=sink)

        with patch("requests.get", side_effect=AttributeError("boom")):
            with pytest.raises(AirflowException):
                _call(hook)

        assert all(event["outcome"] != _OUTCOME_UNKNOWN for event in sink.pushed)
