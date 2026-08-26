"""The token never leaves the process, by any channel, under any failure.

Every other test file checks one gate on its own.  This one drives whole
exports — the campaign list, a day of statistics, the connection test — against
answers that plant the credential wherever a server, a proxy or the network can
put it, and then reads back *everything* the export let out: each line of the
task log at any level, each event handed to the diagnostics sink, and the
exception that ended it — its own text, the text of every exception it carries
as cause or context, and the traceback Airflow prints for it into the task log.
A channel added later that forgets the gate fails here even when its own file
has nothing to say about tokens.
"""

from __future__ import annotations

import json
import logging
import traceback
from unittest.mock import MagicMock, patch

import pytest
import requests
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _TOKEN_REDACTED,
    _TOKEN_SCHEME,
    AdmetricaHook,
)

#: A token long enough to be described by its ends rather than replaced whole,
#: so a leak of the middle is a leak of something the mask would never show.
TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

#: The middle alone, which no masked form of the token ever spells out: an
#: assertion on it catches a partial leak that the ends of the mask would hide.
SECRET_MIDDLE = "MIDDLE-OF-THE-SECRET"

_HOOK_LOGGER = "airflow_provider_yandex_admetrica.hooks.yandex_admetrica"

DIMENSIONS = ["am:e:placement"]
METRICS = ["am:e:renders"]
DATE = "2026-08-20"


class _Sink:
    """A Loki stand-in that keeps every event it was handed."""

    def __init__(self) -> None:
        self.enabled = True
        self.pushed: list[dict] = []

    def push(self, event: dict) -> None:
        self.pushed.append(dict(event))


def _hook(sink: _Sink) -> AdmetricaHook:
    conn = Connection(
        conn_id="admetrica",
        conn_type="http",
        password=TOKEN,
        extra=json.dumps({"advertiser_id": 17004}),
    )
    hook = AdmetricaHook(admetrica_conn_id="admetrica", request_delay=0, loki=sink)
    hook.get_connection = MagicMock(return_value=conn)
    return hook


def _response(status: int, payload: object, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.content = json.dumps(payload, ensure_ascii=False).encode()
    resp.headers = headers or {}
    return resp


def _raw_response(status: int, body: bytes, headers: dict | None = None) -> MagicMock:
    """An answer whose body is not JSON at all, as a proxy's error page is."""
    resp = MagicMock()
    resp.status_code = status
    resp.json.side_effect = ValueError("No JSON object could be decoded")
    resp.content = body
    resp.headers = headers or {}
    return resp


def _run(answer, caplog, *, campaigns_first: bool = False) -> tuple[_Sink, str]:
    """Export a day against *answer* and return the sink and the failure text.

    The campaign list is served ready unless *campaigns_first* says the answer
    under test is the list's own, so that a statistics scenario reaches the
    statistics endpoint rather than failing on the way to it.
    """
    sink = _Sink()
    hook = _hook(sink)
    if not campaigns_first:
        hook._campaigns = [{"campaign_id": 123456, "name": "Campaign"}]
    failure: BaseException | None = None
    with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
        with patch("requests.get", **answer):
            try:
                hook.get_stats(DATE, DIMENSIONS, METRICS)
            except (AirflowException, ValueError) as exc:
                failure = exc
    return sink, failure


def _failure_text(failure: str | BaseException | None) -> str:
    """Everything a failure spells out on its way to the task log.

    An exception reaches the log as the traceback Airflow formats for it, and
    that traceback carries the text of every exception attached to it as cause
    or context as well as its own: an original scrubbed into a new message still
    prints itself in full when it travels along as ``__cause__``.  The chain is
    walked in its own right too, so that an exception the formatter renders
    poorly is searched anyway — along the same links a formatter follows, since
    a context the raise explicitly dropped is one nothing prints.
    """
    if failure is None:
        return ""
    if isinstance(failure, str):
        return failure
    parts = ["".join(traceback.format_exception(failure))]
    seen: list[int] = []
    exc: BaseException | None = failure
    while exc is not None and id(exc) not in seen:
        seen.append(id(exc))
        parts.append(str(exc))
        parts.extend(str(arg) for arg in exc.args)
        if exc.__cause__ is not None:
            exc = exc.__cause__
        else:
            exc = None if exc.__suppress_context__ else exc.__context__
    return "\n".join(parts)


def _everything_that_left(sink: _Sink, failure: str | BaseException | None, caplog) -> str:
    """Every channel out of the process, as one text to search.

    The task log at every level, not only the warnings an attempt writes: an
    INFO line about a lag and a DEBUG line about diagnostics are as much a way
    out as a WARNING.  The events are dumped whole, keys and values alike, so a
    field added to the schema is searched without this test being told about it.
    """
    lines = [
        record.getMessage()
        for record in caplog.records
        if record.name == _HOOK_LOGGER
    ]
    events = json.dumps(sink.pushed, ensure_ascii=False, default=str)
    return "\n".join([*lines, events, _failure_text(failure)])


#: One entry per way an export can fail, with the credential planted where that
#: failure would carry it: inside a refusal the server worded, inside a body no
#: one can parse, inside a header, inside the text of a network failure, and
#: inside the values a successful answer says about its own numbers.
#:
#: Each entry is a factory rather than an answer: half a dozen tests run every
#: scenario, and one shared mock would accumulate their calls between them while
#: one shared exception would grow a chain of contexts as it was re-raised.
SCENARIOS = {
    "401 wording its refusal with the header it read": lambda: {
        "return_value": _response(
            401, {"error": {"code": 401, "message": f"bad credential {_TOKEN_SCHEME} {TOKEN}"}}
        )
    },
    "400 wording its refusal with the header it read": lambda: {
        "return_value": _response(
            400, {"error": {"code": 400, "message": f"unparsed {_TOKEN_SCHEME} {TOKEN}"}}
        )
    },
    "400 naming the kind of its refusal with the header it read": lambda: {
        "return_value": _response(
            400,
            {
                "errors": [
                    {
                        "error_type": f"{_TOKEN_SCHEME} {TOKEN}",
                        "message": "Query is too complicated.",
                    }
                ],
                "code": 400,
            },
        )
    },
    "403 wording its refusal with the header it read": lambda: {
        "return_value": _response(403, {"error": {"message": f"{_TOKEN_SCHEME} {TOKEN}"}})
    },
    "500 repeated until the attempts run out": lambda: {
        "return_value": _response(500, {"message": f"upstream saw {_TOKEN_SCHEME} {TOKEN}"})
    },
    "429 naming its wait and echoing the header": lambda: {
        "return_value": _response(
            429,
            {"error": {"code": 429, "message": f"quota for {TOKEN}"}},
            {"Retry-After": "1", "X-RateLimit-Limit": TOKEN, "X-RateLimit-Remaining": "0"},
        )
    },
    "a 200 that is not JSON at all": lambda: {
        "return_value": _raw_response(
            200, f"<html>proxy saw {_TOKEN_SCHEME} {TOKEN}</html>".encode()
        )
    },
    "a proxy error page in place of a refusal": lambda: {
        "return_value": _raw_response(
            502, f"<html>proxy saw {_TOKEN_SCHEME} {TOKEN}</html>".encode()
        )
    },
    "a 200 carrying no readable rows": lambda: {
        "return_value": _response(
            200, {"data": "not a list", "hint": f"{_TOKEN_SCHEME} {TOKEN}"}
        )
    },
    "a network failure naming the proxy it dialled": lambda: {
        "side_effect": requests.ConnectionError(
            f"proxy https://user:{TOKEN}@proxy.local refused the connection"
        )
    },
    "a failure of an unforeseen kind carrying the header it read": lambda: {
        "side_effect": AttributeError(f"no such attribute on {_TOKEN_SCHEME} {TOKEN}")
    },
    "an AirflowException from below naming the proxy it dialled": lambda: {
        # The class the whole scheduler raises, worded by something under this
        # module — an adapter, a wrapper, a piece of middleware — and therefore
        # out of text no gate here has seen.
        "side_effect": AirflowException(
            f"proxy https://user:{TOKEN}@proxy.local refused the connection"
        )
    },
    "a network failure the last attempt gives up on": lambda: {
        "side_effect": requests.Timeout(f"timed out talking to https://{TOKEN}@proxy.local")
    },
    "an answer whose caveats about its own numbers carry the header": lambda: {
        "return_value": _response(
            200,
            {
                "data": [],
                "total_rows": 0,
                "sampled": True,
                "sample_share": f"{_TOKEN_SCHEME} {TOKEN}",
                "sample_size": f"{_TOKEN_SCHEME} {TOKEN}",
                "sample_space": f"{_TOKEN_SCHEME} {TOKEN}",
                "contains_sensitive_data": True,
                "data_lag": f"{_TOKEN_SCHEME} {TOKEN}",
            },
        )
    },
    "an answer declaring a total no page can reach": lambda: {
        "return_value": _response(
            200,
            {
                "data": [{"dimensions": [{"name": f"{_TOKEN_SCHEME} {TOKEN}"}], "metrics": [1]}],
                "total_rows": 99,
                "total_rows_rounded": False,
            },
        )
    },
}


@pytest.mark.parametrize("scenario", list(SCENARIOS), ids=list(SCENARIOS))
class TestNoChannelSpellsTheTokenOut:
    def test_the_day_ends_without_the_token_anywhere(self, scenario, caplog):
        sink, failure = _run(SCENARIOS[scenario](), caplog)
        assert TOKEN not in _everything_that_left(sink, failure, caplog)

    def test_not_even_the_middle_of_it(self, scenario, caplog):
        """The ends of the mask are meant to travel; the middle never is."""
        sink, failure = _run(SCENARIOS[scenario](), caplog)
        assert SECRET_MIDDLE not in _everything_that_left(sink, failure, caplog)

    def test_the_campaign_list_is_no_different(self, scenario, caplog):
        """The same answers, met on the way to the campaign list."""
        sink, failure = _run(SCENARIOS[scenario](), caplog, campaigns_first=True)
        assert SECRET_MIDDLE not in _everything_that_left(sink, failure, caplog)

    def test_something_actually_left_the_process(self, scenario, caplog):
        """Guards the assertions above: a silent run would pass them for free."""
        sink, failure = _run(SCENARIOS[scenario](), caplog)
        assert _everything_that_left(sink, failure, caplog).strip()
        assert sink.pushed


class TestTheRequestHeadersOfEveryEvent:
    """``request_headers`` is the one field the token is *meant* to be near."""

    @pytest.mark.parametrize("scenario", list(SCENARIOS), ids=list(SCENARIOS))
    def test_the_authorization_value_is_the_mask_and_never_the_token(self, scenario, caplog):
        sink, _ = _run(SCENARIOS[scenario](), caplog)
        for event in sink.pushed:
            value = event["request_headers"]["Authorization"]
            assert value.startswith(f"{_TOKEN_SCHEME} ")
            assert TOKEN not in value
            assert SECRET_MIDDLE not in value


class TestTheRowsThemselves:
    """A repeated row is named in the exception by words the server chose."""

    def test_a_repeated_row_is_reported_without_the_token(self, caplog):
        sink = _Sink()
        hook = _hook(sink)
        hook._campaigns = [{"campaign_id": 123456}]
        hook.limit = 1
        row = {"dimensions": [{"name": f"{_TOKEN_SCHEME} {TOKEN}"}], "metrics": [1]}
        page = _response(200, {"data": [row], "total_rows": 9, "total_rows_rounded": False})
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=page):
                with pytest.raises(AirflowException) as excinfo:
                    hook.get_stats(DATE, DIMENSIONS, METRICS)
        assert "already carried" in str(excinfo.value)
        assert SECRET_MIDDLE not in _everything_that_left(sink, excinfo.value, caplog)


class TestExtraParams:
    """A DAG may add query parameters of its own, and put a credential in one."""

    def test_a_parameter_carrying_the_token_travels_masked(self, caplog):
        sink = _Sink()
        hook = _hook(sink)
        hook._campaigns = [{"campaign_id": 123456}]
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=_response(500, {})):
                with pytest.raises(AirflowException):
                    hook.get_stats(
                        DATE, DIMENSIONS, METRICS, extra_params={"proxy_auth": TOKEN}
                    )
        assert sink.pushed
        for event in sink.pushed:
            assert event["request_params"]["proxy_auth"] == _TOKEN_REDACTED
        assert SECRET_MIDDLE not in _everything_that_left(sink, "", caplog)

    def test_a_parameter_named_after_the_token_travels_masked(self, caplog):
        """A credential lands as readily in the name of a parameter as in its value."""
        sink = _Sink()
        hook = _hook(sink)
        hook._campaigns = [{"campaign_id": 123456}]
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=_response(500, {})):
                with pytest.raises(AirflowException):
                    hook.get_stats(DATE, DIMENSIONS, METRICS, extra_params={TOKEN: "1"})
        assert sink.pushed
        for event in sink.pushed:
            assert _TOKEN_REDACTED in event["request_params"]
        assert SECRET_MIDDLE not in _everything_that_left(sink, "", caplog)


class TestTheNamesTheCallerAsksBy:
    """A metric or grouping name is the caller's own text, and can hold anything."""

    def _day(self, hook, dimensions, metrics, row_metrics, caplog):
        page = _response(
            200,
            {
                "data": [{"dimensions": [{"name": "Main"}], "metrics": row_metrics}],
                "total_rows": 1,
                "total_rows_rounded": False,
            },
        )
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=page):
                return hook.get_stats(DATE, dimensions, metrics)

    def test_two_names_writing_one_key_are_reported_by_position(self, caplog):
        """The collision is a WARNING, and the names behind it stay in the request."""
        sink = _Sink()
        hook = _hook(sink)
        hook._campaigns = [{"campaign_id": 123456}]
        records = self._day(hook, DIMENSIONS, [f"am:e:{TOKEN}"] * 2, [1, 2], caplog)

        assert len(records) == 1
        assert "position 2" in caplog.text
        assert SECRET_MIDDLE not in _everything_that_left(sink, "", caplog)

    def test_a_name_whose_placeholder_goes_unanswered_is_reported_by_length(self, caplog):
        """The name is given by its length: the request carries no value for it."""
        sink = _Sink()
        hook = _hook(sink)
        hook._campaigns = [{"campaign_id": 123456}]
        records = self._day(hook, DIMENSIONS, [f"am:e:{TOKEN}<goal_id>Reaches"], [1], caplog)

        assert len(records) == 1
        assert "goal_id" in caplog.text
        assert SECRET_MIDDLE not in _everything_that_left(sink, "", caplog)

    def test_a_placeholder_holding_the_token_is_reported_without_it(self, caplog):
        """The parameter's own name is written out, so it passes the gate too."""
        sink = _Sink()
        hook = _hook(sink)
        hook._campaigns = [{"campaign_id": 123456}]
        records = self._day(hook, DIMENSIONS, [f"am:e:goal<{TOKEN}>Reaches"], [1], caplog)

        assert len(records) == 1
        assert SECRET_MIDDLE not in _everything_that_left(sink, "", caplog)


class TestWhatTheCallerConfigures:
    """A refusal written before the first request names the caller's own value."""

    def test_a_limit_carrying_the_token_is_refused_without_it(self, caplog):
        sink = _Sink()
        hook = _hook(sink)
        hook.limit = TOKEN
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get") as mock_get:
                with pytest.raises(ValueError) as excinfo:
                    hook.get_stats(DATE, DIMENSIONS, METRICS)
        assert mock_get.call_count == 0
        assert "limit must be" in str(excinfo.value)
        assert SECRET_MIDDLE not in _everything_that_left(sink, excinfo.value, caplog)

    def test_a_date_carrying_the_token_is_refused_without_it(self, caplog):
        sink = _Sink()
        hook = _hook(sink)
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get") as mock_get:
                with pytest.raises(ValueError) as excinfo:
                    hook.get_stats(TOKEN, DIMENSIONS, METRICS)
        assert mock_get.call_count == 0
        assert "date must be a day" in str(excinfo.value)
        assert SECRET_MIDDLE not in _everything_that_left(sink, excinfo.value, caplog)


class TestTheConnectionTest:
    """The button shows its text to whoever pressed it."""

    @pytest.mark.parametrize("scenario", list(SCENARIOS), ids=list(SCENARIOS))
    def test_a_failing_check_answers_without_the_token(self, scenario, caplog):
        sink = _Sink()
        hook = _hook(sink)
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", **SCENARIOS[scenario]()):
                ok, message = hook.test_connection()
        assert ok is False
        assert SECRET_MIDDLE not in _everything_that_left(sink, message, caplog)

    def test_a_passing_check_says_nothing_about_the_credential(self, caplog):
        sink = _Sink()
        hook = _hook(sink)
        page = _response(200, {"campaigns": [{"campaign_id": 1}], "total": 1})
        with caplog.at_level(logging.DEBUG, logger=_HOOK_LOGGER):
            with patch("requests.get", return_value=page):
                ok, message = hook.test_connection()
        assert ok is True
        assert SECRET_MIDDLE not in _everything_that_left(sink, message, caplog)
