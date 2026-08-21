from __future__ import annotations

import json
import logging
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from airflow.exceptions import AirflowTaskTerminated, AirflowTaskTimeout
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.loki import (
    LokiClient,
    _build_target,
    _SERVICE,
    _TargetError,
)
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _BODY_LIMIT,
    _HEADER_LIMIT,
    _PARAMS_LIMIT,
    _TEXT_LIMIT,
    _new_event,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conn(
    host: str | None = "loki.example.ru",
    schema: str | None = "https",
    port: int | None = None,
    login: str | None = None,
    password: str | None = None,
) -> Connection:
    return Connection(
        conn_id="loki_test",
        conn_type="http",
        host=host,
        schema=schema,
        port=port,
        login=login,
        password=password,
    )


def _mock_response(status_code: int = 204) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    return resp


def _pushed_body(mock_post: MagicMock) -> dict:
    """Return the event body of the single log line the client sent."""
    payload = mock_post.call_args.kwargs["json"]
    line = payload["streams"][0]["values"][0][1]
    return json.loads(line)


def _warnings(caplog) -> list[logging.LogRecord]:
    """WARNINGs emitted by the client under test, ignoring the rest of Airflow."""
    return [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == "airflow_provider_yandex_admetrica.hooks.loki"
    ]


# ---------------------------------------------------------------------------
# Successful push
# ---------------------------------------------------------------------------


class TestPushSuccess:
    def test_push_sends_expected_url_labels_and_body(self, caplog):
        client = LokiClient(conn_id="loki_test", context={"dag_id": "d", "task_id": "t"})
        before = time.time_ns()
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success", "offset": 0})
        after = time.time_ns()

        assert mock_post.call_args.args[0] == "https://loki.example.ru/loki/api/v1/push"
        payload = mock_post.call_args.kwargs["json"]
        stream = payload["streams"][0]
        assert stream["stream"] == {"service": "airflow-provider-yandex-admetrica"}
        assert len(stream["values"]) == 1
        timestamp, _line = stream["values"][0]
        # Loki expects nanoseconds: seconds or milliseconds fall outside this window.
        assert before <= int(timestamp) <= after
        assert _pushed_body(mock_post) == {
            "dag_id": "d",
            "task_id": "t",
            "outcome": "success",
            "offset": 0,
        }
        assert _warnings(caplog) == []

    def test_service_is_the_only_stream_label(self):
        """Label cardinality stays constant: everything variable rides in the line."""
        client = LokiClient(conn_id="loki_test", context={"dag_id": "d"})
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success", "advertiser_id": 17004, "campaign_id": 1})

        stream = mock_post.call_args.kwargs["json"]["streams"][0]["stream"]
        assert _SERVICE == "airflow-provider-yandex-admetrica"
        assert stream == {"service": _SERVICE}
        assert _pushed_body(mock_post)["advertiser_id"] == 17004

    def test_event_wins_on_key_collision_with_context(self):
        client = LokiClient(conn_id="loki_test", context={"dag_id": "from_context"})
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"dag_id": "from_event"})

        assert _pushed_body(mock_post)["dag_id"] == "from_event"

    def test_basic_auth_over_https(self):
        client = LokiClient(conn_id="loki_test")
        conn = _conn(login="user", password="pw")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_args.kwargs["auth"] == ("user", "pw")

    def test_no_auth_when_credentials_absent(self):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_args.kwargs["auth"] is None

    def test_request_uses_narrow_timeout_and_no_redirects(self):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_args.kwargs["timeout"] == (2, 3)
        assert mock_post.call_args.kwargs["allow_redirects"] is False

    @pytest.mark.parametrize("status", [204, 200])
    def test_response_body_is_never_downloaded(self, status):
        """Only the status is used: an unbounded body must not become the task's problem."""
        client = LokiClient(conn_id="loki_test")
        resp = _mock_response(status)
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=resp) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_args.kwargs["stream"] is True
        resp.close.assert_called_once_with()
        assert resp.method_calls == [("close", (), {})]

    def test_connection_resolved_once_for_repeated_pushes(self):
        client = LokiClient(conn_id="loki_test")
        with patch(
            "airflow.hooks.base.BaseHook.get_connection", return_value=_conn()
        ) as mock_get_conn:
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        assert mock_get_conn.call_count == 1
        assert mock_post.call_count == 2


# ---------------------------------------------------------------------------
# URL building
# ---------------------------------------------------------------------------


class TestBuildTarget:
    """`_build_target` is a pure function of the connection — exercised directly."""

    @pytest.mark.parametrize(
        "host,schema,port,expected",
        [
            ("loki.example.ru", "http", 3100, "http://loki.example.ru:3100/loki/api/v1/push"),
            ("loki.example.ru", "https", None, "https://loki.example.ru/loki/api/v1/push"),
            ("gw.example.ru/loki", "https", 3100, "https://gw.example.ru:3100/loki/loki/api/v1/push"),
            ("loki.example.ru:3100", "http", 3100, "http://loki.example.ru:3100/loki/api/v1/push"),
            ("loki.example.ru/", "https", None, "https://loki.example.ru/loki/api/v1/push"),
        ],
        ids=["schema_and_port", "no_port", "path_keeps_port", "port_in_host", "trailing_slash"],
    )
    def test_host_without_scheme_is_assembled_from_the_connection(
        self, host, schema, port, expected
    ):
        assert _build_target(_conn(host=host, schema=schema, port=port))[0] == expected

    @pytest.mark.parametrize(
        "host,expected",
        [
            ("https://loki.example.ru", "https://loki.example.ru/loki/api/v1/push"),
            ("https://loki.example.ru:3100", "https://loki.example.ru:3100/loki/api/v1/push"),
            ("https://loki.example.ru/", "https://loki.example.ru/loki/api/v1/push"),
            ("https://gw.example.ru/loki/", "https://gw.example.ru/loki/loki/api/v1/push"),
            ("HTTPS://loki.example.ru", "https://loki.example.ru/loki/api/v1/push"),
            (
                "https://loki.example.ru/loki/api/v1/push",
                "https://loki.example.ru/loki/api/v1/push",
            ),
        ],
    )
    def test_host_with_scheme_used_as_is(self, host, expected):
        assert _build_target(_conn(host=host, schema=None, port=None))[0] == expected

    @pytest.mark.parametrize(
        "host,schema,port,expected",
        [
            ("[::1]", "http", 3100, "http://[::1]:3100/loki/api/v1/push"),
            ("[::1]", "http", None, "http://[::1]/loki/api/v1/push"),
            ("[::1]:3100", "http", None, "http://[::1]:3100/loki/api/v1/push"),
            ("http://[::1]", None, None, "http://[::1]/loki/api/v1/push"),
            (
                "[2001:db8::8a2e:370:7334]",
                "https",
                3100,
                "https://[2001:db8::8a2e:370:7334]:3100/loki/api/v1/push",
            ),
        ],
        ids=["port_field", "no_port", "port_in_host", "scheme_in_host", "full_address"],
    )
    def test_bracketed_ipv6_host_keeps_its_colons_apart_from_the_port(
        self, host, schema, port, expected
    ):
        """An IPv6 address is full of colons; only the one after `]` marks a port."""
        assert _build_target(_conn(host=host, schema=schema, port=port))[0] == expected

    @pytest.mark.parametrize("host", ["loki.example.ru:abc", "loki.example.ru:99999"])
    def test_host_with_an_unusable_port_is_rejected(self, host):
        with pytest.raises(_TargetError, match="not a number in range"):
            _build_target(_conn(host=host, schema="https"))

    def test_host_with_scheme_owns_the_authority(self):
        """`Port` belongs to the bare-host form; a full URL is not amended with it."""
        url, _ = _build_target(_conn(host="https://loki.example.ru", schema=None, port=3100))

        assert url == "https://loki.example.ru/loki/api/v1/push"

    @pytest.mark.parametrize(
        "host,schema,is_https",
        [
            ("loki.example.ru", "https", True),
            ("loki.example.ru", "http", False),
            ("https://loki.example.ru", None, True),
            ("HTTP://loki.example.ru", None, False),
        ],
    )
    def test_is_https_follows_the_effective_scheme(self, host, schema, is_https):
        assert _build_target(_conn(host=host, schema=schema))[1] is is_https

    @pytest.mark.parametrize("host", [None, "", "   "])
    def test_empty_host_is_rejected(self, host):
        with pytest.raises(_TargetError, match="Host is empty"):
            _build_target(_conn(host=host))

    def test_host_without_scheme_and_empty_schema_is_rejected(self):
        """The scheme is never guessed — the message has to name the fix."""
        with pytest.raises(_TargetError, match="use https://host in Host, or set Schema"):
            _build_target(_conn(host="loki.example.ru", schema=None))

    @pytest.mark.parametrize(
        "host,schema",
        [("ftp://loki.example.ru", None), ("loki.example.ru", "ftp")],
    )
    def test_unsupported_scheme_is_rejected(self, host, schema):
        with pytest.raises(_TargetError, match="ftp"):
            _build_target(_conn(host=host, schema=schema))

    @pytest.mark.parametrize("host", ["https://", "https:///loki"])
    def test_scheme_without_hostname_is_rejected(self, host):
        with pytest.raises(_TargetError, match="no hostname"):
            _build_target(_conn(host=host))

    @pytest.mark.parametrize(
        "host,schema",
        [
            ("https://loki.example.ru?x=1", None),
            ("https://loki.example.ru#f", None),
            ("loki.example.ru?x=1", "https"),
            ("loki.example.ru#f", "https"),
        ],
    )
    def test_query_or_fragment_is_rejected(self, host, schema):
        with pytest.raises(_TargetError, match="query string or fragment"):
            _build_target(_conn(host=host, schema=schema))

    @pytest.mark.parametrize(
        "host,schema",
        [
            ("http://user:SEKRET@loki.example.ru", None),
            ("https://user:SEKRET@loki.example.ru", None),
            ("user:SEKRET@loki.example.ru", "https"),
        ],
    )
    def test_userinfo_in_the_host_is_rejected(self, host, schema):
        """Userinfo in the URL would become a Basic Auth header behind the HTTPS rule."""
        with pytest.raises(_TargetError, match="Login and Password") as excinfo:
            _build_target(_conn(host=host, schema=schema))

        assert "SEKRET" not in str(excinfo.value)

    def test_an_unusable_host_disables_the_client_with_one_warning(self, caplog):
        """The resolve path turns a `_TargetError` into a single WARNING and stops."""
        client = LokiClient(conn_id="loki_test")
        conn = _conn(host="loki.example.ru", schema=None)
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("requests.post") as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        mock_post.assert_not_called()
        warnings = _warnings(caplog)
        assert len(warnings) == 1
        assert "use https://host in Host, or set Schema" in warnings[0].getMessage()

    def test_the_built_url_is_the_one_pushed_to(self):
        client = LokiClient(conn_id="loki_test")
        conn = _conn(host="loki.example.ru", schema="http", port=3100)
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_args.args[0] == _build_target(conn)[0]

    def test_host_with_scheme_over_https_allows_auth(self):
        client = LokiClient(conn_id="loki_test")
        conn = _conn(host="https://loki.example.ru", schema=None, login="user", password="pw")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_args.kwargs["auth"] == ("user", "pw")


# ---------------------------------------------------------------------------
# Credential policy
# ---------------------------------------------------------------------------


class TestCredentialPolicy:
    def test_auth_over_plain_http_is_refused(self, caplog):
        client = LokiClient(conn_id="loki_test")
        conn = _conn(schema="http", login="user", password="pw")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("requests.post") as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        mock_post.assert_not_called()
        warnings = _warnings(caplog)
        assert len(warnings) == 1
        assert "non-HTTPS" in warnings[0].getMessage()

    @pytest.mark.parametrize(
        "host",
        [
            "http://user:SEKRET@loki.example.ru",
            "https://user:SEKRET@loki.example.ru",
        ],
    )
    def test_credentials_embedded_in_host_are_refused(self, caplog, host):
        """Userinfo in the URL would become a Basic Auth header behind the HTTPS rule."""
        client = LokiClient(conn_id="loki_test")
        with patch(
            "airflow.hooks.base.BaseHook.get_connection", return_value=_conn(host=host)
        ):
            with patch("requests.post") as mock_post:
                client.push({"outcome": "success"})

        mock_post.assert_not_called()
        warnings = _warnings(caplog)
        assert len(warnings) == 1
        assert "Login and Password" in warnings[0].getMessage()
        assert "SEKRET" not in warnings[0].getMessage()

    @pytest.mark.parametrize(
        "login,password",
        [("user", None), ("user", ""), (None, "pw"), ("", "pw")],
    )
    def test_incomplete_credentials_disable_push(self, caplog, login, password):
        client = LokiClient(conn_id="loki_test")
        conn = _conn(login=login, password=password)
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("requests.post") as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        mock_post.assert_not_called()
        warnings = _warnings(caplog)
        assert len(warnings) == 1
        assert "incomplete credentials" in warnings[0].getMessage()


# ---------------------------------------------------------------------------
# Failure handling and circuit breaker
# ---------------------------------------------------------------------------


class TestFailureHandling:
    def test_non_204_status_is_a_failure_with_one_warning(self, caplog):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(200)) as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        warnings = _warnings(caplog)
        assert len(warnings) == 1
        assert "push failed" in warnings[0].getMessage()
        assert mock_post.call_count == 1

    def test_network_failure_warning_carries_the_type_not_the_text(self, caplog):
        """A ProxyError's text embeds the environment's proxy URL, credentials included."""
        client = LokiClient(conn_id="loki_test")
        exc = requests.exceptions.ProxyError("proxy http://user:SEKRET@proxy:3128 refused")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", side_effect=exc):
                client.push({"outcome": "success"})

        record = _warnings(caplog)[0]
        assert "ProxyError" in record.getMessage()
        assert "SEKRET" not in record.getMessage()
        # No traceback either: the same text would land in the task log through it.
        assert record.exc_info is None

    def test_circuit_breaker_stops_further_http_attempts(self, caplog):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", side_effect=OSError("boom")) as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        assert mock_post.call_count == 1
        assert len(_warnings(caplog)) == 1

    def test_get_connection_failure_is_swallowed_with_one_warning(self, caplog):
        client = LokiClient(conn_id="loki_test")
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=RuntimeError("no such connection"),
        ) as mock_get_conn:
            with patch("requests.post") as mock_post:
                client.push({"outcome": "success"})
                client.push({"outcome": "success"})

        mock_post.assert_not_called()
        assert mock_get_conn.call_count == 1
        assert len(_warnings(caplog)) == 1

    def test_unserializable_event_value_does_not_raise(self, caplog):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success", "weird": object()})

        # `default=str` keeps the line serializable, so the push still goes out.
        assert mock_post.call_count == 1
        assert isinstance(_pushed_body(mock_post)["weird"], str)
        assert _warnings(caplog) == []

    def test_unserializable_value_without_str_fallback_does_not_raise(self, caplog):
        class Hostile:
            def __str__(self) -> str:
                raise RuntimeError("nope")

        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success", "weird": Hostile()})

        mock_post.assert_not_called()
        assert len(_warnings(caplog)) == 1


# ---------------------------------------------------------------------------
# The size of the line a push carries
# ---------------------------------------------------------------------------


#: Loki's default `limits_config.max_line_size`, counted in bytes.
_MAX_LINE_BYTES = 256 * 1024

#: A token of the shape the hook masks, kept clear of the filler the fields are
#: written in so that redaction never shortens the fixture behind the
#: measurement's back.
_TOKEN = "y0__xD" + "s" * 50 + "q9Az"


def _pushed_line(mock_post: MagicMock) -> str:
    """Return the log line the client sent, exactly as it went over the wire."""
    payload = mock_post.call_args.kwargs["json"]
    return payload["streams"][0]["values"][0][1]


def _full_event(response_body: str, filler: str) -> dict:
    """The largest event a request can build, answered by *response_body*.

    Every bounded field sits at its budget and every optional one is filled,
    each free-text one with the character the measurement is being made in, so
    the result covers the event a bad day produces rather than a typical one.
    The request parameters are the widest the statistics endpoint accepts: the
    documented limits are 20 metrics, 10 dimensions and a filter of 10 000
    characters, and every one of them is written in the same filler.

    The schema and the budgets come from the hook module, and the measurement
    belongs here: how long a line the client may hand to Loki is a property of
    the client, so the fixture is built from the widest event the hook can
    produce.
    """
    event = _new_event(
        endpoint="stat",
        params={
            "ids": 123456,
            "date1": "2026-08-20",
            "date2": "2026-08-20",
            "metrics": ",".join(f"am:e:{filler * 20}{i}" for i in range(20)),
            "dimensions": ",".join(f"am:e:{filler * 20}{i}" for i in range(10)),
            "sort": ",".join(f"am:e:{filler * 20}{i}" for i in range(10)),
            "limit": 10000,
            "offset": 40001,
            "accuracy": "full",
            "include_undefined": "true",
            "filters": filler * 10000,
            "timezone": "+03:00",
            "lang": "ru",
        },
        headers={"Authorization": f"OAuth {_TOKEN}", "Accept": "application/json"},
        token=_TOKEN,
        advertiser_id=17004,
        campaign_id=123456,
        date="2026-08-20",
        offset=40001,
        attempt=4,
        max_attempts=4,
    )
    event.update(
        outcome="unexpected_error",
        level="error",
        http_status=500,
        duration_ms=61234,
        rows_count=10000,
        rows_shape_ok=False,
        payload_kind="rows_non_list",
        total_rows=1234567,
        total_rows_rounded=True,
        sampled=True,
        sample_share=0.1234567890123,
        sample_size=1234567,
        sample_space=12345678,
        contains_sensitive_data=True,
        data_lag=86400,
        error_code=403,
        error_message=filler * _TEXT_LIMIT,
        exception_type="AirflowException",
        exception_message=filler * _TEXT_LIMIT,
        rate_limit_limit=filler * _HEADER_LIMIT,
        rate_limit_remaining=filler * _HEADER_LIMIT,
        response_body=response_body,
    )
    return event


class TestPushedLineFitsTheLokiLimit:
    """A full event has to fit the line Loki accepts, or the run loses diagnostics.

    The first refusal disables the client for the rest of the task, so one
    oversized answer would silence every event after it.  Measured on the line
    the client really sent, in bytes: the limit counts bytes, and the escaping
    that inflates them happens inside the push.
    """

    @pytest.mark.parametrize(
        "filler",
        ["\x00", "\U0001f600", '"', "\\", "ю", "x"],
        ids=["control", "emoji", "quote", "backslash", "cyrillic", "ascii"],
    )
    def test_a_worst_case_answer_stays_within_the_line_limit(self, filler):
        body = filler * _BODY_LIMIT
        client = LokiClient(
            conn_id="loki_test",
            context={
                "dag_id": "osnova_admetrica",
                "task_id": "collect",
                "dag_run_id": "scheduled__2026-08-21T00:00:00+00:00",
                "try_number": 3,
                "map_index": 12,
            },
        )

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push(_full_event(body, filler))

        line = _pushed_line(mock_post)
        assert json.loads(line)["response_body"] == body
        assert len(line.encode("utf-8")) <= _MAX_LINE_BYTES

    def test_the_widest_answer_and_the_widest_parameters_together_fit(self):
        """A body costs the most in control characters, a parameter in emoji.

        Neither field is written in what costs the other the most, so the two
        worst cases fall on one line only when they are put there on purpose.
        """
        client = LokiClient(conn_id="loki_test")

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push(_full_event("\x00" * _BODY_LIMIT, "\U0001f600"))

        line = _pushed_line(mock_post)
        assert len(line.encode("utf-8")) <= _MAX_LINE_BYTES

    def test_the_parameters_of_that_event_really_sit_at_their_budget(self):
        """The measurement is only worth as much as the fixture's own size."""
        params = _full_event("", "x")["request_params"]

        spent = sum(len(k) + len(v) for k, v in params.items() if isinstance(v, str))
        assert spent > _PARAMS_LIMIT // 2


# ---------------------------------------------------------------------------
# Interruptions of the task itself
# ---------------------------------------------------------------------------


class TestTaskControlExceptions:
    """A signal landing in a push stops the task; only Loki failures are absorbed."""

    def test_airflow_task_control_exceptions_live_outside_exception(self):
        """The guard in `push` is `except Exception` — these must stay out of its reach."""
        assert not issubclass(AirflowTaskTimeout, Exception)
        assert not issubclass(AirflowTaskTerminated, Exception)

    @pytest.mark.parametrize(
        "exc",
        [
            AirflowTaskTimeout("execution_timeout"),
            AirflowTaskTerminated("Task received SIGTERM signal"),
            KeyboardInterrupt(),
            SystemExit(1),
        ],
        ids=["task_timeout", "sigterm", "keyboard_interrupt", "system_exit"],
    )
    def test_push_lets_the_interruption_through(self, exc, caplog):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", side_effect=exc):
                with pytest.raises(type(exc)):
                    client.push({"outcome": "success"})

        # Not a Loki failure: nothing is logged as one.
        assert _warnings(caplog) == []

    def test_client_stays_usable_after_an_interruption(self, caplog):
        client = LokiClient(conn_id="loki_test")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=_conn()):
            with patch("requests.post", side_effect=AirflowTaskTimeout("boom")):
                with pytest.raises(AirflowTaskTimeout):
                    client.push({"outcome": "success"})
            with patch("requests.post", return_value=_mock_response(204)) as mock_post:
                client.push({"outcome": "success"})

        assert mock_post.call_count == 1
        assert _warnings(caplog) == []
