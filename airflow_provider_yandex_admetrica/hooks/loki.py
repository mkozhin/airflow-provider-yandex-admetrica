"""Client that ships this provider's diagnostic events to a Loki instance."""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import requests
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection

log = logging.getLogger(__name__)

#: The only stream label attached to pushed entries. Everything else lives in
#: the log line body, so label cardinality stays constant.
_SERVICE = "airflow-provider-yandex-admetrica"

_PUSH_PATH = "/loki/api/v1/push"

#: (connect, read) — a push must never delay the task it instruments for long.
#: The read half bounds the quiet between received bytes, so an unresponsive
#: Loki costs about 5 s; only the response status is read, never its body.
_TIMEOUT = (2, 3)

#: Loki answers a successful push with 204 and nothing else.
_EXPECTED_STATUS = 204

_SUPPORTED_SCHEMES = ("http", "https")


class _TargetError(ValueError):
    """Raised by :func:`_build_target` when a connection cannot be turned into a push URL."""


def _build_target(conn: Connection) -> tuple[str, bool]:
    """Return ``(push_url, is_https)`` for an Airflow connection.

    ``Host`` may carry an explicit scheme (``https://loki.example.ru:3100``),
    in which case it holds the whole authority and ``Schema``/``Port`` are not
    consulted; otherwise the URL is assembled from ``Schema``, ``Host`` and
    ``Port``, and ``Port`` applies only when ``Host`` names none. A trailing
    slash is dropped, and a ``Host`` that already ends in the push path is
    accepted as is.

    An IPv6 address is written in brackets (``http://[::1]``, ``[::1]`` with a
    ``Schema``), the form that keeps its colons apart from a port's.

    The scheme is never guessed: a ``Host`` without one and an empty ``Schema``
    raise :class:`_TargetError`, as do an empty or hostname-less ``Host``, a
    port that is not a number in range, and any scheme other than http/https.

    Credentials belong in ``Login``/``Password``: a ``Host`` carrying userinfo
    (``https://user:token@loki.example.ru``) is rejected, because ``requests``
    would turn it into a ``Authorization: Basic`` header that the
    HTTPS-required-for-auth rule never sees. A query string or fragment is
    rejected for the same reason — the push path has to be the end of the URL.
    """
    host = (conn.host or "").strip()
    if not host:
        raise _TargetError("Host is empty")

    if "://" in host:
        base_url, port = host, None
    else:
        schema = (conn.schema or "").strip().lower()
        if not schema:
            raise _TargetError(
                "Host has no scheme and Schema is empty — "
                "use https://host in Host, or set Schema"
            )
        base_url, port = f"{schema}://{host}", conn.port

    parts = urlsplit(base_url)
    scheme = parts.scheme.lower()
    if not parts.hostname:
        raise _TargetError("Host has no hostname")
    if parts.username is not None or parts.password is not None:
        raise _TargetError(
            "Host carries credentials in the URL — move them to Login and Password"
        )
    if parts.query or parts.fragment:
        raise _TargetError("Host carries a query string or fragment — use a plain base URL")
    if scheme not in _SUPPORTED_SCHEMES:
        raise _TargetError(f"unsupported URL scheme {scheme!r}, expected http or https")
    try:
        host_port = parts.port
    except ValueError as e:
        raise _TargetError("Host names a port that is not a number in range") from e

    netloc = parts.netloc if host_port is not None or not port else f"{parts.netloc}:{port}"
    base = f"{scheme}://{netloc}{parts.path}".rstrip("/")
    is_https = scheme == "https"
    if base.endswith(_PUSH_PATH):
        return base, is_https
    return base + _PUSH_PATH, is_https


class LokiClient:
    """Best-effort sink that ships one diagnostic event per call to Loki.

    ``push`` absorbs every diagnostics failure and never influences the caller's
    control flow; only an interruption of the task itself travels on (see
    :meth:`push`).  The first failure of any kind — unusable connection,
    misconfigured credentials, network error, unexpected status — logs a single
    WARNING and disables the client for the rest of its lifetime, so a Loki
    outage costs one timeout per task instead of one per HTTP attempt.

    Not thread-safe: one operator run constructs one client and pushes through
    it sequentially.
    """

    def __init__(self, conn_id: str, context: dict | None = None) -> None:
        self._conn_id = conn_id
        #: Correlation fields merged into every event (dag/task/run identity).
        self._context: dict = dict(context or {})
        self._disabled = False
        self._target: tuple[str, tuple[str, str] | None] | None = None

    @property
    def enabled(self) -> bool:
        """Whether a push would still be attempted, or the breaker has tripped.

        A caller that pays to fill an event — reading and decoding the raw
        answer — asks first, so a run that has already lost Loki does not keep
        assembling events for a sink that drops them on arrival.
        """
        return not self._disabled

    def push(self, event: dict) -> None:
        """Send *event* to Loki as a single log line.

        No diagnostics failure escapes: an unusable connection, a network error
        or an unexpected status is caught, logged once and turns the client off.

        Interruptions of the task itself are outside that guard and travel on:
        ``KeyboardInterrupt``, ``SystemExit`` and Airflow's task-control
        exceptions (``AirflowTaskTimeout`` from ``execution_timeout``,
        ``AirflowTaskTerminated`` from the SIGTERM handler) derive from
        ``BaseException`` rather than ``Exception``. They mean the task is being
        stopped, not that Loki failed, and swallowing one would keep a task
        running past the moment it must die — the alarm and the signal fire once.
        """
        if self._disabled:
            return
        try:
            target = self._resolve()
            if target is None:
                return
            url, auth = target

            # Key sets are disjoint by contract; the event wins if they ever collide.
            body = {**self._context, **event}
            payload = {
                "streams": [
                    {
                        "stream": {"service": _SERVICE},
                        "values": [
                            [
                                str(time.time_ns()),
                                json.dumps(body, ensure_ascii=False, default=str),
                            ]
                        ],
                    }
                ]
            }
            resp = requests.post(
                url,
                json=payload,
                auth=auth,
                timeout=_TIMEOUT,
                allow_redirects=False,
                stream=True,
            )
            # The status is the whole answer: a successful push has no body, and
            # streaming means an unexpected one is dropped at the socket instead
            # of being read into the task's memory on the task's time.
            resp.close()
            if resp.status_code != _EXPECTED_STATUS:
                raise ValueError(
                    f"Loki push returned {resp.status_code} (expected {_EXPECTED_STATUS})"
                )
        except Exception as e:
            # Every diagnostics failure ends here. The type, not the text: a
            # ProxyError or ConnectionError embeds the environment's proxy URL,
            # credentials included.
            self._fail_once(f"push failed with {type(e).__name__}")

    def _resolve(self) -> tuple[str, tuple[str, str] | None] | None:
        """Return the cached ``(url, auth)`` pair, resolving it on first use.

        Returns ``None`` when the connection cannot be used, having already
        disabled the client through :meth:`_fail_once`. Credentials are checked
        here, once: they must be complete, and Basic Auth requires an HTTPS URL.
        """
        if self._target is not None:
            return self._target

        conn = BaseHook.get_connection(self._conn_id)
        login = conn.login or None
        password = conn.password or None
        if (login is None) != (password is None):
            self._fail_once(
                "incomplete credentials — set both Login and Password, or neither"
            )
            return None

        try:
            url, is_https = _build_target(conn)
        except _TargetError as e:
            self._fail_once(f"cannot build push URL: {e}")
            return None

        auth = (login, password) if login is not None else None
        if auth is not None and not is_https:
            self._fail_once("refusing to send Basic Auth credentials over a non-HTTPS URL")
            return None

        self._target = (url, auth)
        return self._target

    def _fail_once(self, msg: str) -> None:
        """Log the first failure as a WARNING and disable the client."""
        if not self._disabled:
            self._disabled = True
            log.warning(
                "Loki diagnostics disabled for connection %r: %s",
                self._conn_id,
                msg,
            )
