"""Hook for the Yandex Metrica for Display Advertising (AdMetrica) report API."""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.models import Connection

    from airflow_provider_yandex_admetrica.hooks.loki import LokiClient

log = logging.getLogger(__name__)

#: Version of the diagnostic event's field set.  A reader of a stored event
#: knows by this number which fields it may expect.
_SCHEMA_VERSION = 1

#: Host every request goes to.
_API_HOST = "https://api.media.metrika.yandex.net"

#: The two endpoints this provider instruments, by the label an event carries
#: in ``endpoint``.  One mapping holds both the label a dashboard groups by and
#: the address the request goes to, so the event and the request it describes
#: can never name two different endpoints.
_ENDPOINT_URLS = {
    "campaigns": f"{_API_HOST}/v1/management/campaigns",
    "stat": f"{_API_HOST}/v1/stat/data",
}

#: The key each endpoint's rows arrive under.  The two answers name their rows
#: differently, and this is the only place that difference is written down:
#: a body is read for rows through the endpoint label the event already carries.
_ROWS_KEYS = {"campaigns": "campaigns", "stat": "data"}

#: Seconds of quiet between two requests.  AdMetrica publishes neither a quota
#: nor a rate, so the pace is a conservative guess a task may raise or lower
#: through the operator once a real advertiser has been measured.
_DEFAULT_REQUEST_DELAY = 0.2

#: Rows one statistics page asks for.  The endpoint allows up to 100 000 and
#: falls back to 100 when asked for nothing; a tenth of the ceiling keeps a day
#: of a large advertiser inside a few pages while leaving a single answer small
#: enough to hold in memory and to retry cheaply.
_DEFAULT_LIMIT = 10000

#: Rows one page of the campaign list asks for.  The endpoint names neither a
#: default nor a ceiling, so both ``limit`` and ``offset`` go out spelled; a
#: thousand holds the list of an advertiser in one answer and still asks for a
#: page small enough to repeat cheaply.
_CAMPAIGNS_LIMIT = 1000

#: The row the statistics endpoint counts its offset from.  The two endpoints
#: count differently — the campaign list skips rows and therefore starts at
#: zero, while ``/v1/stat/data`` numbers them and starts at one — so a page
#: asked for at offset 0 there is the same page as the one at offset 1, and a
#: walk that counted from zero would return the first row twice.
_STAT_OFFSET_BASE = 1

#: Documented ceilings of one report request.  Checking them here turns a
#: refusal that arrives as an opaque 400 after a request has been paid for into
#: a message naming the list that is too long and by how much.
_MAX_METRICS = 20
_MAX_DIMENSIONS = 10

#: Request parameters the hook owns, which ``extra_params`` may therefore not
#: carry.  Each of them is either the question being asked or an answer to how
#: it is asked, and a silent override of one would be invisible in the data:
#: another ``date1`` would fetch another day while the records still carry the
#: operator's date, and ``accuracy`` or ``include_undefined`` would drop the
#: defaults that stand against drifting and truncated numbers — with the
#: completeness check still passing, because ``total_rows`` agrees with the
#: truncated selection.  The last five have parameters of their own on the
#: operator, so nothing needs this route to reach them.
_RESERVED_PARAMS = frozenset(
    {
        "ids",
        "date1",
        "date2",
        "metrics",
        "dimensions",
        "limit",
        "offset",
        "sort",
        "accuracy",
        "include_undefined",
        "filters",
        "timezone",
        "lang",
    }
)

#: What a report says about the page it just returned, beyond the rows: how many
#: rows there are in total and whether that number is rounded, whether the
#: answer was sampled and on what share, whether rows were withheld, and how far
#: behind the data is.  Every one of them is a caveat about the numbers, so all
#: of them travel with the event describing the request that brought them.
_REPORT_META_FIELDS = (
    "total_rows",
    "total_rows_rounded",
    "sampled",
    "sample_share",
    "sample_size",
    "sample_space",
    "contains_sensitive_data",
    "data_lag",
)

#: The fields a campaign dictionary record keeps, in the order they are written.
#: What the answer carries beside them — spend, impressions, days left,
#: conversions — measures the campaign at the moment of the request rather than
#: describing it, and a measure belongs to the statistics tables.
_CAMPAIGN_FIELDS = (
    "campaign_id",
    "name",
    "status",
    "date_start",
    "date_end",
    "advertiser_id",
    "advertiser_name",
)

#: Prefix every grouping and metric of the report API carries.  It says which
#: namespace the name belongs to, which is the same one for every name this
#: provider sends, so it is dropped from the record key rather than repeated on
#: every row of every file.
_NAME_PREFIX = "am:e:"

#: A parameter spelled into a name, as in ``am:e:goal<goal_id>Reaches``, with
#: the parameter's own name inside the brackets.
_PLACEHOLDER_RE = re.compile(r"<([^<>]*)>")

#: The two boundaries a camelCase name is cut at.  The first ends a run of
#: capitals in front of a word that starts with one — ``RUBRevenue`` is a
#: currency followed by ``Revenue``, not one nine-letter word; the second cuts
#: between a lowercase letter or a digit and the capital after it, which is
#: where ``deviceType`` and ``goal12345Reaches`` come apart.  Neither cuts
#: inside a run of digits, so ``interest2d1`` stays whole.
_ACRONYM_SPLIT_RE = re.compile(r"([A-Z]+)([A-Z][a-z])")
_WORD_SPLIT_RE = re.compile(r"([a-z0-9])([A-Z])")

#: What a record key is allowed to hold, and the run of separators it is
#: collapsed to.  A key is read back by an analyst as ``dimensions.device_type``
#: and by a warehouse as a JSON path, so anything else a name or a substituted
#: parameter value carries — a colon, a space, a currency sign — becomes the one
#: separator the rest of the key already uses.
_NON_KEY_CHARS_RE = re.compile(r"[^a-z0-9_]+")
_UNDERSCORE_RUN_RE = re.compile(r"_+")

#: Seconds a single request is given before it counts as one the network did
#: not carry.  A day of a large advertiser is dozens of requests, so an answer
#: that never comes must cost a bounded amount rather than hold the task until
#: Airflow's own timeout ends it.
_REQUEST_TIMEOUT = 30

#: Statuses a repeat can fix: the rate limit, and every server-side failure.
#: The whole 5xx range is in rather than the familiar four — a proxy in front of
#: the API answers with codes of its own choosing, and every one of them says
#: the request never reached the logic that would refuse it on its merits.
_RETRY_STATUSES = frozenset({429, *range(500, 600)})

#: The pause before each repeat, in seconds.  The length of the ladder is also
#: the number of repeats, so a request gets one attempt plus one rung per pause.
#: AdMetrica publishes no quota, so the ladder is short and conservative: a
#: refusal the server means to last is answered within seconds rather than
#: minutes, and what a task spends on retries stays small.
_BACKOFF_DELAYS = [1, 2, 4]

#: The header a server names its own wait in, and the longest such wait this
#: provider honours.  The header outranks the ladder — a server that says when
#: to come back knows its window better than a rung chosen in advance — but only
#: up to the cap: a wait of hours would hold a task slot for the whole of it,
#: and failing the day costs less than that.
_RETRY_AFTER_HEADER = "Retry-After"
_RETRY_AFTER_MAX = 300

#: Outcome a diagnostic event carries until the attempt determines its own.
#: It never reaches Loki: the request path replaces it before the push.
_OUTCOME_UNKNOWN = "unknown"

#: Outcomes the provider answers with a repeat: a refusal the server may not
#: repeat itself, and a request the network did not carry at all.  While an
#: attempt is left, an event carrying one of them is a warning rather than a
#: failure.
_RETRIED_OUTCOMES = frozenset({"retryable_error", "network_error"})

#: Character budget shared by every name and value of ``request_params``.
#: Loki's line limit counts bytes and the response body already spends most of
#: them, so what is left is shared out: 8 KiB of characters costs at most 32 KB
#: of line even when every one of them is an emoji, the widest a character gets
#: once JSON has escaped it.  A request whose parameters go on past that budget
#: is described up to it, and a marker says how many were left out.
_PARAMS_LIMIT = 8192

#: Key under which :func:`_redact_params` reports the parameters left out; its
#: value is how many there were.
_PARAMS_TRUNCATED = "<params truncated>"

#: Characters charged against the shared parameter budget for a value that is
#: not text: a number, a flag, an absent value, or the type name standing in
#: for a value of another kind.
_SCALAR_PARAM_COST = 16
_OTHER_PARAM_COST = 32

#: Response headers read for the rate-limit fields of an event.  AdMetrica
#: documents no headers of the kind, so these are the conventional spellings,
#: read in case the API sends them; an answer naming neither leaves both fields
#: empty.
_RATE_LIMIT_LIMIT_HEADER = "X-RateLimit-Limit"
_RATE_LIMIT_REMAINING_HEADER = "X-RateLimit-Remaining"

#: Character budget for a free-text diagnostic string (error messages,
#: exception texts).
_TEXT_LIMIT = 300

#: A run of whitespace or control characters in text the server wrote, replaced
#: by a single space before that text travels.  The task log gives one
#: unsuccessful attempt exactly one line, and a ``message`` holding a line break
#: would write several — a reader, or an alert counting lines, would see
#: attempts that never happened.
_WHITESPACE_RUN_RE = re.compile(r"[\s\x00-\x1f\x7f-\x9f]+")

#: Character budget for a response header value copied into an event.
_HEADER_LIMIT = 32

#: Character budget for a raw response body copied into an event.  Counted in
#: characters while Loki's line limit counts bytes: a body of control
#: characters, the widest they get after JSON escaping, makes a line of about
#: 200 KB against Loki's 256 KB default (``limits_config.max_line_size``).
_BODY_LIMIT = 32768

#: Encoding a response body is decoded with unless the server named one itself.
_ASSUMED_ENCODING = "utf-8"

#: Reads the ``charset`` parameter out of a ``Content-Type`` header, which is
#: the only place the server states an encoding of its own.
_CHARSET_RE = re.compile(r'charset\s*=\s*"?([\w.:+-]+)"?', re.IGNORECASE)

#: Marks a body cut to :data:`_BODY_LIMIT`; counted against the same budget.
#: Spelled out rather than the bare ellipsis :func:`_truncate` uses: on a 32 KB
#: body a single character would not read as a deliberate cut.
_TRUNCATED_SUFFIX = "…[truncated]"

#: Stands in for the live token wherever text that leaves the process spells it
#: out.  The two channels treat the token differently: text coming back from the
#: server has it cut out and replaced by this marker, while the request headers
#: the event carries hold the mask :func:`_mask_token` builds, whose ends tell
#: two tokens apart.
_TOKEN_REDACTED = "<token>"

#: Characters of an OAuth token kept at each end of its masked form.
_TOKEN_HEAD = 6
_TOKEN_TAIL = 4

#: Shortest token described by its ends rather than replaced whole: twice what
#: the mask shows, so at least half of any masked value stays hidden.
_TOKEN_MIN_LENGTH = 2 * (_TOKEN_HEAD + _TOKEN_TAIL)

#: The request header carrying the credential, and the scheme AdMetrica spells
#: it with.  The header value an event carries is rebuilt from these two and the
#: mask, never copied from the outgoing headers: a copy would travel as far as
#: whatever the value happened to hold.
_AUTH_HEADER = "Authorization"
_TOKEN_SCHEME = "OAuth"

#: JSON decoder messages copied into a diagnostic event verbatim.  Each is a
#: fixed literal of the standard decoder — the C scanner and the pure-Python
#: one together — naming the parse failure without quoting the document.  The
#: pure-Python scanner also formats two of its messages around a character
#: taken from the document (``Invalid \escape: 'q'``, ``Invalid control
#: character '\t' at``); those fall outside this set and are reported as
#: :data:`_DECODER_MESSAGE_OTHER`.
_DECODER_MESSAGES = frozenset(
    {
        "Expecting value",
        "Expecting ',' delimiter",
        "Expecting ':' delimiter",
        "Expecting property name enclosed in double quotes",
        "Extra data",
        "Unterminated string starting at",
        "Invalid control character at",
        "Invalid \\escape",
        "Invalid \\uXXXX escape",
    }
)

#: Stands in for a decoder message outside :data:`_DECODER_MESSAGES`.
_DECODER_MESSAGE_OTHER = "<other decoder message>"


def _truncate(value: str, limit: int = _TEXT_LIMIT, *, suffix: str = "…") -> str:
    """Bound a free-text diagnostic string to at most *limit* characters.

    The *suffix* marking the cut counts toward the limit, so the result never
    exceeds it; a limit too small to hold both a character of the value and the
    suffix yields as much of the suffix as fits.
    """
    if limit <= 0:
        return ""
    if len(value) <= limit:
        return value
    if limit <= len(suffix):
        return suffix[:limit]
    return value[: limit - len(suffix)] + suffix


def _one_line(value: str) -> str:
    """Flatten text the server wrote onto a single line of single spaces.

    Every run of whitespace or control characters becomes one space and the ends
    are trimmed, so the result holds no line break, no carriage return and no
    escape sequence a terminal would act on.  Text that says nothing else comes
    back empty, and the caller reads that as no text at all.

    Runs before the token is cut out: a value split across a line break is one
    value again once the break becomes a space, and the search that removes it
    works on text already in its final shape.
    """
    return _WHITESPACE_RUN_RE.sub(" ", value).strip()


def _mask_token(token: object) -> str:
    """Return an OAuth token reduced to its ends, enough to tell tokens apart.

    Only a value of exactly ``str`` is described at all: a subclass owns the
    length and slicing used below, and anything else would have to be rendered
    through code of its own.  A token shorter than :data:`_TOKEN_MIN_LENGTH` is
    replaced whole, so what the mask shows is never most of the value.  Runs
    while a diagnostic event is assembled, so it never raises.
    """
    if type(token) is not str or len(token) < _TOKEN_MIN_LENGTH:
        return "***"
    return f"{token[:_TOKEN_HEAD]}…{token[-_TOKEN_TAIL:]}"


def _drop_cut_token(text: str, token: str) -> str:
    """Replace a trailing beginning of *token* in *text* with :data:`_TOKEN_REDACTED`.

    A slice taken at a fixed size can land inside an echoed token, and the piece
    left behind is no longer the exact value that the search for whole
    occurrences cuts out.  The guarantee is about the value, so the longest
    ending of *text* that starts *token* goes as well.  What that costs is a few
    characters of a body already cut short, and they leave the same marker
    behind as the occurrences before them.
    """
    for size in range(min(len(token) - 1, len(text)), 0, -1):
        if text.endswith(token[:size]):
            return text[: len(text) - size] + _TOKEN_REDACTED
    return text


def _strip_token(text: str, token: object, *, cut: bool) -> str | None:
    """Return *text* with every trace of the live token replaced, or ``None``.

    Whole occurrences go first; *cut* says the text came from a slice taken at
    the byte budget, where the last occurrence can be a beginning of the token
    rather than all of it.  ``None`` means the text must not travel at all: an
    answer that spells the token out with something standing between its
    characters — UTF-16 read as UTF-8 leaves every one of them between NULs, and
    a line break inside the value does the same once flattening turns it into a
    space — puts the value out of reach of a search for it, and the value
    outranks the diagnostic.  That last check reads both the text and the token
    with every run of whitespace and control characters taken out, so what it
    compares is what a person reading the line would see.

    A token that is not a non-empty ``str`` names no value to look for, so the
    text passes through untouched.
    """
    if not isinstance(token, str) or not token:
        return text
    text = text.replace(token, _TOKEN_REDACTED)
    if cut:
        text = _drop_cut_token(text, token)
    needle = _WHITESPACE_RUN_RE.sub("", token)
    if needle and needle in _WHITESPACE_RUN_RE.sub("", text):
        return None
    return text


def _scrub(text: object, token: object, *, cut: bool = False) -> str | None:
    """The one gate every text passes through on its way out of the process.

    Request headers, response bodies, server-written error messages and the text
    of an exception all leave by this door, because the credential travels in a
    header and any of them can carry it back: a proxy echoes the
    ``Authorization`` header into an error page, an API reflects it inside a
    JSON ``message``, a network failure names the proxy URL it dialled.  One
    door means one place to read to know what a channel may carry, and a channel
    added later inherits the guarantee by calling it.

    ``None`` means the text must not travel: either it is not a ``str`` this
    module will operate on, or the token survived the search and the value
    outranks the diagnostic.  *cut* is for text taken from a slice at a byte
    budget, where the last occurrence of the token can be only its beginning.

    Never raises: it runs while an event is assembled, often with an exception
    already in flight.
    """
    if type(text) is not str:
        return None
    try:
        return _strip_token(text, token, cut=cut)
    except Exception:
        return None


def _safe_text(value: object, token: object, *, limit: int = _TEXT_LIMIT) -> str | None:
    """Return free text the server wrote, fit to travel, or ``None``.

    Flattens onto one line, cuts the token out and bounds the result, in that
    order: the flattening joins a value a line break had split, so the search
    sees the text in the shape it will travel in, and the budget counts that
    same shape.  Text that says nothing, and text the token survived, both come
    back as ``None``, which every caller reads as no text at all.
    """
    if type(value) is not str:
        return None
    flat = _one_line(value)
    if not flat:
        return None
    clean = _scrub(flat, token)
    if not clean:
        return None
    return _truncate(clean, limit)


def _exception_text(exc: BaseException, token: object) -> str:
    """Return the text of *exc* in the form an exception of this module may carry.

    The text of a failure the environment raised is not this module's writing: a
    network failure names the proxy it dialled, credentials and all, and a
    server's own words arrive inside whatever wrapped them.  It therefore leaves
    the process through the one gate every text leaves by, flattened onto a line
    and bounded like any other free text.

    Text that says nothing, and text the token survived, both come back as the
    type name alone: the exception that ends the attempt still names what
    happened, and no channel has to choose between saying too much and saying
    nothing.  Never raises — it runs with the real failure already in flight.
    """
    try:
        text = _safe_text(str(exc), token)
    except Exception:
        text = None
    return text or f"<{type(exc).__name__}>"


def _bounded_header(value: object) -> str | None:
    """Bound a response header value to a short, diagnostics-sized string.

    Only a value of exactly type ``str`` is copied.  Anything else the response
    object hands over is described by its type: converting an unknown object
    would run its own ``__str__`` and put whatever that returns into the event,
    and a ``str`` subclass could redefine truthiness, length and slicing — the
    operations used below.  Runs inside the instrumented request, on the path
    that decides whether a 429 is retried, so it never raises and never turns a
    retryable status into a hard failure.
    """
    if value is None:
        return None
    if type(value) is not str:
        return f"<non-str header: {type(value).__name__}>"
    if not value:
        return None
    return _truncate(value, limit=_HEADER_LIMIT)


def _redact(headers: object, token: object) -> dict | None:
    """Return the outgoing request headers in the form an event may carry.

    The ``Authorization`` value is rebuilt from the scheme and the mask rather
    than copied and edited, so what the event carries is bounded by construction
    whatever the outgoing value held.  Every other value is scrubbed and bounded
    like a header, so a header that happens to repeat the credential does not
    become the way out that the ``Authorization`` header is not.

    ``None`` says there were no headers to describe.  Never raises.
    """
    if not isinstance(headers, dict):
        return None
    redacted: dict = {}
    try:
        items = list(headers.items())
    except Exception:
        return None
    for key, value in items:
        if type(key) is not str:
            redacted[f"<non-str header name: {type(key).__name__}>"] = None
            continue
        if key.lower() == _AUTH_HEADER.lower():
            redacted[key] = f"{_TOKEN_SCHEME} {_mask_token(token)}"
            continue
        if type(value) is str:
            redacted[key] = _bounded_header(_scrub(value, token))
        else:
            redacted[key] = _bounded_header(value)
    return redacted


def _declared_charset(resp: object) -> str | None:
    """Return the charset the server named in ``Content-Type``, or ``None``.

    ``Response.encoding`` cannot answer this question: ``requests`` fills
    ``ISO-8859-1`` in for any ``text/*`` answer that names no charset, so that
    value stands both for a guess of its own and for a statement by the server,
    and a Russian-language error page from a proxy would arrive as mojibake if
    the guess were believed.  The header carries only what the server wrote.

    Only a header of exactly type ``str`` is read; a response object of unknown
    provenance is treated as one that named nothing.
    """
    headers = getattr(resp, "headers", None)
    value = headers.get("Content-Type") if hasattr(headers, "get") else None
    if type(value) is not str:
        return None
    match = _CHARSET_RE.search(value)
    return match.group(1) if match else None


def _bounded_body(resp: object, token: object) -> str | None:
    """Return the response text bounded to :data:`_BODY_LIMIT`, or ``None``.

    Decodes a bounded slice of the raw bytes rather than reading
    ``Response.text``.  The answer is whole in memory by the time the request
    returns, so what the slice bounds is the work spent on it — the decode and
    the copy the event carries away — and a proxy that answered with megabytes
    costs the same here as one that answered with kilobytes.  That matters
    because this runs with an exception in flight.

    A body that goes on past that slice ends with :data:`_TRUNCATED_SUFFIX`, and
    the bytes left behind are what puts it there: an answer cut short says so
    whatever its encoding decoded to and however much of it redaction removed.

    The charset the server named in ``Content-Type`` decides how the bytes are
    read, and :data:`_ASSUMED_ENCODING` stands in when it named none or named a
    codec Python does not know — a body no one can read is worth less here than
    one read on a good assumption.

    Runs inside the instrumented request, so it never raises on the failures it
    exists for: a missing response, bytes that cannot be read or decoded, and a
    token that answers with code of its own are reported as the absence of a
    value, never as an exception that would change what the caller sees.

    The live token is cut out by :func:`_scrub`: a server or a proxy can echo the
    ``Authorization`` header back in an error body, and the token is the one
    secret this event never carries.  An answer that keeps the token out of that
    gate's reach is dropped whole.
    """
    if resp is None:
        return None
    budget = _BODY_LIMIT * 4
    try:
        # Four bytes is the widest a UTF-8 character gets, so the slice always
        # holds at least `_BODY_LIMIT` characters; the truncation below trims
        # the rest.
        raw = resp.content[:budget]
        charset = _declared_charset(resp) or _ASSUMED_ENCODING
        try:
            text = raw.decode(charset, errors="replace")
        except LookupError:
            text = raw.decode(_ASSUMED_ENCODING, errors="replace")
        # One byte past the budget is what tells an answer that ends there from
        # one that goes on.  Four bytes per character — UTF-32, or UTF-8 spent
        # entirely on emoji — decode the slice to exactly `_BODY_LIMIT`
        # characters, and the length of the text alone would call such a body
        # whole.
        dropped = bool(resp.content[budget : budget + 1])
        text = _scrub(text, token, cut=dropped)
    except Exception:
        return None
    if text is None:
        return None
    if dropped:
        # The bytes left behind are what the mark reports, so it stays even
        # where redaction shrank the text back under the budget.
        text += _TRUNCATED_SUFFIX
    return _truncate(text, _BODY_LIMIT, suffix=_TRUNCATED_SUFFIX)


def _decoder_position(exc: BaseException) -> str | None:
    """Describe where JSON decoding gave up, or return ``None`` when unknowable.

    Only :class:`json.JSONDecodeError` is described, and only through wording
    this module chose in advance: the message is copied when it is one of the
    literals in :data:`_DECODER_MESSAGES` and replaced by
    :data:`_DECODER_MESSAGE_OTHER` otherwise, so ``exception_message`` stays a
    description of the failure rather than a quote from the document.  The
    document itself reaches the event through ``response_body``, where it is
    bounded and has the token cut out.  The coordinates are counted in the
    document and carry nothing from it.  Any other ``ValueError`` — a
    third-party decoder, a response object of unknown provenance — may quote
    what it was parsing and is left to ``exception_type`` alone.

    The value is rebuilt from the exception's own attributes rather than taken
    from its ``__str__``, and every attribute is used only at the exact type it
    is expected in.  Runs with a real exception in flight, so it never raises.
    """
    if not isinstance(exc, json.JSONDecodeError):
        return None
    msg = getattr(exc, "msg", None)
    lineno = getattr(exc, "lineno", None)
    colno = getattr(exc, "colno", None)
    pos = getattr(exc, "pos", None)
    known = type(msg) is str and msg in _DECODER_MESSAGES
    text = msg if known else _DECODER_MESSAGE_OTHER
    if type(lineno) is int and type(colno) is int and type(pos) is int:
        text = f"{text}: line {lineno} column {colno} (char {pos})"
    return _truncate(text)


def _redact_params(params: object, token: object) -> dict | None:
    """Return the request's query parameters in the form an event may carry.

    Text values pass the gate every text leaving the process passes: a
    parameter is written by this provider from the operator's own arguments,
    but ``extra_params`` lets a DAG add names of its own, and a credential put
    in one would otherwise travel unmasked.  A number, a flag or an absent
    value is copied as it is; anything else is described by its type, so that
    an object of unknown provenance never reaches the push to be serialized
    there.

    Every name and value shares the one budget of :data:`_PARAMS_LIMIT`
    characters, spent in the order the parameters come in.  Once it is gone the
    rest are left out and :data:`_PARAMS_TRUNCATED` says how many, so a
    parameter of any size costs the line a bounded amount.

    ``None`` says there were no parameters to describe.  Never raises: it runs
    while an event is assembled, often with an exception already in flight.
    """
    if not isinstance(params, dict):
        return None
    try:
        items = list(params.items())
    except Exception:
        return None
    redacted: dict = {}
    budget = _PARAMS_LIMIT
    for position, (key, value) in enumerate(items):
        if type(key) is not str:
            key = f"<non-str param name: {type(key).__name__}>"
        budget -= len(key)
        if budget <= 0:
            redacted[_PARAMS_TRUNCATED] = len(items) - position
            break
        if type(value) is str:
            text = _safe_text(value, token, limit=budget)
            redacted[key] = text
            budget -= len(text) if text else 0
        elif value is None or type(value) in (int, float, bool):
            redacted[key] = value
            budget -= _SCALAR_PARAM_COST
        else:
            redacted[key] = f"<non-scalar param: {type(value).__name__}>"
            budget -= _OTHER_PARAM_COST
    return redacted


def _new_event(
    *,
    endpoint: str,
    params: object,
    headers: object,
    token: object,
    advertiser_id: object = None,
    campaign_id: object = None,
    date: str | None = None,
    offset: int | None = None,
    attempt: int,
    max_attempts: int,
) -> dict:
    """Return a diagnostic event with every field of the schema present.

    The full field list of schema version :data:`_SCHEMA_VERSION` lives here:
    fields the attempt has yet to determine start as ``None``, so the key set of
    a pushed event is the same whatever happens to the request.

    The request is described from what the caller is about to send, and the two
    fields that carry text from outside are built here rather than stamped
    later: the headers through :func:`_redact`, which rebuilds the
    ``Authorization`` value from the scheme and the mask, and the parameters
    through :func:`_redact_params`.  The raw header value therefore never enters
    an event at all, whatever the request does with it afterwards.

    ``campaign_id``, ``date`` and ``offset`` belong to the statistics endpoint;
    a request for the campaign list leaves them empty.
    """
    return {
        "schema_version": _SCHEMA_VERSION,
        "outcome": _OUTCOME_UNKNOWN,
        "level": None,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "advertiser_id": advertiser_id,
        "campaign_id": campaign_id,
        "date": date,
        "offset": offset,
        "attempt": attempt,
        "max_attempts": max_attempts,
        # Both endpoints are read with GET, so the verb is spelled out here;
        # the URL comes from the same mapping the request reads it from.
        "request_method": "GET",
        "request_url": _ENDPOINT_URLS.get(endpoint),
        "request_params": _redact_params(params, token),
        "request_headers": _redact(headers, token),
        "http_status": None,
        "duration_ms": None,
        "rows_count": None,
        "rows_shape_ok": None,
        "payload_kind": None,
        "total_rows": None,
        "total_rows_rounded": None,
        "sampled": None,
        "sample_share": None,
        "sample_size": None,
        "sample_space": None,
        "contains_sensitive_data": None,
        "data_lag": None,
        "error_code": None,
        "error_message": None,
        "exception_type": None,
        "exception_message": None,
        "rate_limit_limit": None,
        "rate_limit_remaining": None,
        "response_body": None,
    }


def _is_empty_rows(rows: object) -> bool:
    """Whether a rows value that is not a list is an empty one.

    Separates a present-but-empty rows value from one that is neither a list
    nor empty, so the label says which of the two the body carried.  A value
    whose own ``__bool__`` raises is the loud half and is reported as such.
    Never raises.
    """
    try:
        return not rows
    except Exception:
        return False


def _summarize_rows(data: object, rows_key: str) -> tuple[int | None, bool, str]:
    """Return (rows_count, shape_ok, payload_kind) for a raw body.

    Never raises, and the guarantee is whole rather than a property of the
    values a real body holds: the answer decides whether the export goes on, and
    a body of unknown provenance answers ``dict.get``, the ``in`` test and the
    truthiness of a value with code of its own.  A body that refuses to be read
    that way is one nobody can read rows out of — ``"non_dict"``.

    ``shape_ok=True`` names the one body an export may go on reading: the rows
    value is a list and every element of it is a dict.  Anything else — an
    absent or non-list rows value, a list holding a non-dict — answers
    ``False``, and the attempt ends on that answer.  ``rows_count`` is ``None``
    whenever there is no list to count.

    ``payload_kind`` is a bounded label naming the level at which the body
    stopped matching, and the refusal is worded from it by
    :func:`_describe_unreadable_body`:

    * ``"dict"`` — the body is a dict holding the rows key, whether that value
      is the list it promises or an empty one of another type.
    * ``"rows_absent"`` — the body has no rows key at all.
    * ``"rows_non_list"`` — the rows key holds a non-empty value that is not a
      list.
    * ``"non_dict"`` — the body itself is not a dict.

    A well-formed answer, including the empty one meaning "no rows for this
    campaign on this day", is ``"dict"`` with ``shape_ok=True``; it is the only
    combination that leaves the task green.
    """
    try:
        if not isinstance(data, dict):
            return None, False, "non_dict"
        if rows_key not in data:
            return None, False, "rows_absent"
        rows = data.get(rows_key)
        if not isinstance(rows, list):
            return None, False, "dict" if _is_empty_rows(rows) else "rows_non_list"
        return len(rows), all(isinstance(row, dict) for row in rows), "dict"
    except Exception:
        return None, False, "non_dict"


def _classify_payload(event: dict, data: object) -> bool:
    """Describe an HTTP-200 body on *event* and answer whether it may be read.

    Fills in the count, the shape and the outcome so far, and returns the same
    ``rows_shape_ok`` the event receives.  The caller decides on the returned
    value rather than on ``event["rows_shape_ok"]``: reading the event back
    would make the diagnostics schema part of the export's behaviour, so that
    editing the set of fields would edit what the provider does.

    Which key holds the rows follows from the endpoint the event names, so one
    body-reading step serves both.  Never raises.
    """
    rows_key = _ROWS_KEYS.get(event["endpoint"], "")
    count, shape_ok, kind = _summarize_rows(data, rows_key)
    event["rows_count"], event["rows_shape_ok"], event["payload_kind"] = count, shape_ok, kind
    if kind == "non_dict":
        event["outcome"] = "unexpected_error"
    else:
        # `success` covers a well-formed answer, the empty one included;
        # `empty_shape` flags a body in which no list of rows was recognised,
        # and `payload_kind` names the level at which it stopped matching.
        event["outcome"] = "success" if shape_ok else "empty_shape"
    return shape_ok


def _find_error(data: object) -> object | None:
    """Return the object a body describes a refusal with, or ``None``.

    The specification describes the answer to a successful request and nothing
    else, so the shapes searched for here are a convention of this provider
    rather than a reading of a document.  Three are looked for, in this order:

    * ``{"error": {…}}`` — a refusal under a key of its own.
    * ``{"errors": [{…}, …]}`` — the plural spelling the Yandex API family
      writes; the first element is the one described, since the event carries
      one code and one message.
    * ``{"code": …, "message": …}`` — the pair written at the top level.  Both
      parts are required and ``code`` has to say something, so an answer that
      merely carries a ``message`` stays an answer rather than a refusal.

    Every shape is taken on truthiness rather than on ``isinstance(dict)``: a
    non-dict ``error`` is still an error, which :func:`_summarize_error`
    describes by its type, and a ``code`` of ``0`` names no refusal anyone can
    act on.

    Never raises.  The body normally comes from ``resp.json()`` and holds
    standard types, but the same path runs against a substituted response, so
    ``dict.get``, the ``in`` test and the truthiness of a value can each run
    code of their own.  All three live inside one ``try``, and a body that
    refuses to be read carries no error anyone can name — that is ``None``, not
    an exception.
    """
    try:
        if not isinstance(data, dict):
            return None
        error = data.get("error")
        if error:
            return error
        errors = data.get("errors")
        if isinstance(errors, list) and errors and errors[0]:
            return errors[0]
        if data.get("code") and "message" in data:
            return data
        return None
    except Exception:
        return None


def _summarize_error(error: object, token: object) -> tuple[int | None, str | None]:
    """Allowlist extractor for a refusal: only ``code``/``message``, bounded.

    Values of an unexpected type are described by their type rather than
    serialized, at both levels — a non-dict error and a non-str ``message`` —
    so the pair stays a short, queryable summary of the failure whatever the
    server put there, and a dashboard can group by it.  A code is copied only at
    exactly ``int``, which leaves a code spelled as text unread rather than
    guessed at.  Defensive against odd values: it must not itself raise and mask
    the real ``AirflowException``.

    The message passes :func:`_safe_text`, so it reaches the event flattened
    onto one line, bounded, and with the token cut out: a server or a proxy is
    free to quote the ``Authorization`` header back inside a JSON ``message``,
    and this pair words both the event's fields and the line the attempt logs.
    A message that survives none of that is reported as no message.
    """
    if not isinstance(error, dict):
        return None, f"<non-dict error: {type(error).__name__}>"
    raw_code = error.get("code")
    code = raw_code if type(raw_code) is int else None  # `type(...) is int` excludes bool
    message = error.get("message")
    if message is None:
        return code, None
    if type(message) is not str:
        return code, f"<non-str message: {type(message).__name__}>"
    return code, _safe_text(message, token)


def _describe_error_code(code: int) -> str:
    """Spell a code out for the task log: the number, bare.

    The API documents no codes, so a reading invented here would say more than
    is known, and nothing in this provider branches on the value: the policy for
    a refusal is decided by the HTTP status alone.  The argument is a code and
    only a code — :func:`_summarize_error` hands over ``int | None``, and the
    caller assembles the line without this part when there is no code.
    """
    return str(code)


def _describe_error(code: int | None, message: str | None) -> str:
    """Read a refusal out of its two parsed parts, for a human.

    The composition is the same wherever the refusal is told — the line an
    attempt logs and the text of the exception that ends the request: the code
    first, the server's own message after it.  Either part may be missing, and
    the phrase then holds what there is.  ``message`` arrives already bounded and
    masked by :func:`_summarize_error`.
    """
    head = f"code {_describe_error_code(code)}" if code is not None else None
    if head is not None and message:
        return f"{head}: {message}"
    return head or message or "no code and no message"


def _with_error(message: str, event: dict) -> str:
    """Append the refusal *event* parsed to *message*, when it parsed one.

    Every exception this module raises for an answer it read is worded this way,
    so a reader comparing two failures sees the same two halves in both: what the
    provider was doing and what the status was, then the server's own words for
    it.  An answer that named neither a code nor a message is left as it is
    rather than followed by an empty phrase.

    Reads the event's own keys, so it runs on an event this module assembled.
    """
    code, error_message = event["error_code"], event["error_message"]
    if code is not None or error_message:
        return f"{message}: {_describe_error(code, error_message)}"
    return message


def _describe_unreadable_body(event: dict) -> str:
    """Say what a body held instead of rows, in the terms the event describes it in.

    ``payload_kind`` names the level at which the body stopped matching, and
    ``rows_count`` separates the two defects that share the ``"dict"`` label: a
    rows value present but empty of another type counts nothing, while a list
    holding non-dict elements counts them.  The exception that ends the attempt
    and the line it logs are built from this one phrase, so a reader comparing
    the two never has to reconcile them.

    Reads the event's own keys, so it runs on an event this module assembled.
    """
    detail = f"payload_kind={event['payload_kind']}"
    if event["rows_count"] is not None:
        detail = f"{detail}, rows_count={event['rows_count']}"
    return f"no readable rows ({detail})"


def _describe_target(event: dict) -> str:
    """Name the request an attempt was made for, in the terms the event holds.

    The endpoint always, and each of the three locators the statistics endpoint
    fills in — the campaign, the day, the page — whenever the event carries it.
    A request for the campaign list names only what it has, so a line never
    claims a day or a campaign that no request was scoped to.

    Reads the event's own keys, so it runs on an event this module assembled.
    """
    located = " ".join(
        f"{name}={event[name]}"
        for name in ("campaign_id", "date", "offset")
        if event[name] is not None
    )
    head = f"AdMetrica {event['endpoint']}"
    return f"{head} {located}" if located else head


def _attempt_reason(event: dict) -> str:
    """Read a finished attempt as the phrase naming why it failed.

    Assembled from parsed fields only — the HTTP status, the refusal the body
    carried, the label naming what stood in place of rows, the position a JSON
    document broke at, the type of a network failure.  The raw answer belongs to
    the other channel: it travels in the event's ``response_body``, bounded and
    with the token cut out, and never reaches the task log.

    Reads the event's own keys, so it runs on an event this module assembled.
    """
    status = event["http_status"]
    parts = [f"HTTP {status}" if status is not None else "no response"]
    outcome = event["outcome"]
    code, message = event["error_code"], event["error_message"]
    if code is not None or message:
        parts.append(_describe_error(code, message))
    if outcome in ("empty_shape", "unexpected_error") and event["payload_kind"] is not None:
        parts.append(_describe_unreadable_body(event))
    elif outcome == "invalid_json":
        position = event["exception_message"]
        parts.append(f"invalid JSON ({position})" if position else "invalid JSON")
    elif outcome in ("network_error", "unexpected_error") and event["exception_type"] is not None:
        parts.append(event["exception_type"])
    return ", ".join(parts)


def _log_attempt(event: dict, retry_delay: float | None) -> None:
    """Write the one task-log line an unsuccessful attempt leaves behind.

    Every outcome but ``success`` gets a line, the last attempt included: the
    attempt after which the task fails is the one whose reason is wanted most,
    and a chronicle that stopped one line short of it would answer the question
    "what was tried, and why did it end" everywhere except there.  The final line
    differs by what it lacks — ``Retrying in N s`` appears only where a pause
    really follows, so a reader and a test tell the two apart by the same word.
    """
    line = (
        f"{_describe_target(event)}: "
        f"attempt {event['attempt']}/{event['max_attempts']} failed — "
        f"{_attempt_reason(event)}"
    )
    if retry_delay is not None:
        line = f"{line}. Retrying in {retry_delay} s"
    log.warning("%s", line)


def _stamp_response_error(event: dict, resp: object, token: object) -> None:
    """Copy the refusal a non-200 answer carries onto *event*.

    The body goes through the same :func:`_find_error` and
    :func:`_summarize_error` as an HTTP-200 one, so the exception, the line the
    attempt logs and the event's ``error_code``/``error_message`` tell one
    failure in one set of terms: a dashboard grouping by ``error_code`` sees the
    refusals that came with a status as well as the ones that came inside a 200.
    An answer that names no error this way — HTML from a proxy, an empty answer,
    a ``json()`` that raises — leaves the pair as it found it and is described by
    its status alone.

    Parsing the whole document is what ``resp.json()`` costs, and it is the same
    cost the HTTP-200 branch pays: what :func:`_bounded_body` bounds is the copy
    the event carries away, not the parse every branch needs.

    Never raises.  It runs with the branch's own exception about to be built, so
    a body that refuses to be read must leave both the type of that exception
    and the reason for it exactly as they are.
    """
    try:
        error = _find_error(resp.json())
        if error is not None:
            event["error_code"], event["error_message"] = _summarize_error(error, token)
    except Exception:
        pass


def _meta_value(value: object, token: object) -> object:
    """Return one report caveat in a form an event may carry.

    Numbers and flags travel as they arrived, because a dashboard reads them as
    numbers.  Text goes out through the gate every text goes out by, and a value
    of any other kind is described by its type rather than converted: converting
    it would run code the answer chose and put whatever it returns into the
    event.
    """
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    if type(value) is str:
        return _safe_text(value, token)
    return f"<{type(value).__name__}>"


def _stamp_report_meta(event: dict, data: object, token: object) -> None:
    """Copy what the answer says about its own page onto *event*.

    A caveat the answer carries — a rounded total, a sampled selection, rows
    withheld, data still catching up — describes the request that brought it,
    so it belongs to the event describing that request and is read there
    alongside the status and the duration.  A field the answer left out stays
    empty, so an event says "not declared" rather than inventing a value.

    Only the answer decides which of the fields are present, so this serves both
    endpoints: the campaign list declares none of them and passes through
    untouched.  Never raises — it runs on the successful path of an attempt that
    has a page to hand back.
    """
    try:
        if not isinstance(data, dict):
            return
        for field in _REPORT_META_FIELDS:
            if field in data:
                event[field] = _meta_value(data[field], token)
    except Exception:
        return


def _stamp_duration(event: dict, started: float) -> None:
    """Record how long the request took, in milliseconds since *started*."""
    event["duration_ms"] = round((time.monotonic() - started) * 1000)


def _record_rate_limit(event: dict, resp: requests.Response) -> None:
    """Copy the conventional rate-limit headers onto *event*, bounded.

    Runs inside the instrumented request, on the path that decides whether a 429
    is retried, so it never raises and never turns a retryable status into a hard
    failure.
    """
    try:
        headers = resp.headers
        event["rate_limit_limit"] = _bounded_header(headers.get(_RATE_LIMIT_LIMIT_HEADER))
        event["rate_limit_remaining"] = _bounded_header(headers.get(_RATE_LIMIT_REMAINING_HEADER))
    except Exception:
        pass


def _seconds_until(http_date: str) -> float | None:
    """Seconds from now to the moment an HTTP-date names, or ``None``.

    ``Retry-After`` is written either as a count of seconds or as the date at
    which the wait ends, and both spellings are in use; a date left unread would
    silently become the ladder's rung.  A date already past yields a negative
    value, which the caller reads as no wait at all.

    Never raises: it runs on a header a server wrote, and a value nobody can
    parse is a wait nobody asked for.
    """
    try:
        moment = parsedate_to_datetime(http_date)
    except Exception:
        return None
    if moment is None:
        return None
    try:
        if moment.tzinfo is None:
            # An HTTP-date without a zone is GMT by the specification, and
            # reading it as local time would move the wait by whole hours.
            moment = moment.replace(tzinfo=timezone.utc)
        return (moment - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return None


def _retry_after(resp: object) -> float | None:
    """The wait the server asked for, bounded, or ``None`` when it asked for none.

    Both spellings of the header are read, and the result is capped at
    :data:`_RETRY_AFTER_MAX`: the value comes from outside and a task slot held
    for hours costs more than the day it would have saved.  A wait of zero or
    less, a value in neither spelling, and a header of another type all mean the
    server named no wait, and the ladder decides instead.

    Runs on the path that decides how long a 429 waits, so it never raises and
    never turns a retryable status into a hard failure.
    """
    try:
        headers = getattr(resp, "headers", None)
        value = headers.get(_RETRY_AFTER_HEADER) if hasattr(headers, "get") else None
    except Exception:
        return None
    if type(value) is not str:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        seconds = _seconds_until(value)
    # Written as a comparison rather than as its negation so that a value of
    # `nan`, which answers every comparison with `False`, is no wait at all.
    if seconds is None or not seconds > 0:
        return None
    return min(seconds, _RETRY_AFTER_MAX)


def _retry_delay(resp: object, fallback: float) -> float:
    """How long to wait before the next attempt: the server's answer, or the rung.

    The server's own ``Retry-After`` wins whenever it named one, in both
    directions: a wait longer than the rung is a window that has not passed yet,
    and a shorter one is an invitation to come back sooner than the ladder would
    have.
    """
    asked = _retry_after(resp)
    return fallback if asked is None else asked


def _record_exception(event: dict, exc: BaseException) -> None:
    """Stamp the exception's type onto *event*, keeping the first one recorded.

    Only the type: an exception text reaching this point is assembled for a
    human reading the task log, and it carries the server's own ``message``,
    which the server is free to quote a token back into, or — for a network
    failure — the environment's proxy URL, credentials included.  The event
    describes the same failure through fields it builds itself: ``error_code``
    and ``error_message`` from the refusal the body carries, ``response_body``
    from a bounded slice of the answer with the token cut out, and the type alone
    for a network failure, where there is no response at all.  The one outcome
    whose text is safe by construction — a JSON document that would not parse —
    fills ``exception_message`` from :func:`_decoder_position`, which is built
    from the exception's own coordinates and quotes nothing.

    The first type recorded is the one kept: an attempt that failed and then
    failed again while being described is told by the failure that started it.
    """
    if event["exception_type"] is None:
        event["exception_type"] = type(exc).__name__


def _event_level(event: dict) -> str:
    """Return the severity of a finished attempt: ``info``, ``warn`` or ``error``.

    Answers "is the answer intelligible, and is there still hope", not "did the
    task fail": an answer nobody can read rows out of is an error at once, while
    an answer the next attempt may still fix is a warning.

    A well-formed answer is routine whether or not it carried rows: a campaign
    with no impressions on a day is the ordinary state of most of an
    advertiser's campaigns, and the caveats a successful answer can carry —
    sampling, rows the API withheld — travel as fields of their own and as
    warnings in the task log.  A refusal the provider repeats — a rate limit, a
    server-side failure, a network that did not carry the request — is a warning
    while an attempt is left and a failure on the last one.  A refusal to
    authorize is an error at first sight: the token is long-lived and nothing
    here refreshes it, so the attempt after it would be refused the same way.

    The level is the content policy as well as the severity: an answer that
    stays at ``info`` keeps its body inside the process, and moving a row of
    this table changes what leaves it.

    Reads the event's own keys, so it runs on an event this module assembled.
    """
    outcome = event["outcome"]
    if outcome == "success":
        return "info"
    if outcome in _RETRIED_OUTCOMES and event["attempt"] < event["max_attempts"]:
        return "warn"
    return "error"


def _emit_event(loki: object, event: dict, resp: object, token: object) -> None:
    """Finish *event* with the two fields the push needs and hand it to *loki*.

    The level decides the severity and the content together: an intelligible
    answer stays in the process, everything else travels with its body.

    A sink whose circuit breaker has tripped drops the event on arrival, so
    nothing is spent filling one nobody will read; a sink that offers ``push``
    alone counts as ready.  That question is asked here rather than at the call
    site: a sink answers it in code of its own, and this runs with an exception
    in flight that no answer of its may replace.
    """
    if not getattr(loki, "enabled", True):
        return
    event["level"] = _event_level(event)
    if event["level"] != "info":
        event["response_body"] = _bounded_body(resp, token)
    loki.push(event)


@dataclass
class AdvertiserConfig:
    """The advertiser one connection stands for.

    A connection carries a single advertiser, so its whole configuration is this
    one number: the ``advertiser_id`` every request names and every written
    record repeats.
    """

    advertiser_id: int


def _as_advertiser_id(value: object) -> int | None:
    """Return the advertiser id *value* names, or ``None``.

    An id written as text is an ordinary way to write it: ``extra`` is JSON
    typed by hand in the Airflow UI, where quoting a number is as natural as
    leaving it bare, so both forms name the same advertiser.

    A flag is not a number here even though Python counts one as an ``int``, a
    fractional value names no advertiser, and neither does a number at or below
    zero: ids the API issues start above it.
    """
    if type(value) is bool:
        return None
    if type(value) is int:
        return value if value > 0 else None
    if type(value) is str:
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed > 0 else None
    return None


def parse_connection(extra: dict) -> AdvertiserConfig | None:
    """Read ``connection.extra`` into an :class:`AdvertiserConfig`, or ``None``.

    The single point of knowledge about the shape of ``extra``, which is
    ``{"advertiser_id": 17004}``.  Everything else the connection holds is the
    connection's own business: the token lives in ``password``, and no other
    field is read.

    Best-effort — it never raises.  ``None`` means the connection names no
    usable advertiser, and the caller is the one that turns that into a failure
    a task reads; a WARNING says which of the ways it came out that way.

    The warnings name the type of an unusable value rather than the value:
    ``extra`` is a free-form field, and a credential pasted into the wrong key
    of it must not travel to the task log because the key was misspelled.
    """
    if not isinstance(extra, dict):
        log.warning(
            "Connection extra is not an object (got %s); it names no advertiser.",
            type(extra).__name__,
        )
        return None
    try:
        raw = extra.get("advertiser_id")
    except Exception:
        log.warning("Connection extra could not be read for 'advertiser_id'.")
        return None
    if raw is None:
        log.warning("Connection extra holds no 'advertiser_id'.")
        return None
    advertiser_id = _as_advertiser_id(raw)
    if advertiser_id is None:
        log.warning(
            "Connection extra field 'advertiser_id' is not a positive whole number (got %s).",
            type(raw).__name__,
        )
        return None
    return AdvertiserConfig(advertiser_id=advertiser_id)


def _token_from_password(password: object) -> str | None:
    """Return the OAuth token a connection's password holds, or ``None``.

    The password field holds the token itself.  A value written with the scheme
    in front of it names the same token, and the scheme is dropped here so that
    the outgoing header spells it exactly once: a doubled scheme is answered
    with a 401, which reads as a dead token rather than as a connection filled
    in one way instead of the other.

    ``None`` means there is no token to be had — an empty field, a field of
    spaces, or a field holding a scheme and nothing after it.
    """
    if type(password) is not str:
        return None
    value = password.strip()
    head, _, rest = value.partition(" ")
    token = rest.strip() if head.casefold() == _TOKEN_SCHEME.casefold() else value
    return token or None


def _declared_total(value: object) -> int | None:
    """Return the row count an answer declares, or ``None`` when none is readable.

    The count is what the pagination is checked against, so only a whole number
    counts as one: a flag, a string or a missing field say that the answer named
    no total, and the caller reports an unverified list rather than comparing
    against a value it invented.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _campaign_record(row: dict) -> dict:
    """Return one dictionary record: the named fields, in a fixed order.

    Values are handed on as the API worded them — an identifier stays a number,
    a date stays the string it arrived as — because the dictionary describes the
    campaign the API knows and any rewording here would be a second source of
    truth about it.

    A field the answer left out is present and empty, so every record of a file
    carries the same keys and a table schema written once holds them all.
    """
    return {field: row.get(field) for field in _CAMPAIGN_FIELDS}


def _resolve_placeholders(text: str, extra_params: dict | None) -> str:
    """Return *text* with every ``<parameter>`` replaced by its actual value.

    A parameterised name reaches the API in two spellings — the value written
    into the name, or a placeholder in the name and the value in a field of its
    own — and both describe the same column of the same report.  Substituting
    here makes the record key the same for both, so a report rewritten from one
    spelling to the other keeps writing the goal or the currency it already did.

    A placeholder no parameter answers stays in the key under its own name.  It
    reads as wrong wherever it lands, which is the point: dropping it instead
    would merge every goal of the account into one column, and the merge would
    only be visible as numbers that are too large.
    """
    if "<" not in text:
        return text
    params = extra_params if isinstance(extra_params, dict) else {}

    def substitute(match: re.Match) -> str:
        parameter = match.group(1)
        if parameter in params:
            return str(params[parameter])
        log.warning(
            "Name %r names the parameter %r, which the request does not carry; "
            "the record key keeps the parameter's name in place of its value.",
            text,
            parameter,
        )
        return parameter

    return _PLACEHOLDER_RE.sub(substitute, text)


def _normalize_name(name: object, extra_params: dict | None = None) -> str:
    """Return the record key a grouping or a metric is written under.

    One rule serves both, because both are named the same way by the API:
    the shared ``am:e:`` prefix comes off, a parameter spelled into the name is
    replaced by its value, and the camelCase that is left becomes snake_case —
    ``am:e:deviceType`` is ``device_type``, ``am:e:operatingSystemRoot`` is
    ``operating_system_root``, ``am:e:videoCompletePercent`` is
    ``video_complete_percent``, and ``am:e:interest2d1``, which has no capital
    in it, is ``interest2d1``.

    This key is a public contract: it is what an analyst writes in
    ``JSON_VALUE(dimensions.device_type)`` and what the documentation lists
    beside every name.  Changing the rule renames columns of every file written
    after the change while leaving the ones before it under the old names, and
    a query that names the old key answers NULL rather than failing.

    The fields *inside* a grouping's value are not touched by this: they are
    the API's own wording of what the grouping matched, and
    :func:`_map_row` keeps them as they arrived.
    """
    text = str(name)
    if text.startswith(_NAME_PREFIX):
        text = text[len(_NAME_PREFIX) :]
    text = _resolve_placeholders(text, extra_params)
    text = _WORD_SPLIT_RE.sub(r"\1_\2", _ACRONYM_SPLIT_RE.sub(r"\1_\2", text))
    return _UNDERSCORE_RUN_RE.sub("_", _NON_KEY_CHARS_RE.sub("_", text.lower())).strip("_")


def _row_values(raw_row: object, key: str) -> list:
    """Return the row's list under *key*, empty when the row carries none.

    A row that is not a dict, or one whose groupings or metrics are not a list,
    is answered as a row that brought nothing: the caller pairs by position and
    reports what it was short of, which says more than an exception raised over
    a single row of a page that arrived otherwise whole.
    """
    values = raw_row.get(key) if isinstance(raw_row, dict) else None
    return values if isinstance(values, list) else []


def _named_values(
    names: Sequence[str],
    values: list,
    extra_params: dict | None,
    kind: str,
    campaign_id: object,
) -> dict:
    """Pair requested *names* with the *values* the answer returned, by position.

    The answer carries values in the order they were asked for and names none of
    them, so position is the only thing tying a number to the metric it
    measures.  A name the answer has no value for is present and empty, which
    keeps every record of a request carrying the same keys; a value no name
    claims has nowhere to go and is left out.  Either way the mismatch is
    logged, because both mean the request and the answer disagree about the
    report.
    """
    if len(values) != len(names):
        log.warning(
            "AdMetrica returned %d %s value(s) for campaign %s where %d were asked "
            "for; the record keeps what lines up by position.",
            len(values),
            kind,
            campaign_id,
            len(names),
        )
    record: dict = {}
    for position, name in enumerate(names):
        key = _normalize_name(name, extra_params)
        if key in record:
            log.warning(
                "%s %r writes the record key %r, which an earlier %s of the same "
                "request already writes; the later value is the one in the record.",
                kind.capitalize(),
                name,
                key,
                kind,
            )
        record[key] = values[position] if position < len(values) else None
    return record


def _map_row(
    raw_row: object,
    date: str,
    advertiser_id: int,
    campaign_id: int,
    dimensions: Sequence[str],
    metrics: Sequence[str],
    extra_params: dict | None = None,
) -> dict:
    """Return one record of statistics built from one row of a report.

    Pure: it reads its arguments and returns a dict, so what a record looks like
    is decided in one place and can be checked without a network or a
    connection.

    The service fields are flat and typed.  ``date`` is stamped here because the
    report has no date in it at all: a day is asked for as ``date1=date2`` and
    the answer says nothing about which day it is, so the day the request was
    made for is the day the row belongs to.

    The variable half is two nested objects.  Under ``dimensions`` each value is
    the object the API returned, with exactly the fields it arrived with — a
    grouping that brings an ``id`` beside its ``name`` keeps it, one that brings
    only a ``name`` stays that way, and a field neither this provider nor its
    documentation has seen is carried through untouched.  Under ``metrics`` are
    the numbers, under the keys their names normalise to.  Nesting is what makes
    a new field in the answer a change to a JSON value rather than to a table
    schema, and what lets two rows of the same day carry different fields
    without either being padded out to match the other.

    Key order is fixed: the service fields, then the groupings in the order they
    were requested, then the metrics in theirs.  Files written from the same
    request are byte-comparable that way, which is what makes a re-export
    reviewable as a diff.
    """
    return {
        "date": date,
        "advertiser_id": advertiser_id,
        "campaign_id": campaign_id,
        "dimensions": _named_values(
            dimensions,
            _row_values(raw_row, "dimensions"),
            extra_params,
            "dimension",
            campaign_id,
        ),
        "metrics": _named_values(
            metrics,
            _row_values(raw_row, "metrics"),
            extra_params,
            "metric",
            campaign_id,
        ),
    }


def _row_key(raw_row: object) -> tuple:
    """Return the identity of one report row: what its groupings matched.

    The report is aggregated by the groupings that were asked for, so inside one
    answer the combination of their values names the row and no two rows carry
    the same one.  That makes the key the one thing a page can be checked
    against: a row seen twice while walking a campaign says the pages overlap,
    and pages that overlap are pages that also skip.

    The projection is spelled out rather than left to the objects themselves,
    which are dicts and cannot be put in a set.  Each grouping contributes its
    ``id`` where the answer carried one and its ``name`` otherwise, tagged with
    which of the two it is: two placements sharing a name and differing in ``id``
    are two rows, and reading the name alone would call the second one a
    duplicate and fail a day that is perfectly whole.
    """
    key = []
    for value in _row_values(raw_row, "dimensions"):
        if isinstance(value, dict) and "id" in value:
            key.append(("id", str(value["id"])))
        elif isinstance(value, dict):
            key.append(("name", str(value.get("name"))))
        else:
            key.append(("raw", str(value)))
    return tuple(key)


def _check_report_limits(dimensions: Sequence[str], metrics: Sequence[str]) -> None:
    """Fail on a request the API documents as too large, before it is sent.

    The ceilings are the documented ones, and the refusal names which list is
    over and by how much — an unchecked request comes back as a 400 whose body
    says considerably less, after the wait for it has been paid.
    """
    if len(metrics) > _MAX_METRICS:
        raise ValueError(
            f"AdMetrica accepts at most {_MAX_METRICS} metrics per request; "
            f"{len(metrics)} were given."
        )
    if len(dimensions) > _MAX_DIMENSIONS:
        raise ValueError(
            f"AdMetrica accepts at most {_MAX_DIMENSIONS} dimensions per request; "
            f"{len(dimensions)} were given."
        )


def _check_extra_params(extra_params: dict | None) -> None:
    """Fail on an ``extra_params`` key the hook itself owns.

    :data:`_RESERVED_PARAMS` says why each name is refused; the refusal is here,
    before the first request, so that a report configured this way never runs at
    all rather than running and writing plausible numbers about something other
    than what was asked for.
    """
    if not extra_params:
        return
    taken = sorted(set(extra_params) & _RESERVED_PARAMS)
    if taken:
        raise ValueError(
            f"extra_params may not carry {', '.join(taken)}: the hook builds "
            f"{', '.join(sorted(_RESERVED_PARAMS))} itself, and each of them has a "
            f"parameter of its own where the caller is meant to set it."
        )


def _log_report_caveats(page: dict, date: str, campaign_id: object) -> None:
    """Say in the task log what the answer said about its own numbers.

    Sampling and withheld rows are warnings because the numbers written are then
    not the whole truth about the day, and nothing downstream can tell that from
    the rows alone.  A lag is information: the day is whole as far as the API
    has it, and how far behind it is says whether the day is worth exporting
    again later.
    """
    if page.get("sampled"):
        log.warning(
            "AdMetrica sampled the report for campaign %s on %s (sample_share=%s, "
            "sample_size=%s of %s); the numbers are an estimate.",
            campaign_id,
            date,
            page.get("sample_share"),
            page.get("sample_size"),
            page.get("sample_space"),
        )
    if page.get("contains_sensitive_data"):
        log.warning(
            "AdMetrica withheld part of the rows for campaign %s on %s as sensitive; "
            "the day is short by whatever they held.",
            campaign_id,
            date,
        )
    data_lag = page.get("data_lag")
    if data_lag:
        log.info(
            "AdMetrica reports a data lag of %s for campaign %s on %s.",
            data_lag,
            campaign_id,
            date,
        )


class AdmetricaHook(BaseHook):
    """Hook for the AdMetrica report API, scoped to one advertiser.

    One connection is one advertiser: the OAuth token lives in the connection's
    password and the ``advertiser_id`` in its extra, so the advertiser a task
    works on is named in one place and read from it by everyone.

    Diagnostics are opt-in.  Without a sink the hook still runs and still logs;
    with one, every attempt at every endpoint leaves an event behind.
    """

    conn_name_attr = "admetrica_conn_id"
    default_conn_name = "yandex_admetrica_default"
    conn_type = "http"
    hook_name = "Yandex AdMetrica"

    def __init__(
        self,
        *,
        admetrica_conn_id: str = default_conn_name,
        loki: LokiClient | None = None,
        request_delay: float = _DEFAULT_REQUEST_DELAY,
        limit: int = _DEFAULT_LIMIT,
    ) -> None:
        super().__init__()
        self.admetrica_conn_id = admetrica_conn_id
        self.request_delay = request_delay
        self.limit = limit
        #: Diagnostics sink; ``None`` leaves every event unbuilt.
        self._loki = loki
        #: The connection, read on first use and kept for the hook's lifetime.
        self._connection: Connection | None = None
        #: The advertiser's campaigns, fetched on first use and kept: the
        #: statistics and the dictionary of one run are served by one answer.
        self._campaigns: list[dict] | None = None
        #: Whether a request has already gone out, which is what the pace is
        #: measured from: the first one of a task waits for nothing.
        self._request_made = False

    def _get_connection(self) -> Connection:
        """Return the connection, reading it from Airflow exactly once.

        A day of one advertiser is dozens of requests, and each of them needs
        the same token and the same advertiser id; reading them once means the
        secrets backend is asked once, whatever the export costs.
        """
        if self._connection is None:
            self._connection = self.get_connection(self.admetrica_conn_id)
        return self._connection

    def _get_extra(self) -> dict:
        """Return the connection's extra as an object, empty when unreadable.

        Text that is not JSON says the same thing as an extra without an
        advertiser in it, and the caller answers both with the one message that
        spells out the form the field takes.
        """
        try:
            extra = self._get_connection().extra_dejson
        except Exception:
            log.warning(
                "Connection %r has an extra that is not readable as JSON.",
                self.admetrica_conn_id,
            )
            return {}
        return extra if isinstance(extra, dict) else {}

    def _get_token(self) -> str:
        """Return the OAuth token, or fail with what to put where."""
        token = _token_from_password(self._get_connection().password)
        if token is None:
            raise AirflowException(
                f"Connection {self.admetrica_conn_id!r} holds no OAuth token. "
                f"Put the token in the connection's password field, without the "
                f"{_TOKEN_SCHEME!r} scheme in front of it."
            )
        return token

    def _pace(self) -> None:
        """Hold the export to ``request_delay`` seconds between requests.

        AdMetrica publishes neither a quota nor a rate, so the pace is what
        keeps a day of a large advertiser — dozens of requests in a row — from
        looking like a burst to whatever limiter stands in front of the API.

        The pause separates the requests the export makes, not the attempts a
        single one costs: a repeat already waits out its rung of the ladder, and
        a second pause on top of it would only lengthen a failure.
        """
        if self._request_made and self.request_delay > 0:
            time.sleep(self.request_delay)
        self._request_made = True

    def _request_page(self, endpoint: str, params: dict, event_fields: dict) -> dict:
        """GET one page from *endpoint*, retrying what a repeat can fix.

        The one place a request is made, and it knows nothing of the loops above
        it: a campaign, a day and a page reach it as parameters to send and as
        *event_fields* for the diagnostics to name the request by.  The endpoint
        arrives as the label an event carries rather than as an address, so that
        the request and the event describing it can never name two different
        endpoints — :data:`_ENDPOINT_URLS` holds both halves.

        A 429 or a 5xx and a request the network did not carry are repeated
        along :data:`_BACKOFF_DELAYS`, and the wait is the server's own
        ``Retry-After`` wherever it named one.  A 401 fails at once: the token is
        long-lived and nothing here refreshes it, so the attempt after it would
        be refused the same way.  A 400 or any other 4xx fails at once too, with
        the server's words for it read out of the body — the request itself is
        what is being refused, and a repeat brings back the same answer.

        An HTTP-200 body is asked one question: is the rows value the list of
        objects it promises.  Only a body that answers yes — the empty list of a
        campaign with no impressions on a day included — comes back to the
        caller; every other one fails the attempt naming the ``payload_kind``
        that describes it, so a zero the provider could not read is never handed
        on as a zero the API reported.

        Every attempt that did not bring a page back leaves one line in the task
        log, the last one included, so that a minute of waiting reads as a
        chronicle rather than as a silence.  The line carries parsed fields only.

        Every attempt emits exactly one diagnostic event when a sink is
        configured: how the attempt went, the request as it went out with the
        token masked, and — for anything :func:`_event_level` rates above
        ``info`` — the raw body, bounded and with the token cut out.  Emission
        never affects control flow: it lives in ``finally`` and every failure of
        its own is swallowed there.
        """
        url = _ENDPOINT_URLS[endpoint]
        token = self._get_token()
        # Spelled out here so that this dict names every header the provider
        # chooses and the event's copy of it names them all too; the connection
        # headers `requests` adds are outside it.
        headers = {
            "Authorization": f"{_TOKEN_SCHEME} {token}",
            "Accept": "application/json",
        }
        max_attempts = len(_BACKOFF_DELAYS) + 1
        self._pace()

        for attempt in range(max_attempts):
            event = _new_event(
                endpoint=endpoint,
                params=params,
                headers=headers,
                token=token,
                attempt=attempt + 1,
                max_attempts=max_attempts,
                **event_fields,
            )
            # Set where a pause follows, and read at the foot of the loop as the
            # one answer to "does anything follow this attempt".
            retry_delay: float | None = None
            last_attempt = attempt == max_attempts - 1
            # Per attempt, so that a network failure reports the absence of a
            # response instead of the body the previous attempt received.
            resp: requests.Response | None = None
            try:
                started = time.monotonic()
                try:
                    resp = requests.get(
                        url, params=params, headers=headers, timeout=_REQUEST_TIMEOUT
                    )
                except requests.RequestException as e:
                    _stamp_duration(event, started)
                    event["outcome"] = "network_error"
                    _record_exception(event, e)
                    if last_attempt:
                        raise AirflowException(
                            f"Request to {url} failed after {max_attempts} attempts: "
                            f"{_exception_text(e, token)}"
                        ) from e
                    retry_delay = _BACKOFF_DELAYS[attempt]
                else:
                    _stamp_duration(event, started)
                    event["http_status"] = resp.status_code

                    # One chain, not a run of separate `if`s: the retryable
                    # branch has a path that neither returns nor raises — it
                    # names a pause and leaves for `time.sleep` at the foot of
                    # the loop.
                    if resp.status_code == 200:
                        try:
                            data = resp.json()
                        except ValueError as e:  # JSONDecodeError subclasses ValueError
                            event["outcome"] = "invalid_json"
                            event["exception_message"] = _decoder_position(e)
                            _record_exception(event, e)
                            position = event["exception_message"]
                            detail = f" ({position})" if position else ""
                            raise AirflowException(
                                f"AdMetrica {endpoint} returned an HTTP 200 body "
                                f"that is not JSON{detail}"
                            ) from e

                        if _classify_payload(event, data):
                            _stamp_report_meta(event, data, token)
                            return data

                        # No list of rows was recognised, so there is no page to
                        # hand on.  Ending the attempt here is what keeps a zero
                        # from an unreadable answer apart from the zero of a
                        # campaign that had no impressions.
                        error = _find_error(data)
                        if error is not None:
                            event["error_code"], event["error_message"] = _summarize_error(
                                error, token
                            )
                        raise AirflowException(
                            _with_error(
                                f"AdMetrica {endpoint} returned HTTP 200 with "
                                f"{_describe_unreadable_body(event)}",
                                event,
                            )
                        )

                    elif resp.status_code == 401:
                        event["outcome"] = "auth_error"
                        _stamp_response_error(event, resp, token)
                        raise AirflowException(
                            _with_error(
                                f"AdMetrica {endpoint} returned 401 Unauthorized: the OAuth "
                                f"token in connection {self.admetrica_conn_id!r} was refused, "
                                f"and nothing here refreshes it",
                                event,
                            )
                        )

                    elif resp.status_code in _RETRY_STATUSES:
                        event["outcome"] = "retryable_error"
                        if resp.status_code == 429:
                            _record_rate_limit(event, resp)
                        _stamp_response_error(event, resp, token)
                        if last_attempt:
                            raise AirflowException(
                                _with_error(
                                    f"AdMetrica {endpoint} returned {resp.status_code} for "
                                    f"{url} on attempt {max_attempts} of {max_attempts}",
                                    event,
                                )
                            )
                        retry_delay = _retry_delay(resp, _BACKOFF_DELAYS[attempt])

                    else:
                        event["outcome"] = "http_error"
                        _stamp_response_error(event, resp, token)
                        raise AirflowException(
                            _with_error(
                                f"AdMetrica {endpoint} returned {resp.status_code} for {url}",
                                event,
                            )
                        )
            except BaseException as e:
                # Safety net: record what escaped, then let the original
                # exception through untouched — type, message and traceback are
                # the caller's contract.  `BaseException` so that an interrupted
                # attempt is classified too: the push in `finally` runs for those
                # as well, and `outcome` must never reach Loki as the
                # placeholder "unknown".
                _record_exception(event, e)
                if event["outcome"] == _OUTCOME_UNKNOWN:
                    event["outcome"] = "unexpected_error"
                raise
            finally:
                # An interruption on its way out means the task is being
                # stopped, with the alarm or signal behind it firing once:
                # pushing here would hold the stop for the length of a push, so
                # the attempt a stop cut short goes unreported, deliberately —
                # the reason for it is in the Airflow task log.
                in_flight = sys.exc_info()[1]
                if in_flight is None or isinstance(in_flight, Exception):
                    try:
                        if event["outcome"] != "success":
                            # The task log gets the chronicle of the page: one
                            # line per attempt that did not bring one back,
                            # whether a pause follows it or the exception in
                            # flight ends the export.
                            _log_attempt(event, retry_delay)
                        if self._loki is not None:
                            _emit_event(self._loki, event, resp, token)
                    except Exception as diag_error:
                        # Defense in depth: a raise here would replace the
                        # exception in flight, so everything reporting does —
                        # reading the body and wording the line included — is
                        # covered.  The type, not the text: an unexpected
                        # failure can carry the environment's proxy URL,
                        # credentials included.
                        log.debug(
                            "Diagnostics raised %s; the event is dropped",
                            type(diag_error).__name__,
                        )

            # Reached only where an attempt named a pause: a non-final retryable
            # status, or a request the network did not carry with attempts left.
            # The event is already pushed, so a Loki outage never delays its
            # visibility.
            time.sleep(retry_delay)

    @property
    def advertiser_id(self) -> int:
        """The advertiser this hook's connection stands for.

        The way out of the connection for a number the written records carry and
        the paths in S3 are built from: a caller that needs the advertiser reads
        it here instead of being configured with it a second time.
        """
        config = parse_connection(self._get_extra())
        if config is None:
            raise AirflowException(
                f"Connection {self.admetrica_conn_id!r} names no advertiser. "
                f"Its extra must hold a positive whole 'advertiser_id', as in "
                f'{{"advertiser_id": 17004}}.'
            )
        return config.advertiser_id

    def get_campaigns(self) -> list[dict]:
        """Return the advertiser's campaigns, one record per campaign.

        Every status is asked for: no ``status`` goes out with the request,
        because an archived campaign ran in the past and its statistics are as
        real as an active one's — a filter here would silently shorten every
        re-export of an earlier period.

        Pagination walks the list with ``limit`` and ``offset`` spelled out, the
        offset counting the rows already skipped and therefore starting at zero.
        It ends on the declared total or on a page shorter than the one asked
        for, and the count collected is checked against the total afterwards:
        a page cut short before the total is closed means whole campaigns were
        lost, and with them every row of statistics they would have contributed,
        so the day fails instead of arriving short.  An answer that declares no
        readable total leaves that check nothing to compare against and says so
        in the log.

        The list is fetched once and kept for the hook's lifetime: the
        statistics and the dictionary of one run are two readers of one answer.

        ``snapshot_date`` is not here.  The field belongs to the operator, which
        writes it into every record, names the file with the same date and
        reports it as the date of the result, so path, column and partition can
        never disagree.
        """
        if self._campaigns is not None:
            return self._campaigns

        advertiser_id = self.advertiser_id
        campaigns: list[dict] = []
        total: int | None = None
        offset = 0
        while True:
            page = self._request_page(
                "campaigns",
                {
                    "advertiser_id": advertiser_id,
                    "limit": _CAMPAIGNS_LIMIT,
                    "offset": offset,
                },
                {"advertiser_id": advertiser_id, "offset": offset},
            )
            # A list of dicts under the endpoint's rows key is the only body the
            # request path hands back at all; every other shape has already
            # failed the attempt there.
            rows = page["campaigns"]
            campaigns.extend(_campaign_record(row) for row in rows)
            if total is None:
                total = _declared_total(page.get("total"))
            if len(rows) < _CAMPAIGNS_LIMIT:
                break
            offset += len(rows)
            if total is not None and len(campaigns) >= total:
                break

        if total is None:
            log.warning(
                "AdMetrica declared no readable campaign total for advertiser %s; "
                "%d campaigns were collected and their completeness is unverified.",
                advertiser_id,
                len(campaigns),
            )
        elif len(campaigns) != total:
            raise AirflowException(
                f"AdMetrica returned {len(campaigns)} campaigns for advertiser "
                f"{advertiser_id} while declaring {total}. A campaign missing from "
                f"the list takes all of its statistics with it, so the export stops "
                f"here rather than writing a day that is short."
            )

        self._campaigns = campaigns
        return campaigns

    def _maskable_token(self) -> str | None:
        """Return the token already read, or ``None`` while none has been.

        What the masking gate looks for, taken from the connection this hook
        holds rather than by reading one: it answers while a failure is being
        described, and a failure that arrives before the connection was read
        arrived before anything could carry the token, so there is nothing to
        search the text for.  Never raises.
        """
        connection = self._connection
        if connection is None:
            return None
        try:
            return _token_from_password(connection.password)
        except Exception:
            return None

    def test_connection(self) -> tuple[bool, str]:
        """Answer the Test Connection button: does this connection work.

        The check is the campaign list, because it exercises at once everything
        a task depends on — the token is accepted, the advertiser is real and
        the account may read the API — and it costs one request to an endpoint
        that returns no statistics.

        Both halves of the answer belong to the button, so nothing leaves here
        as an exception, and the text of a failure passes through the same gate
        every text leaves this module by: a server, a proxy or a wrapped network
        failure can word an error with the credential inside it, and the button
        shows its text to whoever pressed it.
        """
        try:
            # First, so that a connection Airflow cannot find is reported as
            # itself: everything after this reads the connection through a
            # best-effort path that would describe its absence as an extra
            # missing an advertiser.
            self._get_connection()
            advertiser_id = self.advertiser_id
            campaigns = self.get_campaigns()
        except Exception as exc:
            return False, _exception_text(exc, self._maskable_token())
        return (
            True,
            f"Connected to AdMetrica as advertiser {advertiser_id}: "
            f"{len(campaigns)} campaigns are readable.",
        )

    def get_stats(
        self,
        date: str,
        dimensions: Sequence[str],
        metrics: Sequence[str],
        *,
        filters: str | None = None,
        accuracy: str | None = "full",
        include_undefined: bool = True,
        timezone: str | None = None,
        lang: str | None = None,
        extra_params: dict | None = None,
    ) -> list[dict]:
        """Return one day of statistics for every campaign of the advertiser.

        A day is the unit because the report has no date in it: ``/v1/stat/data``
        is asked for ``date1=date2=<day>`` and the day is stamped onto every
        record here.  Asking a day at a time also keeps each selection small,
        which is what makes sampling unlikely enough for ``accuracy="full"`` to
        be answered in full.

        A campaign is a request because the API offers no grouping by campaign
        and sums the campaigns named in ``ids`` together: one request per
        campaign is the only way the split survives at all.  The list of
        campaigns comes from :meth:`get_campaigns`, so one run asks the
        management API once however many days it exports.

        ``sort`` goes out naming every grouping that was asked for.  The report
        is aggregated by them, so their combination orders the rows completely
        and repeatably from one page to the next; sorting by a metric would not,
        since a long tail of placements shares ``renders=1`` and the order
        within that tail is the API's to choose — and rows would move between
        pages while the walk was reading them.

        The documented ceilings and the parameters this hook owns are checked
        before the first request, so a report configured wrongly costs nothing
        and says what is wrong.

        Raises :class:`ValueError` on a request the API documents as too large
        or on an ``extra_params`` key the hook owns, and
        :class:`~airflow.exceptions.AirflowException` on a day that came back
        incomplete.
        """
        dimensions = list(dimensions)
        metrics = list(metrics)
        _check_report_limits(dimensions, metrics)
        _check_extra_params(extra_params)

        advertiser_id = self.advertiser_id
        base_params = self._report_params(
            date=date,
            dimensions=dimensions,
            metrics=metrics,
            filters=filters,
            accuracy=accuracy,
            include_undefined=include_undefined,
            timezone=timezone,
            lang=lang,
            extra_params=extra_params,
        )

        records: list[dict] = []
        for campaign in self.get_campaigns():
            records.extend(
                self._collect_campaign(
                    date=date,
                    advertiser_id=advertiser_id,
                    campaign_id=campaign["campaign_id"],
                    base_params=base_params,
                    dimensions=dimensions,
                    metrics=metrics,
                    extra_params=extra_params,
                )
            )
        return records

    def _report_params(
        self,
        *,
        date: str,
        dimensions: Sequence[str],
        metrics: Sequence[str],
        filters: str | None,
        accuracy: str | None,
        include_undefined: bool | None,
        timezone: str | None,
        lang: str | None,
        extra_params: dict | None,
    ) -> dict:
        """Build the part of the query that is the same for every campaign.

        Everything the report is defined by lives here and is built once; what
        varies between requests — the campaign and the page — is added at the
        call site, so nothing about the report can differ from one campaign to
        the next.

        A parameter left unset is left out of the query, which is how the API's
        own default is asked for.  ``include_undefined`` is spelled in the
        lowercase the API writes flags in rather than in Python's capitalised
        form.

        Merging is one-directional: ``extra_params`` adds names the query does
        not already carry and overrides none.  What it may not carry at all was
        refused before this ran.
        """
        params: dict = {
            "metrics": ",".join(metrics),
            "date1": date,
            "date2": date,
            "limit": self.limit,
        }
        if dimensions:
            params["dimensions"] = ",".join(dimensions)
            params["sort"] = ",".join(dimensions)
        if accuracy is not None:
            params["accuracy"] = accuracy
        if include_undefined is not None:
            params["include_undefined"] = "true" if include_undefined else "false"
        if filters is not None:
            params["filters"] = filters
        if timezone is not None:
            params["timezone"] = timezone
        if lang is not None:
            params["lang"] = lang
        for name, value in (extra_params or {}).items():
            params.setdefault(name, value)
        return params

    def _collect_campaign(
        self,
        *,
        date: str,
        advertiser_id: int,
        campaign_id: object,
        base_params: dict,
        dimensions: Sequence[str],
        metrics: Sequence[str],
        extra_params: dict | None,
    ) -> list[dict]:
        """Walk one campaign's report for one day and return its records.

        The offset counts rows from one, which is this endpoint's own numbering:
        a walk starting at zero would ask for the first row twice.

        Where the walk stops depends on ``total_rows_rounded``.  With the flag
        clear the declared total is a number to stop on, alongside a page
        shorter than the one asked for.  With it set only the short page stops
        the walk, because a rounded total is unusable as a stopping condition:
        10 437 rows declared as 10 000 against ``limit=10000`` would fill the
        first page exactly, match the total, and end the export with the tail
        left behind.  The flag is remembered once seen, so a total declared
        exactly on an earlier page cannot re-arm a check the answer has since
        disowned.

        Rows are checked against each other by the identity of their groupings,
        and the set of what has been seen lives here — one campaign, one set.
        The combination is unique inside one report and repeats between reports
        as a matter of course, since one placement runs in several campaigns of
        the same advertiser; a set shared across campaigns would fail an export
        of perfectly ordinary data.  A repeat inside one campaign fails the day:
        pages that overlap are pages that also skip, and counting rows cannot
        tell the two apart.  With no groupings asked for the report is a single
        row, there is no pagination and nothing to check.
        """
        records: list[dict] = []
        seen: set[tuple] = set()
        total: int | None = None
        rounded = False
        offset = _STAT_OFFSET_BASE

        while True:
            page = self._request_page(
                "stat",
                {"ids": campaign_id, **base_params, "offset": offset},
                {
                    "advertiser_id": advertiser_id,
                    "campaign_id": campaign_id,
                    "date": date,
                    "offset": offset,
                },
            )
            # A list of dicts under the endpoint's rows key is the only body the
            # request path hands back at all; every other shape has already
            # failed the attempt there.
            rows = page["data"]
            _log_report_caveats(page, date, campaign_id)
            if total is None:
                total = _declared_total(page.get("total_rows"))
            rounded = rounded or bool(page.get("total_rows_rounded"))

            for raw_row in rows:
                if dimensions:
                    key = _row_key(raw_row)
                    if key in seen:
                        raise AirflowException(
                            f"AdMetrica returned a row for campaign {campaign_id} on "
                            f"{date} that an earlier page of the same campaign already "
                            f"carried ({key}). Pages that repeat a row are pages that "
                            f"skip another one, so the day stops here rather than "
                            f"being written with a hole in it."
                        )
                    seen.add(key)
                records.append(
                    _map_row(
                        raw_row,
                        date,
                        advertiser_id,
                        campaign_id,
                        dimensions,
                        metrics,
                        extra_params,
                    )
                )

            if len(rows) < self.limit:
                break
            offset += len(rows)
            if not rounded and total is not None and len(records) >= total:
                break

        self._check_row_total(
            collected=len(records),
            total=total,
            rounded=rounded,
            date=date,
            campaign_id=campaign_id,
        )
        return records

    @staticmethod
    def _check_row_total(
        *,
        collected: int,
        total: int | None,
        rounded: bool,
        date: str,
        campaign_id: object,
    ) -> None:
        """Compare what was collected against what the report declared.

        An exact total that does not match means pages went missing, and rows
        lost this way are lost silently: the file is written, the day looks
        exported, and what is not in it is discovered weeks later when the
        period can no longer be re-requested.  So the day fails instead.

        A rounded total cannot be matched exactly by definition, so the same
        difference is a warning: something to read when the numbers look wrong,
        not a reason to fail a day that is probably whole.  An answer declaring
        no readable total leaves the check nothing to compare against and says
        so, so that an unverified day is at least visibly unverified.
        """
        if total is None:
            log.warning(
                "AdMetrica declared no readable total_rows for campaign %s on %s; "
                "%d rows were collected and their completeness is unverified.",
                campaign_id,
                date,
                collected,
            )
            return
        if collected == total:
            return
        summary = (
            f"AdMetrica returned {collected} rows for campaign {campaign_id} on "
            f"{date} while declaring {total}"
        )
        if rounded:
            log.warning(
                "%s. total_rows_rounded is set, so the declared number is an "
                "approximation and the collected rows are the ones written.",
                summary,
            )
            return
        raise AirflowException(
            f"{summary}. Rows lost between pages are lost silently, so the day "
            f"stops here rather than being written short."
        )
