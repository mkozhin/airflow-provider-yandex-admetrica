"""Hook for the Yandex Metrica for Display Advertising (AdMetrica) report API."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
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
