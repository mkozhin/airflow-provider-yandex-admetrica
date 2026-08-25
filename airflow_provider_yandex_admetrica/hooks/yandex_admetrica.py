"""Hook for the Yandex Metrica for Display Advertising (AdMetrica) report API."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from datetime import timezone as _timezone
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


class _MaskedError(AirflowException):
    """A failure this module worded, out of text the masking gate has seen.

    Every failure raised here is one of these, and the request path recognises
    its own failures by this class alone.  ``AirflowException`` is the whole
    scheduler's currency — a wrapper around ``requests``, a response adapter or
    a piece of middleware may raise one too — and its text is then the
    environment's own writing, which can name the proxy it dialled credentials
    and all.  A class only this module raises tells the two apart exactly, so
    text that has never passed the gate is re-worded on its way out instead of
    travelling as it is.
    """


#: Version of the diagnostic event's field set.  A reader of a stored event
#: knows by this number which fields it may expect.
_SCHEMA_VERSION = 2

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

#: How a day is written wherever one is named.  ``date1`` and ``date2`` carry a
#: day in this spelling and the endpoint reads no other; the same string names
#: the file an export writes and the partition it lands in, so one format holds
#: for the request and for everything downstream of it alike.
DATE_FORMAT = "%Y-%m-%d"

#: Seconds of quiet between two requests.  AdMetrica publishes neither a quota
#: nor a rate, so the pace is a conservative guess a task may raise or lower
#: through the operator once a real advertiser has been measured.
DEFAULT_REQUEST_DELAY = 0.2

#: Rows one statistics page asks for.  The endpoint allows up to 100 000 and
#: falls back to 100 when asked for nothing; a tenth of the ceiling keeps a day
#: of a large advertiser inside a few pages while leaving a single answer small
#: enough to hold in memory and to retry cheaply.
DEFAULT_LIMIT = 10000

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
_MAX_LIMIT = 100000

#: How much one walk may collect before it is called a walk that does not end.
#: Both endpoints are paginated by an ``offset`` the answer is free to ignore,
#: and an endpoint that answers every offset with a full page of rows would be
#: read forever: the export would hold a task slot, fill a disk and never
#: finish.  Ten million rows of one campaign on one day, and a million campaigns
#: in one advertiser's list, are far past any real advertiser, so reaching
#: either says the walk is not converging rather than that the data is large.
_MAX_ROWS = 10_000_000
_MAX_CAMPAIGNS = 1_000_000

#: Requests one walk may spend reaching the ceiling above.  The ceiling is
#: counted in rows, so on its own it says nothing about how long a walk may
#: take: a page of ten rows would be asked for a million times before it was
#: reached.  This is what ends such a walk instead.
_MAX_PAGES = 100_000

#: Request parameters the hook owns, which ``extra_params`` may therefore not
#: carry.  Each of them is either the question being asked or an answer to how
#: it is asked, and a silent override of one would be invisible in the data:
#: another ``date1`` would fetch another day while the records still carry the
#: operator's date, and ``accuracy`` or ``include_undefined`` would drop the
#: defaults that stand against drifting and truncated numbers — with the
#: completeness check still passing, because ``total_rows`` agrees with the
#: truncated selection.  ``preset`` is refused for the same reason one step
#: further out: it lets the API define the report's own metrics and dimensions,
#: while the records pair returned values by position against the names the
#: caller asked for, so the numbers would be written under the wrong keys.
#: ``filters``, ``timezone``, ``lang``, ``accuracy`` and ``include_undefined``
#: have parameters of their own on the operator, so nothing needs this route to
#: reach them.
_RESERVED_PARAMS = frozenset(
    {
        "ids",
        "date1",
        "date2",
        "metrics",
        "dimensions",
        "preset",
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

#: Statuses that name the token rather than the request.  Both say the same
#: thing about a long-lived credential nothing here refreshes — the API answers
#: a token it will not accept with either of them — so both end the attempt
#: where it stands.
_AUTH_STATUSES = frozenset({401, 403})

#: The pause before each repeat of a rate limit, a server-side failure or a
#: request the network did not carry, in seconds.  The length of the ladder is
#: also the number of repeats, so a request gets one attempt plus one rung per
#: pause.  AdMetrica publishes no quota, so the ladder is short and
#: conservative: a refusal the server means to last is answered within seconds
#: rather than minutes, and what a task spends on retries stays small.
_BACKOFF_DELAYS = [1, 2, 4]

#: The pause before each repeat of a refusal the API states in the body of a
#: 400, in seconds.  The span of the ladder is a minute because the condition
#: behind such a refusal drifts on the scale of minutes: a request refused now
#: is answered once the window it fell into has passed.
_QUERY_ERROR_DELAYS = [5, 15, 45]

#: The values of ``error_type`` a repeat can fix.  The check is membership of
#: this set rather than the status alone: a 400 carries every complaint the API
#: has about a request, and only the one named here says the request is sound
#: and the moment was not.
_RETRYABLE_ERROR_TYPES = frozenset({"query_error"})

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

#: What ``request_params`` may cost the line: how many parameters are described
#: at all, and how long a name and a text value of one may be.  Loki's line
#: limit counts bytes and the response body already spends most of them, so the
#: three together are chosen so that their product — every described parameter
#: at the full length of both of its bounds — is at most 32 KB of line even when
#: every character is an emoji, the widest one gets once JSON has escaped it.
#: Each bound is its own: a name and a value are cut where they run past theirs,
#: with the ellipsis of :func:`_truncate` marking the cut, and no budget is
#: shared between them.
_PARAMS_MAX = 24
_PARAM_NAME_LIMIT = 40
_PARAM_VALUE_LIMIT = 300

#: Stands in for a name the gate left nothing of — a name that was only
#: whitespace, or only the token.  The parameter is still described, because
#: what it carried is as much a part of the request as any other.
_PARAM_NAME_EMPTY = "<param name withheld>"

#: Key under which :func:`_redact_params` reports the parameters left out; its
#: value is how many there were.
_PARAMS_TRUNCATED = "<params truncated>"

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

    A name passes the same gate as a value, and for the same reason: a
    credential pasted into the left-hand side of an ``extra_params`` entry is as
    much of a leak as one pasted into the right.  Two names the bound above
    reduces to the same text are told apart by the position of the parameter,
    which is written into the name inside its own bound, so one value never
    takes the other's place in the event.

    What the field costs the line is bounded three ways: at most
    :data:`_PARAMS_MAX` parameters are described, a name at
    :data:`_PARAM_NAME_LIMIT` characters and a text value at
    :data:`_PARAM_VALUE_LIMIT`.  Parameters past the count are left out and
    :data:`_PARAMS_TRUNCATED` says how many, so a request of any size costs a
    bounded amount whatever any one parameter holds.

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
    for position, (key, value) in enumerate(items[:_PARAMS_MAX], start=1):
        if type(key) is str:
            name = _safe_text(key, token, limit=_PARAM_NAME_LIMIT) or _PARAM_NAME_EMPTY
        else:
            name = f"<non-str param name: {type(key).__name__}>"
        suffix = f"#{position}"
        while name in redacted:
            name = _truncate(name, _PARAM_NAME_LIMIT - len(suffix)) + suffix
            suffix += "#"
        if type(value) is str:
            redacted[name] = _safe_text(value, token, limit=_PARAM_VALUE_LIMIT)
        elif value is None or type(value) in (int, float, bool):
            redacted[name] = value
        else:
            redacted[name] = f"<non-scalar param: {type(value).__name__}>"
    if len(items) > _PARAMS_MAX:
        redacted[_PARAMS_TRUNCATED] = len(items) - _PARAMS_MAX
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

    ``campaign_id`` and ``date`` belong to the statistics endpoint; a request
    for the campaign list leaves them empty.  ``offset`` is filled in by both,
    with the base each endpoint counts from: the campaign list counts rows
    already skipped and starts at 0, the statistics endpoint counts rows and
    starts at 1.
    """
    return {
        "schema_version": _SCHEMA_VERSION,
        "outcome": _OUTCOME_UNKNOWN,
        "level": None,
        "sent_at": datetime.now(_timezone.utc).isoformat(),
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
        "error_type": None,
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
      one refusal.
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


def _summarize_error(error: object, token: object) -> tuple[int | None, str | None, str | None]:
    """Allowlist extractor for a refusal: ``code``, ``error_type``, ``message``.

    Values of an unexpected type are described by their type rather than
    serialized, at every level — a non-dict error, a non-str ``error_type``, a
    non-str ``message`` — so the triple stays a short, queryable summary of the
    failure whatever the server put there, and a dashboard can group by it.  A
    code is copied only at exactly ``int``, which leaves a code spelled as text
    unread rather than guessed at.  Defensive against odd values: it must not
    itself raise and mask the real ``AirflowException``.

    ``error_type`` is the API's own name for the kind of refusal, and it is the
    part a repeat is decided by where the status alone does not settle the
    question, so it is read at exactly ``str``.

    The type and the message pass :func:`_safe_text`, so they reach the event
    flattened onto one line, bounded, and with the token cut out: a server or a
    proxy is free to quote the ``Authorization`` header back inside a JSON
    ``message``, and this triple words both the event's fields and the line the
    attempt logs.  Text that survives none of that is reported as no text.
    """
    if not isinstance(error, dict):
        return None, None, f"<non-dict error: {type(error).__name__}>"
    raw_code = error.get("code")
    code = raw_code if type(raw_code) is int else None  # `type(...) is int` excludes bool
    raw_type = error.get("error_type")
    if raw_type is None:
        error_type = None
    elif type(raw_type) is not str:
        error_type = f"<non-str error_type: {type(raw_type).__name__}>"
    else:
        error_type = _safe_text(raw_type, token)
    message = error.get("message")
    if message is None:
        return code, error_type, None
    if type(message) is not str:
        return code, error_type, f"<non-str message: {type(message).__name__}>"
    return code, error_type, _safe_text(message, token)


def _describe_error(code: int | None, error_type: str | None, message: str | None) -> str:
    """Read a refusal out of its three parsed parts, for a human.

    The composition is the same wherever the refusal is told — the line an
    attempt logs and the text of the exception that ends the request: the code
    first, the API's own name for the kind of refusal after it, the server's own
    message last.  Any part may be missing, and the phrase then holds what there
    is.  The code is spelled bare, because the API documents no codes and
    nothing here branches on the value; ``error_type`` is spelled as the server
    wrote it, and it is the part the policy for a 400 is read from, so a reader
    comparing a repeated refusal with a final one sees in the line the very word
    the branch weighed.  ``error_type`` and ``message`` arrive already bounded
    and masked by :func:`_summarize_error`.
    """
    head = ", ".join(
        part for part in (f"code {code}" if code is not None else None, error_type) if part
    )
    if head and message:
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
    code, error_type = event["error_code"], event["error_type"]
    error_message = event["error_message"]
    if code is not None or error_type or error_message:
        return f"{message}: {_describe_error(code, error_type, error_message)}"
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

    The endpoint always, and each of the three locators — the campaign, the day,
    the page — whenever the event carries it.  The page belongs to both
    endpoints; the campaign and the day are the statistics endpoint's own, so a
    line about the campaign list names only what it has and never claims a day
    or a campaign that no request was scoped to.

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
    code, error_type = event["error_code"], event["error_type"]
    message = event["error_message"]
    if code is not None or error_type or message:
        parts.append(_describe_error(code, error_type, message))
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


def _stamp_response_error(
    event: dict, resp: object, token: object
) -> tuple[int | None, str | None, str | None]:
    """Copy the refusal a non-200 answer carries onto *event*, and return it.

    The body goes through the same :func:`_find_error` and
    :func:`_summarize_error` as an HTTP-200 one, so the exception, the line the
    attempt logs and the event's ``error_code``/``error_type``/``error_message``
    tell one failure in one set of terms: a dashboard grouping by ``error_type``
    sees the refusals that came with a status as well as the ones that came
    inside a 200.  An answer that names no error this way — HTML from a proxy,
    an empty answer, a ``json()`` that raises — leaves the three fields as it
    found them and is described by its status alone.

    The triple is returned as well as stamped, so that a branch choosing what to
    do about the refusal reads the parsed value rather than the event: what the
    provider does stays independent of which fields the diagnostic schema
    happens to carry.

    Parsing the whole document is what ``resp.json()`` costs, and it is the same
    cost the HTTP-200 branch pays: what :func:`_bounded_body` bounds is the copy
    the event carries away, not the parse every branch needs.

    Never raises.  It runs with the branch's own exception about to be built, so
    a body that refuses to be read must leave both the type of that exception
    and the reason for it exactly as they are.
    """
    code: int | None = None
    error_type: str | None = None
    message: str | None = None
    try:
        error = _find_error(resp.json())
        if error is not None:
            code, error_type, message = _summarize_error(error, token)
            event["error_code"] = code
            event["error_type"] = error_type
            event["error_message"] = message
    except Exception:
        pass
    return code, error_type, message


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


def _record_rate_limit(event: dict, resp: requests.Response, token: object) -> None:
    """Copy the conventional rate-limit headers onto *event*, bounded and scrubbed.

    A response header is text the server wrote, so it leaves by the gate every
    text leaves this module by before it is bounded: the headers named here are
    conventionally numbers, but what arrives under a name is the answer's to
    choose, and an answer that put the credential there would otherwise carry it
    into the event whole.

    Runs inside the instrumented request, on the path that decides whether a 429
    is retried, so it never raises and never turns a retryable status into a hard
    failure.
    """
    try:
        headers = resp.headers
        for field, name in (
            ("rate_limit_limit", _RATE_LIMIT_LIMIT_HEADER),
            ("rate_limit_remaining", _RATE_LIMIT_REMAINING_HEADER),
        ):
            value = headers.get(name)
            event[field] = _bounded_header(
                _scrub(value, token) if type(value) is str else value
            )
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
            moment = moment.replace(tzinfo=_timezone.utc)
        return (moment - datetime.now(_timezone.utc)).total_seconds()
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
    server-side failure, a 400 the API states a retryable ``error_type`` in, a
    network that did not carry the request — is a warning while an attempt is
    left and a failure on the last one.  A refusal to
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


def _as_positive_id(value: object) -> int | None:
    """Return the identifier *value* names, or ``None``.

    An id written as text is an ordinary way to write one: ``extra`` is JSON
    typed by hand in the Airflow UI, where quoting a number is as natural as
    leaving it bare, and an answer is free to word an identifier either way too.

    A flag is not a number here even though Python counts one as an ``int``, a
    fractional value names nothing, and neither does a number at or below zero:
    the ids the API issues start above it.
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


def parse_connection(extra: dict) -> int | None:
    """Read the ``advertiser_id`` out of ``connection.extra``, or return ``None``.

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
    advertiser_id = _as_positive_id(raw)
    if advertiser_id is None:
        log.warning(
            "Connection extra field 'advertiser_id' is not a positive whole number (got %s).",
            type(raw).__name__,
        )
    return advertiser_id


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


#: Stands for a field an answer did not carry at all.  A field holding ``null``
#: and a field that is absent both read as ``None`` through :meth:`dict.get`,
#: and the two say opposite things about completeness: one is a flag nothing can
#: read, the other is the answer declining to call its total an approximation.
_ABSENT = object()


def _declared_total(value: object) -> int | None:
    """Return the row count an answer declares, or ``None`` when none is readable.

    The count is what the pagination is checked against, so only a whole number
    counts as one: a flag, a string or a missing field say that the answer named
    no total, and the caller fails the walk rather than checking it against a
    value invented here.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _declared_rounded(value: object) -> bool | None:
    """Return whether an answer calls its total an approximation, or ``None``.

    The field is a boolean, and an answer carrying no such field at all — which
    is what :data:`_ABSENT` says — is one whose total is exact, the reading the
    completeness check is strictest under.  Anything else, ``null`` included, is
    unreadable and comes back as ``None`` for the caller to fail on: read as a
    truthy value, a string such as ``"false"`` would disown the exact check and
    let a short export finish green, and read as a falsy one it would fail a day
    that is whole.  A flag that cannot be read leaves no way to know which, and
    completeness is not something to guess at.
    """
    if value is _ABSENT:
        return False
    if isinstance(value, bool):
        return value
    return None


def _page_budget(limit: int, ceiling: int) -> int:
    """Return how many pages of *limit* rows a walk may ask for.

    The budget is worked out from *ceiling*, the amount of data a walk is
    allowed, rather than fixed at a number of pages: a campaign-day of a million
    rows is the same day whether it arrives in a hundred pages or in ten
    thousand, and a page size chosen to ask the API for less at a time must not
    turn a large day into a failure that reads as a walk going nowhere.

    :data:`_MAX_PAGES` bounds the count from above, because the ceiling is
    counted in rows and a small page would spend the whole of a task reaching
    it.  A walk reading such pages is the one case where the budget runs out
    before the ceiling does.
    """
    return min(-(-ceiling // max(limit, 1)), _MAX_PAGES)


def _campaign_record(row: dict) -> dict:
    """Return one dictionary record: the named fields, in a fixed order.

    Values are handed on as the API worded them — a date stays the string it
    arrived as — because the dictionary describes the campaign the API knows and
    any rewording here would be a second source of truth about it.

    ``campaign_id`` is the exception, and it is read as a positive whole number
    or as nothing at all.  The value names a campaign in a request, in a record
    key, in an ``INTEGER`` column and — through the diagnostic event and the
    task log — in text that leaves the process, so what an answer put there is
    admitted at one type only: a value of any other kind would travel unbounded
    into all four.  Nothing at all is what :meth:`AdmetricaHook.get_campaigns`
    fails the export over, so no record written anywhere carries it.

    A field the answer left out is present and empty, so every record of a file
    carries the same keys and a table schema written once holds them all.
    """
    record = {field: row.get(field) for field in _CAMPAIGN_FIELDS}
    record["campaign_id"] = _as_positive_id(record["campaign_id"])
    return record


def _describe_campaign(row: dict, token: object) -> str:
    """Return the words that name a campaign whose id could not be read.

    The type of what the answer put in ``campaign_id``, the value itself when it
    is one a diagnostic may carry, and the campaign's name when the answer gave
    one.  The name and a textual id are the server's own words, so they leave
    through the gate every text leaves by; a number, which is short and holds no
    text, is spelled out as it arrived.
    """
    raw = row.get("campaign_id")
    if type(raw) in (int, float, bool):
        described = f"a {type(raw).__name__} of {raw}"
    elif type(raw) is str:
        text = _safe_text(raw, token, limit=_PARAM_VALUE_LIMIT)
        described = f"a str of {text!r}" if text else "a str of unusable text"
    else:
        described = f"a {type(raw).__name__}"
    name = _safe_text(row.get("name"), token)
    return f"{described}; the campaign is named {name!r}" if name else described


def _quoted_parameter(parameter: str, token: object) -> str:
    """Return a parameter name fit for a task log: quoted, or named by length.

    The name is the caller's own text and a credential is among the things a
    caller can write into it, so it leaves through :func:`_scrub` like every
    other text this module writes out.  A name the token survived is described
    by its length instead of quoted — the WARNING loses the key it was naming
    and keeps its shape, which is the trade every channel here makes.

    Bounded like free text, because a name is as long as whoever wrote it made
    it and the line is read from a log.
    """
    clean = _scrub(_one_line(parameter), token)
    if clean is None:
        return f"of {len(parameter)} character(s), which cannot be quoted here"
    return repr(_truncate(clean))


def _resolve_placeholders(
    text: str, extra_params: dict | None, token: object = None
) -> str:
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

    Such a placeholder is also the one piece of the caller's own text this module
    writes out in full, because the WARNING is only actionable if it names the
    key to add to ``extra_params``.  *token* is what makes that safe: the name
    goes out through :func:`_quoted_parameter`, the same gate the caller's text
    passes through on every other channel.
    """
    if "<" not in text:
        return text
    params = extra_params if isinstance(extra_params, dict) else {}

    def substitute(match: re.Match) -> str:
        parameter = match.group(1)
        if parameter in params:
            return str(params[parameter])
        log.warning(
            "A name of %d character(s) in this request names the parameter %s, "
            "which the request does not carry; the record key keeps the "
            "parameter's name in place of its value. The name around it is given "
            "by its length rather than quoted: it is the caller's own text and "
            "this line is read from a task log.",
            len(text),
            _quoted_parameter(parameter, token),
        )
        return parameter

    return _PLACEHOLDER_RE.sub(substitute, text)


def _normalize_name(
    name: object, extra_params: dict | None = None, token: object = None
) -> str:
    """Return the record key a grouping or a metric is written under.

    One rule serves both, because both are named the same way by the API:
    the shared ``am:e:`` prefix comes off, a parameter spelled into the name is
    replaced by its value, and the camelCase that is left becomes snake_case —
    ``am:e:deviceType`` is ``device_type``, ``am:e:operatingSystemRoot`` is
    ``operating_system_root``, ``am:e:videoCompletePercent`` is
    ``video_complete_percent``, and ``am:e:interest2d1``, which has no capital
    in it, is ``interest2d1``.

    This key is a public contract: it is what an analyst writes in
    ``JSON_VALUE(dimensions, '$.device_type')`` and what the documentation
    lists beside every name.  Changing the rule renames columns of every file written
    after the change while leaving the ones before it under the old names, and
    a query that names the old key answers NULL rather than failing.

    The fields *inside* a grouping's value are not touched by this: they are
    the API's own wording of what the grouping matched, and
    :func:`_map_row` keeps them as they arrived.
    """
    text = str(name)
    if text.startswith(_NAME_PREFIX):
        text = text[len(_NAME_PREFIX) :]
    text = _resolve_placeholders(text, extra_params, token)
    text = _WORD_SPLIT_RE.sub(r"\1_\2", _ACRONYM_SPLIT_RE.sub(r"\1_\2", text))
    return _UNDERSCORE_RUN_RE.sub("_", _NON_KEY_CHARS_RE.sub("_", text.lower())).strip("_")


def _row_values(raw_row: object, key: str) -> list:
    """Return the row's list under *key*, empty when the row carries none.

    A row that is not a dict, and one whose groupings or metrics are not a list,
    both answer as a row that brought nothing.  What that means is the caller's
    to decide, and it decides it by the number of names the request asked for:
    a row that brought nothing where anything was asked for is a row the answer
    cannot be read out of at all.
    """
    values = raw_row.get(key) if isinstance(raw_row, dict) else None
    return values if isinstance(values, list) else []


def _record_keys(
    names: Sequence[str],
    extra_params: dict | None,
    kind: str,
    token: object = None,
) -> list[str]:
    """Return the record key each requested name is written under, in request order.

    Called once for a request, not once for a row.  The names and the parameters
    are the request's own configuration, identical for every row of every
    campaign of the day, so both of the warnings below describe the request and
    say the same thing about the ten-thousandth row as about the first.  Read
    per row they would fill a task log with one line per row while the export
    itself succeeds, and the regular expressions behind
    :func:`_normalize_name` would run over every name of every row.

    Two requested names can normalise to one record key — the two spellings of
    a parameterised name do, and ``am:e:goal12345Reaches`` beside
    ``am:e:goal<goal_id>Reaches`` with ``goal_id=12345`` is the case that
    happens.  The key appears twice in the answer, the record holds the later
    value under it, and a WARNING names the two by their position in the request
    and the key by its length.  The requested names themselves do not reach that
    line: they are the caller's own text, a credential is among the things a
    caller can write there, and a position says as much about which name to fix.

    *token* is what the one name this path does write out is held against — the
    unanswered placeholder :func:`_resolve_placeholders` reports.
    """
    keys: list[str] = []
    first_position: dict[str, int] = {}
    for position, name in enumerate(names):
        key = _normalize_name(name, extra_params, token)
        if key in first_position:
            log.warning(
                "The %s at position %d of this request writes the record key that "
                "the %s at position %d already writes, a key of %d character(s); "
                "the record holds the later value under it. The two are named by "
                "their position rather than quoted: a requested name is the "
                "caller's own text and this line is read from a task log.",
                kind,
                position + 1,
                kind,
                first_position[key] + 1,
                len(key),
            )
        else:
            first_position[key] = position
        keys.append(key)
    return keys


def _named_values(
    keys: Sequence[str],
    values: list,
    kind: str,
    date: str,
    campaign_id: object,
) -> dict:
    """Pair the record *keys* with the *values* the answer returned, by position.

    The answer carries values in the order they were asked for and names none of
    them, so position is the only thing tying a number to the metric it
    measures.  A row carrying another number of values is therefore a row whose
    values cannot be told apart: keeping what lines up would leave keys empty or
    drop the values no name claims, while the row counts towards ``total_rows``
    either way, so the day would pass its completeness check and be written with
    values missing from a row.  Such a row fails the day instead, naming the
    campaign, the day and the two counts.

    An empty value is written through as it arrived, on both halves.  A metric
    that divides or prices — ``am:e:ctr``, ``am:e:cpm``, ``am:e:cpc``,
    ``am:e:cpa<goal_id>``, the ``video*Percent`` family, the
    ``am:e:ecommerce<currency>Revenue`` family — comes back empty wherever its
    denominator or its cost is, so a report asking for those alone answers in
    rows of empty numbers; a grouping the API has no value for is what
    ``include_undefined`` asks to be sent.  Each is a documented answer, reaches
    the record as ``None`` and is written as JSON ``null``, which BigQuery reads
    as NULL.

    Two of the keys can be the same one, which :func:`_record_keys` has already
    warned about; the record holds the later value under it.
    """
    if len(values) != len(keys):
        raise _MaskedError(
            f"AdMetrica answered with {len(values)} {kind} value(s) in a row of "
            f"campaign {campaign_id} on {date}, where the request asks for "
            f"{len(keys)}. "
            f"The answer names none of them, so position is all that ties a value "
            f"to the name it was asked for by, and a row of another length is a "
            f"row whose values cannot be told apart — while it counts towards "
            f"total_rows whatever is read out of it. The day stops here rather "
            f"than being written with values missing from a row."
        )
    return {key: values[position] for position, key in enumerate(keys)}


def _map_row(
    raw_row: object,
    date: str,
    advertiser_id: int,
    campaign_id: int,
    dimension_keys: Sequence[str],
    metric_keys: Sequence[str],
) -> dict:
    """Return one record of statistics built from one row of a report.

    Pure: it reads its arguments and answers out of them alone — a dict, or a
    failure over a row it will not make a record of — so what a record looks
    like is decided in one place and can be checked without a network or a
    connection.

    A row carrying another number of values than the request asked for fails the
    day here.  It would otherwise become a record with nothing under some of its
    keys while counting towards the total the day is checked against, so the
    file would look whole with a row of it hollowed out.

    The service fields are flat and typed.  ``date`` is stamped here because the
    report has no date in it at all: a day is asked for as ``date1=date2`` and
    the answer says nothing about which day it is, so the day the request was
    made for is the day the row belongs to.

    The variable half is two nested objects.  Under ``dimensions`` each value is
    the object the API returned, with exactly the fields it arrived with — a
    grouping that brings an ``id`` beside its ``name`` keeps it, one that brings
    only a ``name`` stays that way, and a field neither this provider nor its
    documentation has seen is carried through untouched.  Under ``metrics`` are
    the numbers, under the keys :func:`_record_keys` made of their names once
    for the whole request.  Nesting is what makes
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
            dimension_keys,
            _row_values(raw_row, "dimensions"),
            "dimension",
            date,
            campaign_id,
        ),
        "metrics": _named_values(
            metric_keys,
            _row_values(raw_row, "metrics"),
            "metric",
            date,
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
    ``id`` where the answer carried a value under it and its ``name`` otherwise,
    tagged with which of the two it is: two placements sharing a name and
    differing in ``id`` are two rows, and reading the name alone would call the
    second one a duplicate and fail a day that is perfectly whole.  The
    specification guarantees ``name`` alone, so an ``id`` holding ``null`` — an
    unregistered creative, a placement the answer has no number for — says as
    little as one that never arrived, and the name is what tells such rows
    apart.

    A value carrying neither, and a value that is not an object at all, are read
    whole instead: the whole of what arrived is what tells two such rows apart,
    and projecting them onto a constant would call every one of them a repeat of
    the first and fail a day nothing is wrong with.
    """
    key = []
    for value in _row_values(raw_row, "dimensions"):
        if isinstance(value, dict) and value.get("id") is not None:
            key.append(("id", str(value["id"])))
        elif isinstance(value, dict) and "name" in value:
            key.append(("name", str(value["name"])))
        elif isinstance(value, dict):
            key.append(("raw", str(sorted((str(k), str(v)) for k, v in value.items()))))
        else:
            key.append(("raw", str(value)))
    return tuple(key)


def _describe_row_key(key: tuple, token: object) -> str:
    """Return a row's identity in the form the exception naming it may carry.

    The values in a key are the server's own words for what the groupings
    matched — a placement's name, a campaign's own wording of a segment — so
    they leave by the gate every text leaves this module by.  A key that gate
    will not pass is named by its shape alone, which still says which row of the
    page repeated without quoting anything the answer wrote.
    """
    return _safe_text(str(key), token) or f"<{len(key)} grouping value(s)>"


def check_date(date: object) -> None:
    """Fail on a day that is not a day, before anything is requested.

    ``date1`` and ``date2`` carry this value and the endpoint reads a day in one
    spelling only, so a day written any other way is a 400 whose body says
    considerably less, after the wait for it has been paid.  The same value is
    stamped onto every record and, above this module, names a file and a
    partition, so holding it to :data:`DATE_FORMAT` here is what makes those
    three agree.

    The parse is written back out and compared: ``strptime`` reads an unpadded
    month, and a day spelled ``2026-8-20`` would go out as one string and name
    records that no other day of the export matches.

    The refusal names the value by type and length rather than quoting it.  The
    day arrives from the caller and may hold anything at all, a credential among
    the possibilities, while this text is read from a task log and a traceback
    like every other text this module lets out.
    """
    parsed = None
    if type(date) is str:
        try:
            parsed = datetime.strptime(date, DATE_FORMAT)
        except ValueError:
            parsed = None
    if parsed is None or parsed.strftime(DATE_FORMAT) != date:
        given = (
            f"a str of {len(date)} character(s)"
            if type(date) is str
            else f"a value of type {type(date).__name__}"
        )
        raise ValueError(
            f"date must be a day written as {DATE_FORMAT}, as in 2026-08-20; "
            f"{given} was given."
        )


def _check_report_limits(
    dimensions: Sequence[str], metrics: Sequence[str], limit: object
) -> None:
    """Fail on a request the API documents as out of bounds, before it is sent.

    The ceilings are the documented ones, and the refusal names which list is
    over and by how much — an unchecked request comes back as a 400 whose body
    says considerably less, after the wait for it has been paid.

    A report with no metrics is refused here for the same reason: the parameter
    is required, an empty one goes out as ``metrics=``, and the 400 that answers
    it says nothing about which parameter was empty.

    ``limit`` is checked at both ends.  Above the documented ceiling the API
    refuses the request; at or below zero it silently falls back to its own
    default of 100 rows, and a page shorter than the one the walk believes it
    asked for is then never short — the walk would go on until the page cap
    stops it.

    A ``limit`` that is not a whole number is named by its type rather than
    quoted: it arrives from the caller and may hold anything at all, a
    credential among the possibilities, while this text is read from a task log
    and a traceback like every other text this module lets out.  A whole number
    is quoted as itself, since a number says nothing beyond the range it fell
    outside of.
    """
    if not metrics:
        raise ValueError(
            "AdMetrica needs at least one metric per request; none were given."
        )
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
    if type(limit) is not int or not 0 < limit <= _MAX_LIMIT:
        given = repr(limit) if type(limit) is int else f"a {type(limit).__name__}"
        raise ValueError(
            f"limit must be a whole number between 1 and {_MAX_LIMIT}; {given} "
            f"was given."
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


def _log_report_caveats(page: dict, date: str, campaign_id: object, token: object) -> None:
    """Say in the task log what the answer said about its own numbers.

    Sampling and withheld rows are warnings because the numbers written are then
    not the whole truth about the day, and nothing downstream can tell that from
    the rows alone.  A lag is information: the day is whole as far as the API
    has it, and how far behind it is says whether the day is worth exporting
    again later.

    The values are the answer's own, so they reach the line through
    :func:`_meta_value`, the same gate that puts them in the event: the task log
    is as much a way out of the process as a push to Loki, and a field the API
    documents as a number is still whatever the answer put there.
    """
    if page.get("sampled"):
        log.warning(
            "AdMetrica sampled the report for campaign %s on %s (sample_share=%s, "
            "sample_size=%s of %s); the numbers are an estimate.",
            campaign_id,
            date,
            _meta_value(page.get("sample_share"), token),
            _meta_value(page.get("sample_size"), token),
            _meta_value(page.get("sample_space"), token),
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
            _meta_value(data_lag, token),
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

    :meth:`test_connection` is a helper to call from a DAG or a shell rather than
    the Airflow UI's Test button: this provider declares no ``connection-types``,
    so Airflow never registers the class, and a connection of type HTTP is tested
    by the HTTP provider's own hook.  The class attributes below say which
    connection kind this hook reads and what to call it, and nothing in Airflow
    dispatches on them.
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
        request_delay: float = DEFAULT_REQUEST_DELAY,
        limit: int = DEFAULT_LIMIT,
    ) -> None:
        super().__init__()
        self.admetrica_conn_id = admetrica_conn_id
        self.request_delay = request_delay
        self.limit = limit
        #: Diagnostics sink; ``None`` leaves every event unbuilt.
        self._loki = loki
        #: The connection, read on first use and kept for the hook's lifetime.
        self._connection: Connection | None = None
        #: The advertiser the connection names, read on first use and kept.
        self._advertiser_id: int | None = None
        #: The OAuth token, read out of the connection on first use and kept.
        self._token: str | None = None
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

        The connection is looked up outside that leniency, so a connection
        Airflow does not have fails as itself.  The two are different mistakes
        with different fixes — a typo in ``admetrica_conn_id`` or a secrets
        backend that would not answer, against an extra to correct — and reading
        the extra of a connection nobody found is not a thing to describe.
        """
        conn = self._get_connection()
        try:
            extra = conn.extra_dejson
        except Exception:
            log.warning(
                "Connection %r has an extra that is not readable as JSON.",
                self.admetrica_conn_id,
            )
            return {}
        return extra if isinstance(extra, dict) else {}

    def _get_token(self) -> str:
        """Return the OAuth token, or fail with what to put where.

        Read out of the connection once and kept, so that the value the masking
        gate searches for is the one value this hook ever read.
        """
        if self._token is None:
            self._token = _token_from_password(self._get_connection().password)
        token = self._token
        if token is None:
            raise _MaskedError(
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
        ``Retry-After`` wherever it named one.  A 400 whose body names an
        ``error_type`` listed in :data:`_RETRYABLE_ERROR_TYPES` is repeated too,
        along the minute-wide :data:`_QUERY_ERROR_DELAYS`: such a refusal says
        the request is sound and the moment was not, so the repeat waits out the
        window instead of the seconds a rate limit is answered in.  A status in
        :data:`_AUTH_STATUSES` fails at once: the token is long-lived and
        nothing here refreshes it, so the attempt after it would be refused the
        same way.  Every other 4xx fails
        at once as well, with the server's words for it read out of the body —
        the request itself is what is being refused, and a repeat brings back
        the same answer.

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
            # Set by the safety net below for an interruption of the task
            # itself, and read in `finally` as the one answer to "may this
            # attempt still be reported".
            interrupted = False
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
                        # Raised with no exception attached: the original is
                        # the environment's own writing — a proxy URL with the
                        # credential in it, a wrapped socket error — and an
                        # exception carried as cause or context is printed with
                        # its own text into the task log, past every gate. What
                        # it said travels as the scrubbed text above instead.
                        raise _MaskedError(
                            f"Request to {url} failed after {max_attempts} attempts: "
                            f"{_exception_text(e, token)}"
                        ) from None
                    retry_delay = _BACKOFF_DELAYS[attempt]
                else:
                    _stamp_duration(event, started)
                    event["http_status"] = resp.status_code

                    # One chain, not a run of separate `if`s: a refusal a
                    # repeat can fix has a path that neither returns nor raises
                    # — it names a pause and leaves for `time.sleep` at the foot
                    # of the loop.
                    if resp.status_code == 200:
                        try:
                            data = resp.json()
                        except ValueError as e:  # JSONDecodeError subclasses ValueError
                            event["outcome"] = "invalid_json"
                            event["exception_message"] = _decoder_position(e)
                            _record_exception(event, e)
                            position = event["exception_message"]
                            detail = f" ({position})" if position else ""
                            # Nothing attached, for the reason the network
                            # failure above is raised the same way: an exception
                            # carried along would print its own text, which is
                            # the decoder's report of a body no gate has seen.
                            raise _MaskedError(
                                f"AdMetrica {endpoint} returned an HTTP 200 body "
                                f"that is not JSON{detail}"
                            ) from None

                        if _classify_payload(event, data):
                            _stamp_report_meta(event, data, token)
                            return data

                        # No list of rows was recognised, so there is no page to
                        # hand on.  Ending the attempt here is what keeps a zero
                        # from an unreadable answer apart from the zero of a
                        # campaign that had no impressions.
                        error = _find_error(data)
                        if error is not None:
                            (
                                event["error_code"],
                                event["error_type"],
                                event["error_message"],
                            ) = _summarize_error(error, token)
                        raise _MaskedError(
                            _with_error(
                                f"AdMetrica {endpoint} returned HTTP 200 with "
                                f"{_describe_unreadable_body(event)}",
                                event,
                            )
                        )

                    elif resp.status_code in _AUTH_STATUSES:
                        event["outcome"] = "auth_error"
                        _stamp_response_error(event, resp, token)
                        raise _MaskedError(
                            _with_error(
                                f"AdMetrica {endpoint} returned {resp.status_code}: the OAuth "
                                f"token in connection {self.admetrica_conn_id!r} was refused, "
                                f"and nothing here refreshes it",
                                event,
                            )
                        )

                    elif resp.status_code in _RETRY_STATUSES:
                        event["outcome"] = "retryable_error"
                        if resp.status_code == 429:
                            _record_rate_limit(event, resp, token)
                        _stamp_response_error(event, resp, token)
                        if last_attempt:
                            raise _MaskedError(
                                _with_error(
                                    f"AdMetrica {endpoint} returned {resp.status_code} for "
                                    f"{url} on attempt {max_attempts} of {max_attempts}",
                                    event,
                                )
                            )
                        # The server's own `Retry-After` wins whenever it
                        # named one, in both directions: a longer wait is a
                        # window that has not passed yet, a shorter one an
                        # invitation to come back before the ladder would.
                        asked = _retry_after(resp)
                        retry_delay = (
                            _BACKOFF_DELAYS[attempt] if asked is None else asked
                        )

                    else:
                        # The body is read before the outcome is chosen: a 400
                        # states what it objects to in `error_type`, and that
                        # word decides the policy along with the status.  The
                        # branch reads the value `_stamp_response_error`
                        # returns rather than the event it stamped, so what the
                        # provider does stays independent of which fields the
                        # diagnostic schema carries.
                        _, error_type, _ = _stamp_response_error(event, resp, token)
                        if (
                            resp.status_code == 400
                            and error_type in _RETRYABLE_ERROR_TYPES
                        ):
                            event["outcome"] = "retryable_error"
                            if last_attempt:
                                raise _MaskedError(
                                    _with_error(
                                        f"AdMetrica {endpoint} returned {resp.status_code} "
                                        f"for {url} on attempt {max_attempts} of "
                                        f"{max_attempts}",
                                        event,
                                    )
                                )
                            retry_delay = _QUERY_ERROR_DELAYS[attempt]
                        else:
                            event["outcome"] = "http_error"
                            raise _MaskedError(
                                _with_error(
                                    f"AdMetrica {endpoint} returned {resp.status_code} "
                                    f"for {url}",
                                    event,
                                )
                            )
            except BaseException as e:
                # Safety net: record what escaped, then decide how it leaves.
                # `BaseException` so that an interrupted attempt is classified
                # too: the push in `finally` runs for those as well, and
                # `outcome` must never reach Loki as the placeholder "unknown".
                _record_exception(event, e)
                if event["outcome"] == _OUTCOME_UNKNOWN:
                    event["outcome"] = "unexpected_error"
                interrupted = not isinstance(e, Exception)
                if interrupted or isinstance(e, _MaskedError):
                    # An interruption is the task being stopped and belongs to
                    # Airflow whole; a `_MaskedError` is this module's own
                    # wording, every one of them out of text the gate has
                    # already seen.
                    raise
                # Anything else was raised by the environment, and its text is
                # the environment's own writing — a library naming the proxy it
                # dialled, credentials and all.  It leaves as text the gate has
                # seen, with the type named so the failure is still identifiable,
                # and with nothing attached: an exception carried as cause or
                # context is printed with its own text into the task log, past
                # every gate.
                raise _MaskedError(
                    f"Request to {url} failed with an unexpected "
                    f"{type(e).__name__}: {_exception_text(e, token)}"
                ) from None
            finally:
                # An interruption on its way out means the task is being
                # stopped, with the alarm or signal behind it firing once:
                # pushing here would hold the stop for the length of a push, so
                # the attempt a stop cut short goes unreported, deliberately —
                # the reason for it is in the Airflow task log.
                if not interrupted:
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

            # Reached only where an attempt named a pause: a non-final refusal
            # a repeat can fix — a retryable status or a 400 the API states a
            # retryable `error_type` in — or a request the network did not carry
            # with attempts left.
            # The event is already pushed, so a Loki outage never delays its
            # visibility.
            time.sleep(retry_delay)

    @property
    def advertiser_id(self) -> int:
        """The advertiser this hook's connection stands for.

        The way out of the connection for a number the written records carry and
        the paths in S3 are built from: a caller that needs the advertiser reads
        it here instead of being configured with it a second time.

        Read once and kept, like the connection it comes from: a day of one
        advertiser asks for it dozens of times, and an extra that names no
        advertiser would otherwise write its WARNING once per ask.
        """
        if self._advertiser_id is None:
            advertiser_id = parse_connection(self._get_extra())
            if advertiser_id is None:
                raise _MaskedError(
                    f"Connection {self.admetrica_conn_id!r} names no advertiser. "
                    f"Its extra must hold a positive whole 'advertiser_id', as in "
                    f'{{"advertiser_id": 17004}}.'
                )
            self._advertiser_id = advertiser_id
        return self._advertiser_id

    def get_campaigns(self) -> list[dict]:
        """Return the advertiser's campaigns, one record per campaign.

        Every status is asked for: no ``status`` goes out with the request,
        because an archived campaign ran in the past and its statistics are as
        real as an active one's — a filter here would silently shorten every
        re-export of an earlier period.

        A campaign whose ``campaign_id`` is not a positive whole number fails
        the export.  Statistics are asked for one campaign at a time and named
        by that id, so such a campaign is one whose rows no request can ask for:
        leaving it out would write a day short of everything it ran, and the
        shortfall would be found weeks later, when the period can no longer be
        re-requested.

        Pagination walks the list with ``limit`` and ``offset`` spelled out, the
        offset counting the rows already skipped and therefore starting at zero.
        It ends on a page shorter than the one asked for — the answer's own word
        that the campaigns have run out — and the declared total is what the
        collected list is checked against, never where the walk stops.  A total
        reached exactly on a full page would end the walk on the API's count of
        the list rather than on the list itself, and a count that is one page
        stale would leave every campaign after it unasked for, along with every
        row of statistics they ran.  The price is one further request whenever
        the list ends exactly on a page boundary, and it buys the tail.

        A page cut short before the total is reached means whole campaigns were
        lost, so the export fails instead of arriving short.  The total is read
        from every page and has to be there and stay the same: a page declaring
        no readable total is a page nothing can check the list against, and a
        total that changes between pages leaves two answers to how long the list
        is; either way the export fails rather than claiming a completeness it
        cannot show.

        The list is fetched once and kept for the hook's lifetime, which is one
        task instance: the statistics and the dictionary of one day are two
        readers of one answer.

        ``snapshot_date`` is not here.  The field belongs to the operator, which
        writes it into every record, names the file with the same date and
        reports it as the date of the result, so path, column and partition can
        never disagree.
        """
        if self._campaigns is not None:
            return self._campaigns

        advertiser_id = self.advertiser_id
        campaigns: list[dict] = []
        seen: set[int] = set()
        total: int | None = None
        offset = 0
        budget = _page_budget(_CAMPAIGNS_LIMIT, _MAX_CAMPAIGNS)
        for _ in range(budget):
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
            rows = page[_ROWS_KEYS["campaigns"]]
            for row in rows:
                record = _campaign_record(row)
                campaign_id = record["campaign_id"]
                if campaign_id is None:
                    raise _MaskedError(
                        f"AdMetrica listed a campaign of advertiser {advertiser_id} "
                        f"whose campaign_id is not a positive whole number "
                        f"({_describe_campaign(row, self._maskable_token())}). "
                        f"Statistics are asked for one campaign at a time and named "
                        f"by that id, so a campaign that cannot be named is a "
                        f"campaign whose rows would be missing from every day "
                        f"exported, and missing quietly. The export stops here."
                    )
                if campaign_id in seen:
                    raise _MaskedError(
                        f"AdMetrica listed campaign {campaign_id} of advertiser "
                        f"{advertiser_id} twice. A list that repeats a campaign is "
                        f"a list the offset is not moving through, so the walk "
                        f"stops here rather than reading the same page forever."
                    )
                seen.add(campaign_id)
                campaigns.append(record)
            declared = _declared_total(page.get("total"))
            if declared is None:
                raise _MaskedError(
                    f"AdMetrica answered a page of campaigns for advertiser "
                    f"{advertiser_id} at offset {offset} with no readable total "
                    f"(the field holds {type(page.get('total')).__name__}). The total "
                    f"is what the collected list is checked against, so a list without "
                    f"one is a list whose missing campaigns would take all of their "
                    f"statistics with them, quietly. The export stops here."
                )
            if total is None:
                total = declared
            elif declared != total:
                raise _MaskedError(
                    f"AdMetrica declared {total} campaigns for advertiser "
                    f"{advertiser_id} on an earlier page and {declared} on the page at "
                    f"offset {offset}. The declared total is what the collected list "
                    f"is checked against, so a total that changes under the walk is "
                    f"one neither answer can be trusted from. The export stops here "
                    f"rather than being checked against a number the API has already "
                    f"replaced."
                )
            if len(rows) < _CAMPAIGNS_LIMIT:
                break
            offset += len(rows)
        else:
            raise _MaskedError(
                f"AdMetrica kept answering with full pages of campaigns for "
                f"advertiser {advertiser_id} after {budget} of them, "
                f"{budget * _CAMPAIGNS_LIMIT} campaigns in all. A walk that does "
                f"not converge is one the offset is not moving through, so it "
                f"stops here rather than running for the length of the task."
            )

        if len(campaigns) != total:
            raise _MaskedError(
                f"AdMetrica returned {len(campaigns)} campaigns for advertiser "
                f"{advertiser_id} while declaring {total}. A campaign missing from "
                f"the list takes all of its statistics with it, so the export stops "
                f"here rather than writing a day that is short."
            )

        self._campaigns = campaigns
        return campaigns

    def _maskable_token(self) -> str | None:
        """Return the token already read, or ``None`` while none has been.

        What the masking gate looks for, taken from what this hook has already
        read rather than by reading a connection: it answers while a failure is
        being described, and a failure that arrives before the token was read
        arrived before anything could carry it, so there is nothing to search
        the text for.  Never raises.
        """
        return self._token

    def test_connection(self) -> tuple[bool, str]:
        """Answer whether this connection works, as a flag and a sentence.

        The check is the campaign list, because it exercises at once everything
        a task depends on — the token is accepted, the advertiser is real and
        the account may read the API — and it costs one request to an endpoint
        that returns no statistics.

        The whole answer is the returned pair, so nothing leaves here as an
        exception, and the text of a failure passes through the same gate every
        text leaves this module by: a server, a proxy or a wrapped network
        failure can word an error with the credential inside it, and the text is
        written to be read by a person.
        """
        try:
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
        campaigns comes from :meth:`get_campaigns`, so one task instance asks
        the management API once however many campaigns it walks.

        ``sort`` goes out naming every grouping that was asked for.  The report
        is aggregated by them, so their combination orders the rows completely
        and repeatably from one page to the next; sorting by a metric would not,
        since a long tail of placements shares ``renders=1`` and the order
        within that tail is the API's to choose — and rows would move between
        pages while the walk was reading them.

        The day, the documented ceilings and the parameters this hook owns are
        checked before the first request, so a report configured wrongly costs
        nothing and says what is wrong.

        Raises :class:`ValueError` on a day not written as :data:`DATE_FORMAT`,
        on a request the API documents as too large or on an ``extra_params``
        key the hook owns, and :class:`~airflow.exceptions.AirflowException` on
        a day that came back incomplete.
        """
        check_date(date)
        dimensions = list(dimensions)
        metrics = list(metrics)
        _check_report_limits(dimensions, metrics, self.limit)
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

        # Once for the request, not once for a row: the names and the parameters
        # are the same for every row of every campaign of the day, and so is
        # everything `_record_keys` has to say about them.
        #
        # The token is read here rather than taken from what a request has
        # already read, because this runs before the first one: a requested name
        # is the caller's own text, `_record_keys` writes one such name to the
        # task log, and the value it is held against has to be the live one.
        token = self._get_token()
        dimension_keys = _record_keys(dimensions, extra_params, "dimension", token)
        metric_keys = _record_keys(metrics, extra_params, "metric", token)

        records: list[dict] = []
        for campaign in self.get_campaigns():
            # A positive whole number for every campaign of the list, because
            # `get_campaigns` fails the export over one it could not read: `ids`
            # is required, and `requests` drops a parameter of `None`, so the
            # request would go out asking for every campaign at once and come
            # back summed, with the rows stamped as belonging to no campaign.
            campaign_id = campaign["campaign_id"]
            records.extend(
                self._collect_campaign(
                    date=date,
                    advertiser_id=advertiser_id,
                    campaign_id=campaign_id,
                    base_params=base_params,
                    dimension_keys=dimension_keys,
                    metric_keys=metric_keys,
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
        dimension_keys: Sequence[str],
        metric_keys: Sequence[str],
    ) -> list[dict]:
        """Walk one campaign's report for one day and return its records.

        The offset counts rows from one, which is this endpoint's own numbering:
        a walk starting at zero would ask for the first row twice.

        The walk ends on a page shorter than the one asked for, and on nothing
        else: that short page is the report's own word that the rows have run
        out, while ``total_rows`` is a number about the report rather than the
        report itself.  Stopping on it would end a day early whenever it is low
        by a page — 10 437 rows declared as 10 000 against ``limit=10000`` fill
        the first page exactly, match the total and leave the tail behind — and
        ``total_rows_rounded`` says outright that such a number may be off.  So
        the total is what the collected rows are checked against afterwards, and
        the walk pays one further request whenever a campaign-day ends exactly
        on a page boundary.

        ``total_rows`` and ``total_rows_rounded`` are read from every page and
        both have to be readable: a page without a usable ``total_rows``, one
        whose ``total_rows_rounded`` is not a boolean, and — while the totals
        are exact — one naming a different total than an earlier page all fail
        the day.  Each is a completeness signal the day would otherwise be
        called whole without, and a rounded total is exempt from the last only
        because approximations are free to differ.  The rounding flag is
        remembered once seen, so a later page declaring an exact total cannot
        re-arm a check the answer has already disowned.

        Rows are checked against each other by the identity of their groupings,
        and the set of what has been seen lives here — one campaign, one set.
        The combination is unique inside one report and repeats between reports
        as a matter of course, since one placement runs in several campaigns of
        the same advertiser; a set shared across campaigns would fail an export
        of perfectly ordinary data.  A repeat inside one campaign fails the day:
        pages that overlap are pages that also skip, and counting rows cannot
        tell the two apart.  With no groupings asked for the report is a single
        row, there is no pagination and nothing to check.

        :func:`_page_budget` bounds the walk whatever the answers say.  A short
        page is the one thing that ends it, so an endpoint answering every
        offset with a full page of rows would be read for as long as the task
        lives; the budget turns that into a failure naming it, and is worked out
        from ``limit`` so that the rows a walk is allowed are the same however
        small the pages carrying them.
        """
        records: list[dict] = []
        seen: set[tuple] = set()
        total: int | None = None
        rounded = False
        offset = _STAT_OFFSET_BASE
        budget = _page_budget(self.limit, _MAX_ROWS)

        for _ in range(budget):
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
            rows = page[_ROWS_KEYS["stat"]]
            _log_report_caveats(page, date, campaign_id, self._maskable_token())
            declared_rounded = page.get("total_rows_rounded", _ABSENT)
            flag = _declared_rounded(declared_rounded)
            if flag is None:
                raise _MaskedError(
                    f"AdMetrica answered for campaign {campaign_id} on {date} at "
                    f"offset {offset} with a total_rows_rounded that is not a boolean "
                    f"(the field holds {type(declared_rounded).__name__}). The flag "
                    f"says whether total_rows may be matched exactly, so it decides "
                    f"how strictly the day is checked. The day stops here rather than "
                    f"being checked under a guess."
                )
            rounded = rounded or flag
            declared = _declared_total(page.get("total_rows"))
            if declared is None:
                raise _MaskedError(
                    f"AdMetrica answered for campaign {campaign_id} on {date} at "
                    f"offset {offset} with no readable total_rows (the field holds "
                    f"{type(page.get('total_rows')).__name__}). The total is what the "
                    f"collected rows are checked against, so an answer without one is "
                    f"an answer a short day would pass unnoticed under. The day stops "
                    f"here."
                )
            if total is None:
                total = declared
            elif declared != total and not rounded:
                raise _MaskedError(
                    f"AdMetrica declared {total} rows for campaign {campaign_id} on "
                    f"{date} on an earlier page and {declared} on the page at offset "
                    f"{offset}, both of them exact. The declared total is what the "
                    f"collected rows are checked against, so a total that changes "
                    f"under the walk is one neither answer can be trusted from. The "
                    f"day stops here rather than being checked against a number the "
                    f"API has already replaced."
                )

            for raw_row in rows:
                if dimension_keys:
                    key = _row_key(raw_row)
                    if key in seen:
                        raise _MaskedError(
                            f"AdMetrica returned a row for campaign {campaign_id} on "
                            f"{date} that an earlier page of the same campaign already "
                            f"carried ({_describe_row_key(key, self._maskable_token())}). "
                            f"Pages that repeat a row are pages that "
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
                        dimension_keys,
                        metric_keys,
                    )
                )

            if len(rows) < self.limit:
                break
            offset += len(rows)
        else:
            raise _MaskedError(
                f"AdMetrica kept answering with full pages for campaign "
                f"{campaign_id} on {date} after {budget} of them, "
                f"{budget * self.limit} rows in all. A walk that does not converge "
                f"is one the offset is not moving through, so it stops here rather "
                f"than running for the length of the task."
            )

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
        total: int,
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
        not a reason to fail a day that is probably whole.  The walk under a
        rounded total ends only on a short page, which is the API's own word
        that the rows have run out.
        """
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
        raise _MaskedError(
            f"{summary}. Rows lost between pages are lost silently, so the day "
            f"stops here rather than being written short."
        )
