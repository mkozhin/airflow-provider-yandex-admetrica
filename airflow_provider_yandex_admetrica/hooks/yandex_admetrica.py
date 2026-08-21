"""Hook for the Yandex Metrica for Display Advertising (AdMetrica) report API."""

from __future__ import annotations

import json
import logging
import re

log = logging.getLogger(__name__)

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
