"""Tests for the text bounding and secret masking behind every diagnostic field."""

from __future__ import annotations

import json

import pytest

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _ASSUMED_ENCODING,
    _AUTH_HEADER,
    _BODY_LIMIT,
    _DECODER_MESSAGE_OTHER,
    _HEADER_LIMIT,
    _TEXT_LIMIT,
    _TOKEN_HEAD,
    _TOKEN_MIN_LENGTH,
    _TOKEN_REDACTED,
    _TOKEN_SCHEME,
    _TOKEN_TAIL,
    _TRUNCATED_SUFFIX,
    _bounded_body,
    _bounded_header,
    _declared_charset,
    _decoder_position,
    _drop_cut_token,
    _mask_token,
    _one_line,
    _redact,
    _safe_text,
    _scrub,
    _strip_token,
    _truncate,
)

#: A token long enough to be described by its ends rather than replaced whole.
TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"


class _HostileStr(str):
    """A ``str`` subclass whose every string operation raises.

    Stands for any object that passes an ``isinstance(..., str)`` gate while
    answering the operations behind it with code of its own.
    """

    def __bool__(self) -> bool:
        raise RuntimeError("nope")

    def __len__(self) -> int:
        raise RuntimeError("nope")

    def __getitem__(self, item) -> str:
        raise RuntimeError("nope")


class _Body:
    """A response stand-in whose bytes and ``Content-Type`` are set by the test."""

    def __init__(self, data: bytes, charset: str | None = None) -> None:
        content_type = "application/json"
        if charset is not None:
            content_type = f"{content_type}; charset={charset}"
        self.headers = {"Content-Type": content_type}
        self.content = data


# ---------------------------------------------------------------------------
# _truncate
# ---------------------------------------------------------------------------


class TestTruncate:
    def test_short_string_is_unchanged(self):
        assert _truncate("short") == "short"

    def test_string_exactly_at_the_limit_is_unchanged(self):
        value = "x" * _TEXT_LIMIT

        assert _truncate(value) == value

    def test_one_character_over_the_limit_is_cut_back_to_it(self):
        result = _truncate("z" * (_TEXT_LIMIT + 1))

        assert len(result) == _TEXT_LIMIT
        assert result.endswith("…")

    def test_the_suffix_counts_against_the_limit(self):
        result = _truncate("y" * 100, limit=32)

        assert result == "y" * 31 + "…"

    def test_a_limit_too_small_for_the_suffix_yields_what_fits(self):
        assert _truncate("abcdef", limit=2, suffix="…[cut]") == "…["

    def test_zero_and_negative_limits_yield_nothing(self):
        assert _truncate("anything", limit=0) == ""
        assert _truncate("anything", limit=-5) == ""

    def test_empty_string(self):
        assert _truncate("") == ""


# ---------------------------------------------------------------------------
# _one_line
# ---------------------------------------------------------------------------


class TestOneLine:
    def test_plain_text_is_unchanged(self):
        assert _one_line("rate limit exceeded") == "rate limit exceeded"

    def test_line_breaks_become_single_spaces(self):
        assert _one_line("first\nsecond\r\nthird") == "first second third"

    def test_a_run_of_whitespace_collapses_to_one_space(self):
        assert _one_line("a \t\n  b") == "a b"

    def test_control_characters_collapse_too(self):
        assert _one_line("bad\x00value\x1b[31m") == "bad value [31m"

    def test_the_ends_are_trimmed(self):
        assert _one_line("  padded  ") == "padded"

    def test_text_that_says_nothing_else_comes_back_empty(self):
        assert _one_line(" \n\t\x07 ") == ""


# ---------------------------------------------------------------------------
# _mask_token
# ---------------------------------------------------------------------------


class TestMaskToken:
    def test_long_token_keeps_both_ends(self):
        assert _mask_token(TOKEN) == "y0__xD…q9Az"

    def test_the_middle_of_the_token_is_gone(self):
        result = _mask_token(TOKEN)

        assert "MIDDLE-OF-THE-SECRET" not in result
        assert TOKEN not in result
        assert len(result) == _TOKEN_HEAD + 1 + _TOKEN_TAIL

    def test_the_shortest_described_token_hides_as_much_as_it_shows(self):
        token = "abcdef" + "0123456789" + "wxyz"
        assert len(token) == _TOKEN_MIN_LENGTH

        result = _mask_token(token)

        assert result == "abcdef…wxyz"
        assert "0123456789" not in result

    def test_a_token_as_long_as_the_two_ends_is_replaced_whole(self):
        assert _mask_token("a" * (_TOKEN_HEAD + _TOKEN_TAIL)) == "***"

    def test_a_token_the_mask_would_almost_spell_out_is_replaced_whole(self):
        assert _mask_token("a" * (_TOKEN_HEAD + _TOKEN_TAIL + 1)) == "***"

    def test_a_short_token_is_replaced_whole(self):
        assert _mask_token("short") == "***"

    def test_empty_string_is_replaced_whole(self):
        assert _mask_token("") == "***"

    @pytest.mark.parametrize("value", [None, 12345678901234, ["tok"], b"token-bytes-here"])
    def test_a_non_str_value_is_replaced_whole(self, value):
        assert _mask_token(value) == "***"

    def test_a_str_subclass_is_replaced_whole(self):
        assert _mask_token(_HostileStr(TOKEN)) == "***"


# ---------------------------------------------------------------------------
# _strip_token / _drop_cut_token / _scrub
# ---------------------------------------------------------------------------


class TestStripToken:
    def test_every_occurrence_is_replaced(self):
        text = f"header {TOKEN} and again {TOKEN}"

        result = _strip_token(text, TOKEN, cut=False)

        assert TOKEN not in result
        assert result == f"header {_TOKEN_REDACTED} and again {_TOKEN_REDACTED}"

    def test_text_without_the_token_is_untouched(self):
        assert _strip_token("nothing here", TOKEN, cut=False) == "nothing here"

    @pytest.mark.parametrize("token", [None, "", 42, b"bytes"])
    def test_a_value_that_names_no_token_lets_the_text_through(self, token):
        assert _strip_token("plain", token, cut=False) == "plain"

    def test_a_beginning_of_the_token_at_a_cut_goes_as_well(self):
        text = "body ends with " + TOKEN[:12]

        result = _strip_token(text, TOKEN, cut=True)

        assert result == "body ends with " + _TOKEN_REDACTED
        assert TOKEN[:12] not in result

    def test_a_beginning_of_the_token_survives_when_the_text_was_not_cut(self):
        text = "body ends with " + TOKEN[:12]

        assert _strip_token(text, TOKEN, cut=False) == text

    def test_a_token_spelled_out_between_nuls_drops_the_text_whole(self):
        text = "\x00".join(TOKEN)

        assert _strip_token(text, TOKEN, cut=False) is None


class TestDropCutToken:
    def test_the_longest_matching_beginning_is_taken(self):
        assert _drop_cut_token("x" + TOKEN[:20], TOKEN) == "x" + _TOKEN_REDACTED

    def test_a_whole_token_at_the_end_is_not_this_helper_s_business(self):
        """Whole occurrences are already gone by the time a cut is considered."""
        assert _drop_cut_token("tail", TOKEN) == "tail"

    def test_a_single_matching_character_is_enough(self):
        assert _drop_cut_token("body " + TOKEN[0], TOKEN) == "body " + _TOKEN_REDACTED


class TestScrub:
    def test_the_token_never_survives(self):
        assert TOKEN not in _scrub(f"Authorization: OAuth {TOKEN}", TOKEN)

    @pytest.mark.parametrize("value", [None, 42, b"bytes", ["text"], _HostileStr("x")])
    def test_a_value_that_is_not_exactly_a_str_does_not_travel(self, value):
        assert _scrub(value, TOKEN) is None

    def test_a_token_out_of_reach_drops_the_text(self):
        assert _scrub("\x00".join(TOKEN), TOKEN) is None

    def test_a_token_that_answers_with_code_of_its_own_drops_the_text(self):
        assert _scrub("plain text", _HostileStr(TOKEN)) is None


# ---------------------------------------------------------------------------
# _safe_text
# ---------------------------------------------------------------------------


class TestSafeText:
    def test_plain_message_passes_through(self):
        assert _safe_text("invalid date1", TOKEN) == "invalid date1"

    def test_a_message_is_flattened_before_it_is_bounded(self):
        assert _safe_text("first\nsecond", TOKEN) == "first second"

    def test_a_long_message_is_bounded(self):
        result = _safe_text("m" * 500, TOKEN)

        assert len(result) == _TEXT_LIMIT
        assert result.endswith("…")

    def test_the_limit_is_the_caller_s_to_set(self):
        assert len(_safe_text("m" * 500, TOKEN, limit=20)) == 20

    def test_a_token_reflected_by_the_server_is_masked(self):
        message = json.dumps({"message": f"bad credentials: OAuth {TOKEN}"})

        result = _safe_text(message, TOKEN)

        assert TOKEN not in result
        assert _TOKEN_REDACTED in result

    def test_a_token_split_across_a_line_break_is_masked_too(self):
        """Flattening joins the value before the search for it runs."""
        split = TOKEN[:10] + "\n" + TOKEN[10:]
        token = TOKEN[:10] + " " + TOKEN[10:]

        result = _safe_text(f"echo: {split}", token)

        assert token not in result
        assert result == f"echo: {_TOKEN_REDACTED}"

    def test_a_message_that_says_nothing_is_no_message(self):
        assert _safe_text("   \n  ", TOKEN) is None

    def test_a_message_that_is_nothing_but_the_token_becomes_the_marker(self):
        assert _safe_text(TOKEN, TOKEN) == _TOKEN_REDACTED

    @pytest.mark.parametrize("value", [None, 42, b"bytes", _HostileStr("x")])
    def test_a_non_str_value_is_no_message(self, value):
        assert _safe_text(value, TOKEN) is None

    def test_a_token_out_of_reach_drops_the_message(self):
        assert _safe_text("\x00".join(TOKEN), TOKEN) is None


# ---------------------------------------------------------------------------
# _bounded_header
# ---------------------------------------------------------------------------


class TestBoundedHeader:
    def test_short_value_is_unchanged(self):
        assert _bounded_header("100") == "100"

    def test_long_value_is_bounded(self):
        result = _bounded_header("h" * 200)

        assert len(result) == _HEADER_LIMIT
        assert result.endswith("…")

    def test_missing_and_empty_values_are_none(self):
        assert _bounded_header(None) is None
        assert _bounded_header("") is None

    @pytest.mark.parametrize(
        "value,expected",
        [
            (5, "<non-str header: int>"),
            (["1"], "<non-str header: list>"),
            (b"1", "<non-str header: bytes>"),
        ],
    )
    def test_a_non_str_value_is_described_by_its_type(self, value, expected):
        assert _bounded_header(value) == expected

    def test_a_str_subclass_is_described_by_its_type(self):
        assert _bounded_header(_HostileStr("100")) == "<non-str header: _HostileStr>"


# ---------------------------------------------------------------------------
# _redact
# ---------------------------------------------------------------------------


class TestRedact:
    def test_the_authorization_header_carries_the_mask_and_the_scheme(self):
        headers = {_AUTH_HEADER: f"{_TOKEN_SCHEME} {TOKEN}", "Accept": "application/json"}

        result = _redact(headers, TOKEN)

        assert result[_AUTH_HEADER] == f"{_TOKEN_SCHEME} {_mask_token(TOKEN)}"
        assert TOKEN not in result[_AUTH_HEADER]
        assert result["Accept"] == "application/json"

    def test_the_header_is_rebuilt_rather_than_copied(self):
        """Whatever the outgoing value held, the event carries the mask alone."""
        headers = {_AUTH_HEADER: f"Bearer {TOKEN} trailing junk " + "x" * 500}

        result = _redact(headers, TOKEN)

        assert result[_AUTH_HEADER] == f"{_TOKEN_SCHEME} {_mask_token(TOKEN)}"

    def test_the_header_name_is_matched_whatever_its_case(self):
        result = _redact({"authorization": f"OAuth {TOKEN}"}, TOKEN)

        assert TOKEN not in result["authorization"]

    def test_another_header_repeating_the_token_is_scrubbed(self):
        result = _redact({"X-Echo": f"OAuth {TOKEN}"}, TOKEN)

        assert TOKEN not in result["X-Echo"]
        assert _TOKEN_REDACTED in result["X-Echo"]

    def test_a_long_ordinary_header_is_bounded(self):
        result = _redact({"X-Long": "v" * 200}, TOKEN)

        assert len(result["X-Long"]) == _HEADER_LIMIT

    def test_a_non_str_header_value_is_described_by_its_type(self):
        assert _redact({"X-Count": 7}, TOKEN) == {"X-Count": "<non-str header: int>"}

    def test_a_non_str_header_name_is_described_by_its_type(self):
        assert _redact({7: "v"}, TOKEN) == {"<non-str header name: int>": None}

    def test_no_headers_to_describe(self):
        assert _redact(None, TOKEN) is None
        assert _redact("Authorization: OAuth x", TOKEN) is None

    def test_empty_headers_describe_nothing_and_still_answer(self):
        assert _redact({}, TOKEN) == {}

    def test_a_token_out_of_reach_drops_the_header_value(self):
        result = _redact({"X-Echo": "\x00".join(TOKEN)}, TOKEN)

        assert result["X-Echo"] is None


# ---------------------------------------------------------------------------
# _declared_charset
# ---------------------------------------------------------------------------


class TestDeclaredCharset:
    def test_the_charset_the_server_named_is_read(self):
        assert _declared_charset(_Body(b"", charset="windows-1251")) == "windows-1251"

    def test_the_parameter_is_read_whatever_its_case_and_spacing(self):
        resp = _Body(b"")
        resp.headers["Content-Type"] = 'text/html; CharSet = "UTF-8"'

        assert _declared_charset(resp) == "UTF-8"

    def test_a_header_naming_no_charset(self):
        assert _declared_charset(_Body(b"")) is None

    def test_a_response_with_no_headers_named_nothing(self):
        assert _declared_charset(object()) is None

    def test_a_non_str_header_named_nothing(self):
        resp = _Body(b"")
        resp.headers["Content-Type"] = 42

        assert _declared_charset(resp) is None


# ---------------------------------------------------------------------------
# _bounded_body
# ---------------------------------------------------------------------------


class TestBoundedBody:
    def test_short_body_is_copied_as_it_is(self):
        assert _bounded_body(_Body(b'{"data": []}'), TOKEN) == '{"data": []}'

    def test_a_missing_response_has_no_body(self):
        assert _bounded_body(None, TOKEN) is None

    def test_body_exactly_at_the_limit_is_not_cut(self):
        body = "x" * _BODY_LIMIT

        result = _bounded_body(_Body(body.encode()), TOKEN)

        assert result == body
        assert not result.endswith(_TRUNCATED_SUFFIX)

    def test_a_longer_body_is_cut_within_the_limit_and_says_so(self):
        result = _bounded_body(_Body(b"x" * (_BODY_LIMIT * 5)), TOKEN)

        assert len(result) == _BODY_LIMIT
        assert result.endswith(_TRUNCATED_SUFFIX)

    def test_a_body_without_a_charset_is_read_on_the_assumption(self):
        text = "ошибка авторизации"

        result = _bounded_body(_Body(text.encode(_ASSUMED_ENCODING)), TOKEN)

        assert result == text

    def test_the_charset_the_server_named_is_honoured(self):
        text = "ошибка авторизации"

        result = _bounded_body(_Body(text.encode("windows-1251"), charset="windows-1251"), TOKEN)

        assert result == text

    def test_a_charset_python_does_not_know_falls_back_to_the_assumption(self):
        text = "ошибка"

        result = _bounded_body(_Body(text.encode(_ASSUMED_ENCODING), charset="utf-42"), TOKEN)

        assert result == text

    def test_bytes_that_do_not_decode_are_replaced_rather_than_dropped(self):
        result = _bounded_body(_Body(b"\xff\xfe broken"), TOKEN)

        assert result is not None
        assert "broken" in result

    def test_a_body_echoing_the_token_carries_the_marker_instead(self):
        body = json.dumps({"message": f"bad token: OAuth {TOKEN}"}).encode()

        result = _bounded_body(_Body(body), TOKEN)

        assert TOKEN not in result
        assert _TOKEN_REDACTED in result

    def test_a_beginning_of_the_token_left_by_the_cut_does_not_survive(self):
        head = TOKEN[:15]
        body = ("y" * (_BODY_LIMIT * 4 - len(head)) + head + "tail").encode()

        result = _bounded_body(_Body(body), TOKEN)

        assert head not in result
        assert result.endswith(_TRUNCATED_SUFFIX)

    def test_a_token_echoed_by_a_body_that_goes_on_is_still_replaced(self):
        body = (f"OAuth {TOKEN} " + "y" * (_BODY_LIMIT * 5)).encode()

        result = _bounded_body(_Body(body), TOKEN)

        assert TOKEN not in result
        assert result.startswith(f"OAuth {_TOKEN_REDACTED} ")
        assert result.endswith(_TRUNCATED_SUFFIX)

    def test_a_token_out_of_reach_drops_the_body_whole(self):
        body = "\x00".join(TOKEN).encode("utf-8")

        assert _bounded_body(_Body(body), TOKEN) is None

    def test_a_response_whose_bytes_raise_has_no_body(self):
        class Hostile:
            headers: dict = {}

            @property
            def content(self):
                raise RuntimeError("nope")

        assert _bounded_body(Hostile(), TOKEN) is None


# ---------------------------------------------------------------------------
# _decoder_position
# ---------------------------------------------------------------------------


class TestDecoderPosition:
    def _decode_error(self, document: str) -> json.JSONDecodeError:
        with pytest.raises(json.JSONDecodeError) as exc_info:
            json.loads(document)
        return exc_info.value

    def test_a_known_message_is_copied_with_its_coordinates(self):
        result = _decoder_position(self._decode_error("not json"))

        assert result == "Expecting value: line 1 column 1 (char 0)"

    def test_a_message_formatted_around_the_document_is_replaced(self):
        exc = json.JSONDecodeError("Invalid \\escape: 'q'", '{"a": "\\q"}', 7)

        result = _decoder_position(exc)

        assert result.startswith(_DECODER_MESSAGE_OTHER)
        assert "q" not in result.replace("column", "").replace(_DECODER_MESSAGE_OTHER, "")

    def test_coordinates_of_an_unexpected_type_are_left_out(self):
        exc = json.JSONDecodeError("Extra data", "{}", 0)
        exc.lineno = "one"

        assert _decoder_position(exc) == "Extra data"

    def test_another_exception_is_left_to_its_type_alone(self):
        assert _decoder_position(ValueError("could not parse {\"token\": \"x\"}")) is None
        assert _decoder_position(RuntimeError("boom")) is None
