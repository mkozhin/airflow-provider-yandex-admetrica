"""Tests for the text bounding and secret masking behind every diagnostic field."""

from __future__ import annotations

import json

import pytest

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _ASSUMED_ENCODING,
    _AUTH_HEADER,
    _BODY_LIMIT,
    _DECODER_MESSAGE_OTHER,
    _ENDPOINT_URLS,
    _HEADER_LIMIT,
    _OUTCOME_UNKNOWN,
    _PARAM_NAME_EMPTY,
    _PARAM_NAME_LIMIT,
    _PARAM_VALUE_LIMIT,
    _PARAMS_MAX,
    _PARAMS_TRUNCATED,
    _SCHEMA_VERSION,
    _TEXT_LIMIT,
    _TOKEN_HEAD,
    _TOKEN_MIN_LENGTH,
    _TOKEN_REDACTED,
    _TOKEN_SCHEME,
    _TOKEN_TAIL,
    _TRUNCATED_SUFFIX,
    _bounded_body,
    _bounded_header,
    _classify_payload,
    _declared_charset,
    _decoder_position,
    _describe_error,
    _describe_unreadable_body,
    _seconds_until,
    _stamp_report_meta,
    _summarize_rows,
    _drop_cut_token,
    _declared_total,
    _emit_event,
    _event_level,
    _find_error,
    _mask_token,
    _meta_value,
    _new_event,
    _one_line,
    _record_exception,
    _record_rate_limit,
    _redact,
    _redact_params,
    _safe_text,
    _scrub,
    _stamp_duration,
    _stamp_response_error,
    _strip_token,
    _summarize_error,
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


# ---------------------------------------------------------------------------
# Helpers for the event schema
# ---------------------------------------------------------------------------


#: Every field a pushed event carries, whatever happened to the request.
EVENT_KEYS = frozenset(
    {
        "schema_version",
        "outcome",
        "level",
        "sent_at",
        "endpoint",
        "advertiser_id",
        "campaign_id",
        "date",
        "offset",
        "attempt",
        "max_attempts",
        "request_method",
        "request_url",
        "request_params",
        "request_headers",
        "http_status",
        "duration_ms",
        "rows_count",
        "rows_shape_ok",
        "payload_kind",
        "total_rows",
        "total_rows_rounded",
        "sampled",
        "sample_share",
        "sample_size",
        "sample_space",
        "contains_sensitive_data",
        "data_lag",
        "error_code",
        "error_type",
        "error_message",
        "exception_type",
        "exception_message",
        "rate_limit_limit",
        "rate_limit_remaining",
        "response_body",
    }
)


def _event(**overrides) -> dict:
    """A statistics-request event, with the fields a test cares about stamped on."""
    event = _new_event(
        endpoint="stat",
        params={"ids": 123456, "date1": "2026-08-20"},
        headers={_AUTH_HEADER: f"{_TOKEN_SCHEME} {TOKEN}"},
        token=TOKEN,
        advertiser_id=17004,
        campaign_id=123456,
        date="2026-08-20",
        offset=1,
        attempt=1,
        max_attempts=4,
    )
    event.update(overrides)
    return event


class _Sink:
    """A Loki stand-in that records what it was handed."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.pushed: list[dict] = []

    def push(self, event: dict) -> None:
        self.pushed.append(dict(event))


class _Answer:
    """A response stand-in answering with a body of the test's choosing."""

    def __init__(self, payload=None, *, content: bytes = b"", headers=None) -> None:
        self._payload = payload
        self.content = content
        self.headers = headers if headers is not None else {}

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON object could be decoded")
        return self._payload


# ---------------------------------------------------------------------------
# _redact_params
# ---------------------------------------------------------------------------


class TestRedactParams:
    def test_scalars_travel_as_they_are(self):
        params = {"ids": 123456, "limit": 10000, "pretty": True, "sort": None}
        assert _redact_params(params, TOKEN) == params

    def test_a_value_that_is_neither_text_nor_scalar_is_named_by_its_type(self):
        assert _redact_params({"metrics": ["am:e:renders"]}, TOKEN) == {
            "metrics": "<non-scalar param: list>"
        }

    def test_a_non_str_name_is_named_by_its_type(self):
        assert _redact_params({7: "x"}, TOKEN) == {"<non-str param name: int>": "x"}

    def test_a_parameter_carrying_the_token_travels_masked(self):
        redacted = _redact_params({"oauth_token": TOKEN}, TOKEN)

        assert TOKEN not in json.dumps(redacted)
        assert redacted["oauth_token"] == _TOKEN_REDACTED

    def test_a_name_carrying_the_token_travels_masked(self):
        """A credential lands in the name of a parameter as readily as in its value."""
        redacted = _redact_params({f"proxy {TOKEN}": "1"}, TOKEN)

        assert TOKEN not in json.dumps(redacted)
        assert list(redacted) == [f"proxy {_TOKEN_REDACTED}"]

    def test_a_name_the_gate_leaves_nothing_of_still_describes_its_parameter(self):
        """Whitespace alone says nothing, and the parameter is described anyway."""
        redacted = _redact_params({"  ": "2", "\t": "3"}, TOKEN)

        assert list(redacted.values()) == ["2", "3"]
        assert all(_PARAM_NAME_EMPTY in name for name in redacted)

    def test_a_name_is_flattened_onto_one_line(self):
        assert _redact_params({"a\n b": "1"}, TOKEN) == {"a b": "1"}

    def test_two_names_sharing_a_bounded_prefix_keep_both_values(self):
        """A name cut to its budget must not take another parameter's place."""
        params = {"n" * 500 + str(i): str(i) for i in range(3)}

        redacted = _redact_params(params, TOKEN)

        assert len(redacted) == 3
        assert sorted(redacted.values()) == ["0", "1", "2"]
        assert all(len(name) <= _PARAM_NAME_LIMIT for name in redacted)

    def test_a_value_is_flattened_onto_one_line(self):
        assert _redact_params({"filters": "a\n b"}, TOKEN) == {"filters": "a b"}

    def test_a_long_value_is_cut_to_the_value_budget(self):
        redacted = _redact_params({"filters": "x" * 10000}, TOKEN)

        assert len(redacted["filters"]) == _PARAM_VALUE_LIMIT

    def test_a_long_name_is_cut_to_the_name_budget(self):
        redacted = _redact_params({"n" * 500: "x"}, TOKEN)

        (name,) = redacted
        assert len(name) == _PARAM_NAME_LIMIT

    def test_only_so_many_parameters_are_described_at_all(self):
        params = {f"p{i}": "x" * 1000 for i in range(_PARAMS_MAX + 7)}

        redacted = _redact_params(params, TOKEN)

        described = {k: v for k, v in redacted.items() if k != _PARAMS_TRUNCATED}
        assert len(described) == _PARAMS_MAX
        assert redacted[_PARAMS_TRUNCATED] == 7

    def test_the_field_cannot_outgrow_its_share_of_the_line(self):
        params = {"n" * 500 + str(i): "x" * 10000 for i in range(_PARAMS_MAX + 7)}

        redacted = _redact_params(params, TOKEN)

        described = {k: v for k, v in redacted.items() if k != _PARAMS_TRUNCATED}
        spent = sum(len(k) + len(v or "") for k, v in described.items())
        assert spent <= _PARAMS_MAX * (_PARAM_NAME_LIMIT + _PARAM_VALUE_LIMIT)

    def test_parameters_within_the_budget_are_all_described(self):
        params = {f"p{i}": "x" * 10 for i in range(_PARAMS_MAX)}

        assert _redact_params(params, TOKEN) == params

    def test_a_non_dict_is_no_parameters_at_all(self):
        assert _redact_params("ids=1", TOKEN) is None
        assert _redact_params(None, TOKEN) is None


# ---------------------------------------------------------------------------
# _new_event
# ---------------------------------------------------------------------------


class TestNewEvent:
    def test_every_field_of_the_schema_is_present(self):
        assert set(_event()) == EVENT_KEYS

    def test_a_request_for_the_campaign_list_carries_the_same_keys(self):
        event = _new_event(
            endpoint="campaigns",
            params={"advertiser_id": 17004, "limit": 100, "offset": 0},
            headers={_AUTH_HEADER: f"{_TOKEN_SCHEME} {TOKEN}"},
            token=TOKEN,
            advertiser_id=17004,
            attempt=1,
            max_attempts=4,
        )

        assert set(event) == EVENT_KEYS
        assert (event["campaign_id"], event["date"], event["offset"]) == (None, None, None)

    def test_fields_the_attempt_has_yet_to_determine_start_empty(self):
        event = _event()

        undetermined = EVENT_KEYS - {
            "schema_version",
            "outcome",
            "sent_at",
            "endpoint",
            "advertiser_id",
            "campaign_id",
            "date",
            "offset",
            "attempt",
            "max_attempts",
            "request_method",
            "request_url",
            "request_params",
            "request_headers",
        }
        assert {event[key] for key in undetermined} == {None}

    def test_the_outcome_starts_undetermined(self):
        assert _event()["outcome"] == _OUTCOME_UNKNOWN

    def test_the_schema_version_is_stamped(self):
        assert _event()["schema_version"] == _SCHEMA_VERSION

    def test_the_field_set_is_the_second_one(self):
        """Spelled out, not read from the module.

        The number is what a reader of a stored event resolves its field set
        by, so a change to it is a change to the contract with that reader and
        has to be made deliberately rather than travel along with an edit to
        the schema.
        """
        assert _event()["schema_version"] == 2

    @pytest.mark.parametrize("endpoint", ["stat", "campaigns"])
    def test_the_url_follows_the_endpoint(self, endpoint):
        event = _new_event(
            endpoint=endpoint, params={}, headers={}, token=TOKEN, attempt=1, max_attempts=4
        )

        assert event["request_url"] == _ENDPOINT_URLS[endpoint]
        assert event["request_method"] == "GET"

    def test_the_authorization_header_reaches_the_event_masked(self):
        event = _event()

        assert TOKEN not in json.dumps(event["request_headers"])
        assert event["request_headers"][_AUTH_HEADER].startswith(f"{_TOKEN_SCHEME} ")

    def test_the_sent_at_stamp_is_an_utc_timestamp(self):
        from datetime import datetime

        stamp = datetime.fromisoformat(_event()["sent_at"])

        assert stamp.utcoffset().total_seconds() == 0


# ---------------------------------------------------------------------------
# _summarize_rows / _classify_payload
# ---------------------------------------------------------------------------


class TestClassifyPayload:
    def test_a_well_formed_report_reads(self):
        event = _event()
        rows = [{"dimensions": [{"name": "Главная"}], "metrics": [1]}]

        assert _classify_payload(event, {"data": rows}) is True
        assert event["rows_count"] == 1
        assert event["rows_shape_ok"] is True
        assert event["payload_kind"] == "dict"
        assert event["outcome"] == "success"

    def test_a_day_without_rows_reads(self):
        event = _event()

        assert _classify_payload(event, {"data": []}) is True
        assert event["rows_count"] == 0
        assert event["outcome"] == "success"

    def test_the_campaign_list_reads_under_its_own_key(self):
        event = _new_event(
            endpoint="campaigns", params={}, headers={}, token=TOKEN, attempt=1, max_attempts=4
        )

        assert _classify_payload(event, {"campaigns": [{"campaign_id": 1}], "total": 1}) is True
        assert event["rows_count"] == 1
        assert event["outcome"] == "success"

    def test_the_other_endpoints_key_is_not_read(self):
        event = _event()

        assert _classify_payload(event, {"campaigns": [{"campaign_id": 1}]}) is False
        assert event["payload_kind"] == "rows_absent"

    def test_an_absent_rows_key_stops_the_attempt(self):
        event = _event()

        assert _classify_payload(event, {"query": {}}) is False
        assert event["payload_kind"] == "rows_absent"
        assert event["rows_count"] is None
        assert event["outcome"] == "empty_shape"

    def test_a_rows_value_that_is_not_a_list_stops_the_attempt(self):
        event = _event()

        assert _classify_payload(event, {"data": {"row": 1}}) is False
        assert event["payload_kind"] == "rows_non_list"
        assert event["outcome"] == "empty_shape"

    def test_an_empty_rows_value_of_another_type_is_an_empty_answer(self):
        event = _event()

        assert _classify_payload(event, {"data": None}) is False
        assert event["payload_kind"] == "dict"
        assert event["outcome"] == "empty_shape"

    def test_a_list_holding_something_other_than_rows_stops_the_attempt(self):
        event = _event()

        assert _classify_payload(event, {"data": [{"metrics": [1]}, "row"]}) is False
        assert event["rows_count"] == 2
        assert event["rows_shape_ok"] is False
        assert event["payload_kind"] == "dict"
        assert event["outcome"] == "empty_shape"

    def test_a_body_that_is_not_a_dict_is_unexpected(self):
        event = _event()

        assert _classify_payload(event, ["data"]) is False
        assert event["payload_kind"] == "non_dict"
        assert event["outcome"] == "unexpected_error"

    def test_a_body_that_refuses_to_be_read_is_unexpected(self):
        class Hostile(dict):
            def __contains__(self, item):
                raise RuntimeError("nope")

        event = _event()

        assert _classify_payload(event, Hostile()) is False
        assert event["payload_kind"] == "non_dict"
        assert event["outcome"] == "unexpected_error"


# ---------------------------------------------------------------------------
# _find_error / _summarize_error
# ---------------------------------------------------------------------------


class TestFindError:
    def test_an_error_under_a_key_of_its_own(self):
        assert _find_error({"error": {"code": 403, "message": "no"}}) == {
            "code": 403,
            "message": "no",
        }

    def test_the_plural_spelling_describes_its_first_element(self):
        body = {"errors": [{"message": "first"}, {"message": "second"}]}

        assert _find_error(body) == {"message": "first"}

    def test_a_code_and_a_message_at_the_top_level(self):
        body = {"code": 403, "message": "Forbidden"}

        assert _find_error(body) is body

    def test_a_message_without_a_code_is_not_a_refusal(self):
        assert _find_error({"message": "Forbidden"}) is None

    def test_a_zero_code_names_no_refusal(self):
        assert _find_error({"code": 0, "message": "ok"}) is None

    def test_a_successful_report_carries_no_error(self):
        assert _find_error({"data": [], "total_rows": 0}) is None

    def test_an_error_that_is_not_a_dict_is_still_an_error(self):
        assert _find_error({"error": "Forbidden"}) == "Forbidden"

    def test_a_body_that_is_not_a_dict_carries_no_error(self):
        assert _find_error(["error"]) is None

    def test_a_body_that_refuses_to_be_read_carries_no_error(self):
        class Hostile(dict):
            def get(self, key, default=None):
                raise RuntimeError("nope")

        assert _find_error(Hostile()) is None


class TestSummarizeError:
    def test_the_code_and_the_message_are_taken(self):
        assert _summarize_error({"code": 403, "message": "Forbidden"}, TOKEN) == (
            403,
            None,
            "Forbidden",
        )

    def test_a_reflected_token_leaves_the_message_masked(self):
        error = {"code": 401, "message": f"Invalid credentials: {_TOKEN_SCHEME} {TOKEN}"}

        code, error_type, message = _summarize_error(error, TOKEN)

        assert (code, error_type) == (401, None)
        assert TOKEN not in message
        assert _TOKEN_REDACTED in message

    def test_a_message_the_token_survives_does_not_travel(self):
        error = {"code": 401, "message": " ".join(TOKEN)}

        assert _summarize_error(error, TOKEN) == (401, None, None)

    def test_a_message_is_flattened_and_bounded(self):
        code, error_type, message = _summarize_error(
            {"message": "a\n\nb" + "x" * _TEXT_LIMIT}, TOKEN
        )

        assert (code, error_type) == (None, None)
        assert len(message) == _TEXT_LIMIT
        assert message.startswith("a b")

    def test_a_code_spelled_as_text_is_left_unread(self):
        assert _summarize_error({"code": "403", "message": "no"}, TOKEN) == (None, None, "no")

    def test_a_flag_is_not_a_code(self):
        assert _summarize_error({"code": True, "message": "no"}, TOKEN) == (None, None, "no")

    def test_an_error_that_is_not_a_dict_is_named_by_its_type(self):
        assert _summarize_error("Forbidden", TOKEN) == (None, None, "<non-dict error: str>")

    def test_a_message_that_is_not_text_is_named_by_its_type(self):
        assert _summarize_error({"code": 1, "message": {"ru": "no"}}, TOKEN) == (
            1,
            None,
            "<non-str message: dict>",
        )

    def test_an_error_without_a_message(self):
        assert _summarize_error({"code": 500}, TOKEN) == (500, None, None)

    def test_a_message_that_says_nothing_is_no_message(self):
        assert _summarize_error({"code": 500, "message": "  \n "}, TOKEN) == (500, None, None)

    def test_the_refusal_this_api_really_sends_is_read_as_a_type(self):
        """The body a refused report answers with, copied from the wire.

        The code sits at the top level of the document while ``_find_error``
        describes ``errors[0]``, so ``error_code`` stays empty and the type is
        the only name the refusal has.
        """
        error = _find_error(
            {
                "errors": [
                    {
                        "error_type": "query_error",
                        "message": "Query is too complicated. Please reduce the date "
                        "interval or sampling.",
                    }
                ],
                "code": 400,
                "message": "Query is too complicated. Please reduce the date interval "
                "or sampling.",
            }
        )

        code, error_type, message = _summarize_error(error, TOKEN)

        assert (code, error_type) == (None, "query_error")
        assert message.startswith("Query is too complicated")

    def test_a_type_that_is_not_text_is_named_by_its_type(self):
        assert _summarize_error({"error_type": {"ru": "no"}, "message": "no"}, TOKEN) == (
            None,
            "<non-str error_type: dict>",
            "no",
        )

    def test_a_refusal_naming_only_a_type_carries_it_alone(self):
        assert _summarize_error({"error_type": "query_error"}, TOKEN) == (
            None,
            "query_error",
            None,
        )

    def test_a_reflected_token_leaves_the_type_masked(self):
        error = {"error_type": f"{_TOKEN_SCHEME} {TOKEN}", "message": "no"}

        code, error_type, message = _summarize_error(error, TOKEN)

        assert (code, message) == (None, "no")
        assert TOKEN not in error_type
        assert _TOKEN_REDACTED in error_type

    def test_a_type_the_token_survives_does_not_travel(self):
        error = {"error_type": " ".join(TOKEN), "message": "no"}

        assert _summarize_error(error, TOKEN) == (None, None, "no")


class TestDescribeError:
    def test_both_parts(self):
        assert _describe_error(403, None, "Forbidden") == "code 403: Forbidden"

    def test_a_code_alone(self):
        assert _describe_error(403, None, None) == "code 403"

    def test_a_message_alone(self):
        assert _describe_error(None, None, "Forbidden") == "Forbidden"

    def test_none_of_the_three(self):
        assert _describe_error(None, None, None) == "nothing the refusal named"

    def test_the_type_stands_between_the_code_and_the_message(self):
        assert _describe_error(403, "invalid_token", "Forbidden") == (
            "code 403, invalid_token: Forbidden"
        )

    def test_a_type_alone_carries_the_phrase(self):
        assert _describe_error(None, "query_error", None) == "query_error"

    def test_a_type_and_a_message_without_a_code(self):
        assert _describe_error(None, "query_error", "too complicated") == (
            "query_error: too complicated"
        )


class TestDescribeUnreadableBody:
    def test_the_kind_is_named(self):
        event = _event(payload_kind="rows_absent", rows_count=None)

        assert _describe_unreadable_body(event) == "no readable rows (payload_kind=rows_absent)"

    def test_a_counted_list_says_how_many_it_held(self):
        event = _event(payload_kind="dict", rows_count=2)

        assert (
            _describe_unreadable_body(event)
            == "no readable rows (payload_kind=dict, rows_count=2)"
        )


class TestStampResponseError:
    def test_a_refusal_reaches_the_event(self):
        event = _event()

        kind = _stamp_response_error(
            event, _Answer({"error": {"code": 403, "message": "no"}}), TOKEN
        )

        assert (event["error_code"], event["error_type"], event["error_message"]) == (
            403,
            None,
            "no",
        )
        assert kind is None

    def test_the_type_reaches_the_event_and_comes_back(self):
        event = _event()
        body = {"errors": [{"error_type": "query_error", "message": "too complicated"}]}

        kind = _stamp_response_error(event, _Answer(body), TOKEN)

        assert kind == "query_error"
        assert event["error_type"] == "query_error"

    def test_the_kind_that_comes_back_is_the_one_the_body_spelled(self):
        """The decision is read off the API's own word, the event off the gate.

        A type the masking gate rewrites or blanks still names the kind of
        refusal the API stated, so what the provider does about it stays where
        the API put it rather than following the diagnostic layer.
        """
        event = _event()
        body = {"errors": [{"error_type": f"query_error {TOKEN}", "message": "no"}]}

        kind = _stamp_response_error(event, _Answer(body), TOKEN)

        assert kind == f"query_error {TOKEN}"
        assert TOKEN not in json.dumps(event)

    def test_a_type_that_is_not_text_names_no_kind(self):
        event = _event()
        body = {"errors": [{"error_type": 400, "message": "no"}]}

        assert _stamp_response_error(event, _Answer(body), TOKEN) is None
        assert event["error_type"] == "<non-str error_type: int>"

    def test_a_reflected_token_reaches_the_event_masked(self):
        event = _event()
        body = {"error": {"code": 401, "message": f"header was {_TOKEN_SCHEME} {TOKEN}"}}

        _stamp_response_error(event, _Answer(body), TOKEN)

        assert TOKEN not in json.dumps(event)
        assert _TOKEN_REDACTED in event["error_message"]

    def test_an_answer_naming_no_error_leaves_the_pair_alone(self):
        event = _event()

        _stamp_response_error(event, _Answer({"data": []}), TOKEN)

        assert (event["error_code"], event["error_message"]) == (None, None)

    def test_a_body_that_will_not_parse_leaves_the_pair_alone(self):
        event = _event()

        _stamp_response_error(event, _Answer(None), TOKEN)

        assert (event["error_code"], event["error_message"]) == (None, None)


# ---------------------------------------------------------------------------
# _stamp_duration / _record_exception / _record_rate_limit
# ---------------------------------------------------------------------------


class TestStampDuration:
    def test_the_duration_is_recorded_in_milliseconds(self, monkeypatch):
        import time as time_module

        event = _event()
        monkeypatch.setattr(time_module, "monotonic", lambda: 12.5)

        _stamp_duration(event, 10.0)

        assert event["duration_ms"] == 2500


class TestRecordException:
    def test_the_type_is_recorded(self):
        event = _event()

        _record_exception(event, ValueError("boom"))

        assert event["exception_type"] == "ValueError"

    def test_the_text_never_travels(self):
        event = _event()

        _record_exception(event, RuntimeError(f"proxy https://user:{TOKEN}@proxy"))

        assert TOKEN not in json.dumps(event)
        assert event["exception_message"] is None

    def test_the_first_type_recorded_is_the_one_kept(self):
        event = _event()

        _record_exception(event, ValueError("first"))
        _record_exception(event, RuntimeError("second"))

        assert event["exception_type"] == "ValueError"


class TestRecordRateLimit:
    def test_the_headers_are_copied_bounded(self):
        event = _event()
        resp = _Answer(headers={"X-RateLimit-Limit": "9" * 100, "X-RateLimit-Remaining": "0"})

        _record_rate_limit(event, resp, TOKEN)

        assert len(event["rate_limit_limit"]) == _HEADER_LIMIT
        assert event["rate_limit_remaining"] == "0"

    def test_a_header_repeating_the_token_carries_the_marker_instead(self):
        """A response header is text the server wrote, so it passes the gate too."""
        event = _event()
        resp = _Answer(headers={"X-RateLimit-Limit": TOKEN, "X-RateLimit-Remaining": "0"})

        _record_rate_limit(event, resp, TOKEN)

        assert TOKEN not in str(event["rate_limit_limit"])
        assert event["rate_limit_limit"] == _TOKEN_REDACTED

    def test_an_answer_naming_neither_leaves_both_empty(self):
        event = _event()

        _record_rate_limit(event, _Answer(headers={}), TOKEN)

        assert (event["rate_limit_limit"], event["rate_limit_remaining"]) == (None, None)

    def test_a_response_that_refuses_to_be_read_is_not_an_error(self):
        event = _event()

        _record_rate_limit(event, object(), TOKEN)

        assert (event["rate_limit_limit"], event["rate_limit_remaining"]) == (None, None)


# ---------------------------------------------------------------------------
# _event_level
# ---------------------------------------------------------------------------


class TestEventLevel:
    @pytest.mark.parametrize(
        ("event", "expected"),
        [
            (_event(outcome="success", rows_count=100), "info"),
            (_event(outcome="success", rows_count=0), "info"),
            (_event(outcome="retryable_error", attempt=1, max_attempts=4), "warn"),
            (_event(outcome="retryable_error", attempt=4, max_attempts=4), "error"),
            (_event(outcome="auth_error", attempt=1, max_attempts=4), "error"),
            (_event(outcome="http_error", attempt=1, max_attempts=4), "error"),
            (_event(outcome="network_error", attempt=1, max_attempts=4), "warn"),
            (_event(outcome="network_error", attempt=4, max_attempts=4), "error"),
            (_event(outcome="invalid_json"), "error"),
            (_event(outcome="empty_shape"), "error"),
            (_event(outcome="unexpected_error"), "error"),
        ],
        ids=[
            "success_with_rows",
            "success_without_rows",
            "retryable_with_an_attempt_left",
            "retryable_on_the_last_attempt",
            "auth_error",
            "http_error",
            "network_with_an_attempt_left",
            "network_on_the_last_attempt",
            "invalid_json",
            "empty_shape",
            "unexpected_error",
        ],
    )
    def test_the_level_table(self, event, expected):
        assert _event_level(event) == expected


# ---------------------------------------------------------------------------
# _emit_event
# ---------------------------------------------------------------------------


class TestEmitEvent:
    def test_a_readable_answer_keeps_its_body_inside_the_process(self):
        sink = _Sink()
        event = _event(outcome="success", rows_count=1)

        _emit_event(sink, event, _Answer(content=b'{"data": []}'), TOKEN)

        assert sink.pushed[0]["level"] == "info"
        assert sink.pushed[0]["response_body"] is None

    @pytest.mark.parametrize("outcome", ["retryable_error", "empty_shape"], ids=["warn", "error"])
    def test_everything_else_travels_with_its_body(self, outcome):
        """The event is built here: `_emit_event` writes into the one it is given."""
        sink = _Sink()
        event = _event(outcome=outcome)

        _emit_event(sink, event, _Answer(content=b"upstream refused"), TOKEN)

        assert sink.pushed[0]["level"] != "info"
        assert sink.pushed[0]["response_body"] == "upstream refused"

    def test_a_body_echoing_the_token_travels_masked(self):
        sink = _Sink()
        body = f'{{"message": "{_TOKEN_SCHEME} {TOKEN}"}}'.encode()

        _emit_event(sink, _event(outcome="empty_shape"), _Answer(content=body), TOKEN)

        assert TOKEN not in json.dumps(sink.pushed[0])
        assert _TOKEN_REDACTED in sink.pushed[0]["response_body"]

    @pytest.mark.parametrize(
        "outcome",
        ["success", "retryable_error", "auth_error", "unexpected_error"],
        ids=["info", "warn", "error_auth", "error_body"],
    )
    def test_the_request_headers_never_carry_the_token(self, outcome):
        sink = _Sink()
        event = _event(outcome=outcome)

        _emit_event(sink, event, _Answer(content=TOKEN.encode()), TOKEN)

        assert TOKEN not in json.dumps(sink.pushed[0])

    def test_a_sink_whose_breaker_tripped_is_handed_nothing(self):
        sink = _Sink(enabled=False)
        event = _event(outcome="empty_shape")

        _emit_event(sink, event, _Answer(content=b"body"), TOKEN)

        assert sink.pushed == []
        assert event["level"] is None
        assert event["response_body"] is None

    def test_a_sink_that_offers_push_alone_counts_as_ready(self):
        pushed = []

        class Bare:
            def push(self, event):
                pushed.append(event)

        _emit_event(Bare(), _event(outcome="success"), _Answer(), TOKEN)

        assert len(pushed) == 1


# ---------------------------------------------------------------------------
# _meta_value and _declared_total
# ---------------------------------------------------------------------------


class TestMetaValue:
    @pytest.mark.parametrize("value", [0, 1234, 0.5, True, False, None])
    def test_numbers_and_flags_travel_as_they_arrived(self, value):
        assert _meta_value(value, TOKEN) is value

    def test_text_leaves_by_the_gate_every_text_leaves_by(self):
        assert _meta_value(f"{_TOKEN_SCHEME} {TOKEN}", TOKEN) == f"{_TOKEN_SCHEME} {_TOKEN_REDACTED}"

    def test_a_value_of_another_kind_is_named_by_its_type(self):
        """Converting it would run code the answer chose."""
        assert _meta_value({"share": 0.1}, TOKEN) == "<dict>"
        assert _meta_value([1, 2], TOKEN) == "<list>"
        assert _meta_value(object(), TOKEN) == "<object>"


class TestDeclaredTotal:
    def test_a_whole_number_is_the_total(self):
        assert _declared_total(0) == 0
        assert _declared_total(1234) == 1234

    @pytest.mark.parametrize("value", [True, False], ids=["true", "false"])
    def test_a_flag_declares_no_total(self, value):
        """Python counts a flag as an int; `total_rows: true` is not a count of one."""
        assert _declared_total(value) is None

    @pytest.mark.parametrize("value", ["1500", 1500.0, None, [], {"total": 1}])
    def test_anything_else_declares_no_total(self, value):
        assert _declared_total(value) is None


# ---------------------------------------------------------------------------
# The "never raises" promises, met with values that answer in code of their own
# ---------------------------------------------------------------------------


class _HostileMapping(dict):
    """A mapping whose own reading raises, as a substituted answer may."""

    def items(self):
        raise RuntimeError("boom")


class _HostileBool:
    """A value whose truthiness raises."""

    def __bool__(self):
        raise RuntimeError("boom")


class TestNothingHereRaises:
    """Each of these runs while an event is assembled, often with a failure in flight."""

    def test_headers_that_refuse_to_be_read_are_no_headers(self):
        assert _redact(_HostileMapping(), TOKEN) is None

    def test_parameters_that_refuse_to_be_read_are_no_parameters(self):
        assert _redact_params(_HostileMapping(), TOKEN) is None

    def test_a_header_value_of_another_kind_is_named_by_its_type(self):
        assert _bounded_header(42) == "<non-str header: int>"
        assert _bounded_header(None) is None
        assert _bounded_header("") is None

    def test_a_header_name_of_another_kind_is_named_by_its_type(self):
        assert _redact({7: "x"}, TOKEN) == {"<non-str header name: int>": None}

    def test_a_rows_value_whose_truthiness_raises_is_the_loud_half(self):
        count, shape_ok, kind = _summarize_rows({"data": _HostileBool()}, "data")
        assert (count, shape_ok, kind) == (None, False, "rows_non_list")

    def test_a_rows_value_present_but_empty_of_another_type_is_a_dict_body(self):
        assert _summarize_rows({"data": ""}, "data") == (None, False, "dict")

    def test_a_body_that_is_not_a_dict_carries_no_caveats(self):
        event = _event()
        _stamp_report_meta(event, "not a dict", TOKEN)
        assert event["total_rows"] is None

    def test_a_body_that_refuses_to_be_read_carries_no_caveats(self):
        event = _event()
        _stamp_report_meta(event, _HostileMapping(), TOKEN)
        assert event["sampled"] is None

    @pytest.mark.parametrize(
        "value",
        ["not a date at all", "", "Tue, 99 Xxx 2026 00:00:00 GMT"],
        ids=["words", "empty", "impossible"],
    )
    def test_a_wait_nobody_can_parse_is_a_wait_nobody_asked_for(self, value):
        assert _seconds_until(value) is None

    def test_a_response_that_refuses_to_be_read_carries_no_rate_limit(self):
        class Hostile:
            @property
            def headers(self):
                raise RuntimeError("boom")

        event = _event()
        _record_rate_limit(event, Hostile(), TOKEN)
        assert (event["rate_limit_limit"], event["rate_limit_remaining"]) == (None, None)
