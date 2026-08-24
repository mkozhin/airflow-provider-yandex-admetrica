"""Tests for the connection the hook reads its advertiser and its token from."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    DEFAULT_LIMIT,
    DEFAULT_REQUEST_DELAY,
    AdmetricaHook,
    parse_connection,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"


def _hook(*, password: str | None = TOKEN, extra: object = None, **kwargs) -> AdmetricaHook:
    """Return a hook whose connection is the one described by the arguments."""
    if extra is None:
        extra = {"advertiser_id": 17004}
    conn = Connection(
        conn_id="admetrica",
        conn_type="http",
        password=password,
        extra=extra if isinstance(extra, str) else json.dumps(extra),
    )
    hook = AdmetricaHook(admetrica_conn_id="admetrica", **kwargs)
    hook.get_connection = MagicMock(return_value=conn)
    return hook


class TestParseConnection:
    def test_reads_a_whole_number(self):
        assert parse_connection({"advertiser_id": 17004}) == 17004

    def test_reads_a_number_written_as_text(self):
        assert parse_connection({"advertiser_id": "17004"}) == 17004

    def test_reads_a_number_written_as_padded_text(self):
        assert parse_connection({"advertiser_id": " 17004 "}) == 17004

    def test_empty_extra_names_no_advertiser(self, caplog):
        assert parse_connection({}) is None
        assert "advertiser_id" in caplog.text

    def test_missing_field_names_no_advertiser(self, caplog):
        assert parse_connection({"something_else": 1}) is None
        assert "advertiser_id" in caplog.text

    def test_null_field_names_no_advertiser(self):
        assert parse_connection({"advertiser_id": None}) is None

    @pytest.mark.parametrize(
        "value",
        ["not-a-number", "", "17004.5", 17004.5, True, False, 0, -1, [17004], {"id": 17004}],
    )
    def test_unusable_value_names_no_advertiser(self, value, caplog):
        assert parse_connection({"advertiser_id": value}) is None
        assert "advertiser_id" in caplog.text

    def test_extra_that_is_not_an_object_names_no_advertiser(self, caplog):
        assert parse_connection(["advertiser_id"]) is None
        assert "not an object" in caplog.text

    def test_a_mapping_that_refuses_to_be_read_names_no_advertiser(self):
        class Hostile(dict):
            def get(self, key, default=None):
                raise RuntimeError("boom")

        assert parse_connection(Hostile()) is None

    def test_warning_does_not_spell_out_the_unusable_value(self, caplog):
        assert parse_connection({"advertiser_id": TOKEN}) is None
        assert TOKEN not in caplog.text

    def test_other_extra_fields_are_not_read(self):
        assert parse_connection({"advertiser_id": 17004, "token": TOKEN, "limit": 5}) == 17004


class TestHookAttributes:
    def test_class_attributes(self):
        assert AdmetricaHook.conn_name_attr == "admetrica_conn_id"
        assert AdmetricaHook.default_conn_name == "yandex_admetrica_default"
        assert AdmetricaHook.conn_type == "http"
        assert AdmetricaHook.hook_name == "Yandex AdMetrica"

    def test_defaults(self):
        hook = AdmetricaHook()
        assert hook.admetrica_conn_id == AdmetricaHook.default_conn_name
        assert hook.request_delay == DEFAULT_REQUEST_DELAY
        assert hook.limit == DEFAULT_LIMIT
        assert hook._loki is None

    def test_lazy_fields_start_empty(self):
        hook = AdmetricaHook()
        assert hook._connection is None
        assert hook._campaigns is None

    def test_arguments_are_keyword_only(self):
        with pytest.raises(TypeError):
            AdmetricaHook("admetrica")

    def test_arguments_are_kept(self):
        loki = object()
        hook = AdmetricaHook(admetrica_conn_id="other", loki=loki, request_delay=1.5, limit=250)
        assert hook.admetrica_conn_id == "other"
        assert hook._loki is loki
        assert hook.request_delay == 1.5
        assert hook.limit == 250


class TestAdvertiserId:
    def test_reads_the_extra(self):
        assert _hook().advertiser_id == 17004

    def test_reads_a_number_written_as_text(self):
        assert _hook(extra={"advertiser_id": "17004"}).advertiser_id == 17004

    def test_missing_advertiser_fails_the_task(self):
        with pytest.raises(AirflowException, match="names no advertiser"):
            _hook(extra={}).advertiser_id

    def test_unusable_advertiser_fails_the_task(self):
        with pytest.raises(AirflowException, match="names no advertiser"):
            _hook(extra={"advertiser_id": "not-a-number"}).advertiser_id

    def test_extra_that_is_not_json_fails_the_task(self):
        with pytest.raises(AirflowException, match="names no advertiser"):
            _hook(extra="{not json").advertiser_id

    def test_message_names_the_connection_and_the_shape(self):
        with pytest.raises(AirflowException) as exc:
            _hook(extra={}).advertiser_id
        assert "'admetrica'" in str(exc.value)
        assert "advertiser_id" in str(exc.value)

    def test_a_connection_airflow_does_not_have_fails_as_itself(self, caplog):
        """A typo in the id and an extra to correct are different mistakes.

        Describing the first as the second sends the reader to edit the extra of
        a connection nobody found.
        """
        hook = AdmetricaHook(admetrica_conn_id="nope")
        hook.get_connection = MagicMock(
            side_effect=AirflowNotFoundException("The conn_id `nope` isn't defined")
        )
        with caplog.at_level(logging.WARNING):
            with pytest.raises(AirflowNotFoundException):
                hook.advertiser_id
        assert "not readable as JSON" not in caplog.text


class TestToken:
    def test_password_is_the_token(self):
        assert _hook()._get_token() == TOKEN

    def test_surrounding_spaces_are_dropped(self):
        assert _hook(password=f"  {TOKEN}  ")._get_token() == TOKEN

    def test_scheme_written_in_front_of_the_token_is_dropped(self):
        assert _hook(password=f"OAuth {TOKEN}")._get_token() == TOKEN

    def test_scheme_is_recognised_whatever_its_case(self):
        assert _hook(password=f"oauth {TOKEN}")._get_token() == TOKEN

    def test_empty_password_fails_the_task(self):
        with pytest.raises(AirflowException, match="no OAuth token"):
            _hook(password="")._get_token()

    def test_password_of_spaces_fails_the_task(self):
        with pytest.raises(AirflowException, match="no OAuth token"):
            _hook(password="   ")._get_token()

    def test_password_holding_only_the_scheme_fails_the_task(self):
        with pytest.raises(AirflowException, match="no OAuth token"):
            _hook(password="OAuth ")._get_token()

    def test_absent_password_fails_the_task(self):
        with pytest.raises(AirflowException, match="no OAuth token"):
            _hook(password=None)._get_token()

    def test_message_names_the_connection_and_the_field(self):
        with pytest.raises(AirflowException) as exc:
            _hook(password="")._get_token()
        assert "'admetrica'" in str(exc.value)
        assert "password" in str(exc.value)


class TestConnectionIsReadOnce:
    def test_token_and_advertiser_share_one_read(self):
        hook = _hook()
        hook._get_token()
        hook.advertiser_id
        hook._get_token()
        assert hook.get_connection.call_count == 1

    def test_the_read_names_the_configured_connection(self):
        hook = _hook()
        hook._get_token()
        hook.get_connection.assert_called_once_with("admetrica")
