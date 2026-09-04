"""Tests for the record a row of a report becomes: its keys, and their order."""

from __future__ import annotations

import json
import logging

import pytest
from airflow.exceptions import AirflowException

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _map_row,
    _normalize_name,
    _record_keys,
)

_HOOK_LOGGER = "airflow_provider_yandex_admetrica.hooks.yandex_admetrica"

ADVERTISER_ID = 17004
CAMPAIGN_ID = 123456
DATE = "2026-08-20"


def _record(raw_row, dimensions, metrics, extra_params=None) -> dict:
    """The record one row becomes for the fixed advertiser, campaign and day.

    The two steps an export takes: the request's names become record keys once,
    and then every row of the answer is laid out under them.
    """
    return _map_row(
        raw_row,
        DATE,
        ADVERTISER_ID,
        CAMPAIGN_ID,
        _record_keys(dimensions, extra_params, "dimension"),
        _record_keys(metrics, extra_params, "metric"),
    )


class TestNormalizeName:
    """The naming rule, which is the same one for groupings and for metrics."""

    @pytest.mark.parametrize(
        ("name", "key"),
        [
            ("am:e:placement", "placement"),
            ("am:e:deviceType", "device_type"),
            ("am:e:operatingSystemRoot", "operating_system_root"),
            ("am:e:interest2d1", "interest2d1"),
            ("am:e:renders", "renders"),
            ("am:e:videoCompletePercent", "video_complete_percent"),
            ("am:e:goal12345Reaches", "goal12345_reaches"),
        ],
    )
    def test_documented_names(self, name, key):
        """Every name the documentation spells out normalises as documented."""
        assert _normalize_name(name) == key

    def test_prefix_is_dropped_only_where_it_is(self):
        """A name that arrives without the prefix keeps all of its own text."""
        assert _normalize_name("deviceType") == "device_type"

    def test_digits_do_not_start_a_word(self):
        """A run of digits inside a name is not a boundary to cut at."""
        assert _normalize_name("am:e:interest2d3") == "interest2d3"

    def test_capital_run_is_a_word_of_its_own(self):
        """A substituted currency stays a word instead of joining the next one."""
        assert (
            _normalize_name("am:e:ecommerce<currency>Revenue", {"currency": "RUB"})
            == "ecommerce_rub_revenue"
        )

    def test_parameter_is_substituted(self):
        """A placeholder and its value spell the same key as the value written in."""
        spelled_out = _normalize_name("am:e:goal12345Reaches")
        parameterised = _normalize_name("am:e:goal<goal_id>Reaches", {"goal_id": 12345})
        assert parameterised == spelled_out == "goal12345_reaches"

    def test_unanswered_parameter_keeps_its_name(self, caplog):
        """A placeholder nothing answers stays visible instead of merging goals."""
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            key = _normalize_name("am:e:goal<goal_id>Reaches", {})

        assert "goal_id" in key
        assert "<" not in key and ">" not in key
        assert key != _normalize_name("am:e:goalReaches")
        assert "goal_id" in caplog.text
        assert "am:e:goal" not in caplog.text

    def test_extra_params_of_another_kind_is_ignored(self):
        """A parameter set that is not a mapping leaves the placeholder unanswered."""
        assert _normalize_name("am:e:goal<goal_id>Reaches", None) == _normalize_name(
            "am:e:goal<goal_id>Reaches", "goal_id=1"
        )

    def test_key_holds_nothing_a_json_path_cannot(self):
        """Whatever a name carries, the key is letters, digits and separators."""
        key = _normalize_name("am:e:ecommerce<currency>Revenue", {"currency": "US $"})
        assert key == "ecommerce_us_revenue"

    @pytest.mark.parametrize(
        ("currency", "key"),
        [("$RUB", "ecommerce_rub_revenue"), ("RUB$", "ecommerce_rub_revenue")],
        ids=["leading", "trailing"],
    )
    def test_a_key_neither_starts_nor_ends_with_a_separator(self, currency, key):
        """A substituted value edged with a non-key character renames no column."""
        assert _normalize_name("am:e:ecommerce<currency>Revenue", {"currency": currency}) == key

    def test_a_name_that_is_all_separators_leaves_no_edges_behind(self):
        assert _normalize_name("am:e:$$$") == ""

    def test_a_value_of_more_digits_than_can_be_written_is_refused_by_name(self):
        """An unwritable answer to a placeholder stops the export in our words."""
        with pytest.raises(ValueError) as refusal:
            _normalize_name("am:e:goal<goal_id>Reaches", {"goal_id": 10**5000})

        message = str(refusal.value)
        assert "more digits than can be written" in message
        assert "goal_id" in message
        assert "int_max_str_digits" not in message

    def test_the_refusal_names_the_parameter_through_the_gate(self):
        """The parameter is the caller's text, so the token in it is masked."""
        with pytest.raises(ValueError) as refusal:
            _normalize_name(
                "am:e:goal<secret-token>Reaches",
                {"secret-token": 10**5000},
                "secret-token",
            )

        assert "secret-token" not in str(refusal.value)
        assert "<token>" in str(refusal.value)


class TestServiceFields:
    """The flat half of the record: the day, the advertiser and the campaign."""

    def test_fields_are_stamped(self):
        """The three service fields come from the arguments, not from the row."""
        record = _record({"dimensions": [], "metrics": []}, [], [])

        assert record["date"] == DATE
        assert record["advertiser_id"] == ADVERTISER_ID
        assert record["campaign_id"] == CAMPAIGN_ID

    def test_key_order_is_fixed(self):
        """Service fields, then the groupings, then the metrics."""
        record = _record(
            {"dimensions": [{"name": "Main"}], "metrics": [1]},
            ["am:e:placement"],
            ["am:e:renders"],
        )

        assert list(record) == [
            "date",
            "advertiser_id",
            "campaign_id",
            "dimensions",
            "metrics",
        ]


class TestDimensions:
    """The grouping half: the values arrive as they are and keep their fields."""

    def test_value_with_id_and_without(self):
        """A grouping keeps whichever of the two fields the answer sent."""
        record = _record(
            {"dimensions": [{"name": "Main page", "id": 55}, {"name": "mobile"}], "metrics": []},
            ["am:e:placement", "am:e:deviceType"],
            [],
        )

        assert record["dimensions"] == {
            "placement": {"name": "Main page", "id": 55},
            "device_type": {"name": "mobile"},
        }

    def test_unknown_fields_are_carried_through(self):
        """A field the provider has never seen is in the record all the same."""
        value = {"name": "Main page", "id": 55, "favicon": "ya.ru", "nested": {"a": 1}}
        record = _record({"dimensions": [value], "metrics": []}, ["am:e:placement"], [])

        assert record["dimensions"]["placement"] == value

    def test_fields_inside_a_value_are_not_renamed(self):
        """The naming rule stops at the key; the value is the API's own wording."""
        record = _record(
            {"dimensions": [{"name": "Main", "iconType": "url", "id": 55}], "metrics": []},
            ["am:e:placement"],
            [],
        )

        assert list(record["dimensions"]["placement"]) == ["name", "iconType", "id"]

    def test_value_without_name(self):
        """A value the answer left a name out of is kept as the answer sent it."""
        record = _record({"dimensions": [{"id": 55}], "metrics": []}, ["am:e:placement"], [])

        assert record["dimensions"] == {"placement": {"id": 55}}

    def test_rows_may_differ_in_their_fields(self):
        """Two rows of one day need not carry the same fields to be written."""
        rows = [
            {"dimensions": [{"name": "Main", "id": 55}], "metrics": [1]},
            {"dimensions": [{"name": "Other"}], "metrics": [2]},
        ]
        records = [_record(row, ["am:e:placement"], ["am:e:renders"]) for row in rows]

        assert records[0]["dimensions"]["placement"] == {"name": "Main", "id": 55}
        assert records[1]["dimensions"]["placement"] == {"name": "Other"}

    def test_value_of_another_kind_is_kept(self):
        """A value that is not an object is carried rather than reshaped."""
        record = _record({"dimensions": ["mobile"], "metrics": []}, ["am:e:deviceType"], [])

        assert record["dimensions"] == {"device_type": "mobile"}

    def test_empty_dimensions(self):
        """A report asked for without groupings has an empty object, not a missing one."""
        record = _record({"dimensions": [], "metrics": [42]}, [], ["am:e:renders"])

        assert record["dimensions"] == {}
        assert record["metrics"] == {"renders": 42}

    def test_a_row_short_of_a_grouping_stops_the_day(self):
        """A row carrying fewer groupings than were asked for is unreadable."""
        with pytest.raises(AirflowException) as failure:
            _record(
                {"dimensions": [{"name": "Main"}], "metrics": []},
                ["am:e:placement", "am:e:deviceType"],
                [],
            )

        message = str(failure.value)
        assert "1 dimension value(s)" in message
        assert "the request asks for 2" in message
        assert str(CAMPAIGN_ID) in message
        assert DATE in message

    def test_a_grouping_the_api_has_no_value_for_is_kept(self):
        """An empty grouping value is an answer: it is what include_undefined asks for."""
        record = _record(
            {"dimensions": [None], "metrics": [1]},
            ["am:e:placement"],
            ["am:e:renders"],
        )

        assert record["dimensions"] == {"placement": None}


class TestMetrics:
    """The metric half: numbers paired with the names they were asked for by."""

    def test_metrics_are_named_by_position(self):
        """The order of the request is what ties a number to its metric."""
        record = _record(
            {"dimensions": [], "metrics": [12345, 67, 0.54]},
            [],
            ["am:e:renders", "am:e:clicks", "am:e:ctr"],
        )

        assert record["metrics"] == {"renders": 12345, "clicks": 67, "ctr": 0.54}

    def test_key_order_follows_the_request(self):
        """Metrics are in the order they were requested in, not a sorted one."""
        record = _record(
            {"dimensions": [], "metrics": [1, 2, 3]},
            [],
            ["am:e:videoCompletePercent", "am:e:renders", "am:e:clicks"],
        )

        assert list(record["metrics"]) == ["video_complete_percent", "renders", "clicks"]

    def test_short_metric_array_stops_the_day(self):
        """A row short of a metric would be written with an empty column."""
        with pytest.raises(AirflowException) as failure:
            _record(
                {"dimensions": [], "metrics": [1]},
                [],
                ["am:e:renders", "am:e:clicks"],
            )

        message = str(failure.value)
        assert "1 metric value(s)" in message
        assert "the request asks for 2" in message

    def test_long_metric_array_stops_the_day(self):
        """A number no metric claims would be dropped from a row that still counts."""
        with pytest.raises(AirflowException) as failure:
            _record({"dimensions": [], "metrics": [1, 2, 3]}, [], ["am:e:renders"])

        message = str(failure.value)
        assert "3 metric value(s)" in message
        assert "the request asks for 1" in message

    def test_a_row_of_empty_numbers_is_an_answer(self):
        """A report of ratios alone answers in empty numbers where no ratio exists."""
        record = _record(
            {"dimensions": [], "metrics": [None, None]},
            [],
            ["am:e:ctr", "am:e:cpm"],
        )

        assert record["metrics"] == {"ctr": None, "cpm": None}
        assert json.loads(json.dumps(record))["metrics"] == {"ctr": None, "cpm": None}

    def test_parameterised_metric(self):
        """A goal named through a request parameter is a column of that goal."""
        record = _record(
            {"dimensions": [], "metrics": [7]},
            [],
            ["am:e:goal<goal_id>Reaches"],
            {"goal_id": 12345},
        )

        assert record["metrics"] == {"goal12345_reaches": 7}

    def test_spelled_out_goal_needs_no_parameters(self):
        """A goal written into the name reaches the same key without parameters."""
        record = _record({"dimensions": [], "metrics": [7]}, [], ["am:e:goal12345Reaches"])

        assert record["metrics"] == {"goal12345_reaches": 7}

    def test_values_are_not_converted(self):
        """A number is written as the answer wrote it, whatever its type."""
        record = _record(
            {"dimensions": [], "metrics": [None, 0, "1.5"]},
            [],
            ["am:e:renders", "am:e:clicks", "am:e:ctr"],
        )

        assert record["metrics"] == {"renders": None, "clicks": 0, "ctr": "1.5"}

    def test_a_row_of_one_number_and_no_more(self):
        """A row that measured something keeps the empty numbers beside it."""
        record = _record(
            {"dimensions": [], "metrics": [None, 7]},
            [],
            ["am:e:renders", "am:e:clicks"],
        )

        assert record["metrics"] == {"renders": None, "clicks": 7}


class TestMalformedRows:
    """Rows that do not carry what a row carries stop the day they belong to."""

    @pytest.mark.parametrize("raw_row", [None, [], "row", {}, {"dimensions": {}, "metrics": None}])
    def test_row_without_lists(self, raw_row):
        """Nothing readable in the row is a row no value can be read out of."""
        with pytest.raises(AirflowException) as failure:
            _record(raw_row, ["am:e:placement"], ["am:e:renders"])

        assert "0 dimension value(s)" in str(failure.value)

    def test_a_row_of_a_report_asking_for_nothing(self):
        """A report of one row asks for no groupings, and the row carries none."""
        record = _record({"dimensions": [], "metrics": [1]}, [], ["am:e:renders"])

        assert record["dimensions"] == {}
        assert record["date"] == DATE

    def test_names_that_collide(self, caplog):
        """Two names writing one key leave the later value and say so."""
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            record = _record(
                {"dimensions": [], "metrics": [1, 2]},
                [],
                ["am:e:deviceType", "device:Type"],
            )

        assert record["metrics"] == {"device_type": 2}
        assert "position 1" in caplog.text
        assert "position 2" in caplog.text
        assert "deviceType" not in caplog.text
        assert "device_type" not in caplog.text


class TestRecordIsWritable:
    """The record is what a JSONL line is made of, so it must serialise."""

    def test_record_serialises_and_keeps_its_order(self):
        """A record survives a round trip through JSON with its key order."""
        record = _record(
            {"dimensions": [{"name": "Главная", "id": 55}], "metrics": [12345]},
            ["am:e:placement"],
            ["am:e:renders"],
        )
        line = json.dumps(record, ensure_ascii=False)

        assert "Главная" in line
        assert list(json.loads(line)) == list(record)
        assert json.loads(line) == record


class TestTwoNamesWantingOneKey:
    """One key is one column, so a request asking twice for it gets it once."""

    def test_both_spellings_of_one_metric_write_one_key(self, caplog):
        raw_row = {"dimensions": [], "metrics": [7, 9]}
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            record = _record(
                raw_row,
                [],
                ["am:e:goal12345Reaches", "am:e:goal<goal_id>Reaches"],
                {"goal_id": 12345},
            )

        assert record["metrics"] == {"goal12345_reaches": 9}
        assert "position 2" in caplog.text

    def test_the_warning_names_the_two_by_position_and_quotes_neither(self, caplog):
        """A requested name is the caller's own text, so the log gives its place."""
        raw_row = {"dimensions": [], "metrics": [7, 9]}
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            _record(raw_row, [], ["am:e:goalReaches", "am:e:goal_reaches"], None)

        assert "position 1" in caplog.text
        assert "position 2" in caplog.text
        assert "already writes" in caplog.text
        assert "goal" not in caplog.text
