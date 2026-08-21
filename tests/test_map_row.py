"""Tests for the record a row of a report becomes: its keys, and their order."""

from __future__ import annotations

import json
import logging

import pytest

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _map_row,
    _normalize_name,
)

_HOOK_LOGGER = "airflow_provider_yandex_admetrica.hooks.yandex_admetrica"

ADVERTISER_ID = 17004
CAMPAIGN_ID = 123456
DATE = "2026-08-20"


def _record(raw_row, dimensions, metrics, extra_params=None) -> dict:
    """The record one row becomes for the fixed advertiser, campaign and day."""
    return _map_row(
        raw_row,
        DATE,
        ADVERTISER_ID,
        CAMPAIGN_ID,
        dimensions,
        metrics,
        extra_params,
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

    def test_extra_params_of_another_kind_is_ignored(self):
        """A parameter set that is not a mapping leaves the placeholder unanswered."""
        assert _normalize_name("am:e:goal<goal_id>Reaches", None) == _normalize_name(
            "am:e:goal<goal_id>Reaches", "goal_id=1"
        )

    def test_key_holds_nothing_a_json_path_cannot(self):
        """Whatever a name carries, the key is letters, digits and separators."""
        key = _normalize_name("am:e:ecommerce<currency>Revenue", {"currency": "US $"})
        assert key == "ecommerce_us_revenue"


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

    def test_missing_value_is_empty(self, caplog):
        """A grouping the answer sent nothing for is present and empty."""
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            record = _record(
                {"dimensions": [{"name": "Main"}], "metrics": []},
                ["am:e:placement", "am:e:deviceType"],
                [],
            )

        assert record["dimensions"] == {"placement": {"name": "Main"}, "device_type": None}
        assert "dimension" in caplog.text


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

    def test_short_metric_array(self, caplog):
        """A metric the answer is short of is empty, and the shortfall is logged."""
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            record = _record(
                {"dimensions": [], "metrics": [1]},
                [],
                ["am:e:renders", "am:e:clicks"],
            )

        assert record["metrics"] == {"renders": 1, "clicks": None}
        assert "metric" in caplog.text

    def test_long_metric_array(self, caplog):
        """A number no metric claims has no key to go under and is logged."""
        with caplog.at_level(logging.WARNING, logger=_HOOK_LOGGER):
            record = _record({"dimensions": [], "metrics": [1, 2, 3]}, [], ["am:e:renders"])

        assert record["metrics"] == {"renders": 1}
        assert "metric" in caplog.text

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


class TestMalformedRows:
    """Rows that do not carry what a row carries still become a record."""

    @pytest.mark.parametrize("raw_row", [None, [], "row", {}, {"dimensions": {}, "metrics": None}])
    def test_row_without_lists(self, raw_row):
        """Nothing readable in the row leaves every requested key empty."""
        record = _record(raw_row, ["am:e:placement"], ["am:e:renders"])

        assert record["dimensions"] == {"placement": None}
        assert record["metrics"] == {"renders": None}
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
        assert "device_type" in caplog.text


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
