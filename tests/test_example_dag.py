"""Tests for the example DAG: it imports, it is wired as described, and the
pure functions behind its tasks build the dates, the flat list and the keys."""

from __future__ import annotations

import importlib

import pytest
from airflow.models.mappedoperator import MappedOperator

_MOD_NAME = "examples.admetrica_to_bq_and_s3_dag"


@pytest.fixture(scope="module")
def dag_module():
    """Import the example DAG module once for the whole file."""
    return importlib.import_module(_MOD_NAME)


@pytest.fixture(scope="module")
def dag_obj(dag_module):
    """Return the DAG the decorated factory builds."""
    factory = dag_module.admetrica_to_bq_and_s3
    return factory.dag if hasattr(factory, "dag") else factory()


def _record(kind="stats", date="2026-08-20", path="/tmp/f.json", advertiser_id=17004):
    return {"kind": kind, "date": date, "path": path, "advertiser_id": advertiser_id}


class TestImport:
    """The DAG file is importable and defines the expected tasks."""

    def test_module_imports(self, dag_module):
        assert dag_module.admetrica_to_bq_and_s3 is not None

    def test_dag_id(self, dag_obj):
        assert dag_obj.dag_id == "admetrica_to_bq_and_s3"

    def test_expected_tasks_present(self, dag_obj):
        expected = {
            "get_dates",
            "ensure_gcs_bucket",
            "collect",
            "flatten",
            "make_gcs_params",
            "make_bq_params_stats",
            "make_bq_params_dict",
            "make_s3_params",
            "upload_gcs",
            "load_bq_stats",
            "load_bq_dict",
            "upload_s3",
            "cleanup",
        }
        assert expected <= set(dag_obj.task_dict)

    def test_collect_is_mapped_over_dates(self, dag_obj):
        collect = dag_obj.get_task("collect")
        assert isinstance(collect, MappedOperator)
        assert "date" in collect.expand_input.value

    def test_collect_runs_one_day_at_a_time(self, dag_obj):
        assert dag_obj.get_task("collect").max_active_tis_per_dag == 1

    def test_single_active_run(self, dag_obj):
        assert dag_obj.max_active_runs == 1

    def test_cleanup_waits_for_success(self, dag_obj):
        assert dag_obj.get_task("cleanup").trigger_rule == "all_success"

    def test_cleanup_runs_after_every_load(self, dag_obj):
        upstream = dag_obj.get_task("cleanup").upstream_task_ids
        assert {"load_bq_stats", "load_bq_dict", "upload_s3"} <= upstream

    def test_uploads_replace_existing_keys(self, dag_obj):
        assert dag_obj.get_task("upload_s3").partial_kwargs["replace"] is True

    def test_bq_loads_declare_schema_and_partition(self, dag_obj, dag_module):
        stats = dag_obj.get_task("load_bq_stats").partial_kwargs
        campaigns = dag_obj.get_task("load_bq_dict").partial_kwargs
        assert stats["schema_fields"] == dag_module.BQ_STATS_SCHEMA
        assert campaigns["schema_fields"] == dag_module.BQ_DICT_SCHEMA
        assert stats["time_partitioning"] == {"type": "DAY", "field": "date"}
        assert campaigns["time_partitioning"] == {"type": "DAY", "field": "snapshot_date"}
        assert "autodetect" not in stats
        assert "autodetect" not in campaigns


class TestSchemas:
    """The declared schemas match the records the operator writes."""

    def test_stats_schema_fields(self, dag_module):
        by_name = {f["name"]: f["type"] for f in dag_module.BQ_STATS_SCHEMA}
        assert by_name == {
            "date": "DATE",
            "advertiser_id": "INTEGER",
            "campaign_id": "INTEGER",
            "dimensions": "JSON",
            "metrics": "JSON",
        }

    def test_dict_schema_fields(self, dag_module):
        by_name = {f["name"]: f["type"] for f in dag_module.BQ_DICT_SCHEMA}
        assert by_name == {
            "snapshot_date": "DATE",
            "campaign_id": "INTEGER",
            "name": "STRING",
            "status": "STRING",
            "date_start": "STRING",
            "date_end": "STRING",
            "advertiser_id": "INTEGER",
            "advertiser_name": "STRING",
        }

    def test_tables_are_separate(self, dag_module):
        assert dag_module.BQ_STATS_TABLE != dag_module.BQ_DICT_TABLE


class TestBuildDates:
    """The period expands into map indices from the freshest day backwards."""

    def test_descending_order(self, dag_module):
        assert dag_module.build_dates("2026-08-18", "2026-08-21") == [
            "2026-08-21",
            "2026-08-20",
            "2026-08-19",
            "2026-08-18",
        ]

    def test_both_bounds_included(self, dag_module):
        dates = dag_module.build_dates("2026-08-18", "2026-08-21")
        assert dates[0] == "2026-08-21"
        assert dates[-1] == "2026-08-18"

    def test_single_day(self, dag_module):
        assert dag_module.build_dates("2026-08-20", "2026-08-20") == ["2026-08-20"]

    def test_crosses_month_boundary(self, dag_module):
        assert dag_module.build_dates("2026-07-31", "2026-08-01") == [
            "2026-08-01",
            "2026-07-31",
        ]

    def test_reversed_period_rejected(self, dag_module):
        with pytest.raises(ValueError):
            dag_module.build_dates("2026-08-21", "2026-08-18")


class TestFlatten:
    """The result of the mapped task is one list of records."""

    def test_several_map_indexes(self, dag_module):
        mapped = [
            [_record(date="2026-08-21", path="/tmp/21.json")],
            [_record(date="2026-08-20", path="/tmp/20.json")],
        ]
        assert dag_module.flatten_results(mapped) == [
            _record(date="2026-08-21", path="/tmp/21.json"),
            _record(date="2026-08-20", path="/tmp/20.json"),
        ]

    def test_day_without_rows_contributes_nothing(self, dag_module):
        mapped = [[], [_record(path="/tmp/20.json")]]
        assert dag_module.flatten_results(mapped) == [_record(path="/tmp/20.json")]

    def test_empty_result(self, dag_module):
        assert dag_module.flatten_results([]) == []


class TestDedupe:
    """One dictionary snapshot per run reaches the load, not one per day."""

    def test_dictionary_of_every_day_collapses_to_one(self, dag_module):
        snapshot = _record(kind="dict", date="2026-08-21", path="/tmp/dict.json")
        records = [
            _record(date="2026-08-21", path="/tmp/21.json"),
            snapshot,
            _record(date="2026-08-20", path="/tmp/20.json"),
            dict(snapshot),
            _record(date="2026-08-19", path="/tmp/19.json"),
            dict(snapshot),
        ]
        unique = dag_module.dedupe_records(records)
        assert unique.count(snapshot) == 1
        assert len(unique) == 4

    def test_order_of_first_appearance_kept(self, dag_module):
        records = [
            _record(path="/tmp/21.json"),
            _record(path="/tmp/20.json"),
            _record(path="/tmp/21.json"),
        ]
        paths = [r["path"] for r in dag_module.dedupe_records(records)]
        assert paths == ["/tmp/21.json", "/tmp/20.json"]

    def test_statistics_of_different_days_survive(self, dag_module):
        records = [_record(date="2026-08-21", path="/tmp/21.json"),
                   _record(date="2026-08-20", path="/tmp/20.json")]
        assert dag_module.dedupe_records(records) == records


class TestKeys:
    """Keys are built from the record, so the advertiser of the connection owns them."""

    def test_stats_key(self, dag_module):
        key = dag_module.s3_key(_record(kind="stats", date="2026-08-20"))
        assert key == (
            f"{dag_module.S3_PREFIX}/17004/stats"
            "/_year=2026/_month=08/_day=20/_date=20260820/2026-08-20.json"
        )

    def test_dictionary_key(self, dag_module):
        key = dag_module.s3_key(_record(kind="dict", date="2026-08-21"))
        assert key == (
            f"{dag_module.S3_PREFIX}/17004/dict/campaigns"
            "/_year=2026/_month=08/_day=21/_date=20260821/2026-08-21.json"
        )

    def test_advertiser_comes_from_the_record(self, dag_module):
        key = dag_module.s3_key(_record(advertiser_id=99))
        assert f"{dag_module.S3_PREFIX}/99/stats" in key

    def test_run_id_absent_from_the_s3_key(self, dag_module):
        assert "run" not in dag_module.s3_key(_record())

    def test_gcs_object_isolates_the_run(self, dag_module):
        obj = dag_module.gcs_object(_record(), "manual__2026-08-21T00:00:00+00:00")
        assert obj == (
            f"{dag_module.GCS_PREFIX}/manual__2026-08-21T00_00_00_00_00"
            "/17004/stats/2026-08-20.json"
        )

    def test_partition_decorator_addresses_the_day(self, dag_module):
        table = dag_module.bq_table(_record(date="2026-08-20"), "stats")
        assert table.endswith(".stats$20260820")

    def test_dictionary_partition_uses_the_snapshot_day(self, dag_module):
        table = dag_module.bq_table(_record(kind="dict", date="2026-08-21"), "campaigns")
        assert table.endswith(".campaigns$20260821")
