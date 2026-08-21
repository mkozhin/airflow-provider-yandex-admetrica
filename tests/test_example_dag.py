"""Tests for the example DAG: it imports, it is wired as described, and the
pure functions behind its tasks build the dates, the snapshot and the keys."""

from __future__ import annotations

import importlib

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import MappedTaskGroup

_MOD_NAME = "examples.admetrica_to_bq_and_s3_dag"

#: Everything that happens to one day, in the order it happens.
_DAY_TASKS = ("day.collect", "day.params", "day.upload_gcs", "day.upload_s3", "day.load_bq")

#: The chain the snapshot of the campaign dictionary travels, once per run.
_DICTIONARY_TASKS = (
    "dictionary.params",
    "dictionary.upload_gcs",
    "dictionary.upload_s3",
    "dictionary.load_bq",
)


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
        expected = {"get_dates", "ensure_gcs_bucket", "cleanup", *_DAY_TASKS, *_DICTIONARY_TASKS}
        assert expected <= set(dag_obj.task_dict)

    def test_single_active_run(self, dag_obj):
        assert dag_obj.max_active_runs == 1


class TestOneDayIsOneMapIndex:
    """A day is a map index of the group, so days never wait for each other."""

    def test_the_group_of_a_day_is_mapped_over_the_dates(self, dag_obj):
        group = dag_obj.task_group_dict["day"]
        assert isinstance(group, MappedTaskGroup)
        assert "day" in group._expand_input.value

    def test_every_task_of_a_day_belongs_to_the_mapped_group(self, dag_obj):
        """The whole day rides one map index: collection, both uploads and the load."""
        for task_id in _DAY_TASKS:
            assert dag_obj.get_task(task_id).task_group.group_id == "day"

    def test_the_day_reaches_bigquery_without_waiting_for_another_day(self, dag_obj):
        """Everything a load of a day waits for is that same day: no reduction between."""
        for task_id in ("day.params", "day.upload_gcs", "day.upload_s3", "day.load_bq"):
            upstream = dag_obj.get_task(task_id).upstream_task_ids
            assert all(t.startswith("day.") for t in upstream)

    def test_the_chain_of_a_day_runs_in_order(self, dag_obj):
        assert dag_obj.get_task("day.params").upstream_task_ids == {"day.collect"}
        assert {"day.upload_gcs", "day.upload_s3"} <= dag_obj.get_task("day.params").downstream_task_ids
        assert "day.upload_gcs" in dag_obj.get_task("day.load_bq").upstream_task_ids

    def test_collect_runs_one_day_at_a_time(self, dag_obj):
        assert dag_obj.get_task("day.collect").max_active_tis_per_dag == 1

    def test_the_bucket_is_ready_before_the_first_day(self, dag_obj):
        assert "ensure_gcs_bucket" in dag_obj.get_task("day.collect").upstream_task_ids


class TestTheDictionaryOfTheRun:
    """The snapshot is one per run and a failed day does not hold it back."""

    def test_the_snapshot_waits_for_the_days_however_they_ended(self, dag_obj):
        assert dag_obj.get_task("dictionary.params").trigger_rule == "all_done"
        assert "day.collect" in dag_obj.get_task("dictionary.params").upstream_task_ids

    def test_the_snapshot_is_loaded_once(self, dag_obj):
        for task_id in _DICTIONARY_TASKS:
            assert dag_obj.get_task(task_id).task_group.group_id == "dictionary"
            assert not dag_obj.get_task(task_id).get_needs_expansion()


class TestTheEndOfTheRun:
    """Cleanup waits for every load and tolerates a day that had nothing to load."""

    def test_cleanup_tolerates_a_skip(self, dag_obj):
        assert dag_obj.get_task("cleanup").trigger_rule == "none_failed"

    def test_cleanup_runs_after_every_load(self, dag_obj):
        upstream = dag_obj.get_task("cleanup").upstream_task_ids
        assert {"day.load_bq", "day.upload_s3", "dictionary.load_bq", "dictionary.upload_s3"} <= upstream


class TestLoadOperators:
    """The uploads replace, and both loads read the schema they are given."""

    def test_uploads_replace_existing_keys(self, dag_obj):
        assert dag_obj.get_task("day.upload_s3").replace is True
        assert dag_obj.get_task("dictionary.upload_s3").replace is True

    def test_bq_loads_declare_schema_and_partition(self, dag_obj, dag_module):
        stats = dag_obj.get_task("day.load_bq")
        campaigns = dag_obj.get_task("dictionary.load_bq")
        assert stats.schema_fields == dag_module.BQ_STATS_SCHEMA
        assert campaigns.schema_fields == dag_module.BQ_DICT_SCHEMA
        assert stats.time_partitioning == {"type": "DAY", "field": "date"}
        assert campaigns.time_partitioning == {"type": "DAY", "field": "snapshot_date"}

    def test_bq_loads_turn_autodetection_off(self, dag_obj):
        """The operator detects the schema by default, and a nested field is what it loses."""
        assert dag_obj.get_task("day.load_bq").autodetect is False
        assert dag_obj.get_task("dictionary.load_bq").autodetect is False

    def test_bq_loads_overwrite_one_partition(self, dag_obj):
        for task_id in ("day.load_bq", "dictionary.load_bq"):
            assert dag_obj.get_task(task_id).write_disposition == "WRITE_TRUNCATE"


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

    @pytest.mark.parametrize("bounds", [("y0_secret", "2026-08-21"), ("2026-08-18", "y0_secret")])
    def test_a_bound_that_is_not_a_day_is_refused_without_being_quoted(self, dag_module, bounds):
        """A run parameter can hold anything, and the refusal reads from a task log."""
        with pytest.raises(ValueError) as failure:
            dag_module.build_dates(*bounds)

        assert "date must be a day" in str(failure.value)
        assert "y0_secret" not in str(failure.value)


class TestFindingRecords:
    """One record of a kind is taken out of a day, and one snapshot out of a run."""

    def test_the_statistics_of_a_day(self, dag_module):
        records = [_record(kind="dict", path="/tmp/d.json"), _record(path="/tmp/20.json")]
        assert dag_module.find_record(records, "stats") == _record(path="/tmp/20.json")

    def test_a_day_without_a_file_of_that_kind(self, dag_module):
        assert dag_module.find_record([_record()], "dict") is None

    def test_a_day_that_wrote_nothing(self, dag_module):
        assert dag_module.find_record([], "stats") is None
        assert dag_module.find_record(None, "stats") is None

    def test_the_snapshot_of_the_run_comes_from_the_first_day_that_has_it(self, dag_module):
        snapshot = _record(kind="dict", date="2026-08-21", path="/tmp/dict.json")
        mapped = [
            [_record(date="2026-08-21", path="/tmp/21.json"), snapshot],
            [_record(date="2026-08-20", path="/tmp/20.json"), dict(snapshot)],
        ]
        assert dag_module.dictionary_record(mapped) == snapshot

    def test_a_day_that_reported_no_snapshot_is_skipped_over(self, dag_module):
        snapshot = _record(kind="dict", date="2026-08-21", path="/tmp/dict.json")
        mapped = [[], [_record(path="/tmp/20.json")], [snapshot]]
        assert dag_module.dictionary_record(mapped) == snapshot

    def test_a_run_without_a_snapshot(self, dag_module):
        assert dag_module.dictionary_record([[_record()], []]) is None
        assert dag_module.dictionary_record([]) is None
        assert dag_module.dictionary_record(None) is None


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

    def test_one_record_gives_every_address_of_its_load(self, dag_module):
        record = _record(date="2026-08-20", path="/tmp/20.json")
        params = dag_module.load_params(record, "run_a", "stats")
        assert params == {
            "src": "/tmp/20.json",
            "gcs_object": dag_module.gcs_object(record, "run_a"),
            "s3_key": dag_module.s3_key(record),
            "bq_table": dag_module.bq_table(record, "stats"),
        }


class TestStagingLifecycleRule:
    """The rule the DAG puts on the bucket addresses its own prefix and nothing else."""

    def test_the_rule_is_scoped_to_the_staging_prefix(self, dag_module):
        condition = dag_module.STAGING_LIFECYCLE_RULE["condition"]
        assert condition["matchesPrefix"] == [f"{dag_module.GCS_PREFIX}/"]
        assert dag_module.STAGING_LIFECYCLE_RULE["action"] == {"type": "Delete"}

    def test_a_bucket_without_the_rule(self, dag_module):
        rules = [{"action": {"type": "Delete"}, "condition": {"age": 30}}]
        assert dag_module.staging_rule_present(rules) is False

    def test_a_delete_rule_of_another_prefix_is_not_this_one(self, dag_module):
        rules = [
            {"action": {"type": "Delete"}, "condition": {"age": 1, "matchesPrefix": ["other/"]}}
        ]
        assert dag_module.staging_rule_present(rules) is False

    def test_the_rule_is_recognised_whatever_else_the_condition_says(self, dag_module):
        rules = [
            {
                "action": {"type": "Delete"},
                "condition": {"age": 7, "matchesPrefix": [f"{dag_module.GCS_PREFIX}/", "x/"]},
            }
        ]
        assert dag_module.staging_rule_present(rules) is True

    def test_the_rule_the_dag_writes_is_recognised_next_run(self, dag_module):
        assert dag_module.staging_rule_present([dag_module.STAGING_LIFECYCLE_RULE]) is True

    def test_a_rule_of_another_action_is_not_this_one(self, dag_module):
        rules = [
            {
                "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
                "condition": {"age": 1, "matchesPrefix": [f"{dag_module.GCS_PREFIX}/"]},
            }
        ]
        assert dag_module.staging_rule_present(rules) is False


class TestTaskCallables:
    """The functions the tasks run, called directly through ``.python_callable``."""

    def test_get_dates_reads_the_period_out_of_the_params(self, dag_obj):
        get_dates = dag_obj.get_task("get_dates").python_callable
        dates = get_dates(params={"date_from": "2026-08-19", "date_to": "2026-08-21"})
        assert dates == ["2026-08-21", "2026-08-20", "2026-08-19"]

    def test_the_day_loads_its_own_file_into_its_own_partition(self, dag_obj, dag_module):
        day_params = dag_obj.get_task("day.params").python_callable
        record = _record(date="2026-08-20", path="/tmp/20.json")
        params = day_params([record, _record(kind="dict", path="/tmp/d.json")], run_id="run_a")
        assert params == dag_module.load_params(record, "run_a", dag_module.BQ_STATS_TABLE)

    def test_a_day_without_rows_skips_its_loads(self, dag_obj):
        """No file was written, so there is nothing for the uploads to carry."""
        day_params = dag_obj.get_task("day.params").python_callable
        with pytest.raises(AirflowSkipException):
            day_params([_record(kind="dict", path="/tmp/d.json")], run_id="run_a")

    def test_the_snapshot_is_loaded_into_the_dictionary_table(self, dag_obj, dag_module):
        dictionary_params = dag_obj.get_task("dictionary.params").python_callable
        snapshot = _record(kind="dict", date="2026-08-21", path="/tmp/d.json")
        params = dictionary_params(
            [[_record(path="/tmp/20.json"), snapshot], [dict(snapshot)]], run_id="run_a"
        )
        assert params == dag_module.load_params(snapshot, "run_a", dag_module.BQ_DICT_TABLE)

    def test_a_run_without_a_snapshot_skips_the_dictionary(self, dag_obj):
        dictionary_params = dag_obj.get_task("dictionary.params").python_callable
        with pytest.raises(AirflowSkipException):
            dictionary_params([[_record()], []], run_id="run_a")


class TestEnsureBucket:
    """The lifecycle rule is added to what the bucket already has, never instead of it."""

    class _Bucket:
        """A bucket of the client: its rules live in metadata that a load brings in.

        Until the metadata is loaded the bucket reports no rules at all, which is
        what `Bucket.lifecycle_rules` does for a bucket addressed by name only.
        """

        def __init__(self, rules, exists=True):
            self._stored = list(rules)
            self._exists = exists
            self._loaded = False
            self.patched = False

        @property
        def lifecycle_rules(self):
            return list(self._stored) if self._loaded else []

        @lifecycle_rules.setter
        def lifecycle_rules(self, rules):
            self._stored = list(rules)

        def exists(self):
            return self._exists

        def load(self):
            self._loaded = True

        def patch(self):
            self.patched = True

    def _run(self, dag_obj, dag_module, monkeypatch, bucket):
        class _Client:
            def bucket(self, name):
                return bucket

            def lookup_bucket(self, name):
                if not bucket.exists():
                    return None
                bucket.load()
                return bucket

            def create_bucket(self, name):
                bucket.load()
                return bucket

        monkeypatch.setattr(dag_module.GCSHook, "__init__", lambda self, **kwargs: None)
        monkeypatch.setattr(dag_module.GCSHook, "get_conn", lambda self: _Client())
        dag_obj.get_task("ensure_gcs_bucket").python_callable()

    def test_the_rules_of_the_bucket_survive(self, dag_obj, dag_module, monkeypatch):
        kept = {"action": {"type": "Delete"}, "condition": {"age": 30}}
        bucket = self._Bucket([kept])
        self._run(dag_obj, dag_module, monkeypatch, bucket)
        assert bucket.lifecycle_rules == [kept, dag_module.STAGING_LIFECYCLE_RULE]
        assert bucket.patched is True

    def test_a_bucket_that_already_carries_the_rule_is_left_alone(
        self, dag_obj, dag_module, monkeypatch
    ):
        bucket = self._Bucket([dag_module.STAGING_LIFECYCLE_RULE])
        self._run(dag_obj, dag_module, monkeypatch, bucket)
        assert bucket.lifecycle_rules == [dag_module.STAGING_LIFECYCLE_RULE]
        assert bucket.patched is False

    def test_a_bucket_that_is_missing_is_created_and_carries_the_rule(
        self, dag_obj, dag_module, monkeypatch
    ):
        bucket = self._Bucket([], exists=False)
        self._run(dag_obj, dag_module, monkeypatch, bucket)
        assert bucket.lifecycle_rules == [dag_module.STAGING_LIFECYCLE_RULE]
        assert bucket.patched is True


class TestCleanup:
    """The task deletes this run's directory and leaves every other one alone."""

    def _run_dir(self, dag_module, tmp_path, run_id: str):
        run_dir = tmp_path / dag_module.safe_id(run_id)
        (run_dir / "17004" / "stats").mkdir(parents=True)
        (run_dir / "17004" / "stats" / "2026-08-20.json").write_text("{}", encoding="utf-8")
        return run_dir

    def test_deletes_this_run_and_leaves_a_sibling_run_alone(
        self, dag_obj, dag_module, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(dag_module, "BASE_DIR", str(tmp_path))
        mine = self._run_dir(dag_module, tmp_path, "manual__2026-08-21T00:00:00+00:00")
        other = self._run_dir(dag_module, tmp_path, "manual__2026-08-22T00:00:00+00:00")

        cleanup = dag_obj.get_task("cleanup").python_callable
        cleanup(run_id="manual__2026-08-21T00:00:00+00:00")

        assert not mine.exists()
        assert (other / "17004" / "stats" / "2026-08-20.json").is_file()

    def test_a_run_that_wrote_nothing_is_no_failure(self, dag_obj, dag_module, tmp_path, monkeypatch):
        monkeypatch.setattr(dag_module, "BASE_DIR", str(tmp_path))
        cleanup = dag_obj.get_task("cleanup").python_callable
        cleanup(run_id="manual__2026-08-21T00:00:00+00:00")
