"""Tests for the example DAG: it imports, it is wired as described, and the
pure functions behind its tasks build the dates, the snapshot and the keys."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowSkipException, AirflowTaskTimeout
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import MappedTaskGroup

from airflow_provider_yandex_admetrica.operators.stats import ExportRecord

_MOD_NAME = "examples.admetrica_to_bq_and_s3_dag"

#: Everything that happens to one day: the collection and the two directions it feeds.
_DAY_TASKS = ("day.collect", "day.upload_s3", "day.load_bq")

#: The chain the snapshot of the campaign dictionary travels, once per run.
_DICTIONARY_TASKS = (
    "dictionary.params",
    "dictionary.upload_gcs",
    "dictionary.upload_s3",
    "dictionary.create_table",
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


def _record(
    kind="stats", date="2026-08-20", path="/tmp/f.json", advertiser_id=17004, campaign_id=1234
):
    return {
        "kind": kind,
        "date": date,
        "path": path,
        "advertiser_id": advertiser_id,
        "campaign_id": None if kind == "dict" else campaign_id,
    }


def _day_of_two_campaigns(date="2026-08-20"):
    """Return the statistics records of a day that two campaigns had rows for."""
    return [
        _record(date=date, path="/tmp/1234.json", campaign_id=1234),
        _record(date=date, path="/tmp/5678.json", campaign_id=5678),
    ]


class _Job:
    """The job a submission hands back: a wait that breaks on *error*.

    A live worker breaks the wait with a SIGTERM or an ``execution_timeout``,
    which reach the task as exceptions off ``BaseException`` rather than off
    ``Exception``, and *error* is how a test asks for the same thing.
    """

    def __init__(self, error: BaseException | None = None):
        self.error = error

    def result(self):
        if self.error is not None:
            raise self.error


def _submissions(error: BaseException | None = None):
    """Return a factory of jobs whose wait breaks the way the test asked for."""
    return lambda _kwargs: _Job(error)



@pytest.fixture
def fake_cloud(dag_module, monkeypatch, recording_hook):
    """Replace the hooks the module holds and hand back the record of their calls.

    The stand-ins are held to the signatures of the real hooks, so a keyword the
    provider would refuse fails the test rather than a live run.
    """
    s3 = recording_hook(S3Hook, "aws_conn_id", calls="load_file")
    gcs = recording_hook(GCSHook, "gcp_conn_id", calls="upload")
    bigquery = recording_hook(
        BigQueryHook,
        "gcp_conn_id",
        calls="insert_job",
        tables="create_table",
        cancels="cancel_job",
    )
    bigquery.returns["insert_job"] = _submissions()
    monkeypatch.setattr(dag_module, "S3Hook", s3)
    monkeypatch.setattr(dag_module, "GCSHook", gcs)
    monkeypatch.setattr(dag_module, "BigQueryHook", bigquery)
    return SimpleNamespace(s3=s3, gcs=gcs, bigquery=bigquery)


def _load_configs(fake_cloud):
    """Return the ``load`` section of every job the day handed to BigQuery."""
    return [call["configuration"]["load"] for call in fake_cloud.bigquery.calls]


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
        """The whole day rides one map index: the collection and both directions."""
        for task_id in _DAY_TASKS:
            assert dag_obj.get_task(task_id).task_group.group_id == "day"

    def test_the_day_reaches_both_destinations_without_waiting_for_another_day(self, dag_obj):
        """Everything a load of a day waits for is that same day: no reduction between."""
        for task_id in ("day.upload_s3", "day.load_bq"):
            upstream = dag_obj.get_task(task_id).upstream_task_ids
            assert all(t.startswith("day.") for t in upstream)

    def test_the_chain_of_a_day_runs_in_order(self, dag_obj):
        for task_id in ("day.upload_s3", "day.load_bq"):
            assert dag_obj.get_task(task_id).upstream_task_ids == {"day.collect"}
            assert task_id in dag_obj.get_task("day.collect").downstream_task_ids

    def test_the_group_of_a_day_holds_exactly_its_chain(self, dag_obj):
        """A day is the collection and the two directions — a task more is a task
        the map index pays for on every day of the period."""
        assert set(dag_obj.task_group_dict["day"].children) == set(_DAY_TASKS)

    def test_collect_runs_one_day_at_a_time(self, dag_obj):
        assert dag_obj.get_task("day.collect").max_active_tis_per_dag == 1

    def test_the_bucket_is_ready_before_the_first_day(self, dag_obj):
        assert "ensure_gcs_bucket" in dag_obj.get_task("day.collect").upstream_task_ids


class TestTheTwoDirectionsAreIndependent:
    """S3 and BigQuery are two tasks of one day, and neither waits for the other."""

    def test_neither_direction_is_upstream_of_the_other(self, dag_obj):
        """A failure of one destination must leave the other free to reach its mart."""
        s3 = dag_obj.get_task("day.upload_s3")
        bq = dag_obj.get_task("day.load_bq")
        assert "day.load_bq" not in s3.upstream_task_ids | s3.downstream_task_ids
        assert "day.upload_s3" not in bq.upstream_task_ids | bq.downstream_task_ids

    def test_both_directions_hang_off_the_collection_of_the_day(self, dag_obj):
        downstream = dag_obj.get_task("day.collect").downstream_task_ids
        assert {"day.upload_s3", "day.load_bq"} <= downstream

    def test_each_direction_reads_the_files_of_the_day_itself(self, dag_obj, fake_cloud):
        """No shared params task between them: each takes the records of the day."""
        records = _day_of_two_campaigns()
        dag_obj.get_task("day.upload_s3").python_callable(records)
        dag_obj.get_task("day.load_bq").python_callable(records, run_id="run_a")

        assert len(fake_cloud.s3.calls) == 2
        assert len(fake_cloud.gcs.calls) == 2

    def test_one_direction_carries_a_day_the_other_never_touched(self, dag_obj, fake_cloud):
        """The S3 upload of a day stands on its own: nothing of BigQuery ran before it."""
        dag_obj.get_task("day.upload_s3").python_callable(_day_of_two_campaigns())

        assert len(fake_cloud.s3.calls) == 2
        assert fake_cloud.gcs.calls == []
        assert fake_cloud.bigquery.calls == []

    def test_the_other_direction_carries_a_day_s3_never_touched(self, dag_obj, fake_cloud):
        """And the BigQuery load of a day stands on its own the same way."""
        dag_obj.get_task("day.load_bq").python_callable(_day_of_two_campaigns(), run_id="run_a")

        assert len(fake_cloud.gcs.calls) == 2
        assert len(fake_cloud.bigquery.calls) == 2
        assert fake_cloud.s3.calls == []


class TestTheDictionaryOfTheRun:
    """The snapshot is one per run and a failed day does not hold it back."""

    def test_the_snapshot_waits_for_the_days_however_they_ended(self, dag_obj):
        assert dag_obj.get_task("dictionary.params").trigger_rule == "all_done"
        assert "day.collect" in dag_obj.get_task("dictionary.params").upstream_task_ids

    def test_the_snapshot_is_loaded_once(self, dag_obj):
        for task_id in _DICTIONARY_TASKS:
            assert dag_obj.get_task(task_id).task_group.group_id == "dictionary"
            assert not dag_obj.get_task(task_id).get_needs_expansion()

    def test_the_snapshot_travels_both_directions_of_its_group(self, dag_obj):
        params = dag_obj.get_task("dictionary.params")
        assert {"dictionary.upload_gcs", "dictionary.upload_s3"} <= params.downstream_task_ids
        assert "dictionary.upload_gcs" in dag_obj.get_task("dictionary.load_bq").upstream_task_ids


class TestTheEndOfTheRun:
    """Cleanup waits for every load and tolerates a day that had nothing to load."""

    def test_cleanup_tolerates_a_skip(self, dag_obj):
        assert dag_obj.get_task("cleanup").trigger_rule == "none_failed"

    def test_cleanup_runs_after_every_load(self, dag_obj):
        upstream = dag_obj.get_task("cleanup").upstream_task_ids
        assert {
            "day.load_bq",
            "day.upload_s3",
            "dictionary.load_bq",
            "dictionary.upload_s3",
        } <= upstream


class TestLoadOperators:
    """The dictionary rides declarative loads: one record per run, one table each."""

    def test_the_snapshot_replaces_the_key_it_lands_on(self, dag_obj):
        assert dag_obj.get_task("dictionary.upload_s3").replace is True

    def test_the_dictionary_load_declares_schema_and_partition(self, dag_obj, dag_module):
        campaigns = dag_obj.get_task("dictionary.load_bq")
        assert campaigns.schema_fields == dag_module.BQ_DICT_SCHEMA
        assert campaigns.time_partitioning == {"type": "DAY", "field": "snapshot_date"}

    def test_bq_loads_turn_autodetection_off(self, dag_obj):
        """The operator detects the schema by default, and a nested field is what it loses."""
        assert dag_obj.get_task("dictionary.load_bq").autodetect is False

    def test_the_dictionary_load_overwrites_one_partition(self, dag_obj):
        assert dag_obj.get_task("dictionary.load_bq").write_disposition == "WRITE_TRUNCATE"

    def test_the_dictionary_load_names_the_region_of_the_dataset(self, dag_obj, dag_module):
        assert dag_obj.get_task("dictionary.load_bq").location == dag_module.BQ_LOCATION

    def test_the_table_of_the_dictionary_is_created_before_its_partition_is_loaded(
        self, dag_obj, dag_module
    ):
        """A partition decorator addresses a partition of a table that exists, so a
        run against a dataset without `campaigns` would fail without this step."""
        create = dag_obj.get_task("dictionary.create_table")
        assert create.project_id == dag_module.BQ_PROJECT
        assert create.dataset_id == dag_module.BQ_DATASET
        assert create.table_id == dag_module.BQ_DICT_TABLE
        assert create.table_resource == {
            "schema": {"fields": dag_module.BQ_DICT_SCHEMA},
            "timePartitioning": {"type": "DAY", "field": "snapshot_date"},
        }
        assert create.location == dag_module.BQ_LOCATION
        assert "dictionary.create_table" in dag_obj.get_task("dictionary.load_bq").upstream_task_ids

    def test_the_dictionary_table_is_created_without_the_partition_decorator(self, dag_obj):
        """A decorator names a partition, and a table is named without one."""
        assert "$" not in dag_obj.get_task("dictionary.create_table").table_id

    def test_a_dictionary_table_that_is_already_there_is_left_alone(self, dag_obj):
        """Every run after the first meets the table the run before it created."""
        assert dag_obj.get_task("dictionary.create_table").if_exists.value == "ignore"

    def test_the_dictionary_table_is_created_only_for_a_run_that_has_a_snapshot(self, dag_obj):
        """`params` skips a run without a snapshot, and the create step skips with it."""
        create = dag_obj.get_task("dictionary.create_table")
        assert create.upstream_task_ids == {"dictionary.params"}


class TestTheDayUploadsEveryCampaign:
    """A day reaches S3 whole: as many files as campaigns that had rows."""

    def _upload(self, dag_obj):
        return dag_obj.get_task("day.upload_s3").python_callable

    def test_one_upload_per_statistics_record(self, dag_obj, fake_cloud):
        """Uploading only the first record would lose every other campaign in silence."""
        self._upload(dag_obj)([*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")])

        assert len(fake_cloud.s3.calls) == 2

    def test_each_campaign_travels_from_its_own_file_to_its_own_key(
        self, dag_obj, dag_module, fake_cloud
    ):
        records = _day_of_two_campaigns()
        self._upload(dag_obj)(records)

        calls = fake_cloud.s3.calls
        assert [call["filename"] for call in calls] == ["/tmp/1234.json", "/tmp/5678.json"]
        assert [call["key"] for call in calls] == [dag_module.s3_key(r) for r in records]

    def test_the_keys_of_one_day_differ_by_campaign(self, dag_obj, fake_cloud):
        self._upload(dag_obj)(_day_of_two_campaigns())

        keys = [call["key"] for call in fake_cloud.s3.calls]
        assert keys[0] != keys[1]
        assert "_campaign_id=1234" in keys[0]
        assert "_campaign_id=5678" in keys[1]

    def test_every_file_of_a_day_replaces_the_key_it_lands_on(self, dag_obj, fake_cloud):
        self._upload(dag_obj)(_day_of_two_campaigns())

        assert [call["replace"] for call in fake_cloud.s3.calls] == [True, True]

    def test_the_uploads_of_a_day_go_through_the_configured_connection(
        self, dag_obj, dag_module, fake_cloud
    ):
        self._upload(dag_obj)(_day_of_two_campaigns())

        assert fake_cloud.s3.conn_ids == [dag_module.S3_CONN_ID]
        assert {call["bucket_name"] for call in fake_cloud.s3.calls} == {dag_module.S3_BUCKET}

    def test_the_snapshot_of_the_dictionary_is_not_uploaded_by_the_day(self, dag_obj, fake_cloud):
        """The dictionary is one per run and rides its own group."""
        self._upload(dag_obj)([*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")])

        assert "/tmp/d.json" not in [call["filename"] for call in fake_cloud.s3.calls]

    def test_a_day_without_statistics_uploads_nothing_and_skips(self, dag_obj, fake_cloud):
        with pytest.raises(AirflowSkipException):
            self._upload(dag_obj)([_record(kind="dict", path="/tmp/d.json")])

        assert fake_cloud.s3.calls == []

    def test_a_day_that_wrote_nothing_at_all_skips(self, dag_obj, fake_cloud):
        with pytest.raises(AirflowSkipException):
            self._upload(dag_obj)([])

        assert fake_cloud.s3.calls == []

    def test_a_refused_upload_fails_the_day(self, dag_obj, fake_cloud):
        """A swallowed failure would ship a day quietly missing campaigns."""
        fake_cloud.s3.fail_at["load_file"] = 2

        with pytest.raises(RuntimeError):
            self._upload(dag_obj)(_day_of_two_campaigns())

        assert len(fake_cloud.s3.calls) == 2


class TestTheDayLoadsEveryCampaign:
    """A day reaches BigQuery whole: as many jobs as campaigns that had rows."""

    def _load(self, dag_obj):
        return dag_obj.get_task("day.load_bq").python_callable

    def test_one_upload_and_one_job_per_statistics_record(self, dag_obj, fake_cloud):
        """Loading only the first record would lose every other campaign in silence."""
        records = [*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")]
        self._load(dag_obj)(records, run_id="run_a")

        assert len(fake_cloud.gcs.calls) == 2
        assert len(fake_cloud.bigquery.calls) == 2

    def test_each_campaign_travels_from_its_own_file_to_its_own_object(
        self, dag_obj, dag_module, fake_cloud
    ):
        records = _day_of_two_campaigns()
        self._load(dag_obj)(records, run_id="run_a")

        uploads = fake_cloud.gcs.calls
        assert [call["filename"] for call in uploads] == ["/tmp/1234.json", "/tmp/5678.json"]
        assert [call["object_name"] for call in uploads] == [
            dag_module.gcs_object(record, "run_a") for record in records
        ]

    def test_the_objects_of_one_day_differ_by_campaign(self, dag_obj, fake_cloud):
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        objects = [call["object_name"] for call in fake_cloud.gcs.calls]
        assert objects[0] != objects[1]
        assert objects[0].endswith("/2026-08-20/1234.json")
        assert objects[1].endswith("/2026-08-20/5678.json")

    def test_each_campaign_lands_in_its_own_table(self, dag_obj, dag_module, fake_cloud):
        records = _day_of_two_campaigns()
        self._load(dag_obj)(records, run_id="run_a")

        tables = [config["destinationTable"]["tableId"] for config in _load_configs(fake_cloud)]
        assert tables == [dag_module.stats_table_partition(record) for record in records]
        assert tables == ["stats_17004_1234$20260820", "stats_17004_5678$20260820"]

    def test_every_job_reads_the_object_that_was_just_uploaded(
        self, dag_obj, dag_module, fake_cloud
    ):
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        objects = [call["object_name"] for call in fake_cloud.gcs.calls]
        assert [config["sourceUris"] for config in _load_configs(fake_cloud)] == [
            [f"gs://{dag_module.GCS_BUCKET}/{name}"] for name in objects
        ]

    def test_the_job_names_the_project_and_the_dataset_beside_the_table(
        self, dag_obj, dag_module, fake_cloud
    ):
        """`insert_job` takes the three apart, so the table id carries no qualification."""
        self._load(dag_obj)([_record()], run_id="run_a")

        destination = _load_configs(fake_cloud)[0]["destinationTable"]
        assert destination == {
            "projectId": dag_module.BQ_PROJECT,
            "datasetId": dag_module.BQ_DATASET,
            "tableId": "stats_17004_1234$20260820",
        }

    def test_the_job_declares_schema_partition_and_write_disposition(
        self, dag_obj, dag_module, fake_cloud
    ):
        self._load(dag_obj)([_record()], run_id="run_a")

        config = _load_configs(fake_cloud)[0]
        assert config["schema"] == {"fields": dag_module.BQ_STATS_SCHEMA}
        assert config["autodetect"] is False
        assert config["sourceFormat"] == "NEWLINE_DELIMITED_JSON"
        assert config["writeDisposition"] == "WRITE_TRUNCATE"
        assert config["timePartitioning"] == {"type": "DAY", "field": "date"}

    def test_the_table_of_every_campaign_is_created_before_its_partition_is_loaded(
        self, dag_obj, dag_module, fake_cloud
    ):
        """A partition decorator addresses a partition of a table that exists,
        so the first day of a campaign would fail without this step."""
        records = _day_of_two_campaigns()
        self._load(dag_obj)(records, run_id="run_a")

        created = fake_cloud.bigquery.tables
        assert [call["table_id"] for call in created] == [
            dag_module.stats_table(record) for record in records
        ]
        assert [call["table_id"] for call in created] == ["stats_17004_1234", "stats_17004_5678"]
        assert all(call["exists_ok"] is True for call in created)
        assert all(call["schema_fields"] == dag_module.BQ_STATS_SCHEMA for call in created)
        assert all(
            call["table_resource"]["timePartitioning"] == {"type": "DAY", "field": "date"}
            for call in created
        )
        assert all(call["project_id"] == dag_module.BQ_PROJECT for call in created)
        assert all(call["dataset_id"] == dag_module.BQ_DATASET for call in created)

    def test_the_table_is_created_without_the_partition_decorator(self, dag_obj, fake_cloud):
        """A decorator names a partition, and a table is named without one."""
        self._load(dag_obj)([_record()], run_id="run_a")

        assert "$" not in fake_cloud.bigquery.tables[0]["table_id"]

    def test_the_load_and_the_table_name_the_region_of_the_dataset(
        self, dag_obj, dag_module, fake_cloud
    ):
        """A dataset outside the default multi-region is reached by naming it."""
        self._load(dag_obj)([_record()], run_id="run_a")

        assert fake_cloud.bigquery.calls[0]["location"] == dag_module.BQ_LOCATION
        assert fake_cloud.bigquery.tables[0]["location"] == dag_module.BQ_LOCATION

    def test_a_refused_load_fails_the_day(self, dag_obj, fake_cloud):
        """A swallowed failure would ship a day quietly missing campaigns."""
        fake_cloud.bigquery.fail_at["insert_job"] = 2

        with pytest.raises(RuntimeError):
            self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert len(fake_cloud.bigquery.calls) == 2

    def test_every_job_carries_a_name_the_task_gave_it(self, dag_obj, fake_cloud):
        """A name of the task's own is what a cancel has to name the job by."""
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert all(call["job_id"] for call in fake_cloud.bigquery.calls)

    def test_the_campaigns_of_a_day_are_named_apart(self, dag_obj, fake_cloud):
        """One name for two jobs is a 409 on the second."""
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert len({call["job_id"] for call in fake_cloud.bigquery.calls}) == 2

    def test_a_second_attempt_names_its_jobs_apart_from_the_first(self, dag_obj, fake_cloud):
        """A name carried over from the attempt before meets its job as a 409."""
        self._load(dag_obj)([_record()], run_id="run_a")
        self._load(dag_obj)([_record()], run_id="run_a")

        first, second = fake_cloud.bigquery.calls
        assert first["job_id"] != second["job_id"]

    def test_the_loads_of_a_day_go_through_the_configured_connection(
        self, dag_obj, dag_module, fake_cloud
    ):
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert fake_cloud.gcs.conn_ids == [dag_module.GCP_CONN_ID]
        assert fake_cloud.bigquery.conn_ids == [dag_module.GCP_CONN_ID]
        assert {call["bucket_name"] for call in fake_cloud.gcs.calls} == {dag_module.GCS_BUCKET}

    def test_the_snapshot_of_the_dictionary_is_not_loaded_by_the_day(self, dag_obj, fake_cloud):
        """The dictionary is one per run and rides its own group."""
        records = [*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")]
        self._load(dag_obj)(records, run_id="run_a")

        assert "/tmp/d.json" not in [call["filename"] for call in fake_cloud.gcs.calls]

    def test_a_day_without_statistics_loads_nothing_and_skips(self, dag_obj, fake_cloud):
        with pytest.raises(AirflowSkipException):
            self._load(dag_obj)([_record(kind="dict", path="/tmp/d.json")], run_id="run_a")

        assert fake_cloud.gcs.calls == []
        assert fake_cloud.bigquery.calls == []
        assert fake_cloud.bigquery.tables == []

    def test_a_day_that_wrote_nothing_at_all_skips(self, dag_obj, fake_cloud):
        with pytest.raises(AirflowSkipException):
            self._load(dag_obj)([], run_id="run_a")

        assert fake_cloud.gcs.calls == []
        assert fake_cloud.bigquery.calls == []
        assert fake_cloud.bigquery.tables == []


class TestTheDayOwnsTheJobsItSubmits:
    """A load job outlives the task that submitted it, so the task cancels the one
    it is waiting on when the wait breaks: left running, it would race the
    ``WRITE_TRUNCATE`` of the next attempt for the same partition, and whichever
    finished last would decide what the partition holds. The task names the job
    before submitting it, so the cancel has a name to use even where the break
    caught the submission itself."""

    def _load(self, dag_obj):
        return dag_obj.get_task("day.load_bq").python_callable

    def test_the_day_waits_on_the_job_itself(self, dag_obj, fake_cloud):
        """A wait inside the task is what a termination can break into."""
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert [call["nowait"] for call in fake_cloud.bigquery.calls] == [True, True]

    def test_a_broken_wait_cancels_the_job_it_was_waiting_on(
        self, dag_obj, dag_module, fake_cloud
    ):
        fake_cloud.bigquery.returns["insert_job"] = _submissions(AirflowTaskTimeout("timed out"))

        with pytest.raises(AirflowTaskTimeout):
            self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert fake_cloud.bigquery.cancels == [
            {
                "job_id": fake_cloud.bigquery.calls[0]["job_id"],
                "location": dag_module.BQ_LOCATION,
            }
        ]

    def test_a_broken_submission_cancels_the_job_by_the_name_it_was_given(
        self, dag_obj, dag_module, fake_cloud
    ):
        """BigQuery takes a job before the answer naming it comes back, so a
        submission broken in flight can have left one running all the same."""
        fake_cloud.bigquery.fail_at["insert_job"] = 1

        with pytest.raises(RuntimeError):
            self._load(dag_obj)([_record()], run_id="run_a")

        assert fake_cloud.bigquery.cancels == [
            {
                "job_id": fake_cloud.bigquery.calls[0]["job_id"],
                "location": dag_module.BQ_LOCATION,
            }
        ]

    def test_a_broken_wait_stops_the_day_where_it_stands(self, dag_obj, fake_cloud):
        """The campaigns behind the broken wait are the retry's to load."""
        fake_cloud.bigquery.returns["insert_job"] = _submissions(AirflowTaskTimeout("timed out"))

        with pytest.raises(AirflowTaskTimeout):
            self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert len(fake_cloud.bigquery.calls) == 1

    def test_a_refused_cancel_leaves_the_failure_it_came_for(self, dag_obj, fake_cloud):
        """The task is dying of the broken wait, and that is what Airflow has to
        see: a cancel BigQuery refuses is a line in the log, not the new failure."""
        fake_cloud.bigquery.returns["insert_job"] = _submissions(AirflowTaskTimeout("timed out"))
        fake_cloud.bigquery.fail_at["cancel_job"] = 1

        with pytest.raises(AirflowTaskTimeout):
            self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert len(fake_cloud.bigquery.cancels) == 1

    def test_a_day_that_loads_cleanly_cancels_nothing(self, dag_obj, fake_cloud):
        self._load(dag_obj)(_day_of_two_campaigns(), run_id="run_a")

        assert fake_cloud.bigquery.cancels == []


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


class TestTheRecordIsTheOperators:
    """The fixtures of this file describe what the operator really returns."""

    def test_the_fixture_carries_every_key_of_an_export_record(self):
        """A key added to the operator and not here would surface as a KeyError
        on a live run, since the example builds every address out of the record."""
        assert set(_record()) == set(ExportRecord.__annotations__)


class TestFindingRecords:
    """Records of a kind are taken out of a day, and one snapshot out of a run."""

    def test_every_statistics_record_of_a_day_is_selected(self, dag_module):
        records = [*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")]
        assert dag_module.select_records(records, "stats") == _day_of_two_campaigns()

    def test_the_selection_keeps_the_order_the_operator_returned(self, dag_module):
        records = _day_of_two_campaigns()
        selected = dag_module.select_records(list(reversed(records)), "stats")
        assert [r["campaign_id"] for r in selected] == [5678, 1234]

    def test_a_day_with_no_record_of_that_kind_selects_nothing(self, dag_module):
        assert dag_module.select_records([_record()], "dict") == []
        assert dag_module.select_records([], "stats") == []
        assert dag_module.select_records(None, "stats") == []

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
        key = dag_module.s3_key(_record(kind="stats", date="2026-08-20", campaign_id=1234))
        assert key == (
            f"{dag_module.S3_PREFIX}/17004/stats"
            "/_year=2026/_month=08/_day=20/_date=20260820/_campaign_id=1234/2026-08-20.json"
        )

    def test_the_campaign_is_the_last_partition_of_a_stats_key(self, dag_module):
        """A range is picked by date and narrowed by campaign, so the date comes first."""
        key = dag_module.s3_key(_record(campaign_id=5678))
        partitions = [part for part in key.split("/") if part.startswith("_")]
        assert partitions[-1] == "_campaign_id=5678"

    def test_two_campaigns_of_one_day_land_on_different_keys(self, dag_module):
        keys = {dag_module.s3_key(record) for record in _day_of_two_campaigns()}
        assert len(keys) == 2

    def test_the_dictionary_key_carries_no_campaign(self, dag_module):
        """The snapshot describes the whole cabinet, so no campaign addresses it."""
        assert "_campaign_id" not in dag_module.s3_key(_record(kind="dict", date="2026-08-21"))

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

    def test_gcs_object_isolates_the_dag_and_the_run(self, dag_module):
        obj = dag_module.gcs_object(_record(), "manual__2026-08-21T00:00:00+00:00")
        assert obj == (
            f"{dag_module.GCS_PREFIX}/admetrica_to_bq_and_s3-415827a2"
            "/manual__2026-08-21T00_00_00_00_00-f3d888b4"
            "/17004/stats/2026-08-20/1234.json"
        )

    def test_two_campaigns_of_one_day_land_on_different_objects(self, dag_module):
        """One object per day would let the second campaign overwrite the first."""
        objects = {dag_module.gcs_object(record, "run_a") for record in _day_of_two_campaigns()}
        assert len(objects) == 2

    def test_the_dictionary_object_carries_no_campaign(self, dag_module):
        obj = dag_module.gcs_object(_record(kind="dict", date="2026-08-21"), "run_a")
        assert obj.endswith("/17004/dict/campaigns/2026-08-21.json")

    def test_two_dags_sharing_the_bucket_do_not_share_a_gcs_object(
        self, dag_module, monkeypatch
    ):
        # A run id is unique inside its DAG and nothing wider, and the bucket is
        # shared, so the DAG has to be in the key for the two to stay apart.
        run_id = "scheduled__2026-08-21T00:00:00+00:00"
        mine = dag_module.gcs_object(_record(), run_id)
        monkeypatch.setattr(dag_module, "DAG_ID", "admetrica_other_advertiser")
        assert dag_module.gcs_object(_record(), run_id) != mine

    def test_dictionary_partition_uses_the_snapshot_day(self, dag_module):
        table = dag_module.bq_dictionary_table(_record(kind="dict", date="2026-08-21"))
        assert table.endswith(".campaigns$20260821")

    def test_the_dictionary_table_is_qualified_by_project_and_dataset(self, dag_module):
        table = dag_module.bq_dictionary_table(_record(kind="dict", date="2026-08-21"))
        assert table == f"{dag_module.BQ_PROJECT}.{dag_module.BQ_DATASET}.campaigns$20260821"

    def test_the_statistics_table_names_the_advertiser_and_the_campaign(self, dag_module):
        table = dag_module.stats_table_partition(_record(date="2026-08-20", campaign_id=5678))
        assert table == f"{dag_module.BQ_STATS_TABLE}_17004_5678$20260820"

    def test_the_statistics_table_is_a_bare_identifier(self, dag_module):
        """`insert_job` names the project and the dataset in fields of its own."""
        table = dag_module.stats_table_partition(_record())
        assert dag_module.BQ_PROJECT not in table
        assert not table.startswith(f"{dag_module.BQ_DATASET}.")

    def test_two_campaigns_of_one_day_land_in_different_tables(self, dag_module):
        tables = {dag_module.stats_table_partition(record) for record in _day_of_two_campaigns()}
        assert len(tables) == 2

    def test_a_prefix_of_another_advertiser_is_not_matched_by_a_wildcard(self, dag_module):
        """`stats_123_*` must not reach the tables of advertiser 1234."""
        near = dag_module.stats_table_partition(_record(advertiser_id=1234, campaign_id=5))
        assert not near.startswith("stats_123_")

    def test_the_table_of_a_campaign_is_named_without_a_partition(self, dag_module):
        """The load addresses a partition, the creation of the table does not."""
        assert dag_module.stats_table(_record()) == "stats_17004_1234"
        assert dag_module.stats_table_partition(_record()).startswith(
            f"{dag_module.stats_table(_record())}$"
        )

    def test_one_record_gives_every_address_of_its_load(self, dag_module):
        record = _record(kind="dict", date="2026-08-21", path="/tmp/d.json")
        params = dag_module.dictionary_load_params(record, "run_a")
        assert params == {
            "src": "/tmp/d.json",
            "gcs_object": dag_module.gcs_object(record, "run_a"),
            "s3_key": dag_module.s3_key(record),
            "bq_table": dag_module.bq_dictionary_table(record),
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

    def test_the_snapshot_is_loaded_into_the_dictionary_table(self, dag_obj, dag_module):
        dictionary_params = dag_obj.get_task("dictionary.params").python_callable
        snapshot = _record(kind="dict", date="2026-08-21", path="/tmp/d.json")
        params = dictionary_params(
            [[_record(path="/tmp/20.json"), snapshot], [dict(snapshot)]], run_id="run_a"
        )
        assert params == dag_module.dictionary_load_params(snapshot, "run_a")

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
        run_dir = tmp_path / dag_module.id_segment(dag_module.DAG_ID) / dag_module.id_segment(run_id)
        day = run_dir / "17004" / "stats" / "2026-08-20"
        day.mkdir(parents=True)
        (day / "1234.json").write_text("{}", encoding="utf-8")
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
        assert (other / "17004" / "stats" / "2026-08-20" / "1234.json").is_file()

    def test_a_run_that_wrote_nothing_is_no_failure(self, dag_obj, dag_module, tmp_path, monkeypatch):
        monkeypatch.setattr(dag_module, "BASE_DIR", str(tmp_path))
        cleanup = dag_obj.get_task("cleanup").python_callable
        cleanup(run_id="manual__2026-08-21T00:00:00+00:00")
