"""Tests for the S3 example DAG: it imports, it is wired as described, and the
pure functions behind its tasks build the dates, the snapshot and the key."""

from __future__ import annotations

import importlib

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import MappedTaskGroup

from airflow_provider_yandex_admetrica.operators.stats import ExportRecord

_MOD_NAME = "examples.admetrica_to_s3_dag"

#: Everything that happens to one day, in the order it happens.
_DAY_TASKS = ("day.collect", "day.upload_s3")

#: The chain the snapshot of the campaign dictionary travels, once per run.
_DICTIONARY_TASKS = ("dictionary.params", "dictionary.upload_s3")


@pytest.fixture(scope="module")
def dag_module():
    """Import the example DAG module once for the whole file."""
    return importlib.import_module(_MOD_NAME)


@pytest.fixture(scope="module")
def dag_obj(dag_module):
    """Return the DAG the decorated factory builds."""
    factory = dag_module.admetrica_to_s3
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


@pytest.fixture
def fake_s3(dag_module, monkeypatch, recording_hook):
    """Replace the hook the module holds and hand back the record of its calls.

    The stand-in is held to the signature of the real hook, so a keyword the
    provider would refuse fails the test rather than a live run.
    """
    hook = recording_hook(S3Hook, "aws_conn_id", calls="load_file")
    monkeypatch.setattr(dag_module, "S3Hook", hook)
    return hook


class TestImport:
    """The DAG file is importable and defines the expected tasks."""

    def test_module_imports(self, dag_module):
        assert dag_module.admetrica_to_s3 is not None

    def test_dag_id(self, dag_obj):
        assert dag_obj.dag_id == "admetrica_to_s3"

    def test_expected_tasks_present(self, dag_obj):
        expected = {"get_dates", "cleanup", *_DAY_TASKS, *_DICTIONARY_TASKS}
        assert expected <= set(dag_obj.task_dict)

    def test_nothing_of_the_other_destination_is_here(self, dag_obj):
        """This example carries one destination: the BigQuery half is its own DAG."""
        assert not [t for t in dag_obj.task_dict if "gcs" in t or "bq" in t]

    def test_single_active_run(self, dag_obj):
        assert dag_obj.max_active_runs == 1


class TestOneDayIsOneMapIndex:
    """A day is a map index of the group, so days never wait for each other."""

    def test_the_group_of_a_day_is_mapped_over_the_dates(self, dag_obj):
        group = dag_obj.task_group_dict["day"]
        assert isinstance(group, MappedTaskGroup)
        assert "day" in group._expand_input.value

    def test_every_task_of_a_day_belongs_to_the_mapped_group(self, dag_obj):
        """The whole day rides one map index: the collection and the upload."""
        for task_id in _DAY_TASKS:
            assert dag_obj.get_task(task_id).task_group.group_id == "day"

    def test_the_day_reaches_s3_without_waiting_for_another_day(self, dag_obj):
        """Everything an upload of a day waits for is that same day: no reduction between."""
        upstream = dag_obj.get_task("day.upload_s3").upstream_task_ids
        assert all(t.startswith("day.") for t in upstream)

    def test_the_chain_of_a_day_runs_in_order(self, dag_obj):
        assert dag_obj.get_task("day.upload_s3").upstream_task_ids == {"day.collect"}
        assert "day.upload_s3" in dag_obj.get_task("day.collect").downstream_task_ids

    def test_the_group_of_a_day_holds_exactly_its_chain(self, dag_obj):
        """A day is the collection and the upload — a task more is a task the
        map index pays for on every day of the period."""
        assert set(dag_obj.task_group_dict["day"].children) == set(_DAY_TASKS)

    def test_collect_runs_one_day_at_a_time(self, dag_obj):
        assert dag_obj.get_task("day.collect").max_active_tis_per_dag == 1


class TestTheDictionaryOfTheRun:
    """The snapshot is one per run and a failed day does not hold it back."""

    def test_the_snapshot_waits_for_the_days_however_they_ended(self, dag_obj):
        assert dag_obj.get_task("dictionary.params").trigger_rule == "all_done"
        assert "day.collect" in dag_obj.get_task("dictionary.params").upstream_task_ids

    def test_the_snapshot_is_uploaded_once(self, dag_obj):
        for task_id in _DICTIONARY_TASKS:
            assert dag_obj.get_task(task_id).task_group.group_id == "dictionary"
            assert not dag_obj.get_task(task_id).get_needs_expansion()


class TestTheEndOfTheRun:
    """Cleanup waits for every upload and tolerates a day that had nothing to upload."""

    def test_cleanup_tolerates_a_skip(self, dag_obj):
        assert dag_obj.get_task("cleanup").trigger_rule == "none_failed"

    def test_cleanup_runs_after_every_upload(self, dag_obj):
        upstream = dag_obj.get_task("cleanup").upstream_task_ids
        assert {"day.upload_s3", "dictionary.upload_s3"} <= upstream


class TestUploadOperator:
    """The uploads replace, so a re-run overwrites exactly what it re-collected."""

    def test_the_snapshot_replaces_the_key_it_lands_on(self, dag_obj):
        assert dag_obj.get_task("dictionary.upload_s3").replace is True

    def test_every_file_of_a_day_replaces_the_key_it_lands_on(self, dag_obj, fake_s3):
        upload = dag_obj.get_task("day.upload_s3").python_callable
        upload(_day_of_two_campaigns())

        assert [call["replace"] for call in fake_s3.calls] == [True, True]

    def test_the_uploads_of_a_day_go_through_the_configured_connection(
        self, dag_obj, dag_module, fake_s3
    ):
        upload = dag_obj.get_task("day.upload_s3").python_callable
        upload(_day_of_two_campaigns())

        assert fake_s3.conn_ids == [dag_module.S3_CONN_ID]
        assert {call["bucket_name"] for call in fake_s3.calls} == {dag_module.S3_BUCKET}


class TestTheDayUploadsEveryCampaign:
    """A day is uploaded whole: as many files as campaigns that had rows."""

    def test_one_upload_per_statistics_record(self, dag_obj, fake_s3):
        """Uploading only the first record would lose every other campaign in silence."""
        upload = dag_obj.get_task("day.upload_s3").python_callable
        upload([*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")])

        assert len(fake_s3.calls) == 2

    def test_each_campaign_travels_from_its_own_file_to_its_own_key(
        self, dag_obj, dag_module, fake_s3
    ):
        records = _day_of_two_campaigns()
        upload = dag_obj.get_task("day.upload_s3").python_callable
        upload(records)

        assert [call["filename"] for call in fake_s3.calls] == ["/tmp/1234.json", "/tmp/5678.json"]
        assert [call["key"] for call in fake_s3.calls] == [dag_module.s3_key(r) for r in records]

    def test_the_keys_of_one_day_differ_by_campaign(self, dag_obj, fake_s3):
        upload = dag_obj.get_task("day.upload_s3").python_callable
        upload(_day_of_two_campaigns())

        keys = [call["key"] for call in fake_s3.calls]
        assert keys[0] != keys[1]
        assert "_campaign_id=1234" in keys[0]
        assert "_campaign_id=5678" in keys[1]

    def test_the_snapshot_of_the_dictionary_is_not_uploaded_by_the_day(self, dag_obj, fake_s3):
        """The dictionary is one per run and rides its own group."""
        upload = dag_obj.get_task("day.upload_s3").python_callable
        upload([*_day_of_two_campaigns(), _record(kind="dict", path="/tmp/d.json")])

        assert "/tmp/d.json" not in [call["filename"] for call in fake_s3.calls]

    def test_a_day_without_statistics_uploads_nothing_and_skips(self, dag_obj, fake_s3):
        upload = dag_obj.get_task("day.upload_s3").python_callable
        with pytest.raises(AirflowSkipException):
            upload([_record(kind="dict", path="/tmp/d.json")])

        assert fake_s3.calls == []

    def test_a_day_that_wrote_nothing_at_all_skips(self, dag_obj, fake_s3):
        upload = dag_obj.get_task("day.upload_s3").python_callable
        with pytest.raises(AirflowSkipException):
            upload([])

        assert fake_s3.calls == []

    def test_a_refused_upload_fails_the_day(self, dag_obj, fake_s3):
        """A swallowed failure would ship a day quietly missing campaigns."""
        fake_s3.fail_at["load_file"] = 2
        upload = dag_obj.get_task("day.upload_s3").python_callable

        with pytest.raises(RuntimeError):
            upload(_day_of_two_campaigns())

        assert len(fake_s3.calls) == 2


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

    def test_one_record_gives_every_address_of_its_upload(self, dag_module):
        record = _record(kind="dict", date="2026-08-21", path="/tmp/d.json")
        assert dag_module.dictionary_load_params(record) == {
            "src": "/tmp/d.json",
            "s3_key": dag_module.s3_key(record),
        }


class TestTaskCallables:
    """The functions the tasks run, called directly through ``.python_callable``."""

    def test_get_dates_reads_the_period_out_of_the_params(self, dag_obj):
        get_dates = dag_obj.get_task("get_dates").python_callable
        dates = get_dates(params={"date_from": "2026-08-19", "date_to": "2026-08-21"})
        assert dates == ["2026-08-21", "2026-08-20", "2026-08-19"]

    def test_the_snapshot_is_uploaded_under_its_own_key(self, dag_obj, dag_module):
        dictionary_params = dag_obj.get_task("dictionary.params").python_callable
        snapshot = _record(kind="dict", date="2026-08-21", path="/tmp/d.json")
        params = dictionary_params([[_record(path="/tmp/20.json"), snapshot], [dict(snapshot)]])
        assert params == dag_module.dictionary_load_params(snapshot)

    def test_a_run_without_a_snapshot_skips_the_dictionary(self, dag_obj):
        dictionary_params = dag_obj.get_task("dictionary.params").python_callable
        with pytest.raises(AirflowSkipException):
            dictionary_params([[_record()], []])


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


class TestTheCampaignsOfARun:
    """The selection is a run parameter, and both halves of it reach the operator."""

    def test_the_scope_is_offered_as_a_choice_of_two(self, dag_obj):
        """A run form picks the scope from a list, so a typo never reaches the operator."""
        assert dag_obj.params.get_param("campaign_scope").schema["enum"] == ["active", "all"]

    def test_the_example_walks_the_whole_cabinet_by_default(self, dag_obj):
        """A run of this example is a backfill of a month, and a campaign archived
        inside that month has real rows for its days."""
        assert dag_obj.params["campaign_scope"] == "all"

    def test_naming_no_campaign_leaves_the_scope_deciding(self, dag_obj):
        assert dag_obj.params["campaign_ids"] == ""

    def test_both_reach_the_operator_as_templates(self, dag_obj):
        collect = dag_obj.get_task("day.collect")
        assert collect.campaign_scope == "{{ params.campaign_scope }}"
        assert collect.campaign_ids == "{{ params.campaign_ids }}"

    def test_the_templates_are_fields_the_operator_renders(self, dag_obj):
        collect = dag_obj.get_task("day.collect")
        assert {"campaign_scope", "campaign_ids"} <= set(collect.template_fields)
