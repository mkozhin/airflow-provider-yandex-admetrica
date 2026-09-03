"""Tests for the operator that writes statistics and campaigns to local files."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import DAG, Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _CAMPAIGN_FIELDS,
    DEFAULT_LIMIT,
    DEFAULT_REQUEST_DELAY,
    AdmetricaHook,
    _campaign_record,
)
from airflow_provider_yandex_admetrica.operators.stats import (
    DICT_CAMPAIGNS_PARTS,
    YandexAdmetricaStatsOperator,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

ADVERTISER_ID = 17004

DATE = "2026-08-20"

#: The campaign every row of the fixtures belongs to unless told otherwise.
CAMPAIGN_ID = 123456

#: A second campaign of the same advertiser, so a day of rows has something to
#: be split by: with one campaign a per-campaign layout and a per-day one look
#: exactly alike.
OTHER_CAMPAIGN_ID = 654321

RUN_ID = "manual__2026-08-21T00:00:00+00:00"

#: The run directory segment for :data:`RUN_ID`, spelled out rather than built
#: by the function under test: it is the layout the DAG's own cleanup and its
#: uploads address, so a change to it is a change every reader has to be told
#: about.
RUN_SEGMENT = "manual__2026-08-21T00_00_00_00_00-f3d888b4"

#: The DAG directory segment of an operator declared outside a DAG, which is
#: what ``dag_id`` answers for one.
DAG_SEGMENT = "adhoc_airflow-4aa313a5"

#: The day the export runs, which is the day after the one it reports on.
SNAPSHOT_DATE = "2026-08-21"

RUN_START = datetime(2026, 8, 21, 3, 15, tzinfo=timezone.utc)

DIMENSIONS = ["am:e:placement", "am:e:deviceType"]

METRICS = ["am:e:renders", "am:e:clicks"]


def _operator(**kwargs) -> YandexAdmetricaStatsOperator:
    defaults = dict(
        task_id="collect",
        admetrica_conn_id="admetrica",
        date=DATE,
        dimensions=DIMENSIONS,
        metrics=METRICS,
    )
    defaults.update(kwargs)
    return YandexAdmetricaStatsOperator(**defaults)


def _context(
    run_id: str = RUN_ID,
    ti: SimpleNamespace | None = None,
    start_date: datetime | None = RUN_START,
) -> dict:
    context: dict = {"run_id": run_id, "dag_run": SimpleNamespace(start_date=start_date)}
    if ti is not None:
        context["ti"] = ti
    return context


def _connection(advertiser_id: object = ADVERTISER_ID) -> Connection:
    return Connection(
        conn_id="admetrica",
        conn_type="http",
        password=TOKEN,
        extra=json.dumps({"advertiser_id": advertiser_id}),
    )


def _campaign(campaign_id: int = CAMPAIGN_ID, name: str = "Летняя кампания") -> dict:
    return {
        "campaign_id": campaign_id,
        "name": name,
        "status": "active",
        "date_start": "2026-06-01",
        "date_end": "2026-08-31",
        "advertiser_id": ADVERTISER_ID,
        "advertiser_name": "Рекламодатель",
    }


def _row(
    placement_id: int = 55, renders: int = 12345, campaign_id: int = CAMPAIGN_ID
) -> dict:
    return {
        "date": DATE,
        "advertiser_id": ADVERTISER_ID,
        "campaign_id": campaign_id,
        "dimensions": {
            "placement": {"name": "Главная страница", "id": placement_id},
            "device_type": {"name": "mobile"},
        },
        "metrics": {"renders": renders, "clicks": 67},
    }


class _Run:
    """One execute() with the hook's connection and its answers stood in for."""

    def __init__(
        self,
        rows: list[dict],
        advertiser_id: object = ADVERTISER_ID,
        campaigns: list[dict] | None = None,
    ) -> None:
        self.rows = rows
        self.connection = _connection(advertiser_id)
        self.get_stats = MagicMock(return_value=rows)
        self.get_campaigns = MagicMock(
            return_value=[_campaign()] if campaigns is None else campaigns
        )
        self.hooks: list[AdmetricaHook] = []

    def __enter__(self):
        original = AdmetricaHook.__init__

        def record(hook, **kwargs):
            original(hook, **kwargs)
            hook.get_connection = MagicMock(return_value=self.connection)
            self.hooks.append(hook)

        self._patches = [
            patch.object(AdmetricaHook, "__init__", record),
            patch.object(AdmetricaHook, "get_stats", self.get_stats),
            patch.object(AdmetricaHook, "get_campaigns", self.get_campaigns),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()
        return False

    @property
    def hook(self) -> AdmetricaHook:
        assert len(self.hooks) == 1
        return self.hooks[0]


def _read(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return f.read().splitlines()


class TestPath:
    def test_names_the_campaign_under_the_day_of_the_advertiser(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        assert result[0]["path"] == os.path.join(
            str(tmp_path),
            DAG_SEGMENT,
            RUN_SEGMENT,
            str(ADVERTISER_ID),
            "stats",
            DATE,
            f"{CAMPAIGN_ID}.json",
        )

    def test_sanitizes_the_run_id(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path(
            "scheduled__2026-08-21T00:00:00+00:00", 17004, ("stats",), DATE, CAMPAIGN_ID
        )
        assert "scheduled__2026-08-21T00_00_00_00_00" in path
        assert ":" not in os.path.relpath(path, str(tmp_path))
        assert "+" not in os.path.relpath(path, str(tmp_path))

    def test_two_runs_do_not_share_a_file(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        first = op._build_path("run_a", ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        second = op._build_path("run_b", ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        assert first != second

    def test_two_dags_sharing_a_base_dir_do_not_share_a_run_directory(self, tmp_path):
        # Airflow holds a run id unique inside its DAG and nothing wider, so two
        # DAGs on one schedule are handed the same one. They serve different
        # advertisers, and the cleanup of either owns the whole run directory.
        with DAG("advertiser_a", start_date=RUN_START):
            first = _operator(base_dir=str(tmp_path))
        with DAG("advertiser_b", start_date=RUN_START):
            second = _operator(base_dir=str(tmp_path))
        run_id = "scheduled__2026-08-21T00:00:00+00:00"
        first_path = first._build_path(
            run_id, ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID
        )
        second_path = second._build_path(
            run_id, ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID
        )
        assert first_path != second_path
        assert os.path.commonpath([first_path, second_path]) == str(tmp_path)

    @pytest.mark.parametrize(
        "identifier",
        ["a" * 250, "я" * 250, "manual__" + "b" * 242],
        ids=["ascii", "cyrillic", "prefixed"],
    )
    def test_the_longest_identifier_airflow_accepts_still_names_a_directory(
        self, tmp_path, identifier
    ):
        # Airflow holds a dag_id and a run_id of up to 250 characters, and a
        # directory name is bounded in bytes rather than characters.
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path(identifier, ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        for segment in os.path.relpath(path, str(tmp_path)).split(os.sep):
            assert len(segment.encode("utf-8")) <= 255
        op._write([_row()], path)
        assert os.path.isfile(path)

    def test_long_run_ids_sharing_their_start_stay_apart(self, tmp_path):
        # The cut takes the tail that would have told them apart, so what does
        # it instead is the digest, which is taken from the whole identifier.
        op = _operator(base_dir=str(tmp_path))
        first = op._build_path(
            "c" * 250 + "first", ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID
        )
        second = op._build_path(
            "c" * 250 + "second", ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID
        )
        assert first != second

    def test_run_ids_that_sanitise_alike_stay_apart(self, tmp_path):
        # The substitution that makes a directory name of a run id maps several
        # run ids onto one name; the digest beside it is what tells them apart.
        op = _operator(base_dir=str(tmp_path))
        first = op._build_path("manual:a", ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        second = op._build_path("manual/a", ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        assert first != second
        assert "manual_a-" in first
        assert "manual_a-" in second


class TestTheCampaignNamingTheFile:
    """The segment is a positive whole number written out, and anything else is
    refused rather than read into one: read through ``int``, a fraction and a
    numeric string both name the file of the campaign they truncate to, and two
    campaigns sharing a file is one campaign's rows read as another's."""

    def test_the_campaign_names_the_file_of_the_day(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path(RUN_ID, ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        assert os.path.basename(path) == f"{CAMPAIGN_ID}.json"

    @pytest.mark.parametrize("campaign_id", [True, False, 1.9, 1.0, "1234", "0001"])
    def test_what_is_no_whole_number_is_refused(self, tmp_path, campaign_id):
        op = _operator(base_dir=str(tmp_path))
        with pytest.raises(TypeError):
            op._build_path(RUN_ID, ADVERTISER_ID, ("stats",), DATE, campaign_id)

    @pytest.mark.parametrize("campaign_id", [0, -7])
    def test_what_is_not_positive_is_refused(self, tmp_path, campaign_id):
        op = _operator(base_dir=str(tmp_path))
        with pytest.raises(ValueError):
            op._build_path(RUN_ID, ADVERTISER_ID, ("stats",), DATE, campaign_id)

    def test_the_dictionary_asks_for_no_campaign_at_all(self, tmp_path):
        """The snapshot of the whole cabinet answers to no campaign."""
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path(RUN_ID, ADVERTISER_ID, DICT_CAMPAIGNS_PARTS, DATE)
        assert os.path.basename(path) == f"{DATE}.json"


class TestWrittenFile:
    def test_writes_one_object_per_line(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        rows = [_row(55, 1), _row(56, 2), _row(57, 3)]
        with _Run(rows):
            result = op.execute(_context())
        lines = _read(result[0]["path"])
        assert len(lines) == 3
        assert [json.loads(line) for line in lines] == rows

    def test_keeps_the_key_order_of_the_record(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        (line,) = _read(result[0]["path"])
        assert list(json.loads(line)) == [
            "date",
            "advertiser_id",
            "campaign_id",
            "dimensions",
            "metrics",
        ]
        assert list(json.loads(line)["dimensions"]) == ["placement", "device_type"]
        assert list(json.loads(line)["metrics"]) == ["renders", "clicks"]

    def test_keeps_rows_whose_groupings_carry_different_fields(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        rows = [_row(), _row()]
        rows[1]["dimensions"] = {
            "placement": {"name": "Спецпроект"},
            "device_type": {"name": "desktop", "id": 3, "note": "extra"},
        }
        with _Run(rows):
            result = op.execute(_context())
        written = [json.loads(line) for line in _read(result[0]["path"])]
        assert written == rows

    def test_writes_names_unescaped(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        (line,) = _read(result[0]["path"])
        assert "Главная страница" in line

    def test_a_day_without_rows_writes_no_statistics(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        with _Run([]):
            result = op.execute(_context())
        assert result == []
        assert list(tmp_path.rglob("*.json")) == []

    def test_a_day_without_rows_still_exports_the_dictionary(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([]):
            result = op.execute(_context())
        assert [record["kind"] for record in result] == ["dict"]


class TestTheGrainIsADayOfACampaign:
    """One file and one record per campaign, so an address reaches one campaign."""

    def test_a_day_of_several_campaigns_gives_a_file_and_a_record_each(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        rows = [
            _row(55, 1),
            _row(56, 2, campaign_id=OTHER_CAMPAIGN_ID),
            _row(57, 3),
        ]
        with _Run(rows):
            result = op.execute(_context())
        assert [record["campaign_id"] for record in result] == [
            CAMPAIGN_ID,
            OTHER_CAMPAIGN_ID,
        ]
        first, second = (record["path"] for record in result)
        assert first != second
        assert [json.loads(line) for line in _read(first)] == [rows[0], rows[2]]
        assert [json.loads(line) for line in _read(second)] == [rows[1]]

    def test_the_path_names_the_campaign_inside_the_directory_of_the_day(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        with _Run([_row(campaign_id=OTHER_CAMPAIGN_ID)]):
            (record,) = op.execute(_context())
        assert record["path"].endswith(
            os.path.join("stats", DATE, f"{OTHER_CAMPAIGN_ID}.json")
        )

    def test_two_campaigns_of_one_day_land_in_the_directory_of_that_day(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        with _Run([_row(), _row(campaign_id=OTHER_CAMPAIGN_ID)]):
            first, second = (record["path"] for record in op.execute(_context()))
        assert os.path.dirname(first) == os.path.dirname(second)
        assert os.path.basename(os.path.dirname(first)) == DATE

    def test_a_campaign_the_day_holds_no_rows_for_writes_nothing(self, tmp_path):
        # The cabinet lists both campaigns; only one of them ran that day, and
        # the other keeps whatever an earlier export left in the warehouse.
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        with _Run(
            [_row()], campaigns=[_campaign(CAMPAIGN_ID), _campaign(OTHER_CAMPAIGN_ID)]
        ):
            result = op.execute(_context())
        assert [record["campaign_id"] for record in result] == [CAMPAIGN_ID]
        day_dir = (
            tmp_path / DAG_SEGMENT / RUN_SEGMENT / str(ADVERTISER_ID) / "stats" / DATE
        )
        assert [entry.name for entry in day_dir.iterdir()] == [f"{CAMPAIGN_ID}.json"]

    def test_a_day_without_rows_addresses_no_statistics_at_all(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([]):
            result = op.execute(_context())
        assert [record["kind"] for record in result] == ["dict"]
        stats_dir = tmp_path / DAG_SEGMENT / RUN_SEGMENT / str(ADVERTISER_ID) / "stats"
        assert not stats_dir.exists()

    def test_the_records_follow_the_order_the_cabinet_lists_the_campaigns(self, tmp_path):
        # The hook walks the cabinet in order and the rows arrive in that order,
        # so reading a result follows the same order as reading the cabinet.
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        rows = [_row(campaign_id=OTHER_CAMPAIGN_ID), _row(campaign_id=CAMPAIGN_ID)]
        with _Run(rows):
            result = op.execute(_context())
        assert [record["campaign_id"] for record in result] == [
            OTHER_CAMPAIGN_ID,
            CAMPAIGN_ID,
        ]

    def test_the_dictionary_stays_one_file_addressed_by_the_day_alone(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row(), _row(campaign_id=OTHER_CAMPAIGN_ID)]):
            result = op.execute(_context())
        (record,) = [r for r in result if r["kind"] == "dict"]
        assert record["campaign_id"] is None
        assert record["path"] == os.path.join(
            str(tmp_path),
            DAG_SEGMENT,
            RUN_SEGMENT,
            str(ADVERTISER_ID),
            "dict",
            "campaigns",
            f"{SNAPSHOT_DATE}.json",
        )

    def test_a_campaign_that_could_not_be_written_fails_the_day(self, tmp_path, monkeypatch):
        """A file that never got written stops the task rather than shortening
        the result: a shorter list is a day the warehouse loads as complete."""
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        written: list[str] = []
        write = op._write

        def failing_write(records, path):
            if written:
                raise OSError("no space left on device")
            written.append(path)
            write(records, path)

        monkeypatch.setattr(op, "_write", failing_write)
        with _Run([_row(), _row(campaign_id=OTHER_CAMPAIGN_ID)]):
            with pytest.raises(OSError):
                op.execute(_context())

        assert len(written) == 1

    def test_every_record_of_a_full_run_carries_the_campaign_key(self, tmp_path):
        """A DAG walks the whole list, so a missing key would fail it on the dict."""
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row(), _row(campaign_id=OTHER_CAMPAIGN_ID)]):
            result = op.execute(_context())
        assert [record["kind"] for record in result] == ["stats", "stats", "dict"]
        assert [record["campaign_id"] for record in result] == [
            CAMPAIGN_ID,
            OTHER_CAMPAIGN_ID,
            None,
        ]


class TestResult:
    def test_describes_the_file_it_wrote(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        with _Run([_row()]):
            result = op.execute(_context())
        assert len(result) == 1
        assert result[0]["kind"] == "stats"
        assert result[0]["date"] == DATE
        assert os.path.exists(result[0]["path"])

    def test_describes_both_files_of_a_full_run(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        assert [record["kind"] for record in result] == ["stats", "dict"]
        assert all(record["advertiser_id"] == ADVERTISER_ID for record in result)
        assert all(os.path.exists(record["path"]) for record in result)

    def test_carries_the_advertiser_of_the_connection(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()], advertiser_id=42):
            result = op.execute(_context())
        assert all(record["advertiser_id"] == 42 for record in result)

    def test_reads_the_advertiser_as_a_number(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()], advertiser_id="17004"):
            result = op.execute(_context())
        assert result[0]["advertiser_id"] == 17004

    def test_a_connection_without_an_advertiser_fails_the_task(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()], advertiser_id=None):
            with pytest.raises(AirflowException, match="advertiser_id"):
                op.execute(_context())


class TestParametersReachTheHook:
    def test_report_parameters_go_out_as_given(self, tmp_path):
        op = _operator(
            base_dir=str(tmp_path),
            filters="am:e:deviceType=='mobile'",
            accuracy="0.1",
            include_undefined=False,
            timezone="+03:00",
            lang="ru",
            extra_params={"goal_id": 12345},
        )
        with _Run([_row()]) as run:
            op.execute(_context())
        args, kwargs = run.get_stats.call_args
        assert args == (DATE, DIMENSIONS, METRICS)
        assert kwargs == {
            "filters": "am:e:deviceType=='mobile'",
            "accuracy": "0.1",
            "include_undefined": False,
            "timezone": "+03:00",
            "lang": "ru",
            "extra_params": {"goal_id": 12345},
        }

    def test_defaults_keep_the_numbers_steady_and_the_rows_whole(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]) as run:
            op.execute(_context())
        _, kwargs = run.get_stats.call_args
        assert kwargs["accuracy"] == "full"
        assert kwargs["include_undefined"] is True

    def test_include_undefined_of_none_reaches_the_hook_as_none(self, tmp_path):
        """``None`` is the ask to leave the parameter out and take the API default."""
        op = _operator(base_dir=str(tmp_path), include_undefined=None)
        with _Run([_row()]) as run:
            op.execute(_context())
        _, kwargs = run.get_stats.call_args
        assert kwargs["include_undefined"] is None

    def test_pace_and_page_size_reach_the_hook(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), limit=500, request_delay=1.5)
        with _Run([_row()]) as run:
            op.execute(_context())
        assert run.hook.limit == 500
        assert run.hook.request_delay == 1.5

    def test_pace_and_page_size_default_to_the_hook_values(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]) as run:
            op.execute(_context())
        assert run.hook.limit == DEFAULT_LIMIT
        assert run.hook.request_delay == DEFAULT_REQUEST_DELAY

    def test_connection_id_reaches_the_hook(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), admetrica_conn_id="other_advertiser")
        with _Run([_row()]) as run:
            op.execute(_context())
        assert run.hook.admetrica_conn_id == "other_advertiser"


class TestDiagnostics:
    def test_nothing_is_built_without_a_connection_id(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]) as run:
            op.execute(_context())
        assert run.hook._loki is None

    def test_nothing_is_built_when_the_template_renders_empty(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), loki_conn_id="")
        with _Run([_row()]) as run:
            op.execute(_context())
        assert run.hook._loki is None

    def test_an_empty_connection_id_leaves_the_context_unread(self, tmp_path):
        """The context of this run carries no "ti"; reading one would raise."""
        op = _operator(base_dir=str(tmp_path))
        context = _context()
        assert "ti" not in context

        with _Run([_row()]) as run:
            result = op.execute(context)

        assert run.hook._loki is None
        assert [r["kind"] for r in result] == ["stats", "dict"]

    def test_a_named_connection_builds_the_sink(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), loki_conn_id="loki")
        ti = SimpleNamespace(try_number=2, map_index=3)
        with _Run([_row()]) as run:
            op.execute(_context(ti=ti))
        loki = run.hook._loki
        assert loki is not None
        assert loki._conn_id == "loki"
        assert loki._context == {
            "dag_id": op.dag_id,
            "task_id": "collect",
            "dag_run_id": RUN_ID,
            "try_number": 2,
            "map_index": 3,
        }


class TestDeclaration:
    def test_templated_fields_cover_the_day_the_connections_and_the_directory(self):
        for field in ("date", "admetrica_conn_id", "loki_conn_id", "base_dir"):
            assert field in YandexAdmetricaStatsOperator.template_fields

    def test_the_operator_has_a_colour(self):
        assert YandexAdmetricaStatsOperator.ui_color

    def test_the_advertiser_default_is_the_hook_default(self):
        op = YandexAdmetricaStatsOperator(
            task_id="collect", date=DATE, dimensions=DIMENSIONS, metrics=METRICS
        )
        assert op.admetrica_conn_id == AdmetricaHook.default_conn_name


class TestCampaignDictionary:
    def test_writes_the_snapshot_under_the_day_it_ran(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        (record,) = [r for r in result if r["kind"] == "dict"]
        assert record["path"] == os.path.join(
            str(tmp_path),
            DAG_SEGMENT,
            RUN_SEGMENT,
            str(ADVERTISER_ID),
            "dict",
            "campaigns",
            f"{SNAPSHOT_DATE}.json",
        )

    def test_dates_the_snapshot_by_the_run_not_by_the_day_reported_on(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        (record,) = [r for r in result if r["kind"] == "dict"]
        assert record["date"] == SNAPSHOT_DATE
        assert record["date"] != DATE

    def test_every_map_index_of_one_run_names_one_day(self, tmp_path):
        earlier = _operator(base_dir=str(tmp_path), date="2026-08-19")
        later = _operator(base_dir=str(tmp_path), date=DATE)
        with _Run([_row()]):
            first = earlier.execute(_context())
        with _Run([_row()]):
            second = later.execute(_context())
        assert [r["path"] for r in first if r["kind"] == "dict"] == [
            r["path"] for r in second if r["kind"] == "dict"
        ]

    def test_falls_back_to_the_current_day_without_a_run(self, tmp_path):
        """The clock is frozen: a real one moves the day under a run at midnight."""
        op = _operator(base_dir=str(tmp_path))
        frozen = datetime(2026, 8, 21, 23, 59, 59, tzinfo=timezone.utc)
        with patch(
            "airflow_provider_yandex_admetrica.operators.stats.datetime"
        ) as clock:
            clock.now.return_value = frozen
            clock.strptime = datetime.strptime
            with _Run([_row()]):
                result = op.execute(_context(start_date=None))
        (record,) = [r for r in result if r["kind"] == "dict"]
        assert record["date"] == "2026-08-21"

    def test_column_file_name_and_result_name_one_day(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()], campaigns=[_campaign(1), _campaign(2), _campaign(3)]):
            result = op.execute(_context())
        (record,) = [r for r in result if r["kind"] == "dict"]
        written = [json.loads(line) for line in _read(record["path"])]
        assert len(written) == 3
        assert {row["snapshot_date"] for row in written} == {record["date"]}
        assert os.path.basename(record["path"]) == f"{record['date']}.json"

    def test_keeps_the_fields_of_the_api_as_they_came(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        (record,) = [r for r in result if r["kind"] == "dict"]
        (line,) = _read(record["path"])
        written = json.loads(line)
        assert list(written) == ["snapshot_date", *_CAMPAIGN_FIELDS]
        assert {k: v for k, v in written.items() if k != "snapshot_date"} == _campaign()

    def test_the_hook_leaves_the_snapshot_day_to_the_operator(self):
        assert "snapshot_date" not in _CAMPAIGN_FIELDS
        assert "snapshot_date" not in _campaign_record(
            {**_campaign(), "snapshot_date": "2000-01-01"}
        )

    def test_switched_off_it_writes_no_dictionary(self, tmp_path):
        op = _operator(base_dir=str(tmp_path), collect_dictionaries=False)
        with _Run([_row()]) as run:
            result = op.execute(_context())
        assert [r["kind"] for r in result] == ["stats"]
        advertiser_dir = tmp_path / DAG_SEGMENT / RUN_SEGMENT / str(ADVERTISER_ID)
        assert not list(advertiser_dir.glob("dict/**/*"))
        run.get_campaigns.assert_not_called()

    def test_an_advertiser_without_campaigns_writes_nothing(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()], campaigns=[]):
            result = op.execute(_context())
        assert [r["kind"] for r in result] == ["stats"]
        advertiser_dir = tmp_path / DAG_SEGMENT / RUN_SEGMENT / str(ADVERTISER_ID)
        assert not list(advertiser_dir.glob("dict/**/*"))

    def test_a_second_run_of_the_same_day_rewrites_one_file(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            first = op.execute(_context())
        with _Run([_row()]):
            second = op.execute(_context())
        assert first == second
        run_dir = tmp_path / DAG_SEGMENT / RUN_SEGMENT
        assert len(list(run_dir.rglob("dict/campaigns/*.json"))) == 1

    def test_the_advertiser_travels_with_the_dictionary(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()], advertiser_id=42):
            result = op.execute(_context())
        (record,) = [r for r in result if r["kind"] == "dict"]
        assert record["advertiser_id"] == 42


class TestTheDayIsHeldToItsFormat:
    """The day names a file and is asked of the API, so it is checked first."""

    @pytest.mark.parametrize(
        "date",
        [
            "../../../../tmp/pwned",
            "2026-08-20/../../etc/passwd",
            "20260820",
            "2026-8-20",
            "not-a-day",
            "",
            None,
        ],
        ids=["traversal", "escaping_segment", "compact", "unpadded", "words", "empty", "none"],
    )
    def test_a_day_that_is_not_a_day_is_refused_before_anything_runs(self, date, tmp_path):
        op = _operator(base_dir=str(tmp_path), date=date)
        with _Run([_row()]) as run:
            with pytest.raises(ValueError, match="date must be a day"):
                op.execute(_context())
        run.get_stats.assert_not_called()
        assert not list(tmp_path.rglob("*.json"))

    def test_a_day_can_name_nothing_outside_the_base_directory(self, tmp_path):
        """Even reached directly, the path stays under the base directory."""
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path(
            RUN_ID, ADVERTISER_ID, ("stats",), "../../../../tmp/pwned", CAMPAIGN_ID
        )
        assert os.path.normpath(path).startswith(str(tmp_path) + os.sep)

    def test_a_campaign_that_is_not_a_number_names_no_file(self, tmp_path):
        """A campaign is a whole number, so a segment that would climb out of the
        directory is refused by its type before it names anything."""
        op = _operator(base_dir=str(tmp_path))
        with pytest.raises(TypeError):
            op._build_path(RUN_ID, ADVERTISER_ID, ("stats",), DATE, "../..")


class TestFailureLeavesNoFile:
    def test_a_day_the_hook_could_not_read_writes_nothing(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]) as run:
            run.get_stats.side_effect = AirflowException("total_rows does not match")
            with pytest.raises(AirflowException):
                op.execute(_context())
        assert not list(tmp_path.rglob("*.json"))


class TestTheWriteIsAtomic:
    """Map indices of one run share the dictionary path, so a half file is fatal."""

    def test_the_path_never_holds_a_half_written_file(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path(RUN_ID, ADVERTISER_ID, ("stats",), DATE, CAMPAIGN_ID)
        op._write([_row(55)], path)

        with pytest.raises(TypeError):
            op._write([_row(56), {"unserializable": object()}], path)

        assert _read(path) == [json.dumps(_row(55), ensure_ascii=False)]
        assert list(os.listdir(os.path.dirname(path))) == [os.path.basename(path)]
