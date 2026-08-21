"""Tests for the operator that writes one day of statistics to a local file."""

from __future__ import annotations

import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _DEFAULT_LIMIT,
    _DEFAULT_REQUEST_DELAY,
    AdmetricaHook,
)
from airflow_provider_yandex_admetrica.operators.stats import (
    YandexAdmetricaStatsOperator,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"

ADVERTISER_ID = 17004

DATE = "2026-08-20"

RUN_ID = "manual__2026-08-21T00:00:00+00:00"

SAFE_RUN_ID = "manual__2026-08-21T00_00_00_00_00"

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


def _context(run_id: str = RUN_ID, ti: SimpleNamespace | None = None) -> dict:
    context: dict = {"run_id": run_id}
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


def _row(placement_id: int = 55, renders: int = 12345) -> dict:
    return {
        "date": DATE,
        "advertiser_id": ADVERTISER_ID,
        "campaign_id": 123456,
        "dimensions": {
            "placement": {"name": "Главная страница", "id": placement_id},
            "device_type": {"name": "mobile"},
        },
        "metrics": {"renders": renders, "clicks": 67},
    }


class _Run:
    """One execute() with the hook's connection and its answers stood in for."""

    def __init__(self, rows: list[dict], advertiser_id: object = ADVERTISER_ID) -> None:
        self.rows = rows
        self.connection = _connection(advertiser_id)
        self.get_stats = MagicMock(return_value=rows)
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
    def test_names_the_day_under_the_advertiser(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        assert result[0]["path"] == os.path.join(
            str(tmp_path), SAFE_RUN_ID, str(ADVERTISER_ID), "stats", f"{DATE}.json"
        )

    def test_sanitizes_the_run_id(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        path = op._build_path("scheduled__2026-08-21T00:00:00+00:00", 17004, ("stats",), DATE)
        assert "scheduled__2026-08-21T00_00_00_00_00" in path
        assert ":" not in os.path.relpath(path, str(tmp_path))
        assert "+" not in os.path.relpath(path, str(tmp_path))

    def test_two_runs_do_not_share_a_file(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        first = op._build_path("run_a", ADVERTISER_ID, ("stats",), DATE)
        second = op._build_path("run_b", ADVERTISER_ID, ("stats",), DATE)
        assert first != second


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

    def test_a_day_without_rows_writes_nothing(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([]):
            result = op.execute(_context())
        assert result == []
        assert list(tmp_path.rglob("*.json")) == []


class TestResult:
    def test_describes_the_file_it_wrote(self, tmp_path):
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            result = op.execute(_context())
        assert len(result) == 1
        assert result[0]["kind"] == "stats"
        assert result[0]["date"] == DATE
        assert os.path.exists(result[0]["path"])

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
        assert run.hook.limit == _DEFAULT_LIMIT
        assert run.hook.request_delay == _DEFAULT_REQUEST_DELAY

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
        op = _operator(base_dir=str(tmp_path))
        with _Run([_row()]):
            op.execute(_context())  # no "ti" in the context at all

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
    def test_templated_fields_cover_the_day_and_the_connections(self):
        for field in ("date", "admetrica_conn_id", "loki_conn_id"):
            assert field in YandexAdmetricaStatsOperator.template_fields

    def test_the_operator_has_a_colour(self):
        assert YandexAdmetricaStatsOperator.ui_color

    def test_the_advertiser_default_is_the_hook_default(self):
        op = YandexAdmetricaStatsOperator(
            task_id="collect", date=DATE, dimensions=DIMENSIONS, metrics=METRICS
        )
        assert op.admetrica_conn_id == AdmetricaHook.default_conn_name
