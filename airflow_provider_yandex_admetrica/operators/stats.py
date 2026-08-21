"""Operator that exports one day of AdMetrica display statistics to a local file."""

from __future__ import annotations

import json
import os
import re
from typing import Sequence, TypedDict

from airflow.models import BaseOperator

from airflow_provider_yandex_admetrica.hooks.loki import LokiClient
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _DEFAULT_LIMIT,
    _DEFAULT_REQUEST_DELAY,
    AdmetricaHook,
)

#: Directory segments under the advertiser that hold the statistics of a day.
_STATS_PARTS = ("stats",)

#: Characters allowed in a path segment built from an identifier; everything
#: else becomes an underscore, so a run id carrying a timestamp with colons and
#: a plus sign still names a directory on every filesystem.
_UNSAFE_SEGMENT_RE = re.compile(r"[^\w-]")


class ExportRecord(TypedDict):
    """One file this operator wrote, described for the tasks downstream.

    ``advertiser_id`` travels with the record because the DAG builds the S3 key
    and the table name from it and has nowhere else to read it: the advertiser
    is named in the connection, which only the hook opens.
    """

    kind: str
    date: str
    path: str
    advertiser_id: int


class YandexAdmetricaStatsOperator(BaseOperator):
    """Collect one day of statistics for every campaign of one advertiser.

    A task is a day. The period is expanded by the DAG and fed in through
    ``expand(date=dates)``, so each day is its own map index: one day failing
    leaves the others alone, and re-running it is a clear of that map index.

    The output is JSONL, one record per line, because the set of columns is not
    known from the request — the groupings and metrics of a record are nested
    objects whose fields come from the answer and may differ between rows.

    A day with no rows writes no file and adds nothing to the result, so a
    previously exported copy of that day stays where it is.
    """

    template_fields = ("date", "admetrica_conn_id", "loki_conn_id", "base_dir")
    ui_color = "#ffe9c7"

    def __init__(
        self,
        *,
        admetrica_conn_id: str = AdmetricaHook.default_conn_name,
        date: str,
        dimensions: Sequence[str],
        metrics: Sequence[str],
        filters: str | None = None,
        accuracy: str | None = "full",
        include_undefined: bool = True,
        limit: int = _DEFAULT_LIMIT,
        request_delay: float = _DEFAULT_REQUEST_DELAY,
        timezone: str | None = None,
        lang: str | None = None,
        extra_params: dict | None = None,
        base_dir: str = "/tmp/yandex_admetrica",
        collect_dictionaries: bool = True,
        loki_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.admetrica_conn_id = admetrica_conn_id
        self.date = date
        self.dimensions = list(dimensions)
        self.metrics = list(metrics)
        self.filters = filters
        self.accuracy = accuracy
        self.include_undefined = include_undefined
        self.limit = limit
        self.request_delay = request_delay
        self.timezone = timezone
        self.lang = lang
        self.extra_params = extra_params
        self.base_dir = base_dir
        self.collect_dictionaries = collect_dictionaries
        self.loki_conn_id = loki_conn_id

    def _build_path(
        self,
        run_id: str,
        advertiser_id: int,
        parts: Sequence[str],
        date: str,
    ) -> str:
        """Return the local file for *date* under *parts* of this advertiser.

        The run id sits in the path so two runs exporting the same day never
        write the same file; it stays local, since the S3 key addresses a day
        of an advertiser and nothing else.
        """
        safe_run_id = _UNSAFE_SEGMENT_RE.sub("_", run_id)
        return os.path.join(
            self.base_dir, safe_run_id, str(advertiser_id), *parts, f"{date}.json"
        )

    def _write(self, records: Sequence[dict], path: str) -> None:
        """Write *records* to *path* as JSONL, one object per line.

        ``ensure_ascii=False`` keeps placement and campaign names readable in
        the file itself; the encoding is UTF-8 either way.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for row in records:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _build_loki_client(self, context) -> LokiClient | None:
        """Return the diagnostics sink for this run, or ``None`` when it is off.

        Diagnostics are opt-in, and an unset ``loki_conn_id`` — including a
        template that renders empty — leaves them off: nothing is constructed
        and the task context is not read beyond what the export itself needs.
        """
        if not self.loki_conn_id:
            return None
        ti = context["ti"]
        return LokiClient(
            conn_id=self.loki_conn_id,
            context={
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "dag_run_id": context["run_id"],
                "try_number": ti.try_number,
                "map_index": ti.map_index,
            },
        )

    def execute(self, context) -> list[ExportRecord]:
        hook = AdmetricaHook(
            admetrica_conn_id=self.admetrica_conn_id,
            loki=self._build_loki_client(context),
            request_delay=self.request_delay,
            limit=self.limit,
        )
        advertiser_id = hook.advertiser_id
        run_id = context["run_id"]
        result: list[ExportRecord] = []

        rows = hook.get_stats(
            self.date,
            self.dimensions,
            self.metrics,
            filters=self.filters,
            accuracy=self.accuracy,
            include_undefined=self.include_undefined,
            timezone=self.timezone,
            lang=self.lang,
            extra_params=self.extra_params,
        )
        if rows:
            path = self._build_path(run_id, advertiser_id, _STATS_PARTS, self.date)
            self._write(rows, path)
            result.append(
                ExportRecord(
                    kind="stats",
                    date=self.date,
                    path=path,
                    advertiser_id=advertiser_id,
                )
            )
        else:
            self.log.info(
                "AdMetrica returned no rows for advertiser %s on %s; no file written.",
                advertiser_id,
                self.date,
            )

        return result
