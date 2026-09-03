"""Operator that exports AdMetrica display statistics and campaigns to local files."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime
from datetime import timezone as _timezone
from typing import TYPE_CHECKING, TypedDict

from airflow.models import BaseOperator

from airflow_provider_yandex_admetrica.hooks.loki import LokiClient
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    DATE_FORMAT,
    DEFAULT_LIMIT,
    DEFAULT_REQUEST_DELAY,
    AdmetricaHook,
    check_date,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

#: Directory segments under the advertiser that hold the statistics of a day.
STATS_PARTS = ("stats",)

#: Directory segments under the advertiser that hold the campaign dictionary.
#: The kind of dictionary names the last segment, so another dictionary lands
#: beside this one instead of mixing into it.
DICT_CAMPAIGNS_PARTS = ("dict", "campaigns")

#: Characters allowed in a path segment built from an identifier; everything
#: else becomes an underscore, so a run id carrying a timestamp with colons and
#: a plus sign still names a directory on every filesystem.
_UNSAFE_SEGMENT_RE = re.compile(r"[^\w-]")

#: Characters of the digest that follows an identifier in a path segment.
_DIGEST_LENGTH = 8

#: Bytes one directory name may take.  ``NAME_MAX`` is 255 on ext4, XFS, APFS
#: and every other filesystem an Airflow worker is likely to write to, and it
#: counts bytes rather than characters.
_SEGMENT_BYTES = 255


def _fit_bytes(text: str, limit: int) -> str:
    """Return *text* cut to *limit* bytes of UTF-8, never mid-character."""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[:limit].decode("utf-8", "ignore")


def id_segment(identifier: str) -> str:
    """Return the directory segment naming *identifier*, one segment per value.

    The readable half is the identifier with every character a directory name
    may not hold replaced by an underscore; the digest half is the first
    :data:`_DIGEST_LENGTH` characters of the SHA-1 of the identifier as it
    arrived.  The readable half is what a person looks for when opening the
    directory during an export, and the digest is what keeps two identifiers
    apart once the substitution has made them look alike — a run triggered as
    ``manual:a`` and one triggered as ``manual/a`` read the same after it, and
    only the digest tells the two exports apart.

    The readable half is cut to whatever :data:`_SEGMENT_BYTES` leaves after the
    digest, so the segment names a directory on every filesystem.  Airflow holds
    a ``dag_id`` and a ``run_id`` of up to 250 characters, and a character
    outside ASCII takes more than one byte, so both reach past the limit on
    their own.  Cutting costs nothing: the digest is taken from the whole
    identifier, so two that share a prefix long enough to survive the cut are
    still told apart by it.
    """
    safe = _UNSAFE_SEGMENT_RE.sub("_", identifier)
    digest = hashlib.sha1(identifier.encode("utf-8")).hexdigest()[:_DIGEST_LENGTH]
    return f"{_fit_bytes(safe, _SEGMENT_BYTES - _DIGEST_LENGTH - 1)}-{digest}"


def _campaign_segment(campaign_id: int) -> str:
    """Return the file name segment naming *campaign_id*.

    A campaign is a positive whole number, and the segment is that number
    written out.  The shape is checked rather than coerced: read through
    ``int``, both ``1.9`` and ``"0001"`` name the file of campaign ``1``, and a
    value that is no campaign at all ends up sharing a file with one that is.
    A boolean is an ``int`` in Python and no campaign, so the type is refused
    before the value is looked at.
    """
    if isinstance(campaign_id, bool) or not isinstance(campaign_id, int):
        raise TypeError(f"campaign id must be an integer, got {campaign_id!r}")
    if campaign_id <= 0:
        raise ValueError(f"campaign id must be positive, got {campaign_id!r}")
    return str(campaign_id)


class ExportRecord(TypedDict):
    """One file this operator wrote, described for the tasks downstream.

    ``advertiser_id`` travels with the record because the DAG builds the S3 key
    and the table name from it and has nowhere else to read it: the advertiser
    is named in the connection, which only the hook opens.

    ``campaign_id`` names the campaign whose rows the file holds, and it is what
    lets an address reach one campaign of one day rather than the whole day.
    The dictionary is a snapshot of the cabinet and belongs to no single
    campaign, so its record carries ``None``.  The key is spelled out on both
    kinds because this ``TypedDict`` is total: a DAG walking a mixed list and
    reading ``record["campaign_id"]`` would otherwise meet a ``KeyError`` on the
    dictionary, and only on a live run.
    """

    kind: str
    date: str
    path: str
    advertiser_id: int
    campaign_id: int | None


class YandexAdmetricaStatsOperator(BaseOperator):
    """Collect one day of statistics for every campaign of one advertiser.

    A task is a day. The period is expanded by the DAG, which hands each day to
    a map index of its own: one day failing leaves the others alone, and
    re-running it is a clear of that map index.

    The output is JSONL, one record per line, because the set of columns is not
    known from the request — the groupings and metrics of a record are nested
    objects whose fields come from the answer and may differ between rows.

    The grain of an export is a day of one campaign.  The rows of the day are
    split by the campaign they belong to and written a file each, and the result
    holds one record per campaign, so every address downstream — the local file,
    the S3 key, the object in GCS, the table in BigQuery — reaches the one
    campaign it was built from.  That is what makes collecting part of a cabinet
    safe: what a re-export rewrites is exactly what it collected, and a task
    cannot reach the data of a campaign it never asked for.

    A campaign with no rows for the day writes no file and adds no record, and a
    day with no rows at all adds no statistics record whatsoever.  A copy
    exported earlier stays where it is, holding the last figures known for that
    campaign and day.

    ``include_undefined`` goes out as the flag the API spells it in: ``True``
    keeps the rows whose first grouping has no value, ``False`` asks the API to
    drop them.  ``None`` leaves the parameter out of the request altogether, so
    whatever the API defaults to decides.

    Beside the statistics the task exports the dictionary of campaigns, unless
    ``collect_dictionaries`` turns it off.  The dictionary is a snapshot of the
    day the export runs rather than of the day it reports on, because the
    management API answers with the state the campaigns are in right now.  It is
    addressed by that day alone: it describes the cabinet whole, so rewriting it
    whole is what a re-export of it means.
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
        include_undefined: bool | None = True,
        limit: int = DEFAULT_LIMIT,
        request_delay: float = DEFAULT_REQUEST_DELAY,
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
        campaign_id: int | None = None,
    ) -> str:
        """Return the local file for *date* under *parts* of this advertiser.

        Given a *campaign_id* the day becomes a directory and the campaign names
        the file inside it, so one file holds the rows of one campaign of one
        day.  Left out, the day names the file itself, which is what the
        dictionary wants: a snapshot of the whole cabinet answers to no campaign.

        The DAG and the run sit in the path so two exports of the same day never
        write the same file; both stay local, since the S3 key addresses a day
        and a campaign of an advertiser and nothing else.  It takes the two of
        them: Airflow holds a run id unique within its DAG and nothing wider,
        while one connection names one advertiser, so serving several
        advertisers means several DAGs — and they share ``base_dir`` unless each
        is given its own.  Two of them on the same schedule are handed the same
        ``scheduled__<logical_date>``, and a run directory named by that alone
        would be one directory two DAGs collect into and either one deletes.

        Both segments are built by :func:`id_segment`, which carries a digest of
        the identifier past the substitution that makes a directory name of it.

        The day is a template field a DAG parameter fills in, so every character
        outside the letters, digits, underscore and hyphen a directory name is
        allowed to hold becomes an underscore: a day written as ``../../etc``
        addresses a directory under the base directory, spelled oddly, rather
        than a file anywhere on the worker.  The substitution holds wherever the
        day lands, the segment naming a directory as much as the one naming a
        file.  The campaign asks for no substitution: :func:`_campaign_segment`
        holds it to a positive whole number and refuses everything else, so no
        character a substitution would have to reach ever gets into the segment.
        It arrives here from
        :func:`~airflow_provider_yandex_admetrica.hooks.yandex_admetrica._campaign_record`,
        which reads it as that same shape, and the refusal is what keeps the two
        ends of that agreement from drifting apart.
        """
        safe_date = _UNSAFE_SEGMENT_RE.sub("_", date)
        tail = (
            (safe_date, f"{_campaign_segment(campaign_id)}.json")
            if campaign_id is not None
            else (f"{safe_date}.json",)
        )
        return os.path.join(
            self.base_dir,
            id_segment(self.dag_id),
            id_segment(run_id),
            str(advertiser_id),
            *parts,
            *tail,
        )

    def _write(self, records: Sequence[dict], path: str) -> None:
        """Write *records* to *path* as JSONL, one object per line.

        ``ensure_ascii=False`` keeps placement and campaign names readable in
        the file itself; the encoding is UTF-8 either way.

        The lines go to a temporary file in the same directory and the finished
        file is moved onto *path* in one step.  The campaign dictionary of a run
        has one path for every map index of it, so two days running at once
        write one file; a move puts the whole of one of them there, where a
        shared handle would leave the truncated middle of both for the upload to
        find.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, staged = tempfile.mkstemp(
            dir=os.path.dirname(path), prefix=os.path.basename(path), suffix=".part"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for row in records:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            os.replace(staged, path)
        except BaseException:
            # The staged file carries however much of the export got written,
            # so it goes rather than being left behind for a reader that walks
            # the directory.
            if os.path.exists(staged):
                os.unlink(staged)
            raise

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

    def _snapshot_date(self, context) -> str:
        """Return the day this export runs, which dates the campaign snapshot.

        The day is taken from the start of the DAG run, so every map index of
        one run names the same day: a period spread over map indices that runs
        across midnight still produces one snapshot in one partition, instead of
        two halves of a run disagreeing about which day they describe.
        """
        started = getattr(context.get("dag_run"), "start_date", None) or datetime.now(
            _timezone.utc
        )
        return started.strftime(DATE_FORMAT)

    def _export_campaigns(
        self,
        hook: AdmetricaHook,
        run_id: str,
        advertiser_id: int,
        snapshot_date: str,
    ) -> ExportRecord | None:
        """Write the campaign dictionary of *snapshot_date*, or write nothing.

        This is the one place ``snapshot_date`` is put on a record.  The same
        day names the file and is reported as the date of the result, so the
        column of a row, the key it is loaded from and the partition decorator
        it is loaded into always name one day: were they to disagree, the rows
        would carry a day next to the partition they are written to and the load
        would be rejected whole.

        The fields of the API keep their own wording, so the dictionary
        describes the campaign the API knows and nothing renames it on the way.
        """
        campaigns = hook.get_campaigns()
        if not campaigns:
            self.log.info(
                "AdMetrica lists no campaigns for advertiser %s; "
                "no dictionary file written.",
                advertiser_id,
            )
            return None

        records = [
            {"snapshot_date": snapshot_date, **campaign} for campaign in campaigns
        ]
        path = self._build_path(
            run_id, advertiser_id, DICT_CAMPAIGNS_PARTS, snapshot_date
        )
        self._write(records, path)
        return ExportRecord(
            kind="dict",
            date=snapshot_date,
            path=path,
            advertiser_id=advertiser_id,
            campaign_id=None,
        )

    def execute(self, context) -> list[ExportRecord]:
        # Before the hook is built, because the day is also what names the file
        # and the partition: a value that is not a day is refused by name here
        # rather than writing a file named after whatever the template held.
        check_date(self.date)
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
        # The hook answers with the day whole, because a row is a row whatever
        # campaign it came from.  Turning rows into files is the operator's
        # business and a file is what carries an address, so the split happens
        # here.  Every row names a campaign — the hook reads ``campaign_id`` as a
        # positive whole number for each of them and fails the export over one it
        # could not — so the key is taken outright rather than defended against.
        # A dictionary keeps insertion order, so the campaigns come out in the
        # order the cabinet lists them in.
        by_campaign: dict[int, list[dict]] = {}
        for row in rows:
            by_campaign.setdefault(row["campaign_id"], []).append(row)

        if by_campaign:
            for campaign_id, campaign_rows in by_campaign.items():
                path = self._build_path(
                    run_id, advertiser_id, STATS_PARTS, self.date, campaign_id
                )
                self._write(campaign_rows, path)
                result.append(
                    ExportRecord(
                        kind="stats",
                        date=self.date,
                        path=path,
                        advertiser_id=advertiser_id,
                        campaign_id=campaign_id,
                    )
                )
            # A file per campaign makes "does this day hold the campaigns it
            # should" a question about the export, and the task log is where it
            # is answered: the line names every campaign a file was written for.
            self.log.info(
                "Wrote %s rows of %s campaigns for advertiser %s on %s: %s.",
                len(rows),
                len(by_campaign),
                advertiser_id,
                self.date,
                ", ".join(str(campaign_id) for campaign_id in by_campaign),
            )
        else:
            self.log.info(
                "AdMetrica returned no rows for advertiser %s on %s; no file written.",
                advertiser_id,
                self.date,
            )

        if self.collect_dictionaries:
            record = self._export_campaigns(
                hook, run_id, advertiser_id, self._snapshot_date(context)
            )
            if record is not None:
                result.append(record)

        return result
