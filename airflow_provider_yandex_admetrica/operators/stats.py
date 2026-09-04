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
from airflow.utils.helpers import render_template_to_string

from airflow_provider_yandex_admetrica.campaign_selection import (
    ACTIVE_STATUS,
    SCOPE_ACTIVE,
    SCOPE_ALL,
    CampaignSelection,
    check_one_of,
)
from airflow_provider_yandex_admetrica.hooks.loki import LokiClient
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    DATE_FORMAT,
    DEFAULT_LIMIT,
    DEFAULT_REQUEST_DELAY,
    AdmetricaHook,
    check_date,
    check_extra_params,
    check_report_limits,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

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

#: Name a campaign the cabinet does not list and the task says so and goes on.
#: The default: a campaign deleted in the interface is gone from the list for
#: good, its figures cannot be asked for at all, and a schedule turning red over
#: that would be reporting on the cabinet rather than on the export.
MISSING_WARN = "warn"

#: Name a campaign the cabinet does not list and the task refuses to run.  What
#: a list typed by hand wants, where a mistyped campaign is a mistake rather
#: than a fact about the cabinet.
MISSING_FAIL = "fail"

#: The policies ``on_missing_campaign`` accepts, in the spelling a refusal
#: offers back.  Held to that vocabulary by
#: :func:`~airflow_provider_yandex_admetrica.campaign_selection.check_one_of`,
#: the same function ``campaign_scope`` is held by: both arrive rendered from a
#: run form, and both are refused before a hook exists to mask what they held.
_MISSING_POLICIES = (MISSING_WARN, MISSING_FAIL)


def _quoted(text: str) -> str:
    """Return *text* as one quoted value of a message, punctuation and all.

    A message of this task counts what it names and quotes each value, so the
    quotation marks are structure: they say where one campaign ends and the next
    begins, and how many of them a count is a count of.  A campaign name is
    written by whoever fills in the run form and a status by the API, and either
    may hold a quotation mark of its own — ``x"; 9 id(s): "123`` is a name a
    cabinet may carry.  A backslash goes before every quotation mark and before
    every backslash, so a mark inside a value never reads as one of the marks
    around it and a value can only ever be read as the single value it is.

    What the escaping answers for is the punctuation.  A character that reorders
    what is drawn without being drawn itself would forge the same structure
    without writing a single quotation mark, and it is gone before this runs:
    :func:`_safe_words` passes every value through the hook's gate first.
    """
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _safe_words(hook: AdmetricaHook, value: object) -> str:
    """Return *value* worded for a message of this task, or described.

    A campaign name is typed into a run form and a status is written by the API:
    both are somebody else's words, both are read out of a task log, and under
    :data:`MISSING_FAIL` a name is read out of a traceback and the UI as well.
    So both pass the hook's gate, which flattens them to one line of characters
    that each take a visible place of their own, replaces the token this hook
    read and bounds the length.  The flattening is what the quoting below rests
    on: it leaves no bidirectional override in the value, so what a value can do
    to the structure of the line is bounded by the escaping alone.  The gate
    lives on the hook because the token does, and by this point the hook is
    built — which is why a value refused here is described where the parsing of
    a selection, running before any hook exists, describes everything.

    A value the gate lets out is quoted by :func:`_quoted`, so that a name of
    several words reads as one thing in a list of them.  A value it refuses
    travels as its length alone.
    """
    text = hook.safe_text(value)
    if text is None:
        return (
            f"<{len(value)} character(s)>"
            if type(value) is str
            else f"<a {type(value).__name__}>"
        )
    return _quoted(text)


def _safe_id(hook: AdmetricaHook, campaign_id: int) -> str:
    """Return a campaign id worded for a message of this task, or described.

    ``campaign_ids`` is filled in from a run form, and a value that reaches this
    function has been proved to be a positive whole number and nothing more: a
    PIN, a one-time code and a numeric token share that shape.  So an id goes
    out through the same gate a campaign name does, and the one credential this
    provider owns — the token of the connection the hook read — comes back
    masked instead of quoted.

    A number too long to be written out is named by that: CPython renders an
    integer only up to ``sys.get_int_max_str_digits`` digits, and a message
    about a campaign is no place for the interpreter's sentence about a limit.
    """
    try:
        written = str(campaign_id)
    except ValueError:
        return "<a number of more digits than can be written>"
    return _safe_words(hook, written)


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
    """Collect one day of statistics for the campaigns of one advertiser.

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

    Which campaigns the day is walked over is what ``campaign_scope``,
    ``campaign_ids`` and ``campaign_names`` decide, and a campaign costs a
    request whether or not it has anything to say: a cabinet is mostly history,
    so the default walks the campaigns the cabinet calls active.
    ``campaign_scope="all"`` walks the whole list, which is what a backfill of a
    past period asks for.  Naming campaigns by id or by name walks exactly those,
    the archived among them, and the scope is then not consulted at all.  All
    three, and ``on_missing_campaign`` below, are template fields, so a schedule
    collects the running campaigns while a run form reaches whichever were asked
    for.  A template renders here as the characters it rendered to, whatever the
    DAG renders its own templates as, so a campaign named in a run form is read
    in the spelling it was typed in; :meth:`_render` says what that is worth.

    What is cut is the walk and nothing else.  The dictionary stays a snapshot of
    the whole cabinet however narrow the walk, because it is what later says
    which campaign was skipped and when it left the cabinet, and the campaign
    list itself is fetched whole and checked against the total the API declares.

    A campaign named by id or by name that the cabinet does not list is reported
    by ``on_missing_campaign``: ``"warn"`` names it in the log and collects the
    rest, ``"fail"`` refuses the task.  Warning is the default;
    :data:`MISSING_WARN` and :data:`MISSING_FAIL` say which cabinet each suits.

    Beside the statistics the task exports the dictionary of campaigns, unless
    ``collect_dictionaries`` turns it off.  The dictionary is a snapshot of the
    day the export runs rather than of the day it reports on, because the
    management API answers with the state the campaigns are in right now.  It is
    addressed by that day alone: it describes the cabinet whole, so rewriting it
    whole is what a re-export of it means.
    """

    template_fields = (
        "date",
        "admetrica_conn_id",
        "loki_conn_id",
        "base_dir",
        "campaign_scope",
        "campaign_ids",
        "campaign_names",
        "on_missing_campaign",
    )
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
        campaign_scope: str = SCOPE_ACTIVE,
        campaign_ids: str | int | Collection[int | str] | None = None,
        campaign_names: str | Collection[str] | None = None,
        on_missing_campaign: str = MISSING_WARN,
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
        self.campaign_scope = campaign_scope
        self.campaign_ids = campaign_ids
        self.campaign_names = campaign_names
        self.on_missing_campaign = on_missing_campaign
        self.loki_conn_id = loki_conn_id

    def get_template_env(self, dag=None):
        """Build the Jinja environment this operator's fields render in.

        A DAG declared with ``render_template_as_native_obj=True`` renders in a
        ``NativeEnvironment``, whose templates yield the Python objects the
        expression evaluated to rather than the characters they are written as:
        a list parameter yields the list itself.  Asking the DAG for a sandboxed
        environment gives back templates that yield characters, whatever the DAG
        says, which is what :meth:`_render` joins into the rendered value.  The
        two together are one decision — the fields of this operator render as
        text — and the environment is the half that makes the text exist.

        With no DAG around the base class already builds a sandboxed
        environment, so an operator rendered on its own is answered by
        ``super()``.
        """
        if dag is None:
            dag = self.get_dag()
        if dag is not None:
            return dag.get_template_env(force_sandboxed=True)
        return super().get_template_env(dag=dag)

    def _render(self, template, context, dag=None):
        """Render one of this operator's templates as text, never as an object.

        Every field this operator templates is text: a day, a connection id, a
        directory, a scope, a policy, and the campaigns a run form names.  A DAG
        declared with ``render_template_as_native_obj=True`` finishes a render
        by reading the rendered characters as a Python literal, and a value that
        looks like one stops being the text that was typed: ``1_2`` filled into
        ``campaign_ids`` becomes the number twelve, ``+999`` becomes nine
        hundred and ninety-nine, and ``None`` filled into ``campaign_names``
        becomes no name at all.  Each of those is a selection quietly other than
        the one that was asked for — a different campaign collected, a name
        dropped and the scope back in charge — and none of them can be told
        apart afterwards, because what was typed is gone by then.

        Rendering as text keeps the characters, and text is what
        :class:`~airflow_provider_yandex_admetrica.campaign_selection.CampaignSelection`
        reads: a list written out is read as a list, an id is held to the one
        spelling it is written in, and a name stays a name.  The characters
        being joined here are the ones :meth:`get_template_env` arranges for.

        This decides how the fields of this operator are rendered and nothing
        else: the DAG renders every other task as it says it does.
        """
        return render_template_to_string(template, context)

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
        campaigns: Sequence[dict],
        run_id: str,
        advertiser_id: int,
        snapshot_date: str,
    ) -> ExportRecord | None:
        """Write the campaign dictionary of *snapshot_date*, or write nothing.

        *campaigns* is the cabinet whole, as :meth:`AdmetricaHook.get_campaigns`
        answered it.  The dictionary describes the cabinet rather than the
        export, so it is written whole however narrow the walk of the day was:
        it is what later says which campaign a day skipped and when that
        campaign left the cabinet.

        This is the one place ``snapshot_date`` is put on a record.  The same
        day names the file and is reported as the date of the result, so the
        column of a row, the key it is loaded from and the partition decorator
        it is loaded into always name one day: were they to disagree, the rows
        would carry a day next to the partition they are written to and the load
        would be rejected whole.

        The fields of the API keep their own wording, so the dictionary
        describes the campaign the API knows and nothing renames it on the way.
        """
        if not campaigns:
            self.log.info(
                "Advertiser %s lists no campaign at all; no dictionary file written.",
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

    def _answer_for_missing(
        self,
        hook: AdmetricaHook,
        selection: CampaignSelection,
        campaigns: Sequence[dict],
        policy: str,
    ) -> None:
        """Answer for the campaigns named that the cabinet does not list.

        *policy* decides which answer that is, and :data:`MISSING_WARN` and
        :data:`MISSING_FAIL` say which cabinet each of them suits.  Either
        answer comes before a single day of statistics is asked for.

        Ids and names are reported apart because they are different mistakes,
        and the ids are sorted so that two runs of one selection read alike.
        Both are counted before either is worded, so that the line stays worth
        reading even where every value in it came back masked: how many of a
        selection the cabinet does not know is the number an operator acts on.

        Both travel through the hook's gate, an id as much as a name.  What is
        typed into ``campaign_ids`` is a number only after this provider has
        agreed it is one, and the shape of a number is also the shape of a PIN
        and of a numeric token.
        """
        missing_ids, missing_names = selection.missing(campaigns)
        if not missing_ids and not missing_names:
            return
        named = []
        if missing_ids:
            worded = [_safe_id(hook, i) for i in sorted(missing_ids)]
            named.append(f"{len(worded)} id(s): {', '.join(worded)}")
        if missing_names:
            worded = sorted(_safe_words(hook, name) for name in missing_names)
            named.append(f"{len(worded)} name(s): {', '.join(worded)}")
        message = (
            f"Advertiser {hook.advertiser_id} lists no campaign for "
            f"{'; '.join(named)}. A campaign deleted in the interface leaves "
            f"the list for good, and its figures cannot be asked for at all."
        )
        if policy == MISSING_FAIL:
            raise ValueError(message)
        self.log.warning("%s Collecting the campaigns that are listed.", message)

    def _report_selection(
        self,
        hook: AdmetricaHook,
        selection: CampaignSelection,
        campaigns: Sequence[dict],
    ) -> None:
        """Say which campaigns of the cabinet the day walks, and which it skips.

        A walk that quietly grew shorter is the risk this selection carries, so
        every run says how many campaigns were skipped and which statuses those
        campaigns carried.  The management API words that field ``"active"`` or
        ``"archived"``, and the statuses named here are what shows an answer
        departing from those two: a cabinet wording a running campaign
        otherwise would be collected short under the default.  They are named on
        the ordinary run, where most of the cabinet is archived history, as much
        as on the cabinet that matches nothing at all.

        An empty selection made by the scope alone is never a failure — an
        advertiser whose campaigns have all been archived is an ordinary
        advertiser — but it is always a warning, and the warning holds both
        readings up: the ordinary one, and an answer wording a running campaign
        outside the two documented values, which is the one that costs a day of
        data.  It quotes the statuses it did meet, which tell the two apart, and
        says what collects the day anyway.  An advertiser with no campaigns at
        all is a case of its own: nothing was narrowed and no status was read,
        so the line says the list itself was empty.
        """
        if not campaigns:
            self.log.warning(
                "Advertiser %s lists no campaign at all; no statistics requested.",
                hook.advertiser_id,
            )
            return

        selected, skipped = selection.partition(campaigns)

        if selection.is_explicit:
            self.log.info(
                "Collecting statistics for %s of %s campaigns (named explicitly, "
                "%s skipped; campaign_scope=%s not applied).",
                len(selected),
                len(campaigns),
                len(skipped),
                selection.scope,
            )
            return

        statuses = ", ".join(
            sorted({_safe_words(hook, campaign.get("status")) for campaign in skipped})
        )
        if not selected:
            self.log.warning(
                "No campaign of the %s listed matches campaign_scope=%s "
                "(statuses seen: %s); no statistics requested. Either every "
                "campaign of the advertiser is over, or the answer words a "
                'running campaign as something other than "%s": compare the '
                "statuses named here with what the interface shows, and collect "
                "the day with campaign_scope=%s or by naming the campaigns in "
                "campaign_ids.",
                len(campaigns),
                selection.scope,
                statuses,
                ACTIVE_STATUS,
                SCOPE_ALL,
            )
            return
        self.log.info(
            "Collecting statistics for %s of %s campaigns (scope=%s, %s skipped%s).",
            len(selected),
            len(campaigns),
            selection.scope,
            len(skipped),
            f"; statuses skipped: {statuses}" if skipped else "",
        )

    def execute(self, context) -> list[ExportRecord]:
        # Before the hook is built, because the day is also what names the file
        # and the partition: a value that is not a day is refused by name here
        # rather than writing a file named after whatever the template held.
        check_date(self.date)
        # The campaign list is read before the day is collected, so a report
        # configured wrongly would otherwise walk the management API before
        # anything refused it. Asked here, such a report costs no request at all,
        # which is what three documents of this provider promise of it.
        check_report_limits(self.dimensions, self.metrics, self.limit)
        check_extra_params(self.extra_params)
        selection = CampaignSelection.parse(
            scope=self.campaign_scope,
            ids=self.campaign_ids,
            names=self.campaign_names,
        )
        policy = check_one_of(
            "on_missing_campaign", self.on_missing_campaign, _MISSING_POLICIES
        )
        hook = AdmetricaHook(
            admetrica_conn_id=self.admetrica_conn_id,
            loki=self._build_loki_client(context),
            request_delay=self.request_delay,
            limit=self.limit,
        )
        advertiser_id = hook.advertiser_id
        run_id = context["run_id"]
        result: list[ExportRecord] = []

        # The list is read whatever ``collect_dictionaries`` says. The policy and
        # the line naming what was skipped describe the walk of the day, not the
        # dictionary, and reading the list only for the dictionary would leave an
        # export with it switched off with no account of a narrowed walk at all.
        campaigns = hook.get_campaigns()
        self._answer_for_missing(hook, selection, campaigns, policy)
        # The line reports the same walk `get_stats` makes: the hook applies
        # this very selection to this very list — held for the life of the hook,
        # so the second reading of it is the first one's answer — and the rule
        # is a pure function of the two.
        self._report_selection(hook, selection, campaigns)

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
            selection=selection,
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
                campaigns, run_id, advertiser_id, self._snapshot_date(context)
            )
            if record is not None:
                result.append(record)

        return result
