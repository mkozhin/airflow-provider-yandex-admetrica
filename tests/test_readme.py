"""Tests that hold both READMEs to the code they describe.

A README is the package's public contract on PyPI, so what it claims is checked
against the operator's own signature, the naming rule the hook applies and the
code blocks it offers to copy. Prose and layout are the writer's; only the
claims that name something in the code are pinned here.

A requirement stated in prose is held the same way: the floor on the google
provider is a claim the examples, the READMEs and the `dev` extra all make, and
it holds only while every one of them names the same version.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import re
from datetime import datetime
from pathlib import Path

import pytest
from airflow.models import DAG, BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator

from airflow_provider_yandex_admetrica.hooks.loki import LokiClient
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _BACKOFF_DELAYS,
    _QUERY_ERROR_DELAYS,
    _RESERVED_PARAMS,
    AdmetricaHook,
    _normalize_name,
)
from airflow_provider_yandex_admetrica.operators.stats import (
    DICT_CAMPAIGNS_PARTS,
    STATS_PARTS,
    YandexAdmetricaStatsOperator,
    id_segment,
)

_ROOT = Path(__file__).resolve().parent.parent
_READMES = {"README.md": _ROOT / "README.md", "README_RU.md": _ROOT / "README_RU.md"}

#: A fenced block and the language it names.
_FENCE_RE = re.compile(r"^```(\w*)\n(.*?)^```", re.MULTILINE | re.DOTALL)

#: A row of a markdown table, split on the pipes that fence its cells.
_TABLE_ROW_RE = re.compile(r"^\|(.+)\|\s*$", re.MULTILINE)

#: The classes a README offers to construct, by the name it writes them under.
_DOCUMENTED_CALLABLES = {
    "YandexAdmetricaStatsOperator": YandexAdmetricaStatsOperator,
    "AdmetricaHook": AdmetricaHook,
    "LokiClient": LokiClient,
}


#: The statistics file the layout section of a README is written about: one
#: campaign of one day, with the identifiers the printed addresses spell out.
_LAYOUT_RECORD = {
    "kind": "stats",
    "date": "2026-08-20",
    "path": "/tmp/local.json",
    "advertiser_id": 17004,
    "campaign_id": 123456,
}

#: The snapshot of the campaign dictionary of the same run, dated the day the
#: export runs rather than the day it reports on.
_LAYOUT_DICT_RECORD = {
    "kind": "dict",
    "date": "2026-08-21",
    "path": "/tmp/dict.json",
    "advertiser_id": 17004,
    "campaign_id": None,
}

#: The run whose addresses are compared, and the DAG that holds it.
_LAYOUT_RUN_ID = "manual__2026-08-21T00:00:00+00:00"
_LAYOUT_DAG_ID = "readme_layout"

#: The example that builds the addresses of every destination a README names.
_LAYOUT_EXAMPLE = "examples.admetrica_to_bq_and_s3_dag"

#: The google provider release that carries the two ways the examples create a
#: table, and with it the floor every place naming the requirement has to spell.
_GOOGLE_FLOOR = "apache-airflow-providers-google>=14.0.0"

#: The files that name the floor: the extra that installs it, the prose a reader
#: of either BigQuery example meets before copying it, and the document that
#: carries the decision behind it.
_FLOOR_IS_WRITTEN_IN = (
    "pyproject.toml",
    "README.md",
    "README_RU.md",
    "CONTEXT.md",
    "examples/admetrica_to_bigquery_dag.py",
    "examples/admetrica_to_bq_and_s3_dag.py",
)


def _layout_operator() -> YandexAdmetricaStatsOperator:
    """Return the operator whose ``_build_path`` writes the local layout."""
    with DAG(dag_id=_LAYOUT_DAG_ID, start_date=datetime(2026, 8, 1), schedule=None):
        return YandexAdmetricaStatsOperator(
            task_id="collect",
            date=_LAYOUT_RECORD["date"],
            dimensions=["am:e:placement"],
            metrics=["am:e:renders"],
            base_dir="/tmp/yandex_admetrica",
        )


def _layout_segments() -> dict[str, str]:
    """Return the values a printed address leaves as placeholders."""
    return {
        "base_dir": "/tmp/yandex_admetrica",
        "dag_segment": id_segment(_LAYOUT_DAG_ID),
        "run_segment": id_segment(_LAYOUT_RUN_ID),
        "advertiser_id": _LAYOUT_RECORD["advertiser_id"],
        "date": _LAYOUT_RECORD["date"],
        "campaign_id": _LAYOUT_RECORD["campaign_id"],
        "snapshot_date": _LAYOUT_DICT_RECORD["date"],
    }


def _text(name: str) -> str:
    return _READMES[name].read_text(encoding="utf-8")


def _python_blocks(text: str) -> list[str]:
    return [body for language, body in _FENCE_RE.findall(text) if language == "python"]


def _cells(row: str) -> list[str]:
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def _table_after(text: str, heading: str) -> list[list[str]]:
    """Return the rows of the first table under *heading*, header and rule aside."""
    start = text.index(f"### {heading}")
    section = text[start : text.index("\n#", start + 1)]
    rows = [_cells(row) for row in _TABLE_ROW_RE.findall(section)]
    return [row for row in rows if not set("".join(row)) <= set("-: ")][1:]


def _accepted_keywords(target) -> set[str]:
    """Return the keyword names *target* accepts, its base's included."""
    names = set(inspect.signature(target).parameters)
    if isinstance(target, type) and issubclass(target, BaseOperator):
        names |= set(inspect.signature(BaseOperator).parameters)
    return names


@pytest.fixture(params=sorted(_READMES))
def readme(request):
    """Run the check over each README in turn, named by its file."""
    return _text(request.param)


class TestCodeBlocks:
    """Every snippet a reader may copy is valid and calls things as they are."""

    def test_python_blocks_are_present(self, readme):
        assert _python_blocks(readme)

    def test_python_blocks_parse(self, readme):
        for block in _python_blocks(readme):
            ast.parse(block)

    def test_documented_calls_use_accepted_keywords(self, readme):
        for block in _python_blocks(readme):
            for node in ast.walk(ast.parse(block)):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                    continue
                target = _DOCUMENTED_CALLABLES.get(node.func.id)
                if target is None:
                    continue
                passed = {kw.arg for kw in node.keywords if kw.arg is not None}
                assert passed <= _accepted_keywords(target), node.func.id

    def test_quick_start_dag_builds(self, readme):
        """The DAG snippets run as written, decorator call included."""
        built = 0
        for block in _python_blocks(readme):
            if "@dag" not in block:
                continue
            exec(compile(block, "<readme>", "exec"), {})
            built += 1
        assert built == 1


class TestOperatorParameters:
    """The parameter table names the operator's own parameters and defaults."""

    def _documented(self, readme: str, heading: str) -> dict[str, str]:
        return {row[0].strip("`"): row[1] for row in _table_after(readme, heading)}

    @pytest.mark.parametrize(
        ("name", "heading"),
        [("README.md", "Operator parameters"), ("README_RU.md", "Параметры оператора")],
    )
    def test_table_names_every_parameter(self, name, heading):
        documented = self._documented(_text(name), heading)
        expected = set(inspect.signature(YandexAdmetricaStatsOperator).parameters) - {"kwargs"}
        assert set(documented) == expected

    @pytest.mark.parametrize(
        ("name", "heading"),
        [("README.md", "Reserved parameters"), ("README_RU.md", "Зарезервированные параметры")],
    )
    def test_reserved_parameters_are_the_refused_ones(self, name, heading):
        """The names the README calls reserved are the ones the hook refuses."""
        paragraph = _text(name).split(f"#### {heading}")[1].split("\n\n")[1]
        listed = set(re.findall(r"`(\w+)`", paragraph.split(";")[0])) - {"extra_params"}
        assert listed == set(_RESERVED_PARAMS)


class TestNamingTable:
    """The requested-name-to-record-key table is the rule the hook applies."""

    def test_documented_keys_match_the_normalizer(self, readme):
        rows = [_cells(row) for row in _TABLE_ROW_RE.findall(readme)]
        checked = 0
        for row in rows:
            if len(row) != 2 or not row[0].startswith("`am:e:"):
                continue
            requested, key = row[0], row[1].strip("`")
            name = requested.strip("`").split("`")[0]
            extra = {"goal_id": 12345} if "<goal_id>" in name else None
            assert _normalize_name(name, extra) == key, name
            checked += 1
        assert checked >= 8


class TestRetryPolicy:
    """The ladders and the statuses a README spells out are the ones the code walks."""

    @pytest.mark.parametrize("ladder", [_BACKOFF_DELAYS, _QUERY_ERROR_DELAYS])
    def test_a_documented_ladder_is_the_one_the_code_walks(self, readme, ladder):
        assert " / ".join(str(rung) for rung in ladder) in readme

    def test_the_statuses_that_name_the_token_are_documented_as_one_pair(self, readme):
        assert re.search(r"401 (or|или) 403", readme)


class TestStructure:
    """Each file points at the other and at the documentation it stands on."""

    def test_each_file_links_to_the_other(self):
        assert "README_RU.md" in _text("README.md")
        assert "README.md" in _text("README_RU.md")

    def test_documentation_links_are_the_yandex_ones(self, readme):
        assert "https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all" in readme
        assert "https://yandex.ru/dev/admetrica/doc/ru/authorization" in readme


class TestTheLayoutIsTheOneTheCodeBuilds:
    """Every address the layout section prints is reproduced by the code.

    The section is the branch's central claim — a campaign reaches an address of
    its own in every destination — and prose cannot be trusted to have followed
    the functions that build them, so each printed line is compared against the
    string its own builder returns for one synthetic record.
    """

    def test_the_local_path_of_a_campaign(self, readme):
        documented = (
            "{base_dir}/{dag_segment}/{run_segment}/{advertiser_id}"
            "/stats/{date}/{campaign_id}.json"
        )
        assert documented in readme
        built = _layout_operator()._build_path(
            _LAYOUT_RUN_ID,
            _LAYOUT_RECORD["advertiser_id"],
            STATS_PARTS,
            _LAYOUT_RECORD["date"],
            _LAYOUT_RECORD["campaign_id"],
        )
        assert built == documented.format(**_layout_segments())

    def test_the_local_path_of_the_dictionary(self, readme):
        documented = (
            "{base_dir}/{dag_segment}/{run_segment}/{advertiser_id}"
            "/dict/campaigns/{snapshot_date}.json"
        )
        assert documented in readme
        built = _layout_operator()._build_path(
            _LAYOUT_RUN_ID,
            _LAYOUT_RECORD["advertiser_id"],
            DICT_CAMPAIGNS_PARTS,
            _LAYOUT_DICT_RECORD["date"],
        )
        assert built == documented.format(**_layout_segments())

    def test_the_s3_key_of_a_campaign(self, readme):
        documented = (
            "{S3_PREFIX}/{advertiser_id}/stats/_year=2026/_month=08/_day=20"
            "/_date=20260820/_campaign_id=123456/2026-08-20.json"
        )
        assert documented in readme
        module = importlib.import_module(_LAYOUT_EXAMPLE)
        assert module.s3_key(_LAYOUT_RECORD) == documented.format(
            S3_PREFIX=module.S3_PREFIX, advertiser_id=_LAYOUT_RECORD["advertiser_id"]
        )

    def test_the_s3_key_of_the_dictionary(self, readme):
        documented = (
            "{S3_PREFIX}/{advertiser_id}/dict/campaigns/_year=2026/_month=08/_day=21"
            "/_date=20260821/2026-08-21.json"
        )
        assert documented in readme
        module = importlib.import_module(_LAYOUT_EXAMPLE)
        assert module.s3_key(_LAYOUT_DICT_RECORD) == documented.format(
            S3_PREFIX=module.S3_PREFIX, advertiser_id=_LAYOUT_RECORD["advertiser_id"]
        )

    def test_the_gcs_object_of_a_campaign(self, readme):
        documented = (
            "{GCS_PREFIX}/{dag_segment}/{run_segment}/{advertiser_id}"
            "/stats/{date}/{campaign_id}.json"
        )
        assert documented in readme
        module = importlib.import_module(_LAYOUT_EXAMPLE)
        segments = {
            **_layout_segments(),
            "GCS_PREFIX": module.GCS_PREFIX,
            "dag_segment": id_segment(module.DAG_ID),
        }
        assert module.gcs_object(_LAYOUT_RECORD, _LAYOUT_RUN_ID) == documented.format(**segments)

    def test_the_gcs_object_of_the_dictionary(self, readme):
        documented = (
            "{GCS_PREFIX}/{dag_segment}/{run_segment}/{advertiser_id}"
            "/dict/campaigns/{snapshot_date}.json"
        )
        assert documented in readme
        module = importlib.import_module(_LAYOUT_EXAMPLE)
        segments = {
            **_layout_segments(),
            "GCS_PREFIX": module.GCS_PREFIX,
            "dag_segment": id_segment(module.DAG_ID),
        }
        built = module.gcs_object(_LAYOUT_DICT_RECORD, _LAYOUT_RUN_ID)
        assert built == documented.format(**segments)

    def test_the_bigquery_table_of_a_campaign(self, readme):
        documented = "stats_{advertiser_id}_{campaign_id}"
        assert f"`{documented}`" in readme
        module = importlib.import_module(_LAYOUT_EXAMPLE)
        table, _, partition = module.stats_table_partition(_LAYOUT_RECORD).partition("$")
        assert table == documented.format(**_layout_segments())
        assert partition == _LAYOUT_RECORD["date"].replace("-", "")


class TestGoogleProviderFloor:
    """The examples create the table every partition decorator addresses: the day's
    load through a hook method, the dictionary through an operator. The earliest
    release on PyPI carrying either is google provider 14.0.0, while the constraint
    set of Airflow 2.9.1 — the oldest Airflow this package supports — pins 10.17.0,
    where both are absent. The floor is therefore a requirement of the examples
    rather than a preference, and it holds only while every place that states it
    agrees."""

    def test_the_hook_carries_the_method_the_examples_call(self):
        assert hasattr(BigQueryHook, "create_table")

    def test_the_provider_carries_the_operator_the_examples_declare(self):
        """The dictionary's table is created declaratively, by this operator."""
        parameters = inspect.signature(BigQueryCreateTableOperator.__init__).parameters
        assert {"dataset_id", "table_id", "table_resource", "if_exists"} <= set(parameters)

    @pytest.mark.parametrize("relative", _FLOOR_IS_WRITTEN_IN)
    def test_every_place_that_states_the_requirement_names_the_same_floor(self, relative):
        assert _GOOGLE_FLOOR in (_ROOT / relative).read_text(encoding="utf-8")
