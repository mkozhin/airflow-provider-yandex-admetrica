"""Tests that hold both READMEs to the code they describe.

A README is the package's public contract on PyPI, so what it claims is checked
against the operator's own signature, the naming rule the hook applies and the
code blocks it offers to copy. Prose and layout are the writer's; only the
claims that name something in the code are pinned here.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

import pytest
from airflow.models import BaseOperator

from airflow_provider_yandex_admetrica.hooks.loki import LokiClient
from airflow_provider_yandex_admetrica.hooks.yandex_admetrica import (
    _AUTH_STATUSES,
    _BACKOFF_DELAYS,
    _QUERY_ERROR_DELAYS,
    _RESERVED_PARAMS,
    AdmetricaHook,
    _normalize_name,
)
from airflow_provider_yandex_admetrica.operators.stats import YandexAdmetricaStatsOperator

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
        assert _AUTH_STATUSES == frozenset({401, 403})
        assert re.search(r"401 (or|или) 403", readme)


class TestStructure:
    """Each file points at the other and at the documentation it stands on."""

    def test_each_file_links_to_the_other(self):
        assert "README_RU.md" in _text("README.md")
        assert "README.md" in _text("README_RU.md")

    def test_documentation_links_are_the_yandex_ones(self, readme):
        assert "https://yandex.ru/dev/admetrica/doc/ru/attrandmetr/dim_all" in readme
        assert "https://yandex.ru/dev/admetrica/doc/ru/authorization" in readme
