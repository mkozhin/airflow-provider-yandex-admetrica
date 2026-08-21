"""Проверки workflow публикации на PyPI.

Workflow исполняется только в GitHub Actions, поэтому здесь разбирается его YAML:
тесты сверяют триггер, состав job-ов и те настройки, без которых публикация ломается
молча — `fetch-depth: 0` для setuptools-scm и `environment: pypi` с `id-token: write`
для OIDC.
"""

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yml"
PYPROJECT = ROOT / "pyproject.toml"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _triggers(workflow: dict) -> dict:
    # YAML читает голое `on` как булев ключ True, поэтому подходят оба написания.
    return workflow.get("on", workflow.get(True))


def _steps(job: dict) -> list[dict]:
    return job["steps"]


def _checkout(job: dict) -> dict:
    for step in _steps(job):
        if str(step.get("uses", "")).startswith("actions/checkout@"):
            return step
    raise AssertionError("в job нет шага actions/checkout")


def _run_commands(job: dict) -> list[str]:
    return [step["run"] for step in _steps(job) if "run" in step]


def test_workflow_file_exists():
    assert WORKFLOW.is_file()


def test_workflow_is_valid_yaml():
    assert isinstance(_workflow(), dict)


def test_triggered_by_version_tag_push():
    triggers = _triggers(_workflow())
    assert list(triggers) == ["push"]
    assert triggers["push"]["tags"] == ["v*"]


def test_jobs_are_test_and_publish():
    assert set(_workflow()["jobs"]) == {"test", "publish"}


def test_test_job_matrix_covers_supported_pythons():
    job = _workflow()["jobs"]["test"]
    assert job["strategy"]["matrix"]["python-version"] == ["3.10", "3.11", "3.12"]


def test_matrix_matches_pyproject_classifiers():
    versions = _workflow()["jobs"]["test"]["strategy"]["matrix"]["python-version"]
    classifiers = re.findall(
        r'"Programming Language :: Python :: (\d+\.\d+)"',
        PYPROJECT.read_text(encoding="utf-8"),
    )
    assert sorted(versions) == sorted(classifiers)


def test_test_job_installs_dev_extras_and_runs_pytest():
    commands = _run_commands(_workflow()["jobs"]["test"])
    assert 'pip install -e ".[dev]"' in commands
    assert "pytest tests/ -v" in commands


def test_publish_job_waits_for_tests():
    assert _workflow()["jobs"]["publish"]["needs"] == "test"


def test_publish_job_uses_pypi_environment_and_oidc():
    job = _workflow()["jobs"]["publish"]
    assert job["environment"] == "pypi"
    assert job["permissions"]["id-token"] == "write"


def test_publish_job_builds_and_publishes():
    job = _workflow()["jobs"]["publish"]
    assert "python -m build" in _run_commands(job)
    uses = [step.get("uses") for step in _steps(job)]
    assert "pypa/gh-action-pypi-publish@release/v1" in uses


@pytest.mark.parametrize("job_name", ["test", "publish"])
def test_checkout_fetches_full_history(job_name):
    # setuptools-scm выводит версию из тегов, а мелкая копия истории их не содержит.
    assert _checkout(_workflow()["jobs"][job_name])["with"]["fetch-depth"] == 0
