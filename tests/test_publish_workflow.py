"""Tests for the PyPI publish workflow.

The workflow runs in GitHub Actions and fails loudly there when it contradicts
itself, so what is checked here is only what would drift silently: the Python
matrix agreeing with the classifiers in `pyproject.toml`, and the full history
in `checkout`, without which `setuptools-scm` sees no tag and builds a package
under the wrong version.
"""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yml"
PYPROJECT = ROOT / "pyproject.toml"

#: What this file checks belongs to the repository rather than to the package,
#: so the workflow it reads is not part of the distribution.  Run from an
#: unpacked sdist there is nothing here to check, and saying so is the answer.
pytestmark = pytest.mark.skipif(
    not WORKFLOW.exists(),
    reason="the publish workflow ships with the repository, not with the package",
)


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _checkout(job: dict) -> dict:
    for step in job["steps"]:
        if str(step.get("uses", "")).startswith("actions/checkout@"):
            return step
    raise AssertionError("the job has no actions/checkout step")


def test_matrix_matches_pyproject_classifiers():
    versions = _workflow()["jobs"]["test"]["strategy"]["matrix"]["python-version"]
    classifiers = re.findall(
        r'"Programming Language :: Python :: (\d+\.\d+)"',
        PYPROJECT.read_text(encoding="utf-8"),
    )
    assert sorted(versions) == sorted(classifiers)


@pytest.mark.parametrize("job_name", ["test", "publish"])
def test_checkout_fetches_full_history(job_name):
    # setuptools-scm derives the version from tags, and a shallow copy of the
    # history carries none.
    assert _checkout(_workflow()["jobs"][job_name])["with"]["fetch-depth"] == 0
