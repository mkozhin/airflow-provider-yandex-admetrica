"""Suite-wide guards that keep the tests fast and self-contained."""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def no_real_sleep():
    """Keep every test from spending a pause in real time.

    Retries back off for seconds at a time, so a test that lets a pause through
    stalls the suite instead of failing it. Tests that assert on the pauses
    patch ``time.sleep`` themselves, over this one.
    """
    with patch("time.sleep"):
        yield
