"""Suite-wide guards that keep the tests fast and self-contained."""

import inspect
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


def _recording_method(real_hook, method: str, calls: list[dict]):
    """Return a stand-in for *method* that records the call it was handed.

    The call is bound to the signature of the real method first, so a keyword
    the provider would refuse fails the test here instead of reaching a live
    run green. The keywords go into *calls* before the refusal a test asked
    for, so what the stand-in was asked to do stays readable either way.
    """
    signature = inspect.signature(getattr(real_hook, method))

    def call(self, **kwargs):
        signature.bind(self, **kwargs)
        calls.append(kwargs)
        if type(self).fail_at.get(method) == len(calls):
            raise RuntimeError(f"{real_hook.__name__}.{method} refused call {len(calls)}")

    return call


@pytest.fixture
def recording_hook():
    """Return a factory of stand-in hooks held to the API of the real ones.

    The example DAGs construct their hooks inside the task, so what a test can
    reach is the class the module holds rather than an instance: the record
    lives on the class. The factory builds one class per call with lists of its
    own, so two tests share nothing.

    ``fail_at`` maps a method to the ordinal of the call that raises, which is
    how a test asks the day for a refusal in the middle of its loop.
    """

    def make(real_hook, conn_kwarg: str, **methods: str):
        """Return a class standing in for *real_hook*.

        Each keyword names an attribute holding a list and the method whose
        calls go into it: ``calls="load_file"`` gives a class recording every
        ``load_file`` in ``calls``. The connection id of every construction
        goes into ``conn_ids``.
        """
        conn_ids: list[str] = []
        namespace: dict = {"conn_ids": conn_ids, "fail_at": {}}

        def __init__(self, **kwargs):
            inspect.signature(real_hook.__init__).bind(self, **kwargs)
            conn_ids.append(kwargs[conn_kwarg])

        namespace["__init__"] = __init__
        for attribute, method in methods.items():
            calls: list[dict] = []
            namespace[attribute] = calls
            namespace[method] = _recording_method(real_hook, method, calls)
        return type(f"Recording{real_hook.__name__}", (), namespace)

    return make
