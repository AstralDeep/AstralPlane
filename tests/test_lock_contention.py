"""Verifies the public contention taxonomy across genuine drivers and wrapped repository errors.
Classification does not authorize retries and never accepts a SQLSTATE-shaped stand-in.
"""

import psycopg2.errors
import pytest

from astralplane import LockContentionError, api, is_lock_contention
from astralplane.database.sqlite import SQLiteLockTimeoutError
from astralplane.errors import RepositoryConflictError


@pytest.mark.parametrize(
    "error",
    [
        LockContentionError("busy"),
        SQLiteLockTimeoutError(),
        psycopg2.errors.LockNotAvailable("busy"),
    ],
)
def test_public_classifier_recognizes_native_and_wrapped_contention(error):
    assert api.is_lock_contention is is_lock_contention
    assert api.LockContentionError is LockContentionError
    assert is_lock_contention(error)
    wrapper = RepositoryConflictError("authorization unavailable")
    wrapper.__cause__ = error
    assert is_lock_contention(wrapper)
    wrapper.__cause__ = None
    wrapper.__suppress_context__ = False
    wrapper.__context__ = error
    assert is_lock_contention(wrapper)
    wrapper.__suppress_context__ = True
    assert not is_lock_contention(wrapper)


def test_classifier_refuses_unproven_semantic_conflicts_and_cycles():
    class FakeSQLStateError(RepositoryConflictError):
        pgcode = "55P03"

    for error in (
        FakeSQLStateError("fake"),
        RepositoryConflictError("assignment_publication_busy"),
        psycopg2.errors.SerializationFailure("snapshot changed"),
        ValueError("bad"),
    ):
        assert not is_lock_contention(error)
    first, second = RuntimeError("first"), RuntimeError("second")
    first.__cause__, second.__cause__ = second, first
    assert not is_lock_contention(first)
    assert isinstance(SQLiteLockTimeoutError(), RepositoryConflictError)
