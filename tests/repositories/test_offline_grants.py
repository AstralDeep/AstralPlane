"""Encrypted offline-grant repository tests."""

from __future__ import annotations

import uuid

import pytest

from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryValidationError,
)
from astralplane.repositories.offline_grants import (
    OfflineGrantRepository,
    OfflineGrantRevocationState,
)
from tests.repositories._support import Result, ScriptedTransaction

GRANT_ID = "9ef050be-0d5f-4a82-b3cb-410de6d93168"


def _grant_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": uuid.UUID(GRANT_ID),
        "user_id": "owner-1",
        "agent_id": "agent-1",
        "refresh_token_enc": memoryview(b"opaque-token"),
        "issued_at": 100,
        "expires_at": 1000,
        "revoked_at": None,
        "created_at": 100,
        "updated_at": 100,
    }
    row.update(overrides)
    return row


def _reference_row(**overrides: object) -> dict[str, object]:
    row = _grant_row(**overrides)
    for field in ("refresh_token_enc", "revoked_at", "created_at", "updated_at"):
        row.pop(field)
    return row


def test_create_grant_stores_opaque_bytes_and_redacts_them() -> None:
    transaction = ScriptedTransaction(one=[_grant_row()])

    record = OfflineGrantRepository().create_grant(
        transaction,  # type: ignore[arg-type]
        grant_id=GRANT_ID,
        owner_id="owner-1",
        agent_id="agent-1",
        encrypted_refresh_token=b"opaque-token",
        issued_at=100,
        expires_at=1000,
    )

    assert record.grant_id == GRANT_ID
    assert record.encrypted_refresh_token == b"opaque-token"
    assert "opaque-token" not in repr(record)
    assert record.active
    assert "ON CONFLICT (id) DO NOTHING" in transaction.fetch_sql()
    assert transaction.calls[0][2] == (
        GRANT_ID,
        "owner-1",
        "agent-1",
        b"opaque-token",
        100,
        1000,
        100,
        100,
    )


def test_create_grant_accepts_exact_replay_after_lifecycle_change() -> None:
    transaction = ScriptedTransaction(
        one=[None, _grant_row(revoked_at=500, updated_at=500)]
    )

    record = OfflineGrantRepository().create_grant(
        transaction,  # type: ignore[arg-type]
        grant_id=GRANT_ID,
        owner_id="owner-1",
        agent_id="agent-1",
        encrypted_refresh_token=bytearray(b"opaque-token"),
        issued_at=100,
        expires_at=1000,
    )

    assert record.revoked_at == 500
    assert not record.active
    assert transaction.calls[1][2] == (GRANT_ID, "owner-1")


@pytest.mark.parametrize(
    "replay",
    [None, _grant_row(expires_at=2000)],
)
def test_create_grant_rejects_other_owner_or_changed_identity(
    replay: dict[str, object] | None,
) -> None:
    transaction = ScriptedTransaction(one=[None, replay])

    with pytest.raises(RepositoryConflictError):
        OfflineGrantRepository().create_grant(
            transaction,  # type: ignore[arg-type]
            grant_id=GRANT_ID,
            owner_id="owner-1",
            agent_id="agent-1",
            encrypted_refresh_token=b"opaque-token",
            issued_at=100,
            expires_at=1000,
        )


def test_owner_get_and_exchange_lookup_use_owner_and_live_predicates() -> None:
    transaction = ScriptedTransaction(one=[_grant_row(), _grant_row(), None])
    repository = OfflineGrantRepository()

    loaded = repository.get_grant(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        grant_id=GRANT_ID,
    )
    exchange = repository.get_active_for_exchange(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        grant_id=GRANT_ID,
        as_of=500,
    )
    inactive = repository.get_active_for_exchange(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        grant_id=GRANT_ID,
        as_of=1000,
    )

    assert loaded is not None and exchange is not None and inactive is None
    assert all(call[2][1] == "owner-1" for call in transaction.calls)  # type: ignore[index]
    assert "revoked_at IS NULL" in transaction.calls[1][1]
    assert "expires_at > %s" in transaction.calls[1][1]


def test_latest_valid_prefers_requested_agent_without_exposing_ciphertext() -> None:
    transaction = ScriptedTransaction(one=[_reference_row()])

    reference = OfflineGrantRepository().find_latest_valid(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        agent_id="agent-1",
        as_of=500,
    )

    assert reference is not None and reference.grant_id == GRANT_ID
    assert not hasattr(reference, "encrypted_refresh_token")
    assert "CASE" in transaction.fetch_sql()
    assert transaction.calls[0][2] == ("owner-1", 500, "agent-1", "agent-1")


def test_latest_valid_without_agent_and_absent_result_are_explicit() -> None:
    transaction = ScriptedTransaction(one=[_reference_row(agent_id=None), None])
    repository = OfflineGrantRepository()

    reference = repository.find_latest_valid(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        as_of=500,
    )
    missing = repository.find_latest_valid(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        as_of=500,
    )

    assert reference is not None and reference.agent_id is None
    assert missing is None
    assert transaction.calls[0][2][-2:] == (None, None)  # type: ignore[index]


@pytest.mark.parametrize(
    ("execute_rowcount", "existing", "expected"),
    [
        (1, None, OfflineGrantRevocationState.REVOKED),
        (0, {"revoked_at": 500}, OfflineGrantRevocationState.ALREADY_REVOKED),
        (0, None, OfflineGrantRevocationState.MISSING),
    ],
)
def test_single_grant_revoke_is_owner_scoped_and_idempotent(
    execute_rowcount: int,
    existing: dict[str, object] | None,
    expected: OfflineGrantRevocationState,
) -> None:
    transaction = ScriptedTransaction(
        execute=[Result(rowcount=execute_rowcount)],
        one=[] if execute_rowcount else [existing],
    )

    state = OfflineGrantRepository().revoke_grant(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        grant_id=GRANT_ID,
        revoked_at=500,
    )

    assert state is expected
    assert transaction.calls[0][2] == (500, 500, GRANT_ID, "owner-1")
    assert "user_id = %s" in transaction.calls[0][1]


def test_owner_revoke_uses_one_timestamp_and_counts_transitions_only() -> None:
    transaction = ScriptedTransaction(execute=[Result(rowcount=3), Result(rowcount=-1)])
    repository = OfflineGrantRepository()

    assert repository.revoke_owner(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        revoked_at=500,
    ) == 3
    assert repository.revoke_owner(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        revoked_at=500,
    ) == 0
    assert transaction.calls[0][2] == (500, 500, "owner-1")
    assert "revoked_at IS NULL" in transaction.calls[0][1]


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("grant_id", "not-a-uuid"),
        ("owner_id", ""),
        ("agent_id", ""),
        ("encrypted_refresh_token", b""),
        ("encrypted_refresh_token", "plaintext"),
        ("issued_at", -1),
        ("expires_at", 100),
    ],
)
def test_create_rejects_invalid_inputs(argument: str, value: object) -> None:
    arguments: dict[str, object] = {
        "grant_id": GRANT_ID,
        "owner_id": "owner-1",
        "agent_id": "agent-1",
        "encrypted_refresh_token": b"opaque-token",
        "issued_at": 100,
        "expires_at": 1000,
    }
    arguments[argument] = value

    with pytest.raises(RepositoryValidationError):
        OfflineGrantRepository().create_grant(  # type: ignore[arg-type]
            ScriptedTransaction(),  # type: ignore[arg-type]
            **arguments,
        )


@pytest.mark.parametrize(
    "row",
    [
        _grant_row(id="bad"),
        _grant_row(refresh_token_enc="plaintext"),
        _grant_row(issued_at=-1),
        _grant_row(expires_at=100),
        _grant_row(revoked_at=-1),
    ],
)
def test_corrupt_persisted_grants_fail_closed(row: dict[str, object]) -> None:
    transaction = ScriptedTransaction(one=[row])

    with pytest.raises(RepositoryDataError):
        OfflineGrantRepository().get_grant(
            transaction,  # type: ignore[arg-type]
            owner_id="owner-1",
            grant_id=GRANT_ID,
        )
