"""Encrypted token-revocation queue repository tests."""

from __future__ import annotations

import pytest

from astralplane.repositories import (
    RepositoryNotFoundError,
    RepositoryValidationError,
)
from astralplane.repositories.revocations import RevocationQueueRepository
from tests.repositories._support import Result, ScriptedTransaction


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 7,
        "user_id": "owner-1",
        "refresh_token_enc": "opaque-refresh-ciphertext",
        "enqueued_at": 42,
        "attempts": 0,
        "client_id": "astral-web",
    }
    row.update(overrides)
    return row


def test_enqueue_is_owner_attributed_and_redacts_ciphertext() -> None:
    transaction = ScriptedTransaction(execute=[Result(returned_records=(_row(),))])

    record = RevocationQueueRepository().enqueue(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        refresh_token_ciphertext="opaque-refresh-ciphertext",
        enqueued_at=42,
        client_id="astral-web",
    )

    assert record.owner_id == "owner-1"
    assert record.client_id == "astral-web"
    assert "opaque-refresh-ciphertext" not in repr(record)
    assert "?" not in transaction.calls[0][1]
    assert transaction.calls[0][2] == (
        "owner-1",
        "opaque-refresh-ciphertext",
        42,
        "astral-web",
    )


def test_pending_queue_is_owner_scoped_and_ordered() -> None:
    transaction = ScriptedTransaction(all_rows=[(_row(), _row(id=8, attempts=1))])

    records = RevocationQueueRepository().pending_for_owner(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        limit=2,
    )

    assert [record.queue_id for record in records] == [7, 8]
    assert "WHERE user_id = %s" in transaction.fetch_sql()
    assert "ORDER BY enqueued_at, id" in transaction.fetch_sql()
    assert transaction.calls[0][2] == ("owner-1", 2)


def test_resolve_is_owner_scoped_and_visible() -> None:
    transaction = ScriptedTransaction(execute=[Result(rowcount=1), Result(rowcount=0)])
    repository = RevocationQueueRepository()

    assert repository.resolve(transaction, owner_id="owner-1", queue_id=7)  # type: ignore[arg-type]
    assert not repository.resolve(transaction, owner_id="owner-2", queue_id=7)  # type: ignore[arg-type]
    assert all("user_id = %s" in call[1] for call in transaction.calls)


def test_attempt_increment_uses_owner_and_compare_and_set_fence() -> None:
    transaction = ScriptedTransaction(execute=[Result(returned_records=(_row(attempts=3),))])

    record = RevocationQueueRepository().bump_attempt(
        transaction,  # type: ignore[arg-type]
        owner_id="owner-1",
        queue_id=7,
        expected_attempts=2,
    )

    assert record.attempts == 3
    assert "attempts = %s" in transaction.calls[0][1]
    assert transaction.calls[0][2] == (7, "owner-1", 2)


def test_attempt_increment_reports_fence_or_owner_miss() -> None:
    transaction = ScriptedTransaction(execute=[Result(returned_records=())])

    with pytest.raises(RepositoryNotFoundError, match="owner-scoped"):
        RevocationQueueRepository().bump_attempt(  # type: ignore[arg-type]
            transaction,
            owner_id="owner-1",
            queue_id=7,
            expected_attempts=1,
        )


@pytest.mark.parametrize(
    ("method", "arguments"),
    [
        pytest.param(
            "enqueue",
            {
                "owner_id": "",
                "refresh_token_ciphertext": "cipher",
                "enqueued_at": 1,
            },
            id="missing-owner",
        ),
        pytest.param(
            "enqueue",
            {
                "owner_id": "owner-1",
                "refresh_token_ciphertext": "",
                "enqueued_at": 1,
            },
            id="missing-ciphertext",
        ),
        pytest.param(
            "enqueue",
            {
                "owner_id": "owner-1",
                "refresh_token_ciphertext": "cipher",
                "enqueued_at": -1,
            },
            id="negative-time",
        ),
        pytest.param(
            "pending_for_owner",
            {"owner_id": "owner-1", "limit": 0},
            id="invalid-limit",
        ),
    ],
)
def test_invalid_inputs_fail_before_sql(method: str, arguments: dict[str, object]) -> None:
    transaction = ScriptedTransaction()

    with pytest.raises(RepositoryValidationError):
        getattr(RevocationQueueRepository(), method)(transaction, **arguments)
    assert transaction.calls == []


def test_zero_queue_id_fails_closed() -> None:
    with pytest.raises(RepositoryNotFoundError):
        RevocationQueueRepository().resolve(  # type: ignore[arg-type]
            ScriptedTransaction(),
            owner_id="owner-1",
            queue_id=0,
        )
