"""Owner-attributed encrypted refresh-token revocation queue persistence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from astralplane.contracts import QueryExecutor, Transaction
from astralplane.repositories import (
    RepositoryNotFoundError,
    _bounded_limit,
    _bounded_text,
    _non_negative_int,
    _required_id,
    _row_value,
    _single_returned,
)


@dataclass(frozen=True, slots=True)
class RevocationQueueRecord:
    queue_id: int
    owner_id: str
    refresh_token_ciphertext: str = field(repr=False)
    client_id: str | None = None
    enqueued_at: int = 0
    attempts: int = 0


def _record(row: Any) -> RevocationQueueRecord:
    return RevocationQueueRecord(
        queue_id=int(_row_value(row, "id")),
        owner_id=str(_row_value(row, "user_id")),
        refresh_token_ciphertext=str(_row_value(row, "refresh_token_enc")),
        client_id=None if row.get("client_id") is None else str(row["client_id"]),
        enqueued_at=int(_row_value(row, "enqueued_at")),
        attempts=int(row.get("attempts") or 0),
    )


def _queue_id(value: object) -> int:
    queue_id = _non_negative_int(value, "queue id")
    if queue_id == 0:
        raise RepositoryNotFoundError("queue id must identify a persisted record")
    return queue_id


class RevocationQueueRepository:
    """Store ciphertext-only logout work with owner predicates on mutations."""

    _FIELDS = "id, user_id, refresh_token_enc, enqueued_at, attempts, client_id"

    def enqueue(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        refresh_token_ciphertext: str,
        enqueued_at: int,
        client_id: str | None = None,
    ) -> RevocationQueueRecord:
        owner = _required_id(owner_id, "owner id")
        ciphertext = _bounded_text(
            refresh_token_ciphertext,
            "refresh token ciphertext",
            maximum=131_072,
        )
        timestamp = _non_negative_int(enqueued_at, "enqueued at")
        client = None if client_id is None else _required_id(client_id, "client id")
        result = transaction.execute(
            f"""
            INSERT INTO auth_revocation_queue (
                user_id, refresh_token_enc, enqueued_at, attempts, client_id
            ) VALUES (%s, %s, %s, 0, %s)
            RETURNING {self._FIELDS}
            """,
            (owner, ciphertext, timestamp, client),
        )
        return _record(_single_returned(result, "enqueue token revocation"))

    def pending_for_owner(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        limit: int = 20,
    ) -> tuple[RevocationQueueRecord, ...]:
        owner = _required_id(owner_id, "owner id")
        bounded_limit = _bounded_limit(limit, maximum=200)
        rows = query.fetch_all(
            f"""
            SELECT {self._FIELDS}
            FROM auth_revocation_queue
            WHERE user_id = %s
            ORDER BY enqueued_at, id
            LIMIT %s
            """,
            (owner, bounded_limit),
        )
        return tuple(_record(row) for row in rows)

    def pending_for_administration(
        self,
        query: QueryExecutor,
        *,
        limit: int = 20,
    ) -> tuple[RevocationQueueRecord, ...]:
        """Return bounded cross-owner work for the trusted revocation drainer."""

        bounded_limit = _bounded_limit(limit, maximum=200)
        rows = query.fetch_all(
            f"""
            SELECT {self._FIELDS}
            FROM auth_revocation_queue
            ORDER BY enqueued_at, id
            LIMIT %s
            """,
            (bounded_limit,),
        )
        return tuple(_record(row) for row in rows)

    def resolve(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        queue_id: int,
    ) -> bool:
        owner = _required_id(owner_id, "owner id")
        identifier = _queue_id(queue_id)
        result = transaction.execute(
            "DELETE FROM auth_revocation_queue WHERE id = %s AND user_id = %s",
            (identifier, owner),
        )
        return result.rowcount == 1

    def bump_attempt(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        queue_id: int,
        expected_attempts: int,
    ) -> RevocationQueueRecord:
        owner = _required_id(owner_id, "owner id")
        identifier = _queue_id(queue_id)
        expected = _non_negative_int(expected_attempts, "expected attempts")
        result = transaction.execute(
            f"""
            UPDATE auth_revocation_queue
            SET attempts = attempts + 1
            WHERE id = %s AND user_id = %s AND attempts = %s
            RETURNING {self._FIELDS}
            """,
            (identifier, owner, expected),
        )
        rows = result.returned_records
        if len(rows) != 1:
            raise RepositoryNotFoundError(
                "owner-scoped revocation record or attempt fence was not found",
                metadata={"operation": "bump token revocation attempt"},
            )
        return _record(rows[0])


__all__ = ("RevocationQueueRecord", "RevocationQueueRepository")
