"""Owner-isolated encrypted offline-grant persistence.

Refresh-token encryption, token exchange, consent policy, and revocation at the
identity provider remain in the embedding product.  AstralPlane exposes only
opaque encrypted bytes and durable grant lifecycle mechanics.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from astralplane.contracts import Transaction
from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryValidationError,
    _non_negative_int,
    _required_id,
    _row_value,
)


class OfflineGrantRevocationState(StrEnum):
    """Idempotent result of an owner-scoped single-grant revocation."""

    REVOKED = "revoked"
    ALREADY_REVOKED = "already_revoked"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class OfflineGrantRecord:
    """Detached durable grant including its opaque encrypted token bytes."""

    grant_id: str
    owner_id: str
    agent_id: str | None
    encrypted_refresh_token: bytes = field(repr=False)
    issued_at: int
    expires_at: int
    revoked_at: int | None
    created_at: int | None
    updated_at: int | None

    @property
    def active(self) -> bool:
        """Whether the durable record has not been explicitly revoked."""

        return self.revoked_at is None


@dataclass(frozen=True, slots=True)
class OfflineGrantReference:
    """Token-free metadata used to locate standing delegated authority."""

    grant_id: str
    owner_id: str
    agent_id: str | None
    issued_at: int
    expires_at: int


class OfflineGrantRepository:
    """Persist encrypted refresh tokens under owner and expiry predicates."""

    _FIELDS = (
        "id, user_id, agent_id, refresh_token_enc, issued_at, expires_at, "
        "revoked_at, created_at, updated_at"
    )

    def create_grant(
        self,
        transaction: Transaction,
        *,
        grant_id: str,
        owner_id: str,
        agent_id: str | None,
        encrypted_refresh_token: bytes,
        issued_at: int,
        expires_at: int,
    ) -> OfflineGrantRecord:
        """Insert a grant or accept an exact immutable replay of its identity."""

        grant = _uuid_text(grant_id, "grant_id")
        owner = _required_id(owner_id, "owner_id")
        agent = _optional_id(agent_id, "agent_id")
        ciphertext = _opaque_bytes(encrypted_refresh_token)
        issued = _non_negative_int(issued_at, "issued_at")
        expires = _non_negative_int(expires_at, "expires_at")
        if expires <= issued:
            raise RepositoryValidationError("expires_at must be later than issued_at")
        row = transaction.fetch_one(
            f"""
            INSERT INTO user_offline_grant (
                id, user_id, agent_id, refresh_token_enc, issued_at, expires_at,
                revoked_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s)
            ON CONFLICT (id) DO NOTHING
            RETURNING {self._FIELDS}
            """,
            (grant, owner, agent, ciphertext, issued, expires, issued, issued),
        )
        if row is None:
            row = transaction.fetch_one(
                f"""
                SELECT {self._FIELDS} FROM user_offline_grant
                 WHERE id = %s AND user_id = %s
                """,
                (grant, owner),
            )
        if row is None:
            raise RepositoryConflictError("offline grant identity is bound to another owner")
        record = _grant(row)
        if (
            record.owner_id != owner
            or record.agent_id != agent
            or record.encrypted_refresh_token != ciphertext
            or record.issued_at != issued
            or record.expires_at != expires
        ):
            raise RepositoryConflictError("offline grant replay changed immutable semantics")
        return record

    def get_grant(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        grant_id: str,
    ) -> OfflineGrantRecord | None:
        owner = _required_id(owner_id, "owner_id")
        grant = _uuid_text(grant_id, "grant_id")
        row = transaction.fetch_one(
            f"SELECT {self._FIELDS} FROM user_offline_grant WHERE id = %s AND user_id = %s",
            (grant, owner),
        )
        return None if row is None else _grant(row)

    def get_active_for_exchange(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        grant_id: str,
        as_of: int,
    ) -> OfflineGrantRecord | None:
        """Resolve opaque token bytes only through an owner and live-state predicate."""

        owner = _required_id(owner_id, "owner_id")
        grant = _uuid_text(grant_id, "grant_id")
        observed_at = _non_negative_int(as_of, "as_of")
        row = transaction.fetch_one(
            f"""
            SELECT {self._FIELDS} FROM user_offline_grant
             WHERE id = %s AND user_id = %s
               AND revoked_at IS NULL AND expires_at > %s
            """,
            (grant, owner, observed_at),
        )
        return None if row is None else _grant(row)

    def replace_refresh_token_if_current(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        grant_id: str,
        expected_encrypted_refresh_token: bytes,
        encrypted_refresh_token: bytes,
        as_of: int,
    ) -> OfflineGrantRecord | None:
        """Replace opaque credential state without reviving a stale or revoked grant.

        The product owns encrypted credential-reference/rotation semantics. A
        failed predicate returns no credential, including when revocation wins
        while a caller is acquiring or settling an exchange.
        """
        owner = _required_id(owner_id, "owner_id")
        grant = _uuid_text(grant_id, "grant_id")
        expected = _opaque_bytes(expected_encrypted_refresh_token)
        replacement = _opaque_bytes(encrypted_refresh_token)
        observed = _non_negative_int(as_of, "as_of")
        row = transaction.fetch_one(
            f"""
            UPDATE user_offline_grant
               SET refresh_token_enc = %s, updated_at = %s
             WHERE id = %s AND user_id = %s AND refresh_token_enc = %s
               AND revoked_at IS NULL AND expires_at > %s
            RETURNING {self._FIELDS}
            """,
            (replacement, observed, grant, owner, expected, observed),
        )
        return None if row is None else _grant(row)

    def find_latest_valid(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        as_of: int,
        agent_id: str | None = None,
    ) -> OfflineGrantReference | None:
        """Prefer an agent-specific live grant, then the owner's newest live grant."""

        owner = _required_id(owner_id, "owner_id")
        agent = _optional_id(agent_id, "agent_id")
        observed_at = _non_negative_int(as_of, "as_of")
        row = transaction.fetch_one(
            """
            SELECT id, user_id, agent_id, issued_at, expires_at
              FROM user_offline_grant
             WHERE user_id = %s AND revoked_at IS NULL AND expires_at > %s
             ORDER BY CASE
                          WHEN %s::text IS NOT NULL AND agent_id = %s THEN 0
                          ELSE 1
                      END,
                      issued_at DESC, id
             LIMIT 1
            """,
            (owner, observed_at, agent, agent),
        )
        return None if row is None else _reference(row)

    def revoke_grant(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        grant_id: str,
        revoked_at: int,
    ) -> OfflineGrantRevocationState:
        owner = _required_id(owner_id, "owner_id")
        grant = _uuid_text(grant_id, "grant_id")
        observed_at = _non_negative_int(revoked_at, "revoked_at")
        result = transaction.execute(
            """
            UPDATE user_offline_grant
               SET revoked_at = %s, updated_at = %s
             WHERE id = %s AND user_id = %s AND revoked_at IS NULL
            """,
            (observed_at, observed_at, grant, owner),
        )
        if result.rowcount == 1:
            return OfflineGrantRevocationState.REVOKED
        row = transaction.fetch_one(
            "SELECT revoked_at FROM user_offline_grant WHERE id = %s AND user_id = %s",
            (grant, owner),
        )
        if row is None:
            return OfflineGrantRevocationState.MISSING
        return OfflineGrantRevocationState.ALREADY_REVOKED

    def revoke_owner(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        revoked_at: int,
    ) -> int:
        """Idempotently revoke every still-live grant owned by one principal."""

        owner = _required_id(owner_id, "owner_id")
        observed_at = _non_negative_int(revoked_at, "revoked_at")
        result = transaction.execute(
            """
            UPDATE user_offline_grant
               SET revoked_at = %s, updated_at = %s
             WHERE user_id = %s AND revoked_at IS NULL
            """,
            (observed_at, observed_at, owner),
        )
        return max(0, result.rowcount)


def _uuid_text(value: object, field: str) -> str:
    if not isinstance(value, (str, uuid.UUID)):
        raise RepositoryValidationError(f"{field} must be a UUID")
    try:
        return str(uuid.UUID(str(value)))
    except ValueError as exc:
        raise RepositoryValidationError(f"{field} must be a UUID") from exc


def _optional_id(value: object, field: str) -> str | None:
    return None if value is None else _required_id(value, field)


def _opaque_bytes(value: object) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise RepositoryValidationError("encrypted_refresh_token must be bytes")
    ciphertext = bytes(value)
    if not ciphertext:
        raise RepositoryValidationError("encrypted_refresh_token must not be empty")
    if len(ciphertext) > 1_000_000:
        raise RepositoryValidationError("encrypted_refresh_token exceeds its maximum length")
    return ciphertext


def _grant(row: Mapping[str, Any]) -> OfflineGrantRecord:
    issued_at = _stored_int(_row_value(row, "issued_at"), "issued_at")
    expires_at = _stored_int(_row_value(row, "expires_at"), "expires_at")
    if expires_at <= issued_at:
        raise RepositoryDataError("persisted offline grant expiry is invalid")
    try:
        ciphertext = _opaque_bytes(_row_value(row, "refresh_token_enc"))
    except RepositoryValidationError as exc:
        raise RepositoryDataError("persisted offline grant ciphertext is invalid") from exc
    return OfflineGrantRecord(
        grant_id=_stored_uuid(_row_value(row, "id")),
        owner_id=str(_row_value(row, "user_id")),
        agent_id=None if row.get("agent_id") is None else str(row["agent_id"]),
        encrypted_refresh_token=ciphertext,
        issued_at=issued_at,
        expires_at=expires_at,
        revoked_at=_optional_stored_int(row.get("revoked_at"), "revoked_at"),
        created_at=_optional_stored_int(row.get("created_at"), "created_at"),
        updated_at=_optional_stored_int(row.get("updated_at"), "updated_at"),
    )


def _reference(row: Mapping[str, Any]) -> OfflineGrantReference:
    issued_at = _stored_int(_row_value(row, "issued_at"), "issued_at")
    expires_at = _stored_int(_row_value(row, "expires_at"), "expires_at")
    if expires_at <= issued_at:
        raise RepositoryDataError("persisted offline grant expiry is invalid")
    return OfflineGrantReference(
        grant_id=_stored_uuid(_row_value(row, "id")),
        owner_id=str(_row_value(row, "user_id")),
        agent_id=None if row.get("agent_id") is None else str(row["agent_id"]),
        issued_at=issued_at,
        expires_at=expires_at,
    )


def _optional_stored_int(value: object, field: str) -> int | None:
    return None if value is None else _stored_int(value, field)


def _stored_uuid(value: object) -> str:
    try:
        return _uuid_text(value, "persisted grant id")
    except RepositoryValidationError as exc:
        raise RepositoryDataError("persisted offline grant id is invalid") from exc


def _stored_int(value: object, field: str) -> int:
    try:
        return _non_negative_int(value, field)
    except ValueError as exc:
        raise RepositoryDataError(
            "persisted grant timestamp is not a non-negative integer",
            metadata={"field": field},
        ) from exc


__all__ = (
    "OfflineGrantRecord",
    "OfflineGrantReference",
    "OfflineGrantRepository",
    "OfflineGrantRevocationState",
)
