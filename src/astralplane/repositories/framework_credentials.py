"""Owner-issued framework credentials: hash-only bearer tokens for external callers.

A framework credential lets an owner mint a durable, independently-lifetimed
bearer credential (for an SDK, MCP, or A2A client) while their interactive
session — or a native client's own already-registered credential — is live.
It is deliberately unlike a delegation chain: it carries no parent binding,
its own expiry and scope set are fixed at mint time from the owner's OWN
authority, and it is never attenuated from another issued credential.

Plane never receives or persists the plaintext token. The caller (Deep) hashes
the generated secret with SHA-256 before calling :meth:`issue`; only the hex
digest and a short non-secret display prefix are ever stored.

Issuance closes the donor's pre-lock-authority defect: the previous reference
implementation read "is the issuer still valid" and computed the expiry BEFORE
acquiring any lock, so a revoke or owner-retirement racing the mint could lose
the race and still see a credential appear. Every write below takes the same
owner advisory-lock domain assignment creation uses, re-reads the issuer and
owner state INSIDE that lock, and computes ``expires_at`` from the database
clock only after both checks pass.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Final

from astralplane.contracts import QueryExecutor, Transaction
from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryNotFoundError,
    RepositoryValidationError,
    _bounded_text,
    _canonical_json,
    _required_id,
    _row_value,
)
from astralplane.repositories.history import (
    FrameworkCredentialExecutionState,
    FrameworkCredentialFence,
    FrameworkCredentialObservation,
    _framework_credential_observation,
    _framework_credential_unavailable,
)

# Closed scope vocabulary a framework credential may ever carry. Deep maps its
# own tool/verb catalog onto this small, stable set; it is deliberately not
# every internal permission name, so widening the product's tool catalog
# never silently widens what an already-issued credential can reach.
FRAMEWORK_CREDENTIAL_SCOPES: Final = frozenset(
    {
        "operations.submit",
        "operations.read",
        "operations.control",
        "artifacts.read",
        "agents.read",
    }
)

_ISSUER_KINDS: Final = frozenset({"session_incarnation", "native_credential"})


@dataclass(frozen=True, slots=True)
class FrameworkCredentialRecord:
    """Detached durable framework-credential metadata. Never carries the token."""

    credential_id: str
    owner_id: str
    name: str
    scopes: tuple[str, ...]
    token_prefix: str
    issuer_kind: str
    issuer_reference: str = field(repr=False)
    max_admissions: int
    consumed_admissions: int
    created_at: int
    expires_at: int
    revoked_at: int | None
    last_used_at: int | None

    @property
    def active(self) -> bool:
        return self.revoked_at is None


class FrameworkCredentialRepository:
    """Persist hash-only framework credentials under owner and issuer predicates."""

    _FIELDS = (
        "id, owner_id, name, scopes, token_hash, token_prefix, issuer_kind, "
        "issuer_reference, max_admissions, consumed_admissions, "
        "extract(epoch FROM created_at)::bigint AS created_epoch, "
        "extract(epoch FROM expires_at)::bigint AS expires_epoch, "
        "extract(epoch FROM revoked_at)::bigint AS revoked_epoch, "
        "extract(epoch FROM last_used_at)::bigint AS last_used_epoch"
    )

    def issue(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        credential_id: str,
        name: str,
        scopes: tuple[str, ...],
        token_hash: str,
        token_prefix: str,
        issuer_kind: str,
        issuer_reference: str,
        max_admissions: int,
        ttl_seconds: int,
    ) -> FrameworkCredentialRecord:
        """Mint one hash-only credential inside the owner's exact issuance lock.

        Re-reads the owner's retirement state and the named issuer (session
        incarnation or native credential) INSIDE the same locked transaction
        that computes ``expires_at``, so a concurrent revoke, session
        rotation, or owner retirement can never race a mint to completion.
        """
        owner = _required_id(owner_id, "owner_id")
        credential = _required_id(credential_id, "credential_id")
        display_name = _bounded_text(name, "name", maximum=256)
        scope_tuple = _validate_scopes(scopes)
        _validate_token_hash(token_hash)
        prefix = _bounded_text(token_prefix, "token_prefix", maximum=16)
        if issuer_kind not in _ISSUER_KINDS:
            raise RepositoryValidationError("unsupported credential issuer kind")
        reference = _required_id(issuer_reference, "issuer_reference", maximum=256)
        limit = _admission_limit(max_admissions)
        ttl = _ttl_seconds(ttl_seconds)
        with transaction.savepoint("framework_credential_issue"):
            self._lock_issuer_owner(transaction, owner)
            if not self._reissue_live_session(transaction, owner, reference):
                raise RepositoryConflictError(
                    "credential_authority_unavailable", code="credential_authority_unavailable"
                )
            row = transaction.fetch_one(
                f"""
                INSERT INTO framework_credential (
                    id, owner_id, name, scopes, token_hash, token_prefix,
                    issuer_kind, issuer_reference, max_admissions,
                    consumed_admissions, created_at, expires_at
                ) VALUES (
                    %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, 0,
                    clock_timestamp(), clock_timestamp() + %s * interval '1 second'
                )
                RETURNING {self._FIELDS}
                """,
                (
                    credential,
                    owner,
                    display_name,
                    _canonical_json(list(scope_tuple), "scopes"),
                    token_hash,
                    prefix,
                    issuer_kind,
                    reference,
                    limit,
                    ttl,
                ),
            )
        if row is None:  # pragma: no cover - PostgreSQL RETURNING invariant
            raise RepositoryValidationError("framework credential mint returned no row")
        return _record(row)

    def revoke(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        credential_id: str,
    ) -> FrameworkCredentialRecord:
        """Revoke one owned credential inside the same owner-issuance lock as mint."""
        owner = _required_id(owner_id, "owner_id")
        credential = _required_id(credential_id, "credential_id")
        self._lock_issuer_owner(transaction, owner)
        row = transaction.fetch_one(
            f"""
            UPDATE framework_credential
               SET revoked_at = clock_timestamp()
             WHERE id = %s AND owner_id = %s AND revoked_at IS NULL
            RETURNING {self._FIELDS}
            """,
            (credential, owner),
        )
        if row is None:
            existing = transaction.fetch_one(
                f"SELECT {self._FIELDS} FROM framework_credential WHERE id=%s AND owner_id=%s",
                (credential, owner),
            )
            if existing is None:
                raise RepositoryNotFoundError("framework credential not found")
            return _record(existing)
        return _record(row)

    def list_for_owner(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
    ) -> tuple[FrameworkCredentialRecord, ...]:
        owner = _required_id(owner_id, "owner_id")
        rows = query.fetch_all(
            f"SELECT {self._FIELDS} FROM framework_credential "
            "WHERE owner_id=%s ORDER BY created_at DESC, id",
            (owner,),
        )
        return tuple(_record(row) for row in rows)

    def consume_admission(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        credential_id: str,
    ) -> FrameworkCredentialRecord:
        """Atomically charge one admission, refusing an exhausted/expired/revoked row."""
        owner = _required_id(owner_id, "owner_id")
        credential = _required_id(credential_id, "credential_id")
        row = transaction.fetch_one(
            f"""
            UPDATE framework_credential
               SET consumed_admissions = consumed_admissions + 1,
                   last_used_at = clock_timestamp()
             WHERE id = %s AND owner_id = %s AND revoked_at IS NULL
               AND expires_at > clock_timestamp()
               AND consumed_admissions < max_admissions
            RETURNING {self._FIELDS}
            """,
            (credential, owner),
        )
        if row is None:
            raise RepositoryConflictError(
                "credential_allowance_exhausted", code="credential_allowance_exhausted"
            )
        return _record(row)

    def assert_current_execution(
        self,
        transaction: Transaction,
        *,
        observation: FrameworkCredentialObservation,
    ) -> FrameworkCredentialExecutionState:
        """Lock the exact credential row and validate a fresh host observation.

        Never persisted. Refuses a revoked, expired, or owner-mismatched
        credential, and refuses when the locked row's token hash no longer
        matches the caller's observation (the credential was replaced).
        """
        observation = _framework_credential_observation(observation)
        credential = observation.credential
        row = transaction.fetch_one(
            f"SELECT {self._FIELDS} FROM framework_credential "
            "WHERE id=%s AND owner_id=%s FOR UPDATE",
            (credential.credential_id, credential.owner_id),
        )
        now = transaction.fetch_one("SELECT clock_timestamp() AS now")["now"]
        if row is None or _row_value(row, "token_hash") != credential.token_hash:
            _framework_credential_unavailable()
        current = _record(row)
        if (
            current.revoked_at is not None
            or now.timestamp() >= current.expires_at
            or not observation.started_at <= now < observation.valid_until
        ):
            _framework_credential_unavailable()
        return FrameworkCredentialExecutionState(
            FrameworkCredentialFence(
                owner_id=current.owner_id,
                credential_id=current.credential_id,
                token_hash=credential.token_hash,
                scopes=current.scopes,
                max_admissions=current.max_admissions,
                consumed_admissions=current.consumed_admissions,
                created_at=current.created_at,
                expires_at=current.expires_at,
                revoked_at=current.revoked_at,
            ),
            now,
        )

    @staticmethod
    def _lock_issuer_owner(transaction: Transaction, owner_id: str) -> None:
        transaction.fetch_one("SELECT pg_advisory_xact_lock(hashtextextended(%s,79))", (owner_id,))
        retired = transaction.fetch_one(
            "SELECT state FROM astralplane_blob_owner_state WHERE owner_id=%s FOR UPDATE",
            (owner_id,),
        )
        if retired is not None and retired["state"] != "active":
            raise RepositoryConflictError(
                "credential_authority_unavailable", code="credential_authority_unavailable"
            )

    @staticmethod
    def _reissue_live_session(transaction: Transaction, owner_id: str, incarnation_id: str) -> bool:
        """Re-read the issuing session INSIDE the lock, current and unexpired.

        Both declared issuer kinds resolve through Plane's one session/
        incarnation mechanism today; ``issuer_kind`` only records, for audit,
        which kind of caller Deep observed when it captured this reference.
        """
        row = transaction.fetch_one(
            "SELECT hard_expires_at FROM web_session WHERE user_id=%s AND incarnation_id=%s",
            (owner_id, incarnation_id),
        )
        if row is None:
            return False
        now = transaction.fetch_one("SELECT clock_timestamp() AS now")["now"]
        return now.timestamp() < row["hard_expires_at"]


def _validate_scopes(scopes: object) -> tuple[str, ...]:
    if not isinstance(scopes, (tuple, list)) or not scopes:
        raise RepositoryValidationError("framework credential scopes must be a non-empty sequence")
    seen: list[str] = []
    for scope in scopes:
        if not isinstance(scope, str) or scope not in FRAMEWORK_CREDENTIAL_SCOPES:
            raise RepositoryValidationError("unsupported framework credential scope")
        if scope not in seen:
            seen.append(scope)
    return tuple(seen)


def _validate_token_hash(value: object) -> None:
    if not isinstance(value, str) or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise RepositoryValidationError("token_hash must be a lowercase SHA-256 hex digest")


def _admission_limit(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RepositoryValidationError("max_admissions must be an integer")
    if not 1 <= value <= 10000:
        raise RepositoryValidationError("max_admissions must be between 1 and 10000")
    return value


def _ttl_seconds(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RepositoryValidationError("ttl_seconds must be an integer")
    if not 1 <= value <= 7_776_000:  # 90 days
        raise RepositoryValidationError("ttl_seconds must be between 1 second and 90 days")
    return value


def _record(row) -> FrameworkCredentialRecord:
    raw_scopes = _row_value(row, "scopes")
    if isinstance(raw_scopes, str):
        try:
            decoded_scopes = json.loads(raw_scopes)
        except ValueError as exc:
            raise RepositoryValidationError(
                "persisted framework credential scopes are invalid"
            ) from exc
    else:
        decoded_scopes = raw_scopes
    if not isinstance(decoded_scopes, (list, tuple)) or not decoded_scopes:
        raise RepositoryValidationError("persisted framework credential scopes are invalid")
    return FrameworkCredentialRecord(
        credential_id=str(_row_value(row, "id")),
        owner_id=str(_row_value(row, "owner_id")),
        name=str(_row_value(row, "name")),
        scopes=tuple(str(scope) for scope in decoded_scopes),
        token_prefix=str(_row_value(row, "token_prefix")),
        issuer_kind=str(_row_value(row, "issuer_kind")),
        issuer_reference=str(_row_value(row, "issuer_reference")),
        max_admissions=int(_row_value(row, "max_admissions")),
        consumed_admissions=int(_row_value(row, "consumed_admissions")),
        created_at=int(_row_value(row, "created_epoch")),
        expires_at=int(_row_value(row, "expires_epoch")),
        revoked_at=None if row.get("revoked_epoch") is None else int(row["revoked_epoch"]),
        last_used_at=None if row.get("last_used_epoch") is None else int(row["last_used_epoch"]),
    )


__all__ = (
    "FRAMEWORK_CREDENTIAL_SCOPES",
    "FrameworkCredentialRecord",
    "FrameworkCredentialRepository",
)
