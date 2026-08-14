"""Append-only, owner-attributed, hash-chained audit persistence primitives."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from astralplane.contracts import Record, Transaction
from astralplane.errors import PlaneError

GENESIS_DIGEST = bytes(32)
AuditAuthenticator = Callable[[str, bytes], bytes]


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_id: str
    chain_id: str
    auth_principal: str
    agent_id: str | None
    event_class: str
    action_type: str
    description: str
    conversation_id: str | None
    correlation_id: str
    outcome: str
    outcome_detail: str | None
    inputs_json: str
    outputs_json: str
    artifact_pointers_json: str
    started_at: datetime
    completed_at: datetime | None
    key_id: str
    schema_version: int = 2

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("event_id", self.event_id, 128),
            ("chain_id", self.chain_id, 512),
            ("auth_principal", self.auth_principal, 512),
            ("event_class", self.event_class, 128),
            ("action_type", self.action_type, 128),
            ("description", self.description, 1024),
            ("correlation_id", self.correlation_id, 128),
            ("key_id", self.key_id, 128),
        ):
            _required(name, value, maximum)
        if self.outcome not in {"in_progress", "success", "failure", "interrupted"}:
            raise ValueError("audit outcome is not supported")
        if self.schema_version not in {1, 2}:
            raise ValueError("schema_version must be 1 or 2")
        _aware("started_at", self.started_at)
        if self.completed_at is not None:
            _aware("completed_at", self.completed_at)
            if self.completed_at < self.started_at:
                raise ValueError("completed_at cannot precede started_at")
        object.__setattr__(
            self,
            "inputs_json",
            canonical_json(self.inputs_json, expected_type=dict),
        )
        object.__setattr__(
            self,
            "outputs_json",
            canonical_json(self.outputs_json, expected_type=dict),
        )
        object.__setattr__(
            self,
            "artifact_pointers_json",
            canonical_json(self.artifact_pointers_json, expected_type=list),
        )


@dataclass(frozen=True, slots=True)
class AuditRecord:
    event: AuditEvent
    sequence: int
    recorded_at: datetime
    previous_digest: bytes
    entry_digest: bytes

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            raise ValueError("audit sequence must be positive")
        _aware("recorded_at", self.recorded_at)
        _digest_bytes("previous_digest", self.previous_digest)
        _digest_bytes("entry_digest", self.entry_digest)


@dataclass(frozen=True, slots=True)
class ChainVerification:
    valid: bool
    first_invalid_event_id: str | None
    reason: str | None
    last_sequence: int
    last_digest: bytes


class AuditRepository:
    """Repository with no update/delete surface for ordinary application code."""

    def append(
        self,
        transaction: Transaction,
        event: AuditEvent,
        authenticate: AuditAuthenticator,
    ) -> AuditRecord:
        transaction.fetch_one(
            "SELECT pg_advisory_xact_lock(hashtext(%s)) AS locked",
            (f"audit_events:{event.chain_id}",),
        )
        existing = transaction.fetch_one(
            """
            SELECT * FROM audit_events
            WHERE actor_user_id = %s AND event_id = %s
            """,
            (event.chain_id, event.event_id),
        )
        if existing is not None:
            record = _record(existing)
            expected = _authenticate(
                authenticate,
                record.event.key_id,
                record.previous_digest + canonical_event_bytes(event, record.sequence),
            )
            if record.event != event or record.entry_digest != expected:
                raise PlaneError(
                    "audit event identity has conflicting semantics",
                    code="audit_idempotency_conflict",
                    metadata={"chain_id": event.chain_id},
                )
            return record

        previous = transaction.fetch_one(
            """
            SELECT chain_sequence, entry_hash FROM audit_events
            WHERE actor_user_id = %s
            ORDER BY chain_sequence DESC LIMIT 1
            """,
            (event.chain_id,),
        )
        sequence = 1 if previous is None else int(previous["chain_sequence"]) + 1
        previous_digest = GENESIS_DIGEST if previous is None else _bytes(previous["entry_hash"])
        entry_digest = _authenticate(
            authenticate,
            event.key_id,
            previous_digest + canonical_event_bytes(event, sequence),
        )
        row = transaction.fetch_one(
            """
            INSERT INTO audit_events (
                event_id, actor_user_id, auth_principal, agent_id, event_class,
                action_type, description, conversation_id, correlation_id,
                outcome, outcome_detail, inputs_meta, outputs_meta,
                artifact_pointers, started_at, completed_at, chain_sequence,
                prev_hash, entry_hash, key_id, schema_version
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *
            """,
            (
                event.event_id,
                event.chain_id,
                event.auth_principal,
                event.agent_id,
                event.event_class,
                event.action_type,
                event.description,
                event.conversation_id,
                event.correlation_id,
                event.outcome,
                event.outcome_detail,
                canonical_json(event.inputs_json, expected_type=dict),
                canonical_json(event.outputs_json, expected_type=dict),
                canonical_json(event.artifact_pointers_json, expected_type=list),
                event.started_at,
                event.completed_at,
                sequence,
                previous_digest,
                entry_digest,
                event.key_id,
                event.schema_version,
            ),
        )
        if row is None:
            raise PlaneError(
                "audit append did not return its durable row",
                code="audit_append_failed",
                metadata={"chain_id": event.chain_id},
            )
        record = _record(row)
        if (
            record.sequence != sequence
            or record.previous_digest != previous_digest
            or record.entry_digest != entry_digest
        ):
            raise PlaneError(
                "audit append returned inconsistent chain evidence",
                code="audit_append_inconsistent",
                metadata={"chain_id": event.chain_id},
            )
        return record

    def get(self, transaction: Transaction, *, chain_id: str, event_id: str) -> AuditRecord | None:
        row = transaction.fetch_one(
            "SELECT * FROM audit_events WHERE actor_user_id = %s AND event_id = %s",
            (chain_id, event_id),
        )
        return None if row is None else _record(row)

    def list_for_chain(
        self,
        transaction: Transaction,
        *,
        chain_id: str,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[AuditRecord, ...]:
        if after_sequence < 0:
            raise ValueError("after_sequence cannot be negative")
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = transaction.fetch_all(
            """
            SELECT * FROM audit_events
            WHERE actor_user_id = %s AND chain_sequence > %s
            ORDER BY chain_sequence, event_id LIMIT %s
            """,
            (chain_id, after_sequence, limit),
        )
        return tuple(_record(row) for row in rows)

    def load_chain(
        self,
        transaction: Transaction,
        *,
        chain_id: str,
        start_sequence: int = 1,
    ) -> tuple[AuditRecord, ...]:
        if start_sequence <= 0:
            raise ValueError("start_sequence must be positive")
        rows = transaction.fetch_all(
            """
            SELECT * FROM audit_events
            WHERE actor_user_id = %s AND chain_sequence >= %s
            ORDER BY chain_sequence, event_id
            """,
            (chain_id, start_sequence),
        )
        return tuple(_record(row) for row in rows)

    def verify_chain(
        self,
        transaction: Transaction,
        *,
        chain_id: str,
        authenticate: AuditAuthenticator,
        start_sequence: int = 1,
        expected_previous_digest: bytes = GENESIS_DIGEST,
    ) -> ChainVerification:
        _digest_bytes("expected_previous_digest", expected_previous_digest)
        records = self.load_chain(
            transaction,
            chain_id=chain_id,
            start_sequence=start_sequence,
        )
        return verify_records(
            records,
            chain_id=chain_id,
            authenticate=authenticate,
            start_sequence=start_sequence,
            expected_previous_digest=expected_previous_digest,
        )


def verify_records(
    records: Sequence[AuditRecord],
    *,
    chain_id: str,
    authenticate: AuditAuthenticator,
    start_sequence: int = 1,
    expected_previous_digest: bytes = GENESIS_DIGEST,
) -> ChainVerification:
    _digest_bytes("expected_previous_digest", expected_previous_digest)
    expected_sequence = start_sequence
    previous_digest = expected_previous_digest
    for record in records:
        reason: str | None = None
        if record.event.chain_id != chain_id:
            reason = "wrong_chain"
        elif record.sequence != expected_sequence:
            reason = "non_contiguous_sequence"
        elif record.previous_digest != previous_digest:
            reason = "previous_digest_mismatch"
        else:
            expected_entry = _authenticate(
                authenticate,
                record.event.key_id,
                previous_digest + canonical_event_bytes(record.event, record.sequence),
            )
            if record.entry_digest != expected_entry:
                reason = "entry_digest_mismatch"
        if reason is not None:
            return ChainVerification(
                valid=False,
                first_invalid_event_id=record.event.event_id,
                reason=reason,
                last_sequence=expected_sequence - 1,
                last_digest=previous_digest,
            )
        expected_sequence += 1
        previous_digest = record.entry_digest
    return ChainVerification(
        valid=True,
        first_invalid_event_id=None,
        reason=None,
        last_sequence=expected_sequence - 1,
        last_digest=previous_digest,
    )


def canonical_event_bytes(event: AuditEvent, sequence: int) -> bytes:
    if sequence <= 0:
        raise ValueError("audit sequence must be positive")
    if event.schema_version == 1:
        return _canonical_event_v1(event)
    if event.schema_version != 2:
        raise ValueError("schema_version must be 1 or 2")
    canonical = {
        "schema_version": event.schema_version,
        "chain_sequence": sequence,
        "event_id": event.event_id,
        "actor_user_id": event.chain_id,
        "auth_principal": event.auth_principal,
        "agent_id": event.agent_id,
        "event_class": event.event_class,
        "action_type": event.action_type,
        "description": event.description,
        "conversation_id": event.conversation_id,
        "correlation_id": event.correlation_id,
        "outcome": event.outcome,
        "outcome_detail": event.outcome_detail,
        "inputs_meta": json.loads(event.inputs_json),
        "outputs_meta": json.loads(event.outputs_json),
        "artifact_pointers": json.loads(event.artifact_pointers_json),
        "started_at": _utc_iso(event.started_at),
        "completed_at": None if event.completed_at is None else _utc_iso(event.completed_at),
    }
    return json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _canonical_event_v1(event: AuditEvent) -> bytes:
    """Reproduce AstralDeep's immutable schema-v1 chain bytes exactly.

    Version 1 predates explicit chain positions.  In particular it uses
    ``json.dumps``' default ASCII escaping and ``+00:00`` UTC offsets.  These
    details are part of already-persisted HMAC inputs and must not be
    normalized to the schema-v2 representation.
    """

    canonical = {
        "schema_version": 1,
        "event_id": event.event_id,
        "actor_user_id": event.chain_id,
        "auth_principal": event.auth_principal,
        "agent_id": event.agent_id,
        "event_class": event.event_class,
        "action_type": event.action_type,
        "description": event.description,
        "conversation_id": event.conversation_id,
        "correlation_id": event.correlation_id,
        "outcome": event.outcome,
        "outcome_detail": event.outcome_detail,
        "inputs_meta": json.loads(event.inputs_json),
        "outputs_meta": json.loads(event.outputs_json),
        "artifact_pointers": json.loads(event.artifact_pointers_json),
        "started_at": event.started_at.astimezone(UTC).isoformat(),
        "completed_at": (
            None if event.completed_at is None else event.completed_at.astimezone(UTC).isoformat()
        ),
    }
    return json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def canonical_json(value: str | Mapping[str, Any] | Sequence[Any], *, expected_type: type) -> str:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, expected_type):
            raise TypeError
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"audit JSON must contain a {expected_type.__name__}") from exc


def _record(row: Record) -> AuditRecord:
    event = AuditEvent(
        event_id=str(row["event_id"]),
        chain_id=str(row["actor_user_id"]),
        auth_principal=str(row["auth_principal"]),
        agent_id=None if row.get("agent_id") is None else str(row["agent_id"]),
        event_class=str(row["event_class"]),
        action_type=str(row["action_type"]),
        description=str(row["description"]),
        conversation_id=(
            None if row.get("conversation_id") is None else str(row["conversation_id"])
        ),
        correlation_id=str(row["correlation_id"]),
        outcome=str(row["outcome"]),
        outcome_detail=(None if row.get("outcome_detail") is None else str(row["outcome_detail"])),
        inputs_json=canonical_json(row.get("inputs_meta") or {}, expected_type=dict),
        outputs_json=canonical_json(row.get("outputs_meta") or {}, expected_type=dict),
        artifact_pointers_json=canonical_json(
            row.get("artifact_pointers") or [], expected_type=list
        ),
        started_at=row["started_at"],
        completed_at=row.get("completed_at"),
        key_id=str(row["key_id"]),
        schema_version=int(row["schema_version"]),
    )
    return AuditRecord(
        event=event,
        sequence=int(row["chain_sequence"]),
        recorded_at=row["recorded_at"],
        previous_digest=_bytes(row["prev_hash"]),
        entry_digest=_bytes(row["entry_hash"]),
    )


def _authenticate(authenticate: AuditAuthenticator, key_id: str, payload: bytes) -> bytes:
    digest = bytes(authenticate(key_id, payload))
    _digest_bytes("audit authenticator result", digest)
    return digest


def _bytes(value: object) -> bytes:
    try:
        return bytes(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("audit digest is not byte-compatible") from exc


def _digest_bytes(name: str, value: bytes) -> None:
    if not isinstance(value, bytes) or len(value) != 32:
        raise ValueError(f"{name} must contain 32 bytes")


def _required(name: str, value: str, maximum: int) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a non-empty string of at most {maximum} characters")


def _aware(name: str, value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = (
    "GENESIS_DIGEST",
    "AuditAuthenticator",
    "AuditEvent",
    "AuditRecord",
    "AuditRepository",
    "ChainVerification",
    "canonical_event_bytes",
    "canonical_json",
    "verify_records",
)
