from __future__ import annotations

import hashlib
import hmac
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from _support import ScriptedTransaction

from astralplane.errors import PlaneError
from astralplane.repositories.audit import (
    GENESIS_DIGEST,
    AuditEvent,
    AuditRecord,
    AuditRepository,
    canonical_event_bytes,
    canonical_json,
    verify_records,
)

NOW = datetime(2026, 8, 13, 20, tzinfo=UTC)
KEY = b"k" * 32


def authenticate(key_id: str, payload: bytes) -> bytes:
    assert key_id == "audit-key-1"
    return hmac.new(KEY, payload, hashlib.sha256).digest()


def event(**overrides: object) -> AuditEvent:
    values: dict[str, object] = {
        "event_id": "event-1",
        "chain_id": "owner-1",
        "auth_principal": "principal-1",
        "agent_id": "agent-1",
        "event_class": "tool_call",
        "action_type": "tool.execute",
        "description": "A bounded event",
        "conversation_id": "chat-1",
        "correlation_id": "correlation-1",
        "outcome": "success",
        "outcome_detail": None,
        "inputs_json": '{"b":2,"a":1}',
        "outputs_json": "{}",
        "artifact_pointers_json": "[]",
        "started_at": NOW,
        "completed_at": NOW + timedelta(seconds=1),
        "key_id": "audit-key-1",
        "schema_version": 2,
    }
    values.update(overrides)
    return AuditEvent(**values)  # type: ignore[arg-type]


def event_row(
    value: AuditEvent | None = None,
    *,
    sequence: int = 1,
    previous: bytes = GENESIS_DIGEST,
    entry: bytes | None = None,
    **overrides: object,
) -> dict[str, object]:
    value = value or event()
    digest = entry or authenticate(value.key_id, previous + canonical_event_bytes(value, sequence))
    row: dict[str, object] = {
        "event_id": value.event_id,
        "actor_user_id": value.chain_id,
        "auth_principal": value.auth_principal,
        "agent_id": value.agent_id,
        "event_class": value.event_class,
        "action_type": value.action_type,
        "description": value.description,
        "conversation_id": value.conversation_id,
        "correlation_id": value.correlation_id,
        "outcome": value.outcome,
        "outcome_detail": value.outcome_detail,
        "inputs_meta": {"a": 1, "b": 2},
        "outputs_meta": {},
        "artifact_pointers": [],
        "started_at": value.started_at,
        "completed_at": value.completed_at,
        "key_id": value.key_id,
        "schema_version": value.schema_version,
        "chain_sequence": sequence,
        "recorded_at": NOW + timedelta(seconds=2),
        "prev_hash": previous,
        "entry_hash": digest,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": ""},
        {"outcome": "unknown"},
        {"schema_version": 0},
        {"schema_version": 3},
        {"started_at": NOW.replace(tzinfo=None)},
        {"completed_at": NOW - timedelta(seconds=1)},
        {"inputs_json": "[]"},
        {"artifact_pointers_json": "{}"},
    ],
)
def test_event_validation(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        event(**changes)


def test_canonical_json_rejects_invalid_and_sorts_keys() -> None:
    assert canonical_json('{"b":2,"a":1}', expected_type=dict) == '{"a":1,"b":2}'
    assert canonical_json([2, 1], expected_type=list) == "[2,1]"
    with pytest.raises(ValueError):
        canonical_json("not-json", expected_type=dict)


def test_schema_v1_canonical_bytes_preserve_legacy_chain_format() -> None:
    value = event(
        schema_version=1,
        description="caf\u00e9",
        inputs_json='{"label":"caf\u00e9"}',
        started_at=datetime(2026, 8, 13, 16, tzinfo=UTC),
        completed_at=None,
    )

    canonical = canonical_event_bytes(value, 47)

    assert b'"chain_sequence"' not in canonical
    assert b"caf\\u00e9" in canonical
    assert b"2026-08-13T16:00:00+00:00" in canonical
    assert b"2026-08-13T16:00:00Z" not in canonical


def test_schema_v2_canonical_bytes_bind_sequence_and_utf8() -> None:
    value = event(description="caf\u00e9", inputs_json='{"label":"caf\u00e9"}')

    canonical = canonical_event_bytes(value, 47)

    assert b'"chain_sequence":47' in canonical
    assert "caf\u00e9".encode() in canonical
    assert b"2026-08-13T20:00:00Z" in canonical


def test_append_genesis_uses_lock_and_returns_detached_record() -> None:
    value = event()
    row = event_row(value)
    transaction = ScriptedTransaction(one=[{"locked": True}, None, None, row])

    result = AuditRepository().append(transaction, value, authenticate)

    assert result.sequence == 1
    assert result.previous_digest == GENESIS_DIGEST
    assert result.entry_digest == row["entry_hash"]
    assert "pg_advisory_xact_lock" in transaction.calls[0][1]
    assert transaction.calls[-1][2][1] == "owner-1"  # type: ignore[index]


def test_append_continues_chain_and_is_idempotent() -> None:
    value = event(event_id="event-2")
    previous = b"p" * 32
    row = event_row(value, sequence=2, previous=previous)
    transaction = ScriptedTransaction(
        one=[{"locked": True}, None, {"chain_sequence": 1, "entry_hash": previous}, row]
    )
    result = AuditRepository().append(transaction, value, authenticate)
    assert result.sequence == 2
    assert result.previous_digest == previous

    replay = ScriptedTransaction(one=[{"locked": True}, row])
    assert AuditRepository().append(replay, value, authenticate) == result


def test_append_conflict_and_inconsistent_return_are_visible() -> None:
    value = event()
    conflicting = event_row(value, entry=b"x" * 32)
    with pytest.raises(PlaneError) as raised:
        AuditRepository().append(
            ScriptedTransaction(one=[{"locked": True}, conflicting]), value, authenticate
        )
    assert raised.value.code == "audit_idempotency_conflict"

    with pytest.raises(PlaneError) as raised:
        AuditRepository().append(
            ScriptedTransaction(one=[{"locked": True}, None, None, None]),
            value,
            authenticate,
        )
    assert raised.value.code == "audit_append_failed"

    wrong = event_row(value, previous=b"z" * 32)
    with pytest.raises(PlaneError) as raised:
        AuditRepository().append(
            ScriptedTransaction(one=[{"locked": True}, None, None, wrong]),
            value,
            authenticate,
        )
    assert raised.value.code == "audit_append_inconsistent"


def test_owner_scoped_reads_and_limits() -> None:
    repository = AuditRepository()
    record = repository.get(
        ScriptedTransaction(one=[event_row()]), chain_id="owner-1", event_id="event-1"
    )
    assert record is not None
    assert (
        repository.get(ScriptedTransaction(one=[None]), chain_id="other", event_id="event-1")
        is None
    )
    assert repository.list_for_chain(
        ScriptedTransaction(all_rows=[(event_row(),)]), chain_id="owner-1", limit=1
    ) == (record,)
    assert repository.load_chain(
        ScriptedTransaction(all_rows=[(event_row(),)]), chain_id="owner-1"
    ) == (record,)
    with pytest.raises(ValueError):
        repository.list_for_chain(ScriptedTransaction(), chain_id="owner-1", after_sequence=-1)
    with pytest.raises(ValueError):
        repository.list_for_chain(ScriptedTransaction(), chain_id="owner-1", limit=0)
    with pytest.raises(ValueError):
        repository.load_chain(ScriptedTransaction(), chain_id="owner-1", start_sequence=0)


def records() -> tuple[AuditRecord, AuditRecord]:
    first_row = event_row()
    first = AuditRepository().get(
        ScriptedTransaction(one=[first_row]), chain_id="owner-1", event_id="event-1"
    )
    assert first is not None
    second_event = event(event_id="event-2", action_type="tool.finish")
    second_row = event_row(
        second_event,
        sequence=2,
        previous=first.entry_digest,
    )
    second = AuditRepository().get(
        ScriptedTransaction(one=[second_row]), chain_id="owner-1", event_id="event-2"
    )
    assert second is not None
    return first, second


def test_chain_verification_accepts_genesis_and_detects_each_tamper_shape() -> None:
    first, second = records()
    verified = verify_records((first, second), chain_id="owner-1", authenticate=authenticate)
    assert verified.valid
    assert verified.last_sequence == 2
    assert verified.last_digest == second.entry_digest

    cases = (
        (replace(first, event=replace(first.event, chain_id="other")), "wrong_chain"),
        (replace(first, sequence=2), "non_contiguous_sequence"),
        (replace(first, previous_digest=b"q" * 32), "previous_digest_mismatch"),
        (replace(first, entry_digest=b"q" * 32), "entry_digest_mismatch"),
    )
    for damaged, reason in cases:
        result = verify_records((damaged,), chain_id="owner-1", authenticate=authenticate)
        assert not result.valid
        assert result.reason == reason
        assert result.first_invalid_event_id == "event-1"


def test_repository_chain_verification_supports_a_retained_start() -> None:
    first, second = records()
    transaction = ScriptedTransaction(
        all_rows=[(event_row(second.event, sequence=2, previous=first.entry_digest),)]
    )
    result = AuditRepository().verify_chain(
        transaction,
        chain_id="owner-1",
        authenticate=authenticate,
        start_sequence=2,
        expected_previous_digest=first.entry_digest,
    )
    assert result.valid and result.last_sequence == 2
    with pytest.raises(ValueError):
        verify_records(
            (), chain_id="owner-1", authenticate=authenticate, expected_previous_digest=b"x"
        )


def test_authenticator_must_return_sha256_sized_bytes() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        AuditRepository().append(
            ScriptedTransaction(one=[{"locked": True}, None, None]),
            event(),
            lambda _key, _payload: b"short",
        )
