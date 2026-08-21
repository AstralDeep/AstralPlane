from __future__ import annotations

import hashlib
import hmac
import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pytest
from _support import ScriptedTransaction

from astralplane.database.baseline import BaselineMigrationRunner
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_REGISTRY,
    MigrationRunner,
)
from astralplane.database.pool import ConnectionPool
from astralplane.database.transaction import PlaneDatabase
from astralplane.errors import PlaneError
from astralplane.repositories.audit import (
    GENESIS_DIGEST,
    AuditCursor,
    AuditEvent,
    AuditPage,
    AuditRecord,
    AuditRepository,
    ToolTrajectoryEvent,
    canonical_event_bytes,
    canonical_json,
    verify_records,
)
from tests.fixtures.pre_split.loader import (
    TEST_DATABASE_ENV,
    FixtureLoadError,
    connect_fixture_database,
    drop_postgres_fixture,
)

NOW = datetime(2026, 8, 13, 20, tzinfo=UTC)
KEY = b"k" * 32


class _SingleConnectionDriverPool:
    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.borrowed = False

    def getconn(self) -> Any:
        if self.borrowed:
            raise RuntimeError("audit integration connection is already borrowed")
        self.borrowed = True
        return self.connection

    def putconn(self, connection: Any, *, close: bool = False) -> None:
        if connection is not self.connection or not self.borrowed or close:
            raise RuntimeError("audit integration connection was returned in an invalid state")
        self.borrowed = False

    def closeall(self) -> None:
        return None


@pytest.fixture
def audit_postgres_database() -> Iterator[PlaneDatabase]:
    database_url = os.environ.get(TEST_DATABASE_ENV)
    if database_url is None:
        pytest.skip(f"set {TEST_DATABASE_ENV} to an isolated PostgreSQL test database")
    try:
        connection = connect_fixture_database(database_url)
    except FixtureLoadError as exc:
        pytest.fail(str(exc))
    schema = f"astralplane_fixture_{uuid.uuid4().hex}"
    cursor = connection.cursor()
    try:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
        cursor.execute(f'SET search_path TO "{schema}", pg_catalog')
        connection.commit()
    finally:
        cursor.close()

    pool = ConnectionPool(_SingleConnectionDriverPool(connection))
    database = PlaneDatabase(pool)
    try:
        migration = MigrationRunner(
            database,
            revision=CURRENT_DATA_PLANE_REVISION,
            registry=MIGRATION_REGISTRY,
        )
        BaselineMigrationRunner(database, migration).run(
            expected_revision=CURRENT_DATA_PLANE_REVISION.schema_revision
        )
        yield database
    finally:
        pool.close()
        drop_postgres_fixture(connection, schema=schema)
        connection.close()


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


def test_canonical_json_normalizes_detached_nested_containers() -> None:
    detached_object = MappingProxyType(
        {
            "z": (
                MappingProxyType(
                    {
                        "items": [2, MappingProxyType({"nested": (3, 4)})],
                    }
                ),
            ),
            "a": MappingProxyType({"enabled": True}),
        }
    )
    detached_list = (
        MappingProxyType({"b": (2, 1)}),
        [MappingProxyType({"a": None})],
    )

    assert canonical_json(detached_object, expected_type=dict) == (
        '{"a":{"enabled":true},"z":[{"items":[2,{"nested":[3,4]}]}]}'
    )
    assert canonical_json(detached_list, expected_type=list) == (
        '[{"b":[2,1]},[{"a":null}]]'
    )


@pytest.mark.parametrize(
    ("value", "expected_type"),
    [
        (MappingProxyType({"unsupported": frozenset({"value"})}), dict),
        ({1: "non-string-key"}, dict),
        ([b"bytes"], list),
        ([float("nan")], list),
        (MappingProxyType({"object": True}), list),
    ],
)
def test_canonical_json_rejects_unsupported_detached_values(
    value: object, expected_type: type
) -> None:
    with pytest.raises(ValueError):
        canonical_json(value, expected_type=expected_type)  # type: ignore[arg-type]


def test_real_postgresql_append_normalizes_detached_json(
    audit_postgres_database: PlaneDatabase,
) -> None:
    value = event(
        event_id=str(uuid.uuid4()),
        correlation_id=str(uuid.uuid4()),
        inputs_json='{"outer":{"items":[{"b":2,"a":1},[3,4]]}}',
        outputs_json='{"summary":{"ok":true}}',
        artifact_pointers_json='[{"meta":{"segments":[1,2]},"kind":"file"}]',
    )
    repository = AuditRepository()

    with audit_postgres_database.transaction() as transaction:
        appended = repository.append(transaction, value, authenticate)
    with audit_postgres_database.transaction() as transaction:
        reloaded = repository.get(
            transaction,
            chain_id=value.chain_id,
            event_id=value.event_id,
        )

    assert appended.event.inputs_json == '{"outer":{"items":[{"a":1,"b":2},[3,4]]}}'
    assert appended.event.outputs_json == '{"summary":{"ok":true}}'
    assert appended.event.artifact_pointers_json == (
        '[{"kind":"file","meta":{"segments":[1,2]}}]'
    )
    assert reloaded == appended


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


def test_descending_owner_page_supports_typed_filters_and_cursor() -> None:
    first = event_row(recorded_at=NOW + timedelta(seconds=4))
    second = event_row(
        event(event_id="event-2", outcome="failure"),
        recorded_at=NOW + timedelta(seconds=3),
    )
    third = event_row(
        event(event_id="event-3"),
        recorded_at=NOW + timedelta(seconds=2),
    )
    transaction = ScriptedTransaction(all_rows=[(first, second, third)])
    cursor = AuditCursor(recorded_at=NOW + timedelta(seconds=5), event_id="event-9")

    page = AuditRepository().list_page(
        transaction,
        owner_id="owner-1",
        event_classes=("tool_call",),
        outcomes=("failure",),
        from_ts=NOW,
        to_ts=NOW + timedelta(hours=1),
        keyword=r"100%_safe\\path",
        cursor=cursor,
        limit=2,
    )

    assert isinstance(page, AuditPage)
    assert [record.event.event_id for record in page.records] == ["event-1", "event-2"]
    assert page.next_cursor == AuditCursor(
        recorded_at=NOW + timedelta(seconds=3), event_id="event-2"
    )
    statement = transaction.calls[0][1]
    parameters = transaction.calls[0][2]
    assert "actor_user_id = %s" in statement
    assert "recorded_at >= %s" in statement and "recorded_at < %s" in statement
    assert "ORDER BY recorded_at DESC, event_id DESC" in statement
    assert "ESCAPE E'\\\\'" in statement
    assert parameters[-3:] == (
        "%100\\%\\_safe\\\\\\\\path%",
        "%100\\%\\_safe\\\\\\\\path%",
        3,
    )


def test_descending_owner_page_empty_and_validation() -> None:
    repository = AuditRepository()
    assert repository.list_page(
        ScriptedTransaction(all_rows=[()]), owner_id="owner-1"
    ) == AuditPage(records=(), next_cursor=None)
    invalid_calls = (
        {"owner_id": "", "limit": 1},
        {"owner_id": "owner-1", "limit": 0},
        {"owner_id": "owner-1", "event_classes": ("",)},
        {"owner_id": "owner-1", "outcomes": ("other",)},
        {"owner_id": "owner-1", "from_ts": NOW.replace(tzinfo=None)},
        {
            "owner_id": "owner-1",
            "from_ts": NOW + timedelta(seconds=1),
            "to_ts": NOW,
        },
        {"owner_id": "owner-1", "keyword": " "},
        {"owner_id": "owner-1", "cursor": "encoded"},
    )
    for arguments in invalid_calls:
        with pytest.raises(ValueError):
            repository.list_page(ScriptedTransaction(), **arguments)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        AuditCursor(recorded_at=NOW.replace(tzinfo=None), event_id="event-1")
    with pytest.raises(ValueError):
        AuditCursor(recorded_at=NOW, event_id="")


def test_admin_trajectory_query_is_fixed_bounded_and_typed() -> None:
    transaction = ScriptedTransaction(
        all_rows=[
            (
                {
                    "agent_id": "agent-1",
                    "correlation_id": "turn-1",
                    "tool_name": "search",
                },
            )
        ]
    )
    result = AuditRepository().list_tool_trajectory_events_for_administration(
        transaction,
        from_ts=NOW,
        to_ts=NOW + timedelta(hours=1),
        limit=12,
    )
    assert result == (
        ToolTrajectoryEvent(
            agent_id="agent-1", correlation_id="turn-1", tool_name="search"
        ),
    )
    assert "event_class = 'agent_tool_call'" in transaction.calls[0][1]
    assert "action_type LIKE 'tool.%.end'" in transaction.calls[0][1]
    assert transaction.calls[0][2] == (NOW, NOW + timedelta(hours=1), 12)


def test_admin_trajectory_query_validates_bounds_and_corrupt_rows() -> None:
    repository = AuditRepository()
    for arguments in (
        {"from_ts": NOW.replace(tzinfo=None), "to_ts": NOW, "limit": 1},
        {"from_ts": NOW + timedelta(seconds=1), "to_ts": NOW, "limit": 1},
        {"from_ts": NOW, "to_ts": NOW, "limit": 0},
    ):
        with pytest.raises(ValueError):
            repository.list_tool_trajectory_events_for_administration(
                ScriptedTransaction(), **arguments
            )
    with pytest.raises(ValueError):
        repository.list_tool_trajectory_events_for_administration(
            ScriptedTransaction(
                all_rows=[
                    (
                        {
                            "agent_id": None,
                            "correlation_id": "turn-1",
                            "tool_name": "search",
                        },
                    )
                ]
            ),
            from_ts=NOW,
            to_ts=NOW,
        )


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
