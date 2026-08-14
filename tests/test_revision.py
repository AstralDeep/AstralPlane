"""Data-plane revision metadata validation tests."""

from __future__ import annotations

import hashlib
import json

import pytest

import astralplane.database.revision as revision_module
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_DIGEST,
    MIGRATION_REGISTRY,
    PLANE_SCHEMA_067_MIGRATION,
    PLANE_SCHEMA_067_STATEMENTS,
)
from astralplane.database.revision import (
    ADVISORY_LOCK_IDS,
    READ_COMPATIBLE_FROM,
    SCHEMA_REVISION,
    DataPlaneRevision,
    validate_revision,
)
from astralplane.errors import SchemaRevisionError


class _RecordingTransaction:
    def __init__(self) -> None:
        self.executions: list[tuple[str, object]] = []

    def execute(self, statement: str, parameters: object = ()) -> object:
        self.executions.append((statement, parameters))
        return object()


def test_schema_lineage_and_lock_identities_bind_the_canonical_registry() -> None:
    assert SCHEMA_REVISION == "067.001"
    assert READ_COMPATIBLE_FROM == "066.001"
    assert ADVISORY_LOCK_IDS == ((1095980114, 60001), (1095980114, 60002))
    assert not hasattr(revision_module, "MIGRATION_DIGEST")
    assert MIGRATION_REGISTRY.digest == MIGRATION_DIGEST
    assert CURRENT_DATA_PLANE_REVISION.schema_revision == SCHEMA_REVISION
    assert CURRENT_DATA_PLANE_REVISION.read_compatible_from == (READ_COMPATIBLE_FROM,)
    assert CURRENT_DATA_PLANE_REVISION.migration_digest == MIGRATION_DIGEST
    assert PLANE_SCHEMA_067_MIGRATION.source_revisions == ("066.001",)
    assert PLANE_SCHEMA_067_MIGRATION.target_revision == SCHEMA_REVISION
    assert len(PLANE_SCHEMA_067_STATEMENTS) == 18
    assert {
        "astralplane_outbox",
        "audit_retention_anchor",
        "astralplane_purge_tombstone",
        "astralplane_reconciliation_marker",
    } <= {word for statement in PLANE_SCHEMA_067_STATEMENTS for word in statement.split()}
    canonical_sql = "\n".join(PLANE_SCHEMA_067_STATEMENTS)
    assert "audit_events ADD COLUMN IF NOT EXISTS chain_sequence BIGINT" in canonical_sql
    assert "SET LOCAL audit.allow_purge = 'true'" in canonical_sql
    assert "audit_events_assign_sequence" in canonical_sql
    assert "astralplane_schema_postcondition" in canonical_sql

    canonical = json.dumps(
        PLANE_SCHEMA_067_STATEMENTS,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    assert PLANE_SCHEMA_067_MIGRATION.checksum == hashlib.sha256(canonical).hexdigest()

    declared = DataPlaneRevision(
        schema_revision=SCHEMA_REVISION,
        read_compatible_from=(READ_COMPATIBLE_FROM,),
        migration_digest="a" * 64,
    )
    assert declared.migration_lock == ADVISORY_LOCK_IDS[0]
    assert declared.accepts_reader_at("066.001")
    assert declared.accepts_reader_at("067.001")
    assert not declared.accepts_reader_at("065.001")


def test_canonical_migration_executes_every_repeat_safe_statement_without_rewriting() -> None:
    transaction = _RecordingTransaction()
    PLANE_SCHEMA_067_MIGRATION.apply(transaction)  # type: ignore[arg-type]

    assert transaction.executions == [(statement, ()) for statement in PLANE_SCHEMA_067_STATEMENTS]


@pytest.mark.parametrize("value", ["66.1", "066.01", "066.0001", "v066.001", "", 1])
def test_revision_format_is_exact(value: object) -> None:
    with pytest.raises(SchemaRevisionError):
        validate_revision(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "arguments",
    [
        {"read_compatible_from": ()},
        {"read_compatible_from": ["065.001"]},
        {"read_compatible_from": ("065.001", "065.001")},
        {"migration_digest": "A" * 64},
        {"advisory_lock_ids": ()},
        {"advisory_lock_ids": [(1, 2)]},
        {"advisory_lock_ids": ((1,),)},
        {"advisory_lock_ids": ((1, "two"),)},
    ],
)
def test_invalid_revision_metadata_fails_closed(arguments: dict[str, object]) -> None:
    values: dict[str, object] = {
        "schema_revision": "066.001",
        "read_compatible_from": ("065.001",),
        "migration_digest": "a" * 64,
    }
    values.update(arguments)
    with pytest.raises(SchemaRevisionError):
        DataPlaneRevision(**values)  # type: ignore[arg-type]
