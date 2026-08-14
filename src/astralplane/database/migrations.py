"""Guarded, repeat-safe PostgreSQL schema migration runner."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Final

from astralplane.contracts import MigrationCallable, PlaneDatabase, Transaction
from astralplane.database.revision import DataPlaneRevision, validate_revision
from astralplane.errors import MigrationDefinitionError, SchemaRevisionError

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SCHEMA_META_REVISION: Final = "revision"
_SCHEMA_META_DIGEST: Final = "astralplane_migration_digest"

_ADVISORY_LOCK_SQL: Final = "SELECT pg_advisory_xact_lock(%s, %s)"
_CREATE_SCHEMA_META_SQL: Final = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""".strip()
_READ_SCHEMA_META_SQL: Final = """
SELECT key, value
FROM schema_meta
WHERE key IN (%s, %s)
""".strip()
_WRITE_SCHEMA_META_SQL: Final = """
INSERT INTO schema_meta (key, value)
VALUES (%s, %s)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
""".strip()

PLANE_SCHEMA_067_STATEMENTS: Final = (
    "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS chain_sequence BIGINT",
    "SET LOCAL audit.allow_purge = 'true'",
    """
DO $astralplane_audit_backfill$
DECLARE
    invalid_rows BIGINT;
BEGIN
    SELECT COUNT(*) INTO invalid_rows
    FROM (
        SELECT
            actor_user_id,
            event_id,
            prev_hash,
            LAG(entry_hash) OVER (
                PARTITION BY actor_user_id ORDER BY recorded_at, event_id
            ) AS expected_previous,
            ROW_NUMBER() OVER (
                PARTITION BY actor_user_id ORDER BY recorded_at, event_id
            ) AS sequence
        FROM audit_events
    ) AS ordered
    WHERE (
        sequence = 1
        AND prev_hash <> decode(repeat('00', 32), 'hex')
    ) OR (
        sequence > 1
        AND prev_hash IS DISTINCT FROM expected_previous
    );
    IF invalid_rows <> 0 THEN
        RAISE EXCEPTION 'audit chain topology is not a complete ordered chain'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*) INTO invalid_rows
    FROM (
        SELECT actor_user_id, entry_hash
        FROM audit_events
        GROUP BY actor_user_id, entry_hash
        HAVING COUNT(*) <> 1
    ) AS duplicate_hashes;
    IF invalid_rows <> 0 THEN
        RAISE EXCEPTION 'audit chain contains duplicate entry hashes'
            USING ERRCODE = '23514';
    END IF;

    WITH ordered AS (
        SELECT
            event_id,
            ROW_NUMBER() OVER (
                PARTITION BY actor_user_id ORDER BY recorded_at, event_id
            ) AS sequence
        FROM audit_events
    )
    UPDATE audit_events AS event
    SET chain_sequence = ordered.sequence
    FROM ordered
    WHERE event.event_id = ordered.event_id
      AND event.chain_sequence IS DISTINCT FROM ordered.sequence;
END
$astralplane_audit_backfill$
""".strip(),
    "ALTER TABLE audit_events ALTER COLUMN chain_sequence SET NOT NULL",
    """
CREATE UNIQUE INDEX IF NOT EXISTS uq_audit_events_actor_sequence
ON audit_events (actor_user_id, chain_sequence)
""".strip(),
    """
CREATE OR REPLACE FUNCTION audit_events_assign_chain_sequence()
RETURNS trigger AS $astralplane_audit_sequence$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('audit_events:' || NEW.actor_user_id));
    IF NEW.chain_sequence IS NULL THEN
        SELECT COALESCE(MAX(chain_sequence), 0) + 1
        INTO NEW.chain_sequence
        FROM audit_events
        WHERE actor_user_id = NEW.actor_user_id;
    END IF;
    RETURN NEW;
END
$astralplane_audit_sequence$ LANGUAGE plpgsql
""".strip(),
    "DROP TRIGGER IF EXISTS audit_events_assign_sequence ON audit_events",
    """
CREATE TRIGGER audit_events_assign_sequence
BEFORE INSERT ON audit_events
FOR EACH ROW EXECUTE FUNCTION audit_events_assign_chain_sequence()
""".strip(),
    """
DO $astralplane_audit_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'audit_events'::regclass
          AND conname = 'audit_events_chain_sequence_check'
    ) THEN
        ALTER TABLE audit_events
        ADD CONSTRAINT audit_events_chain_sequence_check
        CHECK (chain_sequence > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'audit_events'::regclass
          AND conname = 'audit_events_digest_length_check'
    ) THEN
        ALTER TABLE audit_events
        ADD CONSTRAINT audit_events_digest_length_check
        CHECK (octet_length(prev_hash) = 32 AND octet_length(entry_hash) = 32)
        NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'audit_events'::regclass
          AND conname = 'audit_events_schema_version_check'
    ) THEN
        ALTER TABLE audit_events
        ADD CONSTRAINT audit_events_schema_version_check
        CHECK (schema_version IN (1, 2)) NOT VALID;
    END IF;
    ALTER TABLE audit_events
        VALIDATE CONSTRAINT audit_events_chain_sequence_check;
    ALTER TABLE audit_events
        VALIDATE CONSTRAINT audit_events_digest_length_check;
    ALTER TABLE audit_events
        VALIDATE CONSTRAINT audit_events_schema_version_check;
END
$astralplane_audit_constraints$
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_outbox (
    entry_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    canonical_payload BYTEA NOT NULL,
    payload_sha256 CHAR(64) NOT NULL
        CONSTRAINT astralplane_outbox_payload_digest_check
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    idempotency_key TEXT NOT NULL UNIQUE,
    available_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'retry', 'claimed', 'succeeded', 'dead_letter')),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT astralplane_outbox_lease_shape_check CHECK (
        (
            status = 'claimed'
            AND lease_owner IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
        OR (
            status <> 'claimed'
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
        )
    ),
    CONSTRAINT astralplane_outbox_error_shape_check CHECK (
        (
            status IN ('pending', 'claimed', 'succeeded')
            AND last_error_code IS NULL
        )
        OR (
            status IN ('retry', 'dead_letter')
            AND last_error_code IS NOT NULL
        )
    )
)
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_outbox_claim_v2
ON astralplane_outbox (topic, available_at, entry_id)
WHERE status IN ('pending', 'retry')
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_outbox_lease
ON astralplane_outbox (lease_expires_at, entry_id)
WHERE status = 'claimed'
""".strip(),
    """
CREATE TABLE IF NOT EXISTS audit_retention_anchor (
    anchor_id TEXT NOT NULL UNIQUE,
    owner_or_chain TEXT NOT NULL,
    first_retained_sequence BIGINT NOT NULL CHECK (first_retained_sequence > 1),
    previous_entry_digest BYTEA NOT NULL
        CONSTRAINT audit_retention_previous_digest_check
        CHECK (octet_length(previous_entry_digest) = 32),
    retention_policy_digest BYTEA NOT NULL
        CONSTRAINT audit_retention_policy_digest_check
        CHECK (octet_length(retention_policy_digest) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    key_id TEXT NOT NULL,
    signature_or_mac BYTEA NOT NULL,
    PRIMARY KEY (owner_or_chain, first_retained_sequence)
)
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_purge_tombstone (
    tombstone_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    object_kind TEXT NOT NULL
        CHECK (object_kind IN ('attachment', 'artifact', 'knowledge', 'generated_agent')),
    object_id TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    storage_locator_sha256 CHAR(64) NOT NULL
        CONSTRAINT astralplane_purge_locator_digest_check
        CHECK (storage_locator_sha256 ~ '^[0-9a-f]{64}$'),
    requested_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'purged', 'failed', 'manual_review')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    verified_absent_at TIMESTAMPTZ,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT astralplane_purge_state_shape_check CHECK (
        (
            status = 'pending'
            AND verified_absent_at IS NULL
            AND last_error_code IS NULL
        )
        OR (
            status = 'purged'
            AND verified_absent_at IS NOT NULL
            AND last_error_code IS NULL
        )
        OR (
            status IN ('failed', 'manual_review')
            AND verified_absent_at IS NULL
            AND last_error_code IS NOT NULL
        )
    ),
    UNIQUE (owner_id, object_kind, object_id)
)
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_purge_pending
ON astralplane_purge_tombstone (status, available_at, requested_at, tombstone_id)
WHERE status <> 'purged'
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_purge_owner_pending
ON astralplane_purge_tombstone (owner_id, requested_at, tombstone_id)
WHERE status <> 'purged'
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_reconciliation_marker (
    schema_revision TEXT NOT NULL,
    plan_digest CHAR(64) NOT NULL
        CONSTRAINT astralplane_reconciliation_plan_digest_check
        CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
    hook_name TEXT NOT NULL,
    hook_version TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started', 'completed', 'failed')),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    result_digest CHAR(64)
        CONSTRAINT astralplane_reconciliation_result_digest_check
        CHECK (result_digest IS NULL OR result_digest ~ '^[0-9a-f]{64}$'),
    error_type TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT astralplane_reconciliation_state_shape_check CHECK (
        (state = 'started' AND result_digest IS NULL AND error_type IS NULL)
        OR (state = 'completed' AND result_digest IS NOT NULL AND error_type IS NULL)
        OR (state = 'failed' AND result_digest IS NULL AND error_type IS NOT NULL)
    ),
    PRIMARY KEY (schema_revision, plan_digest, hook_name, hook_version)
)
""".strip(),
    """
DO $astralplane_schema_postcondition$
DECLARE
    invalid_objects TEXT;
BEGIN
    SELECT string_agg(required.table_name || '.' || required.column_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('audit_events', 'chain_sequence', 'int8', TRUE),
            ('astralplane_outbox', 'entry_id', 'text', TRUE),
            ('astralplane_outbox', 'canonical_payload', 'bytea', TRUE),
            ('astralplane_outbox', 'payload_sha256', 'bpchar', TRUE),
            ('astralplane_outbox', 'lease_owner', 'text', FALSE),
            ('astralplane_outbox', 'lease_expires_at', 'timestamptz', FALSE),
            ('audit_retention_anchor', 'first_retained_sequence', 'int8', TRUE),
            ('audit_retention_anchor', 'previous_entry_digest', 'bytea', TRUE),
            ('audit_retention_anchor', 'retention_policy_digest', 'bytea', TRUE),
            ('astralplane_purge_tombstone', 'tombstone_id', 'text', TRUE),
            ('astralplane_purge_tombstone', 'storage_locator_sha256', 'bpchar', TRUE),
            ('astralplane_purge_tombstone', 'updated_at', 'timestamptz', TRUE),
            ('astralplane_reconciliation_marker', 'plan_digest', 'bpchar', TRUE),
            ('astralplane_reconciliation_marker', 'result_digest', 'bpchar', FALSE),
            ('astralplane_reconciliation_marker', 'error_type', 'text', FALSE)
    ) AS required(table_name, column_name, type_name, not_null)
    LEFT JOIN pg_attribute AS attribute
      ON attribute.attrelid = to_regclass(required.table_name)
     AND attribute.attname = required.column_name
     AND attribute.attnum > 0
     AND NOT attribute.attisdropped
    LEFT JOIN pg_type AS data_type
      ON data_type.oid = attribute.atttypid
    WHERE attribute.attname IS NULL
       OR data_type.typname <> required.type_name
       OR attribute.attnotnull <> required.not_null;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane schema columns are missing or incompatible: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;

    SELECT string_agg(required.constraint_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('audit_events', 'audit_events_chain_sequence_check'),
            ('audit_events', 'audit_events_digest_length_check'),
            ('audit_events', 'audit_events_schema_version_check'),
            ('astralplane_outbox', 'astralplane_outbox_payload_digest_check'),
            ('astralplane_outbox', 'astralplane_outbox_lease_shape_check'),
            ('astralplane_outbox', 'astralplane_outbox_error_shape_check'),
            ('audit_retention_anchor', 'audit_retention_previous_digest_check'),
            ('audit_retention_anchor', 'audit_retention_policy_digest_check'),
            ('astralplane_purge_tombstone', 'astralplane_purge_locator_digest_check'),
            ('astralplane_purge_tombstone', 'astralplane_purge_state_shape_check'),
            (
                'astralplane_reconciliation_marker',
                'astralplane_reconciliation_plan_digest_check'
            ),
            (
                'astralplane_reconciliation_marker',
                'astralplane_reconciliation_result_digest_check'
            ),
            (
                'astralplane_reconciliation_marker',
                'astralplane_reconciliation_state_shape_check'
            )
    ) AS required(table_name, constraint_name)
    LEFT JOIN pg_constraint AS constraint_record
      ON constraint_record.conrelid = to_regclass(required.table_name)
     AND constraint_record.conname = required.constraint_name
     AND constraint_record.convalidated
    WHERE constraint_record.oid IS NULL;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane schema constraints are missing: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;

    SELECT string_agg(required.index_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('uq_audit_events_actor_sequence'),
            ('idx_astralplane_outbox_claim_v2'),
            ('idx_astralplane_outbox_lease'),
            ('idx_astralplane_purge_pending'),
            ('idx_astralplane_purge_owner_pending')
    ) AS required(index_name)
    LEFT JOIN pg_class AS index_record
      ON index_record.oid = to_regclass(required.index_name)
     AND index_record.relkind = 'i'
    LEFT JOIN pg_index AS index_state
      ON index_state.indexrelid = index_record.oid
     AND index_state.indisvalid
     AND index_state.indisready
    WHERE index_state.indexrelid IS NULL;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane schema indexes are missing or invalid: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;

    SELECT string_agg(required.trigger_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('audit_events_no_update'),
            ('audit_events_assign_sequence')
    ) AS required(trigger_name)
    LEFT JOIN pg_trigger AS trigger_record
      ON trigger_record.tgrelid = 'audit_events'::regclass
     AND trigger_record.tgname = required.trigger_name
     AND trigger_record.tgenabled = 'O'
     AND NOT trigger_record.tgisinternal
    WHERE trigger_record.oid IS NULL;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane audit triggers are missing or disabled: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;
END
$astralplane_schema_postcondition$
""".strip(),
)


@dataclass(frozen=True, slots=True)
class Migration:
    """One declared, repeat-safe database-only migration edge."""

    name: str
    source_revisions: tuple[str | None, ...]
    target_revision: str
    checksum: str
    operation: MigrationCallable

    def __post_init__(self) -> None:
        if not self.name or not self.name.isascii():
            raise MigrationDefinitionError("migration name must be non-empty ASCII")
        if not isinstance(self.source_revisions, tuple) or not self.source_revisions:
            raise MigrationDefinitionError("migration must declare at least one source revision")
        normalized_sources = tuple(
            None if source is None else validate_revision(source, field="migration source")
            for source in self.source_revisions
        )
        if len(set(normalized_sources)) != len(normalized_sources):
            raise MigrationDefinitionError("migration source revisions must be unique")
        target = validate_revision(self.target_revision, field="migration target")
        if target in normalized_sources:
            raise MigrationDefinitionError("migration target must differ from every source")
        if _SHA256_PATTERN.fullmatch(self.checksum) is None:
            raise MigrationDefinitionError("migration checksum must be lowercase SHA-256")
        if not callable(self.operation):
            raise MigrationDefinitionError("migration operation must be callable")

    def apply(self, transaction: Transaction) -> None:
        self.operation(transaction)


class MigrationRegistry:
    """Immutable, non-branching migration graph with a canonical digest."""

    def __init__(self, migrations: tuple[Migration, ...]) -> None:
        if not isinstance(migrations, tuple) or not migrations:
            raise MigrationDefinitionError("migration registry must not be empty")
        by_source: dict[str | None, Migration] = {}
        names: set[str] = set()
        for migration in migrations:
            if migration.name in names:
                raise MigrationDefinitionError("migration names must be unique")
            names.add(migration.name)
            for source in migration.source_revisions:
                if source in by_source:
                    raise MigrationDefinitionError(
                        f"multiple migrations claim source revision {source!r}"
                    )
                by_source[source] = migration
        self._migrations = tuple(migrations)
        self._by_source = by_source
        manifest = [
            {
                "checksum": migration.checksum,
                "name": migration.name,
                "source_revisions": [
                    "<empty>" if source is None else source for source in migration.source_revisions
                ],
                "target_revision": migration.target_revision,
            }
            for migration in sorted(migrations, key=lambda item: item.name)
        ]
        canonical = json.dumps(
            manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        self.digest = hashlib.sha256(canonical).hexdigest()

    @property
    def migrations(self) -> tuple[Migration, ...]:
        return self._migrations

    def next_after(self, revision: str | None) -> Migration | None:
        return self._by_source.get(revision)


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """Detached evidence from one committed migration transaction."""

    source_revision: str | None
    target_revision: str
    applied_steps: tuple[str, ...]
    already_current: bool
    migration_digest: str


class MigrationRunner:
    """Serialize one declared migration path under a PostgreSQL advisory lock."""

    def __init__(
        self,
        database: PlaneDatabase,
        *,
        revision: DataPlaneRevision,
        registry: MigrationRegistry,
    ) -> None:
        if registry.digest != revision.migration_digest:
            raise MigrationDefinitionError(
                "migration registry digest does not match the declared data-plane revision",
                metadata={
                    "declared": revision.migration_digest,
                    "observed": registry.digest,
                },
            )
        self._database = database
        self.revision = revision
        self.registry = registry

    def _read_metadata(self, transaction: Transaction) -> dict[str, str]:
        records = transaction.fetch_all(
            _READ_SCHEMA_META_SQL,
            (_SCHEMA_META_REVISION, _SCHEMA_META_DIGEST),
        )
        metadata: dict[str, str] = {}
        for record in records:
            key = str(record["key"])
            if key in metadata:
                raise SchemaRevisionError("schema metadata contains duplicate keys")
            metadata[key] = str(record["value"])
        return metadata

    @staticmethod
    def _write_metadata(transaction: Transaction, key: str, value: str) -> None:
        transaction.execute(_WRITE_SCHEMA_META_SQL, (key, value))

    def run(self, *, expected_revision: str) -> MigrationReport:
        expected = validate_revision(expected_revision, field="expected revision")
        if expected != self.revision.schema_revision:
            raise SchemaRevisionError(
                "composition expected a different data-plane revision",
                metadata={"declared": self.revision.schema_revision, "expected": expected},
            )

        lock_key, lock_id = self.revision.migration_lock
        with self._database.transaction() as transaction:
            transaction.fetch_one(_ADVISORY_LOCK_SQL, (lock_key, lock_id))
            transaction.execute(_CREATE_SCHEMA_META_SQL)
            metadata = self._read_metadata(transaction)
            source_revision = metadata.get(_SCHEMA_META_REVISION)
            if source_revision is not None:
                validate_revision(source_revision, field="stored schema revision")
            stored_digest = metadata.get(_SCHEMA_META_DIGEST)

            if source_revision == expected:
                if stored_digest is None:
                    raise SchemaRevisionError(
                        "current schema is missing migration-set evidence",
                        metadata={"revision": source_revision},
                    )
                if stored_digest != self.registry.digest:
                    raise SchemaRevisionError(
                        "current schema carries a different migration-set digest",
                        metadata={"revision": source_revision},
                    )
                return MigrationReport(
                    source_revision=source_revision,
                    target_revision=expected,
                    applied_steps=(),
                    already_current=True,
                    migration_digest=self.registry.digest,
                )

            if stored_digest is not None:
                raise SchemaRevisionError(
                    "predecessor schema carries an unrecognized migration-set digest",
                    metadata={"revision": source_revision or "<empty>"},
                )

            applied: list[str] = []
            observed = source_revision
            visited: set[str | None] = set()
            while observed != expected:
                if observed in visited:
                    raise MigrationDefinitionError("migration registry contains a cycle")
                visited.add(observed)
                migration = self.registry.next_after(observed)
                if migration is None:
                    raise SchemaRevisionError(
                        "no declared migration path from the observed schema",
                        metadata={"observed": observed or "<empty>", "target": expected},
                    )
                migration.apply(transaction)
                observed = migration.target_revision
                self._write_metadata(transaction, _SCHEMA_META_REVISION, observed)
                applied.append(migration.name)

            self._write_metadata(transaction, _SCHEMA_META_DIGEST, self.registry.digest)
            return MigrationReport(
                source_revision=source_revision,
                target_revision=expected,
                applied_steps=tuple(applied),
                already_current=False,
                migration_digest=self.registry.digest,
            )


def _apply_plane_schema_067(transaction: Transaction) -> None:
    for statement in PLANE_SCHEMA_067_STATEMENTS:
        transaction.execute(statement)


def _statements_checksum(statements: tuple[str, ...]) -> str:
    canonical = json.dumps(
        statements,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


PLANE_SCHEMA_067_MIGRATION: Final = Migration(
    name="astralplane-067-transactional-recovery",
    source_revisions=("066.001",),
    target_revision="067.001",
    checksum=_statements_checksum(PLANE_SCHEMA_067_STATEMENTS),
    operation=_apply_plane_schema_067,
)
MIGRATION_REGISTRY: Final = MigrationRegistry((PLANE_SCHEMA_067_MIGRATION,))
MIGRATION_DIGEST: Final = MIGRATION_REGISTRY.digest
CURRENT_DATA_PLANE_REVISION: Final = DataPlaneRevision(
    schema_revision="067.001",
    read_compatible_from=("066.001",),
    migration_digest=MIGRATION_DIGEST,
)


__all__ = (
    "CURRENT_DATA_PLANE_REVISION",
    "MIGRATION_DIGEST",
    "MIGRATION_REGISTRY",
    "PLANE_SCHEMA_067_MIGRATION",
    "PLANE_SCHEMA_067_STATEMENTS",
    "Migration",
    "MigrationRegistry",
    "MigrationReport",
    "MigrationRunner",
)
