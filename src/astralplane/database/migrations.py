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

_PLANE_SCHEMA_067_PREFIX: Final = (
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
)

PLANE_SCHEMA_074_STATEMENTS: Final = (
    """
CREATE OR REPLACE FUNCTION astralplane_identifier_is_canonical(candidate TEXT)
RETURNS BOOLEAN AS $astralplane_identifier$
    SELECT candidate ~ '^[A-Za-z0-9]([A-Za-z0-9._:@/-]{0,126}[A-Za-z0-9])?$'
$astralplane_identifier$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
""".strip(),
    """
CREATE OR REPLACE FUNCTION astralplane_capabilities_are_canonical(candidate TEXT[])
RETURNS BOOLEAN AS $astralplane_capabilities$
    SELECT cardinality(candidate) > 0
       AND array_ndims(candidate) = 1
       AND array_lower(candidate, 1) = 1
       AND NOT EXISTS (
            SELECT 1
            FROM generate_subscripts(candidate, 1) AS item(position)
            WHERE NOT astralplane_identifier_is_canonical(candidate[position])
               OR (
                    position > 1
                    AND candidate[position - 1] COLLATE "C"
                        >= candidate[position] COLLATE "C"
               )
       )
$astralplane_capabilities$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_authority_binding (
    binding_id VARCHAR(128) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    runtime_id VARCHAR(128) NOT NULL,
    runtime_generation BIGINT NOT NULL,
    population TEXT NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    envelope_id VARCHAR(128) NOT NULL,
    warden_id VARCHAR(128) NOT NULL,
    lease_id VARCHAR(128) NOT NULL,
    lineage_id VARCHAR(128) NOT NULL,
    subject_id VARCHAR(128) NOT NULL,
    policy_digest CHAR(71) NOT NULL,
    machine_digest CHAR(71) NOT NULL,
    config_epoch BIGINT NOT NULL,
    capabilities TEXT[] NOT NULL,
    lease_sequence BIGINT NOT NULL,
    lease_expires_at_ns BIGINT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    CONSTRAINT astralplane_authority_binding_pk PRIMARY KEY (binding_id),
    CONSTRAINT astralplane_authority_binding_owner_key
        UNIQUE (owner_id, binding_id),
    CONSTRAINT astralplane_authority_binding_runtime_key
        UNIQUE (owner_id, agent_id, runtime_id, runtime_generation),
    CONSTRAINT astralplane_authority_binding_identity_check CHECK (
        astralplane_identifier_is_canonical(binding_id)
        AND astralplane_identifier_is_canonical(owner_id)
        AND astralplane_identifier_is_canonical(agent_id)
        AND astralplane_identifier_is_canonical(runtime_id)
        AND astralplane_identifier_is_canonical(tenant_id)
        AND astralplane_identifier_is_canonical(envelope_id)
        AND astralplane_identifier_is_canonical(warden_id)
        AND astralplane_identifier_is_canonical(lease_id)
        AND astralplane_identifier_is_canonical(lineage_id)
        AND astralplane_identifier_is_canonical(subject_id)
    ),
    CONSTRAINT astralplane_authority_binding_population_check
        CHECK (population IN ('server_dynamic', 'byo_user')),
    CONSTRAINT astralplane_authority_binding_digest_check CHECK (
        policy_digest ~ '^sha256:[0-9a-f]{64}$'
        AND machine_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    CONSTRAINT astralplane_authority_binding_capabilities_check
        CHECK (astralplane_capabilities_are_canonical(capabilities)),
    CONSTRAINT astralplane_authority_binding_numeric_check CHECK (
        runtime_generation > 0
        AND config_epoch > 0
        AND lease_sequence >= 0
        AND lease_expires_at_ns > 0
        AND version >= 0
    ),
    CONSTRAINT astralplane_authority_binding_state_check CHECK (
        state IN (
            'provisioning', 'active', 'quiescent', 'closing', 'closed',
            'revoking', 'revoked', 'reconciling', 'expired'
        )
    ),
    CONSTRAINT astralplane_authority_binding_time_check
        CHECK (updated_at >= created_at)
)
""".strip(),
    """
CREATE UNIQUE INDEX IF NOT EXISTS uq_astralplane_authority_binding_nonterminal
ON astralplane_authority_binding (owner_id, agent_id, population)
WHERE state IN (
    'provisioning', 'active', 'quiescent', 'closing', 'revoking', 'reconciling'
)
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_authority_lifecycle_operation (
    operation_id VARCHAR(128) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    binding_id VARCHAR(128) NOT NULL,
    kind TEXT NOT NULL,
    expected_binding_version BIGINT NOT NULL,
    expected_lease_sequence BIGINT,
    request_fingerprint CHAR(64) NOT NULL,
    status TEXT NOT NULL,
    remote_request_id VARCHAR(128) NOT NULL,
    result_digest CHAR(64),
    error_code VARCHAR(128),
    attempt_count INTEGER NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    reconciled_at TIMESTAMPTZ,
    reconciliation_digest CHAR(64),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    CONSTRAINT astralplane_authority_lifecycle_operation_pk PRIMARY KEY (operation_id),
    CONSTRAINT astralplane_authority_lifecycle_owner_key
        UNIQUE (owner_id, operation_id),
    CONSTRAINT astralplane_authority_lifecycle_binding_fk
        FOREIGN KEY (owner_id, binding_id)
        REFERENCES astralplane_authority_binding (owner_id, binding_id),
    CONSTRAINT astralplane_authority_lifecycle_identity_check CHECK (
        astralplane_identifier_is_canonical(operation_id)
        AND astralplane_identifier_is_canonical(owner_id)
        AND astralplane_identifier_is_canonical(binding_id)
        AND astralplane_identifier_is_canonical(remote_request_id)
        AND remote_request_id = operation_id
        AND (error_code IS NULL OR astralplane_identifier_is_canonical(error_code))
    ),
    CONSTRAINT astralplane_authority_lifecycle_kind_check CHECK (
        kind IN ('provision', 'spawn', 'renew', 'quiesce', 'resume', 'close', 'revoke', 'reconcile')
    ),
    CONSTRAINT astralplane_authority_lifecycle_numeric_check CHECK (
        expected_binding_version >= 0
        AND (expected_lease_sequence IS NULL OR expected_lease_sequence >= 0)
        AND attempt_count >= 0
        AND version >= 0
    ),
    CONSTRAINT astralplane_authority_lifecycle_digest_check CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
        AND (result_digest IS NULL OR result_digest ~ '^[0-9a-f]{64}$')
        AND (
            reconciliation_digest IS NULL
            OR reconciliation_digest ~ '^[0-9a-f]{64}$'
        )
    ),
    CONSTRAINT astralplane_authority_lifecycle_status_check CHECK (
        status IN ('pending', 'in_flight', 'succeeded', 'failed', 'uncertain', 'reconciled')
    ),
    CONSTRAINT astralplane_authority_lifecycle_attempt_check CHECK (
        ((attempt_count = 0 AND last_attempt_at IS NULL)
            OR (attempt_count > 0 AND last_attempt_at IS NOT NULL))
        AND (status = 'pending' OR attempt_count > 0)
    ),
    CONSTRAINT astralplane_authority_lifecycle_result_check CHECK (
        (status IN ('pending', 'in_flight') AND result_digest IS NULL AND error_code IS NULL)
        OR (status = 'succeeded' AND result_digest IS NOT NULL AND error_code IS NULL)
        OR (status IN ('failed', 'uncertain') AND result_digest IS NULL AND error_code IS NOT NULL)
        OR (
            status = 'reconciled'
            AND ((result_digest IS NOT NULL) <> (error_code IS NOT NULL))
        )
    ),
    CONSTRAINT astralplane_authority_lifecycle_reconciliation_check CHECK (
        (
            status = 'reconciled'
            AND reconciled_at IS NOT NULL
            AND reconciliation_digest IS NOT NULL
        )
        OR (
            status <> 'reconciled'
            AND reconciled_at IS NULL
            AND reconciliation_digest IS NULL
        )
    ),
    CONSTRAINT astralplane_authority_lifecycle_time_check CHECK (
        updated_at >= created_at
        AND (next_attempt_at IS NULL OR next_attempt_at >= created_at)
        AND (last_attempt_at IS NULL OR last_attempt_at >= created_at)
        AND (reconciled_at IS NULL OR reconciled_at >= created_at)
        AND (
            status NOT IN ('succeeded', 'failed', 'reconciled')
            OR next_attempt_at IS NULL
        )
    )
)
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_authority_lifecycle_work
ON astralplane_authority_lifecycle_operation (
    owner_id, binding_id, status, next_attempt_at, operation_id
)
WHERE status IN ('pending', 'in_flight', 'uncertain')
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_protected_effect_operation (
    operation_id VARCHAR(128) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    binding_id VARCHAR(128) NOT NULL,
    tool_id VARCHAR(128) NOT NULL,
    astral_scope TEXT NOT NULL,
    lets_capability VARCHAR(128) NOT NULL,
    lets_transition VARCHAR(128) NOT NULL,
    executor_audience VARCHAR(128) NOT NULL,
    nonce VARCHAR(256) NOT NULL,
    effect_digest CHAR(64) NOT NULL,
    expected_sequence BIGINT NOT NULL,
    audit_correlation_id VARCHAR(128) NOT NULL,
    status TEXT NOT NULL,
    receipt_id VARCHAR(128),
    receipt_digest CHAR(64),
    effect_result_digest CHAR(64),
    error_code VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    CONSTRAINT astralplane_protected_effect_operation_pk PRIMARY KEY (operation_id),
    CONSTRAINT astralplane_protected_effect_owner_key UNIQUE (owner_id, operation_id),
    CONSTRAINT astralplane_protected_effect_receipt_key UNIQUE (receipt_id),
    CONSTRAINT astralplane_protected_effect_nonce_key
        UNIQUE (owner_id, binding_id, executor_audience, nonce),
    CONSTRAINT astralplane_protected_effect_binding_fk
        FOREIGN KEY (owner_id, binding_id)
        REFERENCES astralplane_authority_binding (owner_id, binding_id),
    CONSTRAINT astralplane_protected_effect_identity_check CHECK (
        astralplane_identifier_is_canonical(operation_id)
        AND astralplane_identifier_is_canonical(owner_id)
        AND astralplane_identifier_is_canonical(agent_id)
        AND astralplane_identifier_is_canonical(binding_id)
        AND astralplane_identifier_is_canonical(tool_id)
        AND astralplane_identifier_is_canonical(lets_capability)
        AND astralplane_identifier_is_canonical(lets_transition)
        AND astralplane_identifier_is_canonical(executor_audience)
        AND astralplane_identifier_is_canonical(audit_correlation_id)
        AND (receipt_id IS NULL OR astralplane_identifier_is_canonical(receipt_id))
        AND (error_code IS NULL OR astralplane_identifier_is_canonical(error_code))
        AND length(nonce) BETWEEN 16 AND 256
        AND nonce = btrim(nonce)
        AND nonce !~ '[[:cntrl:]]'
    ),
    CONSTRAINT astralplane_protected_effect_scope_check CHECK (
        astral_scope IN (
            'tools:read', 'tools:write', 'tools:search',
            'tools:system', 'tools:files', 'tools:execute'
        )
    ),
    CONSTRAINT astralplane_protected_effect_digest_check CHECK (
        effect_digest ~ '^[0-9a-f]{64}$'
        AND (receipt_digest IS NULL OR receipt_digest ~ '^[0-9a-f]{64}$')
        AND (
            effect_result_digest IS NULL
            OR effect_result_digest ~ '^[0-9a-f]{64}$'
        )
    ),
    CONSTRAINT astralplane_protected_effect_numeric_check
        CHECK (expected_sequence >= 0 AND version >= 0),
    CONSTRAINT astralplane_protected_effect_status_check CHECK (
        status IN (
            'created', 'astral_authorized', 'lets_pending', 'receipt_received',
            'receipt_claimed', 'executing', 'succeeded', 'denied', 'failed_closed',
            'effect_failed', 'outcome_uncertain'
        )
    ),
    CONSTRAINT astralplane_protected_effect_receipt_shape_check CHECK (
        ((receipt_id IS NULL) = (receipt_digest IS NULL))
        AND (
            status NOT IN (
                'receipt_received', 'receipt_claimed', 'executing', 'succeeded',
                'effect_failed', 'outcome_uncertain'
            )
            OR receipt_id IS NOT NULL
        )
        AND (
            status NOT IN ('created', 'astral_authorized', 'lets_pending')
            OR receipt_id IS NULL
        )
    ),
    CONSTRAINT astralplane_protected_effect_result_shape_check CHECK (
        ((status IN ('denied', 'failed_closed', 'effect_failed', 'outcome_uncertain'))
            = (error_code IS NOT NULL))
        AND ((status IN ('succeeded', 'effect_failed')) = (effect_result_digest IS NOT NULL))
    ),
    CONSTRAINT astralplane_protected_effect_time_check
        CHECK (updated_at >= created_at)
)
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_protected_effect_work
ON astralplane_protected_effect_operation (owner_id, binding_id, status, updated_at, operation_id)
WHERE status NOT IN ('succeeded', 'denied', 'failed_closed', 'effect_failed')
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_protected_effect_audit
ON astralplane_protected_effect_operation (owner_id, audit_correlation_id, operation_id)
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_receipt_sequence_watermark (
    warden_id VARCHAR(128) NOT NULL,
    lease_id VARCHAR(128) NOT NULL,
    audience VARCHAR(128) NOT NULL,
    last_sequence BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    expires_at_ns BIGINT NOT NULL,
    version BIGINT NOT NULL,
    CONSTRAINT astralplane_receipt_watermark_pk PRIMARY KEY (warden_id, lease_id, audience),
    CONSTRAINT astralplane_receipt_watermark_identity_check CHECK (
        astralplane_identifier_is_canonical(warden_id)
        AND astralplane_identifier_is_canonical(lease_id)
        AND astralplane_identifier_is_canonical(audience)
    ),
    CONSTRAINT astralplane_receipt_watermark_numeric_check
        CHECK (last_sequence > 0 AND expires_at_ns > 0 AND version >= 0)
)
""".strip(),
    """
CREATE OR REPLACE FUNCTION astralplane_receipt_watermark_require_advance()
RETURNS trigger AS $astralplane_watermark_advance$
BEGIN
    IF ROW(NEW.warden_id, NEW.lease_id, NEW.audience)
        IS DISTINCT FROM ROW(OLD.warden_id, OLD.lease_id, OLD.audience)
        OR NEW.last_sequence <= OLD.last_sequence
        OR NEW.version <> OLD.version + 1
        OR NEW.updated_at < OLD.updated_at
    THEN
        RAISE EXCEPTION 'receipt watermark update must strictly advance its fenced sequence'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$astralplane_watermark_advance$ LANGUAGE plpgsql
""".strip(),
    (
        "DROP TRIGGER IF EXISTS astralplane_receipt_watermark_advance "
        "ON astralplane_receipt_sequence_watermark"
    ),
    """
CREATE TRIGGER astralplane_receipt_watermark_advance
BEFORE UPDATE ON astralplane_receipt_sequence_watermark
FOR EACH ROW EXECUTE FUNCTION astralplane_receipt_watermark_require_advance()
""".strip(),
    """
CREATE TABLE IF NOT EXISTS astralplane_receipt_claim (
    receipt_id VARCHAR(128) NOT NULL,
    operation_id VARCHAR(128) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    binding_id VARCHAR(128) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    envelope_id VARCHAR(128) NOT NULL,
    warden_id VARCHAR(128) NOT NULL,
    lease_id VARCHAR(128) NOT NULL,
    subject_id VARCHAR(128) NOT NULL,
    lineage_id VARCHAR(128) NOT NULL,
    policy_digest CHAR(71) NOT NULL,
    machine_digest CHAR(71) NOT NULL,
    config_epoch BIGINT NOT NULL,
    audience VARCHAR(128) NOT NULL,
    transition VARCHAR(128) NOT NULL,
    nonce VARCHAR(128) NOT NULL,
    resulting_sequence BIGINT NOT NULL,
    evidence_digest CHAR(71),
    issued_at_ns BIGINT NOT NULL,
    expires_at_ns BIGINT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL,
    canonical_digest CHAR(64) NOT NULL,
    anchor_format TEXT NOT NULL,
    anchor_executor_policy_sha256 CHAR(64) NOT NULL,
    anchor_trust_registry_sha256 CHAR(64) NOT NULL,
    anchor_schema_version BIGINT NOT NULL,
    anchor_database_instance_id CHAR(64) NOT NULL,
    anchor_claim_sequence BIGINT NOT NULL,
    anchor_claim_digest CHAR(64) NOT NULL,
    anchor_clock_floor_ns BIGINT NOT NULL,
    anchor_confirmed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT astralplane_receipt_claim_pk PRIMARY KEY (receipt_id),
    CONSTRAINT astralplane_receipt_claim_operation_key UNIQUE (operation_id),
    CONSTRAINT astralplane_receipt_claim_nonce_key
        UNIQUE (tenant_id, envelope_id, audience, nonce),
    CONSTRAINT astralplane_receipt_claim_sequence_key
        UNIQUE (warden_id, lease_id, audience, resulting_sequence),
    CONSTRAINT astralplane_receipt_claim_digest_key UNIQUE (canonical_digest),
    CONSTRAINT astralplane_receipt_claim_binding_fk
        FOREIGN KEY (owner_id, binding_id)
        REFERENCES astralplane_authority_binding (owner_id, binding_id),
    CONSTRAINT astralplane_receipt_claim_effect_fk
        FOREIGN KEY (owner_id, operation_id)
        REFERENCES astralplane_protected_effect_operation (owner_id, operation_id),
    CONSTRAINT astralplane_receipt_claim_identity_check CHECK (
        astralplane_identifier_is_canonical(receipt_id)
        AND astralplane_identifier_is_canonical(operation_id)
        AND astralplane_identifier_is_canonical(owner_id)
        AND astralplane_identifier_is_canonical(binding_id)
        AND astralplane_identifier_is_canonical(tenant_id)
        AND astralplane_identifier_is_canonical(envelope_id)
        AND astralplane_identifier_is_canonical(warden_id)
        AND astralplane_identifier_is_canonical(lease_id)
        AND astralplane_identifier_is_canonical(subject_id)
        AND astralplane_identifier_is_canonical(lineage_id)
        AND astralplane_identifier_is_canonical(audience)
        AND astralplane_identifier_is_canonical(transition)
        AND astralplane_identifier_is_canonical(nonce)
    ),
    CONSTRAINT astralplane_receipt_claim_lets_digest_check CHECK (
        policy_digest ~ '^sha256:[0-9a-f]{64}$'
        AND machine_digest ~ '^sha256:[0-9a-f]{64}$'
        AND (evidence_digest IS NULL OR evidence_digest ~ '^sha256:[0-9a-f]{64}$')
    ),
    CONSTRAINT astralplane_receipt_claim_numeric_check CHECK (
        config_epoch > 0
        AND resulting_sequence > 0
        AND issued_at_ns >= 0
        AND expires_at_ns > issued_at_ns
    ),
    CONSTRAINT astralplane_receipt_claim_digest_check CHECK (
        canonical_digest ~ '^[0-9a-f]{64}$'
        AND anchor_executor_policy_sha256 ~ '^[0-9a-f]{64}$'
        AND anchor_trust_registry_sha256 ~ '^[0-9a-f]{64}$'
        AND anchor_database_instance_id ~ '^[0-9a-f]{64}$'
        AND anchor_claim_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT astralplane_receipt_claim_anchor_check CHECK (
        anchor_format = 'LETS-EXECUTOR-AUTHORITY-ANCHOR/1'
        AND anchor_schema_version > 0
        AND anchor_claim_sequence > 0
        AND anchor_clock_floor_ns >= 0
        AND anchor_confirmed_at >= claimed_at
    )
)
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_receipt_claim_binding
ON astralplane_receipt_claim (owner_id, binding_id, claimed_at, receipt_id)
""".strip(),
    """
DO $astralplane_authority_outbox_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'astralplane_outbox'::regclass
          AND conname = 'astralplane_outbox_payload_size_check'
    ) THEN
        ALTER TABLE astralplane_outbox
        ADD CONSTRAINT astralplane_outbox_payload_size_check
        CHECK (octet_length(canonical_payload) BETWEEN 1 AND 1048576) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'astralplane_outbox'::regclass
          AND conname = 'astralplane_outbox_topic_format_check'
    ) THEN
        ALTER TABLE astralplane_outbox
        ADD CONSTRAINT astralplane_outbox_topic_format_check
        CHECK (topic ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') NOT VALID;
    END IF;
    ALTER TABLE astralplane_outbox
        VALIDATE CONSTRAINT astralplane_outbox_payload_size_check;
    ALTER TABLE astralplane_outbox
        VALIDATE CONSTRAINT astralplane_outbox_topic_format_check;
END
$astralplane_authority_outbox_constraints$
""".strip(),
    """
CREATE INDEX IF NOT EXISTS idx_astralplane_outbox_authority_pending
ON astralplane_outbox (topic, available_at, entry_id)
WHERE status IN ('pending', 'retry') AND topic LIKE 'authority.%'
""".strip(),
    """
DO $astralplane_authority_postcondition$
DECLARE
    invalid_objects TEXT;
BEGIN
    SELECT string_agg(required.table_name || '.' || required.column_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('astralplane_authority_binding', 'binding_id', 'varchar', TRUE),
            ('astralplane_authority_binding', 'runtime_generation', 'int8', TRUE),
            ('astralplane_authority_binding', 'capabilities', '_text', TRUE),
            ('astralplane_authority_binding', 'state', 'text', TRUE),
            ('astralplane_authority_lifecycle_operation', 'operation_id', 'varchar', TRUE),
            ('astralplane_authority_lifecycle_operation', 'request_fingerprint', 'bpchar', TRUE),
            ('astralplane_authority_lifecycle_operation', 'reconciliation_digest', 'bpchar', FALSE),
            ('astralplane_protected_effect_operation', 'operation_id', 'varchar', TRUE),
            ('astralplane_protected_effect_operation', 'nonce', 'varchar', TRUE),
            ('astralplane_protected_effect_operation', 'effect_digest', 'bpchar', TRUE),
            ('astralplane_receipt_sequence_watermark', 'last_sequence', 'int8', TRUE),
            ('astralplane_receipt_sequence_watermark', 'version', 'int8', TRUE),
            ('astralplane_receipt_claim', 'receipt_id', 'varchar', TRUE),
            ('astralplane_receipt_claim', 'canonical_digest', 'bpchar', TRUE),
            ('astralplane_receipt_claim', 'anchor_claim_digest', 'bpchar', TRUE),
            ('astralplane_receipt_claim', 'anchor_clock_floor_ns', 'int8', TRUE),
            ('astralplane_outbox', 'topic', 'text', TRUE),
            ('astralplane_outbox', 'canonical_payload', 'bytea', TRUE),
            ('astralplane_outbox', 'payload_sha256', 'bpchar', TRUE)
    ) AS required(table_name, column_name, type_name, not_null)
    LEFT JOIN pg_attribute AS attribute
      ON attribute.attrelid = to_regclass(required.table_name)
     AND attribute.attname = required.column_name
     AND attribute.attnum > 0
     AND NOT attribute.attisdropped
    LEFT JOIN pg_type AS data_type ON data_type.oid = attribute.atttypid
    WHERE attribute.attname IS NULL
       OR data_type.typname <> required.type_name
       OR attribute.attnotnull <> required.not_null;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane authority columns are missing or incompatible: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;

    SELECT string_agg(required.constraint_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('astralplane_authority_binding', 'astralplane_authority_binding_pk', 'p'),
            ('astralplane_authority_binding', 'astralplane_authority_binding_owner_key', 'u'),
            ('astralplane_authority_binding', 'astralplane_authority_binding_runtime_key', 'u'),
            ('astralplane_authority_binding', 'astralplane_authority_binding_identity_check', 'c'),
            (
                'astralplane_authority_binding',
                'astralplane_authority_binding_capabilities_check',
                'c'
            ),
            (
                'astralplane_authority_lifecycle_operation',
                'astralplane_authority_lifecycle_operation_pk',
                'p'
            ),
            (
                'astralplane_authority_lifecycle_operation',
                'astralplane_authority_lifecycle_binding_fk',
                'f'
            ),
            (
                'astralplane_authority_lifecycle_operation',
                'astralplane_authority_lifecycle_result_check',
                'c'
            ),
            (
                'astralplane_protected_effect_operation',
                'astralplane_protected_effect_operation_pk',
                'p'
            ),
            (
                'astralplane_protected_effect_operation',
                'astralplane_protected_effect_binding_fk',
                'f'
            ),
            (
                'astralplane_protected_effect_operation',
                'astralplane_protected_effect_nonce_key',
                'u'
            ),
            (
                'astralplane_protected_effect_operation',
                'astralplane_protected_effect_receipt_shape_check',
                'c'
            ),
            ('astralplane_receipt_sequence_watermark', 'astralplane_receipt_watermark_pk', 'p'),
            ('astralplane_receipt_claim', 'astralplane_receipt_claim_pk', 'p'),
            ('astralplane_receipt_claim', 'astralplane_receipt_claim_nonce_key', 'u'),
            ('astralplane_receipt_claim', 'astralplane_receipt_claim_sequence_key', 'u'),
            ('astralplane_receipt_claim', 'astralplane_receipt_claim_binding_fk', 'f'),
            ('astralplane_receipt_claim', 'astralplane_receipt_claim_effect_fk', 'f'),
            ('astralplane_receipt_claim', 'astralplane_receipt_claim_anchor_check', 'c'),
            ('astralplane_outbox', 'astralplane_outbox_payload_digest_check', 'c'),
            ('astralplane_outbox', 'astralplane_outbox_payload_size_check', 'c'),
            ('astralplane_outbox', 'astralplane_outbox_topic_format_check', 'c')
    ) AS required(table_name, constraint_name, constraint_type)
    LEFT JOIN pg_constraint AS constraint_record
      ON constraint_record.conrelid = to_regclass(required.table_name)
     AND constraint_record.conname = required.constraint_name
     AND constraint_record.contype = required.constraint_type::"char"
     AND constraint_record.convalidated
    WHERE constraint_record.oid IS NULL;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane authority constraints are missing: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;

    SELECT string_agg(required.index_name, ', ')
    INTO invalid_objects
    FROM (
        VALUES
            ('uq_astralplane_authority_binding_nonterminal', TRUE, TRUE),
            ('idx_astralplane_authority_lifecycle_work', FALSE, TRUE),
            ('idx_astralplane_protected_effect_work', FALSE, TRUE),
            ('idx_astralplane_protected_effect_audit', FALSE, FALSE),
            ('idx_astralplane_receipt_claim_binding', FALSE, FALSE),
            ('idx_astralplane_outbox_authority_pending', FALSE, TRUE)
    ) AS required(index_name, is_unique, is_partial)
    LEFT JOIN pg_class AS index_record
      ON index_record.oid = to_regclass(required.index_name)
     AND index_record.relkind = 'i'
    LEFT JOIN pg_index AS index_state
      ON index_state.indexrelid = index_record.oid
     AND index_state.indisvalid
     AND index_state.indisready
     AND index_state.indisunique = required.is_unique
     AND (index_state.indpred IS NOT NULL) = required.is_partial
    WHERE index_state.indexrelid IS NULL;
    IF invalid_objects IS NOT NULL THEN
        RAISE EXCEPTION 'AstralPlane authority indexes are missing or invalid: %',
            invalid_objects USING ERRCODE = '42P16';
    END IF;

    IF to_regprocedure('astralplane_identifier_is_canonical(text)') IS NULL
       OR to_regprocedure('astralplane_capabilities_are_canonical(text[])') IS NULL
       OR to_regprocedure('astralplane_receipt_watermark_require_advance()') IS NULL
    THEN
        RAISE EXCEPTION 'AstralPlane authority validation functions are missing'
            USING ERRCODE = '42P16';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'astralplane_receipt_sequence_watermark'::regclass
          AND tgname = 'astralplane_receipt_watermark_advance'
          AND tgenabled = 'O'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'AstralPlane receipt watermark trigger is missing or disabled'
            USING ERRCODE = '42P16';
    END IF;
END
$astralplane_authority_postcondition$
""".strip(),
)

_PLANE_SCHEMA_067_REMAINDER: Final = (
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

PLANE_SCHEMA_067_STATEMENTS: Final = (
    *_PLANE_SCHEMA_067_PREFIX,
    *_PLANE_SCHEMA_067_REMAINDER,
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

            accepted_predecessor_digest = self.revision.predecessor_digest_for(source_revision)
            if accepted_predecessor_digest is not None:
                if stored_digest is None:
                    raise SchemaRevisionError(
                        "predecessor schema is missing required migration-set evidence",
                        metadata={"revision": source_revision or "<empty>"},
                    )
                if stored_digest != accepted_predecessor_digest:
                    raise SchemaRevisionError(
                        "predecessor schema carries an unrecognized migration-set digest",
                        metadata={"revision": source_revision or "<empty>"},
                    )
            elif stored_digest is not None:
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


def _apply_plane_schema_074(transaction: Transaction) -> None:
    for statement in PLANE_SCHEMA_074_STATEMENTS:
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
PLANE_SCHEMA_067_REGISTRY_DIGEST: Final = (
    "ae2285e6764cf084eeaf6099443d85fb9b27ae930fcb0684e4e0f458d17bb4f9"
)
if MigrationRegistry((PLANE_SCHEMA_067_MIGRATION,)).digest != PLANE_SCHEMA_067_REGISTRY_DIGEST:
    raise MigrationDefinitionError(
        "historical 067 migration registry no longer matches its pinned digest"
    )
PLANE_SCHEMA_074_MIGRATION: Final = Migration(
    name="astralplane-074-lets-authority",
    source_revisions=("067.001",),
    target_revision="074.001",
    checksum=_statements_checksum(PLANE_SCHEMA_074_STATEMENTS),
    operation=_apply_plane_schema_074,
)
MIGRATION_REGISTRY: Final = MigrationRegistry((PLANE_SCHEMA_074_MIGRATION,))
MIGRATION_DIGEST: Final = MIGRATION_REGISTRY.digest
CURRENT_DATA_PLANE_REVISION: Final = DataPlaneRevision(
    schema_revision="074.001",
    read_compatible_from=("067.001",),
    migration_digest=MIGRATION_DIGEST,
    accepted_predecessor_digests=(("067.001", PLANE_SCHEMA_067_REGISTRY_DIGEST),),
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
