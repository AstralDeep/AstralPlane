"""Executable 066.001 through the current 075.001 recovery path."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import astralplane.database.migrations as migrations_module
from astralplane.api import (
    create_durable_purge_executor,
    create_repository_catalog,
    create_streaming_blob_store,
)
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_DIGEST,
    MIGRATION_REGISTRY,
    PLANE_SCHEMA_067_MIGRATION,
    PLANE_SCHEMA_067_REGISTRY_DIGEST,
    PLANE_SCHEMA_074_001_REGISTRY_DIGEST,
    PLANE_SCHEMA_074_002_MIGRATION,
    PLANE_SCHEMA_074_003_MIGRATION,
    PLANE_SCHEMA_074_004_MIGRATION,
    PLANE_SCHEMA_074_MIGRATION,
    Migration,
    MigrationRegistry,
    MigrationRunner,
)
from astralplane.database.pool import ConnectionPool
from astralplane.database.revision import DataPlaneRevision
from astralplane.database.transaction import PlaneDatabase
from astralplane.errors import SchemaRevisionError
from tests.fixtures.pre_split.loader import (
    TEST_DATABASE_ENV,
    FixtureLoadError,
    FixtureLoadReport,
    connect_fixture_database,
    drop_postgres_fixture,
    load_fixture,
    verify_blob_fixture,
)


class _NonClosingDriverPool:
    """Adapt one dedicated integration connection to the production database facade."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.borrowed = False

    def getconn(self) -> Any:
        if self.borrowed:
            raise RuntimeError("integration connection is already borrowed")
        self.borrowed = True
        return self.connection

    def putconn(self, connection: Any, *, close: bool = False) -> None:
        if connection is not self.connection or not self.borrowed or close:
            raise RuntimeError("integration connection was returned in an invalid state")
        self.borrowed = False

    def closeall(self) -> None:
        return None


@dataclass(slots=True)
class _LoadedFixture:
    connection: Any
    schema: str
    blob_root: Path
    report: FixtureLoadReport
    database: PlaneDatabase


def _quoted_schema(schema: str) -> str:
    assert schema.startswith("astralplane_fixture_")
    assert schema.removeprefix("astralplane_fixture_").isalnum()
    return f'"{schema}"'


def _configure_search_path(connection: Any, schema: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(f"SET search_path TO {_quoted_schema(schema)}, pg_catalog")
        connection.commit()
    finally:
        cursor.close()
        connection.rollback()


def _query_all(
    connection: Any,
    statement: str,
    parameters: tuple[object, ...] = (),
) -> tuple[tuple[object, ...], ...]:
    cursor = connection.cursor()
    try:
        cursor.execute(statement, parameters)
        return tuple(tuple(row) for row in cursor.fetchall())
    finally:
        cursor.close()
        connection.rollback()


def _query_one(
    connection: Any,
    statement: str,
    parameters: tuple[object, ...] = (),
) -> tuple[object, ...] | None:
    rows = _query_all(connection, statement, parameters)
    assert len(rows) <= 1
    return None if not rows else rows[0]


def _schema_exists(connection: Any, schema: str) -> bool:
    row = _query_one(connection, "SELECT to_regnamespace(%s)", (schema,))
    return row is not None and row[0] is not None


def _representative_snapshot(connection: Any) -> dict[str, tuple[tuple[object, ...], ...]]:
    return {
        "audit": _query_all(
            connection,
            """
            SELECT event_id::text, actor_user_id, encode(prev_hash, 'hex'),
                   encode(entry_hash, 'hex'), schema_version
            FROM audit_events
            ORDER BY actor_user_id, recorded_at, event_id
            """,
        ),
        "artifacts": _query_all(
            connection,
            """
            SELECT attachment_id, user_id, storage_path, size_bytes, sha256
            FROM user_attachments ORDER BY attachment_id
            """,
        ),
        "history": _query_all(
            connection,
            """
            SELECT chat.id, chat.user_id, message.role, message.content
            FROM chats AS chat
            JOIN messages AS message ON message.chat_id = chat.id
            ORDER BY chat.id, message.id
            """,
        ),
        "preferences": _query_all(
            connection,
            "SELECT user_id, preferences FROM user_preferences ORDER BY user_id",
        ),
        "quality_audit": _query_all(
            connection,
            """
            SELECT 'run', id, status FROM test_runs
            UNION ALL
            SELECT 'case', id, verification_status FROM test_case_results
            UNION ALL
            SELECT 'evidence', id, evidence_type FROM test_evidence
            UNION ALL
            SELECT 'audit', id, action FROM audit_entries
            UNION ALL
            SELECT 'artifact', id, filename FROM latex_artifacts
            ORDER BY 1, 2
            """,
        ),
        "remote": _query_all(
            connection,
            "SELECT machine_id, owner_user_id, address FROM remote_machine ORDER BY machine_id",
        ),
        "scheduler": _query_all(
            connection,
            "SELECT id::text, user_id, status FROM scheduled_job ORDER BY id",
        ),
        "voice": _query_all(
            connection,
            """
            SELECT session.session_id::text, turn.turn_id::text, session.user_id, turn.state
            FROM voice_session AS session
            JOIN voice_turn AS turn ON turn.session_id = session.session_id
            ORDER BY session.session_id, turn.turn_id
            """,
        ),
        "workspaces": _query_all(
            connection,
            """
            SELECT chat_id, user_id, layout_key, position, layout
            FROM workspace_layout ORDER BY chat_id, layout_key
            """,
        ),
    }


def _metadata(connection: Any) -> dict[str, str]:
    return {
        str(key): str(value)
        for key, value in _query_all(
            connection,
            """
            SELECT key, value FROM schema_meta
            WHERE key IN ('revision', 'astralplane_migration_digest')
            ORDER BY key
            """,
        )
    }


def _current_runner(fixture: _LoadedFixture) -> MigrationRunner:
    return MigrationRunner(
        fixture.database,
        revision=CURRENT_DATA_PLANE_REVISION,
        registry=MIGRATION_REGISTRY,
    )


@pytest.fixture
def pre_split_postgres(tmp_path: Path) -> Iterator[_LoadedFixture]:
    database_url = os.environ.get(TEST_DATABASE_ENV)
    if database_url is None:
        pytest.skip(f"{TEST_DATABASE_ENV} is required for PostgreSQL integration tests")
    try:
        connection = connect_fixture_database(database_url)
    except FixtureLoadError as exc:
        pytest.fail(str(exc))
    schema = f"astralplane_fixture_{uuid.uuid4().hex}"
    blob_root = (tmp_path / "pre-split-blobs").resolve()
    loaded = False
    try:
        report = load_fixture(
            connection,
            schema=schema,
            blob_root=blob_root,
        )
        loaded = True
        _configure_search_path(connection, schema)
        database = PlaneDatabase(ConnectionPool(_NonClosingDriverPool(connection)))
        yield _LoadedFixture(
            connection=connection,
            schema=schema,
            blob_root=blob_root,
            report=report,
            database=database,
        )
    finally:
        if loaded or _schema_exists(connection, schema):
            drop_postgres_fixture(connection, schema=schema)
        connection.close()


def test_pre_split_upgrade_preserves_representative_database_and_blobs(
    pre_split_postgres: _LoadedFixture,
) -> None:
    fixture = pre_split_postgres
    expected = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "pre_split" / "expected.json").read_text(
            encoding="utf-8"
        )
    )
    assert fixture.report.pre_migration_catalog.to_dict() == expected[
        "preMigrationCatalog"
    ]
    assert {
        "builderSha256": fixture.report.legacy_baseline_builder_sha256,
        "loaderSha256": fixture.report.loader_sha256,
        "sourceBlob": fixture.report.legacy_baseline_source_blob,
    } == expected["legacyBaseline"]
    before = _representative_snapshot(fixture.connection)
    blob_evidence = verify_blob_fixture(fixture.blob_root)

    report = _current_runner(fixture).run(expected_revision="075.001")

    assert report.source_revision == "066.001"
    assert report.target_revision == "075.001"
    assert report.applied_steps == (
        "astralplane-067-transactional-recovery",
        "astralplane-074-lets-authority",
        "astralplane-074-quality-audit-ownership",
        "astralplane-074-current-runtime-contract",
        "astralplane-074-pending-attachment-materialization",
        "astralplane-075-client-local-speech",
    )
    assert not report.already_current
    assert _metadata(fixture.connection) == {
        "astralplane_migration_digest": MIGRATION_DIGEST,
        "revision": "075.001",
    }
    assert _representative_snapshot(fixture.connection) == before
    assert _query_all(
        fixture.connection,
        "SELECT speech_backend, transport FROM voice_session ORDER BY session_id",
    ) == (("llm_factory", "livekit"),)
    assert verify_blob_fixture(fixture.blob_root) == blob_evidence
    assert _query_all(
        fixture.connection,
        """
        SELECT attachment_id, materialization_state, materialization_lease_id
        FROM user_attachments ORDER BY attachment_id
        """,
    ) == (
        ("artifact-1", "ready", None),
        ("artifact-2", "ready", None),
    )

    blobs = create_streaming_blob_store(root=fixture.blob_root, create_root=False)
    expected_blobs = (
        (
            "fixture-owner-a",
            "artifact-1/artifact.txt",
            49,
            "b70ef33cdab65179930b27076c17c861f3f6b00b7025833378ac39728cec4be4",
        ),
        (
            "fixture-owner-b",
            "artifact-2/summary.json",
            41,
            "bf54f7326432f19faee001e9dd8885c8798b70e8173fa1e4252698b3dc1ced9b",
        ),
    )
    for owner_id, storage_key, size_bytes, digest in expected_blobs:
        with blobs.open_reader(
            owner_id=owner_id,
            key=storage_key,
            max_bytes=size_bytes,
            expected_size_bytes=size_bytes,
            expected_sha256=digest,
        ) as reader:
            assert hashlib.sha256(reader.read()).hexdigest() == digest

    catalog = create_repository_catalog()
    deleted_tombstone = _query_one(
        fixture.connection,
        """
        SELECT tombstone_id, status
        FROM astralplane_purge_tombstone
        WHERE owner_id = 'fixture-owner-b'
          AND object_id = 'artifact-2'
          AND target_scope = 'attachment_prefix'
        """,
    )
    assert deleted_tombstone is not None and deleted_tombstone[1] == "pending"
    executor = create_durable_purge_executor(
        database=fixture.database,
        purge_store=catalog.purge,
        blobs=blobs,
    )
    result = executor.execute(
        owner_id="fixture-owner-b",
        tombstone_id=str(deleted_tombstone[0]),
        now=datetime(2026, 8, 14, tzinfo=UTC),
        retry_at=datetime(2026, 8, 14, tzinfo=UTC) + timedelta(minutes=1),
    )
    assert result.state.value == "purged"
    assert blobs.is_prefix_absent(owner_id="fixture-owner-b", prefix="artifact-2")
    assert not blobs.is_prefix_absent(owner_id="fixture-owner-a", prefix="artifact-1")
    assert executor.has_incomplete_for_administration() is False
    blobs.close()
    assert _query_all(
        fixture.connection,
        """
        SELECT actor_user_id, chain_sequence
        FROM audit_events ORDER BY actor_user_id, chain_sequence
        """,
    ) == (
        ("fixture-owner-a", 1),
        ("fixture-owner-a", 2),
        ("fixture-owner-b", 1),
    )
    for table_name in (
        "astralplane_outbox",
        "audit_retention_anchor",
        "astralplane_purge_tombstone",
        "astralplane_authority_binding",
        "astralplane_authority_lifecycle_operation",
        "astralplane_protected_effect_operation",
        "astralplane_receipt_sequence_watermark",
        "astralplane_receipt_claim",
    ):
        row = _query_one(fixture.connection, "SELECT to_regclass(%s)::text", (table_name,))
        assert row == (table_name,)
    assert _query_all(
        fixture.connection,
        """
        SELECT owner_id FROM test_runs
        UNION ALL SELECT owner_id FROM test_case_results
        UNION ALL SELECT owner_id FROM test_evidence
        UNION ALL SELECT owner_id FROM audit_entries
        UNION ALL SELECT owner_id FROM latex_artifacts
        """,
    ) == (("system:quality-audit",),) * 5


def test_repeat_upgrade_is_a_noop_with_identical_evidence(
    pre_split_postgres: _LoadedFixture,
) -> None:
    fixture = pre_split_postgres
    runner = _current_runner(fixture)
    first = runner.run(expected_revision="075.001")
    after_first = _representative_snapshot(fixture.connection)
    metadata_after_first = _metadata(fixture.connection)

    second = runner.run(expected_revision="075.001")

    assert first.applied_steps
    assert second.already_current
    assert second.applied_steps == ()
    assert second.migration_digest == first.migration_digest == MIGRATION_DIGEST
    assert _representative_snapshot(fixture.connection) == after_first
    assert _metadata(fixture.connection) == metadata_after_first


def test_post_load_predecessor_damage_is_rejected_before_any_migration_repair(
    pre_split_postgres: _LoadedFixture,
) -> None:
    fixture = pre_split_postgres
    history_before = _query_all(
        fixture.connection,
        "SELECT id, user_id, title FROM chats ORDER BY id",
    )
    cursor = fixture.connection.cursor()
    try:
        cursor.execute("DROP TABLE latex_artifacts")
        fixture.connection.commit()
    finally:
        cursor.close()

    with pytest.raises(
        SchemaRevisionError,
        match="predecessor schema canonical structure",
    ):
        _current_runner(fixture).run(expected_revision="075.001")

    assert _metadata(fixture.connection) == {"revision": "066.001"}
    assert _query_one(fixture.connection, "SELECT to_regclass('latex_artifacts')") == (
        None,
    )
    assert _query_one(
        fixture.connection,
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'audit_events'
          AND column_name = 'chain_sequence'
        """,
    ) == (0,)
    assert _query_all(
        fixture.connection,
        "SELECT id, user_id, title FROM chats ORDER BY id",
    ) == history_before


def test_transactional_failure_rolls_back_both_edges_and_retry_recovers(
    pre_split_postgres: _LoadedFixture,
) -> None:
    fixture = pre_split_postgres
    before = _representative_snapshot(fixture.connection)

    def fail_after_ddl(transaction: Any) -> None:
        transaction.execute("CREATE TABLE fixture_transaction_should_roll_back (id INTEGER)")
        raise RuntimeError("injected migration failure")

    failing_migration = Migration(
        name="astralplane-074-002-injected-failure",
        source_revisions=("074.001",),
        target_revision="074.002",
        checksum=hashlib.sha256(b"astralplane-074-002-injected-failure").hexdigest(),
        operation=fail_after_ddl,
    )
    failing_registry = MigrationRegistry(
        (PLANE_SCHEMA_067_MIGRATION, PLANE_SCHEMA_074_MIGRATION, failing_migration),
        current_schema_verifier=MIGRATION_REGISTRY.current_schema_verifier,
        current_schema_verifier_checksum=(
            MIGRATION_REGISTRY.current_schema_verifier_checksum
        ),
    )
    failing_revision = DataPlaneRevision(
        schema_revision="074.002",
        read_compatible_from=("066.001", "067.001", "074.001"),
        migration_digest=failing_registry.digest,
        accepted_predecessor_digests=(
            ("067.001", PLANE_SCHEMA_067_REGISTRY_DIGEST),
            ("074.001", PLANE_SCHEMA_074_001_REGISTRY_DIGEST),
        ),
    )

    with pytest.raises(RuntimeError, match="injected migration failure"):
        MigrationRunner(
            fixture.database,
            revision=failing_revision,
            registry=failing_registry,
        ).run(expected_revision="074.002")

    assert _metadata(fixture.connection) == {"revision": "066.001"}
    assert _query_one(
        fixture.connection,
        "SELECT to_regclass('fixture_transaction_should_roll_back')",
    ) == (None,)
    assert _query_one(
        fixture.connection,
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'audit_events'
          AND column_name = 'chain_sequence'
        """,
    ) == (0,)
    assert _representative_snapshot(fixture.connection) == before

    recovered = _current_runner(fixture).run(expected_revision="075.001")

    assert recovered.applied_steps == (
        "astralplane-067-transactional-recovery",
        "astralplane-074-lets-authority",
        "astralplane-074-quality-audit-ownership",
        "astralplane-074-current-runtime-contract",
        "astralplane-074-pending-attachment-materialization",
        "astralplane-075-client-local-speech",
    )
    assert _metadata(fixture.connection)["revision"] == "075.001"


def test_075_direct_apply_rejects_a_wrong_predecessor_without_mutation(
    pre_split_postgres: _LoadedFixture,
) -> None:
    fixture = pre_split_postgres
    before = _representative_snapshot(fixture.connection)

    with (
        pytest.raises(Exception, match=r"clean 074[.]004 predecessor"),
        fixture.database.transaction() as transaction,
    ):
        migrations_module.PLANE_SCHEMA_075_MIGRATION.apply(transaction)

    assert _metadata(fixture.connection) == {"revision": "066.001"}
    assert _representative_snapshot(fixture.connection) == before
    assert _query_one(
        fixture.connection,
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'voice_session' "
        "AND column_name = 'speech_backend'",
    ) == (0,)


def test_075_failure_rolls_back_backend_column_and_forward_retry_recovers(
    pre_split_postgres: _LoadedFixture,
) -> None:
    fixture = pre_split_postgres
    with fixture.database.transaction() as transaction:
        for migration in (
            PLANE_SCHEMA_067_MIGRATION,
            PLANE_SCHEMA_074_MIGRATION,
            PLANE_SCHEMA_074_002_MIGRATION,
            PLANE_SCHEMA_074_003_MIGRATION,
            PLANE_SCHEMA_074_004_MIGRATION,
        ):
            migration.apply(transaction)
        transaction.execute(
            "INSERT INTO schema_meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            ("revision", "074.004"),
        )
        transaction.execute(
            "INSERT INTO schema_meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (
                "astralplane_migration_digest",
                migrations_module.PLANE_SCHEMA_074_004_REGISTRY_DIGEST,
            ),
        )

    def fail_after_backend_column(transaction: Any) -> None:
        transaction.execute("ALTER TABLE voice_session ADD COLUMN speech_backend TEXT")
        raise RuntimeError("injected 075 migration failure")

    failing = Migration(
        name="astralplane-075-injected-failure",
        source_revisions=("074.004",),
        target_revision="075.001",
        checksum=hashlib.sha256(b"astralplane-075-injected-failure").hexdigest(),
        operation=fail_after_backend_column,
    )
    failing_registry = MigrationRegistry(
        (failing,),
        current_schema_verifier=MIGRATION_REGISTRY.current_schema_verifier,
        current_schema_verifier_checksum=MIGRATION_REGISTRY.current_schema_verifier_checksum,
    )
    failing_revision = DataPlaneRevision(
        schema_revision="075.001",
        read_compatible_from=("074.004",),
        migration_digest=failing_registry.digest,
        accepted_predecessor_digests=(
            ("074.004", migrations_module.PLANE_SCHEMA_074_004_REGISTRY_DIGEST),
        ),
    )

    with pytest.raises(RuntimeError, match="injected 075 migration failure"):
        MigrationRunner(
            fixture.database,
            revision=failing_revision,
            registry=failing_registry,
        ).run(expected_revision="075.001")

    assert _metadata(fixture.connection) == {
        "revision": "074.004",
        "astralplane_migration_digest": (
            migrations_module.PLANE_SCHEMA_074_004_REGISTRY_DIGEST
        ),
    }
    assert _query_one(
        fixture.connection,
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'voice_session' "
        "AND column_name = 'speech_backend'",
    ) == (0,)

    recovered = _current_runner(fixture).run(expected_revision="075.001")
    assert recovered.source_revision == "074.004"
    assert recovered.applied_steps == ("astralplane-075-client-local-speech",)
    assert _query_one(
        fixture.connection,
        "SELECT speech_backend, transport FROM voice_session",
    ) == ("llm_factory", "livekit")


def test_blob_stage_failure_creates_neither_schema_nor_published_root(
    pre_split_postgres: _LoadedFixture,
    tmp_path: Path,
) -> None:
    fixture = pre_split_postgres
    schema = f"astralplane_fixture_{uuid.uuid4().hex}"
    blob_root = (tmp_path / "failed-blob-root").resolve()

    with pytest.raises(FixtureLoadError, match="injected synthetic"):
        load_fixture(
            fixture.connection,
            schema=schema,
            blob_root=blob_root,
            fail_after_blob_files=1,
        )

    assert not blob_root.exists()
    assert not _schema_exists(fixture.connection, schema)
    assert not tuple(tmp_path.glob(".astralplane-fixture-stage-*"))
    _configure_search_path(fixture.connection, fixture.schema)


def test_documented_joint_restore_returns_to_066_then_reapplies_upgrade(
    pre_split_postgres: _LoadedFixture,
    tmp_path: Path,
) -> None:
    fixture = pre_split_postgres
    baseline_snapshot = _representative_snapshot(fixture.connection)
    baseline_digest = fixture.report.fixture_digest
    original_blob_evidence = verify_blob_fixture(fixture.blob_root)
    _current_runner(fixture).run(expected_revision="075.001")

    drop_postgres_fixture(fixture.connection, schema=fixture.schema)
    restored_blob_root = (tmp_path / "restored-pre-split-blobs").resolve()
    restored_report = load_fixture(
        fixture.connection,
        schema=fixture.schema,
        blob_root=restored_blob_root,
    )
    _configure_search_path(fixture.connection, fixture.schema)

    assert restored_report.fixture_digest == baseline_digest
    assert restored_report.schema_revision == "066.001"
    assert _metadata(fixture.connection) == {"revision": "066.001"}
    assert _representative_snapshot(fixture.connection) == baseline_snapshot
    assert verify_blob_fixture(restored_blob_root) == original_blob_evidence
    assert verify_blob_fixture(fixture.blob_root) == original_blob_evidence

    recovered = _current_runner(fixture).run(expected_revision="075.001")

    assert recovered.source_revision == "066.001"
    assert recovered.target_revision == "075.001"
    assert _metadata(fixture.connection)["astralplane_migration_digest"] == MIGRATION_DIGEST
