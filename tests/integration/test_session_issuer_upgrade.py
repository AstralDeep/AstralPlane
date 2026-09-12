"""An exact 088.002 upgrade adds unknown issuing metadata without adopting identity."""

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import pytest

from astralplane.database import migrations as m
from astralplane.database.baseline import BaselineMigrationRunner
from astralplane.errors import SchemaRevisionError
from tests.integration.test_empty_database_startup import (
    empty_postgres_schema as empty_postgres_schema,
)


def prior_runner(database):
    registry = m.MigrationRegistry(
        tuple(
            edge for edge in m.MIGRATION_REGISTRY.migrations if edge.target_revision <= "088.002"
        ),
        current_schema_verifier=lambda tx: m._verify_predecessor_plane_schema(tx, "088.002"),
        current_schema_verifier_checksum="c9321dc01e0196b626c68c7be653f7c89329cd779f90cbd86776c9843730aae0",
        predecessor_schema_verifier=m._verify_predecessor_plane_schema,
        predecessor_schema_verifier_checksum="ff709a4f975d23dcf10cf27ebd4d7e66d97f213b9f028409cb54cefa0d5fc291",
    )
    assert registry.digest == m.PLANE_SCHEMA_088_002_REGISTRY_DIGEST
    revision = replace(
        m.CURRENT_DATA_PLANE_REVISION,
        schema_revision="088.002",
        migration_digest=registry.digest,
        read_compatible_from=tuple(
            r for r in m.CURRENT_DATA_PLANE_REVISION.read_compatible_from if r < "088.002"
        ),
        accepted_predecessor_digests=tuple(
            pair
            for pair in m.CURRENT_DATA_PLANE_REVISION.accepted_predecessor_digests
            if pair[0] < "088.002"
        ),
    )
    return m.MigrationRunner(database, revision=revision, registry=registry)


def current_runner(database):
    return m.MigrationRunner(
        database, revision=m.CURRENT_DATA_PLANE_REVISION, registry=m.MIGRATION_REGISTRY
    )


def normalize(value):
    if isinstance(value, (bytes, memoryview)):
        return {"$bytea": bytes(value).hex()}
    if isinstance(value, Mapping):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def retained_rows(tx, tables):
    return {
        table: sorted(
            [normalize(dict(row)) for row in tx.fetch_all("SELECT * FROM " + table)],
            key=lambda row: json.dumps(row, sort_keys=True),
        )
        for table in tables
    }


def load_liabilities(tx):
    fixture = json.loads(
        Path(__file__).parents[1].joinpath("fixtures/session_incarnation_088001.json").read_text()
    )
    assert fixture["source_commit"] == "718021ba019abcd3a04ffbd6b80b88e51395bda6"
    assert fixture["synthetic"] is True
    tables = (
        "web_session",
        "user_offline_grant",
        "persistent_assignment",
        "assignment_operation_receipt",
        "persistent_assignment_action",
        "persistent_assignment_activity",
    )
    assert set(fixture["tables"]) == set(tables)
    for table in tables:
        for row in fixture["tables"][table]:
            columns = list(row)
            assert all(column.replace("_", "").isalnum() for column in columns)
            values, placeholders = [], []
            for value in row.values():
                if isinstance(value, dict) and set(value) == {"$bytea"}:
                    value = bytes.fromhex(value["$bytea"])
                    placeholders.append("%s")
                elif isinstance(value, (dict, list)):
                    value = json.dumps(value)
                    placeholders.append("%s::jsonb")
                else:
                    placeholders.append("%s")
                values.append(value)
            tx.execute(
                "INSERT INTO "
                + table
                + " ("
                + ",".join(columns)
                + ") VALUES ("
                + ",".join(placeholders)
                + ")",
                tuple(values),
            )
    for client in (None, "legacy-web-client"):
        tx.execute(
            "INSERT INTO auth_revocation_queue "
            "(user_id,refresh_token_enc,enqueued_at,attempts,client_id) "
            "VALUES (%s,%s,%s,%s,%s)",
            ("synthetic-queue-owner", "opaque-ciphertext", 100, 3, client),
        )
    return (*tables, "auth_revocation_queue")


def test_populated_upgrade_preserves_exact_incarnations_and_issued_liabilities(
    empty_postgres_schema,
):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, prior_runner(db)).run(expected_revision="088.002")
    with db.transaction() as tx:
        tables = load_liabilities(tx)
        before = retained_rows(tx, tables)
        assert {row["state"] for row in before["persistent_assignment_action"]} == {
            "started",
            "uncertain",
        }
        assert len(before["assignment_operation_receipt"]) == 2
        assert before["user_offline_grant"]
    runner = current_runner(db)
    assert runner.run(expected_revision="088.003").applied_steps == (
        "astralplane-088-session-issuer",
    )
    with db.transaction() as tx:
        after = retained_rows(tx, tables)
        sessions = after["web_session"]
        assert all(
            row["issuing_issuer"] is None and row["issuing_client_id"] is None for row in sessions
        )
        for row in sessions:
            del row["issuing_issuer"], row["issuing_client_id"]
        for row in after["auth_revocation_queue"]:
            assert row.pop("issuing_issuer") is None
        assert after == before
    assert runner.run(expected_revision="088.003").already_current
    with db.transaction() as tx:
        assert all(
            row["issuing_issuer"] is None and row["issuing_client_id"] is None
            for row in tx.fetch_all("SELECT * FROM web_session")
        )


@pytest.mark.parametrize(
    "corruption",
    [
        "ALTER TABLE web_session ADD COLUMN issuing_issuer TEXT",
        "ALTER TABLE web_session ADD COLUMN issuing_client_id TEXT",
        "ALTER TABLE auth_revocation_queue ADD COLUMN issuing_issuer TEXT",
        "ALTER TABLE web_session DROP CONSTRAINT web_session_incarnation_uuid4",
    ],
)
def test_wrong_predecessor_catalog_is_refused_before_any_new_edge(
    empty_postgres_schema, corruption
):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, prior_runner(db)).run(expected_revision="088.002")
    with db.transaction() as tx:
        tx.execute(corruption)
    with pytest.raises(SchemaRevisionError):
        current_runner(db).run(expected_revision="088.003")
    with db.transaction() as tx:
        assert (
            tx.fetch_one("SELECT value FROM schema_meta WHERE key='revision'")["value"] == "088.002"
        )


@pytest.mark.parametrize(
    "corruption",
    [
        "ALTER TABLE web_session DROP CONSTRAINT web_session_issuing_metadata",
        "ALTER TABLE web_session ALTER COLUMN issuing_client_id SET DEFAULT 'guessed-client'",
        "ALTER TABLE web_session ALTER COLUMN issuing_issuer TYPE VARCHAR(2048)",
        "ALTER TABLE auth_revocation_queue DROP CONSTRAINT auth_revocation_queue_issuing_metadata",
        "ALTER TABLE auth_revocation_queue ALTER COLUMN issuing_issuer "
        "SET DEFAULT 'guessed-issuer'",
    ],
)
def test_current_issuing_catalog_drift_is_refused(empty_postgres_schema, corruption):
    db = empty_postgres_schema.database
    runner = current_runner(db)
    BaselineMigrationRunner(db, runner).run(expected_revision="088.003")
    with db.transaction() as tx:
        tx.execute(corruption)
    with pytest.raises(SchemaRevisionError):
        runner.run(expected_revision="088.003")


def test_interrupted_upgrade_rolls_back_both_metadata_columns_and_retries(empty_postgres_schema):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, prior_runner(db)).run(expected_revision="088.002")
    with db.transaction() as tx:
        tables = load_liabilities(tx)
        before = retained_rows(tx, tables)

    def interrupt(tx):
        tx.execute(m.PLANE_SCHEMA_088_003_STATEMENTS[0])
        raise RuntimeError("controlled interrupted issuing edge")

    registry = m.MigrationRegistry(
        tuple(
            replace(edge, operation=interrupt) if edge.target_revision == "088.003" else edge
            for edge in m.MIGRATION_REGISTRY.migrations
        ),
        current_schema_verifier=m._verify_current_plane_schema,
        current_schema_verifier_checksum=m.CURRENT_SCHEMA_VERIFIER_CHECKSUM,
        predecessor_schema_verifier=m._verify_predecessor_plane_schema,
        predecessor_schema_verifier_checksum=m.PREDECESSOR_SCHEMA_VERIFIER_CHECKSUM,
    )
    assert registry.digest == m.MIGRATION_DIGEST
    with pytest.raises(Exception, match=r"controlled|migration"):
        m.MigrationRunner(db, revision=m.CURRENT_DATA_PLANE_REVISION, registry=registry).run(
            expected_revision="088.003"
        )
    with db.transaction() as tx:
        m._verify_predecessor_plane_schema(tx, "088.002")
        assert retained_rows(tx, tables) == before
    assert current_runner(db).run(expected_revision="088.003").applied_steps == (
        "astralplane-088-session-issuer",
    )
