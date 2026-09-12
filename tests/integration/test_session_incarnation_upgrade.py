"""088.001 populated upgrade issues identities once and guards the complete catalog."""

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from astralplane.api import create_repository_catalog
from astralplane.database import migrations as m
from astralplane.database.baseline import BaselineMigrationRunner
from astralplane.errors import SchemaRevisionError
from tests.integration.test_empty_database_startup import (
    empty_postgres_schema as empty_postgres_schema,
)


def old_runner(database):
    registry = m.MigrationRegistry(
        tuple(
            edge for edge in m.MIGRATION_REGISTRY.migrations if edge.target_revision <= "088.001"
        ),
        current_schema_verifier=lambda tx: m._verify_predecessor_plane_schema(tx, "088.001"),
        current_schema_verifier_checksum="35bd630d2be86b48988d2fdbe16da54faea293aca68e80d6363db8fb41ded1de",
        predecessor_schema_verifier=m._verify_predecessor_plane_schema,
        predecessor_schema_verifier_checksum="f87fa1fba779b03a64feec13821d5426c28159117e6c0aafb47756847706f885",
    )
    assert registry.digest == m.PLANE_SCHEMA_088_REGISTRY_DIGEST
    revision = replace(
        m.CURRENT_DATA_PLANE_REVISION,
        schema_revision="088.001",
        migration_digest=registry.digest,
        read_compatible_from=tuple(
            r for r in m.CURRENT_DATA_PLANE_REVISION.read_compatible_from if r < "088.001"
        ),
        accepted_predecessor_digests=tuple(
            pair
            for pair in m.CURRENT_DATA_PLANE_REVISION.accepted_predecessor_digests
            if pair[0] < "088.001"
        ),
    )
    return m.MigrationRunner(database, revision=revision, registry=registry)


def current_runner(database):
    return m.MigrationRunner(
        database, revision=m.CURRENT_DATA_PLANE_REVISION, registry=m.MIGRATION_REGISTRY
    )


def test_populated_upgrade_preserves_all_prior_session_fields_and_repeats(empty_postgres_schema):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, old_runner(db)).run(expected_revision="088.001")
    with db.transaction() as tx:
        for index in range(4):
            tx.execute(
                "INSERT INTO web_session(sid,user_id,access_token_enc,refresh_token_enc,"
                "interactive_anchor,hard_expires_at,last_refresh_at,resumed,created_at) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    f"session-{index}",
                    f"owner-{index % 2}",
                    f"opaque-{index}",
                    "opaque-refresh",
                    10,
                    100 if index else 11,
                    20,
                    bool(index % 2),
                    5,
                ),
            )
        before = tuple(dict(row) for row in tx.fetch_all("SELECT * FROM web_session ORDER BY sid"))
        catalog = create_repository_catalog()
        catalog.history.conversations.create(
            tx,
            conversation_id="retained-chat",
            owner_id="owner-0",
            title="Synthetic history",
            agent_id=None,
            created_at=10,
        )
        catalog.offline_grants.create_grant(
            tx,
            grant_id=str(uuid4()),
            owner_id="owner-0",
            agent_id=None,
            encrypted_refresh_token=b"legacy-opaque-reference",
            issued_at=10,
            expires_at=100,
        )
        tables = ("persistent_assignment", "user_offline_grant", "chats")
        unchanged = {
            table: tuple(dict(row) for row in tx.fetch_all("SELECT * FROM " + table))
            for table in tables
        }
    runner = current_runner(db)
    assert runner.run(expected_revision="088.003").applied_steps == (
        "astralplane-088-session-incarnation",
        "astralplane-088-session-issuer",
    )
    with db.transaction() as tx:
        after = tuple(dict(row) for row in tx.fetch_all("SELECT * FROM web_session ORDER BY sid"))
        assert before == tuple(
            {
                key: value
                for key, value in row.items()
                if key not in {"incarnation_id", "issuing_issuer", "issuing_client_id"}
            }
            for row in after
        )
        identities = {str(row["incarnation_id"]) for row in after}
        assert len(identities) == 4
        assert all(UUID(value).version == 4 and str(UUID(value)) == value for value in identities)
        assert unchanged == {
            table: tuple(dict(row) for row in tx.fetch_all("SELECT * FROM " + table))
            for table in tables
        }
    assert BaselineMigrationRunner(db, runner).run(expected_revision="088.003").already_current
    with db.transaction() as tx:
        assert after == tuple(
            dict(row) for row in tx.fetch_all("SELECT * FROM web_session ORDER BY sid")
        )


@pytest.mark.parametrize(
    "corruption",
    [
        "ALTER TABLE web_session ALTER COLUMN incarnation_id DROP DEFAULT",
        "ALTER TABLE web_session ALTER COLUMN incarnation_id DROP NOT NULL",
        "ALTER TABLE web_session DROP CONSTRAINT web_session_incarnation_unique",
        "ALTER TABLE web_session DROP CONSTRAINT web_session_incarnation_uuid4",
    ],
)
def test_current_verifier_refuses_weakened_identity_catalog(empty_postgres_schema, corruption):
    db = empty_postgres_schema.database
    runner = current_runner(db)
    BaselineMigrationRunner(db, runner).run(expected_revision="088.003")
    with db.transaction() as tx:
        tx.execute(corruption)
    with pytest.raises(SchemaRevisionError):
        runner.run(expected_revision="088.003")


def test_predecessor_extra_identity_column_is_refused_without_adoption(empty_postgres_schema):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, old_runner(db)).run(expected_revision="088.001")
    with db.transaction() as tx:
        tx.execute("ALTER TABLE web_session ADD COLUMN incarnation_id UUID")
    with pytest.raises(SchemaRevisionError):
        current_runner(db).run(expected_revision="088.003")
    with db.transaction() as tx:
        assert tx.fetch_one("SELECT count(*) AS n FROM web_session")["n"] == 0


def test_failed_identity_edge_rolls_back_issuance_and_can_retry(empty_postgres_schema):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, old_runner(db)).run(expected_revision="088.001")
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO web_session(sid,user_id,access_token_enc,refresh_token_enc,"
            "interactive_anchor,hard_expires_at,last_refresh_at,resumed,created_at) "
            "VALUES('rollback','owner','opaque','opaque',1,100,1,false,1)"
        )

    def fail(tx):
        tx.execute(m.PLANE_SCHEMA_088_002_STATEMENTS[0])
        raise RuntimeError("injected identity edge failure")

    registry = m.MigrationRegistry(
        tuple(
            replace(edge, operation=fail) if edge.target_revision == "088.002" else edge
            for edge in m.MIGRATION_REGISTRY.migrations
        ),
        current_schema_verifier=m._verify_current_plane_schema,
        current_schema_verifier_checksum=m.MIGRATION_REGISTRY.current_schema_verifier_checksum,
        predecessor_schema_verifier=m._verify_predecessor_plane_schema,
        predecessor_schema_verifier_checksum=m.MIGRATION_REGISTRY.predecessor_schema_verifier_checksum,
    )
    assert registry.digest == m.MIGRATION_REGISTRY.digest
    runner = m.MigrationRunner(db, revision=m.CURRENT_DATA_PLANE_REVISION, registry=registry)
    with pytest.raises(Exception, match=r"injected|migration"):
        runner.run(expected_revision="088.003")
    with db.transaction() as tx:
        m._verify_predecessor_plane_schema(tx, "088.001")
        assert tx.fetch_one("SELECT count(*) AS n FROM web_session")["n"] == 1
    assert current_runner(db).run(expected_revision="088.003").applied_steps == (
        "astralplane-088-session-incarnation",
        "astralplane-088-session-issuer",
    )


def test_populated_088001_issued_and_uncertain_liabilities_survive_upgrade(empty_postgres_schema):
    """The fixture was issued by actual immutable 718 APIs, not reconstructed model state."""
    import json
    from collections.abc import Mapping
    from pathlib import Path

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

    def rows(tx, table):
        values = [normalize(dict(row)) for row in tx.fetch_all("SELECT * FROM " + table)]
        if table == "web_session":
            for row in values:
                row.pop("incarnation_id", None)
                row.pop("issuing_issuer", None)
                row.pop("issuing_client_id", None)
        return sorted(values, key=lambda row: json.dumps(row, sort_keys=True))

    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, old_runner(db)).run(expected_revision="088.001")
    with db.transaction() as tx:
        for table in tables:
            for row in fixture["tables"][table]:
                columns = list(row)
                assert all(column.replace("_", "").isalnum() for column in columns)
                values = []
                placeholders = []
                for column in columns:
                    value = row[column]
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
        before = {table: rows(tx, table) for table in tables}
        assert {row["state"] for row in before["persistent_assignment_action"]} == {
            "started",
            "uncertain",
        }
        assert all(
            row["data"]["operation"]["version"] == 1 for row in before["persistent_assignment"]
        )
        assert all(
            row["data"]["usage"]["outstanding"]["tool_calls"] == 1
            for row in before["persistent_assignment"]
        )
        assert len(before["assignment_operation_receipt"]) == 2
        assert any(
            row["data"]["operation"]["authority"]["reference_kind"] == "offline_grant"
            for row in before["persistent_assignment"]
        )
        for permit in fixture["permits"]:
            row = next(
                row
                for row in before["persistent_assignment_action"]
                if row["id"] == permit["action_id"]
            )
            attempt = next(
                attempt
                for attempt in row["data"]["attempts"]
                if attempt["attempt_id"] == permit["attempt_id"]
            )
            assert attempt["dispatch_token"] == permit["dispatch_token"]
            assert row["data"]["intent"]["request_digest"] == permit["request_digest"]
    runner = current_runner(db)
    assert runner.run(expected_revision="088.003").applied_steps == (
        "astralplane-088-session-incarnation",
        "astralplane-088-session-issuer",
    )
    for _ in range(2):
        with db.transaction() as tx:
            assert {table: rows(tx, table) for table in tables} == before
            identities = [
                str(row["incarnation_id"])
                for row in tx.fetch_all("SELECT incarnation_id FROM web_session")
            ]
            assert all(UUID(value).version == 4 for value in identities)
        assert runner.run(expected_revision="088.003").already_current
