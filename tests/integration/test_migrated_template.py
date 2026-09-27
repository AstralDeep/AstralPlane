"""Tests for tests/fixtures/migrated_template.py: a clone of the session-migrated template is a
current catalog that the real migration verifier accepts, clones never see each other's writes,
and a failed template build leaves no database behind.
"""

from __future__ import annotations

import os

import psycopg2
import pytest
from psycopg2.extensions import make_dsn

from astralplane.database.baseline import (
    BaselineCompatibilityState,
    inspect_baseline_compatibility,
)
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_REGISTRY,
    MigrationRunner,
)
from astralplane.database.transaction import PlaneDatabase
from tests.fixtures.migrated_template import (
    TEMPLATE_SCHEMA,
    DatabaseTemplate,
    MigratedDatabase,
    bound_clone,
)
from tests.fixtures.pre_split.loader import TEST_DATABASE_ENV

_INSERT_CHAT = (
    "INSERT INTO chats (id, user_id, title, created_at, updated_at) VALUES (%s, %s, 't', 1, 1)"
)


def _database_names(dsn: str) -> set[str]:
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT datname FROM pg_database")
            return {str(row[0]) for row in cursor.fetchall()}
    finally:
        connection.close()


def _chat_ids(database: PlaneDatabase) -> tuple[str, ...]:
    with database.transaction() as transaction:
        rows = transaction.fetch_all("SELECT id FROM chats ORDER BY id")
    return tuple(str(row["id"]) for row in rows)


def test_clone_is_a_current_catalog_that_the_real_verifier_accepts(
    migrated_template: DatabaseTemplate,
    migrated_clone: MigratedDatabase,
) -> None:
    compatibility = inspect_baseline_compatibility(migrated_clone.database)
    report = MigrationRunner(
        migrated_clone.database,
        revision=CURRENT_DATA_PLANE_REVISION,
        registry=MIGRATION_REGISTRY,
    ).run(expected_revision=CURRENT_DATA_PLANE_REVISION.schema_revision)
    with migrated_clone.database.transaction() as transaction:
        location = dict(
            transaction.fetch_one(
                "SELECT current_database() AS database, current_schema() AS schema"
            )
        )

    assert compatibility.state is BaselineCompatibilityState.COMPATIBLE
    assert compatibility.observed_revision == CURRENT_DATA_PLANE_REVISION.schema_revision
    assert not compatibility.missing_required_tables
    assert report.already_current
    assert report.applied_steps == ()
    assert location == {"database": migrated_clone.name, "schema": TEMPLATE_SCHEMA}
    assert migrated_clone.name != migrated_template.name
    assert os.environ[TEST_DATABASE_ENV] == migrated_clone.dsn
    template_dsn = make_dsn(migrated_template.administrator_dsn, dbname=migrated_template.name)
    with pytest.raises(psycopg2.OperationalError, match="not currently accepting connections"):
        psycopg2.connect(template_dsn)


def test_clone_writes_are_invisible_to_every_other_clone(
    migrated_template: DatabaseTemplate,
    migrated_clone: MigratedDatabase,
) -> None:
    with migrated_clone.database.transaction() as transaction:
        transaction.execute(_INSERT_CHAT, ("first-clone-chat", "first-clone-owner"))

    with bound_clone(migrated_template) as second:
        assert os.environ[TEST_DATABASE_ENV] == second.dsn
        assert _chat_ids(second.database) == ()
        with second.database.transaction() as transaction:
            transaction.execute(_INSERT_CHAT, ("second-clone-chat", "second-clone-owner"))
        assert _chat_ids(second.database) == ("second-clone-chat",)
        second_name = second.name

    assert os.environ[TEST_DATABASE_ENV] == migrated_clone.dsn
    assert _chat_ids(migrated_clone.database) == ("first-clone-chat",)
    assert second_name not in _database_names(migrated_clone.dsn)
    with migrated_template.clone() as third:
        assert _chat_ids(third.database) == ()


def test_failed_template_build_leaves_no_database_behind(postgres_administrator_dsn: str) -> None:
    def fail(_database: PlaneDatabase) -> None:
        raise RuntimeError("injected template migration failure")

    before = _database_names(postgres_administrator_dsn)
    with pytest.raises(RuntimeError, match="injected template migration failure"):
        DatabaseTemplate.build(postgres_administrator_dsn, fail)

    assert _database_names(postgres_administrator_dsn) == before
