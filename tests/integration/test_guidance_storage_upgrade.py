"""Representative populated 088.004 upgrade, transactional recovery and repeat."""

from dataclasses import replace
from uuid import uuid4

import pytest

from astralplane.database import migrations as m
from astralplane.database.baseline import BaselineMigrationRunner
from astralplane.errors import SchemaRevisionError
from astralplane.repositories.agents import AgentRepository, DeclarativeAgentCommand
from astralplane.repositories.guidance import SkillsRepository
from astralplane.repositories.guidance_models import SkillCommand, SkillDefinition
from astralplane.repositories.preferences import MemoryRecord, PreferencesRepository
from tests.integration.test_declarative_agents_upgrade import seed_existing_agents
from tests.integration.test_empty_database_startup import (
    empty_postgres_schema as empty_postgres_schema,
)
from tests.integration.test_session_issuer_upgrade import load_liabilities, retained_rows


def prior_runner(database):
    # Exact historical verifier identities at e1dfd714, never a new digest
    # presented as the predecessor. The edge statements remain immutable.
    registry = m.MigrationRegistry(
        tuple(
            edge for edge in m.MIGRATION_REGISTRY.migrations if edge.target_revision <= "088.004"
        ),
        current_schema_verifier=lambda tx: m._verify_predecessor_plane_schema(tx, "088.004"),
        current_schema_verifier_checksum="1169d81190322100a04c41c37dfafd291399fef6687106b057ff52d3443570c4",
        predecessor_schema_verifier=m._verify_predecessor_plane_schema,
        predecessor_schema_verifier_checksum="46b4e2dce4a6b15c38e42292617185ba3a8442aa663ce771eb35d7c37584672d",
    )
    assert registry.digest == m.PLANE_SCHEMA_088_004_REGISTRY_DIGEST
    revision = replace(
        m.CURRENT_DATA_PLANE_REVISION,
        schema_revision="088.004",
        migration_digest=registry.digest,
        read_compatible_from=tuple(
            r for r in m.CURRENT_DATA_PLANE_REVISION.read_compatible_from if r < "088.004"
        ),
        accepted_predecessor_digests=tuple(
            p
            for p in m.CURRENT_DATA_PLANE_REVISION.accepted_predecessor_digests
            if p[0] < "088.004"
        ),
    )
    return m.MigrationRunner(database, revision=revision, registry=registry)


def current_runner(database):
    return m.MigrationRunner(
        database, revision=m.CURRENT_DATA_PLANE_REVISION, registry=m.MIGRATION_REGISTRY
    )


def seed_populated(tx):
    tables = (*seed_existing_agents(tx), *load_liabilities(tx))
    prefs = PreferencesRepository()
    prefs.personalization.create_memory(
        tx,
        MemoryRecord(
            str(uuid4()),
            "legacy-memory-owner",
            "context",
            "Existing plaintext automatic memory remains exact.",
            "promoted",
            0.5,
            100,
            100,
            None,
            None,
            "existing",
            "original-signature",
            90,
            None,
            100,
            2,
            101,
            None,
        ),
    )
    prefs.set_chat_phi_notice_enabled(tx, owner_id="legacy-memory-owner", enabled=False)
    agents = AgentRepository()
    command = DeclarativeAgentCommand(
        owner_id="declaration-owner",
        agent_id="prior-declaration",
        command_id=str(uuid4()),
        command="create",
        revision_id=str(uuid4()),
        display_name="Stored draft",
        definition={"version": 1, "purpose": "Exact prior definition"},
    )
    prepared = agents.prepare_declarative_command(tx, command=command)
    created = agents.apply_declarative_command(tx, preparation=prepared)
    selected = DeclarativeAgentCommand(
        owner_id=command.owner_id,
        agent_id=command.agent_id,
        command_id=str(uuid4()),
        command="activate",
        expected_revision=created.agent.state_revision,
        revision_id=command.revision_id,
    )
    agents.apply_declarative_command(
        tx, preparation=agents.prepare_declarative_command(tx, command=selected)
    )
    return tuple(
        dict.fromkeys((*tables, "memory_item", "user_preferences", "user_agent_command_receipt"))
    )


def test_populated_upgrade_preserves_real_history_runtime_notes_and_issued_liabilities(
    empty_postgres_schema,
):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, prior_runner(db)).run(expected_revision="088.004")
    with db.transaction() as tx:
        tables = seed_populated(tx)
        before = retained_rows(tx, tables)
        assert {r["state"] for r in before["persistent_assignment_action"]} == {
            "started",
            "uncertain",
        }
        assert (
            before["memory_item"]
            and before["user_agent_command_receipt"]
            and before["user_offline_grant"]
        )
        assert any(r["selected_definition_revision_id"] for r in before["user_agent"])
    assert current_runner(db).run(expected_revision="088.005").applied_steps == (
        "astralplane-088-owner-guidance",
    )
    with db.transaction() as tx:
        assert retained_rows(tx, tables) == before
        for table in (
            "owner_skill_head",
            "owner_skill_revision",
            "owner_skill_catalog",
            "explicit_note_current",
            "assignment_guidance_selection",
            "assignment_guidance_reference",
        ):
            assert tx.fetch_all("SELECT * FROM " + table) == ()
        result = SkillsRepository().apply_change(
            tx,
            command=SkillCommand(
                "new-owner",
                str(uuid4()),
                str(uuid4()),
                "create",
                0,
                slug="new",
                definition=SkillDefinition("New skill", "New current skill instructions."),
            ),
        )
        assert result.head.revision == 1
    assert current_runner(db).run(expected_revision="088.005").already_current
    with db.transaction() as tx:
        assert retained_rows(tx, tables) == before
    with pytest.raises(SchemaRevisionError):
        prior_runner(db).run(expected_revision="088.004")


@pytest.mark.parametrize(
    "corruption",
    [
        "CREATE TABLE owner_skill_head (owner_id TEXT)",
        "ALTER TABLE user_agent_revision DROP CONSTRAINT user_agent_revision_artifact_check",
    ],
)
def test_wrong_predecessor_refuses_without_partial_guidance_schema(
    empty_postgres_schema, corruption
):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, prior_runner(db)).run(expected_revision="088.004")
    with db.transaction() as tx:
        tx.execute(corruption)
    with pytest.raises(SchemaRevisionError):
        current_runner(db).run(expected_revision="088.005")
    with db.transaction() as tx:
        assert (
            tx.fetch_one("SELECT value FROM schema_meta WHERE key='revision'")["value"] == "088.004"
        )
        assert tx.fetch_one("SELECT to_regclass('explicit_note_current') AS t")["t"] is None


def test_new_edge_rollback_preserves_populated_predecessor_and_can_repeat(empty_postgres_schema):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, prior_runner(db)).run(expected_revision="088.004")
    with db.transaction() as tx:
        tables = seed_populated(tx)
        before = retained_rows(tx, tables)
    with pytest.raises(RuntimeError, match="injected final failure"), db.transaction() as tx:
        m.PLANE_SCHEMA_088_005_MIGRATION.apply(tx)
        raise RuntimeError("injected final failure")
    with db.transaction() as tx:
        assert retained_rows(tx, tables) == before
        assert tx.fetch_one("SELECT to_regclass('explicit_note_current') AS t")["t"] is None
    assert current_runner(db).run(expected_revision="088.005").applied_steps
    assert current_runner(db).run(expected_revision="088.005").already_current


@pytest.mark.parametrize(
    "corruption",
    [
        "DROP INDEX owner_skill_live_alias",
        "DROP TRIGGER owner_skill_revision_immutable ON owner_skill_revision",
        "ALTER TABLE assignment_guidance_reference DROP CONSTRAINT "
        "assignment_guidance_reference_assignment_id_owner_id_fkey",
        "ALTER TABLE explicit_note_current ADD COLUMN prior_ciphertext BYTEA",
    ],
)
def test_current_schema_refuses_missing_guards_or_extra_note_history(
    empty_postgres_schema, corruption
):
    db = empty_postgres_schema.database
    BaselineMigrationRunner(db, current_runner(db)).run(expected_revision="088.005")
    with db.transaction() as tx:
        tx.execute(corruption)
    with pytest.raises(SchemaRevisionError):
        current_runner(db).run(expected_revision="088.005")
