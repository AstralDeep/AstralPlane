"""Real-PostgreSQL proof for owner-scoped assistant publication rebases."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from astralplane.database.baseline import BaselineMigrationRunner
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_REGISTRY,
    MigrationRunner,
)
from astralplane.database.pool import ConnectionPool
from astralplane.database.transaction import PlaneDatabase
from astralplane.repositories.history import HistoryRepository
from astralplane.repositories.workspaces import (
    CanvasComponentRecord,
    LayoutRecord,
    PublicationRebaseComponent,
    PublicationRebaseLayout,
    WorkspaceRepository,
)
from tests.fixtures.pre_split.loader import (
    TEST_DATABASE_ENV,
    FixtureLoadError,
    connect_fixture_database,
    drop_postgres_fixture,
)

NOW = datetime(2026, 8, 14, 20, 0, tzinfo=UTC)


class _DedicatedDriverPool:
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
class _WorkspaceSchema:
    connection: Any
    schema: str
    database: PlaneDatabase


def _quoted_schema(schema: str) -> str:
    assert schema.startswith("astralplane_fixture_")
    assert schema.removeprefix("astralplane_fixture_").isalnum()
    return f'"{schema}"'


@pytest.fixture
def workspace_postgres_schema() -> Iterator[_WorkspaceSchema]:
    database_url = os.environ.get(TEST_DATABASE_ENV)
    if database_url is None:
        pytest.skip(f"{TEST_DATABASE_ENV} is required for PostgreSQL integration tests")
    try:
        connection = connect_fixture_database(database_url)
    except FixtureLoadError as exc:
        pytest.fail(str(exc))
    schema = f"astralplane_fixture_{uuid.uuid4().hex}"
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA {_quoted_schema(schema)}")
        cursor.execute(f"SET search_path TO {_quoted_schema(schema)}, pg_catalog")
        connection.commit()
    finally:
        cursor.close()
    database = PlaneDatabase(ConnectionPool(_DedicatedDriverPool(connection)))
    BaselineMigrationRunner(
        database,
        MigrationRunner(
            database,
            revision=CURRENT_DATA_PLANE_REVISION,
            registry=MIGRATION_REGISTRY,
        ),
    ).run(expected_revision=CURRENT_DATA_PLANE_REVISION.schema_revision)
    try:
        yield _WorkspaceSchema(
            connection=connection,
            schema=schema,
            database=database,
        )
    finally:
        drop_postgres_fixture(connection, schema=schema)
        connection.close()


def _identifier() -> str:
    return str(uuid.uuid4())


def _stage_atomic_head(
    database: PlaneDatabase,
    workspaces: WorkspaceRepository,
    *,
    owner_id: str,
    conversation_id: str,
    base_revision: int,
    expected_head_publication_id: str | None,
    offset: int,
) -> str:
    publication_id = _identifier()
    with database.transaction() as transaction:
        workspaces.publications.stage(
            transaction,
            publication_id=publication_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            request_generation=_identifier(),
            base_render_revision=base_revision,
            started_at=NOW + timedelta(seconds=offset),
        )
        workspaces.publications.commit_at_head(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=publication_id,
            expected_staged_base_render_revision=base_revision,
            expected_head_render_revision=base_revision,
            expected_head_publication_id=expected_head_publication_id,
            committed_at=NOW + timedelta(seconds=offset + 1),
            updated_at=1000 + offset,
        )
    return publication_id


def _stage_assistant_result(
    database: PlaneDatabase,
    history: HistoryRepository,
    workspaces: WorkspaceRepository,
    *,
    owner_id: str,
    conversation_id: str,
    base_publication_id: str,
    base_revision: int,
    offset: int,
) -> tuple[str, str]:
    publication_id = _identifier()
    component_row_id = _identifier()
    with database.transaction() as transaction:
        workspaces.publications.stage(
            transaction,
            publication_id=publication_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            request_generation=_identifier(),
            base_render_revision=base_revision,
            started_at=NOW + timedelta(seconds=offset),
            publication_role="assistant_result",
            parent_publication_id=base_publication_id,
            execution_base_publication_id=base_publication_id,
            execution_base_render_revision=base_revision,
            execution_base_components_sha256="a" * 64,
            execution_base_layouts_sha256="b" * 64,
        )
        history.messages.append(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            role="assistant",
            content="draft result",
            timestamp=1000 + offset,
            publication_id=publication_id,
            commit_position=0,
            committed_render_revision=base_revision + 1,
        )
        workspaces.canvas.create(
            transaction,
            CanvasComponentRecord(
                row_id=component_row_id,
                conversation_id=conversation_id,
                owner_id=owner_id,
                component_id="component-old",
                payload={"component_id": "component-old", "type": "Card"},
                component_type="Card",
                title="Old",
                position=0,
                created_at=1000 + offset,
                updated_at=1000 + offset,
                publication_id=publication_id,
                committed_render_revision=base_revision + 1,
            ),
        )
        workspaces.layouts.create(
            transaction,
            LayoutRecord(
                layout_id=0,
                conversation_id=conversation_id,
                owner_id=owner_id,
                layout_key="layout-old",
                position=3,
                tree=[{"component_id": "component-old"}],
                created_at=1000 + offset,
                updated_at=1000 + offset,
                publication_id=publication_id,
                committed_render_revision=base_revision + 1,
            ),
        )
    return publication_id, component_row_id


def test_assistant_rebase_replay_owner_scope_and_outer_rollback(
    workspace_postgres_schema: _WorkspaceSchema,
) -> None:
    database = workspace_postgres_schema.database
    history = HistoryRepository()
    workspaces = WorkspaceRepository()
    owner_id = "workspace-owner"
    conversation_id = "workspace-chat"
    with database.transaction() as transaction:
        history.conversations.create(
            transaction,
            conversation_id=conversation_id,
            owner_id=owner_id,
            title="Workspace",
            agent_id=None,
            created_at=1000,
        )
    base_publication_id = _stage_atomic_head(
        database,
        workspaces,
        owner_id=owner_id,
        conversation_id=conversation_id,
        base_revision=0,
        expected_head_publication_id=None,
        offset=1,
    )
    result_publication_id, _ = _stage_assistant_result(
        database,
        history,
        workspaces,
        owner_id=owner_id,
        conversation_id=conversation_id,
        base_publication_id=base_publication_id,
        base_revision=1,
        offset=3,
    )
    competing_head_id = _stage_atomic_head(
        database,
        workspaces,
        owner_id=owner_id,
        conversation_id=conversation_id,
        base_revision=1,
        expected_head_publication_id=base_publication_id,
        offset=5,
    )
    target_component = PublicationRebaseComponent(
        row_id=_identifier(),
        component_id="component-new",
        payload={"component_id": "component-new", "type": "Card"},
        component_type="Card",
        title="New",
        position=0,
    )
    target_layout = PublicationRebaseLayout(
        layout_key="layout-new",
        position=7,
        tree=[{"component_id": "component-new"}],
    )
    with database.transaction() as transaction:
        first = workspaces.publications.rebase_assistant_stage(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=result_publication_id,
            expected_staged_base_render_revision=1,
            expected_head_render_revision=2,
            expected_head_publication_id=competing_head_id,
            components=(target_component,),
            layouts=(target_layout,),
            append_conflict_notice=True,
        )
        replay = workspaces.publications.rebase_assistant_stage(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=result_publication_id,
            expected_staged_base_render_revision=1,
            expected_head_render_revision=2,
            expected_head_publication_id=competing_head_id,
            components=(
                PublicationRebaseComponent(
                    row_id=_identifier(),
                    component_id=target_component.component_id,
                    payload=target_component.payload,
                    component_type=target_component.component_type,
                    title=target_component.title,
                    position=target_component.position,
                ),
            ),
            layouts=(target_layout,),
            append_conflict_notice=True,
        )
        assert first == replay
        workspaces.publications.commit_at_head(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=result_publication_id,
            expected_staged_base_render_revision=1,
            expected_head_render_revision=2,
            expected_head_publication_id=competing_head_id,
            committed_at=NOW + timedelta(seconds=8),
            updated_at=1008,
        )

    with database.transaction() as query:
        content = workspaces.publications.get_latest_committed_assistant_content(
            query,
            owner_id=owner_id,
            publication_id=result_publication_id,
        )
        components = workspaces.canvas.list_for_publication(
            query,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=result_publication_id,
            committed_render_revision=3,
            require_state="committed",
        )
        layouts = workspaces.layouts.list_for_publication(
            query,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=result_publication_id,
            committed_render_revision=3,
            require_state="committed",
        )
        assert workspaces.publications.get_for_owner(
            query,
            owner_id="different-owner",
            publication_id=result_publication_id,
        ) is None
    assert content is not None
    assert len(content.content) == 2
    assert content.content[-1]["type"] == "alert"
    assert tuple(record.component_id for record in components) == ("component-new",)
    assert tuple(record.layout_key for record in layouts) == ("layout-new",)
    assert layouts[0].position == 7

    rollback_publication_id, original_row_id = _stage_assistant_result(
        database,
        history,
        workspaces,
        owner_id=owner_id,
        conversation_id=conversation_id,
        base_publication_id=result_publication_id,
        base_revision=3,
        offset=9,
    )
    rollback_head_id = _stage_atomic_head(
        database,
        workspaces,
        owner_id=owner_id,
        conversation_id=conversation_id,
        base_revision=3,
        expected_head_publication_id=result_publication_id,
        offset=11,
    )
    with (
        pytest.raises(RuntimeError, match="injected outer rollback"),
        database.transaction() as transaction,
    ):
        workspaces.publications.rebase_assistant_stage(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=rollback_publication_id,
            expected_staged_base_render_revision=3,
            expected_head_render_revision=4,
            expected_head_publication_id=rollback_head_id,
            components=(
                PublicationRebaseComponent(
                    row_id=_identifier(),
                    component_id="rollback-new",
                    payload={"component_id": "rollback-new", "type": "Card"},
                    component_type="Card",
                    title="Rollback",
                    position=0,
                ),
            ),
            layouts=(),
            append_conflict_notice=True,
        )
        raise RuntimeError("injected outer rollback")
    with database.transaction() as query:
        original = workspaces.canvas.list_for_publication(
            query,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=rollback_publication_id,
            committed_render_revision=4,
            require_state="staged",
        )
        original_message = history.messages.get_by_publication_position(
            query,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=rollback_publication_id,
            commit_position=0,
        )
    assert tuple(record.row_id for record in original) == (original_row_id,)
    assert original_message is not None
    assert original_message.content == "draft result"
    assert original_message.committed_render_revision == 4
