"""Workspace, canvas, layout, snapshot, and atomic publication persistence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from astralplane.contracts import QueryExecutor, Transaction
from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryNotFoundError,
    RepositoryValidationError,
    _bounded_limit,
    _bounded_text,
    _canonical_json,
    _non_negative_int,
    _positive_int,
    _required_id,
    _row_value,
    _single_returned,
    _structured_json,
)


@dataclass(frozen=True, slots=True)
class CanvasComponentRecord:
    row_id: str
    conversation_id: str
    owner_id: str
    component_id: str
    payload: Any
    component_type: str
    title: str | None
    position: int
    created_at: int
    updated_at: int
    publication_id: str | None = None
    committed_render_revision: int | None = None


@dataclass(frozen=True, slots=True)
class LayoutRecord:
    layout_id: int
    conversation_id: str
    owner_id: str
    layout_key: str
    position: int
    tree: Any
    created_at: int
    updated_at: int
    publication_id: str | None = None
    committed_render_revision: int | None = None


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshotRecord:
    snapshot_id: int
    conversation_id: str
    owner_id: str
    turn_message_id: int | None
    cause: str
    components: tuple[Any, ...]
    layouts: tuple[Any, ...]
    created_at: int


@dataclass(frozen=True, slots=True)
class PublicationRecord:
    publication_id: str
    conversation_id: str
    owner_id: str
    request_generation: str
    operation_id: str | None
    operation_execution_generation: int | None
    base_render_revision: int
    committed_render_revision: int | None
    state: str
    started_at: datetime
    committed_at: datetime | None
    aborted_at: datetime | None


def _optional_returned(result: object, operation: str) -> Any:
    if not getattr(result, "returned_records", ()):
        return None
    return _single_returned(result, operation)


def _aware_time(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise RepositoryValidationError(f"{field} must be a timezone-aware datetime")
    return value


def _persisted_time(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise RepositoryDataError(
            "persisted timestamp is not timezone-aware", metadata={"field": field}
        )
    return value


def _scope(
    publication_id: str | None,
    committed_render_revision: int | None,
) -> tuple[str | None, int | None]:
    if publication_id is None and committed_render_revision is None:
        return None, None
    if publication_id is None or committed_render_revision is None:
        raise RepositoryValidationError(
            "publication_id and committed_render_revision must be supplied together"
        )
    return (
        _required_id(publication_id, "publication_id"),
        _positive_int(committed_render_revision, "committed_render_revision"),
    )


def _component(row: Mapping[str, Any]) -> CanvasComponentRecord:
    return CanvasComponentRecord(
        row_id=str(_row_value(row, "id")),
        conversation_id=str(_row_value(row, "chat_id")),
        owner_id=str(_row_value(row, "user_id")),
        component_id=str(_row_value(row, "component_id")),
        payload=_structured_json(_row_value(row, "component_data"), "component_data"),
        component_type=str(_row_value(row, "component_type")),
        title=None if row.get("title") is None else str(row["title"]),
        position=int(_row_value(row, "position")),
        created_at=int(_row_value(row, "created_at")),
        updated_at=int(_row_value(row, "updated_at")),
        publication_id=(
            None
            if row.get("conversation_commit_id") is None
            else str(row["conversation_commit_id"])
        ),
        committed_render_revision=(
            None
            if row.get("committed_render_revision") is None
            else int(row["committed_render_revision"])
        ),
    )


def _layout(row: Mapping[str, Any]) -> LayoutRecord:
    return LayoutRecord(
        layout_id=int(_row_value(row, "id")),
        conversation_id=str(_row_value(row, "chat_id")),
        owner_id=str(_row_value(row, "user_id")),
        layout_key=str(_row_value(row, "layout_key")),
        position=int(_row_value(row, "position")),
        tree=_structured_json(_row_value(row, "layout"), "layout"),
        created_at=int(_row_value(row, "created_at")),
        updated_at=int(_row_value(row, "updated_at")),
        publication_id=(
            None
            if row.get("conversation_commit_id") is None
            else str(row["conversation_commit_id"])
        ),
        committed_render_revision=(
            None
            if row.get("committed_render_revision") is None
            else int(row["committed_render_revision"])
        ),
    )


def _snapshot(row: Mapping[str, Any]) -> WorkspaceSnapshotRecord:
    components = _structured_json(_row_value(row, "components"), "components")
    layouts = _structured_json(row.get("layouts"), "layouts", nullable=True)
    if not isinstance(components, tuple) or (
        layouts is not None and not isinstance(layouts, tuple)
    ):
        raise RepositoryDataError("workspace snapshot payloads must be JSON arrays")
    return WorkspaceSnapshotRecord(
        snapshot_id=int(_row_value(row, "id")),
        conversation_id=str(_row_value(row, "chat_id")),
        owner_id=str(_row_value(row, "user_id")),
        turn_message_id=(
            None if row.get("turn_message_id") is None else int(row["turn_message_id"])
        ),
        cause=str(_row_value(row, "cause")),
        components=components,
        layouts=() if layouts is None else layouts,
        created_at=int(_row_value(row, "created_at")),
    )


def _publication(row: Mapping[str, Any]) -> PublicationRecord:
    return PublicationRecord(
        publication_id=str(_row_value(row, "commit_id")),
        conversation_id=str(_row_value(row, "chat_id")),
        owner_id=str(_row_value(row, "owner_user_id")),
        request_generation=str(_row_value(row, "request_generation")),
        operation_id=(None if row.get("operation_id") is None else str(row["operation_id"])),
        operation_execution_generation=(
            None
            if row.get("operation_execution_generation") is None
            else int(row["operation_execution_generation"])
        ),
        base_render_revision=int(_row_value(row, "base_render_revision")),
        committed_render_revision=(
            None
            if row.get("committed_render_revision") is None
            else int(row["committed_render_revision"])
        ),
        state=str(_row_value(row, "state")),
        started_at=_persisted_time(_row_value(row, "started_at"), "started_at"),
        committed_at=(
            None
            if row.get("committed_at") is None
            else _persisted_time(row["committed_at"], "committed_at")
        ),
        aborted_at=(
            None
            if row.get("aborted_at") is None
            else _persisted_time(row["aborted_at"], "aborted_at")
        ),
    )


class CanvasRepository:
    _FIELDS = """
        id, chat_id, user_id, component_id, component_data, component_type,
        title, position, created_at, updated_at, conversation_commit_id,
        committed_render_revision
    """

    def create(
        self,
        transaction: Transaction,
        record: CanvasComponentRecord,
    ) -> CanvasComponentRecord:
        row_id = _required_id(record.row_id, "row_id")
        conversation_id = _required_id(record.conversation_id, "conversation_id")
        owner_id = _required_id(record.owner_id, "owner_id")
        component_id = _required_id(record.component_id, "component_id")
        component_type = _bounded_text(record.component_type, "component_type", maximum=128)
        if record.title is not None:
            _bounded_text(record.title, "title", maximum=512, allow_empty=True)
        position = _non_negative_int(record.position, "position")
        created_at = _non_negative_int(record.created_at, "created_at")
        updated_at = _non_negative_int(record.updated_at, "updated_at")
        if updated_at < created_at:
            raise RepositoryValidationError("updated_at cannot precede created_at")
        publication_id, revision = _scope(record.publication_id, record.committed_render_revision)
        payload = _canonical_json(record.payload, "payload")
        result = transaction.execute(
            f"""
            INSERT INTO saved_components (
                id, chat_id, user_id, component_id, component_data,
                component_type, title, position, created_at, updated_at,
                conversation_commit_id, committed_render_revision
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            FROM chats AS chat
            WHERE chat.id = %s AND chat.user_id = %s
              AND (
                (%s IS NULL AND COALESCE(chat.render_revision, 0) = 0)
                OR EXISTS (
                    SELECT 1 FROM conversation_commit AS publication
                    WHERE publication.commit_id = %s
                      AND publication.chat_id = chat.id
                      AND publication.owner_user_id = chat.user_id
                      AND publication.state = 'staged'
                      AND publication.base_render_revision + 1 = %s
                )
              )
            ON CONFLICT DO NOTHING
            RETURNING {self._FIELDS}
            """,
            (
                row_id,
                conversation_id,
                owner_id,
                component_id,
                payload,
                component_type,
                record.title,
                position,
                created_at,
                updated_at,
                publication_id,
                revision,
                conversation_id,
                owner_id,
                publication_id,
                publication_id,
                revision,
            ),
        )
        returned = _optional_returned(result, "canvas.create")
        if returned is not None:
            return _component(returned)
        existing = self.get_scoped(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            component_id=component_id,
            publication_id=publication_id,
            committed_render_revision=revision,
        )
        if existing is None:
            chat = transaction.fetch_one(
                "SELECT id FROM chats WHERE id = %s AND user_id = %s",
                (conversation_id, owner_id),
            )
            if chat is None:
                raise RepositoryNotFoundError(
                    "workspace conversation was not found",
                    metadata={"operation": "canvas.create"},
                )
            raise RepositoryConflictError(
                "canvas identity or publication scope is unavailable",
                metadata={"operation": "canvas.create"},
            )
        expected = (
            row_id,
            payload,
            component_type,
            record.title,
            position,
            created_at,
            updated_at,
        )
        observed = (
            existing.row_id,
            _canonical_json(existing.payload, "payload"),
            existing.component_type,
            existing.title,
            existing.position,
            existing.created_at,
            existing.updated_at,
        )
        if observed != expected:
            raise RepositoryConflictError(
                "canvas idempotency identity was reused with different semantics",
                metadata={"operation": "canvas.create"},
            )
        return existing

    def get_scoped(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
        publication_id: str | None = None,
        committed_render_revision: int | None = None,
    ) -> CanvasComponentRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        publication_id, revision = _scope(publication_id, committed_render_revision)
        row = query.fetch_one(
            f"""
            SELECT {self._FIELDS}
            FROM saved_components
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
              AND conversation_commit_id IS NOT DISTINCT FROM %s
              AND committed_render_revision IS NOT DISTINCT FROM %s
            """,
            (conversation_id, owner_id, component_id, publication_id, revision),
        )
        return None if row is None else _component(row)

    def list_current(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
    ) -> tuple[CanvasComponentRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        rows = query.fetch_all(
            """
            SELECT component.id, component.chat_id, component.user_id,
                   component.component_id, component.component_data,
                   component.component_type, component.title, component.position,
                   component.created_at, component.updated_at,
                   component.conversation_commit_id,
                   component.committed_render_revision
            FROM saved_components AS component
            JOIN chats AS chat
              ON chat.id = component.chat_id AND chat.user_id = component.user_id
            WHERE component.chat_id = %s AND component.user_id = %s
              AND (
                (COALESCE(chat.render_revision, 0) = 0
                 AND component.conversation_commit_id IS NULL
                 AND component.committed_render_revision IS NULL)
                OR (
                    chat.render_revision > 0
                    AND component.conversation_commit_id = chat.conversation_commit_id
                    AND component.committed_render_revision = chat.render_revision
                )
              )
            ORDER BY COALESCE(component.position, 2147483647), component.created_at,
                     component.id
            """,
            (conversation_id, owner_id),
        )
        return tuple(_component(row) for row in rows)

    def replace(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
        payload: object,
        component_type: str,
        title: str | None,
        expected_updated_at: int,
        updated_at: int,
        publication_id: str | None = None,
        committed_render_revision: int | None = None,
    ) -> CanvasComponentRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        component_type = _bounded_text(component_type, "component_type", maximum=128)
        if title is not None:
            _bounded_text(title, "title", maximum=512, allow_empty=True)
        expected_updated_at = _non_negative_int(expected_updated_at, "expected_updated_at")
        updated_at = _non_negative_int(updated_at, "updated_at")
        if updated_at <= expected_updated_at:
            raise RepositoryValidationError("updated_at must advance the compare-and-set fence")
        publication_id, revision = _scope(publication_id, committed_render_revision)
        result = transaction.execute(
            f"""
            UPDATE saved_components
            SET component_data = %s, component_type = %s, title = %s, updated_at = %s
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
              AND conversation_commit_id IS NOT DISTINCT FROM %s
              AND committed_render_revision IS NOT DISTINCT FROM %s
              AND updated_at = %s
            RETURNING {self._FIELDS}
            """,
            (
                _canonical_json(payload, "payload"),
                component_type,
                title,
                updated_at,
                conversation_id,
                owner_id,
                component_id,
                publication_id,
                revision,
                expected_updated_at,
            ),
        )
        row = _optional_returned(result, "canvas.replace")
        if row is not None:
            return _component(row)
        existing = self.get_scoped(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            component_id=component_id,
            publication_id=publication_id,
            committed_render_revision=revision,
        )
        if existing is None:
            raise RepositoryNotFoundError(
                "canvas component was not found",
                metadata={"operation": "canvas.replace"},
            )
        raise RepositoryConflictError(
            "canvas component changed since it was read",
            metadata={"operation": "canvas.replace"},
        )

    def remove(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
        publication_id: str | None = None,
        committed_render_revision: int | None = None,
    ) -> bool:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        publication_id, revision = _scope(publication_id, committed_render_revision)
        result = transaction.execute(
            """
            DELETE FROM saved_components
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
              AND conversation_commit_id IS NOT DISTINCT FROM %s
              AND committed_render_revision IS NOT DISTINCT FROM %s
            """,
            (conversation_id, owner_id, component_id, publication_id, revision),
        )
        return result.rowcount == 1


class LayoutRepository:
    _FIELDS = """
        id, chat_id, user_id, layout_key, position, layout, created_at,
        updated_at, conversation_commit_id, committed_render_revision
    """

    def create(self, transaction: Transaction, record: LayoutRecord) -> LayoutRecord:
        conversation_id = _required_id(record.conversation_id, "conversation_id")
        owner_id = _required_id(record.owner_id, "owner_id")
        layout_key = _required_id(record.layout_key, "layout_key")
        position = _non_negative_int(record.position, "position")
        created_at = _non_negative_int(record.created_at, "created_at")
        updated_at = _non_negative_int(record.updated_at, "updated_at")
        if updated_at < created_at:
            raise RepositoryValidationError("updated_at cannot precede created_at")
        publication_id, revision = _scope(record.publication_id, record.committed_render_revision)
        tree_payload = _canonical_json(record.tree, "tree")
        result = transaction.execute(
            f"""
            INSERT INTO workspace_layout (
                chat_id, user_id, layout_key, position, layout, created_at,
                updated_at, conversation_commit_id, committed_render_revision
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
            FROM chats AS chat
            WHERE chat.id = %s AND chat.user_id = %s
              AND (
                (%s IS NULL AND COALESCE(chat.render_revision, 0) = 0)
                OR EXISTS (
                    SELECT 1 FROM conversation_commit AS publication
                    WHERE publication.commit_id = %s
                      AND publication.chat_id = chat.id
                      AND publication.owner_user_id = chat.user_id
                      AND publication.state = 'staged'
                      AND publication.base_render_revision + 1 = %s
                )
              )
            ON CONFLICT DO NOTHING
            RETURNING {self._FIELDS}
            """,
            (
                conversation_id,
                owner_id,
                layout_key,
                position,
                tree_payload,
                created_at,
                updated_at,
                publication_id,
                revision,
                conversation_id,
                owner_id,
                publication_id,
                publication_id,
                revision,
            ),
        )
        row = _optional_returned(result, "layout.create")
        if row is not None:
            return _layout(row)
        existing = self.get_scoped(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            layout_key=layout_key,
            publication_id=publication_id,
            committed_render_revision=revision,
        )
        if existing is None:
            raise RepositoryConflictError(
                "layout identity or publication scope is unavailable",
                metadata={"operation": "layout.create"},
            )
        expected = (tree_payload, position, created_at, updated_at)
        observed = (
            _canonical_json(existing.tree, "tree"),
            existing.position,
            existing.created_at,
            existing.updated_at,
        )
        if observed != expected:
            raise RepositoryConflictError(
                "layout idempotency identity was reused with different semantics",
                metadata={"operation": "layout.create"},
            )
        return existing

    def get_scoped(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        layout_key: str,
        publication_id: str | None = None,
        committed_render_revision: int | None = None,
    ) -> LayoutRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        layout_key = _required_id(layout_key, "layout_key")
        publication_id, revision = _scope(publication_id, committed_render_revision)
        row = query.fetch_one(
            f"""
            SELECT {self._FIELDS}
            FROM workspace_layout
            WHERE chat_id = %s AND user_id = %s AND layout_key = %s
              AND conversation_commit_id IS NOT DISTINCT FROM %s
              AND committed_render_revision IS NOT DISTINCT FROM %s
            """,
            (conversation_id, owner_id, layout_key, publication_id, revision),
        )
        return None if row is None else _layout(row)

    def list_current(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
    ) -> tuple[LayoutRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        rows = query.fetch_all(
            """
            SELECT layout.id, layout.chat_id, layout.user_id, layout.layout_key,
                   layout.position, layout.layout, layout.created_at,
                   layout.updated_at, layout.conversation_commit_id,
                   layout.committed_render_revision
            FROM workspace_layout AS layout
            JOIN chats AS chat
              ON chat.id = layout.chat_id AND chat.user_id = layout.user_id
            WHERE layout.chat_id = %s AND layout.user_id = %s
              AND (
                (COALESCE(chat.render_revision, 0) = 0
                 AND layout.conversation_commit_id IS NULL
                 AND layout.committed_render_revision IS NULL)
                OR (
                    chat.render_revision > 0
                    AND layout.conversation_commit_id = chat.conversation_commit_id
                    AND layout.committed_render_revision = chat.render_revision
                )
              )
            ORDER BY layout.position, layout.id
            """,
            (conversation_id, owner_id),
        )
        return tuple(_layout(row) for row in rows)

    def replace(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        layout_key: str,
        tree: object,
        expected_updated_at: int,
        updated_at: int,
        publication_id: str | None = None,
        committed_render_revision: int | None = None,
    ) -> LayoutRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        layout_key = _required_id(layout_key, "layout_key")
        expected_updated_at = _non_negative_int(expected_updated_at, "expected_updated_at")
        updated_at = _non_negative_int(updated_at, "updated_at")
        if updated_at <= expected_updated_at:
            raise RepositoryValidationError("updated_at must advance the compare-and-set fence")
        publication_id, revision = _scope(publication_id, committed_render_revision)
        result = transaction.execute(
            f"""
            UPDATE workspace_layout
            SET layout = %s, updated_at = %s
            WHERE chat_id = %s AND user_id = %s AND layout_key = %s
              AND conversation_commit_id IS NOT DISTINCT FROM %s
              AND committed_render_revision IS NOT DISTINCT FROM %s
              AND updated_at = %s
            RETURNING {self._FIELDS}
            """,
            (
                _canonical_json(tree, "tree"),
                updated_at,
                conversation_id,
                owner_id,
                layout_key,
                publication_id,
                revision,
                expected_updated_at,
            ),
        )
        row = _optional_returned(result, "layout.replace")
        if row is not None:
            return _layout(row)
        existing = self.get_scoped(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            layout_key=layout_key,
            publication_id=publication_id,
            committed_render_revision=revision,
        )
        if existing is None:
            raise RepositoryNotFoundError(
                "workspace layout was not found",
                metadata={"operation": "layout.replace"},
            )
        raise RepositoryConflictError(
            "workspace layout changed since it was read",
            metadata={"operation": "layout.replace"},
        )

    def remove(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        layout_key: str,
        publication_id: str | None = None,
        committed_render_revision: int | None = None,
    ) -> bool:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        layout_key = _required_id(layout_key, "layout_key")
        publication_id, revision = _scope(publication_id, committed_render_revision)
        result = transaction.execute(
            """
            DELETE FROM workspace_layout
            WHERE chat_id = %s AND user_id = %s AND layout_key = %s
              AND conversation_commit_id IS NOT DISTINCT FROM %s
              AND committed_render_revision IS NOT DISTINCT FROM %s
            """,
            (conversation_id, owner_id, layout_key, publication_id, revision),
        )
        return result.rowcount == 1


class WorkspaceSnapshotRepository:
    def capture(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        cause: str,
        components: Sequence[object],
        layouts: Sequence[object] = (),
        created_at: int,
        turn_message_id: int | None = None,
    ) -> WorkspaceSnapshotRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        cause = _bounded_text(cause, "cause", maximum=128)
        created_at = _non_negative_int(created_at, "created_at")
        if turn_message_id is not None:
            turn_message_id = _positive_int(turn_message_id, "turn_message_id")
        result = transaction.execute(
            """
            INSERT INTO workspace_snapshot (
                chat_id, user_id, turn_message_id, cause, components, layouts, created_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s
            FROM chats
            WHERE id = %s AND user_id = %s
            RETURNING id, chat_id, user_id, turn_message_id, cause,
                      components, layouts, created_at
            """,
            (
                conversation_id,
                owner_id,
                turn_message_id,
                cause,
                _canonical_json(tuple(components), "components"),
                None if not layouts else _canonical_json(tuple(layouts), "layouts"),
                created_at,
                conversation_id,
                owner_id,
            ),
        )
        row = _optional_returned(result, "workspace_snapshot.capture")
        if row is None:
            raise RepositoryNotFoundError(
                "workspace conversation was not found",
                metadata={"operation": "workspace_snapshot.capture"},
            )
        return _snapshot(row)

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        snapshot_id: int,
    ) -> WorkspaceSnapshotRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        snapshot_id = _positive_int(snapshot_id, "snapshot_id")
        row = query.fetch_one(
            """
            SELECT id, chat_id, user_id, turn_message_id, cause,
                   components, layouts, created_at
            FROM workspace_snapshot
            WHERE id = %s AND user_id = %s
            """,
            (snapshot_id, owner_id),
        )
        return None if row is None else _snapshot(row)

    def list_for_conversation(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        limit: int = 50,
    ) -> tuple[WorkspaceSnapshotRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        limit = _bounded_limit(limit)
        rows = query.fetch_all(
            """
            SELECT id, chat_id, user_id, turn_message_id, cause,
                   components, layouts, created_at
            FROM workspace_snapshot
            WHERE chat_id = %s AND user_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (conversation_id, owner_id, limit),
        )
        return tuple(_snapshot(row) for row in rows)


class PublicationRepository:
    _FIELDS = """
        commit_id, chat_id, owner_user_id, request_generation, operation_id,
        operation_execution_generation, base_render_revision,
        committed_render_revision, state, started_at, committed_at, aborted_at
    """

    def stage(
        self,
        transaction: Transaction,
        *,
        publication_id: str,
        owner_id: str,
        conversation_id: str,
        request_generation: str,
        base_render_revision: int,
        started_at: datetime,
        operation_id: str | None = None,
        operation_execution_generation: int | None = None,
    ) -> PublicationRecord:
        publication_id = _required_id(publication_id, "publication_id")
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        request_generation = _required_id(request_generation, "request_generation")
        base_render_revision = _non_negative_int(base_render_revision, "base_render_revision")
        started_at = _aware_time(started_at, "started_at")
        if operation_id is not None:
            operation_id = _required_id(operation_id, "operation_id")
        if operation_execution_generation is not None:
            operation_execution_generation = _positive_int(
                operation_execution_generation, "operation_execution_generation"
            )
        result = transaction.execute(
            f"""
            INSERT INTO conversation_commit (
                commit_id, chat_id, owner_user_id, request_generation,
                operation_id, operation_execution_generation,
                base_render_revision, committed_render_revision, state,
                started_at, committed_at, aborted_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, NULL, 'staged', %s, NULL, NULL
            FROM chats
            WHERE id = %s AND user_id = %s
              AND COALESCE(render_revision, 0) = %s
            ON CONFLICT (chat_id, request_generation) DO NOTHING
            RETURNING {self._FIELDS}
            """,
            (
                publication_id,
                conversation_id,
                owner_id,
                request_generation,
                operation_id,
                operation_execution_generation,
                base_render_revision,
                started_at,
                conversation_id,
                owner_id,
                base_render_revision,
            ),
        )
        row = _optional_returned(result, "publication.stage")
        if row is not None:
            return _publication(row)
        existing = self.get_by_request(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            request_generation=request_generation,
        )
        if existing is None:
            chat = transaction.fetch_one(
                "SELECT id FROM chats WHERE id = %s AND user_id = %s",
                (conversation_id, owner_id),
            )
            if chat is None:
                raise RepositoryNotFoundError(
                    "publication conversation was not found",
                    metadata={"operation": "publication.stage"},
                )
            raise RepositoryConflictError(
                "conversation render revision changed before publication staging",
                metadata={"operation": "publication.stage"},
            )
        expected = (
            publication_id,
            operation_id,
            operation_execution_generation,
            base_render_revision,
        )
        observed = (
            existing.publication_id,
            existing.operation_id,
            existing.operation_execution_generation,
            existing.base_render_revision,
        )
        if expected != observed:
            raise RepositoryConflictError(
                "publication request generation was reused with different semantics",
                metadata={"operation": "publication.stage"},
            )
        return existing

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        publication_id: str,
    ) -> PublicationRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        publication_id = _required_id(publication_id, "publication_id")
        row = query.fetch_one(
            f"""
            SELECT {self._FIELDS}
            FROM conversation_commit
            WHERE commit_id = %s AND chat_id = %s AND owner_user_id = %s
            """,
            (publication_id, conversation_id, owner_id),
        )
        return None if row is None else _publication(row)

    def get_by_request(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        request_generation: str,
    ) -> PublicationRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        request_generation = _required_id(request_generation, "request_generation")
        row = query.fetch_one(
            f"""
            SELECT {self._FIELDS}
            FROM conversation_commit
            WHERE chat_id = %s AND owner_user_id = %s AND request_generation = %s
            """,
            (conversation_id, owner_id, request_generation),
        )
        return None if row is None else _publication(row)

    def _chat_authority_matches(
        self,
        query: QueryExecutor,
        publication: PublicationRecord,
    ) -> bool:
        row = query.fetch_one(
            """
            SELECT 1 AS matched
            FROM chats
            WHERE id = %s AND user_id = %s
              AND render_revision = %s AND conversation_commit_id = %s
            """,
            (
                publication.conversation_id,
                publication.owner_id,
                publication.committed_render_revision,
                publication.publication_id,
            ),
        )
        return row is not None

    def commit(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        publication_id: str,
        expected_base_render_revision: int,
        committed_at: datetime,
        updated_at: int,
    ) -> PublicationRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        publication_id = _required_id(publication_id, "publication_id")
        expected_base_render_revision = _non_negative_int(
            expected_base_render_revision, "expected_base_render_revision"
        )
        committed_at = _aware_time(committed_at, "committed_at")
        updated_at = _non_negative_int(updated_at, "updated_at")
        next_revision = expected_base_render_revision + 1
        result = transaction.execute(
            f"""
            UPDATE conversation_commit
            SET state = 'committed', committed_render_revision = %s,
                committed_at = %s
            WHERE commit_id = %s AND chat_id = %s AND owner_user_id = %s
              AND state = 'staged' AND base_render_revision = %s
            RETURNING {self._FIELDS}
            """,
            (
                next_revision,
                committed_at,
                publication_id,
                conversation_id,
                owner_id,
                expected_base_render_revision,
            ),
        )
        row = _optional_returned(result, "publication.commit")
        if row is None:
            existing = self.get(
                transaction,
                owner_id=owner_id,
                conversation_id=conversation_id,
                publication_id=publication_id,
            )
            if existing is None:
                raise RepositoryNotFoundError(
                    "publication was not found",
                    metadata={"operation": "publication.commit"},
                )
            if (
                existing.state == "committed"
                and existing.committed_render_revision == next_revision
                and self._chat_authority_matches(transaction, existing)
            ):
                return existing
            raise RepositoryConflictError(
                "publication is not in the expected staged generation",
                metadata={"operation": "publication.commit"},
            )
        publication = _publication(row)
        chat_result = transaction.execute(
            """
            UPDATE chats
            SET render_revision = %s, conversation_commit_id = %s,
                snapshot_committed_at = %s, updated_at = %s,
                has_saved_components = EXISTS (
                    SELECT 1 FROM saved_components
                    WHERE chat_id = %s AND user_id = %s
                      AND conversation_commit_id = %s
                      AND committed_render_revision = %s
                )
            WHERE id = %s AND user_id = %s AND render_revision = %s
            """,
            (
                next_revision,
                publication_id,
                committed_at,
                updated_at,
                conversation_id,
                owner_id,
                publication_id,
                next_revision,
                conversation_id,
                owner_id,
                expected_base_render_revision,
            ),
        )
        if chat_result.rowcount != 1:
            raise RepositoryConflictError(
                "conversation authority changed during atomic publication",
                metadata={"operation": "publication.commit"},
            )
        return publication

    def abort(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        publication_id: str,
        aborted_at: datetime,
    ) -> PublicationRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        publication_id = _required_id(publication_id, "publication_id")
        aborted_at = _aware_time(aborted_at, "aborted_at")
        result = transaction.execute(
            f"""
            UPDATE conversation_commit
            SET state = 'aborted', aborted_at = %s
            WHERE commit_id = %s AND chat_id = %s AND owner_user_id = %s
              AND state = 'staged'
            RETURNING {self._FIELDS}
            """,
            (aborted_at, publication_id, conversation_id, owner_id),
        )
        row = _optional_returned(result, "publication.abort")
        if row is None:
            existing = self.get(
                transaction,
                owner_id=owner_id,
                conversation_id=conversation_id,
                publication_id=publication_id,
            )
            if existing is None:
                raise RepositoryNotFoundError(
                    "publication was not found",
                    metadata={"operation": "publication.abort"},
                )
            if existing.state == "aborted":
                return existing
            raise RepositoryConflictError(
                "a committed publication cannot be aborted",
                metadata={"operation": "publication.abort"},
            )
        for table in ("saved_components", "workspace_layout", "messages"):
            transaction.execute(
                f"DELETE FROM {table} "
                "WHERE conversation_commit_id = %s AND chat_id = %s AND user_id = %s",
                (publication_id, conversation_id, owner_id),
            )
        return _publication(row)


class WorkspaceRepository:
    """Grouping of neutral workspace stores without transaction ownership."""

    def __init__(self) -> None:
        self.canvas = CanvasRepository()
        self.layouts = LayoutRepository()
        self.snapshots = WorkspaceSnapshotRepository()
        self.publications = PublicationRepository()


__all__ = (
    "CanvasComponentRecord",
    "CanvasRepository",
    "LayoutRecord",
    "LayoutRepository",
    "PublicationRecord",
    "PublicationRepository",
    "WorkspaceRepository",
    "WorkspaceSnapshotRecord",
    "WorkspaceSnapshotRepository",
)
