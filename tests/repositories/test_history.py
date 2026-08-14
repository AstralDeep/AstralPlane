"""Focused contract tests for neutral history repositories."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

import pytest

from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryNotFoundError,
    RepositoryValidationError,
)
from astralplane.repositories.history import (
    ConversationRepository,
    HistoryRepository,
    MessageRepository,
    SessionRecord,
    SessionRepository,
)


@dataclass(frozen=True)
class Result:
    rowcount: int = 1
    status_message: str | None = None
    returned_records: tuple[dict[str, Any], ...] = ()


class FakeTransaction:
    def __init__(self) -> None:
        self.execute_results: deque[Result] = deque()
        self.fetch_one_results: deque[dict[str, Any] | None] = deque()
        self.fetch_all_results: deque[tuple[dict[str, Any], ...]] = deque()
        self.calls: list[tuple[str, str, object]] = []

    def execute(self, statement: str, parameters: object = ()) -> Result:
        self.calls.append(("execute", statement, parameters))
        return self.execute_results.popleft() if self.execute_results else Result()

    def fetch_one(self, statement: str, parameters: object = ()) -> dict[str, Any] | None:
        self.calls.append(("fetch_one", statement, parameters))
        return self.fetch_one_results.popleft() if self.fetch_one_results else None

    def fetch_all(self, statement: str, parameters: object = ()) -> tuple[dict[str, Any], ...]:
        self.calls.append(("fetch_all", statement, parameters))
        return self.fetch_all_results.popleft() if self.fetch_all_results else ()


def returned(row: dict[str, Any], *, rowcount: int = 1) -> Result:
    return Result(rowcount=rowcount, returned_records=(row,))


def conversation_row(**changes: Any) -> dict[str, Any]:
    row = {
        "id": "chat-1",
        "user_id": "owner-1",
        "title": "A chat",
        "agent_id": "agent-1",
        "created_at": 10,
        "updated_at": 10,
        "render_revision": 0,
        "conversation_commit_id": None,
        "has_saved_components": False,
    }
    row.update(changes)
    return row


def message_row(**changes: Any) -> dict[str, Any]:
    row = {
        "id": 7,
        "chat_id": "chat-1",
        "user_id": "owner-1",
        "role": "user",
        "content": '{"nested":["value"]}',
        "timestamp": 20,
        "conversation_commit_id": None,
        "commit_position": None,
        "committed_render_revision": None,
    }
    row.update(changes)
    return row


def session_row(**changes: Any) -> dict[str, Any]:
    row = {
        "sid": "session-1",
        "user_id": "owner-1",
        "access_token_enc": "cipher-a",
        "refresh_token_enc": "cipher-r",
        "interactive_anchor": 10,
        "hard_expires_at": 100,
        "last_refresh_at": 20,
        "resumed": False,
        "created_at": 5,
    }
    row.update(changes)
    return row


def test_conversation_create_is_owner_scoped_and_replay_safe() -> None:
    repository = ConversationRepository()
    transaction = FakeTransaction()
    transaction.execute_results.append(returned(conversation_row()))

    created = repository.create(
        transaction,
        conversation_id="chat-1",
        owner_id="owner-1",
        title="A chat",
        agent_id="agent-1",
        created_at=10,
    )

    assert created.conversation_id == "chat-1"
    assert transaction.calls[0][2][:3] == ("chat-1", "owner-1", "A chat")
    assert "%s" in transaction.calls[0][1]
    assert "?" not in transaction.calls[0][1]

    replay = FakeTransaction()
    replay.execute_results.append(Result(rowcount=0))
    replay.fetch_one_results.append(conversation_row())
    assert (
        repository.create(
            replay,
            conversation_id="chat-1",
            owner_id="owner-1",
            title="A chat",
            agent_id="agent-1",
            created_at=10,
        )
        == created
    )


def test_conversation_create_rejects_foreign_and_changed_replay() -> None:
    repository = ConversationRepository()
    foreign = FakeTransaction()
    foreign.execute_results.append(Result(rowcount=0))
    foreign.fetch_one_results.append(None)
    with pytest.raises(RepositoryConflictError, match="another namespace"):
        repository.create(
            foreign,
            conversation_id="chat-1",
            owner_id="owner-1",
            title="A chat",
            agent_id=None,
            created_at=10,
        )

    changed = FakeTransaction()
    changed.execute_results.append(Result(rowcount=0))
    changed.fetch_one_results.append(conversation_row(title="Other", agent_id=None))
    with pytest.raises(RepositoryConflictError, match="different semantics"):
        repository.create(
            changed,
            conversation_id="chat-1",
            owner_id="owner-1",
            title="A chat",
            agent_id=None,
            created_at=10,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("conversation_id", ""), ("owner_id", " "), ("title", ""), ("created_at", -1)],
)
def test_conversation_create_validates_inputs(field: str, value: object) -> None:
    values: dict[str, object] = {
        "conversation_id": "chat-1",
        "owner_id": "owner-1",
        "title": "A chat",
        "agent_id": None,
        "created_at": 1,
    }
    values[field] = value
    with pytest.raises(RepositoryValidationError):
        ConversationRepository().create(FakeTransaction(), **values)  # type: ignore[arg-type]


def test_conversation_queries_rename_cas_and_delete() -> None:
    repository = ConversationRepository()
    transaction = FakeTransaction()
    transaction.fetch_one_results.append(conversation_row())
    assert (
        repository.get(transaction, owner_id="owner-1", conversation_id="chat-1").owner_id
        == "owner-1"
    )  # type: ignore[union-attr]
    assert transaction.calls[-1][2] == ("chat-1", "owner-1")

    transaction.fetch_all_results.append(
        (conversation_row(), conversation_row(id="chat-2", agent_id=None))
    )
    assert len(repository.list_recent(transaction, owner_id="owner-1", limit=2)) == 2

    transaction.execute_results.append(returned(conversation_row(title="Renamed", updated_at=11)))
    renamed = repository.rename(
        transaction,
        owner_id="owner-1",
        conversation_id="chat-1",
        title="Renamed",
        expected_updated_at=10,
        updated_at=11,
    )
    assert renamed.title == "Renamed"

    transaction.execute_results.append(Result(rowcount=1))
    assert repository.delete(transaction, owner_id="owner-1", conversation_id="chat-1")
    assert "user_id = %s" in transaction.calls[-1][1]


def test_conversation_rename_distinguishes_missing_and_conflict() -> None:
    repository = ConversationRepository()
    missing = FakeTransaction()
    missing.execute_results.append(Result(rowcount=0))
    missing.fetch_one_results.append(None)
    with pytest.raises(RepositoryNotFoundError):
        repository.rename(
            missing,
            owner_id="owner-1",
            conversation_id="chat-1",
            title="Changed",
            expected_updated_at=10,
            updated_at=11,
        )

    conflict = FakeTransaction()
    conflict.execute_results.append(Result(rowcount=0))
    conflict.fetch_one_results.append(conversation_row(updated_at=12))
    with pytest.raises(RepositoryConflictError):
        repository.rename(
            conflict,
            owner_id="owner-1",
            conversation_id="chat-1",
            title="Changed",
            expected_updated_at=10,
            updated_at=11,
        )

    with pytest.raises(RepositoryValidationError, match="advance"):
        repository.rename(
            FakeTransaction(),
            owner_id="owner-1",
            conversation_id="chat-1",
            title="Changed",
            expected_updated_at=10,
            updated_at=10,
        )


def test_message_append_legacy_and_revisioned_paths_detach_content() -> None:
    repository = MessageRepository()
    legacy = FakeTransaction()
    legacy.execute_results.append(returned(message_row()))
    record = repository.append(
        legacy,
        owner_id="owner-1",
        conversation_id="chat-1",
        role="user",
        content={"nested": ["value"]},
        timestamp=20,
    )
    assert record.content["nested"] == ("value",)
    with pytest.raises(TypeError):
        record.content["new"] = "no"

    revisioned = FakeTransaction()
    revisioned.execute_results.append(
        returned(
            message_row(
                conversation_commit_id="commit-1",
                commit_position=0,
                committed_render_revision=1,
            )
        )
    )
    stored = repository.append(
        revisioned,
        owner_id="owner-1",
        conversation_id="chat-1",
        role="assistant",
        content="plain prose",
        timestamp=21,
        publication_id="commit-1",
        commit_position=0,
        committed_render_revision=1,
    )
    assert stored.content["nested"] == ("value",)
    assert "publication.owner_user_id = %s" in revisioned.calls[0][1]


def test_message_revisioned_replay_and_conflicts_are_visible() -> None:
    repository = MessageRepository()
    replay_row = message_row(
        role="assistant",
        content="plain prose",
        timestamp=21,
        conversation_commit_id="commit-1",
        commit_position=1,
        committed_render_revision=1,
    )
    replay = FakeTransaction()
    replay.execute_results.append(Result(rowcount=0))
    replay.fetch_one_results.append(replay_row)
    record = repository.append(
        replay,
        owner_id="owner-1",
        conversation_id="chat-1",
        role="assistant",
        content="plain prose",
        timestamp=21,
        publication_id="commit-1",
        commit_position=1,
        committed_render_revision=1,
    )
    assert record.content == "plain prose"

    changed = FakeTransaction()
    changed.execute_results.append(Result(rowcount=0))
    changed.fetch_one_results.append(replay_row | {"role": "user"})
    with pytest.raises(RepositoryConflictError, match="different semantics"):
        repository.append(
            changed,
            owner_id="owner-1",
            conversation_id="chat-1",
            role="assistant",
            content="plain prose",
            timestamp=21,
            publication_id="commit-1",
            commit_position=1,
            committed_render_revision=1,
        )

    missing = FakeTransaction()
    missing.execute_results.append(Result(rowcount=0))
    missing.fetch_one_results.append(None)
    with pytest.raises(RepositoryNotFoundError):
        repository.append(
            missing,
            owner_id="owner-1",
            conversation_id="chat-1",
            role="assistant",
            content="plain prose",
            timestamp=21,
            publication_id="commit-1",
            commit_position=1,
            committed_render_revision=1,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"commit_position": 1},
        {
            "publication_id": "commit-1",
            "commit_position": 1,
            "committed_render_revision": 0,
        },
    ],
)
def test_message_append_rejects_incomplete_publication_metadata(kwargs: dict[str, Any]) -> None:
    with pytest.raises(RepositoryValidationError):
        MessageRepository().append(
            FakeTransaction(),
            owner_id="owner-1",
            conversation_id="chat-1",
            role="user",
            content="text",
            timestamp=1,
            **kwargs,
        )


def test_message_legacy_missing_and_query_visibility() -> None:
    repository = MessageRepository()
    missing = FakeTransaction()
    missing.execute_results.append(Result(rowcount=0))
    with pytest.raises(RepositoryNotFoundError):
        repository.append(
            missing,
            owner_id="owner-1",
            conversation_id="chat-1",
            role="user",
            content="text",
            timestamp=1,
        )

    query = FakeTransaction()
    query.fetch_all_results.append(
        (
            message_row(content="plain"),
            message_row(id=8, content="[1,2]", timestamp=21),
        )
    )
    rows = repository.list_visible(
        query,
        owner_id="owner-1",
        conversation_id="chat-1",
        through_render_revision=2,
        limit=20,
    )
    assert rows[0].content == "plain"
    assert rows[1].content == (1, 2)
    assert query.calls[-1][2][:2] == ("chat-1", "owner-1")

    query.fetch_one_results.extend(({"id": 8}, None))
    assert repository.latest_visible_id(query, owner_id="owner-1", conversation_id="chat-1") == 8
    assert repository.latest_visible_id(query, owner_id="owner-1", conversation_id="chat-1") is None


def test_session_put_get_delete_and_owner_conflict() -> None:
    repository = SessionRepository()
    record = SessionRecord(
        session_id="session-1",
        owner_id="owner-1",
        access_token_ciphertext="cipher-a",
        refresh_token_ciphertext="cipher-r",
        interactive_anchor=10,
        hard_expires_at=100,
        last_refresh_at=20,
        resumed=False,
        created_at=5,
    )
    transaction = FakeTransaction()
    transaction.execute_results.append(returned(session_row()))
    assert repository.put(transaction, record) == record
    assert "web_session.user_id = EXCLUDED.user_id" in transaction.calls[0][1]

    query = FakeTransaction()
    query.fetch_one_results.extend((session_row(resumed=True), None))
    assert repository.get(query, owner_id="owner-1", session_id="session-1").resumed
    assert repository.get(query, owner_id="owner-1", session_id="missing") is None

    transaction.execute_results.extend((Result(rowcount=1), Result(rowcount=0)))
    assert repository.delete(transaction, owner_id="owner-1", session_id="session-1")
    assert not repository.delete(transaction, owner_id="owner-1", session_id="session-1")

    conflict = FakeTransaction()
    conflict.execute_results.append(Result(rowcount=0))
    with pytest.raises(RepositoryConflictError):
        repository.put(conflict, record)


@pytest.mark.parametrize(
    "record",
    [
        SessionRecord("", "owner", "a", "r", 1, 2, 1, False, 1),
        SessionRecord("sid", "owner", "", "r", 1, 2, 1, False, 1),
        SessionRecord("sid", "owner", "a", "r", -1, 2, 1, False, 1),
    ],
)
def test_session_validation(record: SessionRecord) -> None:
    with pytest.raises(RepositoryValidationError):
        SessionRepository().put(FakeTransaction(), record)


def test_history_facade_exposes_stateless_repositories() -> None:
    facade = HistoryRepository()
    assert isinstance(facade.conversations, ConversationRepository)
    assert isinstance(facade.messages, MessageRepository)
    assert isinstance(facade.sessions, SessionRepository)
