"""Owner-isolated conversation, message, and durable web-session repositories."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from astralplane.contracts import QueryExecutor, Transaction
from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryNotFoundError,
    RepositoryValidationError,
    _bounded_limit,
    _bounded_text,
    _canonical_json,
    _content_value,
    _non_negative_int,
    _required_id,
    _row_value,
    _single_returned,
)


@dataclass(frozen=True, slots=True)
class ConversationRecord:
    """Detached metadata for one owner-scoped conversation."""

    conversation_id: str
    owner_id: str
    title: str
    agent_id: str | None
    created_at: int
    updated_at: int
    render_revision: int
    publication_id: str | None
    has_saved_components: bool


@dataclass(frozen=True, slots=True)
class MessageRecord:
    """Detached message content visible at an authoritative render revision."""

    message_id: int
    conversation_id: str
    owner_id: str
    role: str
    content: Any
    timestamp: int
    publication_id: str | None
    commit_position: int | None
    committed_render_revision: int | None


@dataclass(frozen=True, slots=True)
class SessionRecord:
    """Opaque encrypted session state; token plaintext never enters Plane."""

    session_id: str
    owner_id: str
    access_token_ciphertext: str
    refresh_token_ciphertext: str
    interactive_anchor: int
    hard_expires_at: int
    last_refresh_at: int
    resumed: bool
    created_at: int


def _optional_returned(result: object, operation: str) -> Any:
    rows = getattr(result, "returned_records", ())
    if not rows:
        return None
    return _single_returned(result, operation)


def _conversation(row: Any) -> ConversationRecord:
    return ConversationRecord(
        conversation_id=str(_row_value(row, "id")),
        owner_id=str(_row_value(row, "user_id")),
        title=str(_row_value(row, "title")),
        agent_id=None if row.get("agent_id") is None else str(row["agent_id"]),
        created_at=int(_row_value(row, "created_at")),
        updated_at=int(_row_value(row, "updated_at")),
        render_revision=int(row.get("render_revision") or 0),
        publication_id=(
            None
            if row.get("conversation_commit_id") is None
            else str(row["conversation_commit_id"])
        ),
        has_saved_components=bool(row.get("has_saved_components")),
    )


def _message(row: Any) -> MessageRecord:
    return MessageRecord(
        message_id=int(_row_value(row, "id")),
        conversation_id=str(_row_value(row, "chat_id")),
        owner_id=str(_row_value(row, "user_id")),
        role=str(_row_value(row, "role")),
        content=_content_value(_row_value(row, "content")),
        timestamp=int(_row_value(row, "timestamp")),
        publication_id=(
            None
            if row.get("conversation_commit_id") is None
            else str(row["conversation_commit_id"])
        ),
        commit_position=(
            None if row.get("commit_position") is None else int(row["commit_position"])
        ),
        committed_render_revision=(
            None
            if row.get("committed_render_revision") is None
            else int(row["committed_render_revision"])
        ),
    )


def _session(row: Any) -> SessionRecord:
    return SessionRecord(
        session_id=str(_row_value(row, "sid")),
        owner_id=str(_row_value(row, "user_id")),
        access_token_ciphertext=str(_row_value(row, "access_token_enc")),
        refresh_token_ciphertext=str(_row_value(row, "refresh_token_enc")),
        interactive_anchor=int(_row_value(row, "interactive_anchor")),
        hard_expires_at=int(_row_value(row, "hard_expires_at")),
        last_refresh_at=int(_row_value(row, "last_refresh_at")),
        resumed=bool(row.get("resumed")),
        created_at=int(_row_value(row, "created_at")),
    )


class ConversationRepository:
    """Durable conversation metadata with stable-ID replay protection."""

    _SELECT = """
        SELECT id, user_id, title, agent_id, created_at, updated_at,
               COALESCE(render_revision, 0) AS render_revision,
               conversation_commit_id, COALESCE(has_saved_components, FALSE)
                   AS has_saved_components
        FROM chats
    """

    def create(
        self,
        transaction: Transaction,
        *,
        conversation_id: str,
        owner_id: str,
        title: str,
        agent_id: str | None,
        created_at: int,
    ) -> ConversationRecord:
        conversation_id = _required_id(conversation_id, "conversation_id")
        owner_id = _required_id(owner_id, "owner_id")
        title = _bounded_text(title, "title", maximum=512)
        if agent_id is not None:
            agent_id = _required_id(agent_id, "agent_id")
        created_at = _non_negative_int(created_at, "created_at")
        result = transaction.execute(
            """
            INSERT INTO chats (
                id, user_id, title, agent_id, created_at, updated_at,
                has_saved_components, render_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, FALSE, 0)
            ON CONFLICT (id) DO NOTHING
            RETURNING id, user_id, title, agent_id, created_at, updated_at,
                      render_revision, conversation_commit_id, has_saved_components
            """,
            (conversation_id, owner_id, title, agent_id, created_at, created_at),
        )
        row = _optional_returned(result, "conversation.create")
        if row is not None:
            return _conversation(row)
        existing = self.get(transaction, owner_id=owner_id, conversation_id=conversation_id)
        if existing is None:
            raise RepositoryConflictError(
                "conversation identity is already owned by another namespace",
                metadata={"operation": "conversation.create"},
            )
        expected = (title, agent_id, created_at)
        observed = (existing.title, existing.agent_id, existing.created_at)
        if observed != expected:
            raise RepositoryConflictError(
                "conversation idempotency identity was reused with different semantics",
                metadata={"operation": "conversation.create"},
            )
        return existing

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
    ) -> ConversationRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        row = query.fetch_one(
            self._SELECT + " WHERE id = %s AND user_id = %s",
            (conversation_id, owner_id),
        )
        return None if row is None else _conversation(row)

    def list_recent(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        limit: int = 20,
    ) -> tuple[ConversationRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        limit = _bounded_limit(limit)
        rows = query.fetch_all(
            self._SELECT + " WHERE user_id = %s ORDER BY updated_at DESC, id ASC LIMIT %s",
            (owner_id, limit),
        )
        return tuple(_conversation(row) for row in rows)

    def rename(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        title: str,
        expected_updated_at: int,
        updated_at: int,
    ) -> ConversationRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        title = _bounded_text(title, "title", maximum=512)
        expected_updated_at = _non_negative_int(expected_updated_at, "expected_updated_at")
        updated_at = _non_negative_int(updated_at, "updated_at")
        if updated_at <= expected_updated_at:
            raise RepositoryValidationError("updated_at must advance the compare-and-set fence")
        result = transaction.execute(
            """
            UPDATE chats
            SET title = %s, updated_at = %s
            WHERE id = %s AND user_id = %s AND updated_at = %s
            RETURNING id, user_id, title, agent_id, created_at, updated_at,
                      render_revision, conversation_commit_id, has_saved_components
            """,
            (title, updated_at, conversation_id, owner_id, expected_updated_at),
        )
        row = _optional_returned(result, "conversation.rename")
        if row is not None:
            return _conversation(row)
        existing = self.get(transaction, owner_id=owner_id, conversation_id=conversation_id)
        if existing is None:
            raise RepositoryNotFoundError(
                "conversation was not found", metadata={"operation": "conversation.rename"}
            )
        raise RepositoryConflictError(
            "conversation changed since it was read",
            metadata={"operation": "conversation.rename"},
        )

    def delete(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
    ) -> bool:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        result = transaction.execute(
            "DELETE FROM chats WHERE id = %s AND user_id = %s",
            (conversation_id, owner_id),
        )
        return result.rowcount == 1


class MessageRepository:
    """Conversation messages with revisioned publication visibility."""

    _FIELDS = (
        "message.id, message.chat_id, message.user_id, message.role, message.content, "
        "message.timestamp, message.conversation_commit_id, message.commit_position, "
        "message.committed_render_revision"
    )

    @staticmethod
    def _stored_content(content: object) -> str:
        return content if isinstance(content, str) else _canonical_json(content, "content")

    def append(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        role: str,
        content: object,
        timestamp: int,
        publication_id: str | None = None,
        commit_position: int | None = None,
        committed_render_revision: int | None = None,
    ) -> MessageRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        role = _bounded_text(role, "role", maximum=64)
        timestamp = _non_negative_int(timestamp, "timestamp")
        stored_content = self._stored_content(content)
        revisioned = publication_id is not None
        if revisioned:
            publication_id = _required_id(publication_id, "publication_id")
            commit_position = _non_negative_int(commit_position, "commit_position")
            committed_render_revision = _non_negative_int(
                committed_render_revision, "committed_render_revision"
            )
            if committed_render_revision == 0:
                raise RepositoryValidationError(
                    "revisioned messages require a positive render revision"
                )
            result = transaction.execute(
                """
                INSERT INTO messages (
                    chat_id, user_id, role, content, timestamp,
                    conversation_commit_id, commit_position,
                    committed_render_revision
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                FROM conversation_commit AS publication
                WHERE publication.commit_id = %s
                  AND publication.chat_id = %s
                  AND publication.owner_user_id = %s
                  AND publication.state = 'staged'
                  AND publication.base_render_revision + 1 = %s
                ON CONFLICT (conversation_commit_id, commit_position)
                    WHERE conversation_commit_id IS NOT NULL
                    DO NOTHING
                RETURNING id, chat_id, user_id, role, content, timestamp,
                          conversation_commit_id, commit_position,
                          committed_render_revision
                """,
                (
                    conversation_id,
                    owner_id,
                    role,
                    stored_content,
                    timestamp,
                    publication_id,
                    commit_position,
                    committed_render_revision,
                    publication_id,
                    conversation_id,
                    owner_id,
                    committed_render_revision,
                ),
            )
        else:
            if commit_position is not None or committed_render_revision is not None:
                raise RepositoryValidationError("legacy messages cannot carry publication metadata")
            result = transaction.execute(
                """
                INSERT INTO messages (chat_id, user_id, role, content, timestamp)
                SELECT %s, %s, %s, %s, %s
                FROM chats
                WHERE id = %s AND user_id = %s
                  AND COALESCE(render_revision, 0) = 0
                RETURNING id, chat_id, user_id, role, content, timestamp,
                          conversation_commit_id, commit_position,
                          committed_render_revision
                """,
                (
                    conversation_id,
                    owner_id,
                    role,
                    stored_content,
                    timestamp,
                    conversation_id,
                    owner_id,
                ),
            )
        row = _optional_returned(result, "message.append")
        if row is not None:
            return _message(row)
        if not revisioned:
            raise RepositoryNotFoundError(
                "conversation is unavailable for legacy message publication",
                metadata={"operation": "message.append"},
            )
        existing = self.get_by_publication_position(
            transaction,
            owner_id=owner_id,
            conversation_id=conversation_id,
            publication_id=publication_id,
            commit_position=commit_position,
        )
        if existing is None:
            raise RepositoryNotFoundError(
                "staged publication is unavailable",
                metadata={"operation": "message.append"},
            )
        expected = (
            role,
            _content_value(stored_content),
            timestamp,
            committed_render_revision,
        )
        observed = (
            existing.role,
            existing.content,
            existing.timestamp,
            existing.committed_render_revision,
        )
        if observed != expected:
            raise RepositoryConflictError(
                "message idempotency position was reused with different semantics",
                metadata={"operation": "message.append"},
            )
        return existing

    def get_by_publication_position(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        publication_id: str,
        commit_position: int,
    ) -> MessageRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        publication_id = _required_id(publication_id, "publication_id")
        commit_position = _non_negative_int(commit_position, "commit_position")
        row = query.fetch_one(
            f"""
            SELECT {self._FIELDS}
            FROM messages AS message
            WHERE message.chat_id = %s AND message.user_id = %s
              AND message.conversation_commit_id = %s
              AND message.commit_position = %s
            """,
            (conversation_id, owner_id, publication_id, commit_position),
        )
        return None if row is None else _message(row)

    def list_visible(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        through_render_revision: int | None = None,
        limit: int = 200,
    ) -> tuple[MessageRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        limit = _bounded_limit(limit, maximum=1000)
        if through_render_revision is not None:
            through_render_revision = _non_negative_int(
                through_render_revision, "through_render_revision"
            )
        rows = query.fetch_all(
            f"""
            SELECT {self._FIELDS}
            FROM messages AS message
            LEFT JOIN conversation_commit AS publication
              ON publication.commit_id = message.conversation_commit_id
             AND publication.chat_id = message.chat_id
             AND publication.owner_user_id = message.user_id
            WHERE message.chat_id = %s AND message.user_id = %s
              AND (
                message.conversation_commit_id IS NULL
                OR (
                    publication.state = 'committed'
                    AND publication.committed_render_revision =
                        message.committed_render_revision
                    AND (%s IS NULL OR message.committed_render_revision <= %s)
                )
              )
            ORDER BY message.timestamp ASC, message.id ASC
            LIMIT %s
            """,
            (
                conversation_id,
                owner_id,
                through_render_revision,
                through_render_revision,
                limit,
            ),
        )
        return tuple(_message(row) for row in rows)

    def latest_visible_id(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
    ) -> int | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        row = query.fetch_one(
            """
            SELECT message.id
            FROM messages AS message
            LEFT JOIN conversation_commit AS publication
              ON publication.commit_id = message.conversation_commit_id
             AND publication.chat_id = message.chat_id
             AND publication.owner_user_id = message.user_id
            WHERE message.chat_id = %s AND message.user_id = %s
              AND (
                message.conversation_commit_id IS NULL
                OR (
                    publication.state = 'committed'
                    AND publication.committed_render_revision =
                        message.committed_render_revision
                )
              )
            ORDER BY message.timestamp DESC, message.id DESC
            LIMIT 1
            """,
            (conversation_id, owner_id),
        )
        return None if row is None else int(_row_value(row, "id"))


class SessionRepository:
    """Durable encrypted web sessions, always fenced by owner identity."""

    _SELECT = """
        SELECT sid, user_id, access_token_enc, refresh_token_enc,
               interactive_anchor, hard_expires_at, last_refresh_at,
               resumed, created_at
        FROM web_session
    """

    def put(self, transaction: Transaction, record: SessionRecord) -> SessionRecord:
        session_id = _required_id(record.session_id, "session_id")
        owner_id = _required_id(record.owner_id, "owner_id")
        access = _bounded_text(
            record.access_token_ciphertext,
            "access_token_ciphertext",
            maximum=131072,
        )
        refresh = _bounded_text(
            record.refresh_token_ciphertext,
            "refresh_token_ciphertext",
            maximum=131072,
        )
        interactive_anchor = _non_negative_int(record.interactive_anchor, "interactive_anchor")
        hard_expires_at = _non_negative_int(record.hard_expires_at, "hard_expires_at")
        last_refresh_at = _non_negative_int(record.last_refresh_at, "last_refresh_at")
        created_at = _non_negative_int(record.created_at, "created_at")
        result = transaction.execute(
            """
            INSERT INTO web_session (
                sid, user_id, access_token_enc, refresh_token_enc,
                interactive_anchor, hard_expires_at, last_refresh_at,
                resumed, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sid) DO UPDATE SET
                access_token_enc = EXCLUDED.access_token_enc,
                refresh_token_enc = EXCLUDED.refresh_token_enc,
                interactive_anchor = EXCLUDED.interactive_anchor,
                hard_expires_at = EXCLUDED.hard_expires_at,
                last_refresh_at = EXCLUDED.last_refresh_at,
                resumed = EXCLUDED.resumed
            WHERE web_session.user_id = EXCLUDED.user_id
              AND web_session.created_at = EXCLUDED.created_at
            RETURNING sid, user_id, access_token_enc, refresh_token_enc,
                      interactive_anchor, hard_expires_at, last_refresh_at,
                      resumed, created_at
            """,
            (
                session_id,
                owner_id,
                access,
                refresh,
                interactive_anchor,
                hard_expires_at,
                last_refresh_at,
                bool(record.resumed),
                created_at,
            ),
        )
        row = _optional_returned(result, "session.put")
        if row is None:
            raise RepositoryConflictError(
                "session identity is owned by another user or creation generation",
                metadata={"operation": "session.put"},
            )
        return _session(row)

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        session_id: str,
    ) -> SessionRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        session_id = _required_id(session_id, "session_id")
        row = query.fetch_one(
            self._SELECT + " WHERE sid = %s AND user_id = %s",
            (session_id, owner_id),
        )
        return None if row is None else _session(row)

    def delete(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        session_id: str,
    ) -> bool:
        owner_id = _required_id(owner_id, "owner_id")
        session_id = _required_id(session_id, "session_id")
        result = transaction.execute(
            "DELETE FROM web_session WHERE sid = %s AND user_id = %s",
            (session_id, owner_id),
        )
        return result.rowcount == 1


class HistoryRepository:
    """Convenience grouping without connection or transaction ownership."""

    def __init__(self) -> None:
        self.conversations = ConversationRepository()
        self.messages = MessageRepository()
        self.sessions = SessionRepository()


__all__ = (
    "ConversationRecord",
    "ConversationRepository",
    "HistoryRepository",
    "MessageRecord",
    "MessageRepository",
    "SessionRecord",
    "SessionRepository",
)
