"""Attachment, materialization, blob-metadata, and artifact-version stores."""

from __future__ import annotations

import re
from collections.abc import Mapping
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

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ATTACHMENT_CATEGORIES = frozenset(
    {
        "archive",
        "data",
        "document",
        "image",
        "medical",
        "presentation",
        "spreadsheet",
        "text",
    }
)
_ARTIFACT_REASONS = frozenset({"refine", "restore"})


@dataclass(frozen=True, slots=True)
class AttachmentRecord:
    attachment_id: str
    owner_id: str
    filename: str
    content_type: str
    category: str
    extension: str
    size_bytes: int
    sha256: str
    storage_locator: str
    created_at: int
    deleted_at: int | None = None


@dataclass(frozen=True, slots=True)
class BlobMetadataRecord:
    object_id: str
    owner_id: str
    object_kind: str
    storage_locator: str
    sha256: str
    size_bytes: int
    created_at: int
    deleted_at: int | None


@dataclass(frozen=True, slots=True)
class MessageAttachmentRecord:
    link_id: str
    conversation_id: str
    message_id: str | None
    attachment_id: str
    owner_id: str
    created_at: int


@dataclass(frozen=True, slots=True)
class ArtifactVersionRecord:
    version_id: int
    conversation_id: str
    owner_id: str
    component_id: str
    version_number: int
    component: Any
    reason: str
    created_at: datetime


def _optional_returned(result: object, operation: str) -> Any:
    if not getattr(result, "returned_records", ()):
        return None
    return _single_returned(result, operation)


def _digest(value: object, field: str = "sha256") -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RepositoryValidationError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _attachment(row: Mapping[str, Any]) -> AttachmentRecord:
    return AttachmentRecord(
        attachment_id=str(_row_value(row, "attachment_id")),
        owner_id=str(_row_value(row, "user_id")),
        filename=str(_row_value(row, "filename")),
        content_type=str(_row_value(row, "content_type")),
        category=str(_row_value(row, "category")),
        extension=str(_row_value(row, "extension")),
        size_bytes=int(_row_value(row, "size_bytes")),
        sha256=str(_row_value(row, "sha256")),
        storage_locator=str(_row_value(row, "storage_path")),
        created_at=int(_row_value(row, "created_at")),
        deleted_at=(None if row.get("deleted_at") is None else int(row["deleted_at"])),
    )


def _blob(row: Mapping[str, Any]) -> BlobMetadataRecord:
    return BlobMetadataRecord(
        object_id=str(_row_value(row, "attachment_id")),
        owner_id=str(_row_value(row, "user_id")),
        object_kind="attachment",
        storage_locator=str(_row_value(row, "storage_path")),
        sha256=str(_row_value(row, "sha256")),
        size_bytes=int(_row_value(row, "size_bytes")),
        created_at=int(_row_value(row, "created_at")),
        deleted_at=(None if row.get("deleted_at") is None else int(row["deleted_at"])),
    )


def _message_attachment(row: Mapping[str, Any]) -> MessageAttachmentRecord:
    return MessageAttachmentRecord(
        link_id=str(_row_value(row, "id")),
        conversation_id=str(_row_value(row, "chat_id")),
        message_id=(None if row.get("message_id") is None else str(row["message_id"])),
        attachment_id=str(_row_value(row, "attachment_id")),
        owner_id=str(_row_value(row, "user_id")),
        created_at=int(_row_value(row, "created_at")),
    )


def _artifact_version(row: Mapping[str, Any]) -> ArtifactVersionRecord:
    created_at = _row_value(row, "created_at")
    if not isinstance(created_at, datetime) or created_at.tzinfo is None:
        raise RepositoryDataError(
            "artifact version timestamp is not timezone-aware",
            metadata={"field": "created_at"},
        )
    component = _structured_json(_row_value(row, "component"), "component")
    if not isinstance(component, Mapping):
        raise RepositoryDataError("artifact version component must be a JSON object")
    return ArtifactVersionRecord(
        version_id=int(_row_value(row, "id")),
        conversation_id=str(_row_value(row, "chat_id")),
        owner_id=str(_row_value(row, "user_id")),
        component_id=str(_row_value(row, "component_id")),
        version_number=int(_row_value(row, "version_no")),
        component=component,
        reason=str(_row_value(row, "reason")),
        created_at=created_at,
    )


class MaterializationRepository:
    """Record a completed external materialization without owning filesystem policy."""

    _FIELDS = """
        attachment_id, user_id, filename, content_type, category, extension,
        size_bytes, sha256, storage_path, created_at, deleted_at
    """

    def register(
        self,
        transaction: Transaction,
        record: AttachmentRecord,
    ) -> AttachmentRecord:
        attachment_id = _required_id(record.attachment_id, "attachment_id")
        owner_id = _required_id(record.owner_id, "owner_id")
        filename = _bounded_text(record.filename, "filename", maximum=1024)
        content_type = _bounded_text(record.content_type, "content_type", maximum=255)
        if record.category not in _ATTACHMENT_CATEGORIES:
            raise RepositoryValidationError("attachment category is unsupported")
        extension = _bounded_text(record.extension, "extension", maximum=64, allow_empty=True)
        size_bytes = _non_negative_int(record.size_bytes, "size_bytes")
        digest = _digest(record.sha256)
        locator = _bounded_text(record.storage_locator, "storage_locator", maximum=4096)
        created_at = _non_negative_int(record.created_at, "created_at")
        if record.deleted_at is not None:
            raise RepositoryValidationError("new materializations cannot start deleted")
        result = transaction.execute(
            f"""
            INSERT INTO user_attachments (
                attachment_id, user_id, filename, content_type, category,
                extension, size_bytes, sha256, storage_path, created_at, deleted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
            ON CONFLICT (attachment_id) DO NOTHING
            RETURNING {self._FIELDS}
            """,
            (
                attachment_id,
                owner_id,
                filename,
                content_type,
                record.category,
                extension,
                size_bytes,
                digest,
                locator,
                created_at,
            ),
        )
        row = _optional_returned(result, "materialization.register")
        if row is not None:
            return _attachment(row)
        existing = AttachmentRepository().get(
            transaction,
            owner_id=owner_id,
            attachment_id=attachment_id,
            include_deleted=True,
        )
        if existing is None:
            raise RepositoryConflictError(
                "materialization identity is owned by another namespace",
                metadata={"operation": "materialization.register"},
            )
        expected = (
            filename,
            content_type,
            record.category,
            extension,
            size_bytes,
            digest,
            locator,
            created_at,
            None,
        )
        observed = (
            existing.filename,
            existing.content_type,
            existing.category,
            existing.extension,
            existing.size_bytes,
            existing.sha256,
            existing.storage_locator,
            existing.created_at,
            existing.deleted_at,
        )
        if observed != expected:
            raise RepositoryConflictError(
                "materialization idempotency identity was reused with different semantics",
                metadata={"operation": "materialization.register"},
            )
        return existing


class AttachmentRepository:
    _SELECT = """
        SELECT attachment_id, user_id, filename, content_type, category,
               extension, size_bytes, sha256, storage_path, created_at, deleted_at
        FROM user_attachments
    """

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        attachment_id: str,
        include_deleted: bool = False,
    ) -> AttachmentRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        attachment_id = _required_id(attachment_id, "attachment_id")
        deleted_clause = "" if include_deleted else " AND deleted_at IS NULL"
        row = query.fetch_one(
            self._SELECT + " WHERE attachment_id = %s AND user_id = %s" + deleted_clause,
            (attachment_id, owner_id),
        )
        return None if row is None else _attachment(row)

    def list_live(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        category: str | None = None,
        limit: int = 50,
        before_created_at: int | None = None,
        before_attachment_id: str | None = None,
    ) -> tuple[AttachmentRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        limit = _bounded_limit(limit)
        if (before_created_at is None) != (before_attachment_id is None):
            raise RepositoryValidationError(
                "attachment cursor time and identity must be supplied together"
            )
        parameters: list[object] = [owner_id]
        clauses = ["user_id = %s", "deleted_at IS NULL"]
        if category is not None:
            if category not in _ATTACHMENT_CATEGORIES:
                raise RepositoryValidationError("attachment category is unsupported")
            clauses.append("category = %s")
            parameters.append(category)
        if before_created_at is not None and before_attachment_id is not None:
            before_created_at = _non_negative_int(before_created_at, "before_created_at")
            before_attachment_id = _required_id(before_attachment_id, "before_attachment_id")
            clauses.append("(created_at, attachment_id) < (%s, %s)")
            parameters.extend((before_created_at, before_attachment_id))
        parameters.append(limit)
        rows = query.fetch_all(
            self._SELECT
            + " WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC, attachment_id DESC LIMIT %s",
            tuple(parameters),
        )
        return tuple(_attachment(row) for row in rows)

    def soft_delete(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        attachment_id: str,
        deleted_at: int,
    ) -> AttachmentRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        attachment_id = _required_id(attachment_id, "attachment_id")
        deleted_at = _non_negative_int(deleted_at, "deleted_at")
        result = transaction.execute(
            f"""
            UPDATE user_attachments
            SET deleted_at = %s
            WHERE attachment_id = %s AND user_id = %s AND deleted_at IS NULL
            RETURNING {MaterializationRepository._FIELDS}
            """,
            (deleted_at, attachment_id, owner_id),
        )
        row = _optional_returned(result, "attachment.soft_delete")
        return None if row is None else _attachment(row)

    def soft_delete_all(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        deleted_at: int,
    ) -> int:
        owner_id = _required_id(owner_id, "owner_id")
        deleted_at = _non_negative_int(deleted_at, "deleted_at")
        result = transaction.execute(
            """
            UPDATE user_attachments
            SET deleted_at = %s
            WHERE user_id = %s AND deleted_at IS NULL
            """,
            (deleted_at, owner_id),
        )
        return max(0, result.rowcount)


class BlobMetadataRepository:
    """Detached metadata and CAS relocation; physical I/O remains host supplied."""

    _FIELDS = """
        attachment_id, user_id, storage_path, sha256, size_bytes,
        created_at, deleted_at
    """
    _SELECT = f"SELECT {_FIELDS} FROM user_attachments"

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        object_id: str,
        include_deleted: bool = False,
    ) -> BlobMetadataRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        object_id = _required_id(object_id, "object_id")
        deleted_clause = "" if include_deleted else " AND deleted_at IS NULL"
        row = query.fetch_one(
            self._SELECT + " WHERE attachment_id = %s AND user_id = %s" + deleted_clause,
            (object_id, owner_id),
        )
        return None if row is None else _blob(row)

    def relocate(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        object_id: str,
        expected_storage_locator: str,
        expected_sha256: str,
        storage_locator: str,
        sha256: str,
        size_bytes: int,
    ) -> BlobMetadataRecord:
        owner_id = _required_id(owner_id, "owner_id")
        object_id = _required_id(object_id, "object_id")
        expected_storage_locator = _bounded_text(
            expected_storage_locator, "expected_storage_locator", maximum=4096
        )
        expected_sha256 = _digest(expected_sha256, "expected_sha256")
        storage_locator = _bounded_text(storage_locator, "storage_locator", maximum=4096)
        sha256 = _digest(sha256)
        size_bytes = _non_negative_int(size_bytes, "size_bytes")
        result = transaction.execute(
            f"""
            UPDATE user_attachments
            SET storage_path = %s, sha256 = %s, size_bytes = %s
            WHERE attachment_id = %s AND user_id = %s AND deleted_at IS NULL
              AND storage_path = %s AND sha256 = %s
            RETURNING {self._FIELDS}
            """,
            (
                storage_locator,
                sha256,
                size_bytes,
                object_id,
                owner_id,
                expected_storage_locator,
                expected_sha256,
            ),
        )
        row = _optional_returned(result, "blob_metadata.relocate")
        if row is not None:
            return _blob(row)
        existing = self.get(
            transaction,
            owner_id=owner_id,
            object_id=object_id,
            include_deleted=True,
        )
        if existing is None:
            raise RepositoryNotFoundError(
                "blob metadata was not found",
                metadata={"operation": "blob_metadata.relocate"},
            )
        raise RepositoryConflictError(
            "blob metadata changed or was deleted before relocation",
            metadata={"operation": "blob_metadata.relocate"},
        )


class MessageAttachmentRepository:
    _FIELDS = "id, chat_id, message_id, attachment_id, user_id, created_at"

    def link(
        self,
        transaction: Transaction,
        *,
        link_id: str,
        owner_id: str,
        conversation_id: str,
        attachment_id: str,
        created_at: int,
        message_id: int | str | None = None,
    ) -> MessageAttachmentRecord:
        link_id = _required_id(link_id, "link_id")
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        attachment_id = _required_id(attachment_id, "attachment_id")
        created_at = _non_negative_int(created_at, "created_at")
        stored_message_id = None if message_id is None else str(message_id)
        result = transaction.execute(
            f"""
            INSERT INTO message_attachment (
                id, chat_id, message_id, attachment_id, user_id, created_at
            )
            SELECT %s, %s, %s, %s, %s, %s
            FROM chats AS chat
            JOIN user_attachments AS attachment
              ON attachment.attachment_id = %s
             AND attachment.user_id = chat.user_id
             AND attachment.deleted_at IS NULL
            WHERE chat.id = %s AND chat.user_id = %s
              AND (
                %s IS NULL OR EXISTS (
                    SELECT 1 FROM messages
                    WHERE id::text = %s AND chat_id = chat.id AND user_id = chat.user_id
                )
              )
            ON CONFLICT (id) DO NOTHING
            RETURNING {self._FIELDS}
            """,
            (
                link_id,
                conversation_id,
                stored_message_id,
                attachment_id,
                owner_id,
                created_at,
                attachment_id,
                conversation_id,
                owner_id,
                stored_message_id,
                stored_message_id,
            ),
        )
        row = _optional_returned(result, "message_attachment.link")
        if row is not None:
            return _message_attachment(row)
        existing = self.get(transaction, owner_id=owner_id, link_id=link_id)
        if existing is None:
            raise RepositoryNotFoundError(
                "owner-scoped attachment link prerequisites are unavailable",
                metadata={"operation": "message_attachment.link"},
            )
        expected = (conversation_id, stored_message_id, attachment_id, created_at)
        observed = (
            existing.conversation_id,
            existing.message_id,
            existing.attachment_id,
            existing.created_at,
        )
        if observed != expected:
            raise RepositoryConflictError(
                "attachment link identity was reused with different semantics",
                metadata={"operation": "message_attachment.link"},
            )
        return existing

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        link_id: str,
    ) -> MessageAttachmentRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        link_id = _required_id(link_id, "link_id")
        row = query.fetch_one(
            f"SELECT {self._FIELDS} FROM message_attachment WHERE id = %s AND user_id = %s",
            (link_id, owner_id),
        )
        return None if row is None else _message_attachment(row)

    def list_for_conversation(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
    ) -> tuple[MessageAttachmentRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        rows = query.fetch_all(
            f"""
            SELECT {self._FIELDS}
            FROM message_attachment
            WHERE chat_id = %s AND user_id = %s
            ORDER BY created_at, id
            """,
            (conversation_id, owner_id),
        )
        return tuple(_message_attachment(row) for row in rows)

    def list_for_message(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        message_id: int | str,
    ) -> tuple[MessageAttachmentRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        stored_message_id = _required_id(str(message_id), "message_id")
        rows = query.fetch_all(
            f"""
            SELECT {self._FIELDS}
            FROM message_attachment
            WHERE message_id = %s AND user_id = %s
            ORDER BY created_at, id
            """,
            (stored_message_id, owner_id),
        )
        return tuple(_message_attachment(row) for row in rows)


class ArtifactVersionRepository:
    """Owner-scoped bounded component history serialized by a chat row lock."""

    _FIELDS = """
        id, chat_id, user_id, component_id, version_no,
        component, reason, created_at
    """

    def archive(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
        component: Mapping[str, object],
        reason: str = "refine",
        retain: int = 5,
    ) -> ArtifactVersionRecord:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        if reason not in _ARTIFACT_REASONS:
            raise RepositoryValidationError("artifact version reason is unsupported")
        retain = _bounded_limit(retain, maximum=100)
        payload = _canonical_json(component, "component")
        authority = transaction.fetch_one(
            "SELECT id FROM chats WHERE id = %s AND user_id = %s FOR UPDATE",
            (conversation_id, owner_id),
        )
        if authority is None:
            raise RepositoryNotFoundError(
                "artifact conversation was not found",
                metadata={"operation": "artifact_version.archive"},
            )
        result = transaction.execute(
            f"""
            INSERT INTO component_version (
                chat_id, user_id, component_id, version_no, component, reason
            )
            SELECT %s, %s, %s, COALESCE(MAX(version_no), 0) + 1, %s::jsonb, %s
            FROM component_version
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
            RETURNING {self._FIELDS}
            """,
            (
                conversation_id,
                owner_id,
                component_id,
                payload,
                reason,
                conversation_id,
                owner_id,
                component_id,
            ),
        )
        record = _artifact_version(_single_returned(result, "artifact_version.archive"))
        if record.version_number > retain:
            transaction.execute(
                """
                DELETE FROM component_version
                WHERE chat_id = %s AND user_id = %s AND component_id = %s
                  AND version_no <= %s
                """,
                (
                    conversation_id,
                    owner_id,
                    component_id,
                    record.version_number - retain,
                ),
            )
        return record

    def get(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
        version_number: int,
    ) -> ArtifactVersionRecord | None:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        version_number = _positive_int(version_number, "version_number")
        row = query.fetch_one(
            f"""
            SELECT {self._FIELDS}
            FROM component_version
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
              AND version_no = %s
            """,
            (conversation_id, owner_id, component_id, version_number),
        )
        return None if row is None else _artifact_version(row)

    def list_for_component(
        self,
        query: QueryExecutor,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
        limit: int = 5,
    ) -> tuple[ArtifactVersionRecord, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        limit = _bounded_limit(limit, maximum=100)
        rows = query.fetch_all(
            f"""
            SELECT {self._FIELDS}
            FROM component_version
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
            ORDER BY version_no DESC
            LIMIT %s
            """,
            (conversation_id, owner_id, component_id, limit),
        )
        return tuple(_artifact_version(row) for row in rows)

    def delete_for_component(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        conversation_id: str,
        component_id: str,
    ) -> int:
        owner_id = _required_id(owner_id, "owner_id")
        conversation_id = _required_id(conversation_id, "conversation_id")
        component_id = _required_id(component_id, "component_id")
        result = transaction.execute(
            """
            DELETE FROM component_version
            WHERE chat_id = %s AND user_id = %s AND component_id = %s
            """,
            (conversation_id, owner_id, component_id),
        )
        return max(0, result.rowcount)


class ArtifactRepository:
    """Grouping of artifact stores without connection or transaction ownership."""

    def __init__(self) -> None:
        self.materializations = MaterializationRepository()
        self.attachments = AttachmentRepository()
        self.blobs = BlobMetadataRepository()
        self.message_attachments = MessageAttachmentRepository()
        self.versions = ArtifactVersionRepository()


__all__ = (
    "ArtifactRepository",
    "ArtifactVersionRecord",
    "ArtifactVersionRepository",
    "AttachmentRecord",
    "AttachmentRepository",
    "BlobMetadataRecord",
    "BlobMetadataRepository",
    "MaterializationRepository",
    "MessageAttachmentRecord",
    "MessageAttachmentRepository",
)
