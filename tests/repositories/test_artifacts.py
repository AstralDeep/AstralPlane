"""Focused contract tests for neutral artifact repositories."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryNotFoundError,
    RepositoryValidationError,
)
from astralplane.repositories.artifacts import (
    ArtifactRepository,
    ArtifactVersionRepository,
    AttachmentRecord,
    AttachmentRepository,
    BlobMetadataRepository,
    MaterializationRepository,
    MessageAttachmentRepository,
)

NOW = datetime(2026, 8, 13, 23, 0, tzinfo=UTC)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64


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


def attachment_record(**changes: Any) -> AttachmentRecord:
    values: dict[str, Any] = {
        "attachment_id": "attachment-1",
        "owner_id": "owner-1",
        "filename": "report.txt",
        "content_type": "text/plain",
        "category": "text",
        "extension": "txt",
        "size_bytes": 12,
        "sha256": DIGEST_A,
        "storage_locator": "owner-1/attachment-1.txt",
        "created_at": 10,
        "deleted_at": None,
    }
    values.update(changes)
    return AttachmentRecord(**values)


def attachment_row(**changes: Any) -> dict[str, Any]:
    record = attachment_record()
    row = {
        "attachment_id": record.attachment_id,
        "user_id": record.owner_id,
        "filename": record.filename,
        "content_type": record.content_type,
        "category": record.category,
        "extension": record.extension,
        "size_bytes": record.size_bytes,
        "sha256": record.sha256,
        "storage_path": record.storage_locator,
        "created_at": record.created_at,
        "deleted_at": record.deleted_at,
    }
    row.update(changes)
    return row


def blob_row(**changes: Any) -> dict[str, Any]:
    row = {
        "attachment_id": "attachment-1",
        "user_id": "owner-1",
        "storage_path": "owner-1/attachment-1.txt",
        "sha256": DIGEST_A,
        "size_bytes": 12,
        "created_at": 10,
        "deleted_at": None,
    }
    row.update(changes)
    return row


def link_row(**changes: Any) -> dict[str, Any]:
    row = {
        "id": "link-1",
        "chat_id": "chat-1",
        "message_id": "7",
        "attachment_id": "attachment-1",
        "user_id": "owner-1",
        "created_at": 20,
    }
    row.update(changes)
    return row


def version_row(**changes: Any) -> dict[str, Any]:
    row = {
        "id": 1,
        "chat_id": "chat-1",
        "user_id": "owner-1",
        "component_id": "component-1",
        "version_no": 1,
        "component": {"type": "Card", "children": ["a"]},
        "reason": "refine",
        "created_at": NOW,
    }
    row.update(changes)
    return row


def test_materialization_registers_and_replays_exact_metadata() -> None:
    repository = MaterializationRepository()
    transaction = FakeTransaction()
    transaction.execute_results.append(returned(attachment_row()))

    created = repository.register(transaction, attachment_record())

    assert created == attachment_record()
    assert transaction.calls[0][2][0:2] == ("attachment-1", "owner-1")
    assert "%s" in transaction.calls[0][1]
    assert "?" not in transaction.calls[0][1]

    replay = FakeTransaction()
    replay.execute_results.append(Result(rowcount=0))
    replay.fetch_one_results.append(attachment_row())
    assert repository.register(replay, attachment_record()) == created


def test_materialization_conflicts_are_not_reported_as_success() -> None:
    repository = MaterializationRepository()
    foreign = FakeTransaction()
    foreign.execute_results.append(Result(rowcount=0))
    foreign.fetch_one_results.append(None)
    with pytest.raises(RepositoryConflictError, match="another namespace"):
        repository.register(foreign, attachment_record())

    changed = FakeTransaction()
    changed.execute_results.append(Result(rowcount=0))
    changed.fetch_one_results.append(attachment_row(size_bytes=99))
    with pytest.raises(RepositoryConflictError, match="different semantics"):
        repository.register(changed, attachment_record())


@pytest.mark.parametrize(
    "record",
    [
        attachment_record(attachment_id=""),
        attachment_record(filename=""),
        attachment_record(category="unknown"),
        attachment_record(extension=3),
        attachment_record(size_bytes=-1),
        attachment_record(sha256="ABC"),
        attachment_record(storage_locator=""),
        attachment_record(deleted_at=20),
    ],
)
def test_materialization_validates_bounded_metadata(record: AttachmentRecord) -> None:
    with pytest.raises(RepositoryValidationError):
        MaterializationRepository().register(FakeTransaction(), record)


def test_attachment_reads_are_owner_scoped_and_paginated() -> None:
    repository = AttachmentRepository()
    query = FakeTransaction()
    query.fetch_one_results.extend((attachment_row(), attachment_row(deleted_at=25), None))
    assert (
        repository.get(query, owner_id="owner-1", attachment_id="attachment-1")
        == attachment_record()
    )
    assert (
        repository.get(
            query,
            owner_id="owner-1",
            attachment_id="attachment-1",
            include_deleted=True,
        ).deleted_at
        == 25
    )  # type: ignore[union-attr]
    assert repository.get(query, owner_id="owner-1", attachment_id="missing") is None

    query.fetch_all_results.extend(
        (
            (attachment_row(),),
            (attachment_row(attachment_id="attachment-0", category="image"),),
        )
    )
    assert len(repository.list_live(query, owner_id="owner-1", limit=1)) == 1
    records = repository.list_live(
        query,
        owner_id="owner-1",
        category="image",
        limit=2,
        before_created_at=11,
        before_attachment_id="attachment-2",
    )
    assert records[0].attachment_id == "attachment-0"
    sql = query.calls[-1][1]
    assert "user_id = %s" in sql and "category = %s" in sql
    assert "(created_at, attachment_id) < (%s, %s)" in sql


@pytest.mark.parametrize(
    "kwargs",
    [
        {"before_created_at": 10},
        {"before_attachment_id": "attachment-1"},
        {"category": "invalid"},
        {"limit": 0},
        {"limit": True},
    ],
)
def test_attachment_list_rejects_invalid_cursor_or_bounds(kwargs: dict[str, Any]) -> None:
    with pytest.raises(RepositoryValidationError):
        AttachmentRepository().list_live(FakeTransaction(), owner_id="owner-1", **kwargs)


def test_attachment_soft_delete_and_bulk_count_are_visible() -> None:
    repository = AttachmentRepository()
    transaction = FakeTransaction()
    transaction.execute_results.extend(
        (
            returned(attachment_row(deleted_at=30)),
            Result(rowcount=0),
            Result(rowcount=3),
            Result(rowcount=-1),
        )
    )
    assert (
        repository.soft_delete(
            transaction,
            owner_id="owner-1",
            attachment_id="attachment-1",
            deleted_at=30,
        ).deleted_at
        == 30
    )  # type: ignore[union-attr]
    assert (
        repository.soft_delete(
            transaction,
            owner_id="owner-1",
            attachment_id="attachment-1",
            deleted_at=31,
        )
        is None
    )
    assert repository.soft_delete_all(transaction, owner_id="owner-1", deleted_at=32) == 3
    assert repository.soft_delete_all(transaction, owner_id="owner-1", deleted_at=33) == 0


def test_blob_metadata_get_and_cas_relocation() -> None:
    repository = BlobMetadataRepository()
    query = FakeTransaction()
    query.fetch_one_results.extend((blob_row(), blob_row(deleted_at=40), None))
    assert (
        repository.get(query, owner_id="owner-1", object_id="attachment-1").object_kind
        == "attachment"
    )  # type: ignore[union-attr]
    assert (
        repository.get(
            query,
            owner_id="owner-1",
            object_id="attachment-1",
            include_deleted=True,
        ).deleted_at
        == 40
    )  # type: ignore[union-attr]
    assert repository.get(query, owner_id="owner-1", object_id="missing") is None

    transaction = FakeTransaction()
    transaction.execute_results.append(
        returned(blob_row(storage_path="new/location", sha256=DIGEST_B, size_bytes=13))
    )
    relocated = repository.relocate(
        transaction,
        owner_id="owner-1",
        object_id="attachment-1",
        expected_storage_locator="owner-1/attachment-1.txt",
        expected_sha256=DIGEST_A,
        storage_locator="new/location",
        sha256=DIGEST_B,
        size_bytes=13,
    )
    assert relocated.storage_locator == "new/location"
    assert "user_id = %s" in transaction.calls[0][1]


def test_blob_relocation_distinguishes_missing_and_conflict() -> None:
    repository = BlobMetadataRepository()
    missing = FakeTransaction()
    missing.execute_results.append(Result(rowcount=0))
    missing.fetch_one_results.append(None)
    with pytest.raises(RepositoryNotFoundError):
        repository.relocate(
            missing,
            owner_id="owner-1",
            object_id="attachment-1",
            expected_storage_locator="old",
            expected_sha256=DIGEST_A,
            storage_locator="new",
            sha256=DIGEST_B,
            size_bytes=1,
        )

    conflict = FakeTransaction()
    conflict.execute_results.append(Result(rowcount=0))
    conflict.fetch_one_results.append(blob_row())
    with pytest.raises(RepositoryConflictError):
        repository.relocate(
            conflict,
            owner_id="owner-1",
            object_id="attachment-1",
            expected_storage_locator="old",
            expected_sha256=DIGEST_A,
            storage_locator="new",
            sha256=DIGEST_B,
            size_bytes=1,
        )

    with pytest.raises(RepositoryValidationError, match="lowercase SHA"):
        repository.relocate(
            FakeTransaction(),
            owner_id="owner-1",
            object_id="attachment-1",
            expected_storage_locator="old",
            expected_sha256="bad",
            storage_locator="new",
            sha256=DIGEST_B,
            size_bytes=1,
        )


def test_message_attachment_link_is_owner_validated_and_replay_safe() -> None:
    repository = MessageAttachmentRepository()
    transaction = FakeTransaction()
    transaction.execute_results.append(returned(link_row()))
    record = repository.link(
        transaction,
        link_id="link-1",
        owner_id="owner-1",
        conversation_id="chat-1",
        attachment_id="attachment-1",
        message_id=7,
        created_at=20,
    )
    assert record.message_id == "7"
    assert "attachment.user_id = chat.user_id" in transaction.calls[0][1]

    replay = FakeTransaction()
    replay.execute_results.append(Result(rowcount=0))
    replay.fetch_one_results.append(link_row())
    assert (
        repository.link(
            replay,
            link_id="link-1",
            owner_id="owner-1",
            conversation_id="chat-1",
            attachment_id="attachment-1",
            message_id=7,
            created_at=20,
        )
        == record
    )


def test_message_attachment_link_conflict_and_missing_are_visible() -> None:
    repository = MessageAttachmentRepository()
    changed = FakeTransaction()
    changed.execute_results.append(Result(rowcount=0))
    changed.fetch_one_results.append(link_row(attachment_id="attachment-2"))
    with pytest.raises(RepositoryConflictError):
        repository.link(
            changed,
            link_id="link-1",
            owner_id="owner-1",
            conversation_id="chat-1",
            attachment_id="attachment-1",
            message_id=None,
            created_at=20,
        )

    missing = FakeTransaction()
    missing.execute_results.append(Result(rowcount=0))
    missing.fetch_one_results.append(None)
    with pytest.raises(RepositoryNotFoundError):
        repository.link(
            missing,
            link_id="link-1",
            owner_id="owner-1",
            conversation_id="chat-1",
            attachment_id="attachment-1",
            message_id=None,
            created_at=20,
        )


def test_message_attachment_queries_return_detached_records() -> None:
    repository = MessageAttachmentRepository()
    query = FakeTransaction()
    query.fetch_one_results.extend((link_row(), None))
    query.fetch_all_results.extend(((link_row(),), (link_row(),)))
    assert repository.get(query, owner_id="owner-1", link_id="link-1").link_id == "link-1"  # type: ignore[union-attr]
    assert repository.get(query, owner_id="owner-1", link_id="missing") is None
    assert (
        len(repository.list_for_conversation(query, owner_id="owner-1", conversation_id="chat-1"))
        == 1
    )
    assert len(repository.list_for_message(query, owner_id="owner-1", message_id=7)) == 1
    assert query.calls[-1][2] == ("7", "owner-1")


def test_artifact_version_archive_serializes_and_prunes() -> None:
    repository = ArtifactVersionRepository()
    transaction = FakeTransaction()
    transaction.fetch_one_results.append({"id": "chat-1"})
    transaction.execute_results.extend(
        (returned(version_row(version_no=6, id=6)), Result(rowcount=1))
    )

    record = repository.archive(
        transaction,
        owner_id="owner-1",
        conversation_id="chat-1",
        component_id="component-1",
        component={"type": "Card", "children": ["a"]},
        reason="restore",
        retain=5,
    )

    assert record.version_number == 6
    assert record.component["children"] == ("a",)
    assert "FOR UPDATE" in transaction.calls[0][1]
    assert "user_id = %s" in transaction.calls[-1][1]

    no_prune = FakeTransaction()
    no_prune.fetch_one_results.append({"id": "chat-1"})
    no_prune.execute_results.append(returned(version_row()))
    repository.archive(
        no_prune,
        owner_id="owner-1",
        conversation_id="chat-1",
        component_id="component-1",
        component={"type": "Card"},
    )
    assert len([call for call in no_prune.calls if call[0] == "execute"]) == 1


def test_artifact_version_archive_validation_and_missing_owner() -> None:
    repository = ArtifactVersionRepository()
    missing = FakeTransaction()
    missing.fetch_one_results.append(None)
    with pytest.raises(RepositoryNotFoundError):
        repository.archive(
            missing,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            component={"type": "Card"},
        )
    with pytest.raises(RepositoryValidationError):
        repository.archive(
            FakeTransaction(),
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            component={"type": "Card"},
            reason="unknown",
        )
    with pytest.raises(RepositoryValidationError):
        repository.archive(
            FakeTransaction(),
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            component={"bad": float("nan")},
        )


def test_artifact_version_queries_delete_and_corruption_visibility() -> None:
    repository = ArtifactVersionRepository()
    query = FakeTransaction()
    query.fetch_one_results.extend((version_row(), None))
    query.fetch_all_results.append((version_row(), version_row(id=2, version_no=2)))
    assert (
        repository.get(
            query,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            version_number=1,
        ).version_number
        == 1
    )  # type: ignore[union-attr]
    assert (
        repository.get(
            query,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            version_number=2,
        )
        is None
    )
    assert (
        len(
            repository.list_for_component(
                query,
                owner_id="owner-1",
                conversation_id="chat-1",
                component_id="component-1",
                limit=2,
            )
        )
        == 2
    )

    query.execute_results.extend((Result(rowcount=4), Result(rowcount=-1)))
    assert (
        repository.delete_for_component(
            query,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
        )
        == 4
    )
    assert (
        repository.delete_for_component(
            query,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
        )
        == 0
    )

    corrupt = FakeTransaction()
    corrupt.fetch_one_results.append(version_row(created_at="not-time"))
    with pytest.raises(RepositoryDataError, match="timestamp"):
        repository.get(
            corrupt,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            version_number=1,
        )
    corrupt.fetch_one_results.append(version_row(component="[]"))
    with pytest.raises(RepositoryDataError, match="JSON object"):
        repository.get(
            corrupt,
            owner_id="owner-1",
            conversation_id="chat-1",
            component_id="component-1",
            version_number=1,
        )


def test_artifact_facade_exposes_neutral_stores() -> None:
    facade = ArtifactRepository()
    assert isinstance(facade.materializations, MaterializationRepository)
    assert isinstance(facade.attachments, AttachmentRepository)
    assert isinstance(facade.blobs, BlobMetadataRepository)
    assert isinstance(facade.message_attachments, MessageAttachmentRepository)
    assert isinstance(facade.versions, ArtifactVersionRepository)
