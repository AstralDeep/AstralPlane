"""Partial-failure, owner-isolation, and filesystem safety tests for purge."""

from __future__ import annotations

import copy
import hashlib
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePath
from typing import Any

import pytest

from astralplane.errors import PlaneError, SQLContractError
from astralplane.purge import (
    DurablePurgeExecutor,
    ExplicitRootBlobStore,
    PostgresPurgeStore,
    PurgeAttemptState,
    PurgeStatus,
    PurgeTombstone,
    storage_locator_sha256,
)

NOW = datetime(2026, 8, 13, 22, tzinfo=UTC)


@dataclass(frozen=True)
class FakeResult:
    rowcount: int
    status_message: str | None = None
    returned_records: tuple[Mapping[str, Any], ...] = ()


class MemoryPurgeTransaction:
    def __init__(self, rows: dict[str, dict[str, Any]]) -> None:
        self.rows = rows
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.override_result: FakeResult | None = None

    def execute(self, statement: str, parameters: tuple[object, ...] = ()) -> FakeResult:
        self.statements.append((statement, parameters))
        if self.override_result is not None:
            result, self.override_result = self.override_result, None
            return result
        if statement.startswith("INSERT INTO astralplane_purge_tombstone"):
            (
                tombstone_id,
                owner_id,
                object_kind,
                object_id,
                storage_key,
                locator_digest,
                requested_at,
                available_at,
            ) = parameters
            if str(tombstone_id) in self.rows or any(
                row["owner_id"] == owner_id
                and row["object_kind"] == object_kind
                and row["object_id"] == object_id
                for row in self.rows.values()
            ):
                return FakeResult(0)
            self.rows[str(tombstone_id)] = {
                "tombstone_id": tombstone_id,
                "owner_id": owner_id,
                "object_kind": object_kind,
                "object_id": object_id,
                "storage_key": storage_key,
                "storage_locator_sha256": locator_digest,
                "requested_at": requested_at,
                "status": "pending",
                "attempt_count": 0,
                "version": 0,
                "available_at": available_at,
                "verified_absent_at": None,
                "last_error_code": None,
                "updated_at": requested_at,
            }
            return FakeResult(1)
        if "SET status = 'purged'" in statement:
            verified_at, available_at, owner_id, tombstone_id, version = parameters
            row = self._fenced(owner_id, tombstone_id, version)
            if row is None:
                return FakeResult(0)
            row.update(
                status="purged",
                attempt_count=row["attempt_count"] + 1,
                version=row["version"] + 1,
                verified_absent_at=verified_at,
                available_at=available_at,
                last_error_code=None,
            )
            return FakeResult(1)
        if "SET status = 'failed'" in statement:
            available_at, error_code, owner_id, tombstone_id, version = parameters
            row = self._fenced(owner_id, tombstone_id, version)
            if row is None:
                return FakeResult(0)
            row.update(
                status="failed",
                attempt_count=row["attempt_count"] + 1,
                version=row["version"] + 1,
                available_at=available_at,
                verified_absent_at=None,
                last_error_code=error_code,
            )
            return FakeResult(1)
        if "SET status = 'manual_review'" in statement:
            error_code, owner_id, tombstone_id, version = parameters
            row = self._fenced(owner_id, tombstone_id, version)
            if row is None:
                return FakeResult(0)
            row.update(
                status="manual_review",
                version=row["version"] + 1,
                last_error_code=error_code,
            )
            return FakeResult(1)
        raise AssertionError(f"unexpected execute: {statement}")

    def fetch_one(
        self, statement: str, parameters: tuple[object, ...] = ()
    ) -> Mapping[str, Any] | None:
        self.statements.append((statement, parameters))
        if "AND object_kind = %s" in statement:
            owner_id, object_kind, object_id = parameters
            return next(
                (
                    copy.deepcopy(row)
                    for row in self.rows.values()
                    if row["owner_id"] == owner_id
                    and row["object_kind"] == object_kind
                    and row["object_id"] == object_id
                ),
                None,
            )
        owner_id, tombstone_id = parameters
        row = self.rows.get(str(tombstone_id))
        return None if row is None or row["owner_id"] != owner_id else copy.deepcopy(row)

    def fetch_all(
        self, statement: str, parameters: tuple[object, ...] = ()
    ) -> tuple[Mapping[str, Any], ...]:
        self.statements.append((statement, parameters))
        owner_id = parameters[0]
        return tuple(
            copy.deepcopy(row)
            for row in sorted(
                self.rows.values(), key=lambda value: (value["requested_at"], value["tombstone_id"])
            )
            if row["owner_id"] == owner_id and row["status"] != "purged"
        )

    def _fenced(
        self, owner_id: object, tombstone_id: object, version: object
    ) -> dict[str, Any] | None:
        row = self.rows.get(str(tombstone_id))
        if (
            row is None
            or row["owner_id"] != owner_id
            or row["version"] != version
            or row["status"] not in {"pending", "failed"}
        ):
            return None
        return row


class MemoryPurgeDatabase:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.transaction_count = 0
        self.fail_commit_numbers: set[int] = set()

    @contextmanager
    def transaction(self, **_: object) -> Iterator[MemoryPurgeTransaction]:
        self.transaction_count += 1
        number = self.transaction_count
        working = copy.deepcopy(self.rows)
        yield MemoryPurgeTransaction(working)
        if number in self.fail_commit_numbers:
            raise RuntimeError("simulated database commit failure")
        self.rows = working


class MemoryBlobStore:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], bytes] = {}
        self.fail_delete = False
        self.deleted: list[tuple[str, str]] = []

    @property
    def root(self) -> PurePath:
        return PurePath("/configured/runtime/blobs")

    def put(self, *, owner_id: str, key: str, content: bytes) -> str:
        self.values[(owner_id, key)] = content
        return "digest"

    def get(self, *, owner_id: str, key: str) -> bytes:
        return self.values[(owner_id, key)]

    def delete(self, *, owner_id: str, key: str) -> None:
        self.deleted.append((owner_id, key))
        if self.fail_delete:
            raise OSError("host path and credentials must not escape")
        self.values.pop((owner_id, key), None)

    def is_absent(self, *, owner_id: str, key: str) -> bool:
        return (owner_id, key) not in self.values


class StickyBlobStore(MemoryBlobStore):
    def delete(self, *, owner_id: str, key: str) -> None:
        self.deleted.append((owner_id, key))


def tombstone(
    identifier: str = "purge-1",
    *,
    owner_id: str = "owner-1",
    key: str = "attachments/file.bin",
    available_at: datetime | None = None,
) -> PurgeTombstone:
    return PurgeTombstone(
        tombstone_id=identifier,
        owner_id=owner_id,
        object_kind="attachment",
        object_id=f"object-{identifier}",
        storage_key=key,
        storage_locator_sha256=storage_locator_sha256(owner_id=owner_id, key=key),
        requested_at=NOW,
        available_at=available_at,
    )


def schedule(
    database: MemoryPurgeDatabase,
    store: PostgresPurgeStore,
    item: PurgeTombstone | None = None,
) -> None:
    with database.transaction() as transaction:
        store.enqueue(transaction, item or tombstone())


def test_tombstone_enqueue_is_atomic_idempotent_and_owner_scoped() -> None:
    database, store = MemoryPurgeDatabase(), PostgresPurgeStore()
    schedule(database, store)
    schedule(database, store)
    assert list(database.rows) == ["purge-1"]

    with database.transaction() as transaction:
        assert store.load(transaction, owner_id="owner-1", tombstone_id="purge-1") is not None
        assert store.load(transaction, owner_id="owner-2", tombstone_id="purge-1") is None
        assert len(store.list_incomplete(transaction, owner_id="owner-1")) == 1
        assert store.list_incomplete(transaction, owner_id="owner-2") == ()
        statements = tuple(statement for statement, _ in transaction.statements)
        assert all("owner_id = %s" in statement for statement in statements)

    with pytest.raises(RuntimeError), database.transaction() as transaction:
        store.enqueue(transaction, tombstone("rolled-back"))
        raise RuntimeError("logical deletion rolled back")
    assert "rolled-back" not in database.rows


def test_conflicting_tombstone_identity_and_stale_fences_fail_closed() -> None:
    database, store = MemoryPurgeDatabase(), PostgresPurgeStore()
    schedule(database, store)
    conflicting = tombstone(key="attachments/other.bin")
    with database.transaction() as transaction:
        with pytest.raises(PlaneError) as raised:
            store.enqueue(transaction, conflicting)
        assert raised.value.code == "purge_idempotency_conflict"

    same_object = replace(tombstone("purge-2"), object_id="object-purge-1")
    with database.transaction() as transaction:
        with pytest.raises(PlaneError) as raised:
            store.enqueue(transaction, same_object)
        assert raised.value.code == "purge_idempotency_conflict"
        assert any("ON CONFLICT DO NOTHING" in statement for statement, _ in transaction.statements)
    with database.transaction() as transaction:
        with pytest.raises(PlaneError) as raised:
            store.mark_purged(
                transaction,
                owner_id="owner-2",
                tombstone_id="purge-1",
                expected_version=0,
                verified_absent_at=NOW,
            )
        assert raised.value.code == "purge_fence_conflict"


def test_every_tombstone_transition_updates_its_freshness_marker() -> None:
    store = PostgresPurgeStore()
    transaction = MemoryPurgeTransaction({})
    for identifier in ("purged", "failed", "review"):
        store.enqueue(transaction, tombstone(identifier))

    store.mark_purged(
        transaction,
        owner_id="owner-1",
        tombstone_id="purged",
        expected_version=0,
        verified_absent_at=NOW,
    )
    store.mark_failed(
        transaction,
        owner_id="owner-1",
        tombstone_id="failed",
        expected_version=0,
        available_at=NOW + timedelta(minutes=1),
        error_code="retryable",
    )
    store.mark_manual_review(
        transaction,
        owner_id="owner-1",
        tombstone_id="review",
        expected_version=0,
        error_code="operator_required",
    )

    transitions = [
        statement
        for statement, _ in transaction.statements
        if statement.startswith("UPDATE astralplane_purge_tombstone")
    ]
    assert len(transitions) == 3
    assert all("updated_at = clock_timestamp()" in statement for statement in transitions)


def test_database_success_then_blob_failure_remains_visible_and_retryable() -> None:
    database, store, blobs = MemoryPurgeDatabase(), PostgresPurgeStore(), MemoryBlobStore()
    schedule(database, store)
    blobs.put(owner_id="owner-1", key="attachments/file.bin", content=b"sensitive")
    blobs.fail_delete = True
    executor = DurablePurgeExecutor(database=database, store=store, blobs=blobs)

    result = executor.execute(
        owner_id="owner-1",
        tombstone_id="purge-1",
        now=NOW,
        retry_at=NOW + timedelta(minutes=5),
    )
    assert result.state is PurgeAttemptState.FAILED
    assert database.rows["purge-1"]["status"] == "failed"
    assert database.rows["purge-1"]["last_error_code"] == "blob_delete_failed"
    assert "host path" not in repr(database.rows)
    assert blobs.get(owner_id="owner-1", key="attachments/file.bin") == b"sensitive"

    blobs.fail_delete = False
    recovered = executor.execute(
        owner_id="owner-1",
        tombstone_id="purge-1",
        now=NOW + timedelta(minutes=5),
        retry_at=NOW + timedelta(minutes=10),
    )
    assert recovered.state is PurgeAttemptState.PURGED
    assert database.rows["purge-1"]["status"] == "purged"
    assert database.rows["purge-1"]["verified_absent_at"] == NOW + timedelta(minutes=5)


def test_blob_success_then_database_failure_never_reports_completion_and_recovers() -> None:
    database, store, blobs = MemoryPurgeDatabase(), PostgresPurgeStore(), MemoryBlobStore()
    schedule(database, store)
    blobs.put(owner_id="owner-1", key="attachments/file.bin", content=b"sensitive")
    executor = DurablePurgeExecutor(database=database, store=store, blobs=blobs)
    # schedule is transaction 1, load is 2, final transition is 3.
    database.fail_commit_numbers.add(3)

    with pytest.raises(RuntimeError, match="commit failure"):
        executor.execute(
            owner_id="owner-1",
            tombstone_id="purge-1",
            now=NOW,
            retry_at=NOW + timedelta(minutes=5),
        )
    assert blobs.is_absent(owner_id="owner-1", key="attachments/file.bin")
    assert database.rows["purge-1"]["status"] == "pending"
    assert database.rows["purge-1"]["verified_absent_at"] is None

    result = executor.execute(
        owner_id="owner-1",
        tombstone_id="purge-1",
        now=NOW + timedelta(seconds=1),
        retry_at=NOW + timedelta(minutes=5),
    )
    assert result.state is PurgeAttemptState.PURGED
    assert database.rows["purge-1"]["status"] == "purged"
    assert blobs.deleted.count(("owner-1", "attachments/file.bin")) == 2


def test_wrong_owner_cannot_read_delete_or_transition_another_owners_blob() -> None:
    database, store, blobs = MemoryPurgeDatabase(), PostgresPurgeStore(), MemoryBlobStore()
    schedule(database, store)
    blobs.put(owner_id="owner-1", key="attachments/file.bin", content=b"sensitive")
    executor = DurablePurgeExecutor(database=database, store=store, blobs=blobs)

    with pytest.raises(PlaneError) as raised:
        executor.execute(
            owner_id="owner-2",
            tombstone_id="purge-1",
            now=NOW,
            retry_at=NOW + timedelta(minutes=1),
        )
    assert raised.value.code == "purge_not_found"
    assert blobs.deleted == []
    assert blobs.get(owner_id="owner-1", key="attachments/file.bin") == b"sensitive"


def test_manual_review_retry_delay_and_completed_integrity_are_explicit() -> None:
    database, store, blobs = MemoryPurgeDatabase(), PostgresPurgeStore(), MemoryBlobStore()
    schedule(database, store, tombstone(available_at=NOW + timedelta(minutes=1)))
    executor = DurablePurgeExecutor(database=database, store=store, blobs=blobs)
    with pytest.raises(PlaneError) as raised:
        executor.execute(
            owner_id="owner-1",
            tombstone_id="purge-1",
            now=NOW,
            retry_at=NOW + timedelta(minutes=2),
        )
    assert raised.value.code == "purge_retry_not_ready"

    with database.transaction() as transaction:
        store.mark_manual_review(
            transaction,
            owner_id="owner-1",
            tombstone_id="purge-1",
            expected_version=0,
            error_code="operator_required",
        )
    with pytest.raises(PlaneError) as raised:
        executor.execute(
            owner_id="owner-1",
            tombstone_id="purge-1",
            now=NOW + timedelta(minutes=1),
            retry_at=NOW + timedelta(minutes=2),
        )
    assert raised.value.code == "purge_manual_review"

    database.rows["purge-1"].update(
        status="purged",
        verified_absent_at=NOW,
        available_at=NOW,
    )
    assert (
        executor.execute(
            owner_id="owner-1",
            tombstone_id="purge-1",
            now=NOW + timedelta(minutes=1),
            retry_at=NOW + timedelta(minutes=2),
        ).state
        is PurgeAttemptState.ALREADY_PURGED
    )
    blobs.put(owner_id="owner-1", key="attachments/file.bin", content=b"returned")
    with pytest.raises(PlaneError) as raised:
        executor.execute(
            owner_id="owner-1",
            tombstone_id="purge-1",
            now=NOW + timedelta(minutes=1),
            retry_at=NOW + timedelta(minutes=2),
        )
    assert raised.value.code == "purge_integrity_failure"


def test_explicit_root_blob_store_is_owner_scoped_idempotent_and_digest_checked(
    tmp_path: Path,
) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    blobs = ExplicitRootBlobStore(root.resolve())

    digest = blobs.put(owner_id="owner-1", key="nested/file.bin", content=b"one")
    blobs.put(owner_id="owner-2", key="nested/file.bin", content=b"two")
    assert digest == "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
    assert blobs.get(owner_id="owner-1", key="nested/file.bin") == b"one"
    assert blobs.get(owner_id="owner-2", key="nested/file.bin") == b"two"
    blobs.delete(owner_id="owner-1", key="nested/file.bin")
    blobs.delete(owner_id="owner-1", key="nested/file.bin")
    assert blobs.is_absent(owner_id="owner-1", key="nested/file.bin")
    assert not blobs.is_absent(owner_id="owner-2", key="nested/file.bin")


@pytest.mark.parametrize(
    "key",
    [
        "",
        "nul\x00key",
        "../outside",
        "nested/../outside",
        "/absolute",
        "C:\\absolute",
        "double//part",
        "a:b",
    ],
)
def test_explicit_root_rejects_traversal_and_platform_escape(tmp_path: Path, key: str) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    blobs = ExplicitRootBlobStore(root.resolve())
    with pytest.raises(SQLContractError):
        blobs.put(owner_id="owner-1", key=key, content=b"unsafe")


def test_explicit_root_rejects_missing_relative_and_link_boundaries(tmp_path: Path) -> None:
    with pytest.raises(SQLContractError, match="absolute"):
        ExplicitRootBlobStore(Path("relative"))
    with pytest.raises(PlaneError, match="unavailable"):
        ExplicitRootBlobStore((tmp_path / "missing").resolve())

    root = tmp_path / "blobs"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    link = root / "owner-1"
    try:
        os.symlink(outside, link, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are not available on this Windows host")
    blobs = ExplicitRootBlobStore(root.resolve())
    with pytest.raises(PlaneError, match="link"):
        blobs.put(owner_id="owner-1", key="file.bin", content=b"unsafe")


def test_explicit_root_rejects_nonfiles_missing_blobs_and_invalid_content(tmp_path: Path) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    blobs = ExplicitRootBlobStore(root.resolve())
    with pytest.raises(SQLContractError, match="bytes"):
        blobs.put(owner_id="owner-1", key="file.bin", content="bad")  # type: ignore[arg-type]
    with pytest.raises(PlaneError) as raised:
        blobs.get(owner_id="owner-1", key="missing.bin")
    assert raised.value.code == "blob_not_found"

    directory = root / "owner-1" / "directory"
    directory.mkdir(parents=True)
    for operation in (
        lambda: blobs.get(owner_id="owner-1", key="directory"),
        lambda: blobs.delete(owner_id="owner-1", key="directory"),
        lambda: blobs.put(owner_id="owner-1", key="directory", content=b"bad"),
    ):
        with pytest.raises(PlaneError) as raised:
            operation()
        assert raised.value.code == "blob_path_unsafe"

    blocking_file = root / "owner-2"
    blocking_file.write_bytes(b"not a directory")
    with pytest.raises((FileExistsError, PlaneError)):
        blobs.put(owner_id="owner-2", key="nested/file.bin", content=b"bad")


def test_explicit_root_cleans_temporary_file_after_durable_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    blobs = ExplicitRootBlobStore(root.resolve())

    def fail_fsync(_: int) -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="fsync failure"):
        blobs.put(owner_id="owner-1", key="file.bin", content=b"content")
    assert list((root / "owner-1").iterdir()) == []


def test_blob_root_itself_must_be_a_directory(tmp_path: Path) -> None:
    file_root = tmp_path / "file-root"
    file_root.write_bytes(b"not a directory")
    with pytest.raises(SQLContractError, match="real directory"):
        ExplicitRootBlobStore(file_root.resolve())


@pytest.mark.parametrize(
    "mutation",
    [
        {"object_kind": "unsupported"},
        {"storage_locator_sha256": "0" * 64},
        {"status": PurgeStatus.PURGED},
        {"verified_absent_at": NOW},
        {"attempt_count": -1},
        {"attempt_count": "one"},
        {"version": -1},
        {"version": "one"},
        {"version": 1},
        {"last_error_code": "raw exception details"},
    ],
)
def test_invalid_tombstones_are_rejected(mutation: dict[str, object]) -> None:
    original = tombstone()
    values: dict[str, object] = {
        "tombstone_id": original.tombstone_id,
        "owner_id": original.owner_id,
        "object_kind": original.object_kind,
        "object_id": original.object_id,
        "storage_key": original.storage_key,
        "storage_locator_sha256": original.storage_locator_sha256,
        "requested_at": original.requested_at,
        "status": original.status,
        "attempt_count": original.attempt_count,
        "version": original.version,
        "available_at": original.available_at,
        "verified_absent_at": original.verified_absent_at,
        "last_error_code": original.last_error_code,
    }
    values.update(mutation)
    with pytest.raises(SQLContractError):
        PostgresPurgeStore().enqueue(
            MemoryPurgeTransaction({}),
            PurgeTombstone(**values),  # type: ignore[arg-type]
        )


def test_store_rejects_invalid_transition_parameters_and_row_counts() -> None:
    store = PostgresPurgeStore()
    transaction = MemoryPurgeTransaction({})
    transaction.override_result = FakeResult(-1)
    with pytest.raises(PlaneError) as raised:
        store.enqueue(transaction, tombstone())
    assert raised.value.code == "purge_write_invalid"
    with pytest.raises(SQLContractError):
        store.mark_failed(
            transaction,
            owner_id="owner-1",
            tombstone_id="purge-1",
            expected_version=True,
            available_at=NOW,
            error_code="failure",
        )
    with pytest.raises(SQLContractError):
        store.mark_manual_review(
            transaction,
            owner_id="owner-1",
            tombstone_id="purge-1",
            expected_version=0,
            error_code="unsafe details",
        )
    with pytest.raises(SQLContractError):
        store.mark_failed(
            transaction,
            owner_id="owner-1",
            tombstone_id="purge-1",
            expected_version=0,
            available_at=NOW,
            error_code="unsafe details",
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("object_kind", 7),
        ("attempt_count", "one"),
        ("requested_at", "yesterday"),
        ("status", "unknown"),
    ],
)
def test_store_rejects_corrupt_database_records(field: str, value: object) -> None:
    database, store = MemoryPurgeDatabase(), PostgresPurgeStore()
    schedule(database, store)
    database.rows["purge-1"][field] = value
    with database.transaction() as transaction, pytest.raises(PlaneError) as raised:
        store.load(transaction, owner_id="owner-1", tombstone_id="purge-1")
    assert raised.value.code == "purge_record_invalid"


def test_non_tombstone_and_invalid_owner_are_rejected() -> None:
    store = PostgresPurgeStore()
    with pytest.raises(SQLContractError):
        store.enqueue(MemoryPurgeTransaction({}), object())  # type: ignore[arg-type]
    with pytest.raises(SQLContractError):
        storage_locator_sha256(owner_id="bad owner", key="file.bin")


def test_blob_still_present_is_recorded_as_failed_and_nonverifiable_store_is_rejected() -> None:
    database, store, blobs = MemoryPurgeDatabase(), PostgresPurgeStore(), StickyBlobStore()
    schedule(database, store)
    blobs.put(owner_id="owner-1", key="attachments/file.bin", content=b"sensitive")
    result = DurablePurgeExecutor(database=database, store=store, blobs=blobs).execute(
        owner_id="owner-1",
        tombstone_id="purge-1",
        now=NOW,
        retry_at=NOW + timedelta(minutes=1),
    )
    assert result.state is PurgeAttemptState.FAILED
    assert database.rows["purge-1"]["status"] == "failed"

    class DeleteOnly:
        root = PurePath("/blobs")

        def put(self, **_: object) -> str:
            return hashlib.sha256(b"").hexdigest()

        def get(self, **_: object) -> bytes:
            return b""

        def delete(self, **_: object) -> None:
            return None

    with pytest.raises(SQLContractError, match="absence verification"):
        DurablePurgeExecutor(
            database=database,
            store=store,
            blobs=DeleteOnly(),  # type: ignore[arg-type]
        )
