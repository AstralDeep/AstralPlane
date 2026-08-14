"""Durable purge tombstones and explicit-root blob mechanics."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePath
from typing import Final, Protocol, runtime_checkable

from astralplane.contracts import (
    BlobStore,
    CommandResultContract,
    PlaneDatabase,
    Record,
    Transaction,
)
from astralplane.errors import PlaneError, SQLContractError

_SAFE_IDENTIFIER: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,254}$")
_SAFE_ERROR_CODE: Final = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_OBJECT_KINDS: Final = frozenset({"attachment", "artifact", "knowledge", "generated_agent"})
_FILE_ATTRIBUTE_REPARSE_POINT: Final = 0x400

_ENQUEUE_SQL: Final = """
INSERT INTO astralplane_purge_tombstone (
    tombstone_id,
    owner_id,
    object_kind,
    object_id,
    storage_key,
    storage_locator_sha256,
    requested_at,
    status,
    attempt_count,
    version,
    available_at,
    verified_absent_at,
    last_error_code
)
VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', 0, 0, %s, NULL, NULL)
ON CONFLICT DO NOTHING
RETURNING tombstone_id
""".strip()

_LOAD_SQL: Final = """
SELECT
    tombstone_id,
    owner_id,
    object_kind,
    object_id,
    storage_key,
    storage_locator_sha256,
    requested_at,
    status,
    attempt_count,
    version,
    available_at,
    verified_absent_at,
    last_error_code
FROM astralplane_purge_tombstone
WHERE owner_id = %s
  AND tombstone_id = %s
""".strip()

_LOAD_BY_OBJECT_SQL: Final = """
SELECT
    tombstone_id,
    owner_id,
    object_kind,
    object_id,
    storage_key,
    storage_locator_sha256,
    requested_at,
    status,
    attempt_count,
    version,
    available_at,
    verified_absent_at,
    last_error_code
FROM astralplane_purge_tombstone
WHERE owner_id = %s
  AND object_kind = %s
  AND object_id = %s
""".strip()

_LIST_INCOMPLETE_SQL: Final = """
SELECT
    tombstone_id,
    owner_id,
    object_kind,
    object_id,
    storage_key,
    storage_locator_sha256,
    requested_at,
    status,
    attempt_count,
    version,
    available_at,
    verified_absent_at,
    last_error_code
FROM astralplane_purge_tombstone
WHERE owner_id = %s
  AND status <> 'purged'
ORDER BY requested_at, tombstone_id
""".strip()

_MARK_PURGED_SQL: Final = """
UPDATE astralplane_purge_tombstone
SET status = 'purged',
    attempt_count = attempt_count + 1,
    version = version + 1,
    verified_absent_at = %s,
    available_at = %s,
    last_error_code = NULL,
    updated_at = clock_timestamp()
WHERE owner_id = %s
  AND tombstone_id = %s
  AND version = %s
  AND status IN ('pending', 'failed')
""".strip()

_MARK_FAILED_SQL: Final = """
UPDATE astralplane_purge_tombstone
SET status = 'failed',
    attempt_count = attempt_count + 1,
    version = version + 1,
    available_at = %s,
    verified_absent_at = NULL,
    last_error_code = %s,
    updated_at = clock_timestamp()
WHERE owner_id = %s
  AND tombstone_id = %s
  AND version = %s
  AND status IN ('pending', 'failed')
""".strip()

_MARK_MANUAL_REVIEW_SQL: Final = """
UPDATE astralplane_purge_tombstone
SET status = 'manual_review',
    version = version + 1,
    last_error_code = %s,
    updated_at = clock_timestamp()
WHERE owner_id = %s
  AND tombstone_id = %s
  AND version = %s
  AND status IN ('pending', 'failed')
""".strip()


class PurgeStatus(StrEnum):
    PENDING = "pending"
    PURGED = "purged"
    FAILED = "failed"
    MANUAL_REVIEW = "manual_review"


@dataclass(frozen=True, slots=True)
class PurgeTombstone:
    """Detached durable proof that logical and physical deletion must converge."""

    tombstone_id: str
    owner_id: str
    object_kind: str
    object_id: str
    storage_key: str = field(repr=False)
    storage_locator_sha256: str
    requested_at: datetime
    status: PurgeStatus = PurgeStatus.PENDING
    attempt_count: int = 0
    version: int = 0
    available_at: datetime | None = None
    verified_absent_at: datetime | None = None
    last_error_code: str | None = None


class PurgeAttemptState(StrEnum):
    PURGED = "purged"
    ALREADY_PURGED = "already_purged"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PurgeAttemptResult:
    """Redacted purge result; raw blob locators never appear here."""

    state: PurgeAttemptState
    tombstone_id: str
    attempt: int
    error_code: str | None = None


@runtime_checkable
class VerifiableBlobStore(BlobStore, Protocol):
    """Blob deletion surface that can prove safe absence."""

    def is_absent(self, *, owner_id: str, key: str) -> bool: ...


def _bounded_identifier(value: str, *, name: str) -> str:
    if not isinstance(value, str) or _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise SQLContractError(f"{name} is not a valid bounded identifier")
    return value


def _utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SQLContractError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _normalized_key(key: str) -> str:
    if not isinstance(key, str) or not key or "\x00" in key:
        raise SQLContractError("blob key must be a non-empty string without NUL bytes")
    if key.startswith(("/", "\\")) or re.match(r"^[A-Za-z]:", key):
        raise SQLContractError("blob key must be relative to the configured root")
    parts = re.split(r"[/\\]", key)
    if any(not part or part in {".", ".."} or len(part) > 255 for part in parts):
        raise SQLContractError("blob key contains an unsafe path component")
    if any(":" in part for part in parts):
        raise SQLContractError("blob key contains a platform-specific path separator")
    return "/".join(parts)


def storage_locator_sha256(*, owner_id: str, key: str) -> str:
    """Hash the normalized owner-scoped locator without exposing it in diagnostics."""

    owner = _bounded_identifier(owner_id, name="owner_id")
    normalized = _normalized_key(key)
    return hashlib.sha256(f"{owner}\0{normalized}".encode()).hexdigest()


def _validate_tombstone(tombstone: PurgeTombstone) -> PurgeTombstone:
    if not isinstance(tombstone, PurgeTombstone):
        raise SQLContractError("tombstone must be a PurgeTombstone")
    tombstone_id = _bounded_identifier(tombstone.tombstone_id, name="tombstone_id")
    owner_id = _bounded_identifier(tombstone.owner_id, name="owner_id")
    object_id = _bounded_identifier(tombstone.object_id, name="object_id")
    if tombstone.object_kind not in _OBJECT_KINDS:
        raise SQLContractError("object_kind is not a supported durable object kind")
    storage_key = _normalized_key(tombstone.storage_key)
    expected_digest = storage_locator_sha256(owner_id=owner_id, key=storage_key)
    if tombstone.storage_locator_sha256 != expected_digest:
        raise SQLContractError("storage locator does not match its SHA-256 digest")
    if (
        isinstance(tombstone.attempt_count, bool)
        or not isinstance(tombstone.attempt_count, int)
        or tombstone.attempt_count < 0
    ):
        raise SQLContractError("attempt_count must be a non-negative integer")
    if (
        isinstance(tombstone.version, bool)
        or not isinstance(tombstone.version, int)
        or tombstone.version < 0
    ):
        raise SQLContractError("version must be a non-negative integer")
    available_at = _utc(
        tombstone.available_at or tombstone.requested_at,
        name="available_at",
    )
    verified_absent_at = (
        None
        if tombstone.verified_absent_at is None
        else _utc(tombstone.verified_absent_at, name="verified_absent_at")
    )
    if tombstone.status is PurgeStatus.PURGED and verified_absent_at is None:
        raise SQLContractError("purged tombstones require verified_absent_at")
    if tombstone.status is not PurgeStatus.PURGED and verified_absent_at is not None:
        raise SQLContractError("incomplete tombstones cannot claim verified absence")
    if tombstone.last_error_code is not None and (
        _SAFE_ERROR_CODE.fullmatch(tombstone.last_error_code) is None
    ):
        raise SQLContractError("last_error_code is not a safe bounded code")
    return PurgeTombstone(
        tombstone_id=tombstone_id,
        owner_id=owner_id,
        object_kind=tombstone.object_kind,
        object_id=object_id,
        storage_key=storage_key,
        storage_locator_sha256=expected_digest,
        requested_at=_utc(tombstone.requested_at, name="requested_at"),
        status=PurgeStatus(tombstone.status),
        attempt_count=tombstone.attempt_count,
        version=tombstone.version,
        available_at=available_at,
        verified_absent_at=verified_absent_at,
        last_error_code=tombstone.last_error_code,
    )


def _record_text(record: Record, field_name: str) -> str:
    value = record.get(field_name)
    if not isinstance(value, str):
        raise PlaneError(
            "purge store returned an invalid field",
            code="purge_record_invalid",
            metadata={"field": field_name},
        )
    return value


def _record_int(record: Record, field_name: str) -> int:
    value = record.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PlaneError(
            "purge store returned an invalid field",
            code="purge_record_invalid",
            metadata={"field": field_name},
        )
    return value


def _record_datetime(record: Record, field_name: str, *, optional: bool = False) -> datetime | None:
    value = record.get(field_name)
    if optional and value is None:
        return None
    try:
        return _utc(value, name=field_name)  # type: ignore[arg-type]
    except SQLContractError as exc:
        raise PlaneError(
            "purge store returned an invalid field",
            code="purge_record_invalid",
            metadata={"field": field_name},
        ) from exc


def _from_record(record: Record) -> PurgeTombstone:
    try:
        status = PurgeStatus(_record_text(record, "status"))
    except ValueError as exc:
        raise PlaneError(
            "purge store returned an invalid status", code="purge_record_invalid"
        ) from exc
    return _validate_tombstone(
        PurgeTombstone(
            tombstone_id=_record_text(record, "tombstone_id"),
            owner_id=_record_text(record, "owner_id"),
            object_kind=_record_text(record, "object_kind"),
            object_id=_record_text(record, "object_id"),
            storage_key=_record_text(record, "storage_key"),
            storage_locator_sha256=_record_text(record, "storage_locator_sha256"),
            requested_at=_record_datetime(record, "requested_at"),  # type: ignore[arg-type]
            status=status,
            attempt_count=_record_int(record, "attempt_count"),
            version=_record_int(record, "version"),
            available_at=_record_datetime(record, "available_at", optional=True),
            verified_absent_at=_record_datetime(record, "verified_absent_at", optional=True),
            last_error_code=(
                None
                if record.get("last_error_code") is None
                else _record_text(record, "last_error_code")
            ),
        )
    )


def _require_update(
    result: CommandResultContract,
    *,
    tombstone_id: str,
    operation: str,
) -> CommandResultContract:
    if result.rowcount != 1:
        raise PlaneError(
            "purge tombstone version fence rejected the transition",
            code="purge_fence_conflict",
            metadata={"tombstone_id": tombstone_id, "operation": operation},
        )
    return result


class PostgresPurgeStore:
    """Owner-scoped durable purge tombstones in caller-owned transactions."""

    def enqueue(
        self,
        transaction: Transaction,
        tombstone: PurgeTombstone,
    ) -> CommandResultContract:
        exact = _validate_tombstone(tombstone)
        if exact.status is not PurgeStatus.PENDING or exact.version or exact.attempt_count:
            raise SQLContractError("new purge tombstones must start pending at version zero")
        result = transaction.execute(
            _ENQUEUE_SQL,
            (
                exact.tombstone_id,
                exact.owner_id,
                exact.object_kind,
                exact.object_id,
                exact.storage_key,
                exact.storage_locator_sha256,
                exact.requested_at,
                exact.available_at,
            ),
        )
        if result.rowcount == 1:
            return result
        if result.rowcount != 0:
            raise PlaneError(
                "purge enqueue returned an invalid row count", code="purge_write_invalid"
            )
        existing = self.load(
            transaction,
            owner_id=exact.owner_id,
            tombstone_id=exact.tombstone_id,
        )
        object_match_record = transaction.fetch_one(
            _LOAD_BY_OBJECT_SQL,
            (exact.owner_id, exact.object_kind, exact.object_id),
        )
        object_match = None if object_match_record is None else _from_record(object_match_record)
        matches = tuple(item for item in (existing, object_match) if item is not None)
        if not matches or any(self._identity(item) != self._identity(exact) for item in matches):
            raise PlaneError(
                "purge tombstone identifier or owner object already represents different work",
                code="purge_idempotency_conflict",
                metadata={"tombstone_id": exact.tombstone_id},
            )
        return result

    def load(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        tombstone_id: str,
    ) -> PurgeTombstone | None:
        owner = _bounded_identifier(owner_id, name="owner_id")
        identifier = _bounded_identifier(tombstone_id, name="tombstone_id")
        record = transaction.fetch_one(_LOAD_SQL, (owner, identifier))
        return None if record is None else _from_record(record)

    def list_incomplete(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
    ) -> tuple[PurgeTombstone, ...]:
        owner = _bounded_identifier(owner_id, name="owner_id")
        records = transaction.fetch_all(_LIST_INCOMPLETE_SQL, (owner,))
        return tuple(_from_record(record) for record in records)

    def mark_purged(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        tombstone_id: str,
        expected_version: int,
        verified_absent_at: datetime,
    ) -> CommandResultContract:
        owner, identifier, version = self._fence(owner_id, tombstone_id, expected_version)
        timestamp = _utc(verified_absent_at, name="verified_absent_at")
        result = transaction.execute(
            _MARK_PURGED_SQL,
            (timestamp, timestamp, owner, identifier, version),
        )
        return _require_update(result, tombstone_id=identifier, operation="mark-purged")

    def mark_failed(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        tombstone_id: str,
        expected_version: int,
        available_at: datetime,
        error_code: str,
    ) -> CommandResultContract:
        owner, identifier, version = self._fence(owner_id, tombstone_id, expected_version)
        if not isinstance(error_code, str) or _SAFE_ERROR_CODE.fullmatch(error_code) is None:
            raise SQLContractError("error_code is not a safe bounded code")
        result = transaction.execute(
            _MARK_FAILED_SQL,
            (_utc(available_at, name="available_at"), error_code, owner, identifier, version),
        )
        return _require_update(result, tombstone_id=identifier, operation="mark-failed")

    def mark_manual_review(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        tombstone_id: str,
        expected_version: int,
        error_code: str,
    ) -> CommandResultContract:
        owner, identifier, version = self._fence(owner_id, tombstone_id, expected_version)
        if not isinstance(error_code, str) or _SAFE_ERROR_CODE.fullmatch(error_code) is None:
            raise SQLContractError("error_code is not a safe bounded code")
        result = transaction.execute(
            _MARK_MANUAL_REVIEW_SQL,
            (error_code, owner, identifier, version),
        )
        return _require_update(result, tombstone_id=identifier, operation="manual-review")

    @staticmethod
    def _fence(owner_id: str, tombstone_id: str, expected_version: int) -> tuple[str, str, int]:
        owner = _bounded_identifier(owner_id, name="owner_id")
        identifier = _bounded_identifier(tombstone_id, name="tombstone_id")
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 0
        ):
            raise SQLContractError("expected_version must be a non-negative integer")
        return owner, identifier, expected_version

    @staticmethod
    def _identity(tombstone: PurgeTombstone) -> tuple[object, ...]:
        return (
            tombstone.tombstone_id,
            tombstone.owner_id,
            tombstone.object_kind,
            tombstone.object_id,
            tombstone.storage_key,
            tombstone.storage_locator_sha256,
            tombstone.requested_at,
        )


def _is_reparse(metadata: os.stat_result) -> bool:
    return stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
    )


class ExplicitRootBlobStore:
    """Owner-scoped blob store that never derives its root from package paths."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        supplied = Path(root)
        if not supplied.is_absolute():
            raise SQLContractError("blob root must be an explicit absolute path")
        try:
            metadata = supplied.lstat()
        except OSError as exc:
            raise PlaneError(
                "configured blob root is unavailable", code="blob_root_unavailable"
            ) from exc
        if _is_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
            raise SQLContractError(
                "blob root must be a real directory, not a link or reparse point"
            )
        self._root = supplied.resolve(strict=True)

    @property
    def root(self) -> PurePath:
        return self._root

    def put(self, *, owner_id: str, key: str, content: bytes) -> str:
        if not isinstance(content, bytes):
            raise SQLContractError("blob content must be bytes")
        target = self._path(owner_id=owner_id, key=key)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._check_chain(target.parent)
        self._reject_existing_unsafe_target(target)
        temporary = target.parent / f".astralplane-{uuid.uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(temporary, flags, 0o600)
            try:
                with os.fdopen(descriptor, "wb", closefd=True) as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                with suppress(OSError):
                    os.close(descriptor)
                raise
            os.replace(temporary, target)
        finally:
            with suppress(FileNotFoundError):
                temporary.unlink()
        return hashlib.sha256(content).hexdigest()

    def get(self, *, owner_id: str, key: str) -> bytes:
        target = self._path(owner_id=owner_id, key=key)
        self._check_chain(target.parent)
        metadata = self._safe_file_metadata(target)
        if not stat.S_ISREG(metadata.st_mode):
            raise PlaneError("blob locator is not a regular file", code="blob_path_unsafe")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(target, flags)
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            return stream.read()

    def delete(self, *, owner_id: str, key: str) -> None:
        target = self._path(owner_id=owner_id, key=key)
        self._check_chain(target.parent)
        try:
            metadata = target.lstat()
        except FileNotFoundError:
            return
        if _is_reparse(metadata) or not stat.S_ISREG(metadata.st_mode):
            raise PlaneError("blob locator is not a safe regular file", code="blob_path_unsafe")
        target.unlink()

    def is_absent(self, *, owner_id: str, key: str) -> bool:
        target = self._path(owner_id=owner_id, key=key)
        self._check_chain(target.parent)
        try:
            metadata = target.lstat()
        except FileNotFoundError:
            return True
        if _is_reparse(metadata):
            raise PlaneError("blob locator resolves through a link", code="blob_path_unsafe")
        return False

    def _path(self, *, owner_id: str, key: str) -> Path:
        owner = _bounded_identifier(owner_id, name="owner_id")
        normalized = _normalized_key(key)
        return self._root.joinpath(owner, *normalized.split("/"))

    def _check_chain(self, directory: Path) -> None:
        try:
            relative = directory.relative_to(self._root)
        except ValueError as exc:
            raise PlaneError(
                "blob path escaped its configured root", code="blob_path_unsafe"
            ) from exc
        current = self._root
        for part in relative.parts:
            current /= part
            try:
                metadata = current.lstat()
            except FileNotFoundError:
                return
            if _is_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
                raise PlaneError(
                    "blob path crosses a link or non-directory", code="blob_path_unsafe"
                )

    @staticmethod
    def _safe_file_metadata(target: Path) -> os.stat_result:
        try:
            metadata = target.lstat()
        except FileNotFoundError as exc:
            raise PlaneError("blob does not exist", code="blob_not_found") from exc
        if _is_reparse(metadata):
            raise PlaneError("blob locator resolves through a link", code="blob_path_unsafe")
        return metadata

    @staticmethod
    def _reject_existing_unsafe_target(target: Path) -> None:
        try:
            metadata = target.lstat()
        except FileNotFoundError:
            return
        if _is_reparse(metadata) or not stat.S_ISREG(metadata.st_mode):
            raise PlaneError("blob locator is not a safe regular file", code="blob_path_unsafe")


class DurablePurgeExecutor:
    """Converge one tombstone without ever hiding a partial failure."""

    def __init__(
        self,
        *,
        database: PlaneDatabase,
        store: PostgresPurgeStore,
        blobs: VerifiableBlobStore,
    ) -> None:
        if not isinstance(blobs, VerifiableBlobStore):
            raise SQLContractError("blobs must support deletion and absence verification")
        self._database = database
        self._store = store
        self._blobs = blobs

    def execute(
        self,
        *,
        owner_id: str,
        tombstone_id: str,
        now: datetime,
        retry_at: datetime,
    ) -> PurgeAttemptResult:
        timestamp = _utc(now, name="now")
        next_retry = _utc(retry_at, name="retry_at")
        with self._database.transaction() as transaction:
            tombstone = self._store.load(
                transaction,
                owner_id=owner_id,
                tombstone_id=tombstone_id,
            )
        if tombstone is None:
            raise PlaneError(
                "purge tombstone was not found in the owner scope",
                code="purge_not_found",
                metadata={"tombstone_id": tombstone_id},
            )
        if tombstone.status is PurgeStatus.MANUAL_REVIEW:
            raise PlaneError(
                "purge tombstone requires manual review",
                code="purge_manual_review",
                metadata={"tombstone_id": tombstone_id},
            )
        if tombstone.status is PurgeStatus.PURGED:
            if not self._blobs.is_absent(owner_id=owner_id, key=tombstone.storage_key):
                raise PlaneError(
                    "purged tombstone no longer matches physical storage",
                    code="purge_integrity_failure",
                    metadata={"tombstone_id": tombstone_id},
                )
            return PurgeAttemptResult(
                state=PurgeAttemptState.ALREADY_PURGED,
                tombstone_id=tombstone_id,
                attempt=tombstone.attempt_count,
            )
        if tombstone.available_at is not None and timestamp < tombstone.available_at:
            raise PlaneError(
                "purge tombstone is not yet available for retry",
                code="purge_retry_not_ready",
                metadata={"tombstone_id": tombstone_id},
            )

        try:
            self._blobs.delete(owner_id=owner_id, key=tombstone.storage_key)
            if not self._blobs.is_absent(owner_id=owner_id, key=tombstone.storage_key):
                raise PlaneError(
                    "blob remained present after deletion", code="blob_delete_incomplete"
                )
        except Exception:
            with self._database.transaction() as transaction:
                result = self._store.mark_failed(
                    transaction,
                    owner_id=owner_id,
                    tombstone_id=tombstone_id,
                    expected_version=tombstone.version,
                    available_at=next_retry,
                    error_code="blob_delete_failed",
                )
                _require_update(result, tombstone_id=tombstone_id, operation="mark-failed")
            return PurgeAttemptResult(
                state=PurgeAttemptState.FAILED,
                tombstone_id=tombstone_id,
                attempt=tombstone.attempt_count + 1,
                error_code="blob_delete_failed",
            )

        with self._database.transaction() as transaction:
            result = self._store.mark_purged(
                transaction,
                owner_id=owner_id,
                tombstone_id=tombstone_id,
                expected_version=tombstone.version,
                verified_absent_at=timestamp,
            )
            _require_update(result, tombstone_id=tombstone_id, operation="mark-purged")
        return PurgeAttemptResult(
            state=PurgeAttemptState.PURGED,
            tombstone_id=tombstone_id,
            attempt=tombstone.attempt_count + 1,
        )


__all__ = (
    "DurablePurgeExecutor",
    "ExplicitRootBlobStore",
    "PostgresPurgeStore",
    "PurgeAttemptResult",
    "PurgeAttemptState",
    "PurgeStatus",
    "PurgeTombstone",
    "VerifiableBlobStore",
    "storage_locator_sha256",
)
