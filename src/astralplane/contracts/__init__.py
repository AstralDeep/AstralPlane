"""Neutral public contracts for the embedded AstralPlane boundary."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import PurePath
from typing import Any, Protocol, TypeAlias, runtime_checkable

Statement: TypeAlias = str
PositionalParameters: TypeAlias = tuple[object, ...]
NamedParameters: TypeAlias = Mapping[str, object]
Parameters: TypeAlias = PositionalParameters | NamedParameters
Record: TypeAlias = Mapping[str, Any]


class IsolationLevel(StrEnum):
    """PostgreSQL transaction isolation levels accepted by AstralPlane."""

    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


@runtime_checkable
class CommandResultContract(Protocol):
    """Detached metadata returned by a completed command."""

    @property
    def rowcount(self) -> int: ...

    @property
    def status_message(self) -> str | None: ...

    @property
    def returned_records(self) -> tuple[Record, ...]: ...


@runtime_checkable
class QueryExecutor(Protocol):
    """Native-parameter query surface shared by transactions and repositories."""

    def execute(
        self, statement: Statement, parameters: Parameters = ()
    ) -> CommandResultContract: ...

    def fetch_one(self, statement: Statement, parameters: Parameters = ()) -> Record | None: ...

    def fetch_all(
        self, statement: Statement, parameters: Parameters = ()
    ) -> tuple[Record, ...]: ...


@runtime_checkable
class Transaction(QueryExecutor, Protocol):
    """Caller-owned transaction; nested consumers never commit it."""

    def savepoint(self, name: str) -> AbstractContextManager[Transaction]: ...


@runtime_checkable
class PlaneDatabase(Protocol):
    """Factory for explicit transaction scopes."""

    def transaction(
        self, *, isolation: IsolationLevel | None = None
    ) -> AbstractContextManager[Transaction]: ...


@runtime_checkable
class SchemaMigration(Protocol):
    """One declared repeat-safe, database-only migration edge."""

    name: str
    source_revisions: tuple[str | None, ...]
    target_revision: str
    checksum: str

    def apply(self, transaction: Transaction) -> None: ...


@runtime_checkable
class Repository(Protocol):
    """Neutral repository whose caller declares transaction ownership."""

    def health(self, transaction: Transaction) -> Mapping[str, object]: ...


@runtime_checkable
class BlobStore(Protocol):
    """Storage mechanics over an explicitly configured durable root."""

    @property
    def root(self) -> PurePath: ...

    def put(self, *, owner_id: str, key: str, content: bytes) -> str: ...

    def get(self, *, owner_id: str, key: str) -> bytes: ...

    def delete(self, *, owner_id: str, key: str) -> None: ...


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """Canonical durable event submitted inside an authoritative transaction."""

    entry_id: str
    topic: str
    canonical_payload: bytes
    payload_sha256: str
    idempotency_key: str
    available_at: datetime


@dataclass(frozen=True, slots=True)
class ClaimedOutboxEntry:
    """Detached lease record returned to one product-owned worker."""

    entry: OutboxEntry
    worker_id: str
    lease_expires_at: datetime
    expected_version: int
    attempt: int


@dataclass(frozen=True, slots=True)
class ReclaimedOutboxEntry:
    """Expired lease made available for a later bounded claim."""

    entry_id: str
    previous_worker_id: str
    expected_version: int
    available_at: datetime


@runtime_checkable
class OutboxStore(Protocol):
    """Durable delivery mechanics; product handlers remain in AstralDeep."""

    def enqueue(
        self,
        transaction: Transaction,
        entry: OutboxEntry,
    ) -> CommandResultContract: ...

    def claim(
        self,
        transaction: Transaction,
        *,
        worker_id: str,
        topics: tuple[str, ...],
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[ClaimedOutboxEntry, ...]: ...

    def ack(
        self,
        transaction: Transaction,
        *,
        entry_id: str,
        worker_id: str,
        expected_version: int,
        now: datetime,
    ) -> CommandResultContract: ...

    def retry(
        self,
        transaction: Transaction,
        *,
        entry_id: str,
        worker_id: str,
        expected_version: int,
        available_at: datetime,
        error_code: str,
        now: datetime,
    ) -> CommandResultContract: ...

    def dead_letter(
        self,
        transaction: Transaction,
        *,
        entry_id: str,
        worker_id: str,
        expected_version: int,
        error_code: str,
        now: datetime,
    ) -> CommandResultContract: ...

    def reclaim_expired(
        self,
        transaction: Transaction,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[ReclaimedOutboxEntry, ...]: ...


@runtime_checkable
class LifecycleStore(Protocol):
    """Neutral durable lifecycle state without product-policy decisions."""

    def compare_and_set(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        operation_id: str,
        expected_version: int,
        state: str,
    ) -> CommandResultContract: ...


@runtime_checkable
class RecoveryInspector(Protocol):
    """Read-only compatibility and recovery evidence surface."""

    def inspect(self, transaction: Transaction) -> Mapping[str, object]: ...

    def verify(self, transaction: Transaction) -> Mapping[str, object]: ...


class ReconciliationMarkerState(StrEnum):
    """Durable lifecycle for one required versioned reconciliation hook."""

    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ReconciliationHookIdentity:
    """Stable idempotency identity; behavior changes require a new version."""

    name: str
    version: str


@dataclass(frozen=True, slots=True)
class ReconciliationMarker:
    """Detached durable proof for one hook attempt under one exact plan."""

    schema_revision: str
    plan_digest: str
    hook: ReconciliationHookIdentity
    state: ReconciliationMarkerState
    attempt: int
    result_digest: str | None = None
    error_type: str | None = None


@runtime_checkable
class ReconciliationSession(Protocol):
    """Durable marker store bound to one cross-process coordination scope."""

    def get_marker(self, hook: ReconciliationHookIdentity) -> ReconciliationMarker | None: ...

    def mark_started(self, hook: ReconciliationHookIdentity) -> ReconciliationMarker: ...

    def mark_completed(
        self,
        hook: ReconciliationHookIdentity,
        *,
        result_digest: str,
    ) -> ReconciliationMarker: ...

    def mark_failed(
        self,
        hook: ReconciliationHookIdentity,
        *,
        error_type: str,
    ) -> ReconciliationMarker: ...


@runtime_checkable
class ReconciliationCoordinator(Protocol):
    """Supply a durable store while holding one cross-process advisory identity."""

    def coordinate(
        self,
        *,
        advisory_lock: tuple[int, int],
        schema_revision: str,
        plan_digest: str,
    ) -> AbstractContextManager[ReconciliationSession]: ...


@runtime_checkable
class ProductReconciler(Protocol):
    """Required named/versioned idempotent hook supplied by the product."""

    @property
    def name(self) -> str: ...

    @property
    def version(self) -> str: ...

    def reconcile(self, context: Mapping[str, object]) -> Mapping[str, object] | None: ...


MigrationCallable: TypeAlias = Callable[[Transaction], None]
ReconcilerFactory: TypeAlias = Callable[[], Iterator[ProductReconciler]]

__all__ = (
    "BlobStore",
    "ClaimedOutboxEntry",
    "CommandResultContract",
    "IsolationLevel",
    "LifecycleStore",
    "MigrationCallable",
    "NamedParameters",
    "OutboxEntry",
    "OutboxStore",
    "Parameters",
    "PlaneDatabase",
    "PositionalParameters",
    "ProductReconciler",
    "QueryExecutor",
    "ReclaimedOutboxEntry",
    "ReconcilerFactory",
    "ReconciliationCoordinator",
    "ReconciliationHookIdentity",
    "ReconciliationMarker",
    "ReconciliationMarkerState",
    "ReconciliationSession",
    "Record",
    "RecoveryInspector",
    "Repository",
    "SchemaMigration",
    "Statement",
    "Transaction",
)
