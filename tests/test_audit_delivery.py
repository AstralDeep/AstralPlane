"""Restart, sink-failure, and acknowledgement tests for audit delivery."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from astralplane.audit_delivery import (
    AUDIT_OUTBOX_TOPIC,
    AuditDeliveryState,
    AuditOutboxDelivery,
    canonical_audit_payload,
)
from astralplane.contracts import ClaimedOutboxEntry, OutboxEntry, ReclaimedOutboxEntry
from astralplane.errors import PlaneError, SQLContractError

NOW = datetime(2026, 8, 13, 21, tzinfo=UTC)


@dataclass(frozen=True)
class FakeResult:
    rowcount: int
    status_message: str | None = None
    returned_records: tuple[Mapping[str, Any], ...] = ()


class MemoryTransaction:
    def __init__(self, rows: dict[str, dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, *args: object, **kwargs: object) -> FakeResult:
        raise AssertionError((args, kwargs))

    def fetch_one(self, *args: object, **kwargs: object) -> None:
        raise AssertionError((args, kwargs))

    def fetch_all(self, *args: object, **kwargs: object) -> tuple[()]:
        raise AssertionError((args, kwargs))


class MemoryDatabase:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.fail_next_commit = False

    @contextmanager
    def transaction(self, **_: object) -> Iterator[MemoryTransaction]:
        working = copy.deepcopy(self.rows)
        yield MemoryTransaction(working)
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise RuntimeError("simulated commit failure")
        self.rows = working


class MemoryOutbox:
    def enqueue(self, transaction: MemoryTransaction, entry: OutboxEntry) -> FakeResult:
        existing = next(
            (
                row
                for row in transaction.rows.values()
                if row["entry"].idempotency_key == entry.idempotency_key
            ),
            None,
        )
        if existing is not None:
            if existing["entry"] != entry:
                raise PlaneError("different work", code="outbox_idempotency_conflict")
            return FakeResult(0)
        transaction.rows[entry.entry_id] = {
            "entry": entry,
            "status": "pending",
            "attempt": 0,
            "version": 0,
            "worker": None,
            "lease_expires_at": None,
            "error_code": None,
            "updated_at": entry.available_at,
        }
        return FakeResult(1)

    def claim(
        self,
        transaction: MemoryTransaction,
        *,
        worker_id: str,
        topics: tuple[str, ...],
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[ClaimedOutboxEntry, ...]:
        available = sorted(
            (
                row
                for row in transaction.rows.values()
                if row["status"] in {"pending", "retry"}
                and row["entry"].topic in topics
                and row["entry"].available_at <= now
            ),
            key=lambda row: (row["entry"].available_at, row["entry"].entry_id),
        )[:limit]
        claims = []
        for row in available:
            row.update(
                status="claimed",
                attempt=row["attempt"] + 1,
                version=row["version"] + 1,
                worker=worker_id,
                lease_expires_at=now + lease_duration,
                updated_at=now,
            )
            claims.append(
                ClaimedOutboxEntry(
                    entry=row["entry"],
                    worker_id=worker_id,
                    lease_expires_at=row["lease_expires_at"],
                    expected_version=row["version"],
                    attempt=row["attempt"],
                )
            )
        return tuple(claims)

    def ack(
        self,
        transaction: MemoryTransaction,
        *,
        entry_id: str,
        worker_id: str,
        expected_version: int,
        now: datetime,
    ) -> FakeResult:
        row = transaction.rows[entry_id]
        if not self._matches(row, worker_id, expected_version, now):
            return FakeResult(0)
        row.update(
            status="succeeded",
            worker=None,
            lease_expires_at=None,
            version=row["version"] + 1,
            updated_at=now,
        )
        return FakeResult(1)

    def retry(
        self,
        transaction: MemoryTransaction,
        *,
        entry_id: str,
        worker_id: str,
        expected_version: int,
        available_at: datetime,
        error_code: str,
        now: datetime,
    ) -> FakeResult:
        row = transaction.rows[entry_id]
        if not self._matches(row, worker_id, expected_version, now):
            return FakeResult(0)
        row["entry"] = OutboxEntry(
            entry_id=row["entry"].entry_id,
            topic=row["entry"].topic,
            canonical_payload=row["entry"].canonical_payload,
            payload_sha256=row["entry"].payload_sha256,
            idempotency_key=row["entry"].idempotency_key,
            available_at=available_at,
        )
        row.update(
            status="retry",
            worker=None,
            lease_expires_at=None,
            version=row["version"] + 1,
            error_code=error_code,
            updated_at=now,
        )
        return FakeResult(1)

    def dead_letter(
        self,
        transaction: MemoryTransaction,
        *,
        entry_id: str,
        worker_id: str,
        expected_version: int,
        error_code: str,
        now: datetime,
    ) -> FakeResult:
        row = transaction.rows[entry_id]
        if not self._matches(row, worker_id, expected_version, now):
            return FakeResult(0)
        row.update(
            status="dead_letter",
            worker=None,
            lease_expires_at=None,
            version=row["version"] + 1,
            error_code=error_code,
            updated_at=now,
        )
        return FakeResult(1)

    def reclaim_expired(
        self,
        transaction: MemoryTransaction,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[ReclaimedOutboxEntry, ...]:
        records = []
        for row in list(transaction.rows.values())[:limit]:
            if row["status"] != "claimed" or row["lease_expires_at"] > now:
                continue
            worker = row["worker"]
            row["entry"] = OutboxEntry(
                entry_id=row["entry"].entry_id,
                topic=row["entry"].topic,
                canonical_payload=row["entry"].canonical_payload,
                payload_sha256=row["entry"].payload_sha256,
                idempotency_key=row["entry"].idempotency_key,
                available_at=now,
            )
            row.update(
                status="retry",
                worker=None,
                lease_expires_at=None,
                version=row["version"] + 1,
                updated_at=now,
            )
            records.append(
                ReclaimedOutboxEntry(
                    entry_id=row["entry"].entry_id,
                    previous_worker_id=worker,
                    expected_version=row["version"],
                    available_at=now,
                )
            )
        return tuple(records)

    @staticmethod
    def _matches(
        row: dict[str, Any],
        worker_id: str,
        expected_version: int,
        now: datetime,
    ) -> bool:
        return bool(
            row["status"] == "claimed"
            and row["worker"] == worker_id
            and row["version"] == expected_version
            and row["lease_expires_at"] > now
        )


class RecordingSink:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.deliveries: list[tuple[str, bytes, str]] = []

    def publish(
        self,
        *,
        event_id: str,
        canonical_payload: bytes,
        idempotency_key: str,
    ) -> None:
        self.deliveries.append((event_id, canonical_payload, idempotency_key))
        if self.failures:
            self.failures -= 1
            raise RuntimeError("credential-bearing sink exception must never be persisted")


def service(
    database: MemoryDatabase,
    outbox: MemoryOutbox,
    sink: RecordingSink,
    **kwargs: object,
) -> AuditOutboxDelivery:
    return AuditOutboxDelivery(
        database=database,
        outbox=outbox,
        sink=sink,
        **kwargs,
    )


def queue_event(delivery: AuditOutboxDelivery, database: MemoryDatabase) -> None:
    with database.transaction() as transaction:
        delivery.enqueue(
            transaction,
            event_id="event-1",
            event={"sequence": 7, "action": "created", "nested": {"token_count": 12}},
            available_at=NOW,
        )


def test_canonical_payload_is_stable_and_rejects_credential_fields() -> None:
    assert canonical_audit_payload({"z": 1, "a": [True, None]}) == b'{"a":[true,null],"z":1}'
    assert canonical_audit_payload({"finite": 1.5}) == b'{"finite":1.5}'
    for event in (
        {"password": "do-not-store"},
        {"nested": {"access-token": "do-not-store"}},
        {"private_key": "do-not-store"},
    ):
        with pytest.raises(SQLContractError, match="credential-bearing"):
            canonical_audit_payload(event)
    with pytest.raises(SQLContractError, match="canonical-JSON"):
        canonical_audit_payload({"bad": float("inf")})
    with pytest.raises(SQLContractError, match="keys"):
        canonical_audit_payload({1: "bad"})  # type: ignore[dict-item]
    with pytest.raises(SQLContractError, match="must be a mapping"):
        canonical_audit_payload([])  # type: ignore[arg-type]


def test_enqueue_is_transactional_idempotent_and_contains_no_sink_configuration() -> None:
    database, outbox, sink = MemoryDatabase(), MemoryOutbox(), RecordingSink()
    delivery = service(database, outbox, sink)
    queue_event(delivery, database)
    queue_event(delivery, database)

    assert list(database.rows) == ["audit:event-1"]
    persisted = database.rows["audit:event-1"]
    assert persisted["entry"].topic == AUDIT_OUTBOX_TOPIC
    assert b"token_count" in persisted["entry"].canonical_payload
    assert "RecordingSink" not in repr(persisted)
    assert "credential" not in repr(persisted)


def test_unavailable_sink_schedules_retry_and_a_new_instance_finishes_it() -> None:
    database, outbox = MemoryDatabase(), MemoryOutbox()
    first_sink = RecordingSink(failures=1)
    first_process = service(
        database,
        outbox,
        first_sink,
        base_retry_delay=timedelta(seconds=10),
    )
    queue_event(first_process, database)

    result = first_process.deliver_one(
        worker_id="worker-1",
        now=NOW,
        lease_duration=timedelta(seconds=30),
    )
    assert result.state is AuditDeliveryState.RETRY_SCHEDULED
    assert result.retry_available_at == NOW + timedelta(seconds=10)
    assert database.rows["audit:event-1"]["status"] == "retry"
    assert database.rows["audit:event-1"]["error_code"] == "audit_sink_unavailable"
    assert "credential-bearing" not in repr(database.rows)

    restarted = service(database, outbox, RecordingSink())
    assert (
        restarted.deliver_one(
            worker_id="worker-2",
            now=NOW + timedelta(seconds=9),
            lease_duration=timedelta(seconds=30),
        ).state
        is AuditDeliveryState.NO_WORK
    )
    delivered = restarted.deliver_one(
        worker_id="worker-2",
        now=NOW + timedelta(seconds=10),
        lease_duration=timedelta(seconds=30),
    )
    assert delivered.state is AuditDeliveryState.DELIVERED
    assert database.rows["audit:event-1"]["status"] == "succeeded"


def test_sink_success_is_not_reported_delivered_before_ack_commit() -> None:
    database, outbox, sink = MemoryDatabase(), MemoryOutbox(), RecordingSink()
    delivery = service(database, outbox, sink)
    queue_event(delivery, database)
    claim = delivery.claim(
        worker_id="worker-1",
        now=NOW,
        lease_duration=timedelta(seconds=10),
        limit=1,
    )[0]
    database.fail_next_commit = True

    with pytest.raises(RuntimeError, match="commit failure"):
        delivery.deliver(claim, now=NOW)
    assert len(sink.deliveries) == 1
    assert database.rows["audit:event-1"]["status"] == "claimed"

    with database.transaction() as transaction:
        outbox.reclaim_expired(transaction, now=NOW + timedelta(seconds=10), limit=1)
    restarted = service(database, outbox, sink)
    result = restarted.deliver_one(
        worker_id="worker-2",
        now=NOW + timedelta(seconds=10),
        lease_duration=timedelta(seconds=10),
    )
    assert result.state is AuditDeliveryState.DELIVERED
    assert len(sink.deliveries) == 2
    assert sink.deliveries[0][2] == sink.deliveries[1][2] == "audit:event-1"


def test_exhausted_sink_failure_is_dead_lettered_after_commit() -> None:
    database, outbox, sink = MemoryDatabase(), MemoryOutbox(), RecordingSink(failures=1)
    delivery = service(database, outbox, sink, max_attempts=1)
    queue_event(delivery, database)
    result = delivery.deliver_one(
        worker_id="worker-1",
        now=NOW,
        lease_duration=timedelta(seconds=30),
    )
    assert result.state is AuditDeliveryState.DEAD_LETTERED
    assert database.rows["audit:event-1"]["status"] == "dead_letter"


def test_fence_failure_and_corrupt_or_wrong_topic_claims_fail_closed() -> None:
    database, outbox, sink = MemoryDatabase(), MemoryOutbox(), RecordingSink()
    delivery = service(database, outbox, sink)
    queue_event(delivery, database)
    claim = delivery.claim(
        worker_id="worker-1",
        now=NOW,
        lease_duration=timedelta(seconds=30),
        limit=1,
    )[0]
    stale = ClaimedOutboxEntry(
        entry=claim.entry,
        worker_id=claim.worker_id,
        lease_expires_at=claim.lease_expires_at,
        expected_version=claim.expected_version + 1,
        attempt=claim.attempt,
    )
    with pytest.raises(PlaneError) as raised:
        delivery.deliver(stale, now=NOW)
    assert raised.value.code == "audit_delivery_fence_conflict"

    wrong_topic = ClaimedOutboxEntry(
        entry=OutboxEntry(
            entry_id="purge:1",
            topic="purge.execute",
            canonical_payload=b"{}",
            payload_sha256="0" * 64,
            idempotency_key="purge:1",
            available_at=NOW,
        ),
        worker_id="worker",
        lease_expires_at=NOW,
        expected_version=1,
        attempt=1,
    )
    with pytest.raises(SQLContractError, match="not an audit"):
        delivery.deliver(wrong_topic, now=NOW)

    corrupt = ClaimedOutboxEntry(
        entry=OutboxEntry(
            entry_id=claim.entry.entry_id,
            topic=claim.entry.topic,
            canonical_payload=claim.entry.canonical_payload + b" ",
            payload_sha256=claim.entry.payload_sha256,
            idempotency_key=claim.entry.idempotency_key,
            available_at=claim.entry.available_at,
        ),
        worker_id=claim.worker_id,
        lease_expires_at=claim.lease_expires_at,
        expected_version=claim.expected_version,
        attempt=claim.attempt,
    )
    with pytest.raises(PlaneError) as raised:
        delivery.deliver(corrupt, now=NOW)
    assert raised.value.code == "audit_payload_corrupt"

    malformed_payload = b"not-json"
    malformed = ClaimedOutboxEntry(
        entry=OutboxEntry(
            entry_id=claim.entry.entry_id,
            topic=claim.entry.topic,
            canonical_payload=malformed_payload,
            payload_sha256=hashlib.sha256(malformed_payload).hexdigest(),
            idempotency_key=claim.entry.idempotency_key,
            available_at=claim.entry.available_at,
        ),
        worker_id=claim.worker_id,
        lease_expires_at=claim.lease_expires_at,
        expected_version=claim.expected_version,
        attempt=claim.attempt,
    )
    with pytest.raises(PlaneError) as raised:
        delivery.deliver(malformed, now=NOW)
    assert raised.value.code == "audit_payload_corrupt"


def test_corrupt_payload_validation_rolls_back_the_claim_transaction() -> None:
    database, outbox, sink = MemoryDatabase(), MemoryOutbox(), RecordingSink()
    delivery = service(database, outbox, sink)
    queue_event(delivery, database)
    original = database.rows["audit:event-1"]["entry"]
    malformed_payload = b"not-json"
    database.rows["audit:event-1"]["entry"] = OutboxEntry(
        entry_id=original.entry_id,
        topic=original.topic,
        canonical_payload=malformed_payload,
        payload_sha256=hashlib.sha256(malformed_payload).hexdigest(),
        idempotency_key=original.idempotency_key,
        available_at=original.available_at,
    )

    with pytest.raises(PlaneError) as raised:
        delivery.claim(
            worker_id="worker-1",
            now=NOW,
            lease_duration=timedelta(seconds=30),
            limit=1,
        )
    assert raised.value.code == "audit_payload_corrupt"
    assert database.rows["audit:event-1"]["status"] == "pending"
    assert database.rows["audit:event-1"]["attempt"] == 0
    assert database.rows["audit:event-1"]["worker"] is None
    assert sink.deliveries == []


def test_expired_claim_is_rejected_before_the_sink_side_effect() -> None:
    database, outbox, sink = MemoryDatabase(), MemoryOutbox(), RecordingSink()
    delivery = service(database, outbox, sink)
    queue_event(delivery, database)
    claim = delivery.claim(
        worker_id="worker-1",
        now=NOW,
        lease_duration=timedelta(seconds=1),
        limit=1,
    )[0]

    with pytest.raises(PlaneError) as raised:
        delivery.deliver(claim, now=claim.lease_expires_at)
    assert raised.value.code == "audit_lease_expired"
    assert sink.deliveries == []
    assert database.rows["audit:event-1"]["status"] == "claimed"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_retry_delay": timedelta(0)},
        {"base_retry_delay": timedelta(seconds=5), "max_retry_delay": timedelta(seconds=4)},
        {"sink": object()},
    ],
)
def test_configuration_rejects_unsafe_delivery_settings(kwargs: dict[str, object]) -> None:
    arguments: dict[str, object] = {
        "database": MemoryDatabase(),
        "outbox": MemoryOutbox(),
        "sink": RecordingSink(),
    }
    arguments.update(kwargs)
    with pytest.raises(SQLContractError):
        AuditOutboxDelivery(**arguments)  # type: ignore[arg-type]


def test_enqueue_and_delivery_require_bounded_ids_and_aware_timestamps() -> None:
    database, outbox = MemoryDatabase(), MemoryOutbox()
    delivery = service(database, outbox, RecordingSink())
    with database.transaction() as transaction, pytest.raises(SQLContractError):
        delivery.enqueue(
            transaction,
            event_id="bad event",
            event={"action": "created"},
            available_at=NOW,
        )
    with pytest.raises(SQLContractError, match="timezone-aware"):
        delivery.deliver_one(
            worker_id="worker",
            now=datetime(2026, 8, 13),
            lease_duration=timedelta(seconds=1),
        )
