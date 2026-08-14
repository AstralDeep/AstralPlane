from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest
from _support import Result, ScriptedTransaction

from astralplane.errors import PlaneError
from astralplane.repositories.scheduler import (
    EffectState,
    OccurrenceState,
    OperationState,
    OperationSubmission,
    ScheduledJob,
    SchedulerRepository,
)

NOW = datetime(2026, 8, 13, 20, tzinfo=UTC)
OPERATION_ID = "11111111-1111-4111-8111-111111111111"
SUBMISSION_ID = "22222222-2222-5222-8222-222222222222"
JOB_ID = "33333333-3333-4333-8333-333333333333"
OCCURRENCE_ID = "44444444-4444-4444-8444-444444444444"
LEASE_TOKEN = "55555555-5555-4555-8555-555555555555"
OPERATION_LEASE_TOKEN = "66666666-6666-4666-8666-666666666666"
OTHER_OCCURRENCE_ID = "77777777-7777-4777-8777-777777777777"
OTHER_JOB_ID = "88888888-8888-4888-8888-888888888888"


def admission_classes() -> tuple[dict[str, object], ...]:
    return (
        {"class_name": "global", "parent_class_name": None, "active_limit": 20},
        {"class_name": "scheduled", "parent_class_name": "global", "active_limit": 5},
    )


def admission_config(**overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "class_name": "scheduled",
        "queue_limit": 100,
        "max_wait_ms": 60_000,
    }
    config.update(overrides)
    return config


def admission_slots() -> tuple[dict[str, object], ...]:
    return (
        {"class_name": "global", "slot_number": 1},
        {"class_name": "scheduled", "slot_number": 1},
    )


def operation_identity() -> dict[str, object]:
    return {"admission_class": "scheduled"}


def operation_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "operation_id": OPERATION_ID,
        "owner_user_id": "owner-1",
        "operation_kind": "scheduled_occurrence",
        "admission_class": "scheduled",
        "normalized_input_digest": "a" * 64,
        "state": "queued",
        "execution_generation": 0,
        "execution_lease_token": None,
        "state_revision": 0,
        "terminal_code": None,
        "accepted_at": NOW,
        "updated_at": NOW,
    }
    row.update(overrides)
    return row


def submission(**overrides: object) -> OperationSubmission:
    values: dict[str, object] = {
        "operation_id": OPERATION_ID,
        "submission_id": SUBMISSION_ID,
        "owner_id": "owner-1",
        "owner_scope": "schedule",
        "operation_kind": "scheduled_occurrence",
        "admission_class": "scheduled",
        "idempotency_namespace": "scheduled_occurrence_attempt",
        "idempotency_key": "job-1:2026-08-13",
        "input_digest": "a" * 64,
        "accepted_at": NOW,
        "queue_deadline_at": NOW + timedelta(minutes=1),
    }
    values.update(overrides)
    return OperationSubmission(**values)  # type: ignore[arg-type]


def job_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": JOB_ID,
        "user_id": "owner-1",
        "name": "Morning review",
        "instruction": "Summarize the inbox",
        "schedule_kind": "cron",
        "schedule_expr": "0 9 * * *",
        "timezone": "UTC",
        "status": "active",
        "next_run_at": 10,
        "created_at": 1,
        "updated_at": 2,
    }
    row.update(overrides)
    return row


def job(**overrides: object) -> ScheduledJob:
    values = {
        "job_id": JOB_ID,
        "owner_id": "owner-1",
        "name": "Morning review",
        "instruction": "Summarize the inbox",
        "schedule_kind": "cron",
        "schedule_expression": "0 9 * * *",
        "timezone": "UTC",
        "status": "active",
        "next_run_at": 10,
        "created_at": 1,
        "updated_at": 2,
    }
    values.update(overrides)
    return ScheduledJob(**values)  # type: ignore[arg-type]


def occurrence_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "occurrence_id": OCCURRENCE_ID,
        "job_id": JOB_ID,
        "owner_user_id": "owner-1",
        "scheduled_for": NOW,
        "state": "pending",
        "claim_generation": 0,
        "lease_token": None,
        "lease_owner": None,
        "lease_expires_at": None,
        "attempt_count": 0,
        "current_operation_id": None,
        "operation_execution_generation": None,
    }
    row.update(overrides)
    return row


def effect_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "occurrence_id": OCCURRENCE_ID,
        "effect_kind": "chat_publication",
        "effect_key": "message-1",
        "payload_digest": "b" * 64,
        "state": "reserved",
        "operation_id": OPERATION_ID,
        "operation_execution_generation": 2,
        "occurrence_claim_generation": 3,
        "downstream_receipt_digest": None,
        "published_at": None,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"operation_id": ""}, "operation_id"),
        ({"operation_id": SUBMISSION_ID}, "non-nil v4"),
        ({"owner_scope": "connection"}, "owner_scope"),
        ({"operation_kind": "Bad.Kind"}, "operation_kind"),
        ({"operation_kind": "a" * 65}, "operation_kind"),
        ({"admission_class": ""}, "admission_class"),
        ({"input_digest": "A" * 64}, "input_digest"),
        ({"accepted_at": NOW.replace(tzinfo=None)}, "accepted_at"),
        ({"queue_deadline_at": NOW - timedelta(seconds=1)}, "cannot precede"),
    ],
)
def test_submission_validation(changes: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        submission(**changes)


def test_records_are_immutable() -> None:
    value = submission()
    with pytest.raises(FrozenInstanceError):
        value.owner_id = "other"  # type: ignore[misc]


def test_submit_is_idempotent_and_owner_scoped() -> None:
    repository = SchedulerRepository()
    transaction = ScriptedTransaction(
        one=[admission_config(), None, operation_row(), {"submission_result_id": "x"}]
    )

    result = repository.submit_operation(transaction, submission())

    assert result.state is OperationState.QUEUED
    assert result.owner_id == "owner-1"
    assert "owner_user_id" in transaction.fetch_sql()
    assert transaction.calls[2][2][3] == "owner-1"  # type: ignore[index]
    assert "config.queue_limit" in transaction.fetch_sql()

    replay = ScriptedTransaction(one=[admission_config(), operation_row(), None])
    assert repository.submit_operation(replay, submission()) == result


def test_submit_rejects_conflicting_replay() -> None:
    transaction = ScriptedTransaction(
        one=[admission_config(), operation_row(normalized_input_digest="c" * 64)]
    )
    with pytest.raises(PlaneError) as raised:
        SchedulerRepository().submit_operation(transaction, submission())
    assert raised.value.code == "operation_idempotency_conflict"
    assert raised.value.metadata == (("owner_id", "owner-1"),)


def test_submit_fails_closed_for_missing_or_exhausted_queue_capacity() -> None:
    repository = SchedulerRepository()
    with pytest.raises(PlaneError) as raised:
        repository.submit_operation(ScriptedTransaction(one=[None]), submission())
    assert raised.value.code == "operation_admission_configuration_invalid"

    with pytest.raises(PlaneError) as raised:
        repository.submit_operation(
            ScriptedTransaction(one=[admission_config(queue_limit=0), None]), submission()
        )
    assert raised.value.code == "operation_capacity_unavailable"

    with pytest.raises(PlaneError) as raised:
        repository.submit_operation(
            ScriptedTransaction(one=[admission_config(), None, None]), submission()
        )
    assert raised.value.code == "operation_capacity_unavailable"


def test_claim_and_terminalize_preserve_fences() -> None:
    repository = SchedulerRepository()
    running = operation_row(
        state="running",
        execution_generation=1,
        execution_lease_token=OPERATION_LEASE_TOKEN,
        state_revision=1,
    )
    transaction = ScriptedTransaction(
        one=[operation_identity(), operation_row(), running],
        all_rows=[admission_classes(), admission_slots()],
    )
    claim = repository.claim_operation(
        transaction,
        owner_id="owner-1",
        operation_id=OPERATION_ID,
        expected_revision=0,
        lease_token=OPERATION_LEASE_TOKEN,
        now=NOW,
        lease_expires_at=NOW + timedelta(minutes=1),
    )
    assert claim.execution_lease_token == OPERATION_LEASE_TOKEN
    assert "owner_user_id = %s" in transaction.fetch_sql()
    assert "FOR UPDATE OF operation" in transaction.fetch_sql()
    assert "lease_expires_at = %s" in transaction.fetch_sql()
    assert [call[0] for call in transaction.calls].count("execute") == 2

    completed = operation_row(
        state="completed",
        execution_generation=1,
        execution_lease_token=None,
        state_revision=2,
    )
    terminal = ScriptedTransaction(one=[completed])
    assert (
        repository.terminalize_operation(
            terminal,
            owner_id="owner-1",
            operation_id=OPERATION_ID,
            execution_generation=1,
            lease_token=OPERATION_LEASE_TOKEN,
            state=OperationState.COMPLETED,
            terminal_code=None,
            safe_summary="done",
            retry_after_ms=None,
            now=NOW,
            purge_after=NOW + timedelta(days=1),
        ).state
        is OperationState.COMPLETED
    )
    assert "operation_admission_slot" in terminal.fetch_sql()


def test_claim_reclaims_only_a_complete_expired_operation_slot_chain() -> None:
    repository = SchedulerRepository()
    candidate = operation_row(
        state="running",
        execution_generation=1,
        execution_lease_token=LEASE_TOKEN,
        state_revision=1,
    )
    reclaimed = operation_row(
        state="running",
        execution_generation=2,
        execution_lease_token=OPERATION_LEASE_TOKEN,
        state_revision=2,
    )
    transaction = ScriptedTransaction(
        one=[operation_identity(), candidate, reclaimed],
        all_rows=[admission_classes(), admission_slots()],
    )

    result = repository.claim_operation(
        transaction,
        owner_id="owner-1",
        operation_id=OPERATION_ID,
        expected_revision=1,
        lease_token=OPERATION_LEASE_TOKEN,
        now=NOW,
        lease_expires_at=NOW + timedelta(minutes=1),
    )

    assert result.execution_generation == 2
    assert "expired.lease_expires_at <= %s" in transaction.fetch_sql()
    assert "operation_id = %s AND lease_expires_at <= %s" in transaction.fetch_sql()


def test_claim_refuses_incomplete_capacity_and_lost_slot_fence() -> None:
    repository = SchedulerRepository()
    missing = ScriptedTransaction(
        one=[operation_identity(), operation_row()],
        all_rows=[admission_classes(), admission_slots()[:1]],
    )
    with pytest.raises(PlaneError) as raised:
        repository.claim_operation(
            missing,
            owner_id="owner-1",
            operation_id=OPERATION_ID,
            expected_revision=0,
            lease_token=OPERATION_LEASE_TOKEN,
            now=NOW,
            lease_expires_at=NOW + timedelta(minutes=1),
        )
    assert raised.value.code == "operation_capacity_unavailable"

    lost = ScriptedTransaction(
        one=[operation_identity(), operation_row()],
        all_rows=[admission_classes(), admission_slots()],
        execute=[Result(rowcount=0)],
    )
    with pytest.raises(PlaneError) as raised:
        repository.claim_operation(
            lost,
            owner_id="owner-1",
            operation_id=OPERATION_ID,
            expected_revision=0,
            lease_token=OPERATION_LEASE_TOKEN,
            now=NOW,
            lease_expires_at=NOW + timedelta(minutes=1),
        )
    assert raised.value.code == "operation_capacity_fence_lost"


def test_terminalize_refuses_a_missing_capacity_lease() -> None:
    completed = operation_row(
        state="completed",
        execution_generation=1,
        execution_lease_token=None,
        state_revision=2,
    )
    transaction = ScriptedTransaction(one=[completed], execute=[Result(rowcount=0)])

    with pytest.raises(PlaneError) as raised:
        SchedulerRepository().terminalize_operation(
            transaction,
            owner_id="owner-1",
            operation_id=OPERATION_ID,
            execution_generation=1,
            lease_token=OPERATION_LEASE_TOKEN,
            state=OperationState.COMPLETED,
            terminal_code=None,
            safe_summary="done",
            retry_after_ms=None,
            now=NOW,
            purge_after=NOW + timedelta(days=1),
        )
    assert raised.value.code == "operation_capacity_lease_missing"


@pytest.mark.parametrize(
    "call",
    [
        lambda repo: repo.claim_operation(
            ScriptedTransaction(),
            owner_id="o",
            operation_id=OPERATION_ID,
            expected_revision=-1,
            lease_token=OPERATION_LEASE_TOKEN,
            now=NOW,
            lease_expires_at=NOW,
        ),
        lambda repo: repo.terminalize_operation(
            ScriptedTransaction(),
            owner_id="o",
            operation_id=OPERATION_ID,
            execution_generation=1,
            lease_token=OPERATION_LEASE_TOKEN,
            state=OperationState.RUNNING,
            terminal_code=None,
            safe_summary=None,
            retry_after_ms=None,
            now=NOW,
            purge_after=NOW,
        ),
        lambda repo: repo.terminalize_operation(
            ScriptedTransaction(),
            owner_id="o",
            operation_id=OPERATION_ID,
            execution_generation=1,
            lease_token=OPERATION_LEASE_TOKEN,
            state=OperationState.RETRYABLE,
            terminal_code="retry",
            safe_summary=None,
            retry_after_ms=-1,
            now=NOW,
            purge_after=NOW,
        ),
    ],
)
def test_operation_transition_validation(call: object) -> None:
    with pytest.raises(ValueError):
        call(SchedulerRepository())  # type: ignore[operator]


def test_stale_operation_fences_are_visible() -> None:
    repository = SchedulerRepository()
    with pytest.raises(PlaneError, match="claim") as raised:
        repository.claim_operation(
            ScriptedTransaction(one=[None]),
            owner_id="owner-1",
            operation_id=OPERATION_ID,
            expected_revision=0,
            lease_token=OPERATION_LEASE_TOKEN,
            now=NOW,
            lease_expires_at=NOW + timedelta(seconds=1),
        )
    assert raised.value.code == "stale_operation_fence"


def test_scheduled_job_crud_and_due_ordering() -> None:
    repository = SchedulerRepository()
    value = job()
    assert repository.put_job(ScriptedTransaction(one=[job_row()]), value) == value
    assert repository.put_job(ScriptedTransaction(one=[None, job_row()]), value) == value
    assert (
        repository.get_job(ScriptedTransaction(one=[job_row()]), owner_id="owner-1", job_id=JOB_ID)
        == value
    )
    assert (
        repository.get_job(ScriptedTransaction(one=[None]), owner_id="other", job_id=JOB_ID) is None
    )
    tx = ScriptedTransaction(all_rows=[(job_row(),)])
    assert repository.list_due_jobs(tx, owner_id="owner-1", due_at_ms=10, limit=1) == (value,)
    assert tx.calls[0][2][0] == "owner-1"  # type: ignore[index]
    with pytest.raises(ValueError):
        repository.list_due_jobs(ScriptedTransaction(), owner_id="owner-1", due_at_ms=1, limit=0)


@pytest.mark.parametrize(
    "changes",
    [
        {"job_id": SUBMISSION_ID},
        {"name": ""},
        {"schedule_kind": "solar"},
        {"status": "deleted"},
        {"created_at": 3, "updated_at": 2},
    ],
)
def test_job_validation(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        job(**changes)


def test_job_conflict_is_visible() -> None:
    with pytest.raises(PlaneError) as raised:
        SchedulerRepository().put_job(
            ScriptedTransaction(one=[None, job_row(name="Different")]), job()
        )
    assert raised.value.code == "scheduled_job_conflict"


def test_occurrence_creation_replay_claim_and_failures() -> None:
    repository = SchedulerRepository()
    created = repository.create_occurrence(
        ScriptedTransaction(one=[occurrence_row()]),
        occurrence_id=OCCURRENCE_ID,
        job_id=JOB_ID,
        owner_id="owner-1",
        scheduled_for=NOW,
    )
    assert created.state is OccurrenceState.PENDING
    replay = repository.create_occurrence(
        ScriptedTransaction(one=[None, occurrence_row()]),
        occurrence_id=OTHER_OCCURRENCE_ID,
        job_id=JOB_ID,
        owner_id="owner-1",
        scheduled_for=NOW,
    )
    assert replay.occurrence_id == OCCURRENCE_ID
    claimed_row = occurrence_row(
        state="claimed",
        claim_generation=1,
        lease_token=LEASE_TOKEN,
        lease_owner="worker",
        lease_expires_at=NOW + timedelta(minutes=1),
        attempt_count=1,
    )
    claimed_transaction = ScriptedTransaction(one=[claimed_row])
    claimed = repository.claim_occurrence(
        claimed_transaction,
        owner_id="owner-1",
        occurrence_id=OCCURRENCE_ID,
        worker_id="worker",
        lease_token=LEASE_TOKEN,
        now=NOW,
        lease_expires_at=NOW + timedelta(minutes=1),
    )
    assert claimed.claim_generation == 1
    assert "state IN ('claimed', 'running')" in claimed_transaction.fetch_sql()
    assert "operation.state IN ('queued', 'running')" in claimed_transaction.fetch_sql()
    with pytest.raises(ValueError):
        repository.claim_occurrence(
            ScriptedTransaction(),
            owner_id="owner-1",
            occurrence_id=OCCURRENCE_ID,
            worker_id="worker",
            lease_token=LEASE_TOKEN,
            now=NOW,
            lease_expires_at=NOW,
        )
    with pytest.raises(PlaneError) as raised:
        repository.claim_occurrence(
            ScriptedTransaction(one=[None]),
            owner_id="owner-1",
            occurrence_id=OCCURRENCE_ID,
            worker_id="worker",
            lease_token=LEASE_TOKEN,
            now=NOW,
            lease_expires_at=NOW + timedelta(seconds=1),
        )
    assert raised.value.code == "stale_occurrence_fence"
    with pytest.raises(PlaneError):
        repository.create_occurrence(
            ScriptedTransaction(one=[None, None]),
            occurrence_id=OCCURRENCE_ID,
            job_id=OTHER_JOB_ID,
            owner_id="owner-1",
            scheduled_for=NOW,
        )


def test_bind_occurrence_operation_requires_both_live_execution_fences() -> None:
    bound_row = occurrence_row(
        state="running",
        claim_generation=3,
        lease_token=LEASE_TOKEN,
        lease_owner="worker",
        lease_expires_at=NOW + timedelta(minutes=1),
        attempt_count=3,
        current_operation_id=OPERATION_ID,
        operation_execution_generation=2,
    )
    transaction = ScriptedTransaction(one=[bound_row])

    result = SchedulerRepository().bind_occurrence_operation(
        transaction,
        owner_id="owner-1",
        occurrence_id=OCCURRENCE_ID,
        occurrence_claim_generation=3,
        occurrence_lease_token=LEASE_TOKEN,
        operation_id=OPERATION_ID,
        operation_execution_generation=2,
        operation_lease_token=OPERATION_LEASE_TOKEN,
        now=NOW,
    )

    assert result.state is OccurrenceState.RUNNING
    assert result.operation_id == OPERATION_ID
    sql = transaction.fetch_sql()
    assert "operation.execution_lease_token = %s" in sql
    assert "occurrence.lease_expires_at > %s" in sql
    assert "operation.owner_scope = 'schedule'" in sql

    with pytest.raises(PlaneError) as raised:
        SchedulerRepository().bind_occurrence_operation(
            ScriptedTransaction(one=[None]),
            owner_id="owner-1",
            occurrence_id=OCCURRENCE_ID,
            occurrence_claim_generation=3,
            occurrence_lease_token=LEASE_TOKEN,
            operation_id=OPERATION_ID,
            operation_execution_generation=2,
            operation_lease_token=OPERATION_LEASE_TOKEN,
            now=NOW,
        )
    assert raised.value.code == "stale_occurrence_fence"

    with pytest.raises(ValueError, match="positive execution generations"):
        SchedulerRepository().bind_occurrence_operation(
            ScriptedTransaction(),
            owner_id="owner-1",
            occurrence_id=OCCURRENCE_ID,
            occurrence_claim_generation=0,
            occurrence_lease_token=LEASE_TOKEN,
            operation_id=OPERATION_ID,
            operation_execution_generation=2,
            operation_lease_token=OPERATION_LEASE_TOKEN,
            now=NOW,
        )


def test_effect_reservation_and_publication_are_idempotent_and_fenced() -> None:
    repository = SchedulerRepository()
    kwargs = {
        "owner_id": "owner-1",
        "occurrence_id": OCCURRENCE_ID,
        "effect_kind": "chat_publication",
        "effect_key": "message-1",
        "payload_digest": "b" * 64,
        "operation_id": OPERATION_ID,
        "operation_execution_generation": 2,
        "occurrence_claim_generation": 3,
    }
    reserved = repository.reserve_effect(ScriptedTransaction(one=[effect_row()]), **kwargs)
    assert reserved.state is EffectState.RESERVED
    assert (
        repository.reserve_effect(ScriptedTransaction(one=[None, effect_row()]), **kwargs)
        == reserved
    )

    published_row = effect_row(
        state="published", downstream_receipt_digest="c" * 64, published_at=NOW
    )
    publish = dict(kwargs)
    publish.update(receipt_digest="c" * 64, published_at=NOW)
    result = repository.publish_effect(ScriptedTransaction(one=[published_row]), **publish)
    assert result.state is EffectState.PUBLISHED
    assert (
        repository.publish_effect(ScriptedTransaction(one=[None, published_row]), **publish)
        == result
    )


def test_effect_conflicts_and_validation_are_visible() -> None:
    repository = SchedulerRepository()
    base = {
        "owner_id": "owner-1",
        "occurrence_id": OCCURRENCE_ID,
        "effect_kind": "chat_publication",
        "effect_key": "message-1",
        "payload_digest": "b" * 64,
        "operation_id": OPERATION_ID,
        "operation_execution_generation": 2,
        "occurrence_claim_generation": 3,
    }
    with pytest.raises(PlaneError) as raised:
        repository.reserve_effect(
            ScriptedTransaction(one=[None, effect_row(payload_digest="d" * 64)]), **base
        )
    assert raised.value.code == "effect_reservation_conflict"
    with pytest.raises(ValueError, match="effect_kind"):
        repository.reserve_effect(
            ScriptedTransaction(),
            **{**base, "effect_kind": "a" * 65},
        )
    with pytest.raises(ValueError):
        repository.reserve_effect(
            ScriptedTransaction(),
            **{**base, "operation_execution_generation": 0},
        )
    publish = dict(base)
    with pytest.raises(PlaneError) as raised:
        repository.publish_effect(
            ScriptedTransaction(one=[None, effect_row()]),
            **publish,
            receipt_digest="c" * 64,
            published_at=NOW,
        )
    assert raised.value.code == "effect_publication_conflict"
    stale_generation = effect_row(
        state="published",
        operation_execution_generation=9,
        downstream_receipt_digest="c" * 64,
        published_at=NOW,
    )
    with pytest.raises(PlaneError) as raised:
        repository.publish_effect(
            ScriptedTransaction(one=[None, stale_generation]),
            **publish,
            receipt_digest="c" * 64,
            published_at=NOW,
        )
    assert raised.value.code == "effect_publication_conflict"
    different_publication = effect_row(
        state="published",
        downstream_receipt_digest="c" * 64,
        published_at=NOW + timedelta(seconds=1),
    )
    with pytest.raises(PlaneError) as raised:
        repository.publish_effect(
            ScriptedTransaction(one=[None, different_publication]),
            **publish,
            receipt_digest="c" * 64,
            published_at=NOW,
        )
    assert raised.value.code == "effect_publication_conflict"
    conflicting_replay = effect_row(
        state="published",
        payload_digest="d" * 64,
        downstream_receipt_digest="c" * 64,
        published_at=NOW,
    )
    with pytest.raises(PlaneError) as raised:
        repository.publish_effect(
            ScriptedTransaction(one=[None, conflicting_replay]),
            **publish,
            receipt_digest="c" * 64,
            published_at=NOW,
        )
    assert raised.value.code == "effect_publication_conflict"
    with pytest.raises(ValueError):
        repository.publish_effect(
            ScriptedTransaction(),
            **publish,
            receipt_digest="bad",
            published_at=NOW,
        )
