"""Durable admission, scheduling, occurrence, and effect-ledger persistence.

This module deliberately owns only neutral state transitions.  It does not run
jobs, authorize work, publish chat content, or execute an effect.  Every method
uses a caller-owned transaction so a product can compose these writes with its
other authoritative state changes.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from astralplane.contracts import Record, Transaction
from astralplane.errors import PlaneError

_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_DURABLE_OWNER_SCOPES = frozenset({"user", "schedule"})


class OperationState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYABLE = "retryable"


class OccurrenceState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYABLE = "retryable"
    CANCELLED = "cancelled"


class EffectState(StrEnum):
    RESERVED = "reserved"
    PUBLISHED = "published"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class OperationSubmission:
    operation_id: str
    submission_id: str
    owner_id: str
    owner_scope: str
    operation_kind: str
    admission_class: str
    idempotency_namespace: str
    idempotency_key: str
    input_digest: str
    accepted_at: datetime
    queue_deadline_at: datetime

    def __post_init__(self) -> None:
        _uuid("operation_id", self.operation_id, version=4)
        _uuid("submission_id", self.submission_id)
        _required("owner_id", self.owner_id)
        if self.owner_scope not in _DURABLE_OWNER_SCOPES:
            raise ValueError("owner_scope must be user or schedule")
        _bounded_code("operation_kind", self.operation_kind, maximum=64)
        _bounded_code("admission_class", self.admission_class)
        _required("idempotency_namespace", self.idempotency_namespace, maximum=128)
        _required("idempotency_key", self.idempotency_key, maximum=256)
        _digest("input_digest", self.input_digest)
        _aware("accepted_at", self.accepted_at)
        _aware("queue_deadline_at", self.queue_deadline_at)
        if self.queue_deadline_at < self.accepted_at:
            raise ValueError("queue_deadline_at cannot precede accepted_at")


@dataclass(frozen=True, slots=True)
class OperationRecord:
    operation_id: str
    owner_id: str
    state: OperationState
    execution_generation: int
    execution_lease_token: str | None
    state_revision: int
    terminal_code: str | None
    accepted_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    job_id: str
    owner_id: str
    name: str
    instruction: str
    schedule_kind: str
    schedule_expression: str
    timezone: str
    status: str
    next_run_at: int | None
    created_at: int
    updated_at: int

    def __post_init__(self) -> None:
        _uuid("job_id", self.job_id, version=4)
        _required("owner_id", self.owner_id)
        _required("name", self.name, maximum=256)
        _required("instruction", self.instruction, maximum=65_536)
        if self.schedule_kind not in {"one_shot", "interval", "cron"}:
            raise ValueError("schedule_kind is not supported")
        _required("schedule_expression", self.schedule_expression, maximum=512)
        _required("timezone", self.timezone, maximum=128)
        if self.status not in {"active", "paused", "expired", "completed", "disabled"}:
            raise ValueError("scheduled job status is not supported")
        if self.created_at < 0 or self.updated_at < self.created_at:
            raise ValueError("scheduled job timestamps are invalid")


@dataclass(frozen=True, slots=True)
class ScheduledOccurrence:
    occurrence_id: str
    job_id: str
    owner_id: str
    scheduled_for: datetime
    state: OccurrenceState
    claim_generation: int
    lease_token: str | None
    lease_owner: str | None
    lease_expires_at: datetime | None
    attempt_count: int
    operation_id: str | None
    operation_execution_generation: int | None


@dataclass(frozen=True, slots=True)
class EffectRecord:
    occurrence_id: str
    effect_kind: str
    effect_key: str
    payload_digest: str
    state: EffectState
    operation_id: str | None
    operation_execution_generation: int
    occurrence_claim_generation: int
    downstream_receipt_digest: str | None
    published_at: datetime | None


class SchedulerRepository:
    """Native-parameter PostgreSQL repository with explicit transaction ownership."""

    def submit_operation(
        self, transaction: Transaction, submission: OperationSubmission
    ) -> OperationRecord:
        config = transaction.fetch_one(
            """
            SELECT class_name, queue_limit, max_wait_ms
            FROM operation_admission_class
            WHERE class_name = %s AND class_name <> 'global'
            FOR UPDATE
            """,
            (submission.admission_class,),
        )
        if config is None:
            raise PlaneError(
                "operation admission class is not configured",
                code="operation_admission_configuration_invalid",
                metadata={"owner_id": submission.owner_id},
            )

        row = transaction.fetch_one(
            """
            SELECT * FROM operation_record
            WHERE owner_user_id = %s AND owner_scope = %s
              AND idempotency_namespace = %s AND idempotency_key = %s
            FOR UPDATE
            """,
            (
                submission.owner_id,
                submission.owner_scope,
                submission.idempotency_namespace,
                submission.idempotency_key,
            ),
        )
        if row is not None:
            if not _same_submission(row, submission):
                raise PlaneError(
                    "operation idempotency identity has conflicting semantics",
                    code="operation_idempotency_conflict",
                    metadata={"owner_id": submission.owner_id},
                )
        else:
            queue_limit = int(config["queue_limit"])
            max_wait_ms = int(config["max_wait_ms"])
            requested_wait_ms = (
                submission.queue_deadline_at - submission.accepted_at
            ).total_seconds() * 1000
            if queue_limit <= 0 or requested_wait_ms > max_wait_ms:
                raise PlaneError(
                    "operation cannot be queued within its configured admission bounds",
                    code="operation_capacity_unavailable",
                    metadata={"owner_id": submission.owner_id},
                )
            row = transaction.fetch_one(
                """
                INSERT INTO operation_record (
                    operation_id, operation_kind, admission_class, owner_scope,
                    owner_user_id, idempotency_namespace, idempotency_key,
                    normalized_input_digest, state, accepted_at, updated_at,
                    queue_deadline_at
                ) SELECT %s, %s, config.class_name, %s, %s, %s, %s, %s,
                         'queued', %s, %s, %s
                  FROM operation_admission_class AS config
                 WHERE config.class_name = %s
                   AND config.queue_limit > (
                       SELECT COUNT(*) FROM operation_record AS queued
                       WHERE queued.admission_class = config.class_name
                         AND queued.state = 'queued'
                         AND queued.queue_deadline_at >= clock_timestamp()
                   )
                ON CONFLICT DO NOTHING
                RETURNING *
                """,
                (
                    submission.operation_id,
                    submission.operation_kind,
                    submission.owner_scope,
                    submission.owner_id,
                    submission.idempotency_namespace,
                    submission.idempotency_key,
                    submission.input_digest,
                    submission.accepted_at,
                    submission.accepted_at,
                    submission.queue_deadline_at,
                    submission.admission_class,
                ),
            )
            if row is None:
                raise PlaneError(
                    "operation queue capacity is unavailable or identity insertion conflicted",
                    code="operation_capacity_unavailable",
                    metadata={"owner_id": submission.owner_id},
                )
        operation = _operation(row)
        transaction.fetch_one(
            """
            INSERT INTO operation_submission_result (
                submission_result_id, submission_id, owner_scope, owner_user_id,
                accepted, operation_id, purge_after
            ) VALUES (%s, %s, %s, %s, TRUE, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING submission_result_id
            """,
            (
                submission.submission_id,
                submission.submission_id,
                submission.owner_scope,
                submission.owner_id,
                operation.operation_id,
                submission.queue_deadline_at,
            ),
        )
        return operation

    def claim_operation(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        operation_id: str,
        expected_revision: int,
        lease_token: str,
        lease_expires_at: datetime,
        now: datetime,
    ) -> OperationRecord:
        _required("owner_id", owner_id)
        _uuid("operation_id", operation_id, version=4)
        _uuid("lease_token", lease_token, version=4)
        _aware("lease_expires_at", lease_expires_at)
        _aware("now", now)
        if expected_revision < 0 or lease_expires_at <= now:
            raise ValueError("claim requires a current revision and future lease")

        identity = transaction.fetch_one(
            """
            SELECT admission_class
            FROM operation_record
            WHERE operation_id = %s AND owner_user_id = %s
              AND owner_scope IN ('user', 'schedule')
            """,
            (operation_id, owner_id),
        )
        if identity is None:
            return _required_row(None, "operation claim lost its owner fence", owner_id)

        admission_class = str(identity["admission_class"])
        class_rows = transaction.fetch_all(
            """
            WITH RECURSIVE class_names AS (
                SELECT class_name, parent_class_name, 0 AS depth
                FROM operation_admission_class
                WHERE class_name = %s
                UNION ALL
                SELECT parent.class_name, parent.parent_class_name, child.depth + 1
                FROM operation_admission_class AS parent
                JOIN class_names AS child
                  ON parent.class_name = child.parent_class_name
            )
            SELECT config.class_name, config.parent_class_name,
                   config.active_limit, names.depth
            FROM operation_admission_class AS config
            JOIN class_names AS names USING (class_name)
            ORDER BY names.depth DESC
            FOR UPDATE OF config
            """,
            (admission_class,),
        )
        class_chain = tuple(str(row["class_name"]) for row in class_rows)
        class_limits = {str(row["class_name"]): int(row["active_limit"]) for row in class_rows}
        if (
            not class_chain
            or class_chain[-1] != admission_class
            or class_rows[0].get("parent_class_name") is not None
            or len(set(class_chain)) != len(class_chain)
        ):
            raise PlaneError(
                "operation admission class has no complete durable slot chain",
                code="operation_admission_configuration_invalid",
                metadata={"owner_id": owner_id},
            )

        candidate = transaction.fetch_one(
            """
            SELECT operation.*
            FROM operation_record AS operation
            WHERE operation.operation_id = %s AND operation.owner_user_id = %s
              AND operation.owner_scope IN ('user', 'schedule')
              AND operation.admission_class = %s
              AND operation.state_revision = %s
              AND (
                    (
                        operation.state = 'queued'
                        AND operation.queue_deadline_at >= %s
                        AND NOT EXISTS (
                            SELECT 1 FROM operation_record AS earlier
                            WHERE earlier.admission_class = operation.admission_class
                              AND earlier.state = 'queued'
                              AND earlier.queue_deadline_at >= %s
                              AND (
                                  earlier.accepted_at < operation.accepted_at
                                  OR (
                                      earlier.accepted_at = operation.accepted_at
                                      AND earlier.operation_id < operation.operation_id
                                  )
                              )
                        )
                    )
                    OR (
                        operation.state = 'running'
                        AND EXISTS (
                            SELECT 1 FROM operation_admission_slot AS expired
                            WHERE expired.operation_id = operation.operation_id
                              AND expired.lease_expires_at <= %s
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM operation_admission_slot AS current
                            WHERE current.operation_id = operation.operation_id
                              AND current.lease_expires_at > %s
                        )
                    )
              )
            FOR UPDATE OF operation
            """,
            (operation_id, owner_id, admission_class, expected_revision, now, now, now, now),
        )
        if candidate is None:
            return _required_row(
                None, "operation claim lost its owner, revision, FIFO, or lease fence", owner_id
            )

        prior_state = OperationState(str(candidate["state"]))
        if prior_state is OperationState.QUEUED:
            slots = transaction.fetch_all(
                """
                SELECT selected.class_name, selected.slot_number
                FROM unnest(%s::text[]) WITH ORDINALITY AS chain(class_name, position)
                JOIN operation_admission_class AS config
                  ON config.class_name = chain.class_name
                JOIN LATERAL (
                    SELECT slot.class_name, slot.slot_number
                    FROM operation_admission_slot AS slot
                    WHERE slot.class_name = chain.class_name
                      AND slot.slot_number <= config.active_limit
                      AND slot.operation_id IS NULL
                    ORDER BY slot.slot_number
                    FOR UPDATE OF slot SKIP LOCKED
                    LIMIT 1
                ) AS selected ON TRUE
                ORDER BY chain.position
                """,
                (list(class_chain),),
            )
        else:
            slots = transaction.fetch_all(
                """
                SELECT class_name, slot_number
                FROM operation_admission_slot AS slot
                WHERE operation_id = %s
                ORDER BY class_name, slot_number
                FOR UPDATE
                """,
                (operation_id,),
            )

        selected = tuple((str(row["class_name"]), int(row["slot_number"])) for row in slots)
        if (
            len(selected) != len(class_chain)
            or {name for name, _ in selected} != set(class_chain)
            or any(slot_number > class_limits.get(name, 0) for name, slot_number in selected)
        ):
            raise PlaneError(
                "operation admission capacity is unavailable or its lease chain is incomplete",
                code="operation_capacity_unavailable",
                metadata={"owner_id": owner_id},
            )

        for class_name, slot_number in selected:
            if prior_state is OperationState.QUEUED:
                result = transaction.execute(
                    """
                    UPDATE operation_admission_slot
                    SET operation_id = %s, lease_token = %s,
                        claim_generation = claim_generation + 1,
                        lease_expires_at = %s
                    WHERE class_name = %s AND slot_number = %s
                      AND operation_id IS NULL
                    """,
                    (operation_id, lease_token, lease_expires_at, class_name, slot_number),
                )
            else:
                result = transaction.execute(
                    """
                    UPDATE operation_admission_slot
                    SET lease_token = %s, claim_generation = claim_generation + 1,
                        lease_expires_at = %s
                    WHERE class_name = %s AND slot_number = %s
                      AND operation_id = %s AND lease_expires_at <= %s
                    """,
                    (lease_token, lease_expires_at, class_name, slot_number, operation_id, now),
                )
            if result.rowcount != 1:
                raise PlaneError(
                    "operation admission slot claim lost its capacity fence",
                    code="operation_capacity_fence_lost",
                    metadata={"owner_id": owner_id},
                )

        row = transaction.fetch_one(
            """
            UPDATE operation_record
            SET state = 'running', execution_generation = execution_generation + 1,
                execution_lease_token = %s, state_revision = state_revision + 1,
                started_at = COALESCE(started_at, %s), updated_at = %s
            WHERE operation_id = %s AND owner_user_id = %s
              AND owner_scope IN ('user', 'schedule')
              AND state = %s AND state_revision = %s
            RETURNING *
            """,
            (
                lease_token,
                now,
                now,
                operation_id,
                owner_id,
                prior_state.value,
                expected_revision,
            ),
        )
        return _required_row(row, "operation claim lost its owner or revision fence", owner_id)

    def terminalize_operation(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        operation_id: str,
        execution_generation: int,
        lease_token: str,
        state: OperationState,
        terminal_code: str | None,
        safe_summary: str | None,
        retry_after_ms: int | None,
        now: datetime,
        purge_after: datetime,
    ) -> OperationRecord:
        _required("owner_id", owner_id)
        _uuid("operation_id", operation_id, version=4)
        _uuid("lease_token", lease_token, version=4)
        if execution_generation <= 0:
            raise ValueError("execution_generation must be positive")
        if state not in {
            OperationState.COMPLETED,
            OperationState.FAILED,
            OperationState.CANCELLED,
            OperationState.RETRYABLE,
        }:
            raise ValueError("operation terminal state is required")
        if state is not OperationState.COMPLETED:
            _bounded_code("terminal_code", terminal_code or "")
        if safe_summary is not None and len(safe_summary) > 512:
            raise ValueError("safe_summary exceeds 512 characters")
        if retry_after_ms is not None and (
            state is not OperationState.RETRYABLE or retry_after_ms < 0
        ):
            raise ValueError("retry_after_ms is valid only for retryable operations")
        _aware("now", now)
        _aware("purge_after", purge_after)
        row = transaction.fetch_one(
            """
            UPDATE operation_record
            SET state = %s, terminal_code = %s, safe_summary = %s,
                retry_after_ms = %s, execution_lease_token = NULL,
                state_revision = state_revision + 1, terminal_at = %s,
                purge_after = %s, updated_at = %s
            WHERE operation_id = %s AND owner_user_id = %s AND state = 'running'
              AND execution_generation = %s AND execution_lease_token = %s
            RETURNING *
            """,
            (
                state.value,
                terminal_code,
                safe_summary,
                retry_after_ms,
                now,
                purge_after,
                now,
                operation_id,
                owner_id,
                execution_generation,
                lease_token,
            ),
        )
        operation = _required_row(row, "operation execution fence is stale", owner_id)
        released = transaction.execute(
            """
            UPDATE operation_admission_slot
            SET operation_id = NULL, lease_token = NULL,
                claim_generation = claim_generation + 1,
                lease_expires_at = NULL
            WHERE operation_id = %s
            """,
            (operation_id,),
        )
        if released.rowcount < 1:
            raise PlaneError(
                "operation execution capacity lease is missing",
                code="operation_capacity_lease_missing",
                metadata={"owner_id": owner_id},
            )
        return operation

    def put_job(self, transaction: Transaction, job: ScheduledJob) -> ScheduledJob:
        row = transaction.fetch_one(
            """
            INSERT INTO scheduled_job (
                id, user_id, name, instruction, schedule_kind, schedule_expr,
                timezone, status, next_run_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            RETURNING *
            """,
            (
                job.job_id,
                job.owner_id,
                job.name,
                job.instruction,
                job.schedule_kind,
                job.schedule_expression,
                job.timezone,
                job.status,
                job.next_run_at,
                job.created_at,
                job.updated_at,
            ),
        )
        if row is None:
            row = transaction.fetch_one(
                "SELECT * FROM scheduled_job WHERE id = %s AND user_id = %s",
                (job.job_id, job.owner_id),
            )
        if row is None or _job(row) != job:
            raise PlaneError(
                "scheduled job identity has conflicting semantics",
                code="scheduled_job_conflict",
                metadata={"owner_id": job.owner_id},
            )
        return job

    def get_job(
        self, transaction: Transaction, *, owner_id: str, job_id: str
    ) -> ScheduledJob | None:
        _required("owner_id", owner_id)
        _uuid("job_id", job_id, version=4)
        row = transaction.fetch_one(
            "SELECT * FROM scheduled_job WHERE id = %s AND user_id = %s",
            (job_id, owner_id),
        )
        return None if row is None else _job(row)

    def list_due_jobs(
        self, transaction: Transaction, *, owner_id: str, due_at_ms: int, limit: int
    ) -> tuple[ScheduledJob, ...]:
        _required("owner_id", owner_id)
        _limit(limit)
        rows = transaction.fetch_all(
            """
            SELECT * FROM scheduled_job
            WHERE user_id = %s AND status = 'active' AND next_run_at <= %s
            ORDER BY next_run_at, id LIMIT %s
            """,
            (owner_id, due_at_ms, limit),
        )
        return tuple(_job(row) for row in rows)

    def create_occurrence(
        self,
        transaction: Transaction,
        *,
        occurrence_id: str,
        job_id: str,
        owner_id: str,
        scheduled_for: datetime,
    ) -> ScheduledOccurrence:
        _uuid("occurrence_id", occurrence_id, version=4)
        _uuid("job_id", job_id, version=4)
        _required("owner_id", owner_id)
        _aware("scheduled_for", scheduled_for)
        row = transaction.fetch_one(
            """
            INSERT INTO scheduled_occurrence (
                occurrence_id, job_id, owner_user_id, scheduled_for, state,
                first_eligible_at
            ) SELECT %s, id, user_id, %s, 'pending', %s
              FROM scheduled_job WHERE id = %s AND user_id = %s
            ON CONFLICT (job_id, scheduled_for) DO NOTHING
            RETURNING *
            """,
            (occurrence_id, scheduled_for, scheduled_for, job_id, owner_id),
        )
        if row is None:
            row = transaction.fetch_one(
                """
                SELECT * FROM scheduled_occurrence
                WHERE job_id = %s AND owner_user_id = %s AND scheduled_for = %s
                """,
                (job_id, owner_id, scheduled_for),
            )
        if row is None:
            raise PlaneError(
                "scheduled occurrence owner or job was not found",
                code="scheduled_occurrence_not_found",
                metadata={"owner_id": owner_id},
            )
        return _occurrence(row)

    def claim_occurrence(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        occurrence_id: str,
        worker_id: str,
        lease_token: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> ScheduledOccurrence:
        _required("owner_id", owner_id)
        _uuid("occurrence_id", occurrence_id, version=4)
        _required("worker_id", worker_id)
        _uuid("lease_token", lease_token, version=4)
        _aware("now", now)
        _aware("lease_expires_at", lease_expires_at)
        if lease_expires_at <= now:
            raise ValueError("occurrence lease must expire after now")
        row = transaction.fetch_one(
            """
            UPDATE scheduled_occurrence
            SET state = 'claimed', lease_token = %s, lease_owner = %s,
                lease_expires_at = %s, claim_generation = claim_generation + 1,
                attempt_count = attempt_count + 1,
                current_operation_id = NULL,
                operation_execution_generation = NULL,
                started_at = NULL, terminal_at = NULL,
                next_attempt_at = NULL, result_code = NULL,
                updated_at = %s
            WHERE occurrence_id = %s AND owner_user_id = %s
              AND (
                    (
                        state IN ('pending', 'retryable')
                        AND scheduled_for <= %s
                        AND (next_attempt_at IS NULL OR next_attempt_at <= %s)
                    )
                    OR (
                        state IN ('claimed', 'running')
                        AND lease_expires_at <= %s
                        AND NOT EXISTS (
                            SELECT 1 FROM operation_record AS operation
                            WHERE operation.operation_id = current_operation_id
                              AND operation.state IN ('queued', 'running')
                        )
                    )
              )
            RETURNING *
            """,
            (
                lease_token,
                worker_id,
                lease_expires_at,
                now,
                occurrence_id,
                owner_id,
                now,
                now,
                now,
            ),
        )
        return _required_occurrence(row, "occurrence claim was denied", owner_id)

    def bind_occurrence_operation(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        occurrence_id: str,
        occurrence_claim_generation: int,
        occurrence_lease_token: str,
        operation_id: str,
        operation_execution_generation: int,
        operation_lease_token: str,
        now: datetime,
    ) -> ScheduledOccurrence:
        """Bind and start one occurrence under both current durable leases."""

        _required("owner_id", owner_id)
        _uuid("occurrence_id", occurrence_id, version=4)
        _uuid("occurrence_lease_token", occurrence_lease_token, version=4)
        _uuid("operation_id", operation_id, version=4)
        _uuid("operation_lease_token", operation_lease_token, version=4)
        _aware("now", now)
        if min(occurrence_claim_generation, operation_execution_generation) <= 0:
            raise ValueError("operation binding requires positive execution generations")
        row = transaction.fetch_one(
            """
            UPDATE scheduled_occurrence AS occurrence
            SET state = 'running', current_operation_id = operation.operation_id,
                operation_execution_generation = operation.execution_generation,
                started_at = COALESCE(occurrence.started_at, %s), updated_at = %s
            FROM operation_record AS operation
            WHERE occurrence.occurrence_id = %s
              AND occurrence.owner_user_id = %s
              AND occurrence.state IN ('claimed', 'running')
              AND occurrence.claim_generation = %s
              AND occurrence.lease_token = %s
              AND occurrence.lease_expires_at > %s
              AND (
                  occurrence.current_operation_id IS NULL
                  OR occurrence.current_operation_id = operation.operation_id
              )
              AND (
                  occurrence.operation_execution_generation IS NULL
                  OR occurrence.operation_execution_generation = operation.execution_generation
              )
              AND operation.operation_id = %s
              AND operation.owner_scope = 'schedule'
              AND operation.owner_user_id = %s
              AND operation.operation_kind = 'scheduled_occurrence'
              AND operation.admission_class = 'scheduled'
              AND operation.state = 'running'
              AND operation.execution_generation = %s
              AND operation.execution_lease_token = %s
            RETURNING occurrence.*
            """,
            (
                now,
                now,
                occurrence_id,
                owner_id,
                occurrence_claim_generation,
                occurrence_lease_token,
                now,
                operation_id,
                owner_id,
                operation_execution_generation,
                operation_lease_token,
            ),
        )
        return _required_occurrence(
            row, "occurrence or operation execution fence is stale", owner_id
        )

    def reserve_effect(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        occurrence_id: str,
        effect_kind: str,
        effect_key: str,
        payload_digest: str,
        operation_id: str,
        operation_execution_generation: int,
        occurrence_claim_generation: int,
    ) -> EffectRecord:
        _required("owner_id", owner_id)
        _uuid("occurrence_id", occurrence_id, version=4)
        _uuid("operation_id", operation_id, version=4)
        _bounded_code("effect_kind", effect_kind, maximum=64)
        _required("effect_key", effect_key, maximum=256)
        _digest("payload_digest", payload_digest)
        if min(operation_execution_generation, occurrence_claim_generation) <= 0:
            raise ValueError("effect reservations require positive execution generations")
        row = transaction.fetch_one(
            """
            INSERT INTO effect_ledger (
                occurrence_id, effect_kind, effect_key, payload_digest, state,
                operation_id, operation_execution_generation,
                occurrence_claim_generation
            ) SELECT occurrence.occurrence_id, %s, %s, %s, 'reserved', %s, %s, %s
              FROM scheduled_occurrence AS occurrence
              JOIN operation_record AS operation
                ON operation.operation_id = occurrence.current_operation_id
             WHERE occurrence.occurrence_id = %s AND occurrence.owner_user_id = %s
               AND occurrence.state = 'running'
               AND occurrence.lease_expires_at > clock_timestamp()
               AND occurrence.claim_generation = %s
               AND occurrence.current_operation_id = %s
               AND occurrence.operation_execution_generation = %s
               AND operation.owner_scope = 'schedule'
               AND operation.owner_user_id = occurrence.owner_user_id
               AND operation.state = 'running'
               AND operation.execution_generation = %s
            ON CONFLICT (occurrence_id, effect_kind, effect_key) DO NOTHING
            RETURNING *
            """,
            (
                effect_kind,
                effect_key,
                payload_digest,
                operation_id,
                operation_execution_generation,
                occurrence_claim_generation,
                occurrence_id,
                owner_id,
                occurrence_claim_generation,
                operation_id,
                operation_execution_generation,
                operation_execution_generation,
            ),
        )
        if row is None:
            row = transaction.fetch_one(
                """
                SELECT effect.* FROM effect_ledger AS effect
                JOIN scheduled_occurrence AS occurrence
                  ON occurrence.occurrence_id = effect.occurrence_id
                WHERE effect.occurrence_id = %s AND effect.effect_kind = %s
                  AND effect.effect_key = %s AND occurrence.owner_user_id = %s
                """,
                (occurrence_id, effect_kind, effect_key, owner_id),
            )
        effect = None if row is None else _effect(row)
        if (
            effect is None
            or effect.payload_digest != payload_digest
            or effect.operation_id != operation_id
            or effect.operation_execution_generation != operation_execution_generation
            or effect.occurrence_claim_generation != occurrence_claim_generation
        ):
            raise PlaneError(
                "effect identity replay has conflicting semantics or a stale fence",
                code="effect_reservation_conflict",
                metadata={"owner_id": owner_id},
            )
        return effect

    def publish_effect(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        occurrence_id: str,
        effect_kind: str,
        effect_key: str,
        payload_digest: str,
        operation_id: str,
        operation_execution_generation: int,
        occurrence_claim_generation: int,
        receipt_digest: str,
        published_at: datetime,
    ) -> EffectRecord:
        _required("owner_id", owner_id)
        _uuid("occurrence_id", occurrence_id, version=4)
        _uuid("operation_id", operation_id, version=4)
        _bounded_code("effect_kind", effect_kind, maximum=64)
        _required("effect_key", effect_key, maximum=256)
        _digest("payload_digest", payload_digest)
        _digest("receipt_digest", receipt_digest)
        _aware("published_at", published_at)
        if min(operation_execution_generation, occurrence_claim_generation) <= 0:
            raise ValueError("effect publication requires positive execution generations")
        row = transaction.fetch_one(
            """
            UPDATE effect_ledger AS effect
               SET state = 'published', published_at = %s,
                   downstream_receipt_digest = %s
              FROM scheduled_occurrence AS occurrence
             WHERE effect.occurrence_id = occurrence.occurrence_id
               AND occurrence.owner_user_id = %s
               AND effect.occurrence_id = %s AND effect.effect_kind = %s
               AND effect.effect_key = %s AND effect.payload_digest = %s
               AND effect.state = 'reserved'
               AND effect.operation_id = %s
               AND effect.operation_execution_generation = %s
               AND effect.occurrence_claim_generation = %s
               AND occurrence.claim_generation = %s
               AND occurrence.state = 'running'
               AND occurrence.lease_expires_at > clock_timestamp()
               AND occurrence.current_operation_id = %s
               AND occurrence.operation_execution_generation = %s
               AND EXISTS (
                   SELECT 1 FROM operation_record AS operation
                   WHERE operation.operation_id = effect.operation_id
                     AND operation.owner_scope = 'schedule'
                     AND operation.owner_user_id = occurrence.owner_user_id
                     AND operation.state = 'running'
                     AND operation.execution_generation = %s
               )
            RETURNING effect.*
            """,
            (
                published_at,
                receipt_digest,
                owner_id,
                occurrence_id,
                effect_kind,
                effect_key,
                payload_digest,
                operation_id,
                operation_execution_generation,
                occurrence_claim_generation,
                occurrence_claim_generation,
                operation_id,
                operation_execution_generation,
                operation_execution_generation,
            ),
        )
        if row is None:
            existing = transaction.fetch_one(
                """
                SELECT effect.* FROM effect_ledger AS effect
                JOIN scheduled_occurrence AS occurrence USING (occurrence_id)
                WHERE effect.occurrence_id = %s AND effect.effect_kind = %s
                  AND effect.effect_key = %s AND occurrence.owner_user_id = %s
                """,
                (occurrence_id, effect_kind, effect_key, owner_id),
            )
            replay = None if existing is None else _effect(existing)
            if (
                replay is not None
                and replay.state is EffectState.PUBLISHED
                and replay.payload_digest == payload_digest
                and replay.operation_id == operation_id
                and replay.operation_execution_generation == operation_execution_generation
                and replay.occurrence_claim_generation == occurrence_claim_generation
                and replay.downstream_receipt_digest == receipt_digest
                and replay.published_at == published_at
            ):
                return replay
            raise PlaneError(
                "effect publication fence is stale or conflicts with prior publication",
                code="effect_publication_conflict",
                metadata={"owner_id": owner_id},
            )
        return _effect(row)


def _required(name: str, value: str, *, maximum: int = 512) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a non-empty string of at most {maximum} characters")


def _bounded_code(name: str, value: str, *, maximum: int = 128) -> None:
    if len(value) > maximum or _CODE.fullmatch(value) is None:
        raise ValueError(f"{name} must be bounded snake_case")


def _uuid(name: str, value: str, *, version: int | None = None) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a canonical UUID") from exc
    if (
        parsed.int == 0
        or str(parsed) != value
        or (version is not None and parsed.version != version)
    ):
        suffix = f"v{version}" if version is not None else "UUID"
        raise ValueError(f"{name} must be a canonical non-nil {suffix}")
    return value


def _digest(name: str, value: str) -> None:
    if _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{name} must be lowercase SHA-256")


def _aware(name: str, value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _limit(limit: int) -> None:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")


def _same_submission(row: Record, submission: OperationSubmission) -> bool:
    return (
        str(row["operation_kind"]) == submission.operation_kind
        and str(row["admission_class"]) == submission.admission_class
        and str(row["normalized_input_digest"]) == submission.input_digest
    )


def _operation(row: Record) -> OperationRecord:
    operation_id = str(row["operation_id"])
    _uuid("operation_id", operation_id, version=4)
    lease_token = (
        None if row.get("execution_lease_token") is None else str(row["execution_lease_token"])
    )
    if lease_token is not None:
        _uuid("execution_lease_token", lease_token, version=4)
    return OperationRecord(
        operation_id=operation_id,
        owner_id=str(row["owner_user_id"]),
        state=OperationState(str(row["state"])),
        execution_generation=int(row["execution_generation"]),
        execution_lease_token=lease_token,
        state_revision=int(row["state_revision"]),
        terminal_code=None if row.get("terminal_code") is None else str(row["terminal_code"]),
        accepted_at=row["accepted_at"],
        updated_at=row["updated_at"],
    )


def _required_row(row: Record | None, message: str, owner_id: str) -> OperationRecord:
    if row is None:
        raise PlaneError(message, code="stale_operation_fence", metadata={"owner_id": owner_id})
    return _operation(row)


def _job(row: Record) -> ScheduledJob:
    job_id = str(row["id"])
    _uuid("job_id", job_id, version=4)
    return ScheduledJob(
        job_id=job_id,
        owner_id=str(row["user_id"]),
        name=str(row["name"]),
        instruction=str(row["instruction"]),
        schedule_kind=str(row["schedule_kind"]),
        schedule_expression=str(row["schedule_expr"]),
        timezone=str(row["timezone"]),
        status=str(row["status"]),
        next_run_at=None if row.get("next_run_at") is None else int(row["next_run_at"]),
        created_at=int(row["created_at"]),
        updated_at=int(row["updated_at"]),
    )


def _occurrence(row: Record) -> ScheduledOccurrence:
    occurrence_id = str(row["occurrence_id"])
    job_id = str(row["job_id"])
    _uuid("occurrence_id", occurrence_id, version=4)
    _uuid("job_id", job_id, version=4)
    lease_token = None if row.get("lease_token") is None else str(row["lease_token"])
    if lease_token is not None:
        _uuid("lease_token", lease_token, version=4)
    operation_id = (
        None if row.get("current_operation_id") is None else str(row["current_operation_id"])
    )
    if operation_id is not None:
        _uuid("current_operation_id", operation_id, version=4)
    return ScheduledOccurrence(
        occurrence_id=occurrence_id,
        job_id=job_id,
        owner_id=str(row["owner_user_id"]),
        scheduled_for=row["scheduled_for"],
        state=OccurrenceState(str(row["state"])),
        claim_generation=int(row["claim_generation"]),
        lease_token=lease_token,
        lease_owner=None if row.get("lease_owner") is None else str(row["lease_owner"]),
        lease_expires_at=row.get("lease_expires_at"),
        attempt_count=int(row["attempt_count"]),
        operation_id=operation_id,
        operation_execution_generation=(
            None
            if row.get("operation_execution_generation") is None
            else int(row["operation_execution_generation"])
        ),
    )


def _required_occurrence(row: Record | None, message: str, owner_id: str) -> ScheduledOccurrence:
    if row is None:
        raise PlaneError(message, code="stale_occurrence_fence", metadata={"owner_id": owner_id})
    return _occurrence(row)


def _effect(row: Record) -> EffectRecord:
    occurrence_id = str(row["occurrence_id"])
    _uuid("occurrence_id", occurrence_id, version=4)
    operation_id = None if row.get("operation_id") is None else str(row["operation_id"])
    if operation_id is not None:
        _uuid("operation_id", operation_id, version=4)
    published_at = row.get("published_at")
    if published_at is not None:
        _aware("published_at", published_at)
    return EffectRecord(
        occurrence_id=occurrence_id,
        effect_kind=str(row["effect_kind"]),
        effect_key=str(row["effect_key"]),
        payload_digest=str(row["payload_digest"]),
        state=EffectState(str(row["state"])),
        operation_id=operation_id,
        operation_execution_generation=int(row["operation_execution_generation"]),
        occurrence_claim_generation=int(row["occurrence_claim_generation"]),
        downstream_receipt_digest=(
            None
            if row.get("downstream_receipt_digest") is None
            else str(row["downstream_receipt_digest"])
        ),
        published_at=published_at,
    )


__all__ = (
    "EffectRecord",
    "EffectState",
    "OccurrenceState",
    "OperationRecord",
    "OperationState",
    "OperationSubmission",
    "ScheduledJob",
    "ScheduledOccurrence",
    "SchedulerRepository",
)
