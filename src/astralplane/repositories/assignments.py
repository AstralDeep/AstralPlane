"""Owner-isolated assignment controller storage with durable execution fencing.

All I/O uses the caller-owned transaction. No callable, token minting or source
access occurs here. Indexed identities retain completed effects independently of
bounded working memory. Authorization remains the embedding application's job.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryNotFoundError,
    RepositoryValidationError,
)
from astralplane.repositories.assignment_models import (  # noqa: F401
    AssignmentActionDecision,
    AssignmentActionIntent,
    AssignmentActionOutcome,
    AssignmentActionReconciliation,
    AssignmentActionRecord,
    AssignmentActionReservation,
    AssignmentActivityRecord,
    AssignmentClaim,
    AssignmentControl,
    AssignmentControlResult,
    AssignmentDefinition,
    AssignmentDispatchPermit,
    AssignmentEpisodeCompletion,
    AssignmentFence,
    AssignmentOperationBinding,
    AssignmentOwnerRetirementResult,
    AssignmentRecord,
    AssignmentRecoveryResult,
    AssignmentResourceAmount,
    AssignmentRetentionResult,
    AssignmentSourceBatch,
    AssignmentSourceEvent,
    AssignmentTask,
    AssignmentTaskClaim,
    AssignmentTaskResult,
)

_DIMENSIONS = ("model_calls", "tool_calls", "tokens", "elapsed_ms")
_PHASES = {
    "waiting",
    "checking",
    "investigating",
    "delegating",
    "waiting_approval",
    "waiting_authorization",
    "budget_exhausted",
    "reconciliation",
    "failed",
}
_TERMINAL = {"stopped", "completed"}


def plain(value: Any) -> Any:
    """Canonical JSON-compatible copy of detached values (no driver objects)."""
    if is_dataclass(value):
        return {f.name: plain(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(v) for v in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise RepositoryValidationError("timestamp must have a timezone")
        return value.astimezone(UTC).isoformat()
    return value


def canonical(value: Any, maximum: int = 262144) -> str:
    try:
        encoded = json.dumps(
            plain(value), ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
    except (TypeError, ValueError) as exc:
        raise RepositoryValidationError("invalid bounded JSON") from exc
    if len(encoded.encode()) > maximum:
        raise RepositoryValidationError("assignment JSON exceeds its bound")
    return encoded


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _text(value, maximum=512):
    if not isinstance(value, str) or not value.strip() or len(value.encode()) > maximum:
        raise RepositoryValidationError("invalid bounded text")
    return value


def _uuid(value):
    try:
        result = uuid.UUID(value)
        if result.version != 4 or str(result) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise RepositoryValidationError("canonical UUID4 required") from exc
    return value


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value):
        raise RepositoryValidationError("SHA-256 required")
    return value


def _integer(value, minimum=0, maximum=2**53 - 1):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise RepositoryValidationError("integer outside supported bound")
    return value


def _time(value):
    if value is None:
        return None
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise RepositoryValidationError("aware timestamp required")
    return result.astimezone(UTC)


def _now(transaction):
    return transaction.fetch_one("SELECT clock_timestamp() AS now")["now"]


def _conflict(code):
    raise RepositoryConflictError(code, code=code)


def _version(data, revision, epoch):
    _integer(revision, 1)
    _integer(epoch, 1)
    if data["instruction_revision"] != revision or data["control_epoch"] != epoch:
        _conflict("assignment_revision_conflict")


def _definition(value):
    return AssignmentDefinition(**value)


def _record(data):
    names = {entry.name for entry in fields(AssignmentRecord)} - {"last_completed_generation"}
    values = {key: data[key] for key in names}
    values["last_completed_generation"] = data.get("last_completion", {}).get("claim_generation", 0)
    values["definition"] = _definition(values["definition"])
    for key in ("created_at", "updated_at", "next_wake_at"):
        values[key] = _time(values[key])
    return AssignmentRecord(**values)


def _intent(data):
    values = dict(data)
    values["maximum"] = AssignmentResourceAmount(**values["maximum"])
    for key in ("quote_expires_at", "approval_expires_at"):
        values[key] = _time(values[key])
    return AssignmentActionIntent(**values)


def _action_record(data):
    return AssignmentActionRecord(
        data["action_id"],
        data["assignment_id"],
        data["owner_id"],
        _intent(data["intent"]),
        data["instruction_revision"],
        data["control_epoch"],
        data["state"],
        data.get("result"),
        tuple(
            {k: v for k, v in item.items() if k not in {"dispatch_token", "binding"}}
            for item in data["attempts"]
        ),
        data.get("interactive_proposal_id"),
        any(item.get("dispatch_token") is not None for item in data["attempts"]),
    )


class AssignmentRepository:
    """Small durable graphs; row locks serialize authority, effects and budgets."""

    @staticmethod
    def validate_definition(definition: AssignmentDefinition) -> None:
        _text(definition.name, 256)
        _text(definition.instructions, 8192)
        if not isinstance(definition.source, Mapping) or not isinstance(definition.limits, Mapping):
            raise RepositoryValidationError("source and limits must be objects")
        canonical(definition.source, 8192)
        if not definition.source or not 1 <= len(definition.allowed_tools) <= 64:
            raise RepositoryValidationError("source and explicit allowed tools required")
        for values in (definition.allowed_tools, definition.consented_scopes):
            if len(values) > 64 or len(set(values)) != len(values):
                raise RepositoryValidationError("invalid tool/scope set")
            for value in values:
                _text(value, 256)
        if definition.offline_grant_id is not None:
            _uuid(definition.offline_grant_id)
        limits = definition.limits
        _integer(limits.get("cadence_seconds"), 60, 31536000)
        for key, maximum in (
            ("max_retries", 10),
            ("max_concurrent_tasks", 5),
            ("max_depth", 4),
            ("max_tasks", 32),
        ):
            _integer(limits.get(key), 0 if key in {"max_retries", "max_depth"} else 1, maximum)
        for key in _DIMENSIONS:
            _integer(limits.get(key), 1)
            _integer(limits.get("daily_" + key), 1)
        if limits.get("spend_micro_units") is not None:
            _integer(limits["spend_micro_units"])
            _integer(limits.get("daily_spend_micro_units"))
            _text(limits.get("currency"), 8)
            coverage = definition.cost_quote_coverage
            if not coverage or not coverage.get("quote_digest") or not coverage.get("expires_at"):
                raise RepositoryValidationError("cost_bound_unavailable")
            _digest(coverage["quote_digest"])
            _time(coverage["expires_at"])
        elif limits.get("currency") is not None:
            raise RepositoryValidationError("currency requires an explicit monetary cap")
        canonical(definition, 32768)

    def _load(self, query, owner_id, assignment_id, *, lock=False, required=True):
        _text(owner_id)
        _uuid(assignment_id)
        row = query.fetch_one(
            "SELECT * FROM persistent_assignment WHERE id=%s AND owner_user_id=%s"
            + (" FOR UPDATE" if lock else ""),
            (assignment_id, owner_id),
        )
        if row is None:
            if required:
                raise RepositoryNotFoundError("assignment_not_found", code="assignment_not_found")
            return None
        try:
            data = plain(row["data"])
            if (
                data["assignment_id"] != assignment_id
                or data["owner_id"] != owner_id
                or data["state_version"] != row["state_version"]
                or data["lifecycle"] != row["lifecycle"]
                or _time(data["next_wake_at"]) != row["next_wake_at"]
                or _time(data["lease_expires_at"]) != row["lease_expires_at"]
            ):
                raise ValueError
            self.validate_definition(_definition(data["definition"]))
            if data["phase"] not in _PHASES or not isinstance(data["tasks"], list):
                raise ValueError
            if len(data["tasks"]) > 32 or not isinstance(data["checkpoint"], dict):
                raise ValueError
            for key in ("instruction_revision", "control_epoch", "state_version"):
                _integer(data[key], 1)
            for bucket in ("spent", "daily", "outstanding"):
                for amount in data["usage"][bucket].values():
                    _integer(amount)
            for task in data["tasks"]:
                AssignmentTask(**task)
            _record(data)
            canonical(data, 262144)
            return data
        except (KeyError, TypeError, ValueError, RepositoryValidationError) as exc:
            raise RepositoryDataError("invalid persisted assignment") from exc

    def _save(self, transaction, data):
        old_version = data["state_version"]
        data["state_version"] += 1
        data["updated_at"] = plain(_now(transaction))
        row = transaction.fetch_one(
            "UPDATE persistent_assignment SET data=%s::jsonb,lifecycle=%s,next_wake_at=%s,"
            "lease_expires_at=%s,state_version=%s WHERE id=%s AND owner_user_id=%s "
            "AND state_version=%s RETURNING id",
            (
                canonical(data),
                data["lifecycle"],
                data["next_wake_at"],
                data["lease_expires_at"],
                data["state_version"],
                data["assignment_id"],
                data["owner_id"],
                old_version,
            ),
        )
        if row is None:
            _conflict("assignment_revision_conflict")
        return _record(data)

    def _fenced(self, transaction, fence, *, action_id=None):
        data = self._load(transaction, fence.owner_id, fence.assignment_id, lock=True)
        _version(data, fence.instruction_revision, fence.control_epoch)
        if (
            data["lifecycle"] != "active"
            or data["claim_token"] != fence.claim_token
            or data["claim_generation"] != fence.claim_generation
            or _time(data["lease_expires_at"]) is None
            or _time(data["lease_expires_at"]) <= _now(transaction)
        ):
            _conflict("assignment_claim_stale")
        if data.get("approved_action_id") and data["approved_action_id"] != action_id:
            _conflict("assignment_action_claim_restricted")
        return data

    @staticmethod
    def _clear_claim(data):
        data.update(
            claim_token=None,
            lease_expires_at=None,
            claimed_by=None,
            operation_binding=None,
            approved_action_id=None,
        )

    def _claim(self, transaction, data, worker_id, lease_seconds, action_id=None):
        _text(worker_id, 128)
        _integer(lease_seconds, 5, 60)
        previous = data.get("operation_binding")
        data.update(
            claim_generation=data["claim_generation"] + 1,
            claim_token=str(uuid.uuid4()),
            lease_expires_at=plain(_now(transaction) + timedelta(seconds=lease_seconds)),
            claimed_by=worker_id,
            operation_binding=None,
            approved_action_id=action_id,
            phase="checking" if action_id is None else "waiting_approval",
        )
        data["claimed_wake_generation"] = data["wake_generation"]
        record = self._save(transaction, data)
        return self._claim_record(data, record, previous)

    @staticmethod
    def _claim_record(data, record=None, previous=None):
        return AssignmentClaim(
            record or _record(data),
            AssignmentFence(
                data["assignment_id"],
                data["owner_id"],
                data["instruction_revision"],
                data["control_epoch"],
                data["claim_generation"],
                data["claim_token"],
            ),
            _time(data["lease_expires_at"]),
            previous,
            data.get("approved_action_id"),
        )

    def create_assignment(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        submission_id,
        submission_digest,
        definition,
        max_owned_assignments=25,
        max_retained_assignments=256,
    ):
        self.validate_definition(definition)
        _text(owner_id)
        _uuid(assignment_id)
        _uuid(submission_id)
        _digest(submission_digest)
        _integer(max_owned_assignments, 1, 25)
        _integer(max_retained_assignments, 1, 256)
        transaction.fetch_one("SELECT pg_advisory_xact_lock(hashtextextended(%s,79))", (owner_id,))
        retired = transaction.fetch_one(
            "SELECT state FROM astralplane_blob_owner_state WHERE owner_id=%s FOR UPDATE",
            (owner_id,),
        )
        if retired and retired["state"] != "active":
            _conflict("assignment_owner_retired")
        replay = transaction.fetch_one(
            "SELECT id,submission_digest,data FROM persistent_assignment "
            "WHERE owner_user_id=%s AND submission_id=%s",
            (owner_id, submission_id),
        )
        if replay:
            if replay["submission_digest"] != submission_digest or replay["data"][
                "initial_definition_digest"
            ] != digest(definition):
                _conflict("assignment_idempotency_conflict")
            return self.get_assignment(
                transaction, owner_id=owner_id, assignment_id=str(replay["id"])
            )
        counts = transaction.fetch_one(
            "SELECT count(*) AS total,count(*) FILTER(WHERE lifecycle IN ('active','paused')) "
            "AS active FROM persistent_assignment WHERE owner_user_id=%s",
            (owner_id,),
        )
        if counts["total"] >= max_retained_assignments or counts["active"] >= max_owned_assignments:
            _conflict("assignment_capacity_exhausted")
        self._validate_references(transaction, owner_id, definition)
        now = plain(_now(transaction))
        data = dict(
            assignment_id=assignment_id,
            owner_id=owner_id,
            submission_id=submission_id,
            submission_digest=submission_digest,
            definition=plain(definition),
            initial_definition_digest=digest(definition),
            instruction_revision=1,
            control_epoch=1,
            state_version=1,
            lifecycle="active",
            phase="waiting",
            next_wake_at=now,
            wake_reason="created",
            wake_generation=1,
            checkpoint={"schema_version": 1},
            tasks=[],
            usage={
                "spent": {},
                "daily": {},
                "outstanding": {},
                "day": now[:10],
                "money_status": "unknown",
            },
            created_at=now,
            updated_at=now,
            safe_error_code=None,
            claim_generation=0,
            claim_token=None,
            lease_expires_at=None,
            claimed_by=None,
            operation_binding=None,
            approved_action_id=None,
            controls={},
            source_batches={},
            plans={},
            claimed_wake_generation=0,
            last_check_at=None,
            next_retry_at=None,
            consecutive_failures=0,
            activity_sequence=0,
        )
        row = transaction.fetch_one(
            "INSERT INTO persistent_assignment(id,owner_user_id,submission_id,submission_digest,"
            "lifecycle,next_wake_at,state_version,data) "
            "VALUES(%s,%s,%s,%s,'active',%s,1,%s::jsonb) "
            "ON CONFLICT DO NOTHING RETURNING id",
            (assignment_id, owner_id, submission_id, submission_digest, now, canonical(data)),
        )
        if row is None:
            _conflict("assignment_idempotency_conflict")
        return _record(data)

    @staticmethod
    def _validate_references(transaction, owner_id, definition):
        if definition.offline_grant_id is not None:
            row = transaction.fetch_one(
                "SELECT id FROM user_offline_grant WHERE id=%s AND user_id=%s "
                "AND revoked_at IS NULL AND expires_at > "
                "extract(epoch FROM clock_timestamp())*1000",
                (definition.offline_grant_id, owner_id),
            )
            if row is None:
                _conflict("assignment_authorization_unavailable")
        else:
            _conflict("assignment_authorization_unavailable")
        if definition.conversation_id is not None:
            row = transaction.fetch_one(
                "SELECT id FROM chats WHERE id=%s AND user_id=%s",
                (definition.conversation_id, owner_id),
            )
            if row is None:
                _conflict("assignment_conversation_not_owned")
        coverage = definition.cost_quote_coverage
        if definition.limits.get("spend_micro_units") is not None and _time(
            coverage["expires_at"]
        ) <= _now(transaction):
            _conflict("assignment_cost_bound_unavailable")

    def get_assignment(self, query, *, owner_id, assignment_id):
        data = self._load(query, owner_id, assignment_id, required=False)
        return _record(data) if data else None

    def get_submission_receipt(
        self, query, *, owner_id, assignment_id, submission_id, submission_digest, command
    ):
        """Inspect accepted client semantics before recapturing server-owned grants."""
        _uuid(submission_id)
        _digest(submission_digest)
        data = self._load(query, owner_id, assignment_id, required=False)
        if data is None:
            return None
        if data["submission_id"] == submission_id:
            if command != "create" or data["submission_digest"] != submission_digest:
                _conflict("assignment_idempotency_conflict")
            return _record(data)
        receipt = data["controls"].get(submission_id)
        if receipt is not None:
            if receipt["command"] != command or receipt["submission_digest"] != submission_digest:
                _conflict("assignment_idempotency_conflict")
            return _record(data)
        receipt = data["controls"].get("check:" + submission_id)
        if receipt is not None:
            if command != "run-now" or receipt != submission_digest:
                _conflict("assignment_idempotency_conflict")
            return _record(data)
        return None

    def list_assignments(self, query, *, owner_id, limit=50, after_id=None):
        _text(owner_id)
        _integer(limit, 1, 100)
        if after_id is not None:
            _uuid(after_id)
        rows = query.fetch_all(
            "SELECT id FROM persistent_assignment WHERE owner_user_id=%s "
            "AND (%s::uuid IS NULL OR id>%s::uuid) ORDER BY id LIMIT %s",
            (owner_id, after_id, after_id, limit),
        )
        return tuple(
            self.get_assignment(query, owner_id=owner_id, assignment_id=str(row["id"]))
            for row in rows
        )

    def apply_control(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        expected_instruction_revision,
        expected_control_epoch,
        submission_id,
        submission_digest,
        control,
        replacement=None,
    ):
        _uuid(submission_id)
        _digest(submission_digest)
        control = AssignmentControl(control)
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        request = digest(
            [
                str(control),
                replacement,
                expected_instruction_revision,
                expected_control_epoch,
                submission_digest,
            ]
        )
        old = data["controls"].get(submission_id)
        if old:
            if old["signature"] != request:
                _conflict("assignment_idempotency_conflict")
            return AssignmentControlResult(_record(data), False)
        _version(data, expected_instruction_revision, expected_control_epoch)
        if data["lifecycle"] in _TERMINAL:
            if control == AssignmentControl.STOP and data["lifecycle"] == "stopped":
                return AssignmentControlResult(_record(data), False)
            _conflict("assignment_not_active")
        if len(data["controls"]) >= 256:
            # A prior expected epoch protects evicted receipts from reapplication.
            # Capacity must never make pause, stop or revocation unavailable.
            data["controls"].pop(next(iter(data["controls"])))
        if control == AssignmentControl.REVISE:
            if replacement is None:
                raise RepositoryValidationError("replacement definition required")
            self.validate_definition(replacement)
            self._validate_references(transaction, owner_id, replacement)
            old_limits = data["definition"]["limits"]
            if old_limits.get("currency") != replacement.limits.get("currency") and (
                any(data["usage"]["spent"].values()) or any(data["usage"]["outstanding"].values())
            ):
                _conflict(
                    "assignment_prior_cost_unknown"
                    if old_limits.get("currency") is None
                    else "assignment_currency_change_invalid"
                )
            data["definition"] = plain(replacement)
            data["instruction_revision"] += 1
            for key in (
                "cursor",
                "source_configuration_digest",
                "last_batch_key",
                "last_checked_at",
                "last_finding",
                "last_observation",
            ):
                data["checkpoint"].pop(key, None)
            data["source_batches"] = {}
            for task in data["tasks"]:
                self._activity(
                    transaction,
                    data,
                    AssignmentActivityRecord(
                        f"revision:{expected_instruction_revision}:task:{task['task_id']}",
                        "task_superseded",
                        task["title"],
                        task["bounded_result"] or "",
                        {
                            "task_id": task["task_id"],
                            "instruction_revision": expected_instruction_revision,
                            "prior_state": task["state"],
                            "result_digest": task["result_digest"],
                            "provenance": task["provenance"],
                        },
                    ),
                )
                task["provenance"]["superseded"] = {
                    "prior_state": task["state"],
                    "by_instruction_revision": data["instruction_revision"],
                }
                task["state"] = "superseded"
                task["task_generation"] += 1
            for row in transaction.fetch_all(
                "SELECT id,data FROM persistent_assignment_event WHERE assignment_id=%s "
                "AND state IN ('pending','processing','failed','reconciliation') FOR UPDATE",
                (assignment_id,),
            ):
                event = plain(row["data"])
                event.update(
                    disposition="superseded",
                    result_digest=digest(
                        [
                            "instruction_superseded",
                            expected_instruction_revision,
                            data["instruction_revision"],
                        ]
                    ),
                )
                transaction.execute(
                    "UPDATE persistent_assignment_event SET state='superseded',data=%s::jsonb "
                    "WHERE id=%s",
                    (canonical(event), row["id"]),
                )
            data["phase"] = "waiting"
        elif control == AssignmentControl.RESUME:
            if data["lifecycle"] != "paused":
                _conflict("assignment_not_paused")
            self._validate_references(transaction, owner_id, _definition(data["definition"]))
            data.update(lifecycle="active", phase="waiting")
        elif control == AssignmentControl.REVOKE:
            data["definition"]["offline_grant_id"] = None
            data.update(phase="waiting_authorization")
        elif control == AssignmentControl.PAUSE:
            data["lifecycle"] = "paused"
        else:
            data["lifecycle"] = "stopped"
        data["control_epoch"] += 1
        data["controls"][submission_id] = {
            "signature": request,
            "submission_digest": submission_digest,
            "command": str(control),
        }
        self._clear_claim(data)
        data["next_wake_at"] = (
            plain(_now(transaction))
            if data["lifecycle"] == "active" and data["phase"] == "waiting"
            else None
        )
        invalidated, begun = self._invalidate_actions(transaction, data)
        for task in data["tasks"]:
            if task["state"] in {"pending", "running"}:
                task["state"] = "cancelled" if control in {"stop", "revise"} else "pending"
                task["task_generation"] += 1
        self._activity(
            transaction,
            data,
            AssignmentActivityRecord(
                f"control:{submission_id}", "control", f"Assignment {control}", "", {}
            ),
            critical=True,
        )
        return AssignmentControlResult(
            self._save(transaction, data), True, tuple(invalidated), tuple(begun)
        )

    def request_check(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        expected_instruction_revision,
        expected_control_epoch,
        submission_id,
        submission_digest,
    ):
        _uuid(submission_id)
        _digest(submission_digest)
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        _version(data, expected_instruction_revision, expected_control_epoch)
        if data["lifecycle"] != "active":
            _conflict("assignment_not_active")
        key = "check:" + submission_id
        if key in data["controls"]:
            if data["controls"][key] != submission_digest:
                _conflict("assignment_idempotency_conflict")
            return _record(data)
        if len(data["controls"]) >= 256:
            _conflict("assignment_history_capacity_exhausted")
        data["controls"][key] = submission_digest
        now = _now(transaction)
        due = now
        if data["last_check_at"]:
            due = max(
                due,
                _time(data["last_check_at"])
                + timedelta(seconds=data["definition"]["limits"]["cadence_seconds"]),
            )
        if data["next_retry_at"]:
            due = max(due, _time(data["next_retry_at"]))
        data.update(
            next_wake_at=plain(due),
            wake_reason="owner_check",
            wake_generation=data["wake_generation"] + 1,
        )
        return self._save(transaction, data)

    def claim_due_for_administration(self, transaction, *, worker_id, limit=20, lease_seconds=30):
        _integer(limit, 1, 100)
        rows = transaction.fetch_all(
            "SELECT id,owner_user_id FROM persistent_assignment WHERE lifecycle='active' "
            "AND next_wake_at<=clock_timestamp() AND lease_expires_at IS NULL "
            "AND data->>'phase' IN ('waiting','failed') "
            "ORDER BY next_wake_at,id LIMIT %s FOR UPDATE SKIP LOCKED",
            (limit,),
        )
        return tuple(
            self._claim(
                transaction,
                self._load(transaction, row["owner_user_id"], str(row["id"]), lock=True),
                worker_id,
                lease_seconds,
            )
            for row in rows
        )

    def bind_operation(self, transaction, *, fence, binding):
        data = self._fenced(
            transaction, fence, action_id=self._foreground_action(transaction, fence)
        )
        _uuid(binding.operation_id)
        _uuid(binding.execution_lease_token)
        _integer(binding.execution_generation, 1)
        if data["operation_binding"] not in (None, plain(binding)):
            _conflict("assignment_operation_conflict")
        data["operation_binding"] = plain(binding)
        return self._save(transaction, data)

    def _foreground_action(self, transaction, fence):
        return self._load(transaction, fence.owner_id, fence.assignment_id).get(
            "approved_action_id"
        )

    def renew_claim(self, transaction, *, fence, lease_seconds=30):
        _integer(lease_seconds, 5, 60)
        data = self._fenced(
            transaction, fence, action_id=self._foreground_action(transaction, fence)
        )
        data["lease_expires_at"] = plain(_now(transaction) + timedelta(seconds=lease_seconds))
        return self._claim_record(data, self._save(transaction, data))

    def assert_current_claim(self, query, *, fence):
        return _record(self._fenced(query, fence, action_id=self._foreground_action(query, fence)))

    def _action(self, transaction, owner_id, assignment_id, action_id, *, required=True):
        _uuid(action_id)
        row = transaction.fetch_one(
            "SELECT data,state FROM persistent_assignment_action "
            "WHERE id=%s AND assignment_id=%s AND owner_user_id=%s FOR UPDATE",
            (action_id, assignment_id, owner_id),
        )
        if row is None:
            if required:
                raise RepositoryNotFoundError("assignment action not found")
            return None
        try:
            data = plain(row["data"])
            if (
                data["action_id"] != action_id
                or data["assignment_id"] != assignment_id
                or data["owner_id"] != owner_id
                or data["state"] != row["state"]
            ):
                raise ValueError
            _action_record(data)
            return data
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryDataError("invalid persisted action") from exc

    @staticmethod
    def _save_action(transaction, data):
        transaction.execute(
            "UPDATE persistent_assignment_action SET data=%s::jsonb,state=%s "
            "WHERE id=%s AND assignment_id=%s AND owner_user_id=%s",
            (
                canonical(data),
                data["state"],
                data["action_id"],
                data["assignment_id"],
                data["owner_id"],
            ),
        )
        return _action_record(data)

    def _invalidate_actions(self, transaction, assignment):
        rows = transaction.fetch_all(
            "SELECT id FROM persistent_assignment_action WHERE assignment_id=%s "
            "AND owner_user_id=%s AND state IN ('ready','proposed','approved','reserved','started',"
            "'uncertain') ORDER BY id FOR UPDATE",
            (assignment["assignment_id"], assignment["owner_id"]),
        )
        invalidated, begun = [], []
        for row in rows:
            action = self._action(
                transaction, assignment["owner_id"], assignment["assignment_id"], str(row["id"])
            )
            if action["state"] in {"started", "uncertain"}:
                begun.append(action["action_id"])
                continue
            for attempt in action["attempts"]:
                if attempt["state"] == "reserved":
                    self._release(assignment, attempt["maximum"])
                    attempt["state"] = "failed_not_started"
            action["state"] = "invalidated"
            if action.get("interactive_proposal_id"):
                self._expire_interactive_proposal(
                    transaction, assignment["owner_id"], action["interactive_proposal_id"]
                )
            self._save_action(transaction, action)
            invalidated.append(action["action_id"])
        return invalidated, begun

    @staticmethod
    def _expire_interactive_proposal(transaction, owner_id, proposal_id):
        # Losing the inverse link must never turn assignment-bound authority into
        # an ordinary remote confirmation capability.
        transaction.execute(
            "UPDATE remote_operation_proposal SET status='expired',decided_at=COALESCE(decided_at, "
            "GREATEST(created_at,floor(extract(epoch FROM clock_timestamp()))::bigint)) "
            "WHERE owner_user_id=%s AND proposal_id=%s AND status IN ('pending','approved')",
            (owner_id, proposal_id),
        )

    @staticmethod
    def _amount(amount):
        values = plain(amount)
        for key in _DIMENSIONS:
            _integer(values[key])
        money = values.get("spend_micro_units")
        if money is not None:
            _integer(money)
            _text(values.get("currency"), 8)
        elif values.get("currency") is not None:
            raise RepositoryValidationError("unknown money cannot carry a currency")
        return values

    @staticmethod
    def _day(data, now):
        day = plain(now)[:10]
        if data["usage"]["day"] != day:
            data["usage"]["day"] = day
            data["usage"]["daily"] = {}

    @staticmethod
    def _release(data, maximum):
        outstanding = data["usage"]["outstanding"]
        for key in (*_DIMENSIONS, "spend_micro_units"):
            amount = maximum.get(key)
            if amount is not None:
                if outstanding.get(key, 0) < amount:
                    raise RepositoryDataError("reservation accounting is inconsistent")
                outstanding[key] = outstanding.get(key, 0) - amount

    def _reserve(self, data, amount):
        usage, limits = data["usage"], data["definition"]["limits"]
        keys = list(_DIMENSIONS)
        if limits.get("spend_micro_units") is not None:
            if amount.get("currency") != limits["currency"]:
                _conflict("assignment_cost_bound_unavailable")
            keys.append("spend_micro_units")
        for key in keys:
            value = amount.get(key)
            if value is None:
                _conflict("assignment_cost_bound_unavailable")
            outstanding = usage["outstanding"].get(key, 0)
            if (
                usage["spent"].get(key, 0) + outstanding + value > limits[key]
                or usage["daily"].get(key, 0) + outstanding + value > limits["daily_" + key]
            ):
                _conflict("assignment_budget_exhausted")
        for key in (*_DIMENSIONS, "spend_micro_units"):
            if amount.get(key) is not None:
                usage["outstanding"][key] = usage["outstanding"].get(key, 0) + amount[key]

    def put_action(self, transaction, *, fence, intent):
        data = self._fenced(transaction, fence)
        _text(intent.action_key)
        _digest(intent.request_digest)
        _digest(intent.permission_digest)
        _digest(intent.precondition_digest)
        canonical(intent.request, 8192)
        if digest(intent.request) != intent.request_digest:
            raise RepositoryValidationError("action request digest mismatch")
        self._amount(intent.maximum)
        if intent.sensitivity not in {"ordinary", "sensitive"}:
            raise RepositoryValidationError("invalid sensitivity disposition")
        if not isinstance(intent.interactive_only, bool):
            raise RepositoryValidationError("interactive_only must be boolean")
        if intent.boundary not in {
            "internal_transaction",
            "downstream_key",
            "read_only",
            "unreplayable",
        }:
            raise RepositoryValidationError("unreviewed effect boundary")
        if intent.boundary == "downstream_key":
            _text(intent.downstream_key)
        if intent.task_id and not any(t["task_id"] == intent.task_id for t in data["tasks"]):
            _conflict("assignment_task_not_found")
        if intent.event_id:
            self._event(transaction, fence.owner_id, fence.assignment_id, intent.event_id)
        row = transaction.fetch_one(
            "SELECT id,data FROM persistent_assignment_action "
            "WHERE assignment_id=%s AND owner_user_id=%s AND action_key=%s",
            (fence.assignment_id, fence.owner_id, intent.action_key),
        )
        if row:
            old = plain(row["data"])
            if old["intent_digest"] != digest(intent):
                _conflict("assignment_idempotency_conflict")
            return _action_record(old)
        counts = transaction.fetch_one(
            "SELECT count(*) AS total,count(*) FILTER "
            "(WHERE state='proposed') AS pending FROM "
            "persistent_assignment_action WHERE assignment_id=%s",
            (fence.assignment_id,),
        )
        if counts["total"] >= 10000 or counts["pending"] >= 100:
            _conflict("assignment_history_capacity_exhausted")
        needs_approval = intent.sensitivity == "sensitive" or intent.interactive_only
        if needs_approval:
            expiry = _time(intent.approval_expires_at)
            now = _now(transaction)
            if expiry is None or not now < expiry <= now + timedelta(hours=24):
                raise RepositoryValidationError("approval expiry outside allowed bound")
        action = dict(
            action_id=str(uuid.uuid4()),
            assignment_id=fence.assignment_id,
            owner_id=fence.owner_id,
            intent=plain(intent),
            intent_digest=digest(intent),
            instruction_revision=fence.instruction_revision,
            control_epoch=fence.control_epoch,
            state="proposed" if needs_approval else "ready",
            result=None,
            attempts=[],
            decision=None,
            foreground_admission=None,
            reconciliation=None,
        )
        transaction.execute(
            "INSERT INTO persistent_assignment_action "
            "(id,assignment_id,owner_user_id,action_key,state,data) "
            "VALUES(%s,%s,%s,%s,%s,%s::jsonb)",
            (
                action["action_id"],
                fence.assignment_id,
                fence.owner_id,
                intent.action_key,
                action["state"],
                canonical(action),
            ),
        )
        if needs_approval:
            data["phase"] = "waiting_approval"
            self._activity(
                transaction,
                data,
                AssignmentActivityRecord(
                    "approval:" + action["action_id"],
                    "approval",
                    "Approval required",
                    "Review the exact proposed action before it can run.",
                    {"action_id": action["action_id"]},
                    notification_state="pending",
                ),
            )
            self._save(transaction, data)
        return _action_record(action)

    def get_action(self, query, *, owner_id, assignment_id, action_id):
        self._load(query, owner_id, assignment_id)
        data = self._action(query, owner_id, assignment_id, action_id, required=False)
        return _action_record(data) if data else None

    def get_action_by_key(self, query, *, owner_id, assignment_id, action_key):
        """Recover the immutable intent without recreating expiring parameters."""
        self._load(query, owner_id, assignment_id)
        _text(action_key)
        row = query.fetch_one(
            "SELECT data FROM persistent_assignment_action WHERE assignment_id=%s "
            "AND owner_user_id=%s AND action_key=%s",
            (assignment_id, owner_id, action_key),
        )
        return None if row is None else _action_record(plain(row["data"]))

    def link_interactive_proposal(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        action_id,
        expected_request_digest,
        proposal_id,
        expected_instruction_revision,
        expected_control_epoch,
    ):
        """Bind one existing attended proposal to identical immutable arguments."""
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        _version(data, expected_instruction_revision, expected_control_epoch)
        _text(proposal_id, 128)
        action = self._action(transaction, owner_id, assignment_id, action_id)
        if (
            data["lifecycle"] != "active"
            or action["state"] not in {"proposed", "approved"}
            or action["intent"]["request_digest"] != expected_request_digest
            or not action["intent"]["interactive_only"]
        ):
            _conflict("assignment_approval_invalid")
        prior = action.get("interactive_proposal_id")
        if prior is not None:
            if prior != proposal_id:
                _conflict("assignment_idempotency_conflict")
            return _action_record(action)
        proposal = transaction.fetch_one(
            "SELECT agent_id,verb,args_fingerprint,expires_at,status "
            "FROM remote_operation_proposal "
            "WHERE proposal_id=%s AND owner_user_id=%s FOR UPDATE",
            (proposal_id, owner_id),
        )
        request = action["intent"]["request"]
        arguments = {
            k: v for k, v in request.get("arguments", {}).items() if not str(k).startswith("_")
        }
        fingerprint = hashlib.sha256(
            json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if (
            proposal is None
            or proposal["status"] != "pending"
            or proposal["expires_at"] <= int(_now(transaction).timestamp())
            or proposal["agent_id"] != request.get("agent_id")
            or proposal["verb"] != request.get("tool_name")
            or proposal["args_fingerprint"] != fingerprint
        ):
            _conflict("assignment_approval_invalid")
        other = transaction.fetch_one(
            "SELECT id FROM persistent_assignment_action WHERE owner_user_id=%s "
            "AND data->>'interactive_proposal_id'=%s",
            (owner_id, proposal_id),
        )
        if other is not None:
            _conflict("assignment_idempotency_conflict")
        action["interactive_proposal_id"] = proposal_id
        return self._save_action(transaction, action)

    def get_action_for_interactive_proposal(self, query, *, owner_id, proposal_id):
        _text(owner_id)
        _text(proposal_id, 128)
        row = query.fetch_one(
            "SELECT data FROM persistent_assignment_action WHERE owner_user_id=%s "
            "AND data->>'interactive_proposal_id'=%s",
            (owner_id, proposal_id),
        )
        return None if row is None else _action_record(plain(row["data"]))

    def observe_interactive_proposal(self, transaction, *, owner_id, proposal_id):
        """Observe an actual remote decline/expiry without accepting caller verdicts."""
        linked = self.get_action_for_interactive_proposal(
            transaction, owner_id=owner_id, proposal_id=proposal_id
        )
        if linked is None:
            return None
        data = self._load(transaction, owner_id, linked.assignment_id, lock=True)
        action = self._action(transaction, owner_id, linked.assignment_id, linked.action_id)
        row = transaction.fetch_one(
            "SELECT status,expires_at FROM remote_operation_proposal "
            "WHERE proposal_id=%s AND owner_user_id=%s FOR UPDATE",
            (proposal_id, owner_id),
        )
        if row is None:
            _conflict("assignment_approval_invalid")
        expired = row["expires_at"] <= int(_now(transaction).timestamp())
        if (row["status"] not in {"declined", "expired"} and not expired) or action[
            "state"
        ] not in {"proposed", "approved", "reserved"}:
            return _action_record(action)
        for attempt in action["attempts"]:
            if attempt["state"] == "reserved":
                self._release(data, attempt["maximum"])
                attempt["state"] = "failed_not_started"
        action["state"] = "declined" if row["status"] == "declined" else "invalidated"
        self._save_action(transaction, action)
        if data["lifecycle"] == "active":
            data.update(
                phase="waiting",
                next_wake_at=plain(_now(transaction)),
                wake_reason="approval_declined",
                wake_generation=data["wake_generation"] + 1,
            )
        self._save(transaction, data)
        return _action_record(action)

    def list_actions(self, query, *, owner_id, assignment_id, states=(), limit=100, after_id=None):
        self._load(query, owner_id, assignment_id)
        _integer(limit, 1, 100)
        if after_id is not None:
            _uuid(after_id)
        if len(states) > 12:
            raise RepositoryValidationError("too many action states")
        rows = query.fetch_all(
            "SELECT data FROM persistent_assignment_action WHERE assignment_id=%s "
            "AND owner_user_id=%s AND (%s::uuid IS NULL OR id>%s::uuid) "
            "AND (cardinality(%s::text[])=0 OR state=ANY(%s::text[])) "
            "ORDER BY id LIMIT %s",
            (assignment_id, owner_id, after_id, after_id, list(states), list(states), limit),
        )
        return tuple(_action_record(plain(row["data"])) for row in rows)

    def decide_action(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        action_id,
        expected_instruction_revision,
        expected_control_epoch,
        decision,
    ):
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        _version(data, expected_instruction_revision, expected_control_epoch)
        action = self._action(transaction, owner_id, assignment_id, action_id)
        _uuid(decision.submission_id)
        _digest(decision.submission_digest)
        if action["decision"] is not None:
            if action["decision"] != plain(decision):
                _conflict("assignment_approval_invalid")
            return _action_record(action)
        self._approve_conditions(
            transaction,
            data,
            action,
            decision.proposal_digest,
            decision.permission_digest,
            decision.precondition_digest,
        )
        if action["state"] != "proposed" or decision.decision not in {"approve", "decline"}:
            _conflict("assignment_approval_invalid")
        action.update(
            decision=plain(decision),
            state="approved" if decision.decision == "approve" else "declined",
        )
        self._save_action(transaction, action)
        self._activity(
            transaction,
            data,
            AssignmentActivityRecord(
                "decision:" + decision.submission_id,
                "approval",
                "Action " + decision.decision,
                "",
                {"action_id": action_id},
            ),
        )
        self._save(transaction, data)
        return _action_record(action)

    @staticmethod
    def _approve_conditions(transaction, data, action, request_digest, permissions, preconditions):
        intent = action["intent"]
        if (
            data["lifecycle"] != "active"
            or action["instruction_revision"] != data["instruction_revision"]
            or action["control_epoch"] != data["control_epoch"]
            or request_digest != intent["request_digest"]
            or permissions != intent["permission_digest"]
            or preconditions != intent["precondition_digest"]
        ):
            _conflict("assignment_approval_invalid")
        if intent["sensitivity"] == "sensitive" or intent["interactive_only"]:
            expiry = _time(intent["approval_expires_at"])
            if expiry is None or expiry <= _now(transaction):
                _conflict("assignment_approval_invalid")

    def claim_for_approved_action(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        action_id,
        expected_request_digest,
        expected_instruction_revision,
        expected_control_epoch,
        interactive_receipt_id,
        submission_id,
        submission_digest,
        worker_id,
        lease_seconds=30,
    ):
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        _version(data, expected_instruction_revision, expected_control_epoch)
        _text(interactive_receipt_id)
        _uuid(submission_id)
        _digest(submission_digest)
        action = self._action(transaction, owner_id, assignment_id, action_id)
        self._approve_conditions(
            transaction,
            data,
            action,
            expected_request_digest,
            action["intent"]["permission_digest"],
            action["intent"]["precondition_digest"],
        )
        if action["state"] != "approved" or action["foreground_admission"] is not None:
            _conflict("assignment_approval_invalid")
        if data["lease_expires_at"] is not None:
            _conflict("assignment_claim_busy")
        claim = self._claim(transaction, data, worker_id, lease_seconds, action_id)
        action["foreground_admission"] = dict(
            submission_id=submission_id,
            submission_digest=submission_digest,
            receipt_id=interactive_receipt_id,
            claim_generation=claim.fence.claim_generation,
        )
        self._save_action(transaction, action)
        return claim

    def reserve_action(
        self,
        transaction,
        *,
        fence,
        action_id,
        attempt_id,
        expected_request_digest,
        maximum,
        quote_digest=None,
        quote_expires_at=None,
    ):
        data = self._fenced(transaction, fence, action_id=action_id)
        action = self._action(transaction, fence.owner_id, fence.assignment_id, action_id)
        _uuid(attempt_id)
        amount = self._amount(maximum)
        if action["intent"]["request_digest"] != expected_request_digest:
            _conflict("assignment_idempotency_conflict")
        if amount != action["intent"]["maximum"]:
            _conflict("assignment_reservation_exceeds_intent")
        if quote_digest != action["intent"]["quote_digest"] or _time(quote_expires_at) != _time(
            action["intent"]["quote_expires_at"]
        ):
            _conflict("assignment_cost_bound_unavailable")
        for attempt in action["attempts"]:
            if attempt["attempt_id"] == attempt_id:
                if attempt["maximum"] != amount or attempt["quote_digest"] != quote_digest:
                    _conflict("assignment_idempotency_conflict")
                return AssignmentActionReservation(
                    _action_record(action), attempt_id, maximum, False
                )
        if action["state"] not in {"ready", "approved", "failed_not_started", "failed"}:
            _conflict("assignment_action_uncertain")
        if action["state"] == "failed" and action["intent"]["boundary"] not in {
            "read_only",
            "downstream_key",
        }:
            _conflict("assignment_action_uncertain")
        if len(action["attempts"]) >= 1 + data["definition"]["limits"]["max_retries"]:
            _conflict("assignment_retry_exhausted")
        now = _now(transaction)
        self._day(data, now)
        if data["definition"]["limits"].get("spend_micro_units") is not None:
            _digest(quote_digest)
            if (
                _time(quote_expires_at) is None
                or _time(quote_expires_at) <= now
                or amount["spend_micro_units"] is None
            ):
                _conflict("assignment_cost_bound_unavailable")
        self._reserve(data, amount)
        action["attempts"].append(
            dict(
                attempt_id=attempt_id,
                state="reserved",
                maximum=amount,
                quote_digest=quote_digest,
                quote_expires_at=plain(quote_expires_at),
                dispatch_token=None,
                binding=None,
                outcome=None,
            )
        )
        action["state"] = "reserved"
        self._save_action(transaction, action)
        self._save(transaction, data)
        return AssignmentActionReservation(_action_record(action), attempt_id, maximum, True)

    def start_action(
        self,
        transaction,
        *,
        fence,
        action_id,
        attempt_id,
        expected_request_digest,
        current_permission_digest,
        current_precondition_digest,
        binding,
        interactive_receipt_id=None,
    ):
        data = self._fenced(transaction, fence, action_id=action_id)
        action = self._action(transaction, fence.owner_id, fence.assignment_id, action_id)
        self._approve_conditions(
            transaction,
            data,
            action,
            expected_request_digest,
            current_permission_digest,
            current_precondition_digest,
        )
        if data["operation_binding"] != plain(binding):
            _conflict("assignment_operation_conflict")
        attempt = self._attempt(action, attempt_id)
        if attempt["state"] != "reserved" or action["state"] != "reserved":
            _conflict("assignment_action_already_started")
        needs_approval = (
            action["intent"]["sensitivity"] == "sensitive" or action["intent"]["interactive_only"]
        )
        if needs_approval and (
            action["decision"] is None
            or action["decision"]["decision"] != "approve"
            or action.get("approval_consumed_at") is not None
        ):
            _conflict("assignment_approval_invalid")
        if action["intent"]["interactive_only"]:
            admission = action["foreground_admission"]
            if (
                not admission
                or admission["receipt_id"] != interactive_receipt_id
                or admission["claim_generation"] != fence.claim_generation
            ):
                _conflict("assignment_approval_invalid")
        if data["definition"]["limits"].get("spend_micro_units") is not None and _time(
            attempt["quote_expires_at"]
        ) <= _now(transaction):
            _conflict("assignment_cost_bound_unavailable")
        token = str(uuid.uuid4())
        if needs_approval:
            action["approval_consumed_at"] = plain(_now(transaction))
        attempt.update(state="started", dispatch_token=token, binding=plain(binding))
        action["state"] = "started"
        self._save_action(transaction, action)
        return AssignmentDispatchPermit(
            action_id, attempt_id, token, expected_request_digest, binding
        )

    @staticmethod
    def _attempt(action, attempt_id):
        for attempt in action["attempts"]:
            if attempt["attempt_id"] == attempt_id:
                return attempt
        raise RepositoryNotFoundError("assignment attempt not found")

    def record_action_outcome(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        action_id,
        attempt_id,
        dispatch_token,
        expected_request_digest,
        outcome,
    ):
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        action = self._action(transaction, owner_id, assignment_id, action_id)
        attempt = self._attempt(action, attempt_id)
        if (
            not dispatch_token
            or attempt["dispatch_token"] != dispatch_token
            or expected_request_digest != action["intent"]["request_digest"]
        ):
            _conflict("assignment_result_conflict")
        if outcome.outcome not in {"succeeded", "failed", "uncertain", "failed_not_started"}:
            raise RepositoryValidationError("invalid action outcome")
        _digest(outcome.result_digest)
        canonical(outcome.result, 8192)
        if attempt["outcome"] is not None:
            if attempt["outcome"] == plain(outcome):
                return _action_record(action)
            if attempt["outcome"]["outcome"] != "uncertain" or outcome.outcome == "uncertain":
                _conflict("assignment_result_conflict")
            attempt["uncertain_observation"] = attempt["outcome"]
        if attempt["state"] not in {"started", "uncertain"}:
            _conflict("assignment_result_conflict")
        if outcome.outcome == "failed_not_started":
            raise RepositoryValidationError("issued permits require observed outcomes, not refunds")
        actual = self._amount(outcome.actual) if outcome.actual is not None else attempt["maximum"]
        maximum = attempt["maximum"]
        if maximum["spend_micro_units"] is not None and actual["spend_micro_units"] is None:
            actual["spend_micro_units"] = maximum["spend_micro_units"]
            actual["currency"] = maximum["currency"]
        if actual["currency"] != maximum["currency"] and maximum["currency"] is not None:
            _conflict("assignment_result_currency_conflict")
        if outcome.outcome != "uncertain":
            self._release(data, maximum)
            self._day(data, _now(transaction))
            for key in (*_DIMENSIONS, "spend_micro_units"):
                if actual.get(key) is not None:
                    for bucket in ("spent", "daily"):
                        data["usage"][bucket][key] = data["usage"][bucket].get(key, 0) + actual[key]
            if actual["spend_micro_units"] is not None:
                data["usage"]["money_status"] = "reported"
        attempt.update(state=outcome.outcome, outcome=plain(outcome))
        action.update(state=outcome.outcome, result=plain(outcome))
        if outcome.outcome == "uncertain":
            data["phase"] = "reconciliation"
        elif data["lifecycle"] == "active":
            data["wake_generation"] += 1
        self._save_action(transaction, action)
        self._save(transaction, data)
        return _action_record(action)

    def release_unstarted_action(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        action_id,
        attempt_id,
        expected_request_digest,
        reason_code,
    ):
        _text(reason_code, 128)
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        action = self._action(transaction, owner_id, assignment_id, action_id)
        if action["intent"]["request_digest"] != expected_request_digest:
            _conflict("assignment_idempotency_conflict")
        attempt = self._attempt(action, attempt_id)
        if attempt["state"] == "failed_not_started":
            return _action_record(action)
        if attempt["state"] != "reserved" or attempt["dispatch_token"] is not None:
            _conflict("assignment_action_uncertain")
        self._release(data, attempt["maximum"])
        attempt["state"] = "failed_not_started"
        action["state"] = "failed_not_started"
        self._save_action(transaction, action)
        self._save(transaction, data)
        return _action_record(action)

    def reconcile_action(
        self,
        transaction,
        *,
        owner_id,
        assignment_id,
        action_id,
        expected_instruction_revision,
        expected_control_epoch,
        decision,
    ):
        data = self._load(transaction, owner_id, assignment_id, lock=True)
        _version(data, expected_instruction_revision, expected_control_epoch)
        action = self._action(transaction, owner_id, assignment_id, action_id)
        _uuid(decision.submission_id)
        _digest(decision.submission_digest)
        _text(decision.evidence_reference, 2048)
        if action["reconciliation"] is not None:
            if action["reconciliation"] != plain(decision):
                _conflict("assignment_idempotency_conflict")
            return _action_record(action)
        if (
            action["state"] != "uncertain"
            or decision.decision not in {"confirmed_applied", "confirmed_not_applied"}
            or (action["result"] or {}).get("result_digest") != decision.prior_result_digest
        ):
            _conflict("assignment_result_conflict")
        attempt = action["attempts"][-1]
        self._release(data, attempt["maximum"])
        self._day(data, _now(transaction))
        for key in (*_DIMENSIONS, "spend_micro_units"):
            if attempt["maximum"].get(key) is not None:
                for bucket in ("spent", "daily"):
                    data["usage"][bucket][key] = (
                        data["usage"][bucket].get(key, 0) + attempt["maximum"][key]
                    )
        action["reconciliation"] = plain(decision)
        action["state"] = (
            "succeeded" if decision.decision == "confirmed_applied" else "failed_not_started"
        )
        attempt["state"] = action["state"]
        action["result"] = {
            "outcome": "reconciled_applied"
            if decision.decision == "confirmed_applied"
            else "reconciled_not_applied",
            "result_digest": digest(["reconciliation", plain(decision)]),
            "result": {},
            "result_available": False,
            "evidence_reference": decision.evidence_reference,
            "reconciliation": {
                "decision": decision.decision,
                "prior_result_digest": decision.prior_result_digest,
            },
        }
        self._save_action(transaction, action)
        if data["lifecycle"] == "active":
            data.update(
                phase="waiting",
                next_wake_at=plain(_now(transaction)),
                wake_reason="reconciled",
                wake_generation=data["wake_generation"] + 1,
            )
        self._save(transaction, data)
        return _action_record(action)

    @staticmethod
    def _event(transaction, owner_id, assignment_id, event_id):
        _uuid(event_id)
        row = transaction.fetch_one(
            "SELECT data FROM persistent_assignment_event WHERE id=%s "
            "AND assignment_id=%s AND owner_user_id=%s",
            (event_id, assignment_id, owner_id),
        )
        if row is None:
            raise RepositoryNotFoundError("assignment event not found")
        return plain(row["data"])

    def record_source_batch(self, transaction, *, fence, expected_state_version, batch):
        data = self._fenced(transaction, fence)
        _text(batch.batch_key)
        _digest(batch.batch_digest)
        _digest(batch.configuration_digest)
        _digest(batch.expected_cursor_digest)
        canonical(batch, 65536)
        if len(batch.events) > 100:
            raise RepositoryValidationError("source batch exceeds item bound")
        batch_signature = digest(batch)
        prior = data["source_batches"].get(batch.batch_key)
        if prior is not None:
            if prior["signature"] != batch_signature:
                _conflict("assignment_idempotency_conflict")
            return _record(data), tuple(
                AssignmentSourceEvent(
                    **self._event(transaction, fence.owner_id, fence.assignment_id, event_id)
                )
                for event_id in prior["event_ids"]
            )
        if data["state_version"] != expected_state_version:
            _conflict("assignment_revision_conflict")
        if batch.configuration_digest != digest(
            data["definition"]["source"]
        ) or batch.expected_cursor_digest != digest(data["checkpoint"].get("cursor")):
            _conflict("assignment_source_cursor_conflict")
        if len(data["source_batches"]) >= 32:
            # Cursor CAS and the relational event ledger protect older batches;
            # unchanged polling must not require infinite receipt storage.
            data["source_batches"].pop(next(iter(data["source_batches"])))
        count = transaction.fetch_one(
            "SELECT count(*) AS n FROM persistent_assignment_event WHERE assignment_id=%s",
            (fence.assignment_id,),
        )["n"]
        results = []
        seen = set()
        for event in batch.events:
            _uuid(event.event_id)
            for key in (event.source_key, event.item_key, event.source_revision):
                _text(key)
            if event.source_key != batch.source_key or event.event_id in seen:
                raise RepositoryValidationError("batch event identity invalid")
            seen.add(event.event_id)
            _digest(event.identity_digest)
            _digest(event.context_digest)
            canonical(event.context, 8192)
            if event.context_digest != digest(event.context) or event.disposition != "pending":
                raise RepositoryValidationError("source context or initial state invalid")
            row = transaction.fetch_one(
                "SELECT data FROM persistent_assignment_event "
                "WHERE assignment_id=%s AND owner_user_id=%s AND source_key=%s "
                "AND item_key=%s AND source_revision=%s",
                (
                    fence.assignment_id,
                    fence.owner_id,
                    event.source_key,
                    event.item_key,
                    event.source_revision,
                ),
            )
            if row:
                old = plain(row["data"])
                if (
                    old["context_digest"] != event.context_digest
                    or old["identity_digest"] != event.identity_digest
                ):
                    _conflict("assignment_idempotency_conflict")
                results.append(AssignmentSourceEvent(**old))
                continue
            if count >= 10000:
                _conflict("assignment_history_capacity_exhausted")
            count += 1
            transaction.execute(
                "INSERT INTO persistent_assignment_event "
                "(id,assignment_id,owner_user_id,source_key,item_key,source_revision,state,data) "
                "VALUES(%s,%s,%s,%s,%s,%s,'pending',%s::jsonb)",
                (
                    event.event_id,
                    fence.assignment_id,
                    fence.owner_id,
                    event.source_key,
                    event.item_key,
                    event.source_revision,
                    canonical(event),
                ),
            )
            results.append(event)
        data["source_batches"][batch.batch_key] = {
            "signature": batch_signature,
            "event_ids": [event.event_id for event in results],
        }
        data["checkpoint"].update(
            cursor=plain(batch.next_cursor),
            source_configuration_digest=batch.configuration_digest,
            last_batch_key=batch.batch_key,
        )
        data["last_check_at"] = plain(_now(transaction))
        return self._save(transaction, data), tuple(results)

    def list_events(
        self, query, *, owner_id, assignment_id, disposition=None, limit=100, after_id=None
    ):
        self._load(query, owner_id, assignment_id)
        _integer(limit, 1, 100)
        if after_id is not None:
            _uuid(after_id)
        rows = query.fetch_all(
            "SELECT data FROM persistent_assignment_event WHERE assignment_id=%s "
            "AND owner_user_id=%s AND (%s::text IS NULL OR state=%s) "
            "AND (%s::uuid IS NULL OR id>%s::uuid) ORDER BY id LIMIT %s",
            (assignment_id, owner_id, disposition, disposition, after_id, after_id, limit),
        )
        return tuple(AssignmentSourceEvent(**plain(row["data"])) for row in rows)

    def put_task_plan(
        self, transaction, *, fence, expected_state_version, plan_key, plan_digest, tasks
    ):
        data = self._fenced(transaction, fence)
        _text(plan_key)
        _digest(plan_digest)
        signature = digest(tasks)
        prior = data["plans"].get(plan_key)
        if prior:
            if prior != [plan_digest, signature]:
                _conflict("assignment_idempotency_conflict")
            return _record(data)
        if data["state_version"] != expected_state_version:
            _conflict("assignment_revision_conflict")
        limits = data["definition"]["limits"]
        if not 1 <= len(tasks) <= limits["max_tasks"] or len(data["plans"]) >= 256:
            _conflict("assignment_history_capacity_exhausted")
        for old in data["tasks"]:
            if old["state"] not in {"completed", "failed", "cancelled", "superseded"}:
                _conflict("assignment_task_plan_active")
            if old["state"] == "completed" and not old["incorporated_by"]:
                _conflict("assignment_task_result_unincorporated")
        values = plain(tasks)
        nodes = {node["task_id"]: node for node in values}
        if len(nodes) != len(tasks):
            raise RepositoryValidationError("duplicate task identity")
        for node in values:
            _text(node["task_id"], 128)
            _text(node["title"], 256)
            _text(node["instruction"], 4096)
            if (
                node["instruction_revision"] != fence.instruction_revision
                or node["plan_key"] != plan_key
                or node["state"] != "pending"
                or node["attempt_count"] != 0
                or node["result_digest"] is not None
                or node["incorporated_by"]
                or node["task_generation"] != 0
            ):
                raise RepositoryValidationError("new task has invalid initial authority/state")
            _integer(node["depth"], 0, limits["max_depth"])
            dependencies = node["depends_on"]
            if len(dependencies) > 8 or len(set(dependencies)) != len(dependencies):
                raise RepositoryValidationError("invalid dependencies")
            if any(dep not in nodes or dep == node["task_id"] for dep in dependencies):
                raise RepositoryValidationError("missing or self dependency")
            parent = nodes.get(node["parent_task_id"]) if node["parent_task_id"] else None
            if node["parent_task_id"] and (parent is None or parent["task_id"] == node["task_id"]):
                raise RepositoryValidationError("invalid task parent")
            allowed = parent["allowed_tools"] if parent else data["definition"]["allowed_tools"]
            if not set(node["allowed_tools"]).issubset(allowed):
                raise RepositoryValidationError("child authority exceeds parent")
            if node["depth"] != (parent["depth"] + 1 if parent else 0):
                raise RepositoryValidationError("invalid task depth")
            if sum(n["parent_task_id"] == node["task_id"] for n in values) > 5:
                raise RepositoryValidationError("task fanout exceeds bound")
            if node["event_id"]:
                self._event(transaction, fence.owner_id, fence.assignment_id, node["event_id"])
        visiting, visited = set(), set()

        def visit(task_id):
            if task_id in visiting:
                raise RepositoryValidationError("task graph cycle")
            if task_id in visited:
                return
            visiting.add(task_id)
            for dep in nodes[task_id]["depends_on"]:
                visit(dep)
            visiting.remove(task_id)
            visited.add(task_id)

        for task_id in nodes:
            visit(task_id)
        canonical(values, 196608)
        data["plans"][plan_key] = [plan_digest, signature]
        data.update(tasks=values, phase="investigating")
        return self._save(transaction, data)

    @staticmethod
    def _task(data, task_id):
        for task in data["tasks"]:
            if task["task_id"] == task_id:
                return task
        raise RepositoryNotFoundError("assignment task not found")

    def claim_task(self, transaction, *, fence, task_id, expected_task_generation):
        data = self._fenced(transaction, fence)
        task = self._task(data, task_id)
        limits = data["definition"]["limits"]
        if task["state"] != "pending" or task["task_generation"] != expected_task_generation:
            _conflict("assignment_task_claim_stale")
        if any(self._task(data, dep)["state"] != "completed" for dep in task["depends_on"]):
            _conflict("assignment_task_dependency_invalid")
        if task["attempt_count"] >= 1 + limits["max_retries"]:
            _conflict("assignment_retry_exhausted")
        if sum(t["state"] == "running" for t in data["tasks"]) >= limits["max_concurrent_tasks"]:
            _conflict("assignment_task_capacity_exhausted")
        task.update(
            state="running",
            attempt_count=task["attempt_count"] + 1,
            task_generation=task["task_generation"] + 1,
            claim_generation=fence.claim_generation,
        )
        self._save(transaction, data)
        return AssignmentTaskClaim(
            fence, task_id, task["task_generation"], task["attempt_count"], AssignmentTask(**task)
        )

    def complete_task(self, transaction, *, claim, result):
        data = self._fenced(transaction, claim.fence)
        task = self._task(data, claim.task_id)
        _digest(result.result_digest)
        canonical(result, 10000)
        if result.state not in {"completed", "failed", "cancelled", "reconciliation"}:
            raise RepositoryValidationError("invalid task result state")
        if task["task_generation"] != claim.task_generation:
            _conflict("assignment_task_claim_stale")
        if task["state"] != "running":
            if (
                task["result_digest"] == result.result_digest
                and task["bounded_result"] == result.bounded_result
                and task["state"] == result.state
                and task["provenance"] == plain(result.provenance)
            ):
                return _record(data)
            _conflict("assignment_result_conflict")
        if len(result.bounded_result.encode()) > 8192:
            raise RepositoryValidationError("task result exceeds bound")
        unresolved = transaction.fetch_one(
            "SELECT count(*) AS n FROM persistent_assignment_action "
            "WHERE assignment_id=%s AND data->'intent'->>'task_id'=%s "
            "AND state IN ('started','uncertain','reserved','proposed','approved')",
            (claim.fence.assignment_id, claim.task_id),
        )["n"]
        if unresolved and result.state == "completed":
            _conflict("assignment_action_uncertain")
        task.update(
            state=result.state,
            result_digest=result.result_digest,
            bounded_result=result.bounded_result,
            provenance=plain(result.provenance),
        )
        data["wake_generation"] += 1
        return self._save(transaction, data)

    def finish_episode(self, transaction, *, fence, completion):
        raw = self._load(transaction, fence.owner_id, fence.assignment_id, lock=True)
        _digest(completion.completion_digest)
        last = raw.get("last_completion")
        signature = digest(completion)
        if last and last["claim_generation"] == fence.claim_generation:
            if (
                last["signature"] != signature
                or raw["control_epoch"] != fence.control_epoch
                or last["claim_token"] != fence.claim_token
            ):
                _conflict("assignment_result_conflict")
            return _record(raw)
        data = self._fenced(transaction, fence, action_id=raw.get("approved_action_id"))
        if data["state_version"] != completion.expected_state_version:
            _conflict("assignment_revision_conflict")
        if completion.phase not in _PHASES:
            raise RepositoryValidationError("invalid assignment phase")
        if transaction.fetch_one(
            "SELECT count(*) AS n FROM persistent_assignment_action "
            "WHERE assignment_id=%s AND state='started'",
            (fence.assignment_id,),
        )["n"]:
            _conflict("assignment_action_in_flight")
        canonical(completion.checkpoint, 65536)
        for key in ("cursor", "source_configuration_digest", "last_batch_key"):
            if (
                key in data["checkpoint"]
                and plain(completion.checkpoint.get(key)) != data["checkpoint"][key]
            ):
                _conflict("assignment_source_cursor_conflict")
        for reference in completion.incorporations:
            task = self._task(data, reference["task_id"])
            parent = reference["parent_task_id"]
            if parent != (task["parent_task_id"] or "__assignment__"):
                _conflict("assignment_result_parent_conflict")
            if task["state"] != "completed" or task["result_digest"] != reference["result_digest"]:
                _conflict("assignment_result_conflict")
            old = task["incorporated_by"].get(parent)
            if old is not None and old != reference["result_digest"]:
                _conflict("assignment_result_conflict")
            task["incorporated_by"][parent] = reference["result_digest"]
        for receipt in completion.event_receipts:
            event = self._event(
                transaction, fence.owner_id, fence.assignment_id, receipt["event_id"]
            )
            if receipt["disposition"] not in {"completed", "irrelevant"}:
                raise RepositoryValidationError("invalid event completion")
            _digest(receipt["result_digest"])
            if event["disposition"] in {"completed", "irrelevant"} and (
                event["disposition"] != receipt["disposition"]
                or event["result_digest"] != receipt["result_digest"]
            ):
                _conflict("assignment_result_conflict")
            if any(
                t["event_id"] == event["event_id"]
                and t["state"] not in {"completed", "failed", "cancelled"}
                for t in data["tasks"]
            ):
                _conflict("assignment_task_dependency_invalid")
            if transaction.fetch_one(
                "SELECT count(*) AS n FROM persistent_assignment_action "
                "WHERE assignment_id=%s AND data->'intent'->>'event_id'=%s "
                "AND state IN ('reserved','started','uncertain','proposed','approved')",
                (fence.assignment_id, event["event_id"]),
            )["n"]:
                _conflict("assignment_action_uncertain")
            event.update(disposition=receipt["disposition"], result_digest=receipt["result_digest"])
            transaction.execute(
                "UPDATE persistent_assignment_event SET data=%s::jsonb,state=%s "
                "WHERE id=%s AND assignment_id=%s AND owner_user_id=%s",
                (
                    canonical(event),
                    event["disposition"],
                    event["event_id"],
                    fence.assignment_id,
                    fence.owner_id,
                ),
            )
        if completion.completed:
            unresolved = transaction.fetch_one(
                "SELECT count(*) AS n FROM persistent_assignment_action "
                "WHERE assignment_id=%s AND state IN "
                "('reserved','started','uncertain','proposed','approved')",
                (fence.assignment_id,),
            )["n"]
            if unresolved or any(
                t["state"] in {"pending", "running", "reconciliation"} for t in data["tasks"]
            ):
                _conflict("assignment_unfinished_work")
            data["lifecycle"] = "completed"
        now = _now(transaction)
        data.update(
            checkpoint=plain(completion.checkpoint),
            phase=completion.phase,
            safe_error_code=completion.safe_error_code,
            wake_reason=completion.wake_reason,
            last_completion={
                "claim_generation": fence.claim_generation,
                "claim_token": fence.claim_token,
                "signature": signature,
            },
        )
        if completion.phase in {"waiting", "failed"} and not completion.completed:
            due = _time(completion.next_wake_at) or now + timedelta(
                seconds=data["definition"]["limits"]["cadence_seconds"]
            )
            if data["last_check_at"] and completion.wake_reason == "cadence":
                due = max(
                    due,
                    _time(data["last_check_at"])
                    + timedelta(seconds=data["definition"]["limits"]["cadence_seconds"]),
                )
            if data["wake_generation"] > data["claimed_wake_generation"] and any(
                t["state"] == "completed" and not t["incorporated_by"] for t in data["tasks"]
            ):
                due = now
            data["next_wake_at"] = plain(max(due, now))
        else:
            data["next_wake_at"] = None
        if completion.phase == "failed":
            data["consecutive_failures"] += 1
            if data["consecutive_failures"] > data["definition"]["limits"]["max_retries"]:
                data.update(next_wake_at=None, safe_error_code="assignment_retry_exhausted")
            elif data["next_wake_at"] is not None:
                backoff = min(
                    data["definition"]["limits"]["cadence_seconds"]
                    * 2 ** (data["consecutive_failures"] - 1),
                    3600,
                )
                data["next_wake_at"] = plain(
                    max(_time(data["next_wake_at"]), now + timedelta(seconds=backoff))
                )
            data["next_retry_at"] = data["next_wake_at"]
        else:
            data.update(consecutive_failures=0, next_retry_at=None)
        if completion.activity is not None:
            self._activity(
                transaction,
                data,
                completion.activity,
                critical=completion.phase
                in {
                    "failed",
                    "reconciliation",
                    "waiting_authorization",
                    "waiting_approval",
                    "budget_exhausted",
                },
            )
        for row in transaction.fetch_all(
            "SELECT id FROM persistent_assignment_action "
            "WHERE assignment_id=%s AND state='reserved'",
            (fence.assignment_id,),
        ):
            reserved = self._action(
                transaction, fence.owner_id, fence.assignment_id, str(row["id"])
            )
            attempt = reserved["attempts"][-1]
            self._release(data, attempt["maximum"])
            reserved["state"] = attempt["state"] = "failed_not_started"
            self._save_action(transaction, reserved)
        for task in data["tasks"]:
            if task["state"] == "running":
                task["state"] = (
                    "reconciliation" if completion.phase == "reconciliation" else "pending"
                )
                task["task_generation"] += 1
        self._clear_claim(data)
        return self._save(transaction, data)

    def recover_expired_for_administration(self, transaction, *, limit=100):
        _integer(limit, 1, 100)
        rows = transaction.fetch_all(
            "SELECT id,owner_user_id FROM persistent_assignment "
            "WHERE lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at,id "
            "LIMIT %s FOR UPDATE SKIP LOCKED",
            (limit,),
        )
        reclaimed, bindings, uncertain = [], [], []
        for row in rows:
            data = self._load(transaction, row["owner_user_id"], str(row["id"]), lock=True)
            reclaimed.append(data["assignment_id"])
            if data["operation_binding"]:
                bindings.append(data["operation_binding"])
            pending = transaction.fetch_all(
                "SELECT id FROM persistent_assignment_action "
                "WHERE assignment_id=%s AND state IN ('reserved','started')",
                (data["assignment_id"],),
            )
            held = False
            for item in pending:
                action = self._action(
                    transaction, data["owner_id"], data["assignment_id"], str(item["id"])
                )
                attempt = action["attempts"][-1]
                if action["state"] == "reserved":
                    self._release(data, attempt["maximum"])
                    action["state"] = attempt["state"] = "failed_not_started"
                elif action["intent"]["boundary"] == "read_only":
                    self._release(data, attempt["maximum"])
                    self._day(data, _now(transaction))
                    for key in (*_DIMENSIONS, "spend_micro_units"):
                        amount = attempt["maximum"].get(key)
                        if amount is not None:
                            for bucket in ("spent", "daily"):
                                data["usage"][bucket][key] = (
                                    data["usage"][bucket].get(key, 0) + amount
                                )
                    action["state"] = attempt["state"] = "failed"
                    attempt["outcome"] = {
                        "outcome": "failed",
                        "result_digest": digest([action["action_id"], "read_interrupted"]),
                        "result": {},
                        "evidence_reference": "read_only_lease_expired",
                        "actual": None,
                    }
                    action["result"] = attempt["outcome"]
                else:
                    action["state"] = attempt["state"] = "uncertain"
                    held = True
                    uncertain.append(action["action_id"])
                    # Retain the maximum liability; a late exact receipt or explicit
                    # reconciliation must settle it. Never reset a begun effect.
                    action["result"] = {
                        "result_digest": digest([action["action_id"], "lease_expired"]),
                        "outcome": "uncertain",
                        "result": {},
                    }
                self._save_action(transaction, action)
            for task in data["tasks"]:
                if task["state"] == "running":
                    task["state"] = "reconciliation" if held else "pending"
                    task["task_generation"] += 1
            data["consecutive_failures"] += 1
            if data["lifecycle"] == "active":
                exhausted = (
                    data["consecutive_failures"] > data["definition"]["limits"]["max_retries"]
                )
                backoff = min(
                    data["definition"]["limits"]["cadence_seconds"]
                    * 2 ** min(data["consecutive_failures"] - 1, 10),
                    3600,
                )
                data.update(
                    phase="reconciliation" if held else "failed",
                    safe_error_code="assignment_action_uncertain"
                    if held
                    else "assignment_interrupted",
                    next_wake_at=None
                    if held or exhausted
                    else plain(_now(transaction) + timedelta(seconds=backoff)),
                )
                data["next_retry_at"] = data["next_wake_at"]
            self._clear_claim(data)
            self._save(transaction, data)
        return AssignmentRecoveryResult(tuple(reclaimed), tuple(bindings), tuple(uncertain))

    def _activity(self, transaction, data, activity, *, critical=False):
        _text(activity.activity_key)
        _text(activity.activity_type, 64)
        _text(activity.title, 256)
        canonical(activity, 12000)
        if activity.notification_state not in {"none", "pending"}:
            raise RepositoryValidationError("invalid initial notification state")
        old = transaction.fetch_one(
            "SELECT data FROM persistent_assignment_activity "
            "WHERE assignment_id=%s AND activity_key=%s",
            (data["assignment_id"], activity.activity_key),
        )
        signature = digest(
            [
                activity.activity_type,
                activity.title,
                activity.summary,
                activity.references,
                activity.notification_state,
            ]
        )
        if old:
            value = plain(old["data"])
            if value.pop("signature") != signature:
                _conflict("assignment_idempotency_conflict")
            value["created_at"] = _time(value["created_at"])
            return AssignmentActivityRecord(**value)
        count = transaction.fetch_one(
            "SELECT count(*) AS n FROM persistent_assignment_activity WHERE assignment_id=%s",
            (data["assignment_id"],),
        )["n"]
        if count >= 1000:
            if critical:
                # Assignment status and the retained control receipt still record
                # this owner decision when the activity projection is full.
                return None
            _conflict("assignment_history_capacity_exhausted")
        data["activity_sequence"] += 1
        value = plain(activity)
        value.update(
            activity_id=str(uuid.uuid4()),
            sequence=data["activity_sequence"],
            created_at=plain(_now(transaction)),
            signature=signature,
        )
        transaction.execute(
            "INSERT INTO persistent_assignment_activity "
            "(id,assignment_id,owner_user_id,activity_key,sequence,notification_state,data) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)",
            (
                value["activity_id"],
                data["assignment_id"],
                data["owner_id"],
                value["activity_key"],
                value["sequence"],
                value["notification_state"],
                canonical(value),
            ),
        )
        value.pop("signature")
        value["created_at"] = _time(value["created_at"])
        return AssignmentActivityRecord(**value)

    def append_activity(self, transaction, *, fence, activity):
        data = self._fenced(transaction, fence)
        result = self._activity(transaction, data, activity)
        self._save(transaction, data)
        return result

    def list_activity(self, query, *, owner_id, assignment_id, after_sequence=0, limit=100):
        self._load(query, owner_id, assignment_id)
        _integer(after_sequence)
        _integer(limit, 1, 100)
        rows = query.fetch_all(
            "SELECT data,notification_state FROM persistent_assignment_activity "
            "WHERE assignment_id=%s AND owner_user_id=%s AND sequence>%s "
            "ORDER BY sequence LIMIT %s",
            (assignment_id, owner_id, after_sequence, limit),
        )
        results = []
        for row in rows:
            value = plain(row["data"])
            value.pop("signature")
            value["created_at"] = _time(value["created_at"])
            value["notification_state"] = row["notification_state"]
            results.append(AssignmentActivityRecord(**value))
        return tuple(results)

    def mark_activity_notified(
        self, transaction, *, owner_id, assignment_id, activity_id, expected_state="pending"
    ):
        self._load(transaction, owner_id, assignment_id)
        _uuid(activity_id)
        if expected_state != "pending":
            raise RepositoryValidationError("only pending notifications can be claimed")
        row = transaction.fetch_one(
            "UPDATE persistent_assignment_activity SET notification_state='notified' "
            "WHERE id=%s AND assignment_id=%s AND owner_user_id=%s "
            "AND notification_state='pending' RETURNING id",
            (activity_id, assignment_id, owner_id),
        )
        return row is not None

    def retain_for_administration(self, transaction, *, limit=100):
        _integer(limit, 1, 100)
        # Payload retention is conservative: identity tombstones are never age-pruned.
        # Remove only old transient activity with no effect/approval references.
        rows = transaction.fetch_all(
            "SELECT id FROM persistent_assignment_activity "
            "WHERE notification_state!='pending' "
            "AND data->'references'='{}'::jsonb "
            "AND (data->>'created_at')::timestamptz < clock_timestamp()-interval '30 days' "
            "ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
            (limit,),
        )
        for row in rows:
            transaction.execute(
                "DELETE FROM persistent_assignment_activity WHERE id=%s", (row["id"],)
            )
        return AssignmentRetentionResult(activity_removals=len(rows))

    def delete_for_owner(self, transaction, *, owner_id, assignment_id, expected_control_epoch):
        data = self._load(transaction, owner_id, assignment_id, lock=True, required=False)
        if data is None:
            return False
        if (
            data["lifecycle"] not in _TERMINAL
            or data["control_epoch"] != expected_control_epoch
            or data["lease_expires_at"] is not None
        ):
            _conflict("assignment_not_terminal")
        pending = transaction.fetch_one(
            "SELECT count(*) AS n FROM persistent_assignment_action "
            "WHERE assignment_id=%s AND state IN ('started','uncertain','reserved')",
            (assignment_id,),
        )["n"]
        if pending:
            _conflict("assignment_action_uncertain")
        for row in transaction.fetch_all(
            "SELECT data->>'interactive_proposal_id' AS proposal_id "
            "FROM persistent_assignment_action WHERE assignment_id=%s AND owner_user_id=%s "
            "AND data->>'interactive_proposal_id' IS NOT NULL",
            (assignment_id, owner_id),
        ):
            self._expire_interactive_proposal(transaction, owner_id, row["proposal_id"])
        transaction.execute(
            "DELETE FROM persistent_assignment WHERE id=%s AND owner_user_id=%s",
            (assignment_id, owner_id),
        )
        return True

    def retire_owner(self, transaction, *, owner_id):
        """Fence account work before purge; unresolved effects require a later retry.

        The caller must commit a result with unresolved IDs, defer physical purge,
        and request reconciliation. Raising inside this transaction undoes fencing.
        """
        _text(owner_id)
        transaction.fetch_one("SELECT pg_advisory_xact_lock(hashtextextended(%s,79))", (owner_id,))
        transaction.execute(
            "INSERT INTO astralplane_blob_owner_state "
            "(owner_id,state,version,retired_at,updated_at) "
            "VALUES(%s,'retired',1,clock_timestamp(),clock_timestamp()) "
            "ON CONFLICT(owner_id) DO UPDATE SET state='retired', "
            "version=astralplane_blob_owner_state.version+"
            "CASE WHEN astralplane_blob_owner_state.state='active' THEN 1 ELSE 0 END, "
            "retired_at=COALESCE(astralplane_blob_owner_state.retired_at,clock_timestamp()), "
            "updated_at=clock_timestamp()",
            (owner_id,),
        )
        rows = transaction.fetch_all(
            "SELECT id FROM persistent_assignment WHERE owner_user_id=%s ORDER BY id FOR UPDATE",
            (owner_id,),
        )
        stopped, deleted, unresolved = [], [], []
        for row in rows:
            assignment_id = str(row["id"])
            data = self._load(transaction, owner_id, assignment_id, lock=True)
            if data["lifecycle"] not in _TERMINAL:
                self.apply_control(
                    transaction,
                    owner_id=owner_id,
                    assignment_id=assignment_id,
                    expected_instruction_revision=data["instruction_revision"],
                    expected_control_epoch=data["control_epoch"],
                    submission_id=str(uuid.uuid4()),
                    submission_digest=digest(["account_retirement", owner_id, assignment_id]),
                    control="stop",
                )
                data = self._load(transaction, owner_id, assignment_id, lock=True)
                stopped.append(assignment_id)
            pending = transaction.fetch_all(
                "SELECT id FROM persistent_assignment_action WHERE assignment_id=%s "
                "AND state IN ('started','uncertain','reserved')",
                (assignment_id,),
            )
            for item in transaction.fetch_all(
                "SELECT data->>'interactive_proposal_id' AS proposal_id "
                "FROM persistent_assignment_action "
                "WHERE assignment_id=%s AND data->>'interactive_proposal_id' IS NOT NULL",
                (assignment_id,),
            ):
                self._expire_interactive_proposal(transaction, owner_id, item["proposal_id"])
            if pending:
                unresolved.extend(str(item["id"]) for item in pending)
            else:
                self.delete_for_owner(
                    transaction,
                    owner_id=owner_id,
                    assignment_id=assignment_id,
                    expected_control_epoch=data["control_epoch"],
                )
                deleted.append(assignment_id)
        return AssignmentOwnerRetirementResult(tuple(stopped), tuple(deleted), tuple(unresolved))
