"""Durable voice session and turn metadata without real-time media behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from astralplane.contracts import Record, Transaction
from astralplane.errors import PlaneError


class VoiceSessionState(StrEnum):
    STARTING = "starting"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    RECONNECTING = "reconnecting"
    ENDING = "ending"
    ENDED = "ended"
    ERROR = "error"


class VoiceTurnState(StrEnum):
    RECOGNIZING = "recognizing"
    SUBMITTING = "submitting"
    ACCEPTED = "accepted"
    PROCESSING = "processing"
    WAITING_ON_USER = "waiting_on_user"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REFUSED = "refused"
    CANCELLED = "cancelled"
    ABANDONED = "abandoned"


_TERMINAL_TURN_STATES = {
    VoiceTurnState.SUCCEEDED,
    VoiceTurnState.FAILED,
    VoiceTurnState.REFUSED,
    VoiceTurnState.CANCELLED,
    VoiceTurnState.ABANDONED,
}


@dataclass(frozen=True, slots=True)
class VoiceSessionCreate:
    session_id: str
    owner_id: str
    activation_id: str
    device_id: str
    device_kind: str
    transport: str
    room_name: str
    participant_identity: str
    visible_chat_id: str
    owner_connection_generation: str
    control_binding_id: str
    control_binding_expires_at: datetime
    lease_expires_at: datetime
    media_grant_nonce_hash: bytes
    media_grant_issued_at: datetime
    media_grant_expires_at: datetime
    started_at: datetime

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("session_id", self.session_id, 64),
            ("owner_id", self.owner_id, 512),
            ("activation_id", self.activation_id, 64),
            ("device_id", self.device_id, 64),
            ("room_name", self.room_name, 255),
            ("participant_identity", self.participant_identity, 255),
            ("visible_chat_id", self.visible_chat_id, 255),
            ("owner_connection_generation", self.owner_connection_generation, 64),
            ("control_binding_id", self.control_binding_id, 64),
        ):
            _required(name, value, maximum)
        if self.device_kind not in {"web", "windows", "android", "ios", "macos", "watchos"}:
            raise ValueError("device_kind is not supported")
        if self.transport not in {"livekit", "watch_pcm_websocket"}:
            raise ValueError("transport metadata is not supported")
        if len(self.media_grant_nonce_hash) != 32:
            raise ValueError("media_grant_nonce_hash must contain 32 bytes")
        for name, value in (
            ("control_binding_expires_at", self.control_binding_expires_at),
            ("lease_expires_at", self.lease_expires_at),
            ("media_grant_issued_at", self.media_grant_issued_at),
            ("media_grant_expires_at", self.media_grant_expires_at),
            ("started_at", self.started_at),
        ):
            _aware(name, value)
        if min(self.control_binding_expires_at, self.lease_expires_at) <= self.started_at:
            raise ValueError("session leases must expire after the session starts")
        if self.media_grant_expires_at <= self.media_grant_issued_at:
            raise ValueError("grant expiry must follow grant issue time")


@dataclass(frozen=True, slots=True)
class VoiceSession:
    session_id: str
    owner_id: str
    activation_id: str
    device_id: str
    device_kind: str
    transport: str
    visible_chat_id: str
    chat_context_revision: int
    state: VoiceSessionState
    generation: int
    owner_connection_generation: str
    lease_expires_at: datetime
    started_at: datetime
    updated_at: datetime
    ended_at: datetime | None
    end_reason: str | None


@dataclass(frozen=True, slots=True)
class VoiceTurnCreate:
    turn_id: str
    client_turn_id: str
    session_id: str
    session_generation: int
    media_grant_revision: int
    owner_id: str
    chat_id: str
    chat_context_revision: int
    execution_base_render_revision: int
    submission_id: str
    request_generation: str

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("turn_id", self.turn_id, 64),
            ("client_turn_id", self.client_turn_id, 64),
            ("session_id", self.session_id, 64),
            ("owner_id", self.owner_id, 512),
            ("chat_id", self.chat_id, 255),
            ("submission_id", self.submission_id, 64),
            ("request_generation", self.request_generation, 64),
        ):
            _required(name, value, maximum)
        if (
            min(
                self.session_generation,
                self.media_grant_revision,
                self.chat_context_revision,
            )
            <= 0
        ):
            raise ValueError("voice turn generations must be positive")
        if self.execution_base_render_revision < 0:
            raise ValueError("execution_base_render_revision cannot be negative")


@dataclass(frozen=True, slots=True)
class VoiceTurn:
    turn_id: str
    client_turn_id: str
    session_id: str
    owner_id: str
    chat_id: str
    submission_id: str
    request_generation: str
    state: VoiceTurnState
    operation_id: str | None
    result_id: str | None
    terminal_kind: str | None
    accepted_at: datetime | None
    terminal_at: datetime | None
    created_at: datetime
    updated_at: datetime


class VoiceRepository:
    """Persist supplied metadata; media workers and transport remain product-owned."""

    def create_session(self, transaction: Transaction, session: VoiceSessionCreate) -> VoiceSession:
        row = transaction.fetch_one(
            """
            INSERT INTO voice_session (
                session_id, user_id, activation_id, device_id, device_kind,
                transport, room_name, participant_identity, visible_chat_id,
                owner_connection_generation, control_binding_id,
                control_binding_expires_at, lease_expires_at,
                media_grant_nonce_hash, media_grant_issued_at,
                media_grant_expires_at, started_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (user_id, activation_id) DO NOTHING
            RETURNING *
            """,
            (
                session.session_id,
                session.owner_id,
                session.activation_id,
                session.device_id,
                session.device_kind,
                session.transport,
                session.room_name,
                session.participant_identity,
                session.visible_chat_id,
                session.owner_connection_generation,
                session.control_binding_id,
                session.control_binding_expires_at,
                session.lease_expires_at,
                session.media_grant_nonce_hash,
                session.media_grant_issued_at,
                session.media_grant_expires_at,
                session.started_at,
                session.started_at,
            ),
        )
        if row is None:
            row = transaction.fetch_one(
                "SELECT * FROM voice_session WHERE user_id = %s AND activation_id = %s",
                (session.owner_id, session.activation_id),
            )
        if row is None or not _same_session(row, session):
            raise PlaneError(
                "voice activation identity has conflicting semantics",
                code="voice_session_idempotency_conflict",
                metadata={"owner_id": session.owner_id},
            )
        return _session(row)

    def get_session(
        self, transaction: Transaction, *, owner_id: str, session_id: str
    ) -> VoiceSession | None:
        row = transaction.fetch_one(
            "SELECT * FROM voice_session WHERE session_id = %s AND user_id = %s",
            (session_id, owner_id),
        )
        return None if row is None else _session(row)

    def get_live_session(self, transaction: Transaction, *, owner_id: str) -> VoiceSession | None:
        row = transaction.fetch_one(
            """
            SELECT * FROM voice_session
            WHERE user_id = %s AND ended_at IS NULL
            ORDER BY started_at DESC LIMIT 1
            """,
            (owner_id,),
        )
        return None if row is None else _session(row)

    def advance_session(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        session_id: str,
        expected_generation: int,
        expected_context_revision: int,
        state: VoiceSessionState,
        visible_chat_id: str,
        lease_expires_at: datetime,
        now: datetime,
    ) -> VoiceSession:
        if state is VoiceSessionState.ENDED:
            raise ValueError("end_session owns the terminal transition")
        _required("visible_chat_id", visible_chat_id, 255)
        _aware("lease_expires_at", lease_expires_at)
        _aware("now", now)
        if min(expected_generation, expected_context_revision) <= 0 or lease_expires_at <= now:
            raise ValueError(
                "session transition requires current positive fences and a future lease"
            )
        row = transaction.fetch_one(
            """
            UPDATE voice_session
            SET state = %s, visible_chat_id = %s,
                chat_context_revision = chat_context_revision + 1,
                generation = generation + 1, lease_expires_at = %s,
                updated_at = %s
            WHERE session_id = %s AND user_id = %s AND ended_at IS NULL
              AND generation = %s AND chat_context_revision = %s
            RETURNING *
            """,
            (
                state.value,
                visible_chat_id,
                lease_expires_at,
                now,
                session_id,
                owner_id,
                expected_generation,
                expected_context_revision,
            ),
        )
        return _required_session(row, owner_id)

    def end_session(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        session_id: str,
        expected_generation: int,
        reason: str,
        ended_at: datetime,
    ) -> VoiceSession:
        if reason not in {
            "user",
            "idle",
            "takeover",
            "logout",
            "auth_expired",
            "chat_deleted",
            "chat_unauthorized",
            "lease_expired",
            "media_error",
            "shutdown",
        }:
            raise ValueError("voice session end reason is not supported")
        _aware("ended_at", ended_at)
        row = transaction.fetch_one(
            """
            UPDATE voice_session
            SET state = 'ended', end_reason = %s, ended_at = %s,
                chat_unavailable_at = CASE
                    WHEN %s IN ('chat_deleted', 'chat_unauthorized') THEN %s
                    ELSE NULL
                END,
                generation = generation + 1, updated_at = %s
            WHERE session_id = %s AND user_id = %s AND ended_at IS NULL
              AND generation = %s
            RETURNING *
            """,
            (
                reason,
                ended_at,
                reason,
                ended_at,
                ended_at,
                session_id,
                owner_id,
                expected_generation,
            ),
        )
        return _required_session(row, owner_id)

    def create_turn(self, transaction: Transaction, turn: VoiceTurnCreate) -> VoiceTurn:
        row = transaction.fetch_one(
            """
            INSERT INTO voice_turn (
                turn_id, client_turn_id, session_id, session_generation,
                media_grant_revision, user_id, chat_id, chat_context_revision,
                execution_base_render_revision, submission_id, request_generation
            ) SELECT %s, %s, session_id, %s, %s, user_id, %s, %s, %s, %s, %s
                FROM voice_session
               WHERE session_id = %s AND user_id = %s AND ended_at IS NULL
                 AND generation = %s AND chat_context_revision = %s
                 AND visible_chat_id = %s AND media_grant_revision = %s
            ON CONFLICT (user_id, submission_id) DO NOTHING
            RETURNING *
            """,
            (
                turn.turn_id,
                turn.client_turn_id,
                turn.session_generation,
                turn.media_grant_revision,
                turn.chat_id,
                turn.chat_context_revision,
                turn.execution_base_render_revision,
                turn.submission_id,
                turn.request_generation,
                turn.session_id,
                turn.owner_id,
                turn.session_generation,
                turn.chat_context_revision,
                turn.chat_id,
                turn.media_grant_revision,
            ),
        )
        if row is None:
            row = transaction.fetch_one(
                "SELECT * FROM voice_turn WHERE user_id = %s AND submission_id = %s",
                (turn.owner_id, turn.submission_id),
            )
        if row is None or not _same_turn(row, turn):
            raise PlaneError(
                "voice turn submission has conflicting semantics or a stale session fence",
                code="voice_turn_idempotency_conflict",
                metadata={"owner_id": turn.owner_id},
            )
        return _turn(row)

    def get_turn(
        self, transaction: Transaction, *, owner_id: str, turn_id: str
    ) -> VoiceTurn | None:
        row = transaction.fetch_one(
            "SELECT * FROM voice_turn WHERE turn_id = %s AND user_id = %s",
            (turn_id, owner_id),
        )
        return None if row is None else _turn(row)

    def transition_turn(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        turn_id: str,
        expected_state: VoiceTurnState,
        state: VoiceTurnState,
        operation_id: str | None,
        result_id: str | None,
        now: datetime,
    ) -> VoiceTurn:
        _aware("now", now)
        terminal = state in _TERMINAL_TURN_STATES
        row = transaction.fetch_one(
            """
            UPDATE voice_turn
            SET state = %s, operation_id = COALESCE(operation_id, %s),
                result_id = COALESCE(result_id, %s), terminal_kind = %s,
                accepted_at = CASE WHEN %s = 'accepted' THEN COALESCE(accepted_at, %s)
                                   ELSE accepted_at END,
                terminal_at = %s, updated_at = %s
            WHERE turn_id = %s AND user_id = %s AND state = %s
              AND state NOT IN (
                  'succeeded', 'failed', 'refused', 'cancelled', 'abandoned'
              )
              AND (%s OR operation_id IS NULL OR operation_id::text = %s)
              AND (%s OR result_id IS NULL OR result_id = %s)
            RETURNING *
            """,
            (
                state.value,
                operation_id,
                result_id,
                state.value if terminal else None,
                state.value,
                now,
                now if terminal else None,
                now,
                turn_id,
                owner_id,
                expected_state.value,
                operation_id is None,
                operation_id,
                result_id is None,
                result_id,
            ),
        )
        if row is None:
            raise PlaneError(
                "voice turn state fence is stale",
                code="voice_turn_state_conflict",
                metadata={"owner_id": owner_id},
            )
        return _turn(row)


def _required(name: str, value: str, maximum: int) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a non-empty string of at most {maximum} characters")


def _aware(name: str, value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _same_session(row: Record, session: VoiceSessionCreate) -> bool:
    return all(
        (
            str(row["session_id"]) == session.session_id,
            str(row["user_id"]) == session.owner_id,
            str(row["activation_id"]) == session.activation_id,
            str(row["device_id"]) == session.device_id,
            str(row["device_kind"]) == session.device_kind,
            str(row["transport"]) == session.transport,
            str(row["room_name"]) == session.room_name,
            str(row["participant_identity"]) == session.participant_identity,
            str(row["visible_chat_id"]) == session.visible_chat_id,
            str(row["owner_connection_generation"]) == session.owner_connection_generation,
            str(row["control_binding_id"]) == session.control_binding_id,
            row["control_binding_expires_at"] == session.control_binding_expires_at,
            row["lease_expires_at"] == session.lease_expires_at,
            bytes(row["media_grant_nonce_hash"]) == session.media_grant_nonce_hash,
            row["media_grant_issued_at"] == session.media_grant_issued_at,
            row["media_grant_expires_at"] == session.media_grant_expires_at,
            row["started_at"] == session.started_at,
        )
    )


def _session(row: Record) -> VoiceSession:
    return VoiceSession(
        session_id=str(row["session_id"]),
        owner_id=str(row["user_id"]),
        activation_id=str(row["activation_id"]),
        device_id=str(row["device_id"]),
        device_kind=str(row["device_kind"]),
        transport=str(row["transport"]),
        visible_chat_id=str(row["visible_chat_id"]),
        chat_context_revision=int(row["chat_context_revision"]),
        state=VoiceSessionState(str(row["state"])),
        generation=int(row["generation"]),
        owner_connection_generation=str(row["owner_connection_generation"]),
        lease_expires_at=row["lease_expires_at"],
        started_at=row["started_at"],
        updated_at=row["updated_at"],
        ended_at=row.get("ended_at"),
        end_reason=None if row.get("end_reason") is None else str(row["end_reason"]),
    )


def _required_session(row: Record | None, owner_id: str) -> VoiceSession:
    if row is None:
        raise PlaneError(
            "voice session owner or generation fence is stale",
            code="voice_session_state_conflict",
            metadata={"owner_id": owner_id},
        )
    return _session(row)


def _same_turn(row: Record, turn: VoiceTurnCreate) -> bool:
    return all(
        (
            str(row["turn_id"]) == turn.turn_id,
            str(row["client_turn_id"]) == turn.client_turn_id,
            str(row["session_id"]) == turn.session_id,
            int(row["session_generation"]) == turn.session_generation,
            int(row["media_grant_revision"]) == turn.media_grant_revision,
            str(row["user_id"]) == turn.owner_id,
            str(row["chat_id"]) == turn.chat_id,
            int(row["chat_context_revision"]) == turn.chat_context_revision,
            int(row["execution_base_render_revision"]) == turn.execution_base_render_revision,
            str(row["submission_id"]) == turn.submission_id,
            str(row["request_generation"]) == turn.request_generation,
        )
    )


def _turn(row: Record) -> VoiceTurn:
    return VoiceTurn(
        turn_id=str(row["turn_id"]),
        client_turn_id=str(row["client_turn_id"]),
        session_id=str(row["session_id"]),
        owner_id=str(row["user_id"]),
        chat_id=str(row["chat_id"]),
        submission_id=str(row["submission_id"]),
        request_generation=str(row["request_generation"]),
        state=VoiceTurnState(str(row["state"])),
        operation_id=None if row.get("operation_id") is None else str(row["operation_id"]),
        result_id=None if row.get("result_id") is None else str(row["result_id"]),
        terminal_kind=None if row.get("terminal_kind") is None else str(row["terminal_kind"]),
        accepted_at=row.get("accepted_at"),
        terminal_at=row.get("terminal_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


__all__ = (
    "VoiceRepository",
    "VoiceSession",
    "VoiceSessionCreate",
    "VoiceSessionState",
    "VoiceTurn",
    "VoiceTurnCreate",
    "VoiceTurnState",
)
