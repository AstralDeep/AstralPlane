"""Tests for src/astralplane/repositories/voice.py: guidance-clock reads fenced over
real PostgreSQL rows, exact-reference selection, nowait lock release, and
DB-clock-based expiry; external admission stays host-owned.
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from test_assignments_postgres import database as database
from test_assignments_postgres import independent_database
from test_authority_clock_reads_postgres import running, schema_of
from test_authority_clock_reads_postgres import work as work

from astralplane.repositories import RepositoryConflictError, RepositoryValidationError
from astralplane.repositories.voice import (
    VoiceRepository,
    VoiceSessionCreate,
    VoiceTurnCreate,
    VoiceTurnState,
)


def uid():
    return str(uuid.uuid4())


def live_turn(database, work, *, backend="client_local"):
    repository = VoiceRepository()
    fence = running(database, work)
    with database.transaction() as tx:
        tx.execute("DELETE FROM voice_turn")
        tx.execute("DELETE FROM voice_session")
        now = tx.fetch_one("SELECT clock_timestamp() AS now")["now"]
        remote = backend == "llm_factory"
        session = repository.create_session(
            tx,
            VoiceSessionCreate(
                session_id=uid(),
                owner_id="clock-owner",
                activation_id=uid(),
                device_id=uid(),
                device_kind="web",
                speech_backend=backend,
                transport="livekit" if remote else "client_local",
                room_name="synthetic-room" if remote else None,
                participant_identity="synthetic-participant" if remote else None,
                visible_chat_id="synthetic-chat",
                owner_connection_generation=uid(),
                control_binding_id=uid(),
                control_binding_expires_at=now + timedelta(minutes=2),
                lease_expires_at=now + timedelta(minutes=2),
                started_at=now,
                media_grant_nonce_hash=b"x" * 32 if remote else None,
                media_grant_issued_at=now if remote else None,
                media_grant_expires_at=now + timedelta(minutes=1) if remote else None,
            ),
        )
        repository.patch_session_record(
            tx,
            owner_id=session.owner_id,
            session_id=session.session_id,
            updates={"state": "active"},
        )
        turn = repository.create_turn(
            tx,
            VoiceTurnCreate(
                turn_id=uid(),
                client_turn_id=uid(),
                session_id=session.session_id,
                session_generation=1,
                media_grant_revision=1,
                owner_id=session.owner_id,
                chat_id=session.visible_chat_id,
                chat_context_revision=1,
                execution_base_render_revision=0,
                submission_id=uid(),
                request_generation=uid(),
            ),
        )
        repository.transition_turn(
            tx,
            owner_id=session.owner_id,
            turn_id=turn.turn_id,
            expected_state=VoiceTurnState.RECOGNIZING,
            state=VoiceTurnState.ACCEPTED,
            operation_id=str(fence.operation_id),
            result_id=None,
            now=now,
        )
        return (
            repository,
            fence,
            dict(
                owner_id=session.owner_id,
                session_id=session.session_id,
                turn_id=turn.turn_id,
                expected_session_generation=1,
                expected_media_grant_revision=1,
                operation_id=str(fence.operation_id),
            ),
        )


@pytest.mark.parametrize("backend", ["client_local", "llm_factory"])
@pytest.mark.parametrize("state", ["accepted", "processing", "waiting_on_user"])
def test_current_voice_observation_is_detached_and_does_not_renew(database, work, backend, state):
    repo, fence, args = live_turn(database, work, backend=backend)
    with database.transaction() as tx:
        repo.patch_turn_record(
            tx, owner_id=args["owner_id"], turn_id=args["turn_id"], updates={"state": state}
        )
        session = repo.get_session_record(
            tx, owner_id=args["owner_id"], session_id=args["session_id"]
        )
        turn = repo.get_turn_record(tx, owner_id=args["owner_id"], turn_id=args["turn_id"])
        work.assert_current_execution_lease(tx, fence)
        observed = repo.assert_current_guidance_turn(tx, **args)
        assert observed.session == session and observed.turn == turn
        assert observed.observed_at < session["lease_expires_at"]
        assert args["owner_id"] not in repr(observed)
        with pytest.raises(TypeError):
            observed.turn["state"] = "failed"
        assert (
            repo.get_session_record(tx, owner_id=args["owner_id"], session_id=args["session_id"])
            == session
        )


@pytest.mark.parametrize(
    "change",
    [
        {"owner_id": "foreign"},
        {"session_id": "missing"},
        {"turn_id": "missing"},
        {"operation_id": "missing"},
        {"expected_session_generation": 2},
        {"expected_media_grant_revision": 2},
    ],
)
def test_voice_exact_reference_never_selects_replacement(database, work, change):
    repo, _, args = live_turn(database, work)
    args.update({key: uid() if value == "missing" else value for key, value in change.items()})
    with database.transaction() as tx:
        with pytest.raises(RepositoryConflictError):
            repo.assert_current_guidance_turn(tx, **args)
        assert tx.fetch_one("SELECT 1 AS usable")["usable"] == 1


@pytest.mark.parametrize(
    "damage",
    [
        "expired",
        "future",
        "suspended",
        "ended",
        "media",
        "recognizing",
        "accepted_missing",
        "terminal",
        "chat",
    ],
)
def test_voice_unavailable_state_refuses(database, work, damage):
    repo, _, args = live_turn(database, work)
    with database.transaction() as tx:
        now = tx.fetch_one("SELECT clock_timestamp() AS now")["now"]
        session_changes = {}
        turn_changes = {}
        if damage == "expired":
            session_changes["lease_expires_at"] = now
        elif damage == "future":
            tx.execute(
                "UPDATE voice_session SET started_at=clock_timestamp()+interval '1 second' "
                "WHERE session_id=%s",
                (args["session_id"],),
            )
        elif damage == "suspended":
            session_changes["state"] = "suspended"
        elif damage == "ended":
            repo.end_session(
                tx,
                owner_id=args["owner_id"],
                session_id=args["session_id"],
                expected_generation=1,
                reason="user",
                ended_at=now,
            )
        elif damage == "media":
            session_changes["media_grant_revision"] = 2
        elif damage == "recognizing":
            turn_changes["state"] = "recognizing"
        elif damage == "accepted_missing":
            turn_changes["accepted_at"] = None
        elif damage == "terminal":
            repo.transition_turn(
                tx,
                owner_id=args["owner_id"],
                turn_id=args["turn_id"],
                expected_state=VoiceTurnState.ACCEPTED,
                state=VoiceTurnState.SUCCEEDED,
                operation_id=args["operation_id"],
                result_id=None,
                now=now,
            )
        elif damage == "chat":
            repo.end_session(
                tx,
                owner_id=args["owner_id"],
                session_id=args["session_id"],
                expected_generation=1,
                reason="chat_deleted",
                ended_at=now,
            )
        if session_changes:
            repo.patch_session_record(
                tx,
                owner_id=args["owner_id"],
                session_id=args["session_id"],
                updates=session_changes,
            )
        if turn_changes:
            repo.patch_turn_record(
                tx, owner_id=args["owner_id"], turn_id=args["turn_id"], updates=turn_changes
            )
        with pytest.raises(RepositoryConflictError):
            repo.assert_current_guidance_turn(tx, **args)


@pytest.mark.parametrize("lock", ["session", "turn"])
def test_nowait_refusal_releases_partial_locks_for_reverse_writer(database, work, lock):
    repo, fence, args = live_turn(database, work)
    schema = schema_of(database)
    with database.transaction() as writer:
        if lock == "turn":
            repo.get_turn_record(
                writer, owner_id=args["owner_id"], turn_id=args["turn_id"], for_update=True
            )
        else:
            repo.get_session_record(
                writer, owner_id=args["owner_id"], session_id=args["session_id"], for_update=True
            )
        with independent_database(schema) as db, db.transaction() as reader:
            work.assert_current_execution_lease(reader, fence)
            with pytest.raises(RepositoryConflictError, match="voice guidance is unavailable"):
                repo.assert_current_guidance_turn(reader, **args)
            assert reader.fetch_one("SELECT 1 AS usable")["usable"] == 1
            writer.execute("SET LOCAL lock_timeout='500ms'")
            repo.get_session_record(
                writer, owner_id=args["owner_id"], session_id=args["session_id"], for_update=True
            )
        repo.patch_turn_record(
            writer,
            owner_id=args["owner_id"],
            turn_id=args["turn_id"],
            updates={"state": "processing"},
        )
    with database.transaction() as tx:
        assert repo.assert_current_guidance_turn(tx, **args).turn["state"] == "processing"


def test_voice_expiry_after_prior_operation_wait_uses_fresh_db_clock(database, work):
    repo, fence, args = live_turn(database, work)
    schema = schema_of(database)
    entered = Event()
    from test_authority_clock_reads_postgres import WatchSQL

    def reader():
        with independent_database(schema) as db, db.transaction() as tx:
            work.assert_current_execution_lease(WatchSQL(tx, entered), fence)
            with pytest.raises(RepositoryConflictError):
                repo.assert_current_guidance_turn(tx, **args)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.transaction() as tx:
            tx.fetch_one(
                "SELECT operation_id FROM operation_record WHERE operation_id=%s FOR UPDATE",
                (args["operation_id"],),
            )
            tx.execute(
                "UPDATE voice_session SET lease_expires_at="
                "clock_timestamp()+interval '150 milliseconds' WHERE session_id=%s",
                (args["session_id"],),
            )
            future = executor.submit(reader)
            assert entered.wait(5)
            time.sleep(0.25)
            assert not future.done()
        future.result(timeout=5)


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", ""),
        ("session_id", "bad"),
        ("turn_id", "bad"),
        ("operation_id", "bad"),
        ("expected_session_generation", True),
        ("expected_media_grant_revision", 0),
        ("expected_session_generation", 2**63),
    ],
)
def test_voice_malformed_input_never_reaches_sql(field, value):
    class NoSQL:
        def savepoint(self, *args):
            pytest.fail("malformed observation reached SQL")

    args = dict(
        owner_id="owner",
        session_id=uid(),
        turn_id=uid(),
        operation_id=uid(),
        expected_session_generation=1,
        expected_media_grant_revision=1,
    )
    args[field] = value
    with pytest.raises(RepositoryValidationError):
        VoiceRepository().assert_current_guidance_turn(NoSQL(), **args)


@pytest.mark.parametrize("damage", ["clock", "bool_counter", "backend", "cancel"])
def test_voice_read_fault_or_cancellation_releases_partial_locks(database, work, damage):
    repo, _, args = live_turn(database, work)
    schema = schema_of(database)

    class FaultedRead:
        def __init__(self, tx):
            self.tx = tx

        def __getattr__(self, name):
            return getattr(self.tx, name)

        def fetch_one(self, sql, params=()):
            if damage == "cancel" and "FROM voice_turn" in sql:
                raise KeyboardInterrupt
            row = self.tx.fetch_one(sql, params)
            if "FROM voice_session" in sql:
                if damage == "bool_counter":
                    return dict(row, generation=True)
                if damage == "backend":
                    return dict(row, speech_backend="unknown")
            if damage == "clock" and "clock_timestamp()" in sql:
                return {"now": None}
            return row

    with database.transaction() as tx:
        with pytest.raises(KeyboardInterrupt if damage == "cancel" else RepositoryConflictError):
            repo.assert_current_guidance_turn(FaultedRead(tx), **args)
        assert tx.fetch_one("SELECT 1 AS usable")["usable"] == 1
        with independent_database(schema) as db, db.transaction() as other:
            other.fetch_one(
                "SELECT session_id FROM voice_session WHERE session_id=%s FOR UPDATE NOWAIT",
                (args["session_id"],),
            )
    with database.transaction() as tx:
        assert repo.assert_current_guidance_turn(tx, **args).turn["state"] == "accepted"
