"""Exercise owner-scoped operation reads while PostgreSQL callers serialize source transitions.
The public read lock keeps completion subscription registration in the caller's transaction.
"""

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from test_assignments_postgres import control, create_operation

from astralplane.api import create_repository_catalog
from astralplane.repositories import RepositoryValidationError
from tests.integration.test_lifecycle_acceptance import (
    concurrent_database,
    wait_for_blocked_statement,
)


@pytest.mark.parametrize("first", ("read", "cancel"))
def test_locked_source_read_serializes_completion_subscription_and_cancel(migrated_clone, first):
    catalog = create_repository_catalog()
    repo = catalog.assignments
    with migrated_clone.database.transaction() as tx:
        source = create_operation(repo, tx)
    entered = Event()
    state = {}

    def register(tx):
        observed = repo.get_operation(
            tx, owner_id="owner", assignment_id=source.assignment_id, lock=True
        )
        if observed.disposition == "queued":
            catalog.completion_wake.register_subscription(
                tx,
                owner_id="owner",
                waiter_operation_id="10000000-0000-4000-8000-000000000001",
                waiter_owner_id="owner",
                source_operation_id=source.assignment_id,
                source_owner_id="owner",
                terminal_condition="cancelled",
                source_revision=source.state_version,
                current_revision_fence=source.state_version + 1,
                created_at=100,
            )
        return observed

    def competing(database):
        with database.transaction() as tx:
            state["pid"] = tx.fetch_one("SELECT pg_backend_pid() AS pid")["pid"]
            entered.set()
            if first == "read":
                return control(repo, tx, source, "stop")
            return register(tx)

    with (
        concurrent_database(migrated_clone) as database,
        concurrent_database(migrated_clone) as observation,
        ThreadPoolExecutor(max_workers=1) as workers,
    ):
        with migrated_clone.database.transaction() as holder:
            blocking_pid = holder.fetch_one("SELECT pg_backend_pid() AS pid")["pid"]
            if first == "read":
                assert register(holder).disposition == "queued"
            else:
                control(repo, holder, source, "stop")
            pending = workers.submit(competing, database)
            assert entered.wait(timeout=30)
            wait_for_blocked_statement(observation, state["pid"], blocking_pid)
        result = pending.result(timeout=30)
    if first == "read":
        assert result.assignment.lifecycle == "stopped"
    else:
        assert result.disposition == "cancelled"
    with migrated_clone.database.transaction() as tx:
        current = repo.get_operation(tx, owner_id="owner", assignment_id=source.assignment_id)
        assert current.disposition == "cancelled"
        row = tx.fetch_one("SELECT count(*) AS count FROM completion_subscription")
        assert row["count"] == (1 if first == "read" else 0)


@pytest.mark.parametrize("options", ({}, {"lock": False}))
def test_default_operation_read_does_not_lock_source_mutation(migrated_clone, options):
    repo = create_repository_catalog().assignments
    with migrated_clone.database.transaction() as tx:
        source = create_operation(repo, tx)

    def read(database):
        with database.transaction() as tx:
            return repo.get_operation(
                tx, owner_id="owner", assignment_id=source.assignment_id, **options
            )

    with (
        concurrent_database(migrated_clone) as database,
        ThreadPoolExecutor(max_workers=1) as workers,
        migrated_clone.database.transaction() as holder,
    ):
        control(repo, holder, source, "stop")
        assert workers.submit(read, database).result(timeout=30).disposition == "queued"


def test_locked_operation_read_keeps_owner_predicate(migrated_clone):
    repo = create_repository_catalog().assignments
    with migrated_clone.database.transaction() as tx:
        source = create_operation(repo, tx)
        assert (
            repo.get_operation(
                tx, owner_id="foreign", assignment_id=source.assignment_id, lock=True
            )
            is None
        )


@pytest.mark.parametrize("lock", (0, 1, None, "yes", [], {}))
def test_operation_read_refuses_non_boolean_lock(migrated_clone, lock):
    repo = create_repository_catalog().assignments
    with (
        migrated_clone.database.transaction() as tx,
        pytest.raises(RepositoryValidationError, match="lock must be a boolean"),
    ):
        repo.get_operation(
            tx,
            owner_id="owner",
            assignment_id="10000000-0000-4000-8000-000000000001",
            lock=lock,
        )
