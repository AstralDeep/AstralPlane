"""Pool lifecycle, transaction ownership, and savepoint tests."""

from __future__ import annotations

import pytest

from astralplane.contracts import IsolationLevel
from astralplane.database.pool import ConnectionPool
from astralplane.database.transaction import PlaneDatabase
from astralplane.errors import (
    ConnectionResetError,
    PoolClosedError,
    PoolInUseError,
    PoolReleaseError,
    TransactionCommitError,
    TransactionStateError,
)


class Cursor:
    description = None
    rowcount = 0
    statusmessage = "OK"

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.closed = False

    def execute(self, statement: str, parameters: object | None = None) -> None:
        self.connection.statements.append((statement, parameters))
        if statement == "BROKEN" or (statement.startswith("BEGIN") and self.connection.fail_begin):
            raise ValueError("broken statement")
        if statement.startswith("RELEASE SAVEPOINT") and self.connection.fail_release_savepoint:
            raise RuntimeError("savepoint release failed")
        if (
            statement.startswith("ROLLBACK TO SAVEPOINT")
            and self.connection.fail_rollback_savepoint
        ):
            raise RuntimeError("savepoint rollback failed")

    def close(self) -> None:
        self.closed = True


class Connection:
    def __init__(self) -> None:
        self.autocommit = True
        self.statements: list[tuple[str, object | None]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.fail_commit = False
        self.fail_rollback = False
        self.fail_begin = False
        self.fail_release_savepoint = False
        self.fail_rollback_savepoint = False

    def cursor(self) -> Cursor:
        return Cursor(self)

    def commit(self) -> None:
        self.commits += 1
        if self.fail_commit:
            raise RuntimeError("commit failed")

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.fail_rollback:
            raise RuntimeError("rollback failed")

    def close(self) -> None:
        self.closed += 1


class DriverPool:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.returned: list[tuple[Connection, bool]] = []
        self.close_calls = 0
        self.fail_release = False
        self.fail_borrow = False

    def getconn(self) -> Connection:
        if self.fail_borrow:
            raise RuntimeError("borrow failed")
        return self.connection

    def putconn(self, connection: Connection, *, close: bool = False) -> None:
        if self.fail_release:
            raise RuntimeError("release failed")
        self.returned.append((connection, close))

    def closeall(self) -> None:
        self.close_calls += 1


def test_connection_scope_rolls_back_implicit_work_before_return() -> None:
    connection = Connection()
    driver = DriverPool(connection)
    pool = ConnectionPool(driver)

    with pool.connection() as borrowed:
        assert borrowed is connection
        assert pool.snapshot.borrowed == 1

    assert connection.rollbacks == 1
    assert driver.returned == [(connection, False)]
    assert pool.snapshot.borrowed == 0


def test_failed_driver_checkout_releases_the_reserved_borrow_slot() -> None:
    connection = Connection()
    driver = DriverPool(connection)
    driver.fail_borrow = True
    pool = ConnectionPool(driver)

    with pytest.raises(RuntimeError, match="borrow failed"), pool.connection():
        pass

    assert pool.snapshot.borrowed == 0


def test_pool_close_is_idempotent_and_refuses_active_borrowers() -> None:
    connection = Connection()
    driver = DriverPool(connection)
    pool = ConnectionPool(driver)

    with pool.connection():
        with pytest.raises(PoolInUseError) as error:
            pool.close()
        assert error.value.metadata == (("borrowed", "1"),)

    pool.close()
    pool.close()
    assert driver.close_calls == 1
    assert pool.snapshot.closed
    with pytest.raises(PoolClosedError), pool.connection():
        pass


def test_reset_failure_discards_connection_and_is_visible() -> None:
    connection = Connection()
    connection.fail_rollback = True
    driver = DriverPool(connection)
    pool = ConnectionPool(driver)

    with pytest.raises(ConnectionResetError), pool.connection():
        pass

    assert driver.returned == [(connection, True)]


def test_pool_release_failure_closes_connection_and_is_visible() -> None:
    connection = Connection()
    driver = DriverPool(connection)
    driver.fail_release = True
    pool = ConnectionPool(driver)

    with pytest.raises(PoolReleaseError), pool.connection():
        pass

    assert connection.closed == 1
    assert pool.snapshot.borrowed == 0


def test_body_failure_is_not_masked_by_reset_or_release_failures() -> None:
    connection = Connection()
    connection.fail_rollback = True
    driver = DriverPool(connection)
    driver.fail_release = True
    pool = ConnectionPool(driver)

    with pytest.raises(LookupError, match="caller failure"), pool.connection():
        raise LookupError("caller failure")

    assert connection.closed == 1


def test_transaction_commits_only_when_outer_caller_scope_succeeds() -> None:
    connection = Connection()
    pool = ConnectionPool(DriverPool(connection))
    database = PlaneDatabase(pool)

    with database.transaction(isolation=IsolationLevel.SERIALIZABLE) as transaction:
        transaction.execute("UPDATE item SET value = %s", (3,))
        assert connection.autocommit is False

    assert connection.statements == [
        ("BEGIN ISOLATION LEVEL SERIALIZABLE", None),
        ("UPDATE item SET value = %s", (3,)),
    ]
    assert connection.commits == 1
    assert connection.autocommit is True


def test_transaction_rolls_back_and_preserves_body_exception() -> None:
    connection = Connection()
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with pytest.raises(KeyError, match="caller"), database.transaction() as transaction:
        transaction.execute("UPDATE")
        raise KeyError("caller")

    assert connection.commits == 0
    assert connection.rollbacks >= 1


def test_handled_driver_failure_cannot_commit_fake_success() -> None:
    connection = Connection()
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with (
        pytest.raises(TransactionStateError, match="rolled back"),
        database.transaction() as transaction,
    ):
        with pytest.raises(ValueError, match="broken"):
            transaction.execute("BROKEN")
        with pytest.raises(TransactionStateError, match="aborted"):
            transaction.execute("UPDATE")

    assert connection.commits == 0


def test_commit_failure_is_typed_and_rolls_back() -> None:
    connection = Connection()
    connection.fail_commit = True
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with pytest.raises(TransactionCommitError), database.transaction() as transaction:
        transaction.execute("UPDATE")

    assert connection.commits == 1
    assert connection.rollbacks >= 1


def test_savepoint_recovers_a_statement_failure_without_nested_commit() -> None:
    connection = Connection()
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with database.transaction() as transaction:
        with pytest.raises(ValueError, match="broken"), transaction.savepoint("bounded_1"):
            transaction.execute("BROKEN")
        transaction.execute("UPDATE")

    statements = [statement for statement, _ in connection.statements]
    assert statements == [
        'SAVEPOINT "bounded_1"',
        "BROKEN",
        'ROLLBACK TO SAVEPOINT "bounded_1"',
        'RELEASE SAVEPOINT "bounded_1"',
        "UPDATE",
    ]
    assert connection.commits == 1


def test_successful_savepoint_releases_without_taking_commit_ownership() -> None:
    connection = Connection()
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with database.transaction() as transaction, transaction.savepoint("safe"):
        transaction.execute("UPDATE")

    assert [statement for statement, _ in connection.statements] == [
        'SAVEPOINT "safe"',
        "UPDATE",
        'RELEASE SAVEPOINT "safe"',
    ]
    assert connection.commits == 1


def test_savepoint_control_failure_aborts_the_outer_transaction() -> None:
    connection = Connection()
    connection.fail_release_savepoint = True
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with (
        pytest.raises(RuntimeError, match="savepoint release"),
        database.transaction() as transaction,
        transaction.savepoint("safe"),
    ):
        transaction.execute("UPDATE")

    assert connection.commits == 0


def test_savepoint_rollback_failure_preserves_original_statement_error() -> None:
    connection = Connection()
    connection.fail_rollback_savepoint = True
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with (
        pytest.raises(ValueError, match="broken"),
        database.transaction() as transaction,
        transaction.savepoint("safe"),
    ):
        transaction.execute("BROKEN")

    assert connection.commits == 0


@pytest.mark.parametrize("name", ["", "has-hyphen", "1starts_wrong", "x" * 64])
def test_savepoint_rejects_unsafe_identifiers(name: str) -> None:
    connection = Connection()
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with (
        database.transaction() as transaction,
        pytest.raises(TransactionStateError),
        transaction.savepoint(name),
    ):
        pass


def test_transaction_methods_fail_outside_their_scope() -> None:
    connection = Connection()
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with database.transaction() as transaction:
        pass

    with pytest.raises(TransactionStateError, match="not active"):
        transaction.execute("UPDATE")


def test_isolation_begin_failure_restores_connection_state() -> None:
    connection = Connection()
    connection.fail_begin = True
    database = PlaneDatabase(ConnectionPool(DriverPool(connection)))

    with (
        pytest.raises(ValueError, match="broken"),
        database.transaction(isolation=IsolationLevel.REPEATABLE_READ),
    ):
        pass

    assert connection.autocommit is True
    assert connection.commits == 0
