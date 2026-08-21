"""Caller-owned transactions with detached immutable results."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from astralplane.contracts import IsolationLevel, Parameters, Statement
from astralplane.database.pool import ConnectionPool
from astralplane.database.sql import execute_native
from astralplane.errors import TransactionCommitError, TransactionStateError

_SAVEPOINT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


class DetachedRecord(Mapping[str, Any]):
    """A driver-independent immutable row copied before cursor closure."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        if any(not isinstance(key, str) for key in values):
            raise TransactionStateError("driver record column names must be strings")
        object.__setattr__(
            self,
            "_values",
            MappingProxyType({key: _freeze(value) for key, value in values.items()}),
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("DetachedRecord is immutable")

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"DetachedRecord({dict(self._values)!r})"


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Immutable command metadata with optional detached RETURNING rows."""

    rowcount: int
    status_message: str | None
    returned_records: tuple[DetachedRecord, ...] = ()


def _column_names(description: Any) -> tuple[str, ...]:
    if description is None:
        return ()
    names: list[str] = []
    for column in description:
        name = getattr(column, "name", None)
        if name is None:
            name = column[0]
        names.append(str(name))
    return tuple(names)


def _detach_row(row: Any, description: Any) -> DetachedRecord:
    if isinstance(row, Mapping):
        return DetachedRecord(row)
    names = _column_names(description)
    if not names:
        raise TransactionStateError("driver returned a row without column metadata")
    try:
        values = tuple(row)
    except TypeError as exc:
        raise TransactionStateError("driver returned a non-iterable row") from exc
    if len(values) != len(names):
        raise TransactionStateError("driver row width does not match column metadata")
    return DetachedRecord(dict(zip(names, values, strict=True)))


class Transaction:
    """One explicit transaction over a connection borrowed by ``PlaneDatabase``."""

    def __init__(
        self,
        connection: Any,
        *,
        isolation: IsolationLevel | None = None,
    ) -> None:
        self._connection = connection
        self._isolation = isolation
        self._active = False
        self._failed = False
        self._prior_autocommit: bool | None = None

    @property
    def active(self) -> bool:
        return self._active

    def _begin(self) -> None:
        if self._active:
            raise TransactionStateError("transaction is already active")
        if hasattr(self._connection, "autocommit"):
            self._prior_autocommit = bool(self._connection.autocommit)
            self._connection.autocommit = False
        self._active = True
        if self._isolation is not None:
            try:
                self._control(f"BEGIN ISOLATION LEVEL {self._isolation.value}")
            except BaseException:
                self._active = False
                self._restore_autocommit()
                raise

    def _restore_autocommit(self) -> None:
        if self._prior_autocommit is not None:
            self._connection.autocommit = self._prior_autocommit
            self._prior_autocommit = None

    def _ensure_usable(self) -> None:
        if not self._active:
            raise TransactionStateError("transaction is not active")
        if self._failed:
            raise TransactionStateError("transaction is aborted and must roll back")

    def _cursor(self) -> Any:
        self._ensure_usable()
        try:
            return self._connection.cursor()
        except BaseException:
            self._failed = True
            raise

    def _close_cursor(self, cursor: Any, *, preserve_active_error: bool) -> None:
        try:
            cursor.close()
        except BaseException:
            self._failed = True
            if not preserve_active_error:
                raise

    def _control(self, statement: str) -> None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(statement)
        finally:
            cursor.close()

    def execute(self, statement: Statement, parameters: Parameters = ()) -> CommandResult:
        """Execute a command and detach every result before closing its cursor."""

        cursor = self._cursor()
        operation_failed = False
        try:
            execute_native(cursor, statement, parameters)
            description = getattr(cursor, "description", None)
            returned_records = (
                tuple(_detach_row(row, description) for row in cursor.fetchall())
                if description is not None
                else ()
            )
            return CommandResult(
                rowcount=int(getattr(cursor, "rowcount", -1)),
                status_message=(
                    None
                    if getattr(cursor, "statusmessage", None) is None
                    else str(cursor.statusmessage)
                ),
                returned_records=returned_records,
            )
        except BaseException:
            operation_failed = True
            self._failed = True
            raise
        finally:
            self._close_cursor(cursor, preserve_active_error=operation_failed)

    def fetch_one(self, statement: Statement, parameters: Parameters = ()) -> DetachedRecord | None:
        cursor = self._cursor()
        operation_failed = False
        try:
            execute_native(cursor, statement, parameters)
            row = cursor.fetchone()
            return None if row is None else _detach_row(row, cursor.description)
        except BaseException:
            operation_failed = True
            self._failed = True
            raise
        finally:
            self._close_cursor(cursor, preserve_active_error=operation_failed)

    def fetch_all(
        self, statement: Statement, parameters: Parameters = ()
    ) -> tuple[DetachedRecord, ...]:
        cursor = self._cursor()
        operation_failed = False
        try:
            execute_native(cursor, statement, parameters)
            return tuple(_detach_row(row, cursor.description) for row in cursor.fetchall())
        except BaseException:
            operation_failed = True
            self._failed = True
            raise
        finally:
            self._close_cursor(cursor, preserve_active_error=operation_failed)

    @contextmanager
    def savepoint(self, name: str) -> Iterator[Transaction]:
        """Create a bounded identifier savepoint without taking commit ownership."""

        self._ensure_usable()
        if _SAVEPOINT_NAME.fullmatch(name) is None:
            raise TransactionStateError("savepoint name is not a safe PostgreSQL identifier")
        quoted_name = f'"{name}"'
        try:
            self._control(f"SAVEPOINT {quoted_name}")
        except BaseException:
            self._failed = True
            raise
        failed_before = self._failed
        try:
            yield self
        except BaseException:
            try:
                self._failed = False
                self._control(f"ROLLBACK TO SAVEPOINT {quoted_name}")
                self._control(f"RELEASE SAVEPOINT {quoted_name}")
                self._failed = failed_before
            except BaseException:
                self._failed = True
            raise
        else:
            try:
                self._control(f"RELEASE SAVEPOINT {quoted_name}")
            except BaseException:
                self._failed = True
                raise

    def _finish(self, *, failed: bool) -> None:
        if not self._active:
            raise TransactionStateError("transaction is not active")
        should_rollback = failed or self._failed
        try:
            if should_rollback:
                self._connection.rollback()
            else:
                try:
                    self._connection.commit()
                except BaseException as exc:
                    with suppress(BaseException):
                        self._connection.rollback()
                    raise TransactionCommitError("transaction commit failed") from exc
        finally:
            self._active = False
            self._restore_autocommit()
        if self._failed and not failed:
            raise TransactionStateError("transaction rolled back after a handled command failure")


class PlaneDatabase:
    """Database facade that grants transaction ownership only to context scopes."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    @contextmanager
    def transaction(self, *, isolation: IsolationLevel | None = None) -> Iterator[Transaction]:
        with self._pool.connection() as connection:
            transaction = Transaction(connection, isolation=isolation)
            transaction._begin()
            try:
                yield transaction
            except BaseException:
                with suppress(BaseException):
                    transaction._finish(failed=True)
                # Preserve the operation failure. The enclosing pool scope
                # still resets or discards the connection before return.
                raise
            else:
                transaction._finish(failed=False)


__all__ = ("CommandResult", "DetachedRecord", "PlaneDatabase", "Transaction")
