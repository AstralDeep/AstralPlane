"""Explicit, driver-neutral pooled connection scopes."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import Any, Protocol

from astralplane.errors import (
    ConnectionResetError,
    PoolClosedError,
    PoolInUseError,
    PoolReleaseError,
)


class DriverPool(Protocol):
    """Subset shared by psycopg2 pools and injectable test pools."""

    def getconn(self) -> Any: ...

    def putconn(self, connection: Any, *, close: bool = False) -> None: ...

    def closeall(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PoolSnapshot:
    """Non-sensitive local pool lifecycle metadata."""

    borrowed: int
    closed: bool


class ConnectionPool:
    """Own one driver pool and return only scoped connections.

    A borrowed connection is rolled back before return even after a successful
    caller scope. That clears implicit read transactions and protects the next
    borrower without ever silently committing caller work.
    """

    def __init__(self, driver_pool: DriverPool) -> None:
        self._driver_pool = driver_pool
        self._state_lock = threading.Lock()
        self._borrowed = 0
        self._closed = False

    @property
    def snapshot(self) -> PoolSnapshot:
        with self._state_lock:
            return PoolSnapshot(borrowed=self._borrowed, closed=self._closed)

    def _borrow(self) -> Any:
        with self._state_lock:
            if self._closed:
                raise PoolClosedError("connection pool is closed")
            # Reserve the scope while holding the lifecycle lock, then let the
            # driver perform a potentially blocking checkout without
            # serializing unrelated borrowers behind this wrapper's lock.
            self._borrowed += 1
        try:
            connection = self._driver_pool.getconn()
            return connection
        except BaseException:
            with self._state_lock:
                self._borrowed -= 1
            raise

    def _release(self, connection: Any, *, discard: bool) -> None:
        release_error: BaseException | None = None
        try:
            self._driver_pool.putconn(connection, close=discard)
        except BaseException as exc:  # preserve KeyboardInterrupt/SystemExit too
            release_error = exc
            with suppress(BaseException):
                connection.close()
        finally:
            with self._state_lock:
                self._borrowed -= 1
        if release_error is not None:
            raise PoolReleaseError("driver pool rejected a returned connection") from release_error

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """Borrow one connection and guarantee a clean or discarded return."""

        connection = self._borrow()
        body_error: BaseException | None = None
        reset_error: BaseException | None = None
        try:
            yield connection
        except BaseException as exc:
            body_error = exc
            raise
        finally:
            try:
                connection.rollback()
            except BaseException as exc:
                reset_error = exc
            try:
                self._release(connection, discard=reset_error is not None)
            except PoolReleaseError:
                if body_error is None and reset_error is None:
                    raise
            if reset_error is not None and body_error is None:
                raise ConnectionResetError(
                    "connection rollback failed before pool return"
                ) from reset_error

    def close(self) -> None:
        """Close an idle pool; active scopes must finish first."""

        with self._state_lock:
            if self._closed:
                return
            if self._borrowed:
                raise PoolInUseError(
                    "cannot close a pool with borrowed connections",
                    metadata={"borrowed": self._borrowed},
                )
            self._closed = True
        self._driver_pool.closeall()


__all__ = ("ConnectionPool", "DriverPool", "PoolSnapshot")
