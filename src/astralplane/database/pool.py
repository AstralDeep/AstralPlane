"""Explicit, driver-neutral pooled connection scopes: ConnectionPool wraps a DriverPool
(psycopg2 or an injectable test pool) and always rolls back a borrowed connection before
returning it, so implicit reads never leak to the next borrower. Close closes admission
from its first attempt, serializes driver cleanup so concurrent callers neither duplicate
it nor report success before it finishes, and raises the typed PoolShutdownError on driver
failure with the attempt count so callers can bound retries and escalate persistent
failures to the host while the pool stays closed to new checkouts.
"""

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
    PoolShutdownError,
)


class DriverPool(Protocol):
    def getconn(self) -> Any: ...

    def putconn(self, connection: Any, *, close: bool = False) -> None: ...

    def closeall(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PoolSnapshot:
    borrowed: int
    closed: bool
    cleanup_complete: bool = False
    cleanup_failed: bool = False
    cleanup_attempts: int = 0


class ConnectionPool:
    def __init__(self, driver_pool: DriverPool) -> None:
        self._driver_pool = driver_pool
        self._state_lock = threading.Lock()
        self._cleanup_lock = threading.Lock()
        self._borrowed = 0
        self._closed = False
        self._cleanup_complete = False
        self._cleanup_failed = False
        self._cleanup_attempts = 0

    @property
    def snapshot(self) -> PoolSnapshot:
        with self._state_lock:
            return PoolSnapshot(
                borrowed=self._borrowed,
                closed=self._closed,
                cleanup_complete=self._cleanup_complete,
                cleanup_failed=self._cleanup_failed,
                cleanup_attempts=self._cleanup_attempts,
            )

    def _borrow(self) -> Any:
        with self._state_lock:
            if self._closed:
                raise PoolClosedError("connection pool is closed")
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
        except BaseException as exc:
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
        with self._state_lock:
            if self._cleanup_complete:
                return
            if self._borrowed:
                raise PoolInUseError(
                    "cannot close a pool with borrowed connections",
                    metadata={"borrowed": self._borrowed},
                )
            self._closed = True
        with self._cleanup_lock:
            with self._state_lock:
                if self._cleanup_complete:
                    return
                self._cleanup_attempts += 1
            try:
                self._driver_pool.closeall()
            except BaseException as exc:
                with self._state_lock:
                    self._cleanup_failed = True
                    attempts = self._cleanup_attempts
                if not isinstance(exc, Exception):
                    raise
                raise PoolShutdownError(
                    "driver pool cleanup failed; admission stays closed and a later"
                    " close call retries cleanup once per call",
                    metadata={"attempts": attempts},
                ) from exc
            with self._state_lock:
                self._cleanup_complete = True
                self._cleanup_failed = False


__all__ = ("ConnectionPool", "DriverPool", "PoolSnapshot")
