"""Bounded event-loop adapter for caller-owned AstralPlane transactions.

AstralPlane's PostgreSQL driver and repositories are intentionally synchronous.
This adapter moves one *complete* caller-owned transaction onto a worker thread;
it does not expose an async raw-SQL facade and it never splits a transaction
across threads.  Admission is bounded so event-loop callers cannot create an
unbounded executor backlog.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from astralplane.contracts import IsolationLevel, Transaction
from astralplane.errors import PlaneError

if TYPE_CHECKING:
    from astralplane.api import PlaneRuntime

T = TypeVar("T")


class AsyncPlaneCapacityError(PlaneError):
    """The bounded async transaction lane could not admit work in time."""

    default_code = "async_plane_capacity_unavailable"


class AsyncPlaneClosedError(PlaneError):
    """The async adapter has stopped accepting new work."""

    default_code = "async_plane_closed"


@dataclass(frozen=True, slots=True)
class AsyncPlaneSnapshot:
    """Non-sensitive admission state for observability and shutdown gates."""

    maximum_concurrency: int
    active: int
    closed: bool


class AsyncPlaneRuntime:
    """Run complete synchronous Plane transactions off an event loop.

    Cancellation of an awaiting coroutine cannot cancel a Python thread that
    has already entered PostgreSQL.  The adapter therefore retains the slot
    until that transaction finishes and consumes its exception before admitting
    replacement work.  Callbacks must use repository idempotency/CAS fences for
    any externally retryable operation.
    """

    def __init__(
        self,
        runtime: PlaneRuntime,
        *,
        maximum_concurrency: int = 8,
        admission_timeout_seconds: float = 5.0,
    ) -> None:
        if isinstance(maximum_concurrency, bool) or not isinstance(
            maximum_concurrency, int
        ):
            raise TypeError("maximum_concurrency must be an integer")
        if not 1 <= maximum_concurrency <= 64:
            raise ValueError("maximum_concurrency must be between 1 and 64")
        if isinstance(admission_timeout_seconds, bool) or not isinstance(
            admission_timeout_seconds, (int, float)
        ):
            raise TypeError("admission_timeout_seconds must be numeric")
        timeout = float(admission_timeout_seconds)
        if not 0.01 <= timeout <= 300.0:
            raise ValueError("admission_timeout_seconds must be between 0.01 and 300")
        if runtime is None:
            raise TypeError("runtime is required")
        self._runtime = runtime
        self._maximum_concurrency = maximum_concurrency
        self._admission_timeout_seconds = timeout
        self._semaphore = asyncio.Semaphore(maximum_concurrency)
        self._active = 0
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def repositories(self) -> object:
        """Expose the exact catalog owned by the wrapped synchronous runtime."""

        return self._runtime.repositories

    def snapshot(self) -> AsyncPlaneSnapshot:
        return AsyncPlaneSnapshot(
            maximum_concurrency=self._maximum_concurrency,
            active=self._active,
            closed=self._closed,
        )

    async def run_in_transaction(
        self,
        callback: Callable[[Transaction], T],
        *,
        isolation: IsolationLevel | None = None,
    ) -> T:
        """Run ``callback`` once inside one worker-thread transaction scope."""

        if not callable(callback):
            raise TypeError("callback must be callable")
        loop = asyncio.get_running_loop()
        self._bind_loop(loop)
        if self._closed:
            raise AsyncPlaneClosedError("AstralPlane async adapter is closed")
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self._admission_timeout_seconds,
            )
        except TimeoutError as exc:
            raise AsyncPlaneCapacityError(
                "AstralPlane async transaction capacity is unavailable",
                metadata={"maximum_concurrency": self._maximum_concurrency},
            ) from exc
        if self._closed:
            self._semaphore.release()
            raise AsyncPlaneClosedError("AstralPlane async adapter is closed")

        self._active += 1
        worker = asyncio.create_task(
            asyncio.to_thread(self._run_sync, callback, isolation),
            name="astralplane-transaction",
        )
        released = False

        def release_slot(completed: asyncio.Task[T]) -> None:
            nonlocal released
            if released:
                return
            released = True
            self._active -= 1
            self._semaphore.release()
            if completed.cancelled():
                return
            # Retrieve failures when the awaiting caller was cancelled.
            completed.exception()

        worker.add_done_callback(release_slot)
        return await asyncio.shield(worker)

    def close(self) -> None:
        """Reject new work without closing the composition-owned Plane runtime."""

        self._closed = True

    def _bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("AsyncPlaneRuntime cannot be shared across event loops")

    def _run_sync(
        self,
        callback: Callable[[Transaction], T],
        isolation: IsolationLevel | None,
    ) -> T:
        with self._runtime.transaction(isolation=isolation) as transaction:
            return callback(transaction)


__all__ = (
    "AsyncPlaneCapacityError",
    "AsyncPlaneClosedError",
    "AsyncPlaneRuntime",
    "AsyncPlaneSnapshot",
)
