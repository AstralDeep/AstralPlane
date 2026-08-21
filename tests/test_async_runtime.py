"""Bounded AsyncPlaneRuntime behavior without a database driver."""

from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from astralplane.async_runtime import (
    AsyncPlaneCapacityError,
    AsyncPlaneClosedError,
    AsyncPlaneRuntime,
)
from astralplane.contracts import IsolationLevel


class RuntimeStub:
    def __init__(self) -> None:
        self.repositories = object()
        self.isolations: list[IsolationLevel | None] = []
        self.threads: list[int] = []

    @contextmanager
    def transaction(self, *, isolation: IsolationLevel | None = None):
        self.isolations.append(isolation)
        self.threads.append(threading.get_ident())
        yield SimpleNamespace(marker="transaction")


def test_transaction_runs_off_loop_with_exact_isolation_and_catalog() -> None:
    runtime = RuntimeStub()
    adapter = AsyncPlaneRuntime(runtime, maximum_concurrency=2)  # type: ignore[arg-type]
    loop_thread = threading.get_ident()

    async def run() -> str:
        return await adapter.run_in_transaction(
            lambda transaction: transaction.marker,
            isolation=IsolationLevel.SERIALIZABLE,
        )

    assert asyncio.run(run()) == "transaction"
    assert runtime.isolations == [IsolationLevel.SERIALIZABLE]
    assert runtime.threads[0] != loop_thread
    assert adapter.repositories is runtime.repositories
    assert adapter.snapshot().active == 0


def test_callback_failure_propagates_and_releases_capacity() -> None:
    runtime = RuntimeStub()
    adapter = AsyncPlaneRuntime(runtime)  # type: ignore[arg-type]

    async def run() -> None:
        with pytest.raises(RuntimeError, match="boom"):
            await adapter.run_in_transaction(
                lambda _transaction: (_ for _ in ()).throw(RuntimeError("boom"))
            )
        assert adapter.snapshot().active == 0

    asyncio.run(run())


def test_admission_timeout_bounds_worker_backlog() -> None:
    runtime = RuntimeStub()
    adapter = AsyncPlaneRuntime(  # type: ignore[arg-type]
        runtime,
        maximum_concurrency=1,
        admission_timeout_seconds=0.02,
    )
    entered = threading.Event()
    release = threading.Event()

    def blocking(_transaction: object) -> str:
        entered.set()
        assert release.wait(timeout=2)
        return "done"

    async def run() -> None:
        first = asyncio.create_task(adapter.run_in_transaction(blocking))
        while not entered.is_set():
            await asyncio.sleep(0)
        with pytest.raises(AsyncPlaneCapacityError) as caught:
            await adapter.run_in_transaction(lambda _transaction: "never")
        assert caught.value.code == "async_plane_capacity_unavailable"
        assert adapter.snapshot().active == 1
        release.set()
        assert await first == "done"

    asyncio.run(run())


def test_cancellation_retains_slot_until_the_thread_finishes() -> None:
    runtime = RuntimeStub()
    adapter = AsyncPlaneRuntime(  # type: ignore[arg-type]
        runtime,
        maximum_concurrency=1,
        admission_timeout_seconds=0.02,
    )
    entered = threading.Event()
    release = threading.Event()

    def blocking(_transaction: object) -> None:
        entered.set()
        release.wait(timeout=2)

    async def run() -> None:
        task = asyncio.create_task(adapter.run_in_transaction(blocking))
        while not entered.is_set():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(AsyncPlaneCapacityError):
            await adapter.run_in_transaction(lambda _transaction: None)
        release.set()
        while adapter.snapshot().active:
            await asyncio.sleep(0.001)
        assert await adapter.run_in_transaction(lambda _transaction: 7) == 7

    asyncio.run(run())


def test_close_rejects_new_work_and_cross_loop_use_fails_closed() -> None:
    runtime = RuntimeStub()
    adapter = AsyncPlaneRuntime(runtime)  # type: ignore[arg-type]
    asyncio.run(adapter.run_in_transaction(lambda _transaction: None))

    with pytest.raises(RuntimeError, match="event loops"):
        asyncio.run(adapter.run_in_transaction(lambda _transaction: None))

    fresh = AsyncPlaneRuntime(runtime)  # type: ignore[arg-type]
    fresh.close()
    assert fresh.snapshot().closed
    with pytest.raises(AsyncPlaneClosedError):
        asyncio.run(fresh.run_in_transaction(lambda _transaction: None))


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"maximum_concurrency": True}, TypeError),
        ({"maximum_concurrency": 0}, ValueError),
        ({"maximum_concurrency": 65}, ValueError),
        ({"admission_timeout_seconds": True}, TypeError),
        ({"admission_timeout_seconds": 0}, ValueError),
        ({"admission_timeout_seconds": 301}, ValueError),
    ],
)
def test_constructor_bounds(kwargs: dict[str, object], error: type[Exception]) -> None:
    with pytest.raises(error):
        AsyncPlaneRuntime(RuntimeStub(), **kwargs)  # type: ignore[arg-type]


def test_non_callable_callback_is_rejected_before_loop_binding() -> None:
    adapter = AsyncPlaneRuntime(RuntimeStub())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="callable"):
        asyncio.run(adapter.run_in_transaction(None))  # type: ignore[arg-type]
