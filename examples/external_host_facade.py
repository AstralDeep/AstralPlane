"""Standalone external-host example demonstrating embedded AstralPlane runtime usage.
Uses only the public AstralPlane facade to execute transactions and queries safely.
"""

from __future__ import annotations

import asyncio
from astralplane import AsyncPlaneRuntime, PlaneRuntime
from astralplane.contracts import Transaction

def sample_task(transaction: Transaction) -> dict[str, str]:
    """Execute a task inside a managed transaction context."""
    return {"status": "executed", "engine": "AstralPlane Public Facade"}

async def main() -> None:
    # Initialize runtime via public facade
    runtime = PlaneRuntime.ephemeral()
    async_runtime = AsyncPlaneRuntime(runtime=runtime, maximum_concurrency=4)

    # Execute concurrent tasks
    result = await async_runtime.run_in_transaction(sample_task)
    print("Execution result:", result)

    # Graceful shutdown drain
    await async_runtime.drain(timeout=5.0)
    runtime.close()
    print("Standalone external-host finished cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
