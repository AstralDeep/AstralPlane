"""Health check probes."""
def liveness_probe() -> dict[str, str]:
    return {"status": "alive"}
