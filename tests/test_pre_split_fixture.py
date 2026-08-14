"""Synthetic pre-split fixture integrity and privacy tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "pre_split"


def test_fixture_is_digest_bound_and_covers_every_durable_cluster() -> None:
    database = json.loads((FIXTURE_ROOT / "database.json").read_text(encoding="utf-8"))
    expected = json.loads((FIXTURE_ROOT / "expected.json").read_text(encoding="utf-8"))
    blob_path = FIXTURE_ROOT / expected["blobRelativePath"]
    blob = blob_path.read_bytes()

    assert database["schemaRevision"] == "066.001"
    assert expected["schemaRevisionAfterUpgrade"] == "067.001"
    assert len(database["owners"]) == expected["ownerCount"] == 2
    assert sorted(database["records"]) == sorted(expected["recordDomains"])
    assert len(blob) == expected["blobBytes"]
    assert hashlib.sha256(blob).hexdigest() == expected["blobSha256"]
    assert blob_path.resolve().is_relative_to(FIXTURE_ROOT.resolve())


def test_fixture_contains_only_explicit_synthetic_non_sensitive_values() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(FIXTURE_ROOT.rglob("*"))
        if path.is_file() and path.name != "README.md"
    ).lower()
    denied = (
        "@uky.edu",
        "authorization: bearer",
        "api_key",
        "apikey",
        "password",
        "patient",
        "private key",
        "secret",
        "token",
    )
    assert all(marker not in text for marker in denied)
    assert "synthetic" in text
