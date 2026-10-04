"""Tests for src/astralplane/compatibility.py: producer-side schema compatibility
metadata and deterministic, stably-ordered mismatch reason codes.
"""

from __future__ import annotations

import pytest

from astralplane.compatibility import (
    BLOB_LAYOUT_VERSION,
    CONTRACT_VERSION,
    MIGRATION_DIGEST,
    PACKAGE_VERSION,
    RECOVERY_CONTRACT_VERSION,
    CompatibilityState,
    inspect_compatibility,
)


@pytest.mark.parametrize(
    ("contract", "schema", "consumer", "reason"),
    [
        ("astralplane.contract/v2", "067.001", "0.1.0", "contract_version_mismatch"),
        (CONTRACT_VERSION, "065.001", "0.1.0", "schema_revision_incompatible"),
        (CONTRACT_VERSION, "bad", "0.1.0", "schema_revision_incompatible"),
        (CONTRACT_VERSION, "067.001", "0.0.9", "consumer_version_too_old"),
        (CONTRACT_VERSION, "067.001", "v0.1.0", "consumer_version_too_old"),
    ],
)
def test_incompatible_compositions_report_stable_reason_codes(
    contract: str,
    schema: str,
    consumer: str,
    reason: str,
) -> None:
    report = inspect_compatibility(
        expected_contract_version=contract,
        observed_schema_revision=schema,
        consumer_version=consumer,
    )
    assert not report.compatible
    assert report.state is CompatibilityState.INCOMPATIBLE
    assert reason in report.reasons


def test_multiple_mismatches_are_reported_in_deterministic_order() -> None:
    report = inspect_compatibility(
        expected_contract_version="wrong",
        observed_schema_revision="000.000",
        consumer_version="bad",
    )
    assert report.reasons == (
        "contract_version_mismatch",
        "schema_revision_incompatible",
        "consumer_version_too_old",
    )

def test_json_ready_rejects_non_string_keys() -> None:
    """Tests that json_ready rejects non-string keys.
    """
    from astralplane.repositories import json_ready
    # Test 1: Flat non-string keys
    with pytest.raises(TypeError):
        json_ready({1: 'a', '1': 'b'})
    # Test 2: Nested non-string keys
    with pytest.raises(TypeError):
        json_ready({'nested': {2: 'value'}})
    # Test 3: Valid string keys
    assert json_ready({'valid': 'string_key'}) == {'valid': 'string_key'}
    # Test 4: Empty dictionary
    assert json_ready({}) == {}
    # Test 5: Dictionary with non-string keys
    with pytest.raises(TypeError):
        json_ready({'non-string-key': 'value'})

    # Raise TypeError when given a dictionary with non-string keys
    def json_ready(input_dict):
        if not all(isinstance(key, str) for key in input_dict.keys()):
            raise TypeError('Input dictionary must have only string keys')
        # Rest of the function remains the same
