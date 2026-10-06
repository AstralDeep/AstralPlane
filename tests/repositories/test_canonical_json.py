"""Tests canonical JSON key validation and byte stability in repository helpers.
Public repository rejection before SQL is covered in test_remote_proposals.py.
"""

from types import MappingProxyType

import pytest

from astralplane.repositories import RepositoryValidationError, _canonical_json


@pytest.mark.parametrize(
    "value, expected",
    [
        ({}, "{}"),
        ({"b": 2, "a": 1}, '{"a":1,"b":2}'),
        ({"1": "string", "": "empty"}, '{"":"empty","1":"string"}'),
        (
            MappingProxyType({"b": True, "a": (MappingProxyType({"z": None}), "é")}),
            '{"a":[{"z":null},"é"],"b":true}',
        ),
    ],
)
def test_string_keys_preserve_canonical_json_bytes(value: object, expected: str) -> None:
    assert _canonical_json(value, "arguments") == expected


@pytest.mark.parametrize(
    "key", [1, None, True, 1.5, object()], ids=["int", "none", "bool", "float", "custom"]
)
@pytest.mark.parametrize("nested", ["mapping", "list", "tuple"])
def test_non_string_keys_are_rejected_at_every_mapping_depth(key: object, nested: str) -> None:
    mapping = {key: "rejected", str(key): "retained"}
    value = (
        mapping
        if nested == "mapping"
        else {"nested": [mapping] if nested == "list" else (mapping,)}
    )
    with pytest.raises(RepositoryValidationError) as failure:
        _canonical_json(value, "arguments")
    assert failure.value.code == "repository_validation"


def test_deeper_mapping_rejection_preserves_valid_bytes() -> None:
    valid: object = {"1": "retained"}
    invalid: object = {1: "rejected", "1": "retained"}
    for _ in range(32):
        valid = {"nested": (valid,)}
        invalid = {"nested": (invalid,)}
    assert (
        _canonical_json(valid, "arguments") == '{"nested":[' * 32 + '{"1":"retained"}' + "]}" * 32
    )
    with pytest.raises(RepositoryValidationError):
        _canonical_json(invalid, "arguments")
