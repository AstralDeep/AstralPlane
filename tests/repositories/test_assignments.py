"""Persistent assignment storage invariants (feature 079)."""

from dataclasses import replace

import pytest

from astralplane.repositories import RepositoryValidationError
from astralplane.repositories.assignments import (
    AssignmentDefinition,
    AssignmentRepository,
    AssignmentResourceAmount,
    canonical,
    plain,
)


def definition(**changes):
    return AssignmentDefinition(
        name="Public changes",
        instructions="Summarize relevant release changes",
        source={"reader": "web-research-1.fetch_page", "url": "https://example.org/releases"},
        allowed_tools=("web-research-1.fetch_page",),
        consented_scopes=("tools:read",),
        offline_grant_id="11111111-1111-4111-8111-111111111111",
        limits={
            "cadence_seconds": 60,
            "max_retries": 3,
            "max_concurrent_tasks": 2,
            "max_depth": 4,
            "max_tasks": 32,
            "model_calls": 100,
            "tool_calls": 1000,
            "tokens": 100000,
            "elapsed_ms": 1000000,
            "daily_model_calls": 100,
            "daily_tool_calls": 1000,
            "daily_tokens": 100000,
            "daily_elapsed_ms": 1000000,
        },
        **changes,
    )


def test_definition_is_deeply_immutable():
    value = definition()
    with pytest.raises(TypeError):
        value.source["url"] = "https://other.invalid"
    AssignmentRepository.validate_definition(value)


@pytest.mark.parametrize("limits", [{}, {"cadence_seconds": 1}, {"tokens": True}])
def test_missing_or_invalid_hard_limits_are_refused(limits):
    with pytest.raises(RepositoryValidationError):
        AssignmentRepository.validate_definition(replace(definition(), limits=limits))


def test_usage_only_has_unknown_money_not_zero():
    amount = AssignmentResourceAmount(model_calls=1, tokens=200, elapsed_ms=1000)
    assert amount.spend_micro_units is None
    assert amount.currency is None


def test_currency_cap_without_trusted_quote_coverage_is_refused():
    limits = dict(
        definition().limits, currency="USD", spend_micro_units=1000, daily_spend_micro_units=1000
    )
    with pytest.raises(RepositoryValidationError):
        AssignmentRepository.validate_definition(replace(definition(), limits=limits))


@pytest.mark.parametrize(
    "change",
    [
        {"name": ""},
        {"instructions": "x" * 9000},
        {"source": {}},
        {"source": []},
        {"allowed_tools": ()},
        {"allowed_tools": ("a", "a")},
        {"consented_scopes": ("",)},
        {"offline_grant_id": "bad"},
        {"offline_grant_id": "11111111-1111-5111-8111-111111111111"},
        {"limits": dict(definition().limits, currency="USD")},
        {"limits": dict(definition().limits, max_depth=5)},
        {"limits": dict(definition().limits, max_concurrent_tasks=True)},
    ],
)
def test_definition_rejects_unbounded_invalid_authority(change):
    with pytest.raises(RepositoryValidationError):
        AssignmentRepository.validate_definition(replace(definition(), **change))


@pytest.mark.parametrize("value", [float("inf"), object(), {"a": set()}, "x" * 300000])
def test_noncanonical_and_oversized_data_is_refused(value):
    with pytest.raises(RepositoryValidationError):
        canonical(value)


def test_naive_time_is_refused():
    from datetime import datetime

    with pytest.raises(RepositoryValidationError):
        plain(datetime(2026, 1, 1))
