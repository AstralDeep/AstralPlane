"""Fixed-manifest synthetic harness cleanup contract tests."""

from __future__ import annotations

import pytest

from astralplane.repositories import RepositoryValidationError
from astralplane.repositories.harness_cleanup import (
    HarnessCleanupProfile,
    HarnessCleanupRepository,
)
from tests.repositories._support import Result, ScriptedTransaction


def test_verification_cleanup_escapes_exact_boundary_and_never_deletes_audit() -> None:
    transaction = ScriptedTransaction(execute=[Result(rowcount=index) for index in range(9)])
    report = HarnessCleanupRepository().purge_run(
        transaction,
        profile=HarnessCleanupProfile.VERIFICATION,
        run_id="run_1",
    )

    assert report.namespace_prefix == "__verif__run_1_"
    assert report.total_deleted == sum(range(9))
    assert tuple(item.table for item in report.tables) == (
        "message_attachment",
        "saved_components",
        "workspace_layout",
        "workspace_snapshot",
        "messages",
        "chats",
        "user_attachments",
        "draft_agents",
        "user_llm_config",
    )
    assert all("audit_events" not in statement for _, statement, _ in transaction.calls)
    assert transaction.calls[0][2] == (r"\_\_verif\_\_run\_1\_%",)
    assert all("ESCAPE '\\'" in statement for _, statement, _ in transaction.calls)


def test_security_cleanup_accepts_prefixed_id_and_uses_fixed_extended_manifest() -> None:
    transaction = ScriptedTransaction(execute=[Result(rowcount=1) for _ in range(10)])
    report = HarnessCleanupRepository().purge_run(
        transaction,
        profile=HarnessCleanupProfile.SECURITY_BENCHMARK,
        run_id="__bench__case-1",
    )

    assert report.namespace_prefix == "__bench__case-1__"
    assert report.total_deleted == 10
    assert tuple(item.table for item in report.tables[-2:]) == (
        "memory_item",
        "short_term_signal",
    )
    assert transaction.calls[0][2] == (r"\_\_bench\_\_case-1\_\_%",)


@pytest.mark.parametrize(
    "run_id",
    ("", "_leading", "contains space", "a/b", "__verif____verif__nested", "%"),
)
def test_cleanup_rejects_noncanonical_or_ambiguous_run_ids(run_id: str) -> None:
    transaction = ScriptedTransaction()
    with pytest.raises(RepositoryValidationError, match="run_id"):
        HarnessCleanupRepository().purge_run(
            transaction,
            profile=HarnessCleanupProfile.VERIFICATION,
            run_id=run_id,
        )
    assert transaction.calls == []


def test_cleanup_rejects_arbitrary_profile_and_invalid_command_metadata() -> None:
    with pytest.raises(RepositoryValidationError, match="profile"):
        HarnessCleanupRepository().purge_run(
            ScriptedTransaction(),
            profile="verification",  # type: ignore[arg-type]
            run_id="run-1",
        )

    invalid = ScriptedTransaction(execute=[Result(rowcount=-1)])
    with pytest.raises(RepositoryValidationError, match="command result"):
        HarnessCleanupRepository().purge_run(
            invalid,
            profile=HarnessCleanupProfile.VERIFICATION,
            run_id="run-1",
        )
