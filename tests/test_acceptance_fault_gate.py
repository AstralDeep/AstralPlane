"""Checks that the bounded acceptance fault gate kills each declared guard defect
and refuses survivors, skipped cases, and collection/import errors. The real
PostgreSQL run emits machine-readable evidence for the CI owner gate.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from scripts import check_acceptance_faults as gate

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_acceptance_faults.py"


def test_fault_cli_missing_database_records_failure(tmp_path):
    report = tmp_path / "faults.json"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--report", str(report)],
        cwd=ROOT,
        env={
            key: value
            for key, value in os.environ.items()
            if key != "ASTRALPLANE_TEST_POSTGRES_DSN"
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert report.is_file()
    assert json.loads(report.read_text())["status"] == "failed"


@pytest.mark.parametrize(
    ("returncode", "body", "case", "marker", "expected"),
    (
        (
            1,
            '<testcase name="target"><failure>assert 2 == 1</failure></testcase>',
            "target",
            "assert 2 == 1",
            "killed",
        ),
        (0, '<testcase name="target"/>', "target", "wrong", "survived"),
        (
            2,
            '<testcase name="target"><error>ImportError</error></testcase>',
            "target",
            "ImportError",
            "invalid",
        ),
        (
            1,
            '<testcase name="target"><failure>ImportError</failure></testcase>',
            "target",
            "assert 2 == 1",
            "invalid",
        ),
        (1, '<testcase name="target"><skipped/></testcase>', "target", "", "invalid"),
        (
            1,
            '<testcase name="other"><failure>assert 2 == 1</failure></testcase>',
            "target",
            "assert 2 == 1",
            "invalid",
        ),
        (
            3,
            '<testcase name="target"><failure>assert 2 == 1</failure></testcase>',
            "target",
            "assert 2 == 1",
            "invalid",
        ),
        (1, "", "target", "", "invalid"),
        (0, '<testcase name="target"/>', None, "", "invalid"),
        (0, '<testcase name="target"/>' * 6, "target", "", "invalid"),
    ),
)
def test_only_intended_assertion_failures_count_as_kills(
    tmp_path,
    returncode,
    body,
    case,
    marker,
    expected,
):
    report = tmp_path / "pytest.xml"
    report.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
    assert gate.classify(returncode, report, case=case, marker=marker) == expected


def test_mutation_refuses_changed_guard_bytes(tmp_path):
    fault = replace(gate.FAULTS[0], before="guard that no longer exists")
    with pytest.raises(ValueError, match="exactly one guard"):
        gate.apply_fault(ROOT, tmp_path, fault)


@pytest.mark.parametrize(
    "error",
    (
        subprocess.TimeoutExpired("pytest", 120),
        FileNotFoundError("missing result"),
        gate.ElementTree.ParseError("malformed result"),
    ),
)
def test_watchdog_or_unreadable_execution_is_invalid(tmp_path, monkeypatch, error):
    def fail(*_, **__):
        raise error

    monkeypatch.setattr(gate.subprocess, "run", fail)
    assert gate.run_case(ROOT, ROOT, tmp_path / "result.xml", case="target") == "invalid"


def test_surviving_fault_fails_gate_and_control_error_is_not_a_kill(tmp_path, monkeypatch):
    monkeypatch.setenv("ASTRALPLANE_TEST_POSTGRES_DSN", "synthetic-unopened-dsn")
    monkeypatch.setattr(gate, "apply_fault", lambda *_: "a" * 64)
    monkeypatch.setattr(
        gate, "run_case", lambda *_, **args: "passed" if args["case"] is None else "survived"
    )
    report = gate.check(root=ROOT)
    assert report["status"] == "failed"
    assert {fault["status"] for fault in report["faults"]} == {"survived"}
    output = tmp_path / "report.json"
    assert gate.main(["--report", str(output)]) == 2
    monkeypatch.setattr(gate, "run_case", lambda *_, **__: "invalid")
    with pytest.raises(ValueError, match="control"):
        gate.check(root=ROOT)
    assert gate.main(["--report", str(output)]) == 2
    assert json.loads(output.read_text())["status"] == "failed"


def test_real_postgresql_control_and_all_declared_faults(postgres_administrator_dsn, tmp_path):
    assert postgres_administrator_dsn
    output = Path(os.environ.get("ASTRALPLANE_FAULT_REPORT", str(tmp_path / "faults.json")))
    assert gate.main(["--report", str(output)]) == 0
    report = json.loads(output.read_text())
    assert report["control"] == "passed"
    assert len(report["faults"]) == 5
    assert {fault["status"] for fault in report["faults"]} == {"killed"}
