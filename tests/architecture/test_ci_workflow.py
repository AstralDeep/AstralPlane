"""Structural contract for AstralPlane's repository-owned CI workflow."""

from __future__ import annotations

import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT_PATH = REPOSITORY_ROOT / "pyproject.toml"
BUILD_REQUIREMENTS_PATH = (
    REPOSITORY_ROOT / "tooling" / "python-ci" / "build-requirements.lock.txt"
)

CHECKOUT_ACTION = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
SETUP_UV_ACTION = "astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9"
POSTGRES_IMAGE = (
    "postgres:17-alpine@"
    "sha256:dc17045ccfd343b49600570ea734b9c4991cf1c3f3302e67df51e3b402dd55c4"
)
DEEP_COMMIT = "fc113c4f99121b2053bb71523835c5c4743f1f56"
OWNER_JOBS = ("quality", "postgresql", "package-compatibility")
SETUPTOOLS_REQUIREMENT = "setuptools==80.10.2"
SETUPTOOLS_HASH = "sha256:95b30ddfb717250edb492926c92b5221f7ef3fbcc2b07579bcd4a27da21d0173"


def _workflow_text() -> str:
    assert WORKFLOW_PATH.is_file(), f"active owner workflow is missing: {WORKFLOW_PATH}"
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def _mapping_block(text: str, key: str, *, indent: int = 0) -> str:
    lines = text.splitlines()
    marker = f"{' ' * indent}{key}:"
    try:
        start = lines.index(marker)
    except ValueError:
        raise AssertionError(f"missing {key!r} mapping at indentation {indent}") from None
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            end = index
            break
    return "\n".join(lines[start + 1 : end]).rstrip()


def _job_blocks(text: str) -> dict[str, str]:
    jobs = _mapping_block(text, "jobs")
    lines = jobs.splitlines()
    starts = [
        (index, match.group(1))
        for index, line in enumerate(lines)
        if (match := re.fullmatch(r"  ([a-z][a-z0-9-]*):", line))
    ]
    blocks: dict[str, str] = {}
    for position, (start, job_id) in enumerate(starts):
        end = starts[position + 1][0] if position + 1 < len(starts) else len(lines)
        blocks[job_id] = "\n".join(lines[start + 1 : end])
    return blocks


def _run_commands(job: str) -> tuple[str, ...]:
    commands: list[str] = []
    lines = job.splitlines()
    for index, line in enumerate(lines):
        match = re.match(r"^\s+(?:-\s+)?run:\s*(.*)$", line)
        if match is None:
            continue
        inline = match.group(1).strip()
        if inline and inline not in {"|", ">"}:
            commands.append(inline)
            continue
        indentation = len(line) - len(line.lstrip())
        body: list[str] = []
        for following in lines[index + 1 :]:
            if following.strip() and len(following) - len(following.lstrip()) <= indentation:
                break
            body.append(following.strip())
        commands.append("\n".join(body).strip())
    return tuple(commands)


def test_owner_workflow_is_active_pinned_and_read_only() -> None:
    text = _workflow_text()
    jobs = _job_blocks(text)

    assert tuple(jobs) == (*OWNER_JOBS, "gates")
    assert _mapping_block(text, "permissions") == "  contents: read"
    assert all("runs-on: ubuntu-24.04" in job for job in jobs.values())
    assert not re.search(r"^\s*if:\s*.*\bfalse\b", text, flags=re.MULTILINE)
    assert "continue-on-error:" not in text
    assert "components/AstralPlane" not in text

    uses = re.findall(r"^\s*- uses:\s*([^\s#]+)", text, flags=re.MULTILINE)
    assert uses
    assert all(re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", action) for action in uses)
    assert CHECKOUT_ACTION in uses
    assert SETUP_UV_ACTION in uses


def test_quality_job_runs_locked_source_and_architecture_gates() -> None:
    job = _job_blocks(_workflow_text())["quality"]
    commands = _run_commands(job)

    assert "fetch-depth: 0" in job
    assert "uv lock --check" in commands
    assert "uv sync --frozen --group ci" in commands
    assert "uv run --frozen --group ci ruff check ." in commands
    assert (
        "uv run --frozen --group ci python tests/architecture/test_dependency_direction.py"
        in commands
    )


def test_postgresql_job_runs_the_complete_suite_with_real_database_and_coverage() -> None:
    job = _job_blocks(_workflow_text())["postgresql"]
    commands = _run_commands(job)

    assert f"image: {POSTGRES_IMAGE}" in job
    assert re.search(
        r"ASTRALPLANE_TEST_POSTGRES_DSN:\s*"
        r"postgresql://astralplane:astralplane_ci@127\.0\.0\.1:5432/astralplane",
        job,
    )
    assert "repository: AstralDeep/AstralDeep" in job
    assert f"ref: {DEEP_COMMIT}" in job
    assert "path: source-deep" in job
    assert "ASTRALDEEP_SOURCE_REPO: ${{ github.workspace }}/source-deep" in job

    pytest_commands = [command for command in commands if re.search(r"\bpytest\b", command)]
    assert len(pytest_commands) == 1
    pytest_command = pytest_commands[0]
    assert "pytest -q -p no:cacheprovider" in pytest_command
    assert "--cov=astralplane" in pytest_command
    assert "--cov-branch" in pytest_command
    assert "--cov-report=xml" in pytest_command
    assert "--cov-fail-under=88.75" in pytest_command
    assert "tests/" not in pytest_command
    assert not re.search(r"--ignore(?:=|\s)", pytest_command)
    assert (
        "uv run --frozen --group ci diff-cover coverage.xml "
        "--compare-branch origin/main --fail-under=90"
        in commands
    )


def test_package_compatibility_builds_and_smokes_a_clean_wheel() -> None:
    job = _job_blocks(_workflow_text())["package-compatibility"]
    commands = _run_commands(job)

    assert re.search(r"python-version:\s*\[\"3\.11\", \"3\.14\"\]", job)
    assert "uv lock --check" in commands
    assert (
        "uv build --build-constraints tooling/python-ci/build-requirements.lock.txt "
        "--require-hashes"
        in commands
    )
    assert not any(command.startswith("uv build --frozen") for command in commands)
    assert any("uv venv" in command and "--seed" in command for command in commands)
    assert any("pip install --no-deps dist/*.whl" in command for command in commands)
    assert any(
        "import astralplane; assert astralplane.CONTRACT_VERSION == "
        "'astralplane.contract/v1'" in command
        for command in commands
    )
    pyproject = PYPROJECT_PATH.read_text(encoding="utf-8")
    assert f'requires = ["{SETUPTOOLS_REQUIREMENT}"]' in pyproject

    assert BUILD_REQUIREMENTS_PATH.is_file(), (
        f"hash-locked build constraint is missing: {BUILD_REQUIREMENTS_PATH}"
    )
    assert BUILD_REQUIREMENTS_PATH.read_text(encoding="utf-8").splitlines() == [
        f"{SETUPTOOLS_REQUIREMENT} --hash={SETUPTOOLS_HASH}"
    ]


def test_aggregate_fails_closed_over_every_owner_job() -> None:
    job = _job_blocks(_workflow_text())["gates"]

    assert "if: always()" in job
    assert "needs: [quality, postgresql, package-compatibility]" in job
    for owner_job in OWNER_JOBS:
        result_expression = (
            f"needs.{owner_job}.result"
            if "-" not in owner_job
            else f"needs['{owner_job}'].result"
        )
        assert result_expression in job
        assert re.search(
            rf"{re.escape(result_expression)}[^\n]+== ['\"]success['\"]",
            job,
        )
