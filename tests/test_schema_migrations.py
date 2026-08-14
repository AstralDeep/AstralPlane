"""Repeatability, concurrency, compatibility, and rollback migration tests."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from astralplane.database.migrations import (
    Migration,
    MigrationRegistry,
    MigrationRunner,
)
from astralplane.database.revision import DataPlaneRevision
from astralplane.errors import MigrationDefinitionError, SchemaRevisionError


def _checksum(name: str) -> str:
    return hashlib.sha256(name.encode("ascii")).hexdigest()


class FakeTransaction:
    def __init__(
        self,
        metadata: dict[str, str],
        objects: set[str],
        executions: list[str],
    ) -> None:
        self.metadata = metadata
        self.objects = objects
        self.executions = executions

    def fetch_one(self, statement: str, parameters: object = ()) -> dict[str, object]:
        assert statement == "SELECT pg_advisory_xact_lock(%s, %s)"
        assert parameters == (1095980114, 60001)
        self.executions.append("advisory-lock")
        return {"pg_advisory_xact_lock": None}

    def fetch_all(self, statement: str, parameters: object = ()) -> tuple[dict[str, str], ...]:
        assert "FROM schema_meta" in statement
        assert parameters == ("revision", "astralplane_migration_digest")
        self.executions.append("read-metadata")
        return tuple(
            {"key": key, "value": value}
            for key, value in sorted(self.metadata.items())
            if key in parameters
        )

    def execute(self, statement: str, parameters: object = ()) -> SimpleNamespace:
        if statement.startswith("CREATE TABLE IF NOT EXISTS schema_meta"):
            self.executions.append("ensure-metadata")
        elif statement.startswith("INSERT INTO schema_meta"):
            key, value = parameters  # type: ignore[misc]
            self.metadata[str(key)] = str(value)
            self.executions.append(f"write:{key}={value}")
        elif statement == "TEST ADD OBJECT":
            (name,) = parameters  # type: ignore[misc]
            self.objects.add(str(name))
            self.executions.append(f"add:{name}")
        else:
            raise AssertionError(f"unexpected SQL: {statement}")
        return SimpleNamespace(rowcount=1, status_message="OK", returned_records=())


class FakeDatabase:
    """Transactional in-memory fixture with one cross-runner lock."""

    def __init__(
        self,
        *,
        revision: str | None = None,
        digest: str | None = None,
        objects: set[str] | None = None,
    ) -> None:
        self.metadata: dict[str, str] = {}
        if revision is not None:
            self.metadata["revision"] = revision
        if digest is not None:
            self.metadata["astralplane_migration_digest"] = digest
        self.objects = set(objects or ())
        self.executions: list[str] = []
        self.transactions = 0
        self.rollbacks = 0
        self._lock = threading.Lock()
        self.duplicate_metadata = False

    @contextmanager
    def transaction(self, **_: object) -> Iterator[FakeTransaction]:
        with self._lock:
            self.transactions += 1
            working_metadata = dict(self.metadata)
            working_objects = set(self.objects)
            working_executions: list[str] = []
            transaction = FakeTransaction(
                working_metadata,
                working_objects,
                working_executions,
            )
            if self.duplicate_metadata:
                original_fetch_all = transaction.fetch_all

                def duplicate_fetch_all(
                    statement: str, parameters: object = ()
                ) -> tuple[dict[str, str], ...]:
                    rows = original_fetch_all(statement, parameters)
                    return (*rows, {"key": "revision", "value": "065.001"})

                transaction.fetch_all = duplicate_fetch_all  # type: ignore[method-assign]
            try:
                yield transaction
            except BaseException:
                self.rollbacks += 1
                raise
            else:
                self.metadata = working_metadata
                self.objects = working_objects
                self.executions.extend(working_executions)


def _add_object(name: str):
    def operation(transaction: Any) -> None:
        transaction.execute("TEST ADD OBJECT", (name,))

    return operation


def _registry() -> MigrationRegistry:
    return MigrationRegistry(
        (
            Migration(
                name="empty-to-065",
                source_revisions=(None,),
                target_revision="065.001",
                checksum=_checksum("empty-to-065"),
                operation=_add_object("base-schema"),
            ),
            Migration(
                name="065-to-066",
                source_revisions=("065.001",),
                target_revision="066.001",
                checksum=_checksum("065-to-066"),
                operation=_add_object("messages-owner-index"),
            ),
        )
    )


def _runner(database: FakeDatabase, registry: MigrationRegistry | None = None) -> MigrationRunner:
    selected_registry = registry or _registry()
    revision = DataPlaneRevision(
        schema_revision="066.001",
        read_compatible_from=("065.001",),
        migration_digest=selected_registry.digest,
    )
    return MigrationRunner(database, revision=revision, registry=selected_registry)  # type: ignore[arg-type]


def test_empty_database_runs_the_complete_declared_path() -> None:
    database = FakeDatabase()

    report = _runner(database).run(expected_revision="066.001")

    assert report.source_revision is None
    assert report.applied_steps == ("empty-to-065", "065-to-066")
    assert report.target_revision == "066.001"
    assert not report.already_current
    assert database.metadata["revision"] == "066.001"
    assert database.metadata["astralplane_migration_digest"] == report.migration_digest
    assert database.objects == {"base-schema", "messages-owner-index"}
    assert database.executions[0] == "advisory-lock"


def test_representative_legacy_state_is_preserved_during_upgrade() -> None:
    database = FakeDatabase(
        revision="065.001",
        objects={"base-schema", "legacy-owner-record", "legacy-voice-session"},
    )

    report = _runner(database).run(expected_revision="066.001")

    assert report.applied_steps == ("065-to-066",)
    assert database.objects == {
        "base-schema",
        "legacy-owner-record",
        "legacy-voice-session",
        "messages-owner-index",
    }


def test_repeated_run_is_already_current_and_does_not_reapply_steps() -> None:
    database = FakeDatabase()
    runner = _runner(database)
    first = runner.run(expected_revision="066.001")
    add_count = sum(item.startswith("add:") for item in database.executions)

    second = runner.run(expected_revision="066.001")

    assert first.applied_steps
    assert second.already_current
    assert second.applied_steps == ()
    assert sum(item.startswith("add:") for item in database.executions) == add_count


def test_current_marker_without_digest_is_rejected_without_relabeling() -> None:
    database = FakeDatabase(revision="066.001", objects={"base-schema"})

    with pytest.raises(SchemaRevisionError, match="missing migration-set evidence"):
        _runner(database).run(expected_revision="066.001")

    assert database.metadata == {"revision": "066.001"}


def test_predecessor_with_unknown_digest_is_rejected_without_upgrade() -> None:
    database = FakeDatabase(revision="065.001", digest="f" * 64, objects={"legacy"})

    with pytest.raises(SchemaRevisionError, match="unrecognized migration-set digest"):
        _runner(database).run(expected_revision="066.001")

    assert database.metadata == {
        "revision": "065.001",
        "astralplane_migration_digest": "f" * 64,
    }
    assert database.objects == {"legacy"}


def test_concurrent_runners_apply_each_step_exactly_once() -> None:
    database = FakeDatabase()
    runner = _runner(database)

    with ThreadPoolExecutor(max_workers=10) as executor:
        reports = list(
            executor.map(
                lambda _: runner.run(expected_revision="066.001"),
                range(10),
            )
        )

    assert sum(not report.already_current for report in reports) == 1
    assert sum(item == "add:base-schema" for item in database.executions) == 1
    assert sum(item == "add:messages-owner-index" for item in database.executions) == 1
    assert database.transactions == 10


def test_partially_applied_repeat_safe_ddl_recovers_from_the_old_marker() -> None:
    database = FakeDatabase(
        revision="065.001",
        objects={"base-schema", "messages-owner-index"},
    )

    report = _runner(database).run(expected_revision="066.001")

    assert report.applied_steps == ("065-to-066",)
    assert database.objects == {"base-schema", "messages-owner-index"}
    assert database.metadata["revision"] == "066.001"


def test_incompatible_revision_is_rejected_without_mutation() -> None:
    database = FakeDatabase(revision="064.001", objects={"legacy"})
    before = (dict(database.metadata), set(database.objects))

    with pytest.raises(SchemaRevisionError, match="no declared migration path"):
        _runner(database).run(expected_revision="066.001")

    assert (database.metadata, database.objects) == before
    assert database.rollbacks == 1


def test_wrong_current_digest_is_rejected_without_relabeling_schema() -> None:
    database = FakeDatabase(revision="066.001", digest="f" * 64)

    with pytest.raises(SchemaRevisionError, match="different migration-set digest"):
        _runner(database).run(expected_revision="066.001")

    assert database.metadata["astralplane_migration_digest"] == "f" * 64


def test_migration_failure_rolls_back_schema_and_revision_together() -> None:
    def fail_after_write(transaction: Any) -> None:
        transaction.execute("TEST ADD OBJECT", ("must-roll-back",))
        raise RuntimeError("interrupted migration")

    registry = MigrationRegistry(
        (
            Migration(
                name="failing",
                source_revisions=("065.001",),
                target_revision="066.001",
                checksum=_checksum("failing"),
                operation=fail_after_write,
            ),
        )
    )
    database = FakeDatabase(revision="065.001", objects={"legacy"})

    with pytest.raises(RuntimeError, match="interrupted migration"):
        _runner(database, registry).run(expected_revision="066.001")

    assert database.metadata == {"revision": "065.001"}
    assert database.objects == {"legacy"}
    assert database.rollbacks == 1


def test_expected_revision_mismatch_fails_before_opening_transaction() -> None:
    database = FakeDatabase(revision="065.001")

    with pytest.raises(SchemaRevisionError, match="composition expected"):
        _runner(database).run(expected_revision="067.001")

    assert database.transactions == 0


def test_duplicate_schema_metadata_is_rejected_and_rolled_back() -> None:
    database = FakeDatabase(revision="065.001")
    database.duplicate_metadata = True

    with pytest.raises(SchemaRevisionError, match="duplicate keys"):
        _runner(database).run(expected_revision="066.001")

    assert database.rollbacks == 1


def test_registry_exposes_its_immutable_declared_steps() -> None:
    registry = _registry()
    assert tuple(migration.name for migration in registry.migrations) == (
        "empty-to-065",
        "065-to-066",
    )


def test_runner_rejects_registry_digest_mismatch_at_construction() -> None:
    database = FakeDatabase()
    registry = _registry()
    wrong_revision = DataPlaneRevision(
        schema_revision="066.001",
        read_compatible_from=("065.001",),
        migration_digest="f" * 64,
    )

    with pytest.raises(MigrationDefinitionError, match="does not match"):
        MigrationRunner(database, revision=wrong_revision, registry=registry)  # type: ignore[arg-type]


def test_registry_cycle_is_detected_and_rolled_back() -> None:
    registry = MigrationRegistry(
        (
            Migration(
                name="forward",
                source_revisions=("065.001",),
                target_revision="066.001",
                checksum=_checksum("forward"),
                operation=_add_object("forward"),
            ),
            Migration(
                name="backward",
                source_revisions=("066.001",),
                target_revision="065.001",
                checksum=_checksum("backward"),
                operation=_add_object("backward"),
            ),
        )
    )
    revision = DataPlaneRevision(
        schema_revision="067.001",
        read_compatible_from=("065.001",),
        migration_digest=registry.digest,
    )
    database = FakeDatabase(revision="065.001")
    runner = MigrationRunner(database, revision=revision, registry=registry)  # type: ignore[arg-type]

    with pytest.raises(MigrationDefinitionError, match="cycle"):
        runner.run(expected_revision="067.001")

    assert database.metadata == {"revision": "065.001"}


@pytest.mark.parametrize(
    "factory",
    [
        lambda: MigrationRegistry(()),
        lambda: MigrationRegistry([]),
        lambda: MigrationRegistry(
            (
                Migration("same", (None,), "065.001", _checksum("1"), _add_object("1")),
                Migration("same", ("065.001",), "066.001", _checksum("2"), _add_object("2")),
            )
        ),
        lambda: MigrationRegistry(
            (
                Migration("one", (None,), "065.001", _checksum("1"), _add_object("1")),
                Migration("two", (None,), "066.001", _checksum("2"), _add_object("2")),
            )
        ),
    ],
)
def test_ambiguous_registry_definitions_fail_closed(factory: Any) -> None:
    with pytest.raises(MigrationDefinitionError):
        factory()


@pytest.mark.parametrize(
    "migration",
    [
        ("", (None,), "065.001", _checksum("x"), _add_object("x")),
        ("bad-\N{SNOWMAN}", (None,), "065.001", _checksum("x"), _add_object("x")),
        ("none", (), "065.001", _checksum("x"), _add_object("x")),
        ("list", [None], "065.001", _checksum("x"), _add_object("x")),
        ("duplicate", (None, None), "065.001", _checksum("x"), _add_object("x")),
        ("same", ("065.001",), "065.001", _checksum("x"), _add_object("x")),
        ("checksum", (None,), "065.001", "A" * 64, _add_object("x")),
        ("callable", (None,), "065.001", _checksum("x"), None),
    ],
)
def test_invalid_migration_definitions_are_rejected(migration: tuple[object, ...]) -> None:
    with pytest.raises((MigrationDefinitionError, SchemaRevisionError)):
        Migration(*migration)  # type: ignore[arg-type]
