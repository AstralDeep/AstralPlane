from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest

from astralplane.api import (
    PlaneHealth,
    PlaneRuntime,
    RepositoryCatalog,
    create_artifact_repository,
    create_audit_repository,
    create_audit_retention_repository,
    create_encrypted_llm_config_repository,
    create_history_repository,
    create_outbox_store,
    create_plane_runtime,
    create_preferences_repository,
    create_purge_store,
    create_remote_repository,
    create_repository_catalog,
    create_revocation_repository,
    create_scheduler_repository,
    create_voice_repository,
    create_workspace_repository,
)
from astralplane.audit_retention import AuditRetentionRepository
from astralplane.compatibility import CONTRACT_VERSION, CompatibilityState
from astralplane.contracts import IsolationLevel, ReconciliationHookIdentity
from astralplane.database.bootstrap import BootStatus
from astralplane.database.pool import PoolSnapshot
from astralplane.database.revision import SCHEMA_REVISION
from astralplane.errors import InitializationError
from astralplane.outbox import PostgresOutboxStore
from astralplane.purge import PostgresPurgeStore
from astralplane.reconciliation import (
    RECONCILIATION_ADVISORY_LOCK,
    ReconciliationHookReport,
    ReconciliationReport,
)
from astralplane.repositories.artifacts import ArtifactRepository
from astralplane.repositories.audit import AuditRepository
from astralplane.repositories.history import HistoryRepository
from astralplane.repositories.preferences import PreferencesRepository
from astralplane.repositories.remote import RemoteRepository
from astralplane.repositories.revocations import RevocationQueueRepository
from astralplane.repositories.scheduler import SchedulerRepository
from astralplane.repositories.secrets import EncryptedLLMConfigRepository
from astralplane.repositories.voice import VoiceRepository
from astralplane.repositories.workspaces import WorkspaceRepository

_DIGEST = "a" * 64


class StubPool:
    def __init__(self) -> None:
        self.closed = False

    @property
    def snapshot(self) -> PoolSnapshot:
        return PoolSnapshot(borrowed=0, closed=self.closed)

    def close(self) -> None:
        self.closed = True


class StubDatabase:
    def __init__(self) -> None:
        self.isolations: list[IsolationLevel | None] = []

    @contextmanager
    def transaction(self, *, isolation: IsolationLevel | None = None) -> Iterator[object]:
        self.isolations.append(isolation)
        yield "transaction"


class StubInitializer:
    def __init__(self, *, status: BootStatus = BootStatus.NEW) -> None:
        self.status = status
        self.expected: list[str] = []

    def initialize(self, *, expected_revision: str) -> object:
        self.expected.append(expected_revision)
        self.status = BootStatus.READY
        return "initialized"


class StubReconciler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, object]]] = []

    def run(self, *, schema_revision: str, context: Mapping[str, object]) -> ReconciliationReport:
        self.calls.append((schema_revision, context))
        return ReconciliationReport(
            schema_revision=schema_revision,
            plan_digest=_DIGEST,
            advisory_lock=RECONCILIATION_ADVISORY_LOCK,
            hooks=(
                ReconciliationHookReport(
                    hook=ReconciliationHookIdentity(name="hook", version="v1"),
                    attempt=1,
                    already_complete=False,
                    result_digest=_DIGEST,
                ),
            ),
            durably_complete=True,
        )


def _runtime(
    *,
    status: BootStatus = BootStatus.NEW,
    expected_contract_version: str = CONTRACT_VERSION,
) -> tuple[PlaneRuntime, StubPool, StubDatabase, StubInitializer, StubReconciler]:
    pool = StubPool()
    database = StubDatabase()
    initializer = StubInitializer(status=status)
    reconciler = StubReconciler()
    runtime = PlaneRuntime(
        pool=pool,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        initializer=initializer,  # type: ignore[arg-type]
        reconciler=reconciler,  # type: ignore[arg-type]
        expected_contract_version=expected_contract_version,
    )
    return runtime, pool, database, initializer, reconciler


def test_explicit_repository_factories_return_the_declared_types() -> None:
    expected = (
        (create_history_repository, HistoryRepository),
        (create_workspace_repository, WorkspaceRepository),
        (create_artifact_repository, ArtifactRepository),
        (create_preferences_repository, PreferencesRepository),
        (create_scheduler_repository, SchedulerRepository),
        (create_voice_repository, VoiceRepository),
        (create_remote_repository, RemoteRepository),
        (create_revocation_repository, RevocationQueueRepository),
        (create_encrypted_llm_config_repository, EncryptedLLMConfigRepository),
        (create_audit_repository, AuditRepository),
        (create_audit_retention_repository, AuditRetentionRepository),
        (create_outbox_store, PostgresOutboxStore),
        (create_purge_store, PostgresPurgeStore),
    )

    for factory, repository_type in expected:
        assert isinstance(factory(), repository_type)


def test_repository_catalog_is_complete_immutable_and_fresh() -> None:
    first = create_repository_catalog()
    second = create_repository_catalog()

    assert isinstance(first, RepositoryCatalog)
    assert tuple(first.as_mapping()) == (
        "artifacts",
        "audit",
        "audit_retention",
        "encrypted_llm_config",
        "history",
        "outbox",
        "preferences",
        "purge",
        "remote",
        "revocations",
        "scheduler",
        "voice",
        "workspaces",
    )
    assert first.history is not second.history
    with pytest.raises(TypeError):
        first.as_mapping()["history"] = second.history  # type: ignore[index]


def test_health_reports_readiness_and_detached_shape() -> None:
    runtime, pool, _, _, _ = _runtime(status=BootStatus.READY)

    health = runtime.health()

    assert isinstance(health, PlaneHealth)
    assert health.ready
    assert health.to_dict()["ready"] is True
    pool.close()
    assert not runtime.health().ready


def test_initialize_rejects_incompatible_composition_before_runner() -> None:
    runtime, _, _, initializer, _ = _runtime(expected_contract_version="other/v1")

    report = runtime.inspect_compatibility()
    assert report.state is CompatibilityState.INCOMPATIBLE
    with pytest.raises(InitializationError) as caught:
        runtime.initialize()

    assert caught.value.metadata == (("reasons", "contract_version_mismatch"),)
    assert not initializer.expected


def test_initialize_delegates_exact_revision_and_unlocks_transactions() -> None:
    runtime, _, database, initializer, _ = _runtime()

    with pytest.raises(InitializationError), runtime.transaction():
        pytest.fail("an uninitialized runtime exposed a transaction")

    assert runtime.initialize(expected_revision="067.001") == "initialized"
    assert initializer.expected == ["067.001"]
    with runtime.transaction(isolation=IsolationLevel.SERIALIZABLE) as transaction:
        assert transaction == "transaction"
    assert database.isolations == [IsolationLevel.SERIALIZABLE]


def test_reconcile_uses_pinned_runner_only_after_ready() -> None:
    runtime, _, _, _, reconciler = _runtime()
    with pytest.raises(InitializationError):
        runtime.reconcile(context={})

    runtime.initialize()
    result = runtime.reconcile(context={"tenant": "opaque"})

    assert result.durably_complete
    assert reconciler.calls == [(SCHEMA_REVISION, {"tenant": "opaque"})]


def test_close_is_idempotent_and_keeps_runtime_closed() -> None:
    runtime, pool, _, _, _ = _runtime(status=BootStatus.READY)

    runtime.close()
    runtime.close()

    assert pool.closed
    with pytest.raises(InitializationError), runtime.transaction():
        pytest.fail("a closed runtime exposed a transaction")


class DriverPoolStub:
    def getconn(self) -> Any:
        raise AssertionError("construction must not borrow a connection")

    def putconn(self, connection: Any, *, close: bool = False) -> None:
        raise AssertionError((connection, close))

    def closeall(self) -> None:
        pass


class CoordinatorStub:
    def coordinate(self, **_: object) -> Any:
        raise AssertionError("construction must not coordinate reconciliation")


@dataclass(frozen=True)
class HookStub:
    name: str = "bootstrap"
    version: str = "v1"

    def reconcile(self, context: Mapping[str, object]) -> Mapping[str, object]:
        return context


def test_create_plane_runtime_is_inert_and_uses_supplied_catalog() -> None:
    catalog = create_repository_catalog()

    runtime = create_plane_runtime(
        DriverPoolStub(),
        identity="plane-api-test-factory",
        coordinator=CoordinatorStub(),  # type: ignore[arg-type]
        reconcilers=(HookStub(),),
        repositories=catalog,
    )

    assert runtime.repositories is catalog
    assert runtime.health().boot_status is BootStatus.NEW
    assert runtime.inspect_compatibility().compatible
    runtime.close()


def test_create_plane_runtime_supplies_the_durable_postgres_coordinator() -> None:
    runtime = create_plane_runtime(
        DriverPoolStub(),
        identity="plane-api-test-default-coordinator",
        reconcilers=(HookStub(),),
    )

    assert runtime.health().boot_status is BootStatus.NEW
    assert runtime._reconciler._coordinator.__class__.__name__ == (
        "PostgresReconciliationCoordinator"
    )
    runtime.close()


def test_package_root_exports_the_stable_composition_facade() -> None:
    import astralplane

    assert astralplane.PlaneRuntime is PlaneRuntime
    assert astralplane.RepositoryCatalog is RepositoryCatalog
    assert astralplane.create_plane_runtime is create_plane_runtime
    assert astralplane.create_repository_catalog is create_repository_catalog
