"""Stable embedded composition facade for AstralPlane consumers.

The public facade owns only local resource composition and lifecycle.  Product
policy, authorization, transport handlers, and reconciliation hook behavior are
supplied by the embedding application.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from types import MappingProxyType

from astralplane.audit_retention import AuditRetentionRepository
from astralplane.compatibility import (
    CONTRACT_VERSION,
    PACKAGE_VERSION,
    CompatibilityReport,
    inspect_compatibility,
)
from astralplane.contracts import (
    IsolationLevel,
    ProductReconciler,
    ReconciliationCoordinator,
    Transaction,
)
from astralplane.database.bootstrap import BootInitializer, BootStatus, InitializationReport
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_REGISTRY,
    MigrationRunner,
)
from astralplane.database.pool import ConnectionPool, DriverPool, PoolSnapshot
from astralplane.database.revision import SCHEMA_REVISION
from astralplane.database.transaction import PlaneDatabase
from astralplane.errors import InitializationError
from astralplane.outbox import PostgresOutboxStore
from astralplane.purge import PostgresPurgeStore
from astralplane.reconciliation import ReconciliationReport, ReconciliationRunner
from astralplane.reconciliation_store import PostgresReconciliationCoordinator
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


def create_history_repository() -> HistoryRepository:
    """Create neutral conversation, message, and session stores."""

    return HistoryRepository()


def create_workspace_repository() -> WorkspaceRepository:
    """Create neutral canvas, layout, snapshot, and publication stores."""

    return WorkspaceRepository()


def create_artifact_repository() -> ArtifactRepository:
    """Create neutral attachment, blob-metadata, and artifact stores."""

    return ArtifactRepository()


def create_preferences_repository() -> PreferencesRepository:
    """Create neutral feedback, onboarding, and personalization stores."""

    return PreferencesRepository()


def create_scheduler_repository() -> SchedulerRepository:
    """Create durable operation, occurrence, and effect stores."""

    return SchedulerRepository()


def create_voice_repository() -> VoiceRepository:
    """Create the voice-session metadata store without a media runtime."""

    return VoiceRepository()


def create_remote_repository() -> RemoteRepository:
    """Create the remote inventory and execution-metadata store."""

    return RemoteRepository()


def create_revocation_repository() -> RevocationQueueRepository:
    """Create the owner-attributed encrypted token-revocation queue."""

    return RevocationQueueRepository()


def create_encrypted_llm_config_repository() -> EncryptedLLMConfigRepository:
    """Create ciphertext-only user and system provider configuration storage."""

    return EncryptedLLMConfigRepository()


def create_audit_repository() -> AuditRepository:
    """Create the append-only audit store."""

    return AuditRepository()


def create_audit_retention_repository() -> AuditRetentionRepository:
    """Create the authenticated audit-retention anchor store."""

    return AuditRetentionRepository()


def create_outbox_store() -> PostgresOutboxStore:
    """Create the durable outbox storage mechanics."""

    return PostgresOutboxStore()


def create_purge_store() -> PostgresPurgeStore:
    """Create the durable purge-tombstone storage mechanics."""

    return PostgresPurgeStore()


@dataclass(frozen=True, slots=True)
class RepositoryCatalog:
    """One discoverable set of stateless repositories for a composition."""

    history: HistoryRepository
    workspaces: WorkspaceRepository
    artifacts: ArtifactRepository
    preferences: PreferencesRepository
    scheduler: SchedulerRepository
    voice: VoiceRepository
    remote: RemoteRepository
    revocations: RevocationQueueRepository
    encrypted_llm_config: EncryptedLLMConfigRepository
    audit: AuditRepository
    audit_retention: AuditRetentionRepository
    outbox: PostgresOutboxStore
    purge: PostgresPurgeStore

    def as_mapping(self) -> Mapping[str, object]:
        """Return an immutable, name-addressable view for dependency wiring."""

        return MappingProxyType(
            {
                "artifacts": self.artifacts,
                "audit": self.audit,
                "audit_retention": self.audit_retention,
                "encrypted_llm_config": self.encrypted_llm_config,
                "history": self.history,
                "outbox": self.outbox,
                "preferences": self.preferences,
                "purge": self.purge,
                "remote": self.remote,
                "revocations": self.revocations,
                "scheduler": self.scheduler,
                "voice": self.voice,
                "workspaces": self.workspaces,
            }
        )


def create_repository_catalog() -> RepositoryCatalog:
    """Create all default repositories without opening a connection."""

    return RepositoryCatalog(
        history=create_history_repository(),
        workspaces=create_workspace_repository(),
        artifacts=create_artifact_repository(),
        preferences=create_preferences_repository(),
        scheduler=create_scheduler_repository(),
        voice=create_voice_repository(),
        remote=create_remote_repository(),
        revocations=create_revocation_repository(),
        encrypted_llm_config=create_encrypted_llm_config_repository(),
        audit=create_audit_repository(),
        audit_retention=create_audit_retention_repository(),
        outbox=create_outbox_store(),
        purge=create_purge_store(),
    )


@dataclass(frozen=True, slots=True)
class PlaneHealth:
    """Non-sensitive local readiness and lifecycle evidence."""

    compatibility: CompatibilityReport
    boot_status: BootStatus
    pool: PoolSnapshot

    @property
    def ready(self) -> bool:
        return (
            self.compatibility.compatible
            and self.boot_status is BootStatus.READY
            and not self.pool.closed
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "boot_status": self.boot_status.value,
            "compatibility": self.compatibility.to_dict(),
            "pool": {"borrowed": self.pool.borrowed, "closed": self.pool.closed},
            "ready": self.ready,
        }


class PlaneRuntime:
    """Embedded, fail-closed lifecycle boundary for one AstralPlane instance."""

    def __init__(
        self,
        *,
        pool: ConnectionPool,
        database: PlaneDatabase,
        initializer: BootInitializer,
        reconciler: ReconciliationRunner,
        repositories: RepositoryCatalog | None = None,
        expected_contract_version: str = CONTRACT_VERSION,
        observed_schema_revision: str = SCHEMA_REVISION,
        consumer_version: str = PACKAGE_VERSION,
    ) -> None:
        self._pool = pool
        self._database = database
        self._initializer = initializer
        self._reconciler = reconciler
        self.repositories = repositories or create_repository_catalog()
        self._expected_contract_version = expected_contract_version
        self._observed_schema_revision = observed_schema_revision
        self._consumer_version = consumer_version

    def inspect_compatibility(self) -> CompatibilityReport:
        """Inspect the immutable producer contract against consumer inputs."""

        return inspect_compatibility(
            expected_contract_version=self._expected_contract_version,
            observed_schema_revision=self._observed_schema_revision,
            consumer_version=self._consumer_version,
        )

    def initialize(self, *, expected_revision: str = SCHEMA_REVISION) -> InitializationReport:
        """Migrate and run the exact required reconciliation plan before readiness."""

        compatibility = self.inspect_compatibility()
        if not compatibility.compatible:
            raise InitializationError(
                "AstralPlane compatibility inspection rejected initialization",
                metadata={"reasons": ",".join(compatibility.reasons)},
            )
        return self._initializer.initialize(expected_revision=expected_revision)

    def reconcile(
        self,
        *,
        context: Mapping[str, object],
        schema_revision: str = SCHEMA_REVISION,
    ) -> ReconciliationReport:
        """Re-run the construction-pinned idempotent reconciliation hook set."""

        self._require_ready()
        return self._reconciler.run(schema_revision=schema_revision, context=context)

    @contextmanager
    def transaction(
        self,
        *,
        isolation: IsolationLevel | None = None,
    ) -> Iterator[Transaction]:
        """Open a caller-owned transaction only after the plane is ready."""

        self._require_ready()
        with self._database.transaction(isolation=isolation) as transaction:
            yield transaction

    def health(self) -> PlaneHealth:
        """Return detached readiness evidence without querying durable data."""

        return PlaneHealth(
            compatibility=self.inspect_compatibility(),
            boot_status=self._initializer.status,
            pool=self._pool.snapshot,
        )

    def close(self) -> None:
        """Close the idle connection pool; repeated close calls are safe."""

        self._pool.close()

    def _require_ready(self) -> None:
        health = self.health()
        if not health.ready:
            raise InitializationError(
                "AstralPlane is not ready for ordinary durable-state work",
                metadata={
                    "boot_status": health.boot_status.value,
                    "compatible": health.compatibility.compatible,
                    "pool_closed": health.pool.closed,
                },
            )


def create_plane_runtime(
    driver_pool: DriverPool,
    *,
    identity: str,
    coordinator: ReconciliationCoordinator | None = None,
    reconcilers: Iterable[ProductReconciler],
    reconciliation_context: Mapping[str, object] | None = None,
    repositories: RepositoryCatalog | None = None,
    expected_contract_version: str = CONTRACT_VERSION,
    observed_schema_revision: str = SCHEMA_REVISION,
    consumer_version: str = PACKAGE_VERSION,
) -> PlaneRuntime:
    """Compose the canonical kernel around caller-supplied driver and hooks."""

    pool = ConnectionPool(driver_pool)
    database = PlaneDatabase(pool)
    migration_runner = MigrationRunner(
        database,
        revision=CURRENT_DATA_PLANE_REVISION,
        registry=MIGRATION_REGISTRY,
    )
    reconciliation_runner = ReconciliationRunner(
        coordinator or PostgresReconciliationCoordinator(pool),
        reconcilers,
    )
    initializer = BootInitializer(
        identity,
        migration_runner,
        reconciliation_runner,
        reconciliation_context=reconciliation_context,
    )
    return PlaneRuntime(
        pool=pool,
        database=database,
        initializer=initializer,
        reconciler=reconciliation_runner,
        repositories=repositories,
        expected_contract_version=expected_contract_version,
        observed_schema_revision=observed_schema_revision,
        consumer_version=consumer_version,
    )


__all__ = (
    "PlaneHealth",
    "PlaneRuntime",
    "RepositoryCatalog",
    "create_artifact_repository",
    "create_audit_repository",
    "create_audit_retention_repository",
    "create_encrypted_llm_config_repository",
    "create_history_repository",
    "create_outbox_store",
    "create_plane_runtime",
    "create_preferences_repository",
    "create_purge_store",
    "create_remote_repository",
    "create_repository_catalog",
    "create_revocation_repository",
    "create_scheduler_repository",
    "create_voice_repository",
    "create_workspace_repository",
)
