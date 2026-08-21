"""Stable embedded composition facade for AstralPlane consumers.

The public facade owns only local resource composition and lifecycle.  Product
policy, authorization, transport handlers, and reconciliation hook behavior are
supplied by the embedding application.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from types import MappingProxyType

from astralplane.async_runtime import AsyncPlaneRuntime
from astralplane.audit_retention import AuditRetentionRepository
from astralplane.authority import (
    AuthorityCompareAndSetConflictError,
    AuthorityIdempotencyConflictError,
    AuthorityRepository,
    ReceiptClaimConflictError,
    ReceiptWatermarkConflictError,
    create_authority_repository,
)
from astralplane.blob_store import ExplicitRootStreamingBlobStore, StreamingBlobStore
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
from astralplane.contracts import (
    PlaneDatabase as PlaneDatabaseContract,
)
from astralplane.database.baseline import (
    BaselineCompatibilityReport,
    BaselineCompatibilityState,
    BaselineInitializationReport,
    BaselineMigrationRunner,
    initialize_empty_database,
    inspect_baseline_compatibility,
)
from astralplane.database.bootstrap import BootInitializer, BootStatus, InitializationReport
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_REGISTRY,
    MigrationRunner,
)
from astralplane.database.pool import ConnectionPool, DriverPool, PoolSnapshot
from astralplane.database.postgres import create_postgres_driver_pool
from astralplane.database.revision import SCHEMA_REVISION
from astralplane.database.transaction import PlaneDatabase
from astralplane.errors import InitializationError
from astralplane.immutable_bundle_store import (
    ArtifactCollisionError,
    ArtifactIntegrityError,
    ArtifactPublicationError,
    ArtifactPublicationRevokedError,
    ArtifactReconciliationError,
    BundlePublicationKey,
    BundlePublicationPaths,
    BundlePublicationReceipt,
    BundleRecoveryDisposition,
    BundleRecoveryResult,
    FinalizedBundle,
    ImmutableBundleContract,
    ImmutableBundleStore,
    PublishedBundle,
    StagedBundleReceipt,
    canonical_bundle_digest,
    paths_for,
    runtime_metadata_for_manifest,
)
from astralplane.outbox import PostgresOutboxStore
from astralplane.purge import (
    DurablePurgeExecutor,
    PostgresPurgeStore,
)
from astralplane.reconciliation import ReconciliationReport, ReconciliationRunner
from astralplane.reconciliation_store import PostgresReconciliationCoordinator
from astralplane.repositories.agent_management import AgentManagementRepository
from astralplane.repositories.agents import AgentRepository, AgentRevisionRecord
from astralplane.repositories.artifacts import (
    ArtifactRepository,
    AttachmentMaterializationCoordinator,
    MaterializationRepository,
)
from astralplane.repositories.attachment_parsers import AttachmentParserRepository
from astralplane.repositories.audit import AuditRepository
from astralplane.repositories.background_tasks import BackgroundTaskRepository
from astralplane.repositories.chat_steps import ChatStepRepository
from astralplane.repositories.conversation_files import ConversationFileRepository
from astralplane.repositories.credentials import CredentialRepository
from astralplane.repositories.drafts import (
    DraftAgentRecord,
    DraftAgentRepository,
    DraftPublicationRecord,
)
from astralplane.repositories.generated_agent_publications import (
    GENERATED_AGENT_BUNDLE_CONTRACT,
    GENERATED_AGENT_PUBLICATION_IDEMPOTENCY_NAMESPACE,
    GENERATED_AGENT_PUBLICATION_OPERATION_KIND,
    GENERATED_AGENT_PUBLICATION_RECOVERY_IDEMPOTENCY_NAMESPACE,
    GENERATED_AGENT_PUBLICATION_RECOVERY_OPERATION_KIND,
    GeneratedAgentPublicationIntent,
    GeneratedAgentPublicationOperationBinding,
    GeneratedAgentPublicationRepository,
    GeneratedAgentPublicationResultMetadata,
    canonical_generated_agent_manifest_digest,
    generated_agent_publication_operation_binding,
    generated_agent_publication_paths,
    generated_agent_publication_recovery_operation_binding,
)
from astralplane.repositories.harness_cleanup import HarnessCleanupRepository
from astralplane.repositories.history import HistoryRepository
from astralplane.repositories.identity import IdentityRepository
from astralplane.repositories.knowledge import KnowledgeRepository
from astralplane.repositories.maintenance import MaintenanceRepository
from astralplane.repositories.offline_grants import OfflineGrantRepository
from astralplane.repositories.personalization_graph import PersonalizationGraphRepository
from astralplane.repositories.preferences import PreferencesRepository
from astralplane.repositories.quality_audit import QualityAuditRepository
from astralplane.repositories.remote import RemoteRepository
from astralplane.repositories.remote_proposals import RemoteOperationProposalRepository
from astralplane.repositories.revocations import RevocationQueueRepository
from astralplane.repositories.saved_components import SavedComponentRepository
from astralplane.repositories.scheduler import SchedulerRepository
from astralplane.repositories.secrets import EncryptedLLMConfigRepository
from astralplane.repositories.share_grants import ShareGrantRepository
from astralplane.repositories.tool_policy import ToolPolicyStateRepository
from astralplane.repositories.tracked_jobs import TrackedJobRepository
from astralplane.repositories.tutorials import TutorialRepository
from astralplane.repositories.voice import VoiceRepository
from astralplane.repositories.work_admission import (
    AdmissionClass,
    ExecutionFence,
    OperationOwner,
    OperationRecord,
    OperationRequest,
    OperationState,
    OwnerScope,
    WorkAdmissionRepository,
)
from astralplane.repositories.workspaces import WorkspaceRepository


def create_history_repository() -> HistoryRepository:
    """Create neutral conversation, message, and session stores."""

    return HistoryRepository()


def create_identity_repository() -> IdentityRepository:
    """Create neutral external-identity observation storage."""

    return IdentityRepository()


def create_agent_repository() -> AgentRepository:
    """Create agent registry, trust, revision, host, and runtime stores."""

    return AgentRepository()


def create_agent_management_repository() -> AgentManagementRepository:
    """Create bounded cross-domain reads for agent-management surfaces."""

    return AgentManagementRepository()


def create_draft_agent_repository() -> DraftAgentRepository:
    """Create owner-isolated draft authoring and publication storage."""

    return DraftAgentRepository()


def create_generated_agent_publication_repository(
    *,
    agents: AgentRepository | None = None,
    drafts: DraftAgentRepository | None = None,
    work_admission: WorkAdmissionRepository | None = None,
) -> GeneratedAgentPublicationRepository:
    """Create the durable generated-agent publication journal coordinator."""

    return GeneratedAgentPublicationRepository(
        agents=agents,
        drafts=drafts,
        work_admission=work_admission,
    )


def create_tool_policy_state_repository() -> ToolPolicyStateRepository:
    """Create neutral durable state consumed by product-owned tool policy."""

    return ToolPolicyStateRepository()


def create_credential_repository() -> CredentialRepository:
    """Create ciphertext-only user and remote-machine credential storage."""

    return CredentialRepository()


def create_offline_grant_repository() -> OfflineGrantRepository:
    """Create owner-isolated encrypted offline-grant storage."""

    return OfflineGrantRepository()


def create_share_grant_repository() -> ShareGrantRepository:
    """Create immutable snapshot share-grant storage."""

    return ShareGrantRepository()


def create_chat_step_repository() -> ChatStepRepository:
    """Create owner-isolated persistent conversation-step storage."""

    return ChatStepRepository()


def create_conversation_file_repository() -> ConversationFileRepository:
    """Create owner-isolated conversation file-link metadata storage."""

    return ConversationFileRepository()


def create_saved_component_repository() -> SavedComponentRepository:
    """Create publication-aware saved-component storage."""

    return SavedComponentRepository()


def create_workspace_repository() -> WorkspaceRepository:
    """Create neutral canvas, layout, snapshot, and publication stores."""

    return WorkspaceRepository()


def create_artifact_repository() -> ArtifactRepository:
    """Create neutral attachment, blob-metadata, and artifact stores."""

    return ArtifactRepository()


def create_attachment_materialization_coordinator(
    *,
    database: PlaneDatabaseContract,
    materializations: MaterializationRepository,
    blobs: StreamingBlobStore,
) -> AttachmentMaterializationCoordinator:
    """Bind one transaction authority, materialization store, and configured blob root."""

    return AttachmentMaterializationCoordinator(
        database=database,
        repository=materializations,
        blobs=blobs,
    )


def create_attachment_parser_repository() -> AttachmentParserRepository:
    """Create global parser coverage with owner-isolated claim provenance."""

    return AttachmentParserRepository()


def create_preferences_repository() -> PreferencesRepository:
    """Create neutral feedback, onboarding, and personalization stores."""

    return PreferencesRepository()


def create_knowledge_repository() -> KnowledgeRepository:
    """Create interaction, quality, quarantine, and proposal state stores."""

    return KnowledgeRepository()


def create_personalization_graph_repository() -> PersonalizationGraphRepository:
    """Create owner-isolated memory-link and consolidation state storage."""

    return PersonalizationGraphRepository()


def create_scheduler_repository() -> SchedulerRepository:
    """Create durable operation, occurrence, and effect stores."""

    return SchedulerRepository()


def create_background_task_repository() -> BackgroundTaskRepository:
    """Create owner-isolated background-task compatibility storage."""

    return BackgroundTaskRepository()


def create_work_admission_repository() -> WorkAdmissionRepository:
    """Create durable operation admission, lifecycle, and fence storage."""

    return WorkAdmissionRepository()


def create_maintenance_repository() -> MaintenanceRepository:
    """Create maintenance-unit membership and lease-fencing storage."""

    return MaintenanceRepository()


def create_tracked_job_repository() -> TrackedJobRepository:
    """Create owner-isolated external tracked-job storage."""

    return TrackedJobRepository()


def create_quality_audit_repository() -> QualityAuditRepository:
    """Create owner-scoped qualification-run, evidence, and review storage."""

    return QualityAuditRepository()


def create_harness_cleanup_repository() -> HarnessCleanupRepository:
    """Create fixed-manifest synthetic verification cleanup storage."""

    return HarnessCleanupRepository()


def create_tutorial_repository() -> TutorialRepository:
    """Create revisioned global tutorial-content storage."""

    return TutorialRepository()


def create_voice_repository() -> VoiceRepository:
    """Create the voice-session metadata store without a media runtime."""

    return VoiceRepository()


def create_remote_repository() -> RemoteRepository:
    """Create the remote inventory and execution-metadata store."""

    return RemoteRepository()


def create_remote_operation_proposal_repository() -> RemoteOperationProposalRepository:
    """Create single-use remote-operation confirmation proposal storage."""

    return RemoteOperationProposalRepository()


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


def create_durable_purge_executor(
    *,
    database: PlaneDatabaseContract,
    purge_store: PostgresPurgeStore,
    blobs: StreamingBlobStore,
) -> DurablePurgeExecutor:
    """Compose purge from the app runtime/transaction source and its one blob store."""

    return DurablePurgeExecutor(
        database=database,
        store=purge_store,
        blobs=blobs,
    )


def create_streaming_blob_store(
    *,
    root: str | os.PathLike[str],
    io_chunk_bytes: int = 1024 * 1024,
    create_root: bool = True,
) -> StreamingBlobStore:
    """Bind bounded streams, provisioning a safe missing suffix below a real ancestor."""

    return ExplicitRootStreamingBlobStore(
        root,
        io_chunk_bytes=io_chunk_bytes,
        create_root=create_root,
    )


@dataclass(frozen=True, slots=True)
class RepositoryCatalog:
    """One discoverable set of stateless repositories for a composition."""

    identity: IdentityRepository
    agents: AgentRepository
    agent_management: AgentManagementRepository
    draft_agents: DraftAgentRepository
    generated_agent_publications: GeneratedAgentPublicationRepository
    tool_policy_state: ToolPolicyStateRepository
    credentials: CredentialRepository
    offline_grants: OfflineGrantRepository
    share_grants: ShareGrantRepository
    chat_steps: ChatStepRepository
    conversation_files: ConversationFileRepository
    saved_components: SavedComponentRepository
    history: HistoryRepository
    workspaces: WorkspaceRepository
    artifacts: ArtifactRepository
    attachment_parsers: AttachmentParserRepository
    preferences: PreferencesRepository
    knowledge: KnowledgeRepository
    personalization_graph: PersonalizationGraphRepository
    scheduler: SchedulerRepository
    background_tasks: BackgroundTaskRepository
    work_admission: WorkAdmissionRepository
    maintenance: MaintenanceRepository
    tracked_jobs: TrackedJobRepository
    quality_audit: QualityAuditRepository
    harness_cleanup: HarnessCleanupRepository
    tutorials: TutorialRepository
    voice: VoiceRepository
    remote: RemoteRepository
    remote_operation_proposals: RemoteOperationProposalRepository
    revocations: RevocationQueueRepository
    encrypted_llm_config: EncryptedLLMConfigRepository
    audit: AuditRepository
    audit_retention: AuditRetentionRepository
    authority: AuthorityRepository
    outbox: PostgresOutboxStore
    purge: PostgresPurgeStore

    def as_mapping(self) -> Mapping[str, object]:
        """Return an immutable, name-addressable view for dependency wiring."""

        return MappingProxyType(
            {
                "agent_management": self.agent_management,
                "agents": self.agents,
                "artifacts": self.artifacts,
                "attachment_parsers": self.attachment_parsers,
                "audit": self.audit,
                "audit_retention": self.audit_retention,
                "authority": self.authority,
                "background_tasks": self.background_tasks,
                "chat_steps": self.chat_steps,
                "conversation_files": self.conversation_files,
                "credentials": self.credentials,
                "draft_agents": self.draft_agents,
                "generated_agent_publications": self.generated_agent_publications,
                "encrypted_llm_config": self.encrypted_llm_config,
                "history": self.history,
                "harness_cleanup": self.harness_cleanup,
                "identity": self.identity,
                "knowledge": self.knowledge,
                "maintenance": self.maintenance,
                "offline_grants": self.offline_grants,
                "outbox": self.outbox,
                "preferences": self.preferences,
                "personalization_graph": self.personalization_graph,
                "purge": self.purge,
                "quality_audit": self.quality_audit,
                "remote": self.remote,
                "remote_operation_proposals": self.remote_operation_proposals,
                "revocations": self.revocations,
                "saved_components": self.saved_components,
                "scheduler": self.scheduler,
                "share_grants": self.share_grants,
                "tool_policy_state": self.tool_policy_state,
                "tracked_jobs": self.tracked_jobs,
                "tutorials": self.tutorials,
                "voice": self.voice,
                "work_admission": self.work_admission,
                "workspaces": self.workspaces,
            }
        )


def create_repository_catalog() -> RepositoryCatalog:
    """Create all default repositories without opening a connection."""

    agents = create_agent_repository()
    drafts = create_draft_agent_repository()
    work_admission = create_work_admission_repository()
    return RepositoryCatalog(
        identity=create_identity_repository(),
        agents=agents,
        agent_management=create_agent_management_repository(),
        draft_agents=drafts,
        generated_agent_publications=create_generated_agent_publication_repository(
            agents=agents,
            drafts=drafts,
            work_admission=work_admission,
        ),
        tool_policy_state=create_tool_policy_state_repository(),
        credentials=create_credential_repository(),
        offline_grants=create_offline_grant_repository(),
        share_grants=create_share_grant_repository(),
        chat_steps=create_chat_step_repository(),
        conversation_files=create_conversation_file_repository(),
        saved_components=create_saved_component_repository(),
        history=create_history_repository(),
        workspaces=create_workspace_repository(),
        artifacts=create_artifact_repository(),
        attachment_parsers=create_attachment_parser_repository(),
        preferences=create_preferences_repository(),
        knowledge=create_knowledge_repository(),
        personalization_graph=create_personalization_graph_repository(),
        scheduler=create_scheduler_repository(),
        background_tasks=create_background_task_repository(),
        work_admission=work_admission,
        maintenance=create_maintenance_repository(),
        tracked_jobs=create_tracked_job_repository(),
        quality_audit=create_quality_audit_repository(),
        harness_cleanup=create_harness_cleanup_repository(),
        tutorials=create_tutorial_repository(),
        voice=create_voice_repository(),
        remote=create_remote_repository(),
        remote_operation_proposals=create_remote_operation_proposal_repository(),
        revocations=create_revocation_repository(),
        encrypted_llm_config=create_encrypted_llm_config_repository(),
        audit=create_audit_repository(),
        audit_retention=create_audit_retention_repository(),
        authority=create_authority_repository(),
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

    def inspect_baseline_compatibility(self) -> BaselineCompatibilityReport:
        """Inspect fresh/existing PostgreSQL structure without changing it."""

        return inspect_baseline_compatibility(self._database)

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
        BaselineMigrationRunner(database, migration_runner),
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


def create_postgres_runtime(
    database_url: str,
    *,
    identity: str,
    reconcilers: Iterable[ProductReconciler],
    coordinator: ReconciliationCoordinator | None = None,
    reconciliation_context: Mapping[str, object] | None = None,
    repositories: RepositoryCatalog | None = None,
    expected_contract_version: str = CONTRACT_VERSION,
    observed_schema_revision: str = SCHEMA_REVISION,
    consumer_version: str = PACKAGE_VERSION,
    minimum_connections: int = 2,
    maximum_connections: int = 10,
    acquire_timeout_seconds: float = 30.0,
    connect_timeout_seconds: int = 10,
    application_name: str | None = None,
) -> PlaneRuntime:
    """Create the canonical runtime while keeping psycopg construction inside Plane."""

    driver_pool = create_postgres_driver_pool(
        database_url,
        minimum_connections=minimum_connections,
        maximum_connections=maximum_connections,
        acquire_timeout_seconds=acquire_timeout_seconds,
        connect_timeout_seconds=connect_timeout_seconds,
        application_name=(
            f"astralplane:{identity}" if application_name is None else application_name
        ),
    )
    try:
        return create_plane_runtime(
            driver_pool,
            identity=identity,
            coordinator=coordinator,
            reconcilers=reconcilers,
            reconciliation_context=reconciliation_context,
            repositories=repositories,
            expected_contract_version=expected_contract_version,
            observed_schema_revision=observed_schema_revision,
            consumer_version=consumer_version,
        )
    except BaseException:
        with suppress(BaseException):
            driver_pool.closeall()
        raise


__all__ = (
    "GENERATED_AGENT_BUNDLE_CONTRACT",
    "GENERATED_AGENT_PUBLICATION_IDEMPOTENCY_NAMESPACE",
    "GENERATED_AGENT_PUBLICATION_OPERATION_KIND",
    "GENERATED_AGENT_PUBLICATION_RECOVERY_IDEMPOTENCY_NAMESPACE",
    "GENERATED_AGENT_PUBLICATION_RECOVERY_OPERATION_KIND",
    "AdmissionClass",
    "AgentRevisionRecord",
    "ArtifactCollisionError",
    "ArtifactIntegrityError",
    "ArtifactPublicationError",
    "ArtifactPublicationRevokedError",
    "ArtifactReconciliationError",
    "AsyncPlaneRuntime",
    "AttachmentMaterializationCoordinator",
    "AuthorityCompareAndSetConflictError",
    "AuthorityIdempotencyConflictError",
    "AuthorityRepository",
    "BaselineCompatibilityReport",
    "BaselineCompatibilityState",
    "BaselineInitializationReport",
    "BundlePublicationKey",
    "BundlePublicationPaths",
    "BundlePublicationReceipt",
    "BundleRecoveryDisposition",
    "BundleRecoveryResult",
    "DraftAgentRecord",
    "DraftPublicationRecord",
    "ExecutionFence",
    "FinalizedBundle",
    "GeneratedAgentPublicationIntent",
    "GeneratedAgentPublicationOperationBinding",
    "GeneratedAgentPublicationRepository",
    "GeneratedAgentPublicationResultMetadata",
    "ImmutableBundleContract",
    "ImmutableBundleStore",
    "OperationOwner",
    "OperationRecord",
    "OperationRequest",
    "OperationState",
    "OwnerScope",
    "PlaneHealth",
    "PlaneRuntime",
    "PublishedBundle",
    "ReceiptClaimConflictError",
    "ReceiptWatermarkConflictError",
    "RepositoryCatalog",
    "StagedBundleReceipt",
    "canonical_bundle_digest",
    "canonical_generated_agent_manifest_digest",
    "create_agent_management_repository",
    "create_agent_repository",
    "create_artifact_repository",
    "create_attachment_materialization_coordinator",
    "create_attachment_parser_repository",
    "create_audit_repository",
    "create_audit_retention_repository",
    "create_authority_repository",
    "create_background_task_repository",
    "create_chat_step_repository",
    "create_conversation_file_repository",
    "create_credential_repository",
    "create_draft_agent_repository",
    "create_durable_purge_executor",
    "create_encrypted_llm_config_repository",
    "create_generated_agent_publication_repository",
    "create_harness_cleanup_repository",
    "create_history_repository",
    "create_identity_repository",
    "create_knowledge_repository",
    "create_maintenance_repository",
    "create_offline_grant_repository",
    "create_outbox_store",
    "create_personalization_graph_repository",
    "create_plane_runtime",
    "create_postgres_runtime",
    "create_preferences_repository",
    "create_purge_store",
    "create_quality_audit_repository",
    "create_remote_operation_proposal_repository",
    "create_remote_repository",
    "create_repository_catalog",
    "create_revocation_repository",
    "create_saved_component_repository",
    "create_scheduler_repository",
    "create_share_grant_repository",
    "create_streaming_blob_store",
    "create_tool_policy_state_repository",
    "create_tracked_job_repository",
    "create_tutorial_repository",
    "create_voice_repository",
    "create_work_admission_repository",
    "create_workspace_repository",
    "generated_agent_publication_operation_binding",
    "generated_agent_publication_paths",
    "generated_agent_publication_recovery_operation_binding",
    "initialize_empty_database",
    "inspect_baseline_compatibility",
    "paths_for",
    "runtime_metadata_for_manifest",
)
