"""Typed PlaneError hierarchy (pool, transaction, schema, migration, reconciliation,
domain, and repository-conflict errors) carrying only bounded, non-sensitive
diagnostic metadata, raised across astralplane and its many AstralDeep callers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class PlaneError(RuntimeError):
    default_code: Final = "plane_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code or self.default_code
        self.metadata = tuple(
            sorted((str(key), str(value)) for key, value in (metadata or {}).items())
        )


class PoolClosedError(PlaneError):
    default_code = "pool_closed"


class PoolInUseError(PlaneError):
    default_code = "pool_in_use"


class ConnectionResetError(PlaneError):
    default_code = "connection_reset_failed"


class PoolReleaseError(PlaneError):
    default_code = "pool_release_failed"


class TransactionStateError(PlaneError):
    default_code = "transaction_state"


class TransactionCommitError(PlaneError):
    default_code = "transaction_commit_failed"


class SQLContractError(PlaneError, ValueError):
    default_code = "sql_contract"


class SchemaRevisionError(PlaneError):
    default_code = "schema_revision_incompatible"


class MigrationDefinitionError(PlaneError, ValueError):
    default_code = "migration_definition"


class InitializationError(PlaneError):
    default_code = "initialization_failed"


class ReconciliationError(PlaneError):
    default_code = "reconciliation_failed"


class DomainValidationError(PlaneError, ValueError):
    default_code = "domain_validation"


class RepositoryConflictError(PlaneError):
    default_code = "repository_conflict"
