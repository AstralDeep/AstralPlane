"""Typed, attribution-preserving AstralPlane failures."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class PlaneError(RuntimeError):
    """Base error carrying bounded, non-sensitive diagnostic metadata."""

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
    """A connection was requested after its pool was closed."""

    default_code = "pool_closed"


class PoolInUseError(PlaneError):
    """A pool close was attempted while connections remained borrowed."""

    default_code = "pool_in_use"


class ConnectionResetError(PlaneError):
    """A connection could not be returned to a clean pooled state."""

    default_code = "connection_reset_failed"


class PoolReleaseError(PlaneError):
    """A connection pool rejected a returned connection."""

    default_code = "pool_release_failed"


class TransactionStateError(PlaneError):
    """A transaction operation was attempted in an invalid state."""

    default_code = "transaction_state"


class TransactionCommitError(PlaneError):
    """A caller-owned transaction could not be committed."""

    default_code = "transaction_commit_failed"


class SQLContractError(PlaneError, ValueError):
    """A statement or parameter object violated the native driver contract."""

    default_code = "sql_contract"


class SchemaRevisionError(PlaneError):
    """The observed schema cannot safely reach the requested revision."""

    default_code = "schema_revision_incompatible"


class MigrationDefinitionError(PlaneError, ValueError):
    """A migration registry is ambiguous or internally inconsistent."""

    default_code = "migration_definition"


class InitializationError(PlaneError):
    """The explicit boot initializer did not reach a ready state."""

    default_code = "initialization_failed"


class ReconciliationError(PlaneError):
    """A separately invoked product reconciliation hook failed."""

    default_code = "reconciliation_failed"


class DomainValidationError(PlaneError, ValueError):
    """A neutral persistence value violated a public boundary contract."""

    default_code = "domain_validation"


class RepositoryConflictError(PlaneError):
    """A version, owner, lease, or idempotency fence rejected a mutation."""

    default_code = "repository_conflict"
