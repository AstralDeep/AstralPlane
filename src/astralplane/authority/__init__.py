"""Neutral durable-authority records owned by AstralPlane."""

from astralplane.authority.claims import (
    EXECUTOR_ANCHOR_FORMAT,
    ExternalAuthorityAnchorMetadata,
    ReceiptClaim,
    ReceiptSequenceWatermark,
)
from astralplane.authority.effects import (
    AstralToolScope,
    ProtectedEffectOperation,
    ProtectedEffectStatus,
)
from astralplane.authority.lifecycle import (
    AuthorityLifecycleKind,
    AuthorityLifecycleOperation,
    AuthorityLifecycleStatus,
)
from astralplane.authority.models import (
    AgentAuthorityBinding,
    AuthorityBindingState,
    AuthorityPopulation,
    PendingAuthorityField,
    pending_authority_identity,
)
from astralplane.authority.repository import (
    AuthorityCompareAndSetConflictError,
    AuthorityIdempotencyConflictError,
    AuthorityRepository,
    ReceiptClaimConflictError,
    ReceiptWatermarkConflictError,
)


def create_authority_repository() -> AuthorityRepository:
    """Create the stateless neutral authority persistence boundary."""

    return AuthorityRepository()


__all__ = (
    "EXECUTOR_ANCHOR_FORMAT",
    "AgentAuthorityBinding",
    "AstralToolScope",
    "AuthorityBindingState",
    "AuthorityCompareAndSetConflictError",
    "AuthorityIdempotencyConflictError",
    "AuthorityLifecycleKind",
    "AuthorityLifecycleOperation",
    "AuthorityLifecycleStatus",
    "AuthorityPopulation",
    "AuthorityRepository",
    "ExternalAuthorityAnchorMetadata",
    "PendingAuthorityField",
    "ProtectedEffectOperation",
    "ProtectedEffectStatus",
    "ReceiptClaim",
    "ReceiptClaimConflictError",
    "ReceiptSequenceWatermark",
    "ReceiptWatermarkConflictError",
    "create_authority_repository",
    "pending_authority_identity",
)
