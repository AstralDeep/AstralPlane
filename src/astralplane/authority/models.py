"""Host-neutral durable authority-binding values.

AstralPlane persists these fences but does not decide which agents are governed
or which lifecycle transition is permitted. Those decisions remain with the
composition host.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from astralplane.domain import require_identifier, require_utc
from astralplane.errors import DomainValidationError

_LETS_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class AuthorityPopulation(StrEnum):
    """Governed populations in the v1 Astral/LETS contract."""

    SERVER_DYNAMIC = "server_dynamic"
    BYO_USER = "byo_user"


class AuthorityBindingState(StrEnum):
    """Durable lifecycle states; terminal states never reopen in place."""

    PROVISIONING = "provisioning"
    ACTIVE = "active"
    QUIESCENT = "quiescent"
    CLOSING = "closing"
    CLOSED = "closed"
    REVOKING = "revoking"
    REVOKED = "revoked"
    RECONCILING = "reconciling"
    EXPIRED = "expired"

    @property
    def terminal(self) -> bool:
        return self in {
            AuthorityBindingState.CLOSED,
            AuthorityBindingState.REVOKED,
            AuthorityBindingState.EXPIRED,
        }


def _positive_integer(value: object, *, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise DomainValidationError(f"{field} must be a positive integer")
    return value


def _non_negative_integer(value: object, *, field: str) -> int:
    if type(value) is not int or value < 0:
        raise DomainValidationError(f"{field} must be a non-negative integer")
    return value


def _lets_digest(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _LETS_DIGEST.fullmatch(value) is None:
        raise DomainValidationError(f"{field} must be a canonical LETS SHA-256 digest")
    return value


def _canonical_capabilities(value: object) -> tuple[str, ...]:
    if not isinstance(value, tuple) or not value:
        raise DomainValidationError("capabilities must be a nonempty canonical tuple")
    if any(not isinstance(item, str) for item in value):
        raise DomainValidationError("capabilities must contain canonical identifiers")
    for capability in value:
        require_identifier(capability, field="capability")
    if tuple(sorted(value)) != value or len(value) != len(set(value)):
        raise DomainValidationError("capabilities must be sorted and unique")
    return value


@dataclass(frozen=True, slots=True)
class AgentAuthorityBinding:
    """Owner- and runtime-generation-fenced external authority binding."""

    binding_id: str
    owner_id: str
    agent_id: str
    runtime_id: str
    runtime_generation: int
    population: AuthorityPopulation
    tenant_id: str
    envelope_id: str
    warden_id: str
    lease_id: str
    lineage_id: str
    subject_id: str
    policy_digest: str
    machine_digest: str
    config_epoch: int
    capabilities: tuple[str, ...]
    lease_sequence: int
    lease_expires_at_ns: int
    state: AuthorityBindingState
    created_at: datetime
    updated_at: datetime
    version: int

    def __post_init__(self) -> None:
        for field, value in (
            ("binding id", self.binding_id),
            ("owner id", self.owner_id),
            ("agent id", self.agent_id),
            ("runtime id", self.runtime_id),
            ("tenant id", self.tenant_id),
            ("envelope id", self.envelope_id),
            ("warden id", self.warden_id),
            ("lease id", self.lease_id),
            ("lineage id", self.lineage_id),
            ("subject id", self.subject_id),
        ):
            require_identifier(value, field=field)
        if not isinstance(self.population, AuthorityPopulation):
            raise DomainValidationError("population must be an approved authority population")
        if not isinstance(self.state, AuthorityBindingState):
            raise DomainValidationError("state must be an authority binding state")

        _positive_integer(self.runtime_generation, field="runtime generation")
        _lets_digest(self.policy_digest, field="policy digest")
        _lets_digest(self.machine_digest, field="machine digest")
        _positive_integer(self.config_epoch, field="config epoch")
        object.__setattr__(
            self,
            "capabilities",
            _canonical_capabilities(self.capabilities),
        )
        _non_negative_integer(self.lease_sequence, field="lease sequence")
        _positive_integer(self.lease_expires_at_ns, field="lease expiry")
        _non_negative_integer(self.version, field="version")

        created_at = require_utc(self.created_at, field="created at")
        updated_at = require_utc(self.updated_at, field="updated at")
        if updated_at < created_at:
            raise DomainValidationError("updated at cannot precede created at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)

    @property
    def owner_agent_key(self) -> tuple[str, str, AuthorityPopulation]:
        """Logical uniqueness key for one governed rollout population."""

        return (self.owner_id, self.agent_id, self.population)

    @property
    def runtime_generation_key(self) -> tuple[str, str, str, int]:
        """Fence one concrete runtime generation within its owner."""

        return (
            self.owner_id,
            self.agent_id,
            self.runtime_id,
            self.runtime_generation,
        )


__all__ = (
    "AgentAuthorityBinding",
    "AuthorityBindingState",
    "AuthorityPopulation",
)
