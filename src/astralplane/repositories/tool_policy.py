"""Neutral durable state used by AstralDeep's tool policy.

This module deliberately does not decide whether a scope or tool is allowed.
It persists explicit user decisions, including legacy rows, so the composing
orchestrator can apply its policy and authorization ordering.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from astralplane.contracts import Transaction
from astralplane.repositories import (
    RepositoryDataError,
    RepositoryValidationError,
    _bounded_limit,
    _bounded_text,
    _non_negative_int,
    _required_id,
)


@dataclass(frozen=True, slots=True)
class ScopeState:
    owner_id: str
    agent_id: str
    scope: str
    enabled: bool
    updated_at: int | None


@dataclass(frozen=True, slots=True)
class ToolOverrideState:
    owner_id: str
    agent_id: str
    tool_name: str
    permission_kind: str | None
    enabled: bool
    updated_at: int | None


@dataclass(frozen=True, slots=True)
class LegacyToolPermission:
    owner_id: str
    agent_id: str
    tool_name: str
    allowed: bool
    updated_at: int | None


@dataclass(frozen=True, slots=True)
class ScopedAgentOwnerRecord:
    owner_id: str
    agent_id: str


class ToolPolicyStateRepository:
    """Store explicit grants, overrides, selections, and agent opt-outs."""

    def list_scopes(
        self, transaction: Transaction, *, owner_id: str, agent_id: str
    ) -> tuple[ScopeState, ...]:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        rows = transaction.fetch_all(
            """
            SELECT user_id, agent_id, scope, enabled, updated_at
            FROM agent_scopes
            WHERE user_id = %s AND agent_id = %s
            ORDER BY scope
            """,
            (owner_id, agent_id),
        )
        return tuple(_scope(row) for row in rows)

    def list_all_scopes(self, transaction: Transaction, *, owner_id: str) -> tuple[ScopeState, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        rows = transaction.fetch_all(
            """
            SELECT user_id, agent_id, scope, enabled, updated_at
            FROM agent_scopes WHERE user_id = %s ORDER BY agent_id, scope
            """,
            (owner_id,),
        )
        return tuple(_scope(row) for row in rows)

    def list_scoped_agent_owners_for_administration(
        self,
        transaction: Transaction,
        *,
        agent_id_suffix: str = "-1",
        limit: int = 5000,
    ) -> tuple[ScopedAgentOwnerRecord, ...]:
        """Inventory scoped runtime identities for authorized orphan cleanup."""

        suffix = _bounded_text(agent_id_suffix, "agent_id_suffix", maximum=128)
        maximum = _bounded_limit(limit, maximum=5000)
        rows = transaction.fetch_all(
            """
            SELECT DISTINCT user_id, agent_id
            FROM agent_scopes
            WHERE RIGHT(agent_id, length(%s)) = %s
            ORDER BY user_id, agent_id
            LIMIT %s
            """,
            (suffix, suffix, maximum),
        )
        return tuple(
            ScopedAgentOwnerRecord(
                owner_id=str(row["user_id"]),
                agent_id=str(row["agent_id"]),
            )
            for row in rows
        )

    def has_any_enabled_scope(self, transaction: Transaction, *, owner_id: str) -> bool:
        owner_id = _required_id(owner_id, "owner_id")
        return (
            transaction.fetch_one(
                "SELECT 1 AS present FROM agent_scopes WHERE user_id = %s AND enabled LIMIT 1",
                (owner_id,),
            )
            is not None
        )

    def set_scopes(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        scopes: Mapping[str, bool],
        updated_at: int,
    ) -> tuple[ScopeState, ...]:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        updated_at = _non_negative_int(updated_at, "updated_at")
        if not isinstance(scopes, Mapping):
            raise RepositoryValidationError("scopes must be a mapping")
        result: list[ScopeState] = []
        for name in sorted(scopes):
            scope_name = _bounded_text(name, "scope", maximum=256)
            enabled = scopes[name]
            if not isinstance(enabled, bool):
                raise RepositoryValidationError("scope state must be boolean")
            row = transaction.fetch_one(
                """
                INSERT INTO agent_scopes (user_id, agent_id, scope, enabled, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, agent_id, scope) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                RETURNING user_id, agent_id, scope, enabled, updated_at
                """,
                (owner_id, agent_id, scope_name, enabled, updated_at),
            )
            if row is None:  # pragma: no cover - PostgreSQL RETURNING invariant
                raise RepositoryDataError("scope upsert returned no row")
            result.append(_scope(row))
        return tuple(result)

    def list_overrides(
        self, transaction: Transaction, *, owner_id: str, agent_id: str
    ) -> tuple[ToolOverrideState, ...]:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        rows = transaction.fetch_all(
            """
            SELECT user_id, agent_id, tool_name, permission_kind, enabled, updated_at
            FROM tool_overrides
            WHERE user_id = %s AND agent_id = %s
            ORDER BY tool_name, permission_kind NULLS FIRST
            """,
            (owner_id, agent_id),
        )
        return tuple(_override(row) for row in rows)

    def set_tool_override(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        tool_name: str,
        permission_kind: str | None,
        enabled: bool,
        updated_at: int,
    ) -> ToolOverrideState:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        tool_name = _bounded_text(tool_name, "tool_name", maximum=512)
        if permission_kind is not None:
            permission_kind = _bounded_text(permission_kind, "permission_kind", maximum=256)
        if not isinstance(enabled, bool):
            raise RepositoryValidationError("enabled must be boolean")
        updated_at = _non_negative_int(updated_at, "updated_at")
        row = transaction.fetch_one(
            """
            INSERT INTO tool_overrides (
                user_id, agent_id, tool_name, permission_kind, enabled, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, agent_id, tool_name, COALESCE(permission_kind, ''))
            DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at
            RETURNING user_id, agent_id, tool_name, permission_kind, enabled, updated_at
            """,
            (owner_id, agent_id, tool_name, permission_kind, enabled, updated_at),
        )
        if row is None:  # pragma: no cover
            raise RepositoryDataError("tool override upsert returned no row")
        return _override(row)

    def create_tool_override_if_absent(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        tool_name: str,
        permission_kind: str | None,
        enabled: bool,
        updated_at: int,
    ) -> bool:
        """Backfill one legacy decision without overwriting a concurrent choice."""

        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        tool_name = _bounded_text(tool_name, "tool_name", maximum=512)
        if permission_kind is not None:
            permission_kind = _bounded_text(permission_kind, "permission_kind", maximum=256)
        if not isinstance(enabled, bool):
            raise RepositoryValidationError("enabled must be boolean")
        updated_at = _non_negative_int(updated_at, "updated_at")
        result = transaction.execute(
            """
            INSERT INTO tool_overrides (
                user_id, agent_id, tool_name, permission_kind, enabled, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, agent_id, tool_name, COALESCE(permission_kind, ''))
            DO NOTHING
            """,
            (owner_id, agent_id, tool_name, permission_kind, enabled, updated_at),
        )
        if result.rowcount not in {0, 1}:
            raise RepositoryDataError("tool override insert returned an invalid row count")
        return result.rowcount == 1

    def clear_tool_override(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        tool_name: str,
        permission_kind: str | None,
    ) -> bool:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        tool_name = _bounded_text(tool_name, "tool_name", maximum=512)
        if permission_kind is not None:
            permission_kind = _bounded_text(permission_kind, "permission_kind", maximum=256)
        row = transaction.fetch_one(
            """
            DELETE FROM tool_overrides
            WHERE user_id = %s AND agent_id = %s AND tool_name = %s
              AND permission_kind IS NOT DISTINCT FROM %s
            RETURNING id
            """,
            (owner_id, agent_id, tool_name, permission_kind),
        )
        return row is not None

    def list_legacy_permissions(
        self, transaction: Transaction, *, owner_id: str, agent_id: str
    ) -> tuple[LegacyToolPermission, ...]:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        rows = transaction.fetch_all(
            """
            SELECT user_id, agent_id, tool_name, allowed, updated_at
            FROM tool_permissions
            WHERE user_id = %s AND agent_id = %s
            ORDER BY tool_name
            """,
            (owner_id, agent_id),
        )
        return tuple(_legacy_permission(row) for row in rows)

    def remove_agent_state(self, transaction: Transaction, *, owner_id: str, agent_id: str) -> int:
        """Remove all three historical permission representations atomically."""

        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        removed = 0
        for table in ("agent_scopes", "tool_overrides", "tool_permissions"):
            result = transaction.execute(
                f"DELETE FROM {table} WHERE user_id = %s AND agent_id = %s",
                (owner_id, agent_id),
            )
            removed += max(result.rowcount, 0)
        return removed

    def remove_owner_state(self, transaction: Transaction, *, owner_id: str) -> int:
        owner_id = _required_id(owner_id, "owner_id")
        removed = 0
        for table in ("agent_scopes", "tool_overrides", "tool_permissions"):
            result = transaction.execute(f"DELETE FROM {table} WHERE user_id = %s", (owner_id,))
            removed += max(result.rowcount, 0)
        return removed

    def prune_agent_overrides(
        self,
        transaction: Transaction,
        *,
        agent_id: str,
        live_tool_names: Iterable[str],
    ) -> int:
        """Prune removed-tool rows across owners; caller authorizes this global sweep."""

        agent_id = _required_id(agent_id, "agent_id", maximum=512)
        live = tuple(
            sorted({_bounded_text(name, "tool_name", maximum=512) for name in live_tool_names})
        )
        if not live:
            result = transaction.execute(
                "DELETE FROM tool_overrides WHERE agent_id = %s", (agent_id,)
            )
        else:
            placeholders = ", ".join("%s" for _ in live)
            result = transaction.execute(
                f"""
                DELETE FROM tool_overrides
                WHERE agent_id = %s AND tool_name NOT IN ({placeholders})
                """,
                (agent_id, *live),
            )
        return max(result.rowcount, 0)

    def get_tool_selection(
        self, transaction: Transaction, *, owner_id: str, agent_id: str
    ) -> tuple[str, ...] | None:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        preferences = _read_preferences(transaction, owner_id)
        selections = preferences.get("tool_selection")
        if not isinstance(selections, Mapping):
            return None
        selected = selections.get(agent_id)
        if selected is None:
            return None
        if not isinstance(selected, list) or any(not isinstance(value, str) for value in selected):
            raise RepositoryDataError("persisted tool selection must be a string array")
        return tuple(selected)

    def set_tool_selection(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        tool_names: Iterable[str],
        updated_at: int,
    ) -> tuple[str, ...]:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        updated_at = _non_negative_int(updated_at, "updated_at")
        selected = _tool_names(tool_names)
        if not selected:
            raise RepositoryValidationError("tool selection must not be empty")
        preferences = _lock_preferences(transaction, owner_id, updated_at)
        raw_selections = preferences.get("tool_selection")
        selections = dict(raw_selections) if isinstance(raw_selections, Mapping) else {}
        selections[agent_id] = list(selected)
        preferences["tool_selection"] = selections
        _write_preferences(transaction, owner_id, preferences, updated_at)
        return selected

    def clear_tool_selection(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        updated_at: int,
    ) -> bool:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        updated_at = _non_negative_int(updated_at, "updated_at")
        preferences = _lock_preferences(transaction, owner_id, updated_at)
        raw_selections = preferences.get("tool_selection")
        if not isinstance(raw_selections, Mapping) or agent_id not in raw_selections:
            return False
        selections = dict(raw_selections)
        del selections[agent_id]
        preferences["tool_selection"] = selections
        _write_preferences(transaction, owner_id, preferences, updated_at)
        return True

    def list_disabled_agents(self, transaction: Transaction, *, owner_id: str) -> tuple[str, ...]:
        owner_id = _required_id(owner_id, "owner_id")
        value = _read_preferences(transaction, owner_id).get("disabled_agents")
        if value is None:
            return ()
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise RepositoryDataError("persisted disabled_agents must be a string array")
        return tuple(value)

    def set_agent_disabled(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        agent_id: str,
        disabled: bool,
        updated_at: int,
    ) -> bool:
        owner_id, agent_id = _owner_agent(owner_id, agent_id)
        if not isinstance(disabled, bool):
            raise RepositoryValidationError("disabled must be boolean")
        updated_at = _non_negative_int(updated_at, "updated_at")
        preferences = _lock_preferences(transaction, owner_id, updated_at)
        raw = preferences.get("disabled_agents")
        if raw is None:
            current: list[str] = []
        elif isinstance(raw, list) and all(isinstance(item, str) for item in raw):
            current = list(raw)
        else:
            raise RepositoryDataError("persisted disabled_agents must be a string array")
        present = agent_id in current
        if present == disabled:
            return False
        if disabled:
            current.append(agent_id)
        else:
            current = [item for item in current if item != agent_id]
        preferences["disabled_agents"] = current
        _write_preferences(transaction, owner_id, preferences, updated_at)
        return True


def _owner_agent(owner_id: str, agent_id: str) -> tuple[str, str]:
    return (
        _required_id(owner_id, "owner_id"),
        _required_id(agent_id, "agent_id", maximum=512),
    )


def _scope(row: Mapping[str, Any]) -> ScopeState:
    return ScopeState(
        owner_id=str(row["user_id"]),
        agent_id=str(row["agent_id"]),
        scope=str(row["scope"]),
        enabled=bool(row["enabled"]),
        updated_at=None if row.get("updated_at") is None else int(row["updated_at"]),
    )


def _override(row: Mapping[str, Any]) -> ToolOverrideState:
    return ToolOverrideState(
        owner_id=str(row["user_id"]),
        agent_id=str(row["agent_id"]),
        tool_name=str(row["tool_name"]),
        permission_kind=(
            None if row.get("permission_kind") is None else str(row["permission_kind"])
        ),
        enabled=bool(row["enabled"]),
        updated_at=None if row.get("updated_at") is None else int(row["updated_at"]),
    )


def _legacy_permission(row: Mapping[str, Any]) -> LegacyToolPermission:
    return LegacyToolPermission(
        owner_id=str(row["user_id"]),
        agent_id=str(row["agent_id"]),
        tool_name=str(row["tool_name"]),
        allowed=bool(row["allowed"]),
        updated_at=None if row.get("updated_at") is None else int(row["updated_at"]),
    )


def _tool_names(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise RepositoryValidationError("tool_names must be an iterable of names")
    selected: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = _bounded_text(value, "tool_name", maximum=512)
        if name not in seen:
            selected.append(name)
            seen.add(name)
    return tuple(selected)


def _decode_preferences(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise RepositoryDataError("persisted user preferences are not valid JSON") from exc
    if not isinstance(decoded, Mapping):
        raise RepositoryDataError("persisted user preferences must be an object")
    return {str(key): item for key, item in decoded.items()}


def _read_preferences(transaction: Transaction, owner_id: str) -> dict[str, Any]:
    row = transaction.fetch_one(
        "SELECT preferences FROM user_preferences WHERE user_id = %s", (owner_id,)
    )
    return {} if row is None else _decode_preferences(row.get("preferences"))


def _lock_preferences(transaction: Transaction, owner_id: str, updated_at: int) -> dict[str, Any]:
    transaction.execute(
        """
        INSERT INTO user_preferences (user_id, preferences, updated_at)
        VALUES (%s, '{}', %s) ON CONFLICT (user_id) DO NOTHING
        """,
        (owner_id, updated_at),
    )
    row = transaction.fetch_one(
        "SELECT preferences FROM user_preferences WHERE user_id = %s FOR UPDATE",
        (owner_id,),
    )
    if row is None:  # pragma: no cover - insert/select invariant
        raise RepositoryDataError("user preference lock returned no row")
    return _decode_preferences(row.get("preferences"))


def _write_preferences(
    transaction: Transaction,
    owner_id: str,
    preferences: Mapping[str, Any],
    updated_at: int,
) -> None:
    try:
        encoded = json.dumps(
            preferences,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise RepositoryValidationError("preferences must be JSON-compatible") from exc
    result = transaction.execute(
        """
        UPDATE user_preferences SET preferences = %s, updated_at = %s
        WHERE user_id = %s
        """,
        (encoded, updated_at, owner_id),
    )
    if result.rowcount != 1:
        raise RepositoryDataError("user preference update did not affect exactly one row")


__all__ = (
    "LegacyToolPermission",
    "ScopeState",
    "ScopedAgentOwnerRecord",
    "ToolOverrideState",
    "ToolPolicyStateRepository",
)
