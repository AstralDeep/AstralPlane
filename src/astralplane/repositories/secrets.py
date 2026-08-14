"""Opaque encrypted LLM-configuration persistence.

AstralPlane stores ciphertext and routing metadata only.  Encryption,
decryption, provider validation, and credential policy remain caller-owned.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from astralplane.contracts import QueryExecutor, Transaction
from astralplane.repositories import (
    RepositoryDataError,
    RepositoryNotFoundError,
    _bounded_text,
    _required_id,
    _row_value,
    _single_returned,
)


@dataclass(frozen=True, slots=True)
class EncryptedLLMConfigRecord:
    """Detached provider metadata whose secret remains opaque ciphertext."""

    scope: str
    owner_id: str | None
    provider: str
    base_url: str
    model: str
    api_key_ciphertext: str | None = field(repr=False)
    updated_by: str | None
    created_at: datetime
    updated_at: datetime


def _stored_time(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise RepositoryDataError(
            "persisted timestamp is not timezone-aware",
            metadata={"field": field_name},
        )
    return value


def _optional_ciphertext(value: object) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, "api key ciphertext", maximum=65_536)


def _user_record(row: Mapping[str, Any]) -> EncryptedLLMConfigRecord:
    return EncryptedLLMConfigRecord(
        scope="user",
        owner_id=str(_row_value(row, "user_id")),
        provider=str(_row_value(row, "provider")),
        base_url=str(_row_value(row, "base_url")),
        model=str(_row_value(row, "model")),
        api_key_ciphertext=_optional_ciphertext(row.get("api_key_enc")),
        updated_by=None,
        created_at=_stored_time(_row_value(row, "created_at"), "created_at"),
        updated_at=_stored_time(_row_value(row, "updated_at"), "updated_at"),
    )


def _system_record(row: Mapping[str, Any]) -> EncryptedLLMConfigRecord:
    return EncryptedLLMConfigRecord(
        scope="system",
        owner_id=None,
        provider=str(_row_value(row, "provider")),
        base_url=str(_row_value(row, "base_url")),
        model=str(_row_value(row, "model")),
        api_key_ciphertext=_optional_ciphertext(row.get("api_key_enc")),
        updated_by=str(_row_value(row, "updated_by")),
        created_at=_stored_time(_row_value(row, "created_at"), "created_at"),
        updated_at=_stored_time(_row_value(row, "updated_at"), "updated_at"),
    )


def _config_values(
    *,
    provider: object,
    base_url: object,
    model: object,
    api_key_ciphertext: object,
) -> tuple[str, str, str, str | None]:
    return (
        _bounded_text(provider, "provider", maximum=128),
        _bounded_text(base_url, "base url", maximum=4_096),
        _bounded_text(model, "model", maximum=512),
        _optional_ciphertext(api_key_ciphertext),
    )


class EncryptedLLMConfigRepository:
    """Owner-scoped user configuration plus one explicit system namespace."""

    _USER_FIELDS = "user_id, provider, base_url, model, api_key_enc, created_at, updated_at"
    _SYSTEM_FIELDS = "provider, base_url, model, api_key_enc, updated_by, created_at, updated_at"

    def get_user(
        self,
        executor: QueryExecutor,
        *,
        owner_id: str,
    ) -> EncryptedLLMConfigRecord | None:
        owner = _required_id(owner_id, "owner id")
        row = executor.fetch_one(
            f"SELECT {self._USER_FIELDS} FROM user_llm_config WHERE user_id = %s",
            (owner,),
        )
        return None if row is None else _user_record(row)

    def upsert_user(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        provider: str,
        base_url: str,
        model: str,
        api_key_ciphertext: str | None,
    ) -> EncryptedLLMConfigRecord:
        owner = _required_id(owner_id, "owner id")
        values = _config_values(
            provider=provider,
            base_url=base_url,
            model=model,
            api_key_ciphertext=api_key_ciphertext,
        )
        result = transaction.execute(
            f"""
            INSERT INTO user_llm_config (
                user_id, provider, base_url, model, api_key_enc, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, clock_timestamp(), clock_timestamp())
            ON CONFLICT (user_id) DO UPDATE SET
                provider = EXCLUDED.provider,
                base_url = EXCLUDED.base_url,
                model = EXCLUDED.model,
                api_key_enc = EXCLUDED.api_key_enc,
                updated_at = clock_timestamp()
            RETURNING {self._USER_FIELDS}
            """,
            (owner, *values),
        )
        return _user_record(_single_returned(result, "upsert user LLM configuration"))

    def delete_user(self, transaction: Transaction, *, owner_id: str) -> None:
        owner = _required_id(owner_id, "owner id")
        result = transaction.execute(
            "DELETE FROM user_llm_config WHERE user_id = %s",
            (owner,),
        )
        if result.rowcount != 1:
            raise RepositoryNotFoundError(
                "owner-scoped LLM configuration was not found",
                metadata={"operation": "delete user LLM configuration"},
            )

    def get_system(self, executor: QueryExecutor) -> EncryptedLLMConfigRecord | None:
        row = executor.fetch_one(
            f"SELECT {self._SYSTEM_FIELDS} FROM system_llm_config WHERE id = 1"
        )
        return None if row is None else _system_record(row)

    def upsert_system(
        self,
        transaction: Transaction,
        *,
        updated_by: str,
        provider: str,
        base_url: str,
        model: str,
        api_key_ciphertext: str | None,
    ) -> EncryptedLLMConfigRecord:
        actor = _required_id(updated_by, "updated by")
        values = _config_values(
            provider=provider,
            base_url=base_url,
            model=model,
            api_key_ciphertext=api_key_ciphertext,
        )
        result = transaction.execute(
            f"""
            INSERT INTO system_llm_config (
                id, provider, base_url, model, api_key_enc, updated_by, created_at, updated_at
            ) VALUES (1, %s, %s, %s, %s, %s, clock_timestamp(), clock_timestamp())
            ON CONFLICT (id) DO UPDATE SET
                provider = EXCLUDED.provider,
                base_url = EXCLUDED.base_url,
                model = EXCLUDED.model,
                api_key_enc = EXCLUDED.api_key_enc,
                updated_by = EXCLUDED.updated_by,
                updated_at = clock_timestamp()
            RETURNING {self._SYSTEM_FIELDS}
            """,
            (*values, actor),
        )
        return _system_record(_single_returned(result, "upsert system LLM configuration"))

    def delete_system(self, transaction: Transaction) -> None:
        result = transaction.execute("DELETE FROM system_llm_config WHERE id = 1")
        if result.rowcount != 1:
            raise RepositoryNotFoundError(
                "system LLM configuration was not found",
                metadata={"operation": "delete system LLM configuration"},
            )


__all__ = ("EncryptedLLMConfigRecord", "EncryptedLLMConfigRepository")
