"""Shared repository invariants for AstralPlane's durable domain stores.

Repository methods accept an explicit :class:`~astralplane.contracts.Transaction`
or query executor.  They never borrow connections or commit transactions, so a
product can compose multiple repository operations into one authoritative unit.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, NoReturn

from astralplane.errors import PlaneError


class RepositoryError(PlaneError):
    """Base failure for a neutral durable-domain repository."""

    default_code = "repository_error"


class RepositoryConflictError(RepositoryError):
    """A compare-and-set or idempotency fence rejected a write."""

    default_code = "repository_conflict"


class RepositoryDataError(RepositoryError):
    """Persisted data did not satisfy the repository's detached record contract."""

    default_code = "repository_data_invalid"


class RepositoryNotFoundError(RepositoryError):
    """An owner-scoped authoritative row was unavailable for a required write."""

    default_code = "repository_not_found"


class RepositoryValidationError(RepositoryError, ValueError):
    """Caller input violated a bounded neutral persistence contract."""

    default_code = "repository_validation"


def _required_id(value: object, field: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RepositoryValidationError(f"{field} must be a non-empty string")
    if len(value) > maximum:
        raise RepositoryValidationError(
            f"{field} exceeds its maximum length",
            metadata={"field": field, "maximum": maximum},
        )
    return value


def _bounded_text(
    value: object,
    field: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise RepositoryValidationError(f"{field} must be a string")
    if not allow_empty and not value.strip():
        raise RepositoryValidationError(f"{field} must not be empty")
    if len(value) > maximum:
        raise RepositoryValidationError(
            f"{field} exceeds its maximum length",
            metadata={"field": field, "maximum": maximum},
        )
    return value


def _bounded_limit(value: object, *, maximum: int = 200) -> int:
    if isinstance(value, bool):
        raise RepositoryValidationError("limit must be an integer")
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise RepositoryValidationError("limit must be an integer") from exc
    if limit < 1 or limit > maximum:
        raise RepositoryValidationError(
            "limit is outside the supported range",
            metadata={"maximum": maximum},
        )
    return limit


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise RepositoryValidationError(f"{field} must be a non-negative integer")
    try:
        integer = int(value)
    except (TypeError, ValueError) as exc:
        raise RepositoryValidationError(f"{field} must be a non-negative integer") from exc
    if integer < 0:
        raise RepositoryValidationError(f"{field} must be a non-negative integer")
    return integer


def _positive_int(value: object, field: str) -> int:
    integer = _non_negative_int(value, field)
    if integer == 0:
        raise RepositoryValidationError(f"{field} must be a positive integer")
    return integer


def _canonical_json(value: object, field: str) -> str:
    def json_ready(item: object) -> object:
        if isinstance(item, Mapping):
            return {str(key): json_ready(nested) for key, nested in item.items()}
        if isinstance(item, (list, tuple)):
            return [json_ready(nested) for nested in item]
        return item

    try:
        return json.dumps(
            json_ready(value),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise RepositoryValidationError(f"{field} must be canonical JSON-compatible data") from exc


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


def _structured_json(value: object, field: str, *, nullable: bool = False) -> Any:
    if value is None and nullable:
        return None
    decoded: object = value
    if isinstance(value, (str, bytes, bytearray)):
        try:
            decoded = json.loads(value)
        except (TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RepositoryDataError(
                f"persisted {field} is not valid JSON", metadata={"field": field}
            ) from exc
    if not isinstance(decoded, (Mapping, Sequence)) or isinstance(
        decoded, (str, bytes, bytearray, memoryview)
    ):
        raise RepositoryDataError(
            f"persisted {field} has an unsupported JSON shape",
            metadata={"field": field},
        )
    return _freeze(decoded)


def _content_value(value: object) -> Any:
    """Decode legacy JSON content while preserving ordinary prose verbatim."""

    if not isinstance(value, str):
        return _freeze(value)
    try:
        return _freeze(json.loads(value))
    except json.JSONDecodeError:
        return value


def _row_value(row: Mapping[str, Any], field: str) -> Any:
    try:
        return row[field]
    except KeyError as exc:
        raise RepositoryDataError(
            "persisted row is missing a required field", metadata={"field": field}
        ) from exc


def _single_returned(result: object, operation: str) -> Mapping[str, Any]:
    rows = getattr(result, "returned_records", ())
    if len(rows) != 1:
        raise RepositoryDataError(
            "write did not return exactly one detached record",
            metadata={"operation": operation, "returned": len(rows)},
        )
    row = rows[0]
    if not isinstance(row, Mapping):
        raise RepositoryDataError(
            "write returned a non-mapping record", metadata={"operation": operation}
        )
    return row


def _raise_write_miss(
    existing: object,
    *,
    operation: str,
    conflict_message: str,
) -> NoReturn:
    if existing is None:
        raise RepositoryNotFoundError(
            "owner-scoped row was not found", metadata={"operation": operation}
        )
    raise RepositoryConflictError(conflict_message, metadata={"operation": operation})


__all__ = (
    "RepositoryConflictError",
    "RepositoryDataError",
    "RepositoryError",
    "RepositoryNotFoundError",
    "RepositoryValidationError",
)
