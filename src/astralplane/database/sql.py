"""Native psycopg statement and parameter handling.

AstralPlane deliberately performs no lexical SQL translation. Question marks,
percent signs, JSON operators, comments, and string literals reach the driver
byte-for-byte as authored. Callers use psycopg's ``%s`` or ``%(name)s``
placeholder contract directly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypeAlias

from astralplane.contracts import Parameters, Statement
from astralplane.errors import SQLContractError

NativeParameters: TypeAlias = tuple[object, ...] | dict[str, object] | None


def validate_statement(statement: Statement) -> str:
    """Validate structural bounds without interpreting SQL text."""

    if not isinstance(statement, str):
        raise SQLContractError("statement must be a string")
    if not statement.strip():
        raise SQLContractError("statement must not be empty")
    if "\x00" in statement:
        raise SQLContractError("statement must not contain a NUL byte")
    return statement


def normalize_parameters(parameters: Parameters) -> NativeParameters:
    """Snapshot one native positional or named parameter collection.

    Empty collections become ``None`` so a no-parameter statement is passed to
    psycopg's one-argument ``execute`` form. This matters for literal percent
    signs: AstralPlane neither scans nor doubles them.
    """

    if isinstance(parameters, Mapping):
        normalized: dict[str, object] = {}
        for key, value in parameters.items():
            if not isinstance(key, str) or not key:
                raise SQLContractError("named parameter keys must be non-empty strings")
            normalized[key] = value
        return normalized or None
    if isinstance(parameters, Sequence) and not isinstance(
        parameters, (str, bytes, bytearray, memoryview)
    ):
        normalized_sequence = tuple(parameters)
        return normalized_sequence or None
    raise SQLContractError("parameters must be a positional sequence or named mapping")


def execute_native(cursor: Any, statement: Statement, parameters: Parameters = ()) -> Any:
    """Execute exact SQL through the driver's native parameter contract."""

    exact_statement = validate_statement(statement)
    native_parameters = normalize_parameters(parameters)
    if native_parameters is None:
        return cursor.execute(exact_statement)
    return cursor.execute(exact_statement, native_parameters)


__all__ = (
    "NativeParameters",
    "execute_native",
    "normalize_parameters",
    "validate_statement",
)
