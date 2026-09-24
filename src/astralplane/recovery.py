"""Retires all web sessions on an explicitly named, already-restored database, for use
only after an operator has independently quiesced writers and verified the restore.
Never runs during ordinary startup or admission.
"""

from __future__ import annotations

from dataclasses import dataclass

from astralplane.database.migrations import CURRENT_DATA_PLANE_REVISION, MIGRATION_REGISTRY
from astralplane.database.pool import ConnectionPool
from astralplane.database.postgres import create_postgres_driver_pool
from astralplane.database.transaction import PlaneDatabase
from astralplane.errors import PlaneError
from astralplane.repositories.history import SessionRepository


class SessionRetirementError(PlaneError):
    default_code = "session_retirement_unavailable"


@dataclass(frozen=True, slots=True)
class RestoredSessionRetirement:
    retired_sessions: int


def _identifier(value: object) -> bool:
    if not isinstance(value, str) or not value or "\0" in value:
        return False
    try:
        return len(value.encode("utf-8")) <= 63
    except UnicodeEncodeError:
        return False


def retire_restored_sessions(
    *,
    database_url: str,
    expected_database: str,
    expected_schema: str,
    expected_schema_revision: str,
    expected_migration_digest: str,
) -> RestoredSessionRetirement:
    try:
        if (
            not _identifier(expected_database)
            or not _identifier(expected_schema)
            or expected_schema.startswith("pg_")
            or expected_schema == "information_schema"
            or expected_schema_revision != CURRENT_DATA_PLANE_REVISION.schema_revision
            or expected_migration_digest != CURRENT_DATA_PLANE_REVISION.migration_digest
            or expected_migration_digest != MIGRATION_REGISTRY.digest
        ):
            raise SessionRetirementError("restored session retirement input is incompatible")
        driver_pool = create_postgres_driver_pool(
            database_url,
            minimum_connections=1,
            maximum_connections=1,
            acquire_timeout_seconds=1,
            connect_timeout_seconds=5,
            application_name="astralplane:restored-session-retirement",
        )
        pool = ConnectionPool(driver_pool)
        try:
            with PlaneDatabase(pool).transaction() as transaction:
                sessions = SessionRepository()
                transaction.execute(
                    "SELECT pg_catalog.set_config('search_path', pg_catalog.concat("
                    "pg_catalog.quote_ident(pg_catalog.current_schema()), ',pg_temp'), true)"
                )
                sessions.bound_request_execution_waits(transaction)
                target = transaction.fetch_one(
                    "SELECT pg_catalog.current_database() AS database, "
                    "pg_catalog.current_schema() AS schema"
                )
                if (
                    target is None
                    or target["database"] != expected_database
                    or target["schema"] != expected_schema
                ):
                    raise SessionRetirementError("restored session retirement target differs")
                search_path = '"' + expected_schema.replace('"', '""') + '",pg_temp'
                transaction.execute(
                    "SELECT pg_catalog.set_config('search_path', %s, true)", (search_path,)
                )
                transaction.fetch_one(
                    "SELECT pg_catalog.pg_advisory_xact_lock(%s, %s)",
                    CURRENT_DATA_PLANE_REVISION.migration_lock,
                )
                # Lock first: a hostile view could run code via a bare select
                transaction.execute("LOCK TABLE schema_meta,web_session IN ACCESS EXCLUSIVE MODE")
                MIGRATION_REGISTRY.verify_current(transaction)
                metadata = transaction.fetch_all(
                    "SELECT key,value FROM schema_meta WHERE key IN (%s,%s)",
                    ("revision", "astralplane_migration_digest"),
                )
                if len(metadata) != 2 or {row["key"]: row["value"] for row in metadata} != {
                    "revision": expected_schema_revision,
                    "astralplane_migration_digest": expected_migration_digest,
                }:
                    raise SessionRetirementError("restored session retirement metadata differs")
                retired = sessions.retire_all_for_recovery(transaction)
            result = RestoredSessionRetirement(retired)
        finally:
            pool.close()
        return result
    except Exception:
        raise SessionRetirementError("restored session retirement unavailable") from None


__all__ = ("RestoredSessionRetirement", "SessionRetirementError", "retire_restored_sessions")
