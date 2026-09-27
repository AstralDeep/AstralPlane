"""Builds PostgreSQL template databases by running real Plane migrations once, then hands each
consumer a fresh CREATE DATABASE ... TEMPLATE copy that keeps the template's fixed schema name.
tests/conftest.py exposes the session-wide current-revision template and per-test clones; upgrade
suites build their own predecessor-revision templates with the same class.
"""

from __future__ import annotations

import re
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Final

import psycopg2
import pytest
from psycopg2.extensions import make_dsn

from astralplane.database.baseline import BaselineMigrationRunner
from astralplane.database.migrations import (
    CURRENT_DATA_PLANE_REVISION,
    MIGRATION_REGISTRY,
    MigrationRunner,
)
from astralplane.database.pool import ConnectionPool
from astralplane.database.transaction import PlaneDatabase
from tests.fixtures.pre_split.loader import TEST_DATABASE_ENV

TEMPLATE_SCHEMA: Final = "astralplane_fixture_" + "0" * 32
_IDENTIFIER: Final = re.compile(r"^(?:plane_tpl|plane_clone|astralplane_fixture)_[0-9a-f]{32}$")
_CLONE_LOCK: Final = threading.Lock()


class DedicatedDriverPool:
    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.borrowed = False

    def getconn(self) -> Any:
        if self.borrowed:
            raise RuntimeError("template database connection is already borrowed")
        self.borrowed = True
        return self.connection

    def putconn(self, connection: Any, *, close: bool = False) -> None:
        if connection is not self.connection or not self.borrowed or close:
            raise RuntimeError("template database connection was returned in an invalid state")
        self.borrowed = False

    def closeall(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class MigratedDatabase:
    name: str
    dsn: str
    schema: str
    connection: Any
    pool: ConnectionPool
    database: PlaneDatabase


def migrate_to_current(database: PlaneDatabase) -> None:
    BaselineMigrationRunner(
        database,
        MigrationRunner(
            database,
            revision=CURRENT_DATA_PLANE_REVISION,
            registry=MIGRATION_REGISTRY,
        ),
    ).run(expected_revision=CURRENT_DATA_PLANE_REVISION.schema_revision)


def _quoted(identifier: str) -> str:
    if _IDENTIFIER.fullmatch(identifier) is None:
        raise ValueError(f"refusing to quote an unexpected identifier: {identifier!r}")
    return f'"{identifier}"'


@contextmanager
def _connected(name: str, dsn: str, *, create_schema: bool) -> Iterator[MigratedDatabase]:
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            if create_schema:
                cursor.execute(f"CREATE SCHEMA {_quoted(TEMPLATE_SCHEMA)}")
            cursor.execute(f"SET search_path TO {_quoted(TEMPLATE_SCHEMA)}, pg_catalog")
        connection.commit()
        pool = ConnectionPool(DedicatedDriverPool(connection))
        try:
            yield MigratedDatabase(
                name=name,
                dsn=dsn,
                schema=TEMPLATE_SCHEMA,
                connection=connection,
                pool=pool,
                database=PlaneDatabase(pool),
            )
        finally:
            pool.close()
    finally:
        connection.close()


class DatabaseTemplate:
    def __init__(self, administrator_dsn: str, name: str) -> None:
        self.administrator_dsn = administrator_dsn
        self.name = name

    @classmethod
    def build(
        cls,
        administrator_dsn: str,
        migrate: Callable[[PlaneDatabase], object],
    ) -> DatabaseTemplate:
        template = cls(administrator_dsn, f"plane_tpl_{uuid.uuid4().hex}")
        template._administer(f"CREATE DATABASE {_quoted(template.name)} TEMPLATE template0")
        try:
            with _connected(
                template.name,
                make_dsn(administrator_dsn, dbname=template.name),
                create_schema=True,
            ) as built:
                migrate(built.database)
            # CREATE DATABASE ... TEMPLATE fails while any session is connected to the template.
            template._administer(
                f"ALTER DATABASE {_quoted(template.name)} WITH ALLOW_CONNECTIONS false"
            )
        except BaseException:
            template.drop()
            raise
        return template

    @contextmanager
    def clone(self) -> Iterator[MigratedDatabase]:
        name = f"plane_clone_{uuid.uuid4().hex}"
        with _CLONE_LOCK:
            self._administer(f"CREATE DATABASE {_quoted(name)} TEMPLATE {_quoted(self.name)}")
        try:
            with _connected(
                name,
                make_dsn(self.administrator_dsn, dbname=name),
                create_schema=False,
            ) as clone:
                yield clone
        finally:
            self._administer(f"DROP DATABASE IF EXISTS {_quoted(name)} WITH (FORCE)")

    def drop(self) -> None:
        self._administer(f"DROP DATABASE IF EXISTS {_quoted(self.name)} WITH (FORCE)")

    def __enter__(self) -> DatabaseTemplate:
        return self

    def __exit__(self, *_exception: object) -> None:
        self.drop()

    def _administer(self, statement: str) -> None:
        connection = psycopg2.connect(self.administrator_dsn)
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(statement)
        finally:
            connection.close()


@contextmanager
def bound_clone(template: DatabaseTemplate) -> Iterator[MigratedDatabase]:
    with template.clone() as clone, pytest.MonkeyPatch.context() as patch:
        patch.setenv(TEST_DATABASE_ENV, clone.dsn)
        yield clone
