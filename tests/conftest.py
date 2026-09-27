"""Session-wide PostgreSQL fixtures: one database migrated to the current revision by the real
migration runner, cloned per test so suites that need only a current catalog skip the full replay.
Built on tests/fixtures/migrated_template.py; skipped when ASTRALPLANE_TEST_POSTGRES_DSN is unset.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tests.fixtures.migrated_template import (
    DatabaseTemplate,
    MigratedDatabase,
    bound_clone,
    migrate_to_current,
)
from tests.fixtures.pre_split.loader import TEST_DATABASE_ENV


@pytest.fixture(scope="session")
def postgres_administrator_dsn() -> str:
    dsn = os.environ.get(TEST_DATABASE_ENV)
    if not dsn:
        pytest.skip(f"{TEST_DATABASE_ENV} is required for PostgreSQL integration tests")
    return dsn


@pytest.fixture(scope="session")
def migrated_template(postgres_administrator_dsn: str) -> Iterator[DatabaseTemplate]:
    with DatabaseTemplate.build(postgres_administrator_dsn, migrate_to_current) as template:
        yield template


@pytest.fixture
def migrated_clone(migrated_template: DatabaseTemplate) -> Iterator[MigratedDatabase]:
    with bound_clone(migrated_template) as clone:
        yield clone
