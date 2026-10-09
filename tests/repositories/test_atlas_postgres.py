"""Real-PostgreSQL tests for astralplane.repositories.atlas: atomic appends,
fenced concurrent edits, rollback, revision immutability, tombstones, and the
guarded 089.001 -> 089.002 migration edge with repeat-safe recovery.
"""

from __future__ import annotations

import os
import uuid

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
from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryNotFoundError,
)
from astralplane.repositories.atlas import AtlasRepository

_PG17_ADMIN_ENV = "ASTRALPLANE_TEST_POSTGRES_DSN"


def uid4() -> str:
    return str(uuid.uuid4())


class _SingleConnectionPool:
    def __init__(self, connection):
        self._connection = connection

    def getconn(self):
        return self._connection

    def putconn(self, connection, *, close=False):
        assert connection is self._connection

    def closeall(self):
        pass


def _second_database(clone, administrator_dsn: str) -> tuple[object, PlaneDatabase]:
    connection = psycopg2.connect(make_dsn(administrator_dsn, dbname=clone.name))
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SET search_path TO "{clone.schema}", pg_catalog')
        connection.commit()
    except BaseException:
        connection.close()
        raise
    pool = ConnectionPool(_SingleConnectionPool(connection))
    return connection, PlaneDatabase(pool)


@pytest.fixture
def atlas_db(migrated_clone):
    database = migrated_clone.database
    with database.transaction() as transaction:
        transaction.execute("DELETE FROM atlas_revision")
        transaction.execute("DELETE FROM atlas_page")
    return database


@pytest.fixture
def repository() -> AtlasRepository:
    return AtlasRepository()


def _create(repository, tx, *, owner="owner-a", slug="atlas-page", request=None):
    return repository.create_page(
        tx,
        owner_id=owner,
        page_id=uid4(),
        slug=slug,
        title="Atlas page",
        ciphertext=b"ciphertext-1",
        request_id=request or uid4(),
    )


def test_postgres_round_trip_appends_and_reads_back_opaque_bodies(
    atlas_db, repository
) -> None:
    with atlas_db.transaction() as tx:
        created = _create(repository, tx)
        page = created.head.page_id
        assert created.head.head_revision == 1
        assert created.revision.predecessor_digest is None

        appended = repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            title="Second",
            ciphertext=b"\x00\x01second",
            request_id=uid4(),
        )
        assert appended.head.head_revision == 2
        assert appended.revision.ciphertext == b"\x00\x01second"

        head = repository.get_page(tx, owner_id="owner-a", page_id=page)
        assert (head.head_revision, head.slug, head.deleted) == (2, "atlas-page", False)
        first = repository.get_revision(tx, owner_id="owner-a", page_id=page, revision=1)
        assert first.ciphertext == b"ciphertext-1"
        assert appended.revision.predecessor_digest == first.content_digest

        history = repository.list_revisions(tx, owner_id="owner-a", page_id=page)
        assert [record.revision for record in history.revisions] == [1, 2]
        report = repository.verify_page_chain(tx, owner_id="owner-a", page_id=page)
        assert report.consistent is True


def test_postgres_failed_transaction_leaves_no_page_behind(atlas_db, repository) -> None:
    page, request = uid4(), uid4()
    try:
        with atlas_db.transaction() as tx:
            repository.create_page(
                tx,
                owner_id="owner-a",
                page_id=page,
                slug="rolled-back",
                title="Atlas page",
                ciphertext=b"ciphertext-1",
                request_id=request,
            )
            raise RuntimeError("simulated caller failure")
    except RuntimeError:
        pass

    with atlas_db.transaction() as tx:
        with pytest.raises(RepositoryNotFoundError):
            repository.get_page(tx, owner_id="owner-a", page_id=page)
        with pytest.raises(RepositoryNotFoundError):
            repository.get_page_by_slug(tx, owner_id="owner-a", slug="rolled-back")


def test_postgres_stale_second_writer_loses_while_first_commits(
    atlas_db, repository, migrated_clone, postgres_administrator_dsn
) -> None:
    with atlas_db.transaction() as tx:
        created = _create(repository, tx, slug="raced-page")
    page = created.head.page_id

    second_connection, second_database = _second_database(
        migrated_clone, postgres_administrator_dsn
    )
    try:
        with atlas_db.transaction() as first_tx:
            repository.append_revision(
                first_tx,
                owner_id="owner-a",
                page_id=page,
                expected_head=1,
                title="First writer",
                ciphertext=b"first",
                request_id=uid4(),
            )
            with second_database.transaction() as second_tx, pytest.raises(RepositoryConflictError):
                repository.append_revision(
                    second_tx,
                    owner_id="owner-a",
                    page_id=page,
                    expected_head=1,
                    title="Stale writer",
                    ciphertext=b"stale",
                    request_id=uid4(),
                )
    finally:
        second_connection.close()

    with atlas_db.transaction() as tx:
        head = repository.get_page(tx, owner_id="owner-a", page_id=page)
        assert head.head_revision == 2
        second = repository.get_revision(tx, owner_id="owner-a", page_id=page, revision=2)
        assert second.title == "First writer"


def test_postgres_revisions_are_immutable(atlas_db, repository) -> None:
    import psycopg2.errors

    with atlas_db.transaction() as tx:
        created = _create(repository, tx)
        page = created.head.page_id
        with pytest.raises(  # noqa: SIM117 -- savepoint must stay nested
            psycopg2.errors.CheckViolation, match="immutable"
        ):
            with tx.savepoint("immutable_check"):
                tx.execute(
                    "UPDATE atlas_revision SET title=%s WHERE owner_id=%s AND page_id=%s",
                    ("mutated", "owner-a", page),
                )


def test_postgres_unique_identities_hold_across_owners(atlas_db, repository) -> None:
    with atlas_db.transaction() as tx:
        created = _create(repository, tx, owner="owner-a", slug="shared-slug")
        page = created.head.page_id
        with pytest.raises(  # noqa: SIM117 -- savepoint must stay nested
            Exception, match=r"(?i)(duplicate|unique|conflict)"
        ):
            with tx.savepoint("identity_clash"):
                tx.execute(
                    "INSERT INTO atlas_page(owner_id,page_id,slug,head_revision,deleted,"
                    "deleted_reason,created_at,updated_at) VALUES(%s,%s,%s,1,FALSE,NULL,1,1)",
                    ("owner-b", page, "other-slug"),
                )


def test_postgres_delete_then_history_still_verifies(atlas_db, repository) -> None:
    with atlas_db.transaction() as tx:
        created = _create(repository, tx)
        page = created.head.page_id
        deleted = repository.delete_page(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            reason="superseded",
            request_id=uid4(),
        )
        assert deleted.head.deleted is True
        history = repository.list_revisions(tx, owner_id="owner-a", page_id=page)
        assert [record.revision for record in history.revisions] == [1, 2]
        assert history.revisions[-1].deleted is True
        assert repository.verify_page_chain(tx, owner_id="owner-a", page_id=page).consistent


def test_postgres_chain_report_detects_a_removed_middle_revision(
    atlas_db, repository
) -> None:
    with atlas_db.transaction() as tx:
        created = _create(repository, tx)
        page = created.head.page_id
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            title="Second",
            ciphertext=b"ciphertext-2",
            request_id=uid4(),
        )
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=2,
            title="Third",
            ciphertext=b"ciphertext-3",
            request_id=uid4(),
        )
        tx.execute(
            "DELETE FROM atlas_revision WHERE owner_id=%s AND page_id=%s AND revision=2",
            ("owner-a", page),
        )
        report = repository.verify_page_chain(tx, owner_id="owner-a", page_id=page)
        assert report.consistent is False
        assert report.contiguous is False


def _prior_089_001_registry():
    from astralplane.database import migrations as canonical

    return canonical.MigrationRegistry(
        tuple(
            e
            for e in canonical.MIGRATION_REGISTRY.migrations
            if e.target_revision <= "089.001"
        ),
        current_schema_verifier=lambda tx: canonical._verify_predecessor_plane_schema(
            tx, "089.001"
        ),
        current_schema_verifier_checksum=(
            "155427334f10cae9a4fb0103b8ada8916762d7dfcb61baadf607bc2431c6fba1"
        ),
        predecessor_schema_verifier=canonical._verify_predecessor_plane_schema,
        predecessor_schema_verifier_checksum=(
            "a91bdcd9592cb719168e81e08b046c90068d26771d68eed5df72647170e3b5ad"
        ),
    )


def _prior_089_001_runner(database):
    from dataclasses import replace

    from astralplane.database import migrations as canonical

    registry = _prior_089_001_registry()
    assert registry.digest != canonical.MIGRATION_REGISTRY.digest
    revision = replace(
        canonical.CURRENT_DATA_PLANE_REVISION,
        schema_revision="089.001",
        migration_digest=registry.digest,
        read_compatible_from=tuple(
            r
            for r in canonical.CURRENT_DATA_PLANE_REVISION.read_compatible_from
            if r < "089.001"
        ),
        accepted_predecessor_digests=tuple(
            p
            for p in canonical.CURRENT_DATA_PLANE_REVISION.accepted_predecessor_digests
            if p[0] < "089.001"
        ),
    )
    return MigrationRunner(database, revision=revision, registry=registry)


def test_postgres_089_002_edge_applies_once_and_repeats_as_noop(
    migrated_clone, postgres_administrator_dsn
) -> None:
    administrator_dsn = postgres_administrator_dsn
    edge_schema = "atlas_edge_" + uuid.uuid4().hex
    admin = psycopg2.connect(make_dsn(administrator_dsn, dbname=migrated_clone.name))
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{edge_schema}"')
    finally:
        admin.close()

    connection = psycopg2.connect(make_dsn(administrator_dsn, dbname=migrated_clone.name))
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SET search_path TO "{edge_schema}", pg_catalog')
        connection.commit()
        pool = ConnectionPool(_SingleConnectionPool(connection))
        try:
            database = PlaneDatabase(pool)
            first = BaselineMigrationRunner(
                database,
                _prior_089_001_runner(database),
            ).run(expected_revision="089.001")
            assert first.target_revision == "089.001"
            assert first.applied_steps[-1] == "astralplane-089-typesafe-credentials"

            # The scratch 089.001 schema carries this test's truncated
            # registry digest (historical verifier pins are not recoverable),
            # so the edge runner accepts it for the 089.001 predecessor slot.
            # Schema CONTENT is the genuine 089.001 migration output, and the
            # structure verifier still pins the canonical 089.001 digest.
            from dataclasses import replace

            edge_revision = replace(
                CURRENT_DATA_PLANE_REVISION,
                accepted_predecessor_digests=tuple(
                    ("089.001", _prior_089_001_registry().digest)
                    if p[0] == "089.001"
                    else p
                    for p in CURRENT_DATA_PLANE_REVISION.accepted_predecessor_digests
                ),
            )
            edge = MigrationRunner(
                database,
                revision=edge_revision,
                registry=MIGRATION_REGISTRY,
            ).run(expected_revision="089.002")
            assert edge.source_revision == "089.001"
            assert edge.target_revision == "089.002"
            assert edge.applied_steps == ("astralplane-089-atlas-revisions",)
            assert edge.migration_digest == MIGRATION_REGISTRY.digest
            assert not edge.already_current

            repository = AtlasRepository()
            with database.transaction() as tx:
                created = repository.create_page(
                    tx,
                    owner_id="edge-owner",
                    page_id=uid4(),
                    slug="edge-page",
                    title="Edge page",
                    ciphertext=b"edge",
                    request_id=uid4(),
                )
                assert created.head.head_revision == 1

            repeat = MigrationRunner(
                database,
                revision=CURRENT_DATA_PLANE_REVISION,
                registry=MIGRATION_REGISTRY,
            ).run(expected_revision="089.002")
            assert repeat.already_current
            assert repeat.applied_steps == ()
            assert repeat.migration_digest == edge.migration_digest
        finally:
            pool.close()
    finally:
        connection.rollback()
        connection.close()
    admin = psycopg2.connect(make_dsn(administrator_dsn, dbname=migrated_clone.name))
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA "{edge_schema}" CASCADE')
    finally:
        admin.close()


def test_postgres_migration_registry_digest_is_current() -> None:
    from astralplane import MIGRATION_DIGEST
    from astralplane.database.migrations import MIGRATION_DIGEST as CANONICAL_DIGEST

    assert MIGRATION_DIGEST == CANONICAL_DIGEST == MIGRATION_REGISTRY.digest
    if os.environ.get(_PG17_ADMIN_ENV) is None:
        pytest.skip(f"{_PG17_ADMIN_ENV} is required for PostgreSQL integration tests")
