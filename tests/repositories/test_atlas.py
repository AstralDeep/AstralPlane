"""Unit tests for astralplane.repositories.atlas: immutable revisions, fenced page
edits, idempotent replay, bounded history, and chain recovery, exercised against
a stateful in-memory transaction double (no PostgreSQL required).
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

import pytest

from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryNotFoundError,
    RepositoryValidationError,
)
from astralplane.repositories.atlas import (
    AtlasRepository,
)


def uid4() -> str:
    return str(uuid.uuid4())


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class FakeAtlasTransaction:
    """Minimal stateful double emulating the atlas_page/atlas_revision tables."""

    def __init__(self, *, now_ms: int = 1700000000000) -> None:
        self.now_ms = now_ms
        self.pages: dict[tuple[str, str], dict[str, Any]] = {}
        self.revisions: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.requests: dict[tuple[str, str], tuple[str, str, int]] = {}
        self.calls: list[str] = []

    # -- Transaction protocol -------------------------------------------------
    def fetch_one(self, statement: str, parameters: object = ()) -> dict[str, Any] | None:
        self.calls.append(statement)
        params = tuple(parameters) if isinstance(parameters, (list, tuple)) else ()
        if "advisory_xact_lock" in statement:
            return {"acquired": True}
        if "clock_timestamp()" in statement:
            return {"now_ms": self.now_ms}
        if "FROM atlas_revision" in statement and "request_id=%s" in statement:
            owner, request = params[0], params[1]
            key = self.requests.get((str(owner), str(request)))
            if key is None:
                return None
            return dict(self.revisions[key])
        if statement.strip().startswith("SELECT page_id FROM atlas_page"):
            if "slug=%s" in statement:
                owner, slug = str(params[0]), str(params[1])
                for (candidate_owner, _), row in self.pages.items():
                    if candidate_owner == owner and row["slug"] == slug:
                        return {"page_id": row["page_id"]}
                return None
            identity = str(params[0])
            for (_, candidate), _ in self.pages.items():
                if candidate == identity:
                    return {"page_id": identity}
            return None
        if "FROM atlas_page" in statement and "slug=%s" in statement:
            owner, slug = str(params[0]), str(params[1])
            for (candidate_owner, _), row in self.pages.items():
                if candidate_owner == owner and row["slug"] == slug:
                    return dict(row)
            return None
        if "FROM atlas_page" in statement and "page_id=%s" in statement:
            if "slug=%s" in statement:
                owner, slug = str(params[0]), str(params[1])
                for (candidate_owner, _), row in self.pages.items():
                    if candidate_owner == owner and row["slug"] == slug:
                        return self._project_page(statement, row)
                return None
            owner, identity = str(params[0]), str(params[1])
            row = self.pages.get((owner, identity))
            if row is None:
                return None
            return self._project_page(statement, row)
        if "count(*) AS count FROM atlas_page" in statement:
            owner = str(params[0])
            total = sum(1 for (candidate_owner, _) in self.pages if candidate_owner == owner)
            return {"count": total}
        if "FROM atlas_revision" in statement and "revision=%s" in statement:
            owner, identity, number = str(params[0]), str(params[1]), params[2]
            row = self.revisions.get((owner, identity, int(number)))  # type: ignore[arg-type]
            return None if row is None else dict(row)
        if "SELECT head_revision FROM atlas_page" in statement:
            owner, identity = str(params[0]), str(params[1])
            row = self.pages.get((owner, identity))
            if row is None:
                return None
            return {"head_revision": row["head_revision"]}
        raise AssertionError(f"unexpected fetch_one: {statement}")

    def fetch_all(self, statement: str, parameters: object = ()) -> tuple[dict[str, Any], ...]:
        self.calls.append(statement)
        params = tuple(parameters) if isinstance(parameters, (list, tuple)) else ()
        if statement.strip().startswith("SELECT revision FROM atlas_revision"):
            owner, identity = str(params[0]), str(params[1])
            numbers = sorted(
                revision
                for (candidate_owner, candidate_page, revision) in self.revisions
                if candidate_owner == owner and candidate_page == identity
            )
            return tuple({"revision": number} for number in numbers)
        if "FROM atlas_revision" in statement:
            owner, identity, after = str(params[0]), str(params[1]), params[2]
            rows = [
                row
                for (candidate_owner, candidate_page, _), row in sorted(
                    self.revisions.items(), key=lambda item: item[0][2]
                )
                if candidate_owner == owner
                and candidate_page == identity
                and (after is None or row["revision"] > int(after))  # type: ignore[arg-type]
            ]
            limit = int(params[4])
            return tuple(dict(row) for row in rows[:limit])
        if "FROM atlas_page" in statement:
            owner, after, _, include_deleted, limit = (
                str(params[0]),
                params[1],
                params[2],
                params[3],
                int(params[4]),
            )
            rows = [
                row
                for (candidate_owner, _), row in sorted(
                    self.pages.items(), key=lambda item: item[1]["slug"]
                )
                if candidate_owner == owner
                and (after is None or row["slug"] > str(after))
                and (bool(include_deleted) or not row["deleted"])
            ]
            return tuple(dict(row) for row in rows[:limit])
        raise AssertionError(f"unexpected fetch_all: {statement}")

    def execute(self, statement: str, parameters: object = ()) -> Any:
        self.calls.append(statement)
        params = tuple(parameters) if isinstance(parameters, (list, tuple)) else ()
        if statement.startswith("INSERT INTO atlas_page"):
            owner, identity, slug, created, updated = (
                str(params[0]),
                str(params[1]),
                str(params[2]),
                int(params[3]),
                int(params[4]),
            )
            self.pages[(owner, identity)] = {
                "owner_id": owner,
                "page_id": identity,
                "slug": slug,
                "head_revision": 1,
                "deleted": False,
                "deleted_reason": None,
                "created_at": created,
                "updated_at": updated,
            }
            return None
        if statement.startswith("INSERT INTO atlas_revision"):
            if len(params) == 8:
                owner, identity, title, body, digest, created, request, fingerprint = (
                    str(params[0]),
                    str(params[1]),
                    str(params[2]),
                    bytes(params[3]),  # type: ignore[arg-type]
                    str(params[4]),
                    int(params[5]),
                    str(params[6]),
                    str(params[7]),
                )
                number, deleted, predecessor = 1, False, None
            else:
                owner, identity, number, title, body, digest, predecessor, created, request = (
                    str(params[0]),
                    str(params[1]),
                    int(params[2]),
                    str(params[3]),
                    bytes(params[4]),  # type: ignore[arg-type]
                    str(params[5]),
                    None if params[6] is None else str(params[6]),
                    int(params[7]),
                    str(params[8]),
                )
                fingerprint = str(params[9])
                deleted = ",TRUE," in statement
            if (owner, request) in self.requests:
                return None
            self.revisions[(owner, identity, number)] = {
                "owner_id": owner,
                "page_id": identity,
                "revision": number,
                "title": title,
                "ciphertext": body,
                "content_digest": digest,
                "predecessor_digest": predecessor,
                "created_at": created,
                "deleted": deleted,
                "request_id": request,
                "request_digest": fingerprint,
            }
            self.requests[(owner, request)] = (owner, identity, number)
            return None
        if statement.startswith("UPDATE atlas_page SET head_revision="):
            if "deleted=TRUE" in statement:
                number, reason, updated, owner, identity = (
                    int(params[0]),
                    str(params[1]),
                    int(params[2]),
                    str(params[3]),
                    str(params[4]),
                )
                row = self.pages[(owner, identity)]
                row["head_revision"] = number
                row["deleted"] = True
                row["deleted_reason"] = reason
                row["updated_at"] = updated
            else:
                number, updated, owner, identity = (
                    int(params[0]),
                    int(params[1]),
                    str(params[2]),
                    str(params[3]),
                )
                row = self.pages[(owner, identity)]
                row["head_revision"] = number
                row["updated_at"] = updated
            return None
        raise AssertionError(f"unexpected execute: {statement}")

    @staticmethod
    def _project_page(statement: str, row: dict[str, Any]) -> dict[str, Any]:
        if statement.strip().startswith("SELECT page_id FROM"):
            return {"page_id": row["page_id"]}
        return dict(row)


@pytest.fixture
def repository() -> AtlasRepository:
    return AtlasRepository()


@pytest.fixture
def tx() -> FakeAtlasTransaction:
    return FakeAtlasTransaction()


def _create(
    repository: AtlasRepository,
    tx: FakeAtlasTransaction,
    *,
    owner: str = "owner-a",
    page: str | None = None,
    slug: str = "atlas-page",
    title: str = "Atlas page",
    body: bytes = b"ciphertext-1",
    request: str | None = None,
) -> Any:
    return repository.create_page(
        tx,
        owner_id=owner,
        page_id=page or uid4(),
        slug=slug,
        title=title,
        ciphertext=body,
        request_id=request or uid4(),
    )


def test_create_page_appends_revision_one_and_advances_head(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    page, request = uid4(), uid4()
    result = repository.create_page(
        tx,
        owner_id="owner-a",
        page_id=page,
        slug="atlas-page",
        title="Atlas page",
        ciphertext=b"ciphertext-1",
        request_id=request,
    )

    assert result.replayed is False
    assert result.head.head_revision == 1
    assert result.head.deleted is False
    assert result.head.deleted_reason is None
    assert result.revision.revision == 1
    assert result.revision.ciphertext == b"ciphertext-1"
    assert result.revision.content_digest == _digest(b"ciphertext-1")
    assert result.revision.predecessor_digest is None
    assert result.revision.request_id == request
    assert "pg_try_advisory_xact_lock" in "\n".join(tx.calls)
    assert "FOR UPDATE" in "\n".join(tx.calls)


def test_append_revision_links_predecessor_digest(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx)
    page = created.head.page_id

    result = repository.append_revision(
        tx,
        owner_id="owner-a",
        page_id=page,
        expected_head=1,
        title="Second",
        ciphertext=b"ciphertext-2",
        request_id=uid4(),
    )

    assert result.replayed is False
    assert result.head.head_revision == 2
    assert result.revision.revision == 2
    assert result.revision.predecessor_digest == _digest(b"ciphertext-1")
    assert result.revision.content_digest == _digest(b"ciphertext-2")

    history = repository.list_revisions(tx, owner_id="owner-a", page_id=page)
    assert [record.revision for record in history.revisions] == [1, 2]
    assert history.next_after is None


def test_exact_replay_returns_stored_result_without_another_write(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    page, request = uid4(), uid4()
    first = repository.create_page(
        tx,
        owner_id="owner-a",
        page_id=page,
        slug="atlas-page",
        title="Atlas page",
        ciphertext=b"ciphertext-1",
        request_id=request,
    )
    writes = len([call for call in tx.calls if call.startswith("INSERT")])

    second = repository.create_page(
        tx,
        owner_id="owner-a",
        page_id=page,
        slug="atlas-page",
        title="Atlas page",
        ciphertext=b"ciphertext-1",
        request_id=request,
    )

    assert second.replayed is True
    assert second.revision == first.revision
    assert second.head == first.head
    assert len([call for call in tx.calls if call.startswith("INSERT")]) == writes

    appended = repository.append_revision(
        tx,
        owner_id="owner-a",
        page_id=page,
        expected_head=1,
        title="Second",
        ciphertext=b"ciphertext-2",
        request_id=(append_request := uid4()),
    )
    replayed_append = repository.append_revision(
        tx,
        owner_id="owner-a",
        page_id=page,
        expected_head=1,
        title="Second",
        ciphertext=b"ciphertext-2",
        request_id=append_request,
    )
    assert replayed_append.replayed is True
    assert replayed_append.revision == appended.revision


def test_reused_request_identity_with_conflicting_semantics_is_rejected(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx, request=(request := uid4()))
    page = created.head.page_id

    with pytest.raises(RepositoryConflictError):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            title="Different title",
            ciphertext=b"ciphertext-2",
            request_id=request,
        )


def test_stale_concurrent_edit_is_rejected(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
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

    with pytest.raises(RepositoryConflictError):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            title="Stale",
            ciphertext=b"stale",
            request_id=uid4(),
        )


def test_wrong_owner_cannot_observe_or_mutate_a_page(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx, owner="owner-a")
    page = created.head.page_id

    with pytest.raises(RepositoryNotFoundError):
        repository.get_page(tx, owner_id="owner-b", page_id=page)
    with pytest.raises(RepositoryNotFoundError):
        repository.get_revision(tx, owner_id="owner-b", page_id=page, revision=1)
    with pytest.raises(RepositoryNotFoundError):
        repository.append_revision(
            tx,
            owner_id="owner-b",
            page_id=page,
            expected_head=1,
            title="Hijack",
            ciphertext=b"x",
            request_id=uid4(),
        )


def test_delete_page_writes_tombstone_and_blocks_resurrection(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx, slug="gone-page")
    page = created.head.page_id

    deleted = repository.delete_page(
        tx,
        owner_id="owner-a",
        page_id=page,
        expected_head=1,
        reason="withdrawn",
        request_id=uid4(),
    )
    assert deleted.head.deleted is True
    assert deleted.head.deleted_reason == "withdrawn"
    assert deleted.head.head_revision == 2
    assert deleted.revision.deleted is True
    assert deleted.revision.ciphertext == b""
    assert deleted.revision.predecessor_digest == _digest(b"ciphertext-1")

    with pytest.raises(RepositoryNotFoundError):
        repository.get_page(tx, owner_id="owner-a", page_id=page)
    assert (
        repository.get_page(tx, owner_id="owner-a", page_id=page, include_deleted=True)
    ).deleted is True

    with pytest.raises(RepositoryConflictError):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=2,
            title="Resurrect",
            ciphertext=b"alive",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryConflictError):
        repository.delete_page(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=2,
            reason="superseded",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryConflictError):
        repository.create_page(
            tx,
            owner_id="owner-a",
            page_id=page,
            slug="new-slug",
            title="Resurrect",
            ciphertext=b"alive",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryConflictError):
        repository.create_page(
            tx,
            owner_id="owner-a",
            page_id=uid4(),
            slug="gone-page",
            title="Resurrect",
            ciphertext=b"alive",
            request_id=uid4(),
        )


def test_duplicate_identities_are_rejected(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx, owner="owner-a", slug="taken")
    page = created.head.page_id

    with pytest.raises(RepositoryConflictError):
        _create(repository, tx, owner="owner-a", page=page, slug="other")
    with pytest.raises(RepositoryConflictError):
        _create(repository, tx, owner="owner-b", page=page, slug="other")
    with pytest.raises(RepositoryConflictError):
        _create(repository, tx, owner="owner-a", slug="taken")


def test_slug_namespace_is_owner_scoped(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    _create(repository, tx, owner="owner-a", slug="shared")
    other = _create(repository, tx, owner="owner-b", slug="shared")
    assert other.head.slug == "shared"


def test_bounded_history_paginates_with_revision_cursors(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx)
    page = created.head.page_id
    for revision in range(2, 6):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=revision - 1,
            title=f"Revision {revision}",
            ciphertext=f"ciphertext-{revision}".encode(),
            request_id=uid4(),
        )

    first = repository.list_revisions(tx, owner_id="owner-a", page_id=page, limit=2)
    assert [record.revision for record in first.revisions] == [1, 2]
    assert first.next_after == 2

    second = repository.list_revisions(
        tx, owner_id="owner-a", page_id=page, limit=2, after=first.next_after
    )
    assert [record.revision for record in second.revisions] == [3, 4]
    assert second.next_after == 4

    tail = repository.list_revisions(tx, owner_id="owner-a", page_id=page, after=4)
    assert [record.revision for record in tail.revisions] == [5]
    assert tail.next_after is None

    assert (
        repository.get_revision(tx, owner_id="owner-a", page_id=page, revision=3)
    ).title == "Revision 3"
    with pytest.raises(RepositoryNotFoundError):
        repository.get_revision(tx, owner_id="owner-a", page_id=page, revision=6)


def test_page_listing_paginates_and_hides_deleted_by_default(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    first = _create(repository, tx, slug="a-page")
    _create(repository, tx, slug="b-page")
    _create(repository, tx, slug="c-page")
    repository.delete_page(
        tx,
        owner_id="owner-a",
        page_id=first.head.page_id,
        expected_head=1,
        reason="superseded",
        request_id=uid4(),
    )

    visible = repository.list_pages(tx, owner_id="owner-a")
    assert [page.slug for page in visible.pages] == ["b-page", "c-page"]

    everything = repository.list_pages(tx, owner_id="owner-a", include_deleted=True)
    assert [page.slug for page in everything.pages] == ["a-page", "b-page", "c-page"]

    window = repository.list_pages(tx, owner_id="owner-a", limit=1)
    assert [page.slug for page in window.pages] == ["b-page"]
    assert window.next_after == "b-page"
    rest = repository.list_pages(tx, owner_id="owner-a", limit=1, after=window.next_after)
    assert [page.slug for page in rest.pages] == ["c-page"]
    assert rest.next_after is None

    assert (
        repository.get_page_by_slug(tx, owner_id="owner-a", slug="b-page")
    ).slug == "b-page"
    with pytest.raises(RepositoryNotFoundError):
        repository.get_page_by_slug(tx, owner_id="owner-a", slug="a-page")
    assert (
        repository.get_page_by_slug(
            tx, owner_id="owner-a", slug="a-page", include_deleted=True
        )
    ).deleted is True
    with pytest.raises(RepositoryNotFoundError):
        repository.get_page_by_slug(tx, owner_id="owner-a", slug="missing")


def test_verify_page_chain_reports_consistency(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
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

    report = repository.verify_page_chain(tx, owner_id="owner-a", page_id=page)
    assert report.head_revision == 2
    assert report.stored_revisions == 2
    assert report.contiguous is True
    assert report.head_matches is True
    assert report.consistent is True

    with pytest.raises(RepositoryNotFoundError):
        repository.verify_page_chain(tx, owner_id="owner-a", page_id=uid4())


def test_verify_page_chain_detects_a_missing_revision(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
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
    del tx.revisions[("owner-a", page, 1)]

    report = repository.verify_page_chain(tx, owner_id="owner-a", page_id=page)
    assert report.consistent is False
    assert report.contiguous is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", ""),
        ("owner_id", "   "),
        ("owner_id", None),
        ("owner_id", "x" * 513),
        ("page_id", "not-a-uuid"),
        ("page_id", "00000000-0000-0000-0000-000000000000"),
        ("slug", "Uppercase"),
        ("slug", "-leading-dash"),
        ("slug", "x" * 65),
        ("slug", ""),
        ("title", ""),
        ("title", "   "),
        ("title", "t" * 257),
        ("ciphertext", ""),
        ("ciphertext", b""),
        ("ciphertext", None),
        ("request_id", "not-a-uuid"),
    ],
)
def test_create_page_rejects_invalid_envelopes(
    repository: AtlasRepository,
    tx: FakeAtlasTransaction,
    field: str,
    value: object,
) -> None:
    envelope: dict[str, Any] = {
        "owner_id": "owner-a",
        "page_id": uid4(),
        "slug": "atlas-page",
        "title": "Atlas page",
        "ciphertext": b"ciphertext-1",
        "request_id": uid4(),
    }
    envelope[field] = value
    with pytest.raises(RepositoryValidationError):
        repository.create_page(tx, **envelope)  # type: ignore[arg-type]


def test_create_page_rejects_oversized_ciphertext(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    with pytest.raises(RepositoryValidationError):
        _create(repository, tx, body=b"x" * (1048576 + 1))


def test_create_page_accepts_bytearray_ciphertext(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    result = _create(repository, tx, body=bytearray(b"ciphertext-1"))
    assert result.revision.ciphertext == b"ciphertext-1"


def test_fenced_writes_require_a_positive_head(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx)
    page = created.head.page_id
    with pytest.raises(RepositoryValidationError):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=0,
            title="Zero",
            ciphertext=b"z",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryValidationError):
        repository.delete_page(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=0,
            reason="withdrawn",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryValidationError):
        repository.delete_page(
            tx,
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            reason="archived",
            request_id=uid4(),
        )
    for bad in (True, "1", 1.0, -1, 9007199254740992):
        with pytest.raises(RepositoryValidationError):
            repository.append_revision(
                tx,
                owner_id="owner-a",
                page_id=page,
                expected_head=bad,  # type: ignore[arg-type]
                title="Bad",
                ciphertext=b"z",
                request_id=uid4(),
            )


def test_reads_reject_invalid_bounds(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    created = _create(repository, tx)
    page = created.head.page_id
    with pytest.raises(RepositoryValidationError):
        repository.get_revision(tx, owner_id="owner-a", page_id=page, revision=0)
    with pytest.raises(RepositoryValidationError):
        repository.list_revisions(tx, owner_id="owner-a", page_id=page, limit=0)
    with pytest.raises(RepositoryValidationError):
        repository.list_revisions(tx, owner_id="owner-a", page_id=page, limit=101)
    with pytest.raises(RepositoryValidationError):
        repository.list_revisions(tx, owner_id="owner-a", page_id=page, limit=True)  # type: ignore[arg-type]
    with pytest.raises(RepositoryValidationError):
        repository.list_pages(tx, owner_id="owner-a", after="Uppercase")
    with pytest.raises(RepositoryValidationError):
        repository.get_page(tx, owner_id="owner-a", page_id=page, include_deleted="yes")  # type: ignore[arg-type]


class _LockContentionError(Exception):
    pgcode = "55P03"

    def __init__(self) -> None:
        super().__init__(
            'could not obtain lock on relation "atlas_page"'
        )


def test_contended_page_fence_fails_fast_as_conflict(
    repository: AtlasRepository,
) -> None:
    from tests.repositories._support import ScriptedTransaction

    page = uid4()
    scripted = ScriptedTransaction(
        one=[{"acquired": True}, None, _LockContentionError()]
    )
    with pytest.raises(RepositoryConflictError, match="concurrent writer"):
        repository.append_revision(
            scripted,  # type: ignore[arg-type]
            owner_id="owner-a",
            page_id=page,
            expected_head=1,
            title="Raced",
            ciphertext=b"r",
            request_id=uid4(),
        )
    assert "FOR UPDATE NOWAIT" in scripted.fetch_sql()


def test_concurrent_owner_writer_fails_fast_as_conflict(
    repository: AtlasRepository,
) -> None:
    from tests.repositories._support import ScriptedTransaction

    scripted = ScriptedTransaction(one=[{"acquired": False}])
    with pytest.raises(RepositoryConflictError, match="concurrent writer"):
        repository.append_revision(
            scripted,  # type: ignore[arg-type]
            owner_id="owner-a",
            page_id=uid4(),
            expected_head=1,
            title="Raced",
            ciphertext=b"r",
            request_id=uid4(),
        )


def test_append_to_a_missing_page_is_not_found(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    with pytest.raises(RepositoryNotFoundError):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=uid4(),
            expected_head=1,
            title="Ghost",
            ciphertext=b"g",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryNotFoundError):
        repository.delete_page(
            tx,
            owner_id="owner-a",
            page_id=uid4(),
            expected_head=1,
            reason="withdrawn",
            request_id=uid4(),
        )
    with pytest.raises(RepositoryNotFoundError):
        repository.get_page(tx, owner_id="owner-a", page_id=uid4())


def test_private_bodies_stay_opaque_behind_digests(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    first = _create(repository, tx, body=b"\x00\x01\x02binary")
    assert first.revision.content_digest == hashlib.sha256(b"\x00\x01\x02binary").hexdigest()
    assert first.revision.ciphertext == b"\x00\x01\x02binary"


def test_persisted_page_corruption_fails_closed(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    from tests.repositories._support import ScriptedTransaction

    base = {
        "owner_id": "owner-a",
        "page_id": uid4(),
        "slug": "atlas-page",
        "head_revision": 1,
        "deleted": False,
        "deleted_reason": None,
        "created_at": 5,
        "updated_at": 5,
    }

    def page_with(**overrides: object) -> dict[str, Any]:
        row = dict(base)
        row.update(overrides)
        return row

    for corrupt in (
        page_with(deleted="no"),
        page_with(deleted_reason="archived"),
        page_with(head_revision=0),
        page_with(head_revision="1"),
        page_with(created_at=-1),
        {"owner_id": "owner-a"},
    ):
        scripted = ScriptedTransaction(one=[corrupt])
        with pytest.raises(RepositoryDataError):
            repository.get_page(
                scripted, owner_id="owner-a", page_id=base["page_id"]  # type: ignore[arg-type]
            )


def test_persisted_revision_corruption_fails_closed(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    from tests.repositories._support import ScriptedTransaction

    page = uid4()
    base = {
        "owner_id": "owner-a",
        "page_id": page,
        "revision": 1,
        "title": "Atlas page",
        "ciphertext": b"ciphertext-1",
        "content_digest": _digest(b"ciphertext-1"),
        "predecessor_digest": None,
        "created_at": 5,
        "deleted": False,
        "request_id": uid4(),
    }

    def revision_with(**overrides: object) -> dict[str, Any]:
        row = dict(base)
        row.update(overrides)
        return row

    for corrupt in (
        revision_with(revision=0),
        revision_with(content_digest="not-a-digest"),
        revision_with(predecessor_digest="not-a-digest"),
        revision_with(predecessor_digest=_digest(b"x")),
        revision_with(deleted="no"),
        revision_with(ciphertext="ciphertext-1"),
        revision_with(ciphertext=b"tampered"),
        revision_with(deleted=True, ciphertext=b"leftover"),
        revision_with(created_at=-1),
        {"owner_id": "owner-a"},
    ):
        scripted = ScriptedTransaction(one=[corrupt])
        with pytest.raises(RepositoryDataError):
            repository.get_revision(
                scripted, owner_id="owner-a", page_id=page, revision=1
            )


def test_persisted_chain_corruption_fails_closed(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    from tests.repositories._support import ScriptedTransaction

    page = uid4()
    scripted = ScriptedTransaction(one=[{"head_revision": "two"}])
    with pytest.raises(RepositoryDataError):
        repository.verify_page_chain(scripted, owner_id="owner-a", page_id=page)
    scripted = ScriptedTransaction(
        one=[{"head_revision": 2}], all_rows=[({"revision": "two"},)]
    )
    with pytest.raises(RepositoryDataError):
        repository.verify_page_chain(scripted, owner_id="owner-a", page_id=page)


def test_database_clock_corruption_fails_closed(
    repository: AtlasRepository, tx: FakeAtlasTransaction
) -> None:
    from tests.repositories._support import ScriptedTransaction

    page = uid4()
    scripted = ScriptedTransaction(
        one=[
            {"acquired": True},
            None,
            None,
            None,
            None,
            {"count": 0},
            {"now_ms": -1},
        ]
    )
    with pytest.raises(RepositoryDataError):
        repository.create_page(
            scripted,
            owner_id="owner-a",
            page_id=page,
            slug="atlas-page",
            title="Atlas page",
            ciphertext=b"ciphertext-1",
            request_id=uid4(),
        )


def test_catalog_factory_exposes_the_atlas_repository() -> None:
    from astralplane import create_atlas_repository, create_repository_catalog

    catalog = create_repository_catalog()
    assert isinstance(catalog.atlas, AtlasRepository)
    assert catalog.as_mapping()["atlas"] is catalog.atlas
    assert isinstance(create_atlas_repository(), AtlasRepository)


def test_atlas_public_contract_behaviors() -> None:
    """Zero-arg scope/replay/concurrency/failure proof for the contract matrix."""

    repository = AtlasRepository()
    # Scope: a foreign owner observes nothing and mutates nothing.
    tx = FakeAtlasTransaction()
    created = _create(repository, tx, owner="owner-a")
    page = created.head.page_id
    with pytest.raises(RepositoryNotFoundError):
        repository.get_page(tx, owner_id="owner-b", page_id=page)
    with pytest.raises(RepositoryNotFoundError):
        repository.get_revision(tx, owner_id="owner-b", page_id=page, revision=1)
    # Replay: an exact envelope replay returns the stored result without a write.
    tx = FakeAtlasTransaction()
    request = uid4()
    first = _create(repository, tx, owner="owner-a", request=request)
    writes = len([call for call in tx.calls if call.startswith("INSERT")])
    second = _create(
        repository,
        tx,
        owner="owner-a",
        page=first.head.page_id,
        request=request,
    )
    assert second.replayed is True
    assert second.head.head_revision == first.head.head_revision == 1
    assert second.revision == first.revision
    assert len([call for call in tx.calls if call.startswith("INSERT")]) == writes
    # Concurrency: a stale expected head is a typed conflict, not a lost update.
    tx = FakeAtlasTransaction()
    created = _create(repository, tx, owner="owner-a")
    repository.append_revision(
        tx,
        owner_id="owner-a",
        page_id=created.head.page_id,
        expected_head=1,
        title="Second",
        ciphertext=b"ciphertext-2",
        request_id=uid4(),
    )
    with pytest.raises(RepositoryConflictError):
        repository.append_revision(
            tx,
            owner_id="owner-a",
            page_id=created.head.page_id,
            expected_head=1,
            title="Stale",
            ciphertext=b"ciphertext-stale",
            request_id=uid4(),
        )
    # Failure: persisted corruption fails closed with a typed data error.
    from tests.repositories._support import ScriptedTransaction

    scripted = ScriptedTransaction(
        one=[{"owner_id": "owner-a", "page_id": page, "head_revision": "two"}]
    )
    with pytest.raises(RepositoryDataError):
        repository.get_page(scripted, owner_id="owner-a", page_id=page)
