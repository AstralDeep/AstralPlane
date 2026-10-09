"""Owner-scoped project Atlas pages with immutable revisions and fenced page edits.

Each Atlas page has a stable identity (`owner_id`, `page_id`) and a single head
revision. Writers append exactly one immutable revision row and advance the head
in the same caller transaction under an owner advisory lock plus a `FOR UPDATE`
page lock, so a stale `expected_head` is a typed conflict instead of a lost
update. Retried requests carry a caller request identity: an exact replay of the
same request envelope returns the stored result without another write, while a
reused request identity with conflicting semantics is rejected. Deleted pages
keep their tombstone head and history; no edit path can resurrect them.

Page bodies stay opaque: Plane stores caller ciphertext bytes and SHA-256
digests only and never interprets document content. Authorization and document
policy remain with the caller; every method takes an explicit caller-owned
transaction and never commits or rolls back.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from astralplane.contracts import Transaction
from astralplane.repositories import (
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryNotFoundError,
    RepositoryValidationError,
    _bounded_limit,
    _bounded_text,
    _required_id,
    _row_value,
)

_MAX_REVISION = 9007199254740991
_MAX_PAGES_PER_OWNER = 200
_MAX_CIPHERTEXT_BYTES = 1048576
_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_DELETE_REASONS = frozenset({"withdrawn", "superseded"})

_PAGE_FIELDS = (
    "owner_id, page_id, slug, head_revision, deleted, deleted_reason, "
    "created_at, updated_at"
)
_REVISION_FIELDS = (
    "owner_id, page_id, revision, title, ciphertext, content_digest, "
    "predecessor_digest, created_at, deleted, request_id"
)


@dataclass(frozen=True, slots=True)
class AtlasPageHead:
    owner_id: str
    page_id: str
    slug: str
    head_revision: int
    deleted: bool
    deleted_reason: str | None
    created_at: int
    updated_at: int


@dataclass(frozen=True, slots=True)
class AtlasRevisionRecord:
    owner_id: str
    page_id: str
    revision: int
    title: str
    ciphertext: bytes = b""
    content_digest: str = ""
    predecessor_digest: str | None = None
    created_at: int = 0
    deleted: bool = False
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class AtlasWriteResult:
    head: AtlasPageHead
    revision: AtlasRevisionRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class AtlasRevisionHistory:
    revisions: tuple[AtlasRevisionRecord, ...]
    next_after: int | None


@dataclass(frozen=True, slots=True)
class AtlasPageListing:
    pages: tuple[AtlasPageHead, ...]
    next_after: str | None


@dataclass(frozen=True, slots=True)
class AtlasChainReport:
    owner_id: str
    page_id: str
    head_revision: int
    stored_revisions: int
    contiguous: bool
    head_matches: bool

    @property
    def consistent(self) -> bool:
        return self.contiguous and self.head_matches


def _clock(transaction: Transaction) -> int:
    observed = transaction.fetch_one(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS now_ms"
    )
    assert observed is not None
    value = _row_value(observed, "now_ms")
    if type(value) is not int or value < 0 or value > _MAX_REVISION:
        raise RepositoryDataError("database clock returned an unsupported timestamp")
    return value


def _lock_owner(transaction: Transaction, owner_id: str) -> None:
    _required_id(owner_id, "owner_id")
    transaction.fetch_one(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,79))", (owner_id,)
    )


def _page_id(value: object) -> str:
    try:
        parsed = uuid.UUID(value) if type(value) is str else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.version != 4 or str(parsed) != value:
        raise RepositoryValidationError("atlas page identity must be canonical UUID4")
    return value  # type: ignore[return-value]


def _request_id(value: object) -> str:
    try:
        parsed = uuid.UUID(value) if type(value) is str else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.version != 4 or str(parsed) != value:
        raise RepositoryValidationError("atlas request identity must be canonical UUID4")
    return value  # type: ignore[return-value]


def _slug(value: object) -> str:
    if type(value) is not str or _SLUG_PATTERN.fullmatch(value) is None:
        raise RepositoryValidationError(
            "atlas slug must match ^[a-z0-9][a-z0-9-]{0,63}$"
        )
    return value


def _title(value: object) -> str:
    return _bounded_text(value, "title", maximum=256)


def _ciphertext(value: object, *, deleted: bool = False) -> bytes:
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    if type(value) is not bytes:
        raise RepositoryValidationError("atlas ciphertext must be bytes")
    if deleted:
        if len(value) != 0:
            raise RepositoryValidationError(
                "atlas tombstone revisions carry no ciphertext"
            )
        return value
    if not 1 <= len(value) <= _MAX_CIPHERTEXT_BYTES:
        raise RepositoryValidationError(
            "atlas ciphertext must be between 1 and 1048576 bytes"
        )
    return value


def _expected_head(value: object) -> int:
    if type(value) is not int or isinstance(value, bool):
        raise RepositoryValidationError("atlas expected head must be an integer")
    if not 0 <= value <= _MAX_REVISION:
        raise RepositoryValidationError("atlas expected head outside declared bounds")
    return value


def _revision_number(value: object) -> int:
    if type(value) is not int or isinstance(value, bool):
        raise RepositoryValidationError("atlas revision must be an integer")
    if not 1 <= value <= _MAX_REVISION:
        raise RepositoryValidationError("atlas revision outside declared bounds")
    return value


def _delete_reason(value: object) -> str:
    if type(value) is not str or value not in _DELETE_REASONS:
        raise RepositoryValidationError(
            "atlas delete reason must be one of withdrawn, superseded"
        )
    return value


def _content_digest(ciphertext: bytes) -> str:
    return hashlib.sha256(ciphertext).hexdigest()


def _request_digest(envelope: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(envelope), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _head(row: Mapping[str, Any]) -> AtlasPageHead:
    try:
        deleted = _row_value(row, "deleted")
        if type(deleted) is not bool:
            raise RepositoryDataError("persisted atlas page deleted flag is unsupported")
        reason = row.get("deleted_reason")
        if reason is not None and reason not in _DELETE_REASONS:
            raise RepositoryDataError("persisted atlas delete reason is unsupported")
        head_revision = _row_value(row, "revision" if "revision" in row else "head_revision")
        if type(head_revision) is not int or not 1 <= head_revision <= _MAX_REVISION:
            raise RepositoryDataError("persisted atlas head revision is unsupported")
        for field in ("created_at", "updated_at"):
            stamp = _row_value(row, field)
            if type(stamp) is not int or stamp < 0:
                raise RepositoryDataError("persisted atlas page timestamp is unsupported")
        return AtlasPageHead(
            owner_id=str(_row_value(row, "owner_id")),
            page_id=str(_row_value(row, "page_id")),
            slug=str(_row_value(row, "slug")),
            head_revision=head_revision,
            deleted=deleted,
            deleted_reason=reason,
            created_at=_row_value(row, "created_at"),
            updated_at=_row_value(row, "updated_at"),
        )
    except KeyError as exc:
        raise RepositoryDataError(
            "persisted atlas page is missing a required field",
            metadata={"field": str(exc)},
        ) from exc


def _stored_revision(row: Mapping[str, Any]) -> AtlasRevisionRecord:
    try:
        revision = _row_value(row, "revision")
        if type(revision) is not int or not 1 <= revision <= _MAX_REVISION:
            raise RepositoryDataError("persisted atlas revision number is unsupported")
        content_digest = str(_row_value(row, "content_digest"))
        if _DIGEST_PATTERN.fullmatch(content_digest) is None:
            raise RepositoryDataError("persisted atlas content digest is unsupported")
        predecessor = row.get("predecessor_digest")
        if predecessor is not None:
            predecessor = str(predecessor)
            if _DIGEST_PATTERN.fullmatch(predecessor) is None:
                raise RepositoryDataError("persisted atlas predecessor digest is unsupported")
        if (revision == 1) != (predecessor is None):
            raise RepositoryDataError("persisted atlas revision chain link is unsupported")
        deleted = _row_value(row, "deleted")
        if type(deleted) is not bool:
            raise RepositoryDataError("persisted atlas revision deleted flag is unsupported")
        ciphertext = _row_value(row, "ciphertext")
        if isinstance(ciphertext, (bytearray, memoryview)):
            ciphertext = bytes(ciphertext)
        if type(ciphertext) is not bytes:
            raise RepositoryDataError("persisted atlas ciphertext is unsupported")
        if hashlib.sha256(ciphertext).hexdigest() != content_digest and not deleted:
            raise RepositoryDataError("persisted atlas revision digest does not match bytes")
        if deleted and len(ciphertext) != 0:
            raise RepositoryDataError("persisted atlas tombstone carries ciphertext")
        request = row.get("request_id")
        created = _row_value(row, "created_at")
        if type(created) is not int or created < 0:
            raise RepositoryDataError("persisted atlas revision timestamp is unsupported")
        return AtlasRevisionRecord(
            owner_id=str(_row_value(row, "owner_id")),
            page_id=str(_row_value(row, "page_id")),
            revision=revision,
            title=str(_row_value(row, "title")),
            ciphertext=ciphertext,
            content_digest=content_digest,
            predecessor_digest=predecessor,
            created_at=created,
            deleted=deleted,
            request_id=None if request is None else str(request),
        )
    except KeyError as exc:
        raise RepositoryDataError(
            "persisted atlas revision is missing a required field",
            metadata={"field": str(exc)},
        ) from exc


class AtlasRepository:
    """Typed public contracts for Atlas page heads and immutable revisions."""

    def get_page(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
        include_deleted: bool = False,
    ) -> AtlasPageHead:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        if type(include_deleted) is not bool:
            raise RepositoryValidationError("include_deleted must be a boolean")
        row = transaction.fetch_one(
            f"SELECT {_PAGE_FIELDS} FROM atlas_page WHERE owner_id=%s AND page_id=%s",
            (owner, identity),
        )
        if row is None:
            raise RepositoryNotFoundError("atlas page not found")
        head = _head(row)
        if head.deleted and not include_deleted:
            raise RepositoryNotFoundError("atlas page not found")
        return head

    def get_page_by_slug(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        slug: str,
        include_deleted: bool = False,
    ) -> AtlasPageHead:
        owner = _required_id(owner_id, "owner_id")
        name = _slug(slug)
        if type(include_deleted) is not bool:
            raise RepositoryValidationError("include_deleted must be a boolean")
        row = transaction.fetch_one(
            f"SELECT {_PAGE_FIELDS} FROM atlas_page WHERE owner_id=%s AND slug=%s",
            (owner, name),
        )
        if row is None:
            raise RepositoryNotFoundError("atlas page not found")
        head = _head(row)
        if head.deleted and not include_deleted:
            raise RepositoryNotFoundError("atlas page not found")
        return head

    def get_revision(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
        revision: int,
    ) -> AtlasRevisionRecord:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        number = _revision_number(revision)
        row = transaction.fetch_one(
            f"SELECT {_REVISION_FIELDS} FROM atlas_revision "
            "WHERE owner_id=%s AND page_id=%s AND revision=%s",
            (owner, identity, number),
        )
        if row is None:
            raise RepositoryNotFoundError("atlas revision not found")
        return _stored_revision(row)

    def list_revisions(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
        limit: int = 20,
        after: int | None = None,
    ) -> AtlasRevisionHistory:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        bounded = _bounded_limit(limit, maximum=100)
        if after is not None:
            _revision_number(after)
        rows = transaction.fetch_all(
            f"SELECT {_REVISION_FIELDS} FROM atlas_revision "
            "WHERE owner_id=%s AND page_id=%s AND (%s::bigint IS NULL OR revision>%s) "
            "ORDER BY revision ASC LIMIT %s",
            (owner, identity, after, after, bounded + 1),
        )
        records = tuple(_stored_revision(row) for row in rows[:bounded])
        cursor = records[-1].revision if len(rows) > bounded else None
        return AtlasRevisionHistory(revisions=records, next_after=cursor)

    def list_pages(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        limit: int = 20,
        after: str | None = None,
        include_deleted: bool = False,
    ) -> AtlasPageListing:
        owner = _required_id(owner_id, "owner_id")
        bounded = _bounded_limit(limit, maximum=100)
        if after is not None:
            _slug(after)
        if type(include_deleted) is not bool:
            raise RepositoryValidationError("include_deleted must be a boolean")
        rows = transaction.fetch_all(
            f"SELECT {_PAGE_FIELDS} FROM atlas_page "
            "WHERE owner_id=%s AND (%s::text IS NULL OR slug>%s) "
            "AND (%s OR NOT deleted) ORDER BY slug ASC LIMIT %s",
            (owner, after, after, include_deleted, bounded + 1),
        )
        pages = tuple(_head(row) for row in rows[:bounded])
        cursor = pages[-1].slug if len(rows) > bounded else None
        return AtlasPageListing(pages=pages, next_after=cursor)

    def create_page(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
        slug: str,
        title: str,
        ciphertext: bytes,
        request_id: str,
    ) -> AtlasWriteResult:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        name = _slug(slug)
        heading = _title(title)
        body = _ciphertext(ciphertext)
        request = _request_id(request_id)
        envelope = {
            "command": "create",
            "owner_id": owner,
            "page_id": identity,
            "slug": name,
            "title": heading,
            "content_digest": _content_digest(body),
        }
        fingerprint = _request_digest(envelope)
        _lock_owner(transaction, owner)
        replayed = self._replay_request(transaction, owner, request, envelope)
        if replayed is not None:
            return replayed
        existing = transaction.fetch_one(
            f"SELECT {_PAGE_FIELDS} FROM atlas_page "
            "WHERE owner_id=%s AND page_id=%s FOR UPDATE",
            (owner, identity),
        )
        if existing is not None:
            raise RepositoryConflictError("atlas page identity already exists")
        if (
            transaction.fetch_one(
                "SELECT page_id FROM atlas_page WHERE page_id=%s", (identity,)
            )
            is not None
        ):
            raise RepositoryConflictError("atlas page identity already exists")
        if (
            transaction.fetch_one(
                "SELECT page_id FROM atlas_page WHERE owner_id=%s AND slug=%s",
                (owner, name),
            )
            is not None
        ):
            raise RepositoryConflictError("atlas page slug already exists")
        count = transaction.fetch_one(
            "SELECT count(*) AS count FROM atlas_page WHERE owner_id=%s", (owner,)
        )
        assert count is not None
        if _row_value(count, "count") >= _MAX_PAGES_PER_OWNER:
            raise RepositoryConflictError("atlas page catalog is full")
        now = _clock(transaction)
        transaction.execute(
            "INSERT INTO atlas_page(owner_id,page_id,slug,head_revision,deleted,"
            "deleted_reason,created_at,updated_at) VALUES(%s,%s,%s,1,FALSE,NULL,%s,%s)",
            (owner, identity, name, now, now),
        )
        transaction.execute(
            "INSERT INTO atlas_revision(owner_id,page_id,revision,title,ciphertext,"
            "content_digest,predecessor_digest,created_at,deleted,request_id,"
            "request_digest) VALUES(%s,%s,1,%s,%s,%s,NULL,%s,FALSE,%s,%s) "
            "ON CONFLICT (owner_id,request_id) DO NOTHING",
            (owner, identity, heading, body, envelope["content_digest"], now, request, fingerprint),
        )
        return self._stored_result(transaction, owner, identity, 1, replayed=False)

    def append_revision(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
        expected_head: int,
        title: str,
        ciphertext: bytes,
        request_id: str,
    ) -> AtlasWriteResult:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        expected = _expected_head(expected_head)
        if expected < 1:
            raise RepositoryValidationError("atlas append requires a positive head fence")
        heading = _title(title)
        body = _ciphertext(ciphertext)
        request = _request_id(request_id)
        envelope = {
            "command": "append",
            "owner_id": owner,
            "page_id": identity,
            "expected_head": expected,
            "title": heading,
            "content_digest": _content_digest(body),
        }
        fingerprint = _request_digest(envelope)
        _lock_owner(transaction, owner)
        replayed = self._replay_request(transaction, owner, request, envelope)
        if replayed is not None:
            return replayed
        row = transaction.fetch_one(
            f"SELECT {_PAGE_FIELDS} FROM atlas_page "
            "WHERE owner_id=%s AND page_id=%s FOR UPDATE",
            (owner, identity),
        )
        if row is None:
            raise RepositoryNotFoundError("atlas page not found")
        head = _head(row)
        if head.deleted:
            raise RepositoryConflictError("atlas page is deleted")
        if head.head_revision != expected:
            raise RepositoryConflictError("atlas revision is stale")
        number = head.head_revision + 1
        if number > _MAX_REVISION:
            raise RepositoryConflictError("atlas page has reached its final revision")
        now = _clock(transaction)
        previous = self.get_revision(
            transaction, owner_id=owner, page_id=identity, revision=head.head_revision
        )
        transaction.execute(
            "INSERT INTO atlas_revision(owner_id,page_id,revision,title,ciphertext,"
            "content_digest,predecessor_digest,created_at,deleted,request_id,"
            "request_digest) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,FALSE,%s,%s) "
            "ON CONFLICT (owner_id,request_id) DO NOTHING",
            (
                owner,
                identity,
                number,
                heading,
                body,
                envelope["content_digest"],
                previous.content_digest,
                now,
                request,
                fingerprint,
            ),
        )
        transaction.execute(
            "UPDATE atlas_page SET head_revision=%s,updated_at=%s "
            "WHERE owner_id=%s AND page_id=%s",
            (number, now, owner, identity),
        )
        return self._stored_result(transaction, owner, identity, number, replayed=False)

    def delete_page(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
        expected_head: int,
        reason: str,
        request_id: str,
    ) -> AtlasWriteResult:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        expected = _expected_head(expected_head)
        if expected < 1:
            raise RepositoryValidationError("atlas delete requires a positive head fence")
        cause = _delete_reason(reason)
        request = _request_id(request_id)
        envelope = {
            "command": "delete",
            "owner_id": owner,
            "page_id": identity,
            "expected_head": expected,
            "reason": cause,
        }
        fingerprint = _request_digest(envelope)
        _lock_owner(transaction, owner)
        replayed = self._replay_request(transaction, owner, request, envelope)
        if replayed is not None:
            return replayed
        row = transaction.fetch_one(
            f"SELECT {_PAGE_FIELDS} FROM atlas_page "
            "WHERE owner_id=%s AND page_id=%s FOR UPDATE",
            (owner, identity),
        )
        if row is None:
            raise RepositoryNotFoundError("atlas page not found")
        head = _head(row)
        if head.deleted:
            raise RepositoryConflictError("atlas page is already deleted")
        if head.head_revision != expected:
            raise RepositoryConflictError("atlas revision is stale")
        number = head.head_revision + 1
        if number > _MAX_REVISION:
            raise RepositoryConflictError("atlas page has reached its final revision")
        now = _clock(transaction)
        previous = self.get_revision(
            transaction, owner_id=owner, page_id=identity, revision=head.head_revision
        )
        transaction.execute(
            "INSERT INTO atlas_revision(owner_id,page_id,revision,title,ciphertext,"
            "content_digest,predecessor_digest,created_at,deleted,request_id,"
            "request_digest) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,%s) "
            "ON CONFLICT (owner_id,request_id) DO NOTHING",
            (
                owner,
                identity,
                number,
                previous.title,
                b"",
                previous.content_digest,
                previous.content_digest,
                now,
                request,
                fingerprint,
            ),
        )
        transaction.execute(
            "UPDATE atlas_page SET head_revision=%s,deleted=TRUE,deleted_reason=%s,"
            "updated_at=%s WHERE owner_id=%s AND page_id=%s",
            (number, cause, now, owner, identity),
        )
        return self._stored_result(transaction, owner, identity, number, replayed=False)

    def verify_page_chain(
        self,
        transaction: Transaction,
        *,
        owner_id: str,
        page_id: str,
    ) -> AtlasChainReport:
        owner = _required_id(owner_id, "owner_id")
        identity = _page_id(page_id)
        row = transaction.fetch_one(
            "SELECT head_revision FROM atlas_page WHERE owner_id=%s AND page_id=%s",
            (owner, identity),
        )
        if row is None:
            raise RepositoryNotFoundError("atlas page not found")
        head_revision = _row_value(row, "head_revision")
        if type(head_revision) is not int or not 1 <= head_revision <= _MAX_REVISION:
            raise RepositoryDataError("persisted atlas head revision is unsupported")
        numbers = transaction.fetch_all(
            "SELECT revision FROM atlas_revision WHERE owner_id=%s AND page_id=%s "
            "ORDER BY revision ASC",
            (owner, identity),
        )
        observed = tuple(item["revision"] for item in numbers)
        if any(type(item) is not int for item in observed):
            raise RepositoryDataError("persisted atlas revision number is unsupported")
        contiguous = observed == tuple(range(1, head_revision + 1))
        matches = (
            bool(observed)
            and observed[-1] == head_revision
            and len(observed) == head_revision
        )
        return AtlasChainReport(
            owner_id=owner,
            page_id=identity,
            head_revision=head_revision,
            stored_revisions=len(observed),
            contiguous=contiguous,
            head_matches=matches,
        )

    def _replay_request(
        self,
        transaction: Transaction,
        owner: str,
        request: str,
        envelope: Mapping[str, Any],
    ) -> AtlasWriteResult | None:
        row = transaction.fetch_one(
            f"SELECT {_REVISION_FIELDS}, request_digest FROM atlas_revision "
            "WHERE owner_id=%s AND request_id=%s",
            (owner, request),
        )
        if row is None:
            return None
        if _row_value(row, "request_digest") != _request_digest(envelope):
            raise RepositoryConflictError("atlas request identity was reused")
        stored = _stored_revision(row)
        head = self.get_page(
            transaction, owner_id=owner, page_id=stored.page_id, include_deleted=True
        )
        return AtlasWriteResult(head=head, revision=stored, replayed=True)

    def _stored_result(
        self,
        transaction: Transaction,
        owner: str,
        page_id: str,
        revision: int,
        *,
        replayed: bool,
    ) -> AtlasWriteResult:
        record = self.get_revision(
            transaction, owner_id=owner, page_id=page_id, revision=revision
        )
        head = self.get_page(
            transaction, owner_id=owner, page_id=page_id, include_deleted=True
        )
        return AtlasWriteResult(head=head, revision=record, replayed=replayed)
