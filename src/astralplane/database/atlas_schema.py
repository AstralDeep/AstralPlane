"""Additive schema for project Atlas pages and their immutable revisions.

Atlas pages are owner-scoped wiki documents with stable identities. Each page has a
single fenced head revision; every edit appends exactly one immutable revision row
and advances the head atomically in the caller's transaction. Page bodies stay
opaque ciphertext: Plane stores bytes and digests only, never document policy.
"""

ATLAS_SCHEMA_STATEMENTS = (
    """
CREATE TABLE atlas_page (
        owner_id TEXT NOT NULL CHECK(char_length(owner_id) BETWEEN 1 AND 256
            AND octet_length(owner_id)<=1024),
        page_id UUID NOT NULL,
        slug TEXT NOT NULL CHECK(slug ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
        head_revision BIGINT NOT NULL CHECK(head_revision BETWEEN 1 AND 9007199254740991),
        deleted BOOLEAN NOT NULL DEFAULT FALSE,
        deleted_reason TEXT CHECK(deleted_reason IN ('withdrawn','superseded')),
        created_at BIGINT NOT NULL CHECK(created_at>=0),
        updated_at BIGINT NOT NULL CHECK(updated_at>=created_at),
        PRIMARY KEY(owner_id,page_id),
        UNIQUE(page_id),
        UNIQUE(owner_id,slug),
        CHECK((NOT deleted AND deleted_reason IS NULL)
           OR (deleted AND deleted_reason IS NOT NULL))
    )
""".strip(),
    """
CREATE TABLE atlas_revision (
        owner_id TEXT NOT NULL,
        page_id UUID NOT NULL,
        revision BIGINT NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
        title TEXT NOT NULL CHECK(char_length(title) BETWEEN 1 AND 256),
        ciphertext BYTEA NOT NULL,
        content_digest TEXT NOT NULL CHECK(content_digest ~ '^[0-9a-f]{64}$'),
        predecessor_digest TEXT CHECK(predecessor_digest ~ '^[0-9a-f]{64}$'),
        created_at BIGINT NOT NULL CHECK(created_at>=0),
        deleted BOOLEAN NOT NULL,
        request_id UUID,
        request_digest TEXT CHECK(request_digest ~ '^[0-9a-f]{64}$'),
        PRIMARY KEY(owner_id,page_id,revision),
        UNIQUE(owner_id,request_id),
        FOREIGN KEY(owner_id,page_id) REFERENCES atlas_page(owner_id,page_id)
            ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
        CHECK((request_id IS NULL AND request_digest IS NULL)
           OR (request_id IS NOT NULL AND request_digest IS NOT NULL)),
        CHECK((revision=1 AND predecessor_digest IS NULL)
           OR (revision>1 AND predecessor_digest IS NOT NULL)),
        CHECK((NOT deleted AND octet_length(ciphertext) BETWEEN 1 AND 1048576)
           OR (deleted AND octet_length(ciphertext)=0))
    )
""".strip(),
    (
        "ALTER TABLE atlas_page ADD CONSTRAINT atlas_page_current_revision "
        "FOREIGN KEY(owner_id,page_id,head_revision) REFERENCES "
        "atlas_revision(owner_id,page_id,revision) DEFERRABLE INITIALLY DEFERRED"
    ),
    (
        "CREATE FUNCTION reject_atlas_revision_update() RETURNS trigger LANGUAGE plpgsql "
        "SET search_path TO pg_catalog AS $fn$ "
        "BEGIN RAISE EXCEPTION 'atlas revisions are immutable' USING ERRCODE='23514'; END "
        "$fn$"
    ),
    (
        "CREATE TRIGGER atlas_revision_immutable BEFORE UPDATE ON atlas_revision "
        "FOR EACH ROW EXECUTE FUNCTION reject_atlas_revision_update()"
    ),
    (
        "DO $astralplane_atlas_function_search_path$ "
        "BEGIN EXECUTE format("\
        "'ALTER FUNCTION %I.reject_atlas_revision_update() "\
        "SET search_path TO pg_catalog, %I, pg_temp', "\
        "current_schema(), current_schema()"\
        "); END "
        "$astralplane_atlas_function_search_path$"
    ),
)
