# Atlas page contracts

Schema `089.006` adds the first bounded Atlas page store behind
`create_atlas_page_repository()` and the `atlas_pages` catalog member. The repository stores
opaque owner- and project-scoped page heads, exact create/delete command receipts, and terminal
tombstones. It does not parse or encrypt content, authorize a caller, audit a command, expose a
network route, edit a page, publish revisions, or store links. Schema `089.007` adds separate
same-owner link and backlink storage described in [Atlas link contracts](atlas-link-contracts.md).

## Page identity and content

A page is identified by `(owner_id, page_id)`. `page_id` is a host-supplied canonical UUID and
remains stable for the lifetime of that page row. `project_id` is an optional exact scope; `None`
is the global project and is distinct from every non-empty project. A live slug is unique within
one owner and one project. The same slug, page identity, and content may exist independently under
another owner or project.

The live row carries a host-sealed `content` byte string of 1 through 16,384 bytes, its SHA-256
digest, up to eight unique source references, the current `revision` (initially `1`), and
caller-supplied millisecond timestamps from 0 through 9,007,199,254,740,991 (the safe 53-bit integer
maximum). All commands, detached records, receipts, and cursors enforce the same timestamp range
before SQL.

Each source reference contains 1 through 512 Unicode characters, with no edge whitespace, surrogate
code points, or control characters other than interior tab/newline. The eight references are unique.
PostgreSQL bounds the UTF-8 bytes of `sources::text` at 16,416: eight strings of 512 four-byte
characters, 16 quote bytes, 14 comma/space bytes, and two bracket bytes. JSON escaping of quotes,
backslashes, tabs, and newlines uses two bytes per character, so it cannot exceed that bound. Real
PostgreSQL tests reach the exact maximum with distinct supplementary-plane strings and exercise all
escaped forms.

Plane never receives a plaintext page body or an
encryption key and never interprets the ciphertext. Reads verify that the stored digest still
matches the stored bytes and fail with `RepositoryDataError` instead of returning damaged content.
`titles`, rendering, source validation policy, PHI checks, encryption, authorization, and audit
remain with the embedding host.

## Lifecycle and replay

`AtlasPageCreateCommand` binds the owner, canonical command UUID, page identity, project, slug,
sources, ciphertext, and creation time. Its request digest covers every immutable input. Replaying
the same owner/command with the same digest returns the original page and receipt with
`replayed=True` and performs no new write. Reusing a command UUID with changed semantics, an
existing page identity, or a live slug conflict fails with `RepositoryConflictError`.

`AtlasPageDeleteCommand` carries the owner, command UUID, page identity, `expected_revision`, and
deletion time. The update validates the owner, live state, revision fence, and monotonic deletion
time in one statement. It advances the revision by one, nulls ciphertext and its digest, clears
source metadata, and writes the tombstone time. Repeating the same delete command returns the same
tombstone without restoring content. A different command cannot delete an existing tombstone, and
a stale revision cannot delete a changed page. PostgreSQL triggers refuse any attempt to update a
tombstone, restore content, or mutate a command receipt.

A tombstone permanently occupies its page identity; recreating that page UUID is refused. A new
page UUID may reuse a slug after the prior page is deleted, while the old identity remains a
content-free tombstone. Reads and listings exclude tombstones unless `get_page(include_deleted=True)`
explicitly requests the detached tombstone record. Ordinary reads, listings, and writes put the
owner predicate in the SQL statement; there is no cross-owner administrative page surface.

## Bounded reads and pagination

`get_page()` reads one owner-scoped identity. `get_page_by_slug()` reads one live owner/project/slug
identity. `list_pages()` reads at most 100 live pages ordered by `(created_at DESC, page_id DESC)`
and returns `AtlasPageBatch`. The cursor carries the exact owner, project, creation time, and page
identity; a cursor from another owner or project is refused before SQL. The query fetches one extra
row to decide whether a continuation exists, so an exact-capacity page has no false continuation.
Keyset iteration is not a snapshot: a later insert may appear on a fresh first page, and a deleted
page disappears.

All methods accept an explicit caller-owned transaction or query executor. They never commit,
roll back, borrow a connection, retain a cursor, or mutate a returned record. The owner-scoped
advisory lock for a mutation is transaction-local, so replay and identity races serialize without
creating a second database or service.

## Migration and recovery

The `089.005 -> 089.006` edge attests the exact predecessor registry digest
`98a9082bb3317192cd6e48df3b63c76dbb9404814cb4a0180eb997f8737e37b0` and complete catalog before
creating only `atlas_page` and `atlas_page_command`. It does not rewrite any existing table or row.
A repeat run verifies the complete current catalog and is a no-op. Partial DDL or a stamp failure
rolls the edge back with its registry metadata; a same-name foreign object or damaged predecessor
fails before adoption.

Follow the closed-admission backup, guarded upgrade, populated repeat, corruption, and rollback
checks in [migration and recovery](migration-and-recovery.md). Once the edge commits, keep both
tables and the exact `089.006` revision/digest intact. Prefer a reviewed forward repair. If the
prior composition must be restored, restore the verified PostgreSQL backup and every paired durable
root together under closed admission, then retire restored sessions and reconcile uncertain
effects before reopening. Never drop the tables, rewrite revision markers, or execute ad-hoc
downgrade SQL to simulate a rollback. Restoring an older database snapshot does not restore content
that was never placed in Plane or that was erased from a tombstone.