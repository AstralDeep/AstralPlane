# Atlas revisions (`089.002`)

Atlas pages are owner-scoped wiki documents with stable identities, never product
policy, rendering, authorization, or document content. The caller owns
authentication, encryption, and document policy; Plane owns the typed rows,
revision/head checks, locks, and tombstones. Page bodies stay opaque: Plane
stores caller ciphertext bytes and SHA-256 digests only and never interprets
document content.

## Transactions and heads

Every method takes an explicit caller-owned transaction and never commits or
rolls back. Writers take the owner advisory lock plus a `FOR UPDATE` page lock,
then append exactly one immutable revision row and advance the page head in the
same transaction, so a stale `expected_head` is a typed `RepositoryConflictError`
instead of a lost update. A `BEFORE UPDATE` trigger rejects any revision-row
mutation, and the page head carries a deferrable foreign key to its current
revision row so the append and the head advance commit atomically.

Retried requests carry a caller request identity: an exact replay of the same
request envelope returns the stored result without another write, while a reused
request identity with conflicting semantics is rejected with
`RepositoryConflictError`. Deleted pages keep their tombstone head and history;
no edit path can resurrect them, and a wrong owner observes only
`RepositoryNotFoundError`.

## Bounded reads and recovery

`list_revisions` pages by revision cursor with a bounded limit, `list_pages`
pages per owner and hides deleted pages unless asked, and `verify_page_chain`
reports head/revision consistency so a missing or reordered row is detectable:
revision 1 carries no predecessor digest and every later revision links the
SHA-256 of its predecessor's ciphertext. Persisted shape violations surface as
`RepositoryDataError` for caller rollback.

The `089.002` edge requires the exact `089.001` registry and catalog, adds the
`atlas_page` and `atlas_revision` tables, and alters nothing that exists. See
[migration and recovery](migration-and-recovery.md) for the guarded upgrade,
repeat-safe recovery, and joint restore procedure.
