# AstralPlane migration and recovery

AstralPlane changes source ownership without moving live data during the first cutover. The
PostgreSQL database and the configured attachment, artifact, workspace, and generated-knowledge
roots remain one recovery unit. Package or submodule directories are never durable-data roots.

## Before upgrade

1. Close admission and quiesce all writers.
2. Record the exact AstralDeep composition, AstralPlane commit, contract version, schema revision,
   migration digest, blob-layout version, and configured durable roots.
3. Create a transactionally consistent PostgreSQL backup and a non-following copy or snapshot of
   every configured blob/workspace root from the same maintenance window.
4. Verify backup readability, file counts, byte counts, and SHA-256 values against durable records.
5. Confirm the target Plane revision declares the observed schema readable. Do not infer a
   destructive downgrade or skip an unknown revision.

## Upgrade

The application boot initializer obtains advisory lock `(1095980114, 60001)`, runs the exact
repeat-safe migration registry, and commits its revision plus migration-set digest atomically.
Required product reconciliation then runs separately under `(1095980114, 60002)` with durable
versioned hook markers. Traffic remains closed until both phases report durable completion.

The initial Plane-owned additive revision is `067.001`, readable from pre-split `066.001`. It adds
transactional outbox, audit-retention anchor, purge-tombstone, and reconciliation-marker storage.
It also verifies the legacy audit-chain topology, backfills an explicit per-owner sequence without
changing existing schema-v1 HMAC bytes, and installs a same-lock transition trigger so legacy
schema-v1 and Plane schema-v2 writers cannot allocate the same sequence. It does not relocate an
existing table or blob.

An empty database is not an approved source for this delta. Fresh installations must first create
the canonical AstralDeep `066.001` baseline; the Plane runner fails closed rather than attaching a
`067.001` marker to a database that lacks the legacy durable tables. A current marker without its
exact migration-set digest is likewise rejected instead of being blessed after the fact.

## Acceptance

Before reopening admission, verify every representative owner-scoped conversation, workspace,
artifact, preference, scheduled operation, voice-session record, remote-metadata record, and audit
chain. Re-run the migration to prove it is already current, verify every required reconciliation
marker, and check database/blob referential integrity. A partial physical purge remains visibly
incomplete and retryable.

## Failure and rollback

- A failed migration transaction rolls back and leaves admission closed.
- A failed reconciliation writes a durable failure marker and leaves admission closed; repair the
  named hook and use the explicit retry procedure.
- Prefer forward repair after a schema advance. An older composition may be reselected only when
  its recorded Plane compatibility explicitly reads the observed schema.
- Never run inferred down-SQL. Restore the verified PostgreSQL and blob snapshots together only
  under the operator recovery procedure.
- Keep the prior compatible composition and verified backup until the acceptance and rollback
  retention window closes.

## Later blob-root relocation

A later relocation is a separate operation: quiesce writers, reject symlink/reparse traversal,
copy to a new explicit root, verify count/size/SHA-256 against durable metadata, switch the root
atomically, and retain the prior copy for rollback. Source checkout or submodule updates must never
delete, overwrite, or discover runtime data implicitly.
