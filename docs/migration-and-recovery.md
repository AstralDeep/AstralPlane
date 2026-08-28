# AstralPlane migration and recovery

AstralPlane changes source ownership without moving live data during the first cutover. The
PostgreSQL database and the configured attachment, artifact, workspace, and generated-knowledge
roots remain one recovery unit. Package or submodule directories are never durable-data roots.
Attachment composition must bind `create_streaming_blob_store(root=...)`, the materialization
coordinator, and the durable purge executor to the same recorded absolute attachment root. The
factory never selects a root.
With `create_root=True` it may securely create only the final missing directory after validating
the complete existing ancestry; operators can require fail-closed pre-provisioning with
`create_root=False`.

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

`create_postgres_runtime(...)` owns psycopg2 pool construction and applies one additional guarded
fresh-install step. If structural inspection proves that the current application schema is empty,
Plane creates the schema-only `066.001` compatibility baseline and its legacy revision marker in
one transaction under the same migration advisory lock. It then runs the canonical registry in a
second transaction. A crash between those transactions leaves an ordinary recoverable `066.001`
predecessor; the next startup resumes at the registry edge. Existing databases never replay the
baseline.

PostgreSQL `TEMPLATE template0` databases legitimately start with schema `public` owned by the
special `pg_database_owner` role and PUBLIC `USAGE`. Predecessor attestation accepts only that exact
schema name, owner, and default ACL as a bounded fresh-database variant; any arbitrary owner or ACL
change still fails before the first edge. Revision `074.004` then transfers the selected schema to
the migration user and revokes every PUBLIC schema privilege in the migration transaction, so the
single current-schema digest has the same owner/ACL posture for default `public` and private
application schemas.

The canonical current path is
`066.001 -> 067.001 -> 074.001 -> 074.002 -> 074.003 -> 074.004 -> 075.001`; every edge required
for one run commits in the same transaction. Before the first write, the runner compares the exact source
revision's complete normalized catalog with its pinned predecessor allowlist. Every edge then runs
its own postcondition. This prevents a later `IF NOT EXISTS` statement from repairing or concealing
a damaged predecessor. The `067.001` edge adds transactional outbox, audit-retention
anchor, purge-tombstone, and reconciliation-marker storage. It also verifies the legacy audit-chain
topology, backfills an explicit per-owner sequence without changing existing schema-v1 HMAC bytes,
and installs a same-lock transition trigger so legacy schema-v1 and Plane schema-v2 writers cannot
allocate the same sequence. The `074.001` edge adds neutral LETS authority binding, lifecycle,
protected-effect, receipt-claim, sequence-watermark, and authority-outbox constraints. Neither edge
relocates an existing table or blob.

The `074.002` edge adds owner-partitioned qualification-audit runs, case results, evidence,
versioned audit entries, and LaTeX artifacts. Existing legacy audit-entry hashes remain version 1;
new atomic review transitions use the version-2 canonical full-record hash. Re-running the edge is
repeat-safe and never silently rewrites an existing audit history.

The `074.003` edge preserves preexisting runtime-contract-v2 host-session history under an explicit
legacy marker while requiring all newly created host sessions to use the exact current
runtime-contract version 3. Versions outside that bounded compatibility set remain rejected.

The `074.004` edge adds the hidden pending/READY attachment-materialization lifecycle, DB-clock
leases, exact owner/attachment/filename storage identity, typed attachment-prefix and owner-namespace
purge scopes, a retired-owner admission fence, and case-fold uniqueness for owner and attachment
identities. It backfills canonical legacy live rows as READY, creates deterministic prefix purge
work for every predecessor soft-deleted attachment, and moves unfenced legacy exact-key tombstones
to manual review. It drops the lifecycle default after backfill so a stale baseline-shaped insert
fails closed. Partial lifecycle namesakes, noncanonical legacy READY locators, case aliases, or
incompatible owner-state objects fail before any mutation or revision stamp.

The `075.001` edge requires the exact `074.004` predecessor revision, historical registry digest,
and attested catalog. Under closed admission it adds nullable `voice_session.speech_backend`,
backfills every predecessor session to `llm_factory`, then makes the discriminator non-null. Only
after verifying the exact predecessor transport, identity, revision, media-grant, and worker-grant
constraints does it replace them. Remote-only columns become conditionally nullable: an
`llm_factory` row retains a `livekit|watch_pcm_websocket` transport and complete bounded remote
media/grant shape, while a `client_local` row uses only the `client_local` transport and carries no
room, participant, worker, media-grant, or worker-grant metadata. Backend and transport remain
immutable repository identities. Voice turns are unchanged and remain bound to their parent
session; no audio, transcript, digest, proof, local-engine, or capability field is added.

### `075.001` deployment profile

1. Quiesce voice admission and drain or end every active session; keep all other writers closed
   under the normal maintenance procedure.
2. Record and verify the exact `074.004` revision/digest and create the paired PostgreSQL and
   durable-root backup described above.
3. Apply the pinned Plane candidate and verify `075.001`, its exact migration digest, the current
   structural verifier, every predecessor session backfilled as `llm_factory`, and preserved voice
   turns.
4. Start the candidate with the product selector still set to `llm_factory`; run the ordinary
   authenticated remote LiveKit/watch smoke before reopening that profile.
5. Qualify `client_local` separately in a candidate environment: create a local-profile session,
   confirm every remote-only database field is null, and exercise ordinary authenticated dispatch
   while remote speech media endpoints are blocked. Do not infer local execution from the database
   discriminator alone.
6. If validation fails before traffic reopens, prefer a guarded forward repair. Otherwise keep
   writers quiescent and restore the verified pre-upgrade PostgreSQL and durable-root snapshots as
   one unit with the prior application. Never run inferred down-SQL or rewrite `client_local` rows
   into a predecessor shape.

An authority provisioning row uses deterministic `pending:<field>:<32hex>` remote identities and
zero lease metadata until a compare-and-set activation supplies the issued LETS identities. That
pending shape is valid only while `provisioning` or after an unissued attempt is durably `closed`;
all other states forbid the reserved prefix and require positive lease expiry. Closing an unissued
attempt preserves its lifecycle evidence while releasing the nonterminal uniqueness slot.

An empty database is an approved source only for Plane's guarded baseline initializer. Structural
inspection accepts either no application tables or a metadata-table-only shell with no metadata;
any other non-empty database must carry a known revision and every required baseline table. A
partial or unknown schema fails closed without being overwritten. A `067.001` predecessor must
carry the pinned historical registry digest, and a `074.001` predecessor must carry its pinned
full-path digest, and a `074.002` predecessor must carry its pinned historical digest. A current
`074.004` predecessor must carry its pinned historical digest. A current `075.001` marker and
digest are still insufficient on their own. The verifier binds the owned
schema owner/ACL and the behavior, durability, authorization, namespace, dependency, and lifecycle
shape of all Plane-owned tables, columns, sequences, constraints, indexes, functions, triggers,
policies, rules, and inheritance edges after a transition and on every already-current startup. A
legacy `066.001` marker has no Plane digest and is accepted only through its explicitly attested
registry edge.

Database-level ACLs and connection-level database/search-path selection are host composition policy
outside Plane's owned-schema digest. The host must connect with the intended database and put the
configured Plane schema first. Plane does attest and harden the selected schema namespace itself,
including its owner and normalized ACL, and every owned relation is resolved by exact namespace OID;
it never falls through to a later search-path namesake. Owned PL/pgSQL functions use an explicit
`pg_catalog, <owned schema>, pg_temp` search path so temporary objects cannot shadow their durable
dependencies.

## Representative fixture replay

`tests/fixtures/pre_split/` contains a targeted synthetic non-PHI PostgreSQL and blob snapshot. It
is migration evidence, not a complete AstralDeep backup. `loader.py` requires an absent unique test
schema, runs the pinned extracted `066.001` historical builder exactly once, and then applies a
DML/supplement-only `baseline.sql`; it never replays repeat-safe builder DDL over a partial
predecessor. The fixture preserves representative records for history, preferences, attachments,
workspaces, scheduling, remote metadata, and voice metadata. `expected.json` binds the historical
builder source blob and SHA-256, loader SHA-256, normalized pre-migration catalog row count/digest,
and two canonical nested blob files by relative key, byte count, and SHA-256. The loader stages
blobs without following links, promotes the verified tree, and commits only after both halves are
ready.

Run the PostgreSQL replay only against an isolated test database whose role may create and drop
schemas:

```text
$env:ASTRALPLANE_TEST_POSTGRES_DSN='<isolated test database URL>'
python -m pytest -q tests/integration/test_pre_split_upgrade.py
python -m pytest -q tests/integration/test_empty_database_startup.py
python scripts/record_migration_evidence.py
```

On POSIX, use the shell's normal `export` form. The evidence recorder runs eight cases
sequentially with one worker and writes only input/output digests and bounded status metadata to
`provenance/checks.json`; it never stores the database URL or raw test output. The committed
scaffold remains `not_run` until that recorder completes successfully against a configured test
database. The integration suite creates only schemas matching
`astralplane_fixture_<32 lowercase hex characters>` and temporary blob roots supplied by pytest. It
verifies predecessor damage rejection before any repair, direct upgrade, repeat-upgrade no-op
behavior, whole-transaction rollback on an injected second-edge failure, staged blob failure,
forward retry, and the joint restore procedure below. An unset URL skips this suite and is not
qualification evidence.

## Acceptance

Before reopening admission, verify every representative owner-scoped conversation, workspace,
artifact, preference, scheduled operation, voice-session record, remote-metadata record, and audit
chain. Re-run the migration to prove it is already current, verify every required reconciliation
marker, and check database/blob referential integrity. A partial physical purge remains visibly
incomplete and retryable.

## Failure and rollback

- A failed baseline transaction rolls back to an empty schema and leaves admission closed.
- A failure after the baseline commit but before registry completion leaves a recoverable
  `066.001` predecessor; the next closed-admission startup resumes forward.
- A failed registry migration transaction rolls back and leaves admission closed.
- A failed reconciliation writes a durable failure marker and leaves admission closed; repair the
  named hook and use the explicit retry procedure.
- Expired hidden materializations are soft-deleted and scheduled for typed prefix purge before
  ordinary purge reconciliation. Global readiness stays degraded while any expired pending row,
  pending/failed tombstone, or manual-review record remains.
- Migrated legacy exact-key tombstones cannot be sent to the hardened automatic deleter. An
  authorized operator must quiesce the predecessor publisher, inspect the exact persisted owner and
  locator digest, complete the documented external storage procedure, retain its evidence artifact,
  and call `resolve_legacy_exact_for_administration(...)` with that evidence SHA-256. Only exact
  replay succeeds; a changed evidence digest conflicts.
- Prefer forward repair after a schema advance. An older composition may be reselected only when
  its recorded Plane compatibility explicitly reads the observed schema.
- Never run inferred down-SQL. Restore the verified PostgreSQL and blob snapshots together only
  under the operator recovery procedure.
- Keep the prior compatible composition and verified backup until the acceptance and rollback
  retention window closes.

### Joint restore procedure

Use this procedure only after admission and every writer are quiescent and the backup from the same
maintenance window has been verified:

1. Record the failed composition, observed schema marker/digest, and database error category without
   copying connection material or raw user content into evidence.
2. Preserve the failed database and blob roots for diagnosis when capacity and policy allow; do not
   mutate them into an assumed predecessor shape.
3. Restore the verified PostgreSQL backup into the selected recovery database and restore every
   paired blob/workspace root into new explicit destinations without following links.
4. Verify restored revision, table/record checks, blob membership, byte counts, and SHA-256 before
   selecting a composition. A restored `066.001` state has no Plane digest; restored `067.001`,
   `074.001`, `074.002`, `074.003`, and `074.004` states must have their exact declared digests and
   pinned predecessor catalog shape; `075.001` must also pass the current structural verifier.
5. Select a composition whose Plane metadata declares the restored revision readable. Prefer the
   current composition and forward-retry the full guarded registry when possible.
6. Re-run migration and required product reconciliation under closed admission, repeat the
   acceptance checks, then reopen traffic. Retain the previous and restored copies until the
   rollback window closes.

## Later blob-root relocation

A later relocation is a separate operation: quiesce writers, reject symlink/reparse traversal,
copy to a new explicit root, verify count/size/SHA-256 against durable metadata, switch the root
atomically, and retain the prior copy for rollback. Source checkout or submodule updates must never
delete, overwrite, or discover runtime data implicitly.

## Schema-neutral repository cutover

Identity, agent, draft-authoring, and tool-policy repository extraction is schema-neutral. Those
repositories operate on tables already present in the `066.001` compatibility baseline and do not
add a migration edge. Their rollback is therefore a composition rollback: restore the prior
Plane/Deep code pins without changing or deleting durable rows. See `identity-agent-state.md`.

Credential, offline-grant, and share-grant extraction is likewise schema-neutral. It reads and
writes the existing `user_credentials`, `machine_credential`, `user_offline_grant`, and
`share_grant` tables without altering their shape. Roll back only the Plane/Deep composition;
never decrypt, rewrite, or delete ciphertext or immutable snapshots as part of code rollback. See
`credentials-and-grants.md`.

Chat-step, conversation-file, and saved-component public repository extraction also reuses only
`066.001` baseline structures. The saved-component factory deliberately points to the existing
publication-aware Canvas repository rather than duplicating its SQL. Rollback is composition-only;
leave `chat_steps`, `messages.step_count`, `chat_files`, and `saved_components` untouched. See
`conversation-extended-state.md`.
