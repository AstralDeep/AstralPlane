# SQLite local WIP qualification

This checkpoint is **unqualified**. It is local implementation and diagnostic evidence, not a
completed port, release, protected CI result, or authorization to change Deep's composition pin.
Base: `529120e`. Python: 3.11.15. SQLite backend package: `0.1.12`; schema: `089.022`.

Current SQLite migration digest:
`e4ae392ed2951f1446cf065185a6b348bbed49c3e8cf37ca81546874887f6ef5`.
Current SQLite structure digest:
`5f45079d23dc20e1dc0bb1367e8076c55a582ba7df7a76224fc0cdbe1a2b4d0b`.
PostgreSQL's schema revision and migration registry remain unchanged. Deep's pin remains unchanged.

## Observed evidence

- PostgreSQL repository suite: 3,800 passed in 150.79s. Its targeted contract, caller-rollback,
  error and session-recovery group: 281 passed in 8.99s. Both preceded the latest refinements;
  the exact checkpoint still requires final PostgreSQL regression qualification.
- SQLite shared repository, authority, contract, caller-rollback, lifecycle, driver/helper and
  recovery run: 4,324 passed, two PostgreSQL process-catalog failures, 163 skipped in 132.52s.
  The two process-catalog tests are now explicitly marked; the resulting current run is pending.
- SQLite authority suite: 229 passed, three physical PostgreSQL skips in 7.67s. Its real SQL
  harness runs against SQLite, including owner isolation, lifecycle/effects, receipts and rollback.
- Six remaining logical concurrency failures were repaired and rerun: 172 passed, 12 physical
  PostgreSQL skips in 6.25s. SQLite test callers retry genuine contention only after rollback and
  a peer transaction finishes; runtime code does not automatically replay callbacks.
- Strict SQLite values, native writer try-lock, URL/configuration, migration drift, public lock
  taxonomy and helper group: 194 passed in 5.84s.
- Earlier broad SQLite core source coverage: 1,277/1,372 statements, 93.08%. This is diagnostic
  statement coverage, not the final committed-range changed-line coverage gate.
- Ruff and `git diff --check` passed before this checkpoint's documentation was added.

The first complete SQLite `pytest tests` diagnostic observed 5,474 passed, 159 failed, 173 setup
errors, 174 skipped and 33 passing subtests in 232.27s. Its exact test identifiers are retained in
[sqlite-diagnostic-inventory.txt](sqlite-diagnostic-inventory.txt). This inventory predates follow-up
repairs and must not be represented as the current remaining failures. It includes direct
PostgreSQL lineage, catalog, endpoint/failover and restore fixtures, plus macOS baseline failures.

## Remaining qualification and adoption boundaries

1. Run the current complete shared repository/authority/contract suite after the final SQL type
   refinements, including actual streaming purge and top-level project tests. The complete
   diagnostic found a purge `EXISTS` result requiring native boolean conversion; its focused fix
   and regression qualification are in progress.
2. Separate PostgreSQL historical migration and server/timeline fixtures from backend-independent
   integration behavior explicitly. The SQLite registry intentionally starts empty at `089.022`.
   PostgreSQL endpoint-readiness, failover fences and `qualify_joint_restore`/`recover_joint_restore`
   remain PostgreSQL-only contracts. SQLite's explicit restored-session retirement is implemented.
3. Finish the exact source PostgreSQL regression and changed-line coverage gate. A complete
   PostgreSQL diagnostic is running with a 30-minute cap; its historical migration/catalog section
   is slow on this host. An incomplete or over-budget run is not a passing gate.
4. The macOS host reproduces these failures unchanged at base `529120e`: immutable bundle publish
   lacks the native no-replace rename used by Linux, and two parser descriptor-capability tests
   fail `samefile` for `/dev/fd`. Linux qualification and a future macOS filesystem repair remain
   required; this checkpoint neither skips nor weakens those tests.
5. Deep must adopt the public `is_lock_contention` taxonomy with its existing pre-callback retry
   bounds, then independently qualify SQLite composition. A typed busy failure must never become
   an automatic replay of external effects or approval from an old snapshot.

See [sqlite-backend.md](sqlite-backend.md) for composition, native locking differences and recovery
configuration. No shared AstralDeep service/database was changed, and nothing was pushed.
