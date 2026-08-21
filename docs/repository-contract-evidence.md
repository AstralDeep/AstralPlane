# Public repository contract evidence

AstralPlane's stable catalog currently contains 36 repository members. The contract matrix in
`tests/contract/test_repository_contract_matrix.py` is ordered against `RepositoryCatalog.as_mapping()`
and fails whenever a public member is added, removed, or reordered without evidence.

For every member, the gate now executes concrete behavioral tests for its applicable scope,
replay/idempotency, compare-and-set or concurrency, and typed failure behavior. It does not accept
source-token matching as behavioral proof. Read-only projections and deliberately distinct-event
queues carry explicit reasons where replay or mutable CAS does not apply. Global tutorial content,
fixed test-harness cleanup, worker queues, and administrative inventories remain explicitly named
and bounded rather than being mislabeled as owner CRUD.

The same matrix executes a real repository query with an attributed sentinel through the supplied
transaction, proves the persistence failure remains visible, and proves no repository attempts to
commit or roll back the caller's transaction. The static no-commit/no-rollback scan is retained only
as an additional packaging guard.

`tests/integration/test_catalog_caller_rollback.py` supplies the corresponding live PostgreSQL
proof. It exactly classifies all 36 catalog members, performs one successful write for each of the
35 write-capable members, confirms the mutation is visible inside the caller's transaction, forces
the caller to abort, then re-reads persistence in a new transaction. Insert/update writes disappear,
and the fixed-manifest cleanup deletion is restored. The one non-write member,
`agent_management`, exposes only the bounded `get_list_context()` and `get_detail_context()` read
projections, so caller rollback is genuinely inapplicable rather than silently omitted.

Serial isolated-PostgreSQL evidence covers the 35-member rollback matrix plus locking-sensitive
seams that a scripted transaction cannot prove: audit JSON detachment, WorkAdmission
replay/fence/rollback, concurrent qualification review serialization, concurrent policy
reconciliation, active authority-binding uniqueness, and claim/outbox savepoint rollback.
Scheduler, maintenance, voice, workspace publication, and history composition retain their focused
product-level PostgreSQL suites in AstralDeep; those callers use the same public Plane repositories
and application runtime.

This evidence uses schema-neutral repositories against the current `074.004` schema. A future catalog addition must add both a
failure/attribution probe and executable behavioral evidence before the exact-catalog assertion can
pass.
