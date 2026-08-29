# AstralPlane

AstralPlane is Astral's independent embedded durable-state library. It owns PostgreSQL connection
and transaction mechanics, guarded schema evolution, owner-scoped repositories, audit-chain
storage, transactional outbox delivery, and explicit blob-purge recovery. AstralDeep installs the
package in-process; AstralPlane does not add a service port or a second database.

## Contracts

- Python: 3.11 or newer
- Package: `astralplane`
- Contract: `astralplane.contract/v1`
- Current schema: `075.001`, with guarded upgrade entry points at `066.001`, `067.001`,
  `074.001`, `074.002`, `074.003`, and `074.004`
- Migration advisory lock: `(1095980114, 60001)`
- Reconciliation advisory lock: `(1095980114, 60002)`

`create_postgres_runtime(...)` owns psycopg2 driver-pool construction, bounded checkout, runtime
composition, and guarded startup. On a truly empty application schema it first installs the
schema-only `066.001` compatibility baseline under the migration advisory lock, then applies every
required edge of
`066.001 -> 067.001 -> 074.001 -> 074.002 -> 074.003 -> 074.004 -> 075.001` in one registry
transaction. A pre-split
`066.001` database has only its legacy revision marker; a `067.001`
database is accepted only when
it carries the pinned historical `067.001` migration-registry digest. A `074.001` predecessor must
carry its pinned historical full-path digest. A `074.002` predecessor must carry its pinned
historical digest, and a `074.004` predecessor must carry its pinned historical digest. Every
supported predecessor is structurally attested before the first migration write. A current
`075.001` database must carry the exact current registry digest and pass canonical
catalog-structure verification over all Plane-owned tables, sequences, functions, indexes,
constraints, triggers, rules, policies, inheritance, and owned-schema privileges. A same-name or
unexpected object with changed behavior is rejected. A non-empty partial or unrecognized schema is
rejected rather than labeled current. The extracted baseline contains only neutral schema and
deterministic database mechanics; catalog cleanup, UI seed content, filesystem discovery, and
product policy remain explicit host reconciliation.

A fresh `TEMPLATE template0` database's exact PostgreSQL default `public` schema
(`pg_database_owner`, PUBLIC `USAGE`) is a qualified predecessor variant. Revision `074.004`
atomically transfers the selected schema to the migration user and revokes PUBLIC schema
privileges; any other predecessor owner or ACL shape remains a fail-closed mismatch.

The package contains no AstralDeep, AstralProjection, AstralPrimitives, LETS, API, UI, agent,
media, or transport implementation dependency. Product policy and authorization remain in
AstralDeep; callers pass neutral owner context and retain transaction ownership.

The stable repository catalog includes four explicit stores for the first identity/agent
cutover slice:

- `identity`: detached Keycloak/OIDC subject observations; authentication and role policy remain
  in Deep.
- `agents`: first-party ownership/trust plus user-agent revisions, host sessions, runtime
  generations, and request fences.
- `draft_agents`: owner-scoped authoring, generation leases, transition idempotency, and immutable
  publication records.
- `tool_policy_state`: explicit scope rows, legacy and per-kind tool overrides, saved selections,
  and per-user agent opt-outs; permission decisions remain in Deep.

Use the matching `create_identity_repository()`, `create_agent_repository()`,
`create_draft_agent_repository()`, and `create_tool_policy_state_repository()` factories when a
composition does not need the full catalog. See `docs/identity-agent-state.md` for transaction and
owner-isolation rules.

`agents.reconcile_validation_policy_for_administration(...)` is the atomic, advisory-locked
startup surface for a Deep-supplied opaque product-policy revision. Exact marker replay is
write-free; a changed marker flags only live mismatched agents in the same caller transaction.

The next schema-neutral catalog slice exposes ciphertext and grant mechanics already present in
the `066.001` baseline:

- `credentials`: opaque user-agent credentials plus owner-bound remote-machine credentials, with
  explicit compare-and-set replacement and a bounded administrative re-encryption page.
- `offline_grants`: encrypted refresh-token records, token-free standing-grant lookup, and
  owner-scoped idempotent revocation.
- `share_grants`: immutable snapshot capabilities stored by digest, metadata-only owner listing,
  and an active-state-checked public open counter.

Use `create_credential_repository()`, `create_offline_grant_repository()`, and
`create_share_grant_repository()` for individual composition. Encryption, raw token handling,
Keycloak exchange, PHI policy, rendering, and audit decisions remain in AstralDeep. See
`docs/credentials-and-grants.md` for the owner, replay, and transaction contracts.

Conversation-adjacent durable state has three additional stable factories:

- `create_chat_step_repository()` for owner/turn-checked progress trails and terminal-state CAS;
- `create_conversation_file_repository()` for ordered opaque file-link metadata; and
- `create_saved_component_repository()` for the same publication-aware component implementation
  already used by Plane workspaces.

They are cataloged as `chat_steps`, `conversation_files`, and `saved_components`. Step redaction and
delivery, uploads, parsing, blob I/O, and canvas policy remain product-owned. See
`docs/conversation-extended-state.md`.

Attachment parser persistence and physical blob mechanics now have explicit composition surfaces:

- `create_attachment_parser_repository()` is cataloged as `attachment_parsers`. It exposes
  redacted global coverage separately from owner-scoped claim provenance, atomically deduplicates
  pending/live gaps, reclaims only failed/discarded gaps, and fences lifecycle changes by status
  plus `updated_at`.
- `create_streaming_blob_store(root=...)` adds pathless bounded readers, a narrowly scoped
  read-only parser lease, cross-process owner exclusion, and hidden staging reservations. It
  securely provisions only the configured root's missing suffix below the nearest existing,
  link-free absolute ancestor. Direct publication and deletion are deliberately absent.
- `create_attachment_materialization_coordinator(...)` is the only production creation composite:
  it commits a pending metadata intent, opens an unpublished staging session under the exact
  owner/lease row fence, and publishes bytes plus READY metadata in one short transaction.
- `create_durable_purge_executor(...)` consumes typed attachment-prefix/owner-namespace tombstones,
  performs capability-bound physical deletion on that same store, verifies absence, and records a
  version-fenced terminal result.

Parser generation/execution and administrator authorization remain in AstralDeep. See
`docs/attachment-parser-and-blob-composition.md` for ownership, retry, purge, and recovery rules.

The remaining knowledge, personalization-graph, and scheduler-extended baseline state is exposed
through `create_knowledge_repository()`, `create_personalization_graph_repository()`,
`create_background_task_repository()`, `create_maintenance_repository()`, and
`create_tracked_job_repository()`. `create_scheduler_repository()` owns scheduled definitions,
occurrences, runs, effects, and atomic chat publication while WorkAdmission remains separate. The
full catalog keys include `knowledge`, `personalization_graph`, `background_tasks`, `maintenance`,
`scheduler`, and `tracked_jobs`. Owner-scoped reads,
immutable replay identities, status/timestamp/lease-generation compare-and-set transitions, and
explicitly named administrative surfaces prevent a caller from accidentally treating global work
as ordinary user state.

`AsyncPlaneRuntime` is a bounded event-loop adapter over whole caller-owned synchronous
transactions. It does not provide async raw-SQL helpers or connection access. See
`docs/knowledge-scheduler-and-async-contracts.md` for the exact lifecycle and cancellation rules.

The public `work_admission` catalog member owns durable operation admission, finite hierarchical
capacity, submission replay, execution leases, fenced terminalization, request-generation binding,
and bounded retention. `configure()` and `load_existing_configs()` return detached snapshots;
`bind_configs()` publishes one only after the caller-owned transaction commits. The repository
validates every public type, timestamp, duration, code, terminal payload, and limit before SQL.

Revision `074.004` retains the owner-partitioned qualification-audit and bounded host-session
compatibility introduced through `074.003`, and adds durable pending attachment materialization,
typed purge scope, retired-owner admission fencing, canonical owner/attachment case-fold isolation,
expired-upload recovery, and whole-schema catalog verification. `quality_audit` provides run, case,
evidence, audit-entry, and LaTeX-artifact
records. Review plus case-status transition is one caller-owned atomic operation with a locked
owner chain head and a versioned full-record hash; legacy v1 entries remain readable without being
silently rewritten. Tutorial content/revisions, remote-operation proposals, feedback paging and
deduplication, personalization mutation, external-identity linking, and the other extended-state
facades are likewise available only through named typed catalog members.

Revision `075.001` adds the immutable `voice_session.speech_backend` discriminator. Historical
sessions backfill to `llm_factory`; new `client_local` rows carry no remote room, participant,
worker, or media-grant metadata. Voice-turn persistence remains unchanged, and Plane adds no audio,
transcript, local-engine, proof, or client-capability storage.

## Local verification

AstralPlane owns qualification of its Python source, architecture boundary, PostgreSQL migration
and repository behavior, and standalone package compatibility. Pull requests and `main` pushes run
the repository-owned `.github/workflows/ci.yml` jobs `quality`, `postgresql`, and
`package-compatibility`; the `gates` aggregate fails closed unless every owner job succeeds. The
PostgreSQL lane runs the complete Python 3.11 suite against PostgreSQL 17 with a measured-baseline
combined branch-coverage floor of 88.75% and a changed-line coverage threshold of 90%. Package
compatibility builds and installs a clean wheel on Python 3.11 and 3.14; it does not replace the
PostgreSQL production lane.

```text
uv lock --check
uv sync --frozen --group ci
uv run --frozen --group ci ruff check .
uv run --frozen --group ci python tests/architecture/test_dependency_direction.py
ASTRALDEEP_SOURCE_REPO=/path/to/AstralDeep \
ASTRALPLANE_TEST_POSTGRES_DSN=postgresql://user:password@127.0.0.1:5432/isolated_database \
  uv run --frozen --group ci pytest -q -p no:cacheprovider \
  --cov=astralplane --cov-branch --cov-report=xml --cov-fail-under=88.75
uv run --frozen --group ci diff-cover coverage.xml --compare-branch origin/main --fail-under=90
uv lock --check
uv build --build-constraints tooling/python-ci/build-requirements.lock.txt --require-hashes
actionlint .github/workflows/ci.yml
```

PostgreSQL integration checks use an isolated test database and the synthetic non-PHI fixture under
`tests/fixtures/pre_split`. Set `ASTRALPLANE_TEST_POSTGRES_DSN` to that isolated database and run
both `tests/integration/test_pre_split_upgrade.py` and
`tests/integration/test_empty_database_startup.py`. An unset URL reports the checks as skipped, not
passed. Runtime databases, blobs, uploads, logs, credentials, generated content, and local
environments must never be committed or placed beneath the package/submodule tree.

See `docs/migration-and-recovery.md` before changing schema or durable roots.
