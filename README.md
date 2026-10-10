# AstralPlane

AstralPlane is Astral's independent embedded durable-state library. It owns PostgreSQL connection
and transaction mechanics, guarded schema evolution, owner-scoped repositories, audit-chain
storage, transactional outbox delivery, and explicit blob-purge recovery. AstralDeep installs the
package in-process; AstralPlane does not add a service port or a second database.

## Contracts

- Python: 3.11 or newer
- Package: `astralplane`
- Contract: `astralplane.contract/v1`
- Current schema: `089.004`, read-compatible from `066.001`, with guarded upgrade entry points
  at `066.001`, `067.001`, `074.001`, `074.002`, `074.003`, `074.004`, `075.001`, `079.001`,
  `088.001`, `088.002`, `088.003`, `088.004`, `088.005`, `088.006`, `088.007`, `088.008`,
  `089.001`, `089.002`, and `089.003`
- Migration advisory lock: `(1095980114, 60001)`
- Reconciliation advisory lock: `(1095980114, 60002)`

`create_postgres_runtime(...)` owns psycopg2 driver-pool construction, bounded checkout, runtime
composition, and guarded startup. On a truly empty application schema it first installs the
schema-only `066.001` compatibility baseline under the migration advisory lock, then applies every
required edge of
`066.001 -> 067.001 -> 074.001 -> 074.002 -> 074.003 -> 074.004 -> 075.001 -> 079.001 -> 088.001 -> 088.002 -> 088.003 -> 088.004 -> 088.005 -> 088.006 -> 088.007 -> 088.008 -> 089.001 -> 089.002 -> 089.003 -> 089.004`
in one registry transaction. A pre-split `066.001` database has only its legacy revision marker.
Every later predecessor, from `067.001` through `089.003`, is accepted only when it carries its own
pinned historical migration-registry digest. Every supported predecessor is structurally attested
before the first migration write. A current `089.004` database must carry the exact current
registry digest and pass canonical catalog-structure verification over all Plane-owned tables,
sequences, functions, indexes, constraints, triggers, rules, policies, inheritance, and
owned-schema privileges. A same-name or unexpected object with changed behavior is rejected. A
non-empty partial or unrecognized schema is rejected rather than labeled current. The extracted
baseline contains only neutral schema and deterministic database mechanics; catalog cleanup, UI
seed content, filesystem discovery, and product policy remain explicit host reconciliation.

A fresh `TEMPLATE template0` database's exact PostgreSQL default `public` schema
(`pg_database_owner`, PUBLIC `USAGE`) is a qualified predecessor variant. Revision `074.004`
atomically transfers the selected schema to the migration user and revokes PUBLIC schema
privileges; any other predecessor owner or ACL shape remains a fail-closed mismatch.

Revision `079.001` adds persistent assignments with durable owner controls, source deduplication,
bounded task graphs, resource reservations, execution permits and immutable approval/effect
records. See [persistent assignment contracts](docs/persistent-assignment-contracts.md),
[result publication contracts](docs/result-publication-contracts.md) and
[migration and recovery](docs/migration-and-recovery.md).

The package contains no AstralDeep, AstralProjection, AstralPrimitives, LETS, API, UI, agent,
media, or transport implementation dependency. Product policy and authorization remain in
AstralDeep; callers pass neutral owner context and retain transaction ownership.

`create_repository_catalog()` returns the stable repository catalog. Its 43 members, in
`RepositoryCatalog.as_mapping()` order, are `assignments`, `agent_management`, `agents`,
`artifacts`, `attachment_parsers`, `audit`, `audit_retention`, `authority`, `background_tasks`,
`chat_steps`, `conversation_files`, `credentials`, `draft_agents`,
`generated_agent_publications`, `encrypted_llm_config`, `encrypted_typesafe_credential`,
`framework_credentials`, `history`, `harness_cleanup`, `identity`, `knowledge`, `maintenance`,
`mesh_enrollment`, `offline_grants`, `outbox`, `preferences`, `personalization_graph`, `purge`,
`quality_audit`, `remote`, `remote_operation_proposals`, `revocations`, `saved_components`,
`scheduler`, `share_grants`, `stop_epochs`, `tool_policy_state`, `tracked_jobs`, `tutorials`, `voice`,
`work_admission`, `workspaces`, and `completion_wake`.

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

The catalog also includes `mesh_enrollment` for neutral personal-mesh membership, enrollment, and
revocation state: owner-scoped mesh/member/public-identity/challenge/invitation records with
monotonic membership and revocation epochs, atomic single-use invitation
expiry/consumption/confirmation, and current-revision fences. Private device keys never reach
Plane; possession proof, IAM, QR/network handling, and admission policy remain in Deep. See
[mesh enrollment records](docs/mesh-enrollment-records.md).

Schema `089.003` adds durable owner stop epochs, bounded immutable peer receipts and
operation epoch bindings through `stop_epochs`. Admission and publication locks remain held
through the host's audit transaction. See [owner stop epochs](docs/owner-stop-epochs.md).

Schema `089.004` adds completion wake automation: owner-scoped `completion_subscription` rows
with live-state/terminal-coverage guards plus immutable `wake_receipt` rows. Accept and revoke
are fenced to owner/live rows with terminal coverage enforced, so revocation races fail closed.
See [migration and recovery](docs/migration-and-recovery.md#089004-completion-subscriptions-and-wake-receipts).

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

Repository writes using the shared canonical JSON encoder require string keys in every mapping,
including mappings nested inside lists and tuples. Non-string keys raise `RepositoryValidationError` before SQL rather
than being converted to strings and colliding with existing string keys. Callers that supplied
non-string keys must update their inputs. Valid canonical JSON bytes remain unchanged; this
validation does not rewrite existing stored payloads, digests, or schema metadata.

Revision `075.001` adds the immutable `voice_session.speech_backend` discriminator. Historical
sessions backfill to `llm_factory`; new `client_local` rows carry no remote room, participant,
worker, or media-grant metadata. Voice-turn persistence remains unchanged, and Plane adds no audio,
transcript, local-engine, proof, or client-capability storage.

The revisions after `079.001`:

- `088.001` adds one-shot operation admission for persistent assignments, with an execution-profile
  discriminator and original-key operation receipts; see
  [persistent assignment contracts](docs/persistent-assignment-contracts.md).
- `088.002` adds a database-issued `web_session` incarnation identity, and `088.003` adds nullable
  issuing issuer/client metadata to sessions and an issuing issuer to deferred revocations; see
  `docs/identity-agent-state.md`.
- `088.004` stores declarative agent definitions as immutable revisions with metadata-only
  receipts.
- `088.005` adds owner guidance storage; see
  [owner guidance contracts](docs/owner-guidance-contracts.md).
- `088.006` adds immutable selected-input envelopes; see
  [selected input contracts](docs/selected-input-contracts.md).
- `088.007` adds optional scheduled-job policy and occurrence-assignment bindings; see
  `docs/knowledge-scheduler-and-async-contracts.md`.
- `088.008` adds hash-only framework credentials and finite offline-grant allowances; see
  `docs/credentials-and-grants.md`.
- `089.001` adds owner-keyed TypeSafe credential ciphertext and third-party data-sharing
  acknowledgments.

Upgrade and recovery procedures for every revision are in
[migration and recovery](docs/migration-and-recovery.md).

## Local verification

AstralPlane owns qualification of its Python source, architecture boundary, PostgreSQL migration
and repository behavior, and standalone package compatibility. Pull requests and `main` pushes run
the repository-owned `.github/workflows/ci.yml` jobs `quality`, `postgresql`, and
`package-compatibility`; the `gates` aggregate fails closed unless every owner job succeeds. The
PostgreSQL lane runs the complete Python 3.11 suite against PostgreSQL 17 with a measured-baseline
combined branch-coverage floor of 88.75% and a changed-line coverage threshold of 90%.
`scripts/check_changed_coverage.py` decides changed-line coverage from diff-cover's JSON report for
the committed range `BASE_SHA..HEAD`, where `BASE_SHA` is the pull request's base commit or, on a
push to `main`, the commit before the push. It fails below 90%; a change with no measurable
executable lines is recorded in the step summary as not applicable, naming the base and candidate
SHAs and every changed path considered. A missing, malformed, or all-zero `BASE_SHA`, a base equal
to the candidate, or a report that does not describe that range fails closed. Package
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
  --cov=astralplane --cov=scripts.check_changed_coverage \
  --cov=scripts.import_staging_fixture --cov=scripts.migrate_qualification_database \
  --cov-branch --cov-report=xml --cov-fail-under=88.75
BASE_SHA="$(git merge-base origin/main HEAD)"
uv run --frozen --group ci diff-cover coverage.xml --compare-branch "$BASE_SHA" \
  --diff-range-notation '..' --ignore-staged --ignore-unstaged \
  --format json:changed-coverage.json
uv run --frozen --group ci python scripts/check_changed_coverage.py \
  --report changed-coverage.json --base-sha "$BASE_SHA" --fail-under 90
uv lock --check
uv build --build-constraints tooling/python-ci/build-requirements.lock.txt --require-hashes
actionlint .github/workflows/ci.yml
```

PostgreSQL integration checks use an isolated test database and the synthetic non-PHI fixture under
`tests/fixtures/pre_split`. Set `ASTRALPLANE_TEST_POSTGRES_DSN` to that isolated database, whose
role must be able to create and drop databases because migrated templates are cloned per test, and
run both `tests/integration/test_pre_split_upgrade.py` and
`tests/integration/test_empty_database_startup.py`. An unset URL reports the checks as skipped, not
passed. Runtime databases, blobs, uploads, logs, credentials, generated content, and local
environments must never be committed or placed beneath the package/submodule tree.

## Audit delivery retries

`AuditOutboxDelivery` retries a failed sink with exponential delays, saturating at
`max_retry_delay` before multiplying a `timedelta`. The exponent remains capped at 30;
defaults remain eight attempts, a five-second base delay, and a one-hour maximum delay.
Microsecond precision is preserved, including when the cap is not an exact multiple of
the base delay.

If the capped delay would place the next retry after UTC `datetime.max`, delivery raises
`PlaneError` with code `audit_retry_out_of_range` and the entry ID. It does not change the
claimed outbox row or report a successful settlement; the existing lease can be reclaimed
after expiry. A retry exactly at `datetime.max` is representable and remains valid.
An exhausted attempt dead-letters without calculating another retry time. Retry and
dead-letter outcomes are reported only after their fenced state transition commits.

See `docs/migration-and-recovery.md` before changing schema or durable roots.
