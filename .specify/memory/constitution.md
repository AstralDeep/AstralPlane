# AstralPlane Constitution

## Core Principles

### I. Neutral Durable-State Boundary

AstralPlane is the embedded durable-state library of the Astral platform. Its public facade
(`astralplane` and `astralplane.api`), guarded migration registry, repositories, PostgreSQL
pool, recovery logic, and configured blob stores own all durable mechanics.

- Plane MUST remain an embedded library: it MUST NOT add a service port or a second database.
  PostgreSQL is its only database.
- Product policy, authentication, authorization, PHI policy, confirmation, egress, tool
  execution, encryption, token handling, rendering, and audit decisions MUST remain with the
  host (AstralDeep). Callers pass neutral owner context and keep transaction ownership.
- Plane MUST NOT import AstralDeep, AstralPrimitives, AstralProjection, or LETS modules, nor
  network or UI libraries. `tests/architecture/test_dependency_direction.py` enforces this
  one-way direction and MUST stay green.
- Plane code is Python compatible with Python 3.11, the production runtime of AstralDeep.

**Rationale**: A neutral library with one-way dependencies lets the host evolve policy while
the durable mechanics stay reproducible and independently testable.

### II. Caller-Owned Transactions and Connection Custody

- Repositories MUST run inside caller-supplied transactions and MUST NOT commit or roll back;
  `tests/contract/test_repository_contract_matrix.py` enforces this.
- Results MUST be detached, immutable records. No cursor or connection may escape its
  declared scope.
- The pool MUST stay bounded, roll back every connection before reuse, and refuse to close
  while connections are borrowed.
- Statements MUST use the driver's native parameter placeholders; values are never
  interpolated into SQL text.

### III. Owner Isolation and Fenced Writes

- Every repository write MUST carry its owner predicate and its version or idempotency fence
  in the same database statement.
- An operation that crosses owners MUST be explicit and named `..._for_administration`.
- Ordinary reads and writes MUST scope to the calling owner in the SQL predicate itself, never
  by filtering results afterwards.

### IV. One Guarded Migration Registry

Schema evolution happens only in the registry in `src/astralplane/database/migrations.py`.
Ad-hoc SQL against deployed databases and any second migration mechanism are prohibited.

- Every schema change MUST add a registry edge and bump `SCHEMA_REVISION`
  (`src/astralplane/database/revision.py`). Historical edges and their pinned registry
  digests are immutable.
- The runner MUST apply pending edges under the migration advisory lock in one transaction,
  only from the exact current digest or a pinned predecessor digest, after attesting the
  predecessor's catalog, and MUST write the new revision and digest together.
- Every boot MUST verify the full current catalog. An unknown or partial schema fails closed;
  an empty database is initialized from the schema-only `066.001` baseline under the same
  lock, then migrated.
- Edges from `079.001` on MUST NOT use `IF NOT EXISTS`: repeat safety comes from the guard and
  a verified no-op, so a same-named foreign object is refused rather than adopted.
- Migrations run automatically when the host initializes the runtime; routine schema
  evolution MUST NOT require manual database intervention. Admission stays closed until
  migration and reconciliation both finish.

**Rationale**: Schema drift is the most common cause of outages after a deploy. One registry
with exact digests keeps every environment reproducible and makes a mismatch fail closed.

### V. Forward Repair and Joint Recovery

- Recovery MUST NOT run inferred down-SQL or rewrite revision markers; forward repair is the
  default.
- Every revision MUST document its recovery procedure in `docs/migration-and-recovery.md`.
  Rollback means a joint PostgreSQL-and-blob restore with admission closed, followed by
  explicit retirement of restored sessions.
- Purges MUST be tombstoned so that a failed purge stays visible and retryable.
- A database transaction and a filesystem rename MUST NOT be treated or described as atomic
  together.

### VI. Tamper-Evident, Opaque Sensitive Data

- Audit events are append-only. Only retention pruning may purge them, under
  `SET LOCAL audit.allow_purge`; each owner's hash chain is appended under an advisory lock
  with a caller-supplied authenticator.
- Plane MUST store only ciphertext and digests for credentials and grants. It never receives
  plaintext credentials, raw share tokens, access tokens, or encryption keys, and it carries
  no cryptography dependency.
- Errors MUST be typed `PlaneError` codes with bounded, non-sensitive metadata.
- Plane holds no PHI, and test fixtures MUST be synthetic and non-PHI.
- Secrets MUST NOT be committed.

### VII. Explicit, Link-Safe Durable Roots

- Durable roots MUST be explicit, absolute operator configuration outside source, package, and
  submodule trees; the factory never selects a root.
- Blob access MUST reject path traversal and symlink or reparse-point crossings, create files
  with mode 0600 and directories with mode 0700, bound key length and depth, and publish or
  purge only through capability objects that callers cannot construct.

### VIII. Exact Compatibility Contract

Plane's public compatibility contract is its contract version (`astralplane.contract/v1`),
schema revision, read-compatible floor, migration registry digest, blob layout version, and
recovery contract version.

- A change to any contract element MUST be deliberate, versioned, and stated in its pull
  request so the consumer can re-pin. The package version in `pyproject.toml` and in
  `src/astralplane/compatibility.py` MUST agree.
- An older binary is never declared compatible merely because its rows remain present;
  compatibility is exactly what the contract states.
- Runtime dependencies MUST stay minimal (currently `psycopg2-binary` only). Because the host
  installs Plane without its dependencies, a pull request that adds a runtime dependency MUST
  say so, so the consumer declares it in its own manifest when it adopts the revision.

### IX. Representative Verification on Real PostgreSQL

- The full suite MUST run against real PostgreSQL (CI pins the server image by digest). A
  suite skipped for lack of a test database is not qualification evidence.
- Every migration edge MUST ship with tests against representative existing data: the
  synthetic `066.001` pre-split fixture and populated upgrade suites that cover repeat,
  damage, rollback, and joint restore where they apply. A migration that passes only on an
  empty database is not sufficient evidence.
- Concurrency properties MUST be proved with the fewest cycles that exercise them (for
  example the barrier-started two-starter migration race); soak loops are prohibited.
- Tests MUST cover golden paths, edge cases, denials, and failures, and shipped code MUST NOT
  contain work-in-progress, stubbed, mocked, hard-coded, or debug-only paths.

### X. Provenance and Evidence-Pinned Bytes

- `provenance/transformations.json` MUST be refreshed in the same change whenever a file it
  binds changes; `tests/test_provenance.py` checks its digests against current bytes.
- Files whose exact bytes are pinned by recorded evidence or fixtures (the legacy baseline,
  the pre-split fixture builder and loader, and the pinned registry digests) change only
  together with a regeneration of that evidence.
- An evidence record that no longer matches current bytes is historical and MUST be labeled
  as such, never cited as current.
- `.gitattributes` keeps attested inputs byte-exact with LF line endings and MUST be kept.

### XI. Fair, Bounded, Deterministic CI

- `.github/workflows/ci.yml` MUST run on every pull request and every push to `main` with
  these jobs: `quality` (lock check, locked sync, ruff, dependency direction), `postgresql`
  (the full suite on real PostgreSQL, the branch-coverage floor, and changed-line coverage),
  `package-compatibility` (Python 3.11 and 3.14, hash-locked build, dependency-free wheel
  install, and import smoke), and a `gates` aggregate that fails unless every job succeeds.
- CI MUST keep the whole-repository branch-coverage floor (currently 88.75%) and at least 90%
  coverage of changed lines. A change with no measurable executable lines makes changed-line
  coverage not applicable, and that outcome MUST be recorded explicitly.
- Every job MUST finish within 30 minutes and declare `timeout-minutes` of at most 30. A suite
  over budget gets cheaper fixtures or loses its slowest tests; the limit is never raised.
- Required gates MUST NOT depend on live third-party network services, exact clock-derived
  values, or wall-clock performance bounds. Per-test retries are permitted; whole-suite
  reruns are not. A gate MUST fail only for a defect the change introduced or can fix.
- Workflows MUST use actions pinned to full commit SHAs, least-privilege permissions, and no
  `continue-on-error`; `tests/architecture/test_ci_workflow.py` pins these properties.

### XII. Self-Documenting Source and Truthful Documentation

- Every source and test file MUST begin with a header of at most three sentences stating what
  it does and how it connects to other files; Python uses a module docstring.
- No other comments or docstrings are permitted, except a single-line comment where one is
  absolutely necessary to explain a non-obvious *why*. Function and class docstrings,
  narrating comments, commented-out code, TODO/FIXME notes, spec, task, and requirement IDs,
  feature numbers, and change history MUST NOT appear in source. Tool directives are not
  comments and are preserved verbatim.
- Evidence-pinned files (Principle X) keep their bytes until their evidence is regenerated.
- Documentation MUST state the live schema revision and catalog, and any claim about Plane's
  behavior MUST match the code as merged.
- Any dependency MAY be added when declared in `pyproject.toml` and `uv.lock`; CI tooling
  lives in the locked `ci` dependency group.
- Files generated and managed by Spec Kit (`.specify/`, `.agents/`, `.claude/`) are upstream
  tooling, exempt from this principle, and change only through Spec Kit.

## Consumers and Cross-Repository Changes

- AstralDeep is Plane's consumer. It pins this repository at `components/AstralPlane` and pins
  the contract version, schema revision, read-compatible floor, migration digest, and blob
  layout in its composition manifest, verifies them for exact equality at boot, and reruns
  this repository's suite when the pin moves.
- This repository never edits AstralDeep. A schema or contract change lands and is qualified
  here first; AstralDeep adopts it by moving its pin under its own constitution.

## Community Triage Controller

- The separate `pr-triage.yml` metadata controller MAY use only `issues: write`
  and `pull-requests: write` with the built-in short-lived token to request missing
  issue context and apply explicit maintainer closure decisions. It MUST run only
  on exact `refs/heads/main` through a reviewed, full-SHA-pinned community action,
  serialize events and recovery, check out no repository code, execute no PR
  input, download no artifacts, and use no secrets, OIDC, contents-write,
  approval, rerun, merge, publishing, or release authority. Closure MUST verify
  the deciding maintainer's immutable identity and current write permission,
  concrete public rationale, and exact reviewed head; changed heads require
  fresh review. Missing links only request context. No-op, unsupported completion,
  duplicate, and superseded findings MUST be reviewed against useful independent
  work before closure. Task issues, branches, and points remain unchanged.
  Contract tests MUST preserve these boundaries; this controller neither qualifies
  product changes nor replaces required review or release gates.

## Development Workflow

- Changes land through pull requests qualified by `ci.yml` unless the owner explicitly
  authorizes a direct push; a directly pushed change runs the same gates on `main`.
- A pull request that changes the schema MUST include the registry edge, the
  `SCHEMA_REVISION` bump, its recovery section, and representative-data tests, together with
  evidence that the migration ran against representative data.
- Reviewers MUST verify constitution compliance, including headers and comments
  (Principle XII), the provenance ledger (Principle X), and documentation currency.
- Spec, task, and feature IDs belong in pull request descriptions, never in source or tests.

## Governance

- This constitution is the highest-authority engineering policy for AstralPlane and
  supersedes `README.md`, `SECURITY.md`, `docs/`, and other guidance where they conflict. It
  replaces the AstralDeep constitution as this repository's governing document; AstralDeep's
  constitution governs only how AstralDeep consumes Plane.
- Amendments land by pull request unless the owner explicitly authorizes a direct push. Each
  amendment is approved by the owner or a lead developer and records its rationale, version
  change, and Sync Impact Report in the pull request or commit message.
- Versioning follows semantic versioning: MAJOR for principle removals or redefinitions,
  MINOR for new principles or materially expanded guidance, and PATCH for clarifications.
- Every pull request and review MUST verify compliance. Violations are resolved before merge,
  and known shortfalls are tracked as follow-up work until closed.
- References to numbered constitution principles in records written before 2026-09-28 refer
  to the AstralDeep constitution v5.0.0.

**Version**: 1.1.0 | **Ratified**: 2026-09-28 | **Last Amended**: 2026-10-05
