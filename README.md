# AstralPlane

AstralPlane is Astral's independent embedded durable-state library. It owns PostgreSQL connection
and transaction mechanics, guarded schema evolution, owner-scoped repositories, audit-chain
storage, transactional outbox delivery, and explicit blob-purge recovery. AstralDeep installs the
package in-process; AstralPlane does not add a service port or a second database.

## Contracts

- Python: 3.11 or newer
- Package: `astralplane`
- Contract: `astralplane.contract/v1`
- Current schema: `067.001`, readable from pre-split `066.001`
- Migration advisory lock: `(1095980114, 60001)`
- Reconciliation advisory lock: `(1095980114, 60002)`

The `067.001` migration accepts only a verified `066.001` predecessor. AstralPlane deliberately
rejects an empty database instead of labeling a partial schema as current; fresh installation still
uses AstralDeep's canonical baseline provisioning before the Plane migration runs.

The package contains no AstralDeep, AstralProjection, AstralPrimitives, LETS, API, UI, agent,
media, or transport implementation dependency. Product policy and authorization remain in
AstralDeep; callers pass neutral owner context and retain transaction ownership.

## Local verification

```text
uv run --offline --isolated --python 3.11 --with pytest --with pytest-cov \
  python -m pytest --cov=astralplane --cov-branch --cov-fail-under=90 -q
uv tool run --offline --python 3.11 --from ruff==0.15.21 ruff check .
python tests/architecture/test_dependency_direction.py
```

PostgreSQL integration checks use an isolated test database and the synthetic non-PHI fixture under
`tests/fixtures/pre_split`. Runtime databases, blobs, uploads, logs, credentials, generated content,
and local environments must never be committed or placed beneath the package/submodule tree.

See `docs/migration-and-recovery.md` before changing schema or durable roots.
