# Externally managed endpoint failover readiness

## Status and supported topology

`astralplane.endpoint-readiness/v1` qualifies one availability topology: an externally managed
PostgreSQL primary plus every explicit, paired blob root that belongs to the same Plane
composition and recovery point. The host owns election, promotion, endpoint switching, writer
fencing, admission, backup verification, blob-inventory verification, and approval. Plane only
observes the selected endpoint and returns neutral fence evidence.

The facade is read-only. It creates no schema, durable row, service port, lease, election
authority, or second database. It does not migrate, initialize, reconcile, retire sessions,
publish blobs, or change admission. A successful result is evidence that the observed endpoint
was writable and structurally exact at inspection time; it is not a durable lease and it does not
prove that another host has stopped writing.

## Public contract

`inspect_endpoint_readiness(...)` accepts explicit `database_url`, a non-empty `blob_roots`
mapping of stable host-selected names to explicit absolute roots, a host-computed
`blob_inventory_digest`, the exact `expected_database`, `expected_schema`,
`expected_schema_revision`, and `expected_migration_digest`, the selected topology, and an
optional `expected_fence` from an earlier inspection. It returns a frozen `EndpointReadiness`
report and never writes to the database or blob roots.

A writable report contains an `EndpointFence` bound to:

- the supported topology;
- the PostgreSQL server address, port, database identity, postmaster start identity, and WAL
  timeline;
- the exact database, schema, schema owner, and schema object identity;
- the current schema revision and migration-registry digest;
- every configured blob-root name and its opaque filesystem identity;
- the host-supplied complete blob-inventory digest and the blob-layout version.

The fence's `digest` is canonical and deterministic for those observations. It contains no DSN,
credential, path, token, key, or blob content. `EndpointReadiness.require_writable()` returns the
fence or raises a data-free `EndpointReadinessError`. `EndpointReadiness.require_fence(...)`
refuses a different fence with `EndpointFenceError` code `endpoint_fence_stale`.

`expected_fence` is how a writer proves continuity across reconnect. Re-inspecting the same
writable primary after a client loss returns the same fence. Inspecting a promoted, restored,
relocated, or otherwise different endpoint with the old fence returns `stale` and never becomes
writable. After the host has independently fenced the previous writer and approved the new
endpoint, it explicitly inspects without `expected_fence` to establish a new writable fence.
Plane never infers that transition.

`blob_inventory_digest` is host evidence, not a Plane enumeration. The host must compute it from
the complete same-recovery-point inventory of every configured root, including relative key,
size, and content digest. Plane validates the SHA-256 shape and binds it into the fence, so a
renamed blob, changed inventory, relocated root, or mixed recovery point cannot retain an old
writable fence. The facade validates root paths and filesystem identities but does not prove the
inventory itself; that remains an operator prerequisite.

## Readiness states

| State | Meaning | Writable |
| --- | --- | --- |
| `writable` | The supported primary, exact current schema/catalog, all root identities, and the supplied inventory match; any supplied expected fence also matches. | Yes |
| `unavailable` | The database endpoint could not be observed or its private pool could not be cleaned up. | No |
| `not_primary` | PostgreSQL is in recovery or the transaction is read-only. | No |
| `stale` | Every current check passed, but the supplied expected fence belongs to another database, timeline, schema, root set, inventory, or composition. | No |
| `incompatible` | The requested composition or observed database/schema, metadata, digest, catalog, blob layout, or topology is not the exact writable contract. | No |
| `ambiguous` | Blob roots or inventory evidence are missing, invalid, duplicated, or cannot be qualified. | No |

A predecessor schema is read-compatible only through the ordinary compatibility contract. It is
not writable readiness. The guarded migration registry must first bring it to the exact current
revision and catalog; this facade does not run that migration.

## Host-owned boundaries

Plane cannot determine leadership, quorum, mesh membership, DNS ownership, or whether an old
personal-mesh node still has a writable connection. PostgreSQL advisory locks coordinate clients
on the same database only. A stale writer that remains connected to a still-writable old primary
must be stopped by the host; the fence gives the host a value to compare and a new endpoint a
value to require, but it is not a distributed lock or a remote kill switch.

Authentication, authorization, audit, session retirement, product policy, external LETS
authority, effect reconciliation, and final admission remain host decisions. A writable endpoint
report does not replace the joint PostgreSQL/blob restore procedure, restored-session retirement,
reconciliation, or approval. The host must retain the report and its own fencing, restore,
inventory, and approval evidence for the recovery review window.

## Recovery sequence

1. Keep admission and every writer closed. Individually fence or stop the failed node and every
   stale writer; Plane cannot prove that step.
2. Inspect the current endpoint with the previous fence. `unavailable`, `not_primary`,
   `incompatible`, `ambiguous`, or `stale` remains non-writable.
3. Restore or promote only through the host's selected availability procedure. Verify the joint
   PostgreSQL/blob recovery point and the complete host-owned blob inventory.
4. After host approval, inspect without `expected_fence` to establish the new fence. Persist that
   fence with the recovery evidence and require it from every writer.
5. Reject every older fence, retire restored sessions through the governed recovery facade, and
   complete required reconciliation before reopening admission.
6. If the endpoint remains ambiguous or incompatible, preserve it and use guarded forward repair
   or the documented joint restore. Do not adopt copied rows, a read-only replica, a renamed root,
   or a changed inventory as a writable state.

## Verification evidence

Focused cases run against real PostgreSQL:

- `tests/integration/test_endpoint_failover_readiness.py::test_primary_loss_reconnect_and_stale_writer_are_explicit`
  covers primary loss, reconnect with the same fence, stale-writer refusal on another endpoint,
  and fresh writable evidence on the promoted endpoint.
- `tests/integration/test_endpoint_failover_readiness.py::test_renamed_root_and_changed_inventory_require_a_new_fence`
  covers relocated roots and changed host inventory.
- `tests/integration/test_endpoint_failover_readiness.py::test_externally_read_only_endpoint_is_never_writable`
  covers an externally read-only endpoint.
- `tests/integration/test_endpoint_failover_readiness.py::test_damaged_or_mistargeted_state_stays_non_writable`
  covers wrong target, metadata, digest, and catalog damage without repair or relabeling.
- `tests/test_endpoint_readiness.py::test_invalid_runtime_inputs_raise_before_any_driver` and
  `tests/test_endpoint_readiness.py::test_ambiguous_blob_evidence_is_non_writable_without_a_driver`
  cover denial before I/O.
- `tests/test_endpoint_readiness.py::test_report_requires_writable_and_exact_current_fence`
  covers stale-fence refusal.
- `tests/test_blob_store.py::test_root_identity_digest_is_stable_opaque_and_tracks_relocation`
  covers opaque root identity, closed-store refusal, and relocation detection.

Run the focused evidence against an isolated PostgreSQL database:

```text
ASTRALPLANE_TEST_POSTGRES_DSN='<isolated test database URL>' \
  python -m pytest -q tests/test_endpoint_readiness.py \
  tests/integration/test_endpoint_failover_readiness.py \
  tests/test_blob_store.py::test_root_identity_digest_is_stable_opaque_and_tracks_relocation
```

A branch that changes this contract must state the new contract version and rerun these cases plus
the complete repository CI. Docker-hosted platform acceptance, real endpoint-manager dispatch, and
institutional staging remain separate host qualifications.