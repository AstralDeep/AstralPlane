# Mesh membership, enrollment, and revocation records

This repository slice exposes the durable identity/challenge state required by owner-confirmed
personal-mesh enrollment. It stores only neutral records: possession proof, IAM, QR/network
handling, and admission policy stay with the host, and private device keys never reach
AstralPlane. The current Plane schema is `089.002`; the slice's tables arrive in the `089.002`
edge over six additive tables.

## Public composition

`create_mesh_enrollment_repository()` returns a stateless `MeshEnrollmentRepository`, also
available as `runtime.repositories.mesh_enrollment`. Every method takes a caller-owned Plane
transaction and never commits, borrows a connection, or runs product callbacks, so Deep can
compose ownership, audit, outbox, and authority writes into one atomic unit.

## Records

- `mesh_record`: one owner's mesh with a bounded display name, monotonic `membership_epoch` and
  `revocation_epoch` counters, and a `record_version` compare-and-set fence.
- `mesh_member`: a `device`, `agent`, or `companion` member with the membership epoch it was
  activated at, an `active`/`revoked`/`retired` status, and its own `record_version` fence.
- `mesh_public_identity`: public key material, algorithm, and a 64-hex fingerprint for one
  member. Binding requires a locked, active member row. Private keys stay at their device.
- `mesh_enrollment_challenge`: an opaque 64-hex possession-challenge digest with host-supplied
  BIGINT issue/expiry windows and a `pending`/`proven`/`expired`/`cancelled` state machine.
- `mesh_enrollment_invitation`: an opaque 64-hex invitation digest with a
  `pending`/`consumed`/`confirmed`/`expired`/`revoked` state machine and the same windows.
- `mesh_member_revocation`: one revocation per member per revocation epoch with a bounded reason.

## Isolation and concurrency

- Every ordinary read and write is scoped to the calling owner in the SQL predicate itself.
- Epoch counters are allocated with `UPDATE mesh_record ... RETURNING` inside the caller's
  transaction, so barrier-started concurrent activations and revocations receive distinct,
  monotonic epochs without a second database.
- Invitation consumption, expiry, and confirmation are single fenced statements keyed on state,
  digest, and expiry; exactly one concurrent consumer or confirmer wins and every loser receives
  a typed conflict (`mesh_invitation_expired`, `mesh_invitation_digest_mismatch`,
  `repository_conflict`) instead of silent success.
- `bootstrap_mesh` creates the mesh and its first member atomically and replays idempotently;
  concurrent bootstraps of the same mesh identity admit exactly one winner.
- Transition fences (`record_version`, state predicates) raise typed conflicts on stale writes.

## Compatibility behavior

The slice is purely additive over the `089.001` catalog: no existing table, column, or function
changes, and no edge in this slice uses `IF NOT EXISTS`. Rollback is code-only: restore the prior
Plane/Deep composition while leaving the schema and rows in place. Joint restore follows the
`089.002` recovery section in [migration and recovery](migration-and-recovery.md); epoch counters
are monotonic by design and are never rewritten.
