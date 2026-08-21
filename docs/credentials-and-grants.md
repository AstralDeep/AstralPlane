# Credentials and grants repository slice

This schema-neutral slice exposes the `066.001` credential and grant tables through three
typed, caller-transaction-owned repositories. It does not move rows or introduce a migration.

## Public composition

- `create_credential_repository()` returns opaque user-agent and remote-machine credential
  storage over `user_credentials` and `machine_credential`.
- `create_offline_grant_repository()` returns encrypted refresh-token lifecycle storage over
  `user_offline_grant`.
- `create_share_grant_repository()` returns digest-bound immutable snapshot storage over
  `share_grant`.

All three are also available as `credentials`, `offline_grants`, and `share_grants` in
`RepositoryCatalog`.

## Security and transaction boundary

Plane never receives plaintext credentials, raw share tokens, Keycloak access tokens, or an
encryption key. The embedding product encrypts or hashes before calling a repository and retains
authorization, credential-key policy, token exchange, PHI disclosure policy, rendering, and audit
event decisions. Opaque ciphertext and immutable share snapshots are marked non-representable on
detached records so routine logs do not disclose them.

Ordinary credential and offline-grant methods require `owner_id`. Creating a machine credential
also proves the machine belongs to that owner through `remote_machine`; a conflict cannot transfer
a credential between owners. The deliberately cross-owner user-credential page is named
`list_agent_credentials_for_reencryption` and is bounded and cursor-paged for an already-authorized
administrative migration worker.

Offline token bytes are returned only by `get_active_for_exchange`, which requires both owner and
grant identity and applies the live/expiry predicate. `find_latest_valid` returns token-free
metadata and preserves the legacy preference for an agent-specific grant before the owner's newest
valid fallback.

Public share resolution is intentionally capability-scoped by the caller-supplied SHA-256 digest;
unknown, revoked, and expired grants all produce `None`. `record_open` repeats the digest,
revocation, and expiry predicates in the increment itself, preventing a resolve/revoke race from
counting or auditing a stale open. Owner listing omits both digest and snapshot content.

## Replay, CAS, and revocation

- Offline grant IDs, share digests, and initial machine credentials accept only exact immutable
  replay. Reuse with changed semantics raises `RepositoryConflictError`.
- User ciphertext re-encryption and machine credential replacement use explicit persisted
  timestamp compare-and-set fences. A legacy nullable user-credential timestamp can be advanced
  through `IS NOT DISTINCT FROM NULL` once.
- Single-grant revocations return `revoked`, `already_revoked`, or `missing`; owner-wide offline
  revocation changes only live rows and returns the transition count.
- Deletes are owner-scoped and idempotent where the legacy caller treats absence as success.

The repositories never borrow or commit a connection. Callers that combine a durable mutation
with Deep-owned audit or authority work must pass the same Plane transaction to every repository
operation.

## Rollback and compatibility

The slice uses columns already present in the extracted `066.001` baseline, so rollback is a code
composition rollback only. Re-pin the prior Plane/Deep composition while leaving PostgreSQL rows
untouched. Existing nullable user-credential timestamps and nullable offline-grant bookkeeping
timestamps remain readable. No ciphertext or snapshot should be copied into diagnostic evidence.
