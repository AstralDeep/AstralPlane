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
- `replace_refresh_token_if_current(transaction, owner_id, grant_id,
  expected_encrypted_refresh_token, encrypted_refresh_token, as_of)` replaces
  opaque credential state only when owner, exact prior ciphertext, unrevoked
  state and expiry predicates all match. It returns the detached new record or
  `None`; it cannot reactivate a revoked grant. This also supports a product-owned
  encrypted reference to a canonical credential without copying refresh tokens.
  The caller owns encryption, exact reference validation and the external exchange.
- `history.sessions.delete_and_return(transaction, owner_id, session_id)`
  atomically returns the encrypted session record that was actually deleted.
  Product logout uses those final credential bytes for best-effort IdP revocation;
  a prior read or process cache cannot safely supply them during rotation.
- Deletes are owner-scoped and idempotent where the legacy caller treats absence as success.

The repositories never borrow or commit a connection. Callers that combine a durable mutation
with Deep-owned audit or authority work must pass the same Plane transaction to every repository
operation.

## Exact interactive execution observation

`history.sessions.get_execution_state(query, owner_id=..., session_id=...)` reads
only the specified owner/session and samples PostgreSQL `clock_timestamp()`. It
returns `None` for absence, otherwise an ephemeral `SessionExecutionState` with
`credential` and `observed_at`; this unlocked read grants no execution authority.
It has no cache, administrative or latest-owner fallback.

`SessionCredentialFence` is version 1 and binds `owner_id`, `session_id`,
`created_at`, `interactive_anchor`, `hard_expires_at`, `refresh_generation`, and
`encrypted_state_binding`. The last field binds both already encrypted credential
values, so deleting/recreating a session with reused timestamps and different
ciphertext cannot revive an old observation. A new observation alone cannot prove
an assignment's original session incarnation: its existing authority metadata
stores only the selected session ID. The host must retain and validate the issued
incarnation before minting such a new observation. `resumed` is excluded because the
reconnect marker does not rotate credentials. Generation is a monotonic counter,
not a wall-clock expiry. `execution_fence(SessionRecord)` derives this typed fence;
no plaintext credential or key enters Plane.

The host captures the database `observed_at` before remote validation/refresh,
performs the existing external exchange without a transaction, verifies current
IAM/roles and persists rotated credentials, then supplies a version 1
`SessionExecutionObservation(credential, started_at, valid_until)`. The observation
uses the exact persisted post-refresh fence and the original pre-exchange database
time. Its aware timestamps must define a positive window no longer than 15 seconds;
future or expired observations refuse execution. It must not be persisted, logged,
sent to clients, synthesized from JWT `sid`, or treated as evidence of remote IAM
validation by itself. Deep owns issuance and the exact signed-cookie reference.

`assert_current_execution(transaction, observation=...)` locks owner retirement,
then the exact session row, and samples database time after the wait. It checks
owner activity, exact credential identity, interactive anchor, hard expiry and the
observation window. Hold the same transaction for guarded writes and recheck after
subsequent lock waits. `compare_and_set_refresh(..., expected_credential=...)`
optionally applies the same exact credential/owner/lifetime guard before advancing
the existing refresh generation. It refuses changes to immutable session identity;
the legacy signature remains available to existing callers.

All three observation/fence DTOs are exported by `astralplane.repositories.history`.
Their credential fields are excluded from routine representations. Error messages
are closed and contain neither ciphertext nor caller-supplied credential fields.
This schema-neutral primitive provides local serialization and bounded freshness;
it does not perform Keycloak calls, prove remote freshness itself, enable an ingress
or runner, or close the host's remaining current-authority integration.

## Rollback and compatibility

The slice uses columns already present in the extracted `066.001` baseline, so rollback is a code
composition rollback only. Re-pin the prior Plane/Deep composition while leaving PostgreSQL rows
untouched. Existing nullable user-credential timestamps and nullable offline-grant bookkeeping
timestamps remain readable. No ciphertext or snapshot should be copied into diagnostic evidence.
