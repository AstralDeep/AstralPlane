# Identity, agent, draft, and tool-policy state

This repository slice exposes the durable mechanics required to remove identity/agent SQL from
AstralDeep without moving identity or authorization policy into AstralPlane. It uses tables already
present in the schema-only `066.001` compatibility baseline, so this slice itself adds no migration
edge. The current Plane schema is `074.004`; its registry digest includes later independent durable
changes.

## Public composition

`create_identity_repository()`, `create_agent_repository()`,
`create_draft_agent_repository()`, and `create_tool_policy_state_repository()` return stateless
repositories. The same instances are available as `runtime.repositories.identity`, `.agents`,
`.draft_agents`, and `.tool_policy_state`.

Every method accepts a caller-owned Plane transaction. Repository methods never borrow a
connection, commit, or run product callbacks. Deep can therefore compose ownership, lifecycle,
audit, outbox, and authority writes into one transaction.

## Isolation and concurrency

- Ordinary identity, user-agent, draft, host, runtime, request, selection, and permission methods
  require the opaque OIDC subject and include it in their SQL predicate.
- Deliberately global administrative inventories and cleanup sweeps are named explicitly; Deep must
  authorize those operations before calling them.
- User-agent, revision, runtime-instance, runtime-request, draft, and publication transitions use
  durable revision/state compare-and-set predicates. Stale writes raise a repository conflict.
- Agent IDs, revision IDs, runtime generations, request generations, draft UUID aliases,
  transition IDs, publication IDs, and claims cannot silently acquire different semantics on
  replay.
- Preference-document changes first materialize the row, then lock it with `FOR UPDATE`, so a tool
  selection or agent opt-out preserves unrelated theme and product settings.
- `AgentRepository.lock_owner()` supplies the existing owner advisory-lock identity for workflows
  that atomically touch several lifecycle tables.
- `AgentRepository.reconcile_validation_policy_for_administration(...)` serializes one product
  policy revision with a stable transaction advisory lock. An exact marker replay performs no
  writes; a changed marker atomically flags only live, mismatched, not-already-flagged agents and
  records the opaque product revision in `schema_meta`. Plane does not interpret that revision.

## Compatibility behavior

The repositories preserve all existing representations during cutover:

- `agent_ownership` remains the first-party visibility record and cannot transfer ownership through
  an upsert.
- `agent_trust` remains a storage-only marker; Deep decides who may mark or reset it.
- `agent_scopes`, legacy `tool_permissions`, legacy NULL-kind `tool_overrides`, and per-kind
  `tool_overrides` remain readable until Deep completes policy reconciliation.
- `draft_agents` retains legacy text/JSON columns while adding owner and revision fencing to every
  new write path.
- `user_agent`, `user_agent_revision`, `agent_host_session`, `agent_runtime_instance`, and
  `agent_runtime_request` retain their feature-060 foreign keys and generation constraints.

Rollback is code-only for this slice: restore the prior Plane/Deep composition while leaving the
unchanged schema and rows in place. No destructive DDL or data rewrite is required.

The administrative policy reconciliation has live PostgreSQL evidence for concurrent starters,
idempotent replay, exact affected-row reporting, owner-neutral selection, and caller-owned rollback.
