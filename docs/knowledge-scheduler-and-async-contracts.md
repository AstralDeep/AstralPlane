# Knowledge, scheduler-extended, and async contracts

This schema-neutral feature-074 slice completes the Plane public contracts over tables already
present in the extracted `066.001` baseline. It adds no migration. Every repository method accepts
an explicit caller-owned transaction or query executor, and every cross-owner/system operation is
named `for_administration` so AstralDeep must make the authorization and audit decision first.

## Knowledge and personalization

`create_knowledge_repository()` returns a grouped facade with four stateless stores:

- `interactions` records an owner-bound conversation interaction or an explicitly administrative
  system interaction, lists unsynthesized rows deterministically, reloads an exact bounded ID set
  in caller order after restart, and verifies the entire ID set after marking synthesis complete;
- `quality_signals` stores bounded window aggregates, uses `(agent_id, tool_name, window_end)` as
  its replay identity, and requires an exact `computed_at` fence before replacing changed
  semantics;
- `quarantine` can hold only feedback proven to be active and owned by the supplied user, then
  requires an administrator-only detected-time lifecycle CAS to release or dismiss it; and
- `proposals` serializes proposal creation per agent/tool with a transaction advisory lock,
  supersedes an older pending proposal, and fences review/application transitions by status.

`create_personalization_graph_repository()` is independently cataloged as
`personalization_graph`. It creates both directions of a memory link only after proving both live
memory endpoints share the supplied owner, rejects partial persisted link pairs, stores
owner-isolated short-term signals under immutable replay identities, and records bounded
consolidation results. Model synthesis, review policy, artifact publishing, and selection of which
memories should be linked remain in AstralDeep.

## Scheduler-extended state

`create_scheduler_repository()` is the complete typed scheduled-job/occurrence/run/effect-ledger
boundary used by Deep's public scheduler store. It owns definition replay, next-occurrence CAS,
bounded due selection, occurrence claiming/recovery, operation binding, run/effect idempotency, and
the atomic staged-chat publication seam. Deep keeps recurrence, command, notification, and chat
policy and now injects the one application Plane runtime/catalog; the scheduler store does not hold
or reconstruct a legacy database pool.

Three focused factories complement it:

- `create_background_task_repository()` persists the legacy task projection under owner, expected
  status, and expected operation-generation predicates. Its operation projection is monotonic and
  idempotent, terminal timestamps are mandatory, notification is a one-row owner CAS, and bounded
  administrative retention methods use row locks for legacy operation-FK-null rows.
- `create_maintenance_repository()` owns unit and input membership, idempotent unit creation,
  `FOR UPDATE SKIP LOCKED` selection, exact lease-token/claim-generation/state-revision updates,
  operation-generation binding, input completion, and terminal output-generation/digest fencing.
  Expired claimed/running units are recovered under bounded row locks before selection, applying
  the persisted maximum-attempt policy. System-wide selection methods are explicitly administrative.
- `create_tracked_job_repository()` owns external scheduler-job metadata. Ordinary reads are owner
  scoped; the cross-owner open-job page is explicitly administrative. Poll writes require the
  owner, expected failure count, and exact prior poll timestamp so two pollers cannot silently
  overwrite one another. Notification is terminal-only and compare-and-set; owner-wide deletion
  exists only for an already-authorized account-retirement transaction.

`create_work_admission_repository()` owns the related operation-record and hierarchical slot
authority. It is documented separately in `work-admission-and-quality-audit-contracts.md`; the
three extended-state repositories above do not duplicate admission or scheduled-occurrence rows.

Plane does not execute coroutines, jobs, models, SSH commands, notifications, or maintenance
outputs. AstralDeep keeps those policies and supplies only validated records and state transitions.

## Bounded event-loop adapter

`AsyncPlaneRuntime` is the only async composition adapter. `run_in_transaction(callback)` admits a
bounded number of operations, then runs the complete synchronous Plane transaction and callback on
one worker thread. It deliberately does not expose `afetch_one`, `afetch_all`, `aexecute`, raw SQL,
connections, or commits. Admission has a bounded wait and raises
`async_plane_capacity_unavailable` rather than growing an unbounded executor queue.

Python cannot cancel a worker thread already inside PostgreSQL. If an awaiting coroutine is
cancelled, the adapter retains that capacity slot until the transaction finishes and consumes any
worker failure. Retryable product callbacks must therefore use the repositories' idempotency and
CAS identities. `close()` rejects new admissions but does not close the composition-owned
`PlaneRuntime`; the host still owns runtime shutdown.

## Verification and rollback

Focused contract verification covers successful writes, replay, owner mismatch, stale fences,
missing rows, corrupt persisted shapes, bounded inputs, async cancellation, and capacity refusal.
The changed repository units retain focused branch-aware coverage above the feature's 90% floor.
Serial live-PostgreSQL migration and admission conformance is recorded separately in
`provenance/checks.json` when an isolated test DSN is available.

Rollback is code-only: restore the prior Plane/Deep composition. Do not delete or rewrite
interaction, quality, quarantine, proposal, memory, scheduler, maintenance, or tracked-job rows.
