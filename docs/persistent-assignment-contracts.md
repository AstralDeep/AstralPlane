# Persistent assignment contracts (`079.001`)

`create_repository_catalog().assignments` and `create_assignment_repository()` expose
`AssignmentRepository`. Immutable public dataclasses live in
`astralplane.repositories.assignment_models` and are also imported by `assignments`.
Every operation uses the caller-owned Plane transaction; Deep supplies policy, authorization,
offline credentials, trusted resource quotes, ordinary dispatcher and work-admission gates.
No repository method performs external I/O or grants permission to a model.

## Owner commands and receipts

Create takes an owner, UUID4 assignment/submission IDs, a semantic submission SHA-256 and an
`AssignmentDefinition`. Definition replacement requires both `instruction_revision` and
`control_epoch`; every accepted pause/resume/revise/stop/revoke changes the control epoch.
Worker writes use `state_version` independently. `get_submission_receipt` accepts the exact
command names `create`, `revise`, `pause`, `resume`, `stop`, `revoke`, and `run-now`; it resolves
accepted client semantics before the caller recaptures server-owned grants. Conflicting reuse
fails. A bounded recent receipt history cannot reapply an evicted command under a stale epoch.

`request_check` coalesces owner intent within cadence and durable retry deadlines. It does not
override lifecycle, missing authorization, approval, reconciliation, or resource limits. At most
25 active/paused and 256 total retained assignments belong to an owner. Capacity never prevents
pause/stop/revocation. Terminal stopped or completed deletion requires the exact control epoch
and no unresolved effect. Account `retire_owner` returns stopped/deleted assignment IDs and any
unresolved action IDs; commit the stop before reporting pending reconciliation and defer purge.

## Claims, durable memory and bounded tasks

`claim_due_for_administration` uses PostgreSQL time and `FOR UPDATE SKIP LOCKED`. Claims carry
owner, instruction revision, control epoch, generation and an opaque token. Bind the ordinary
`AssignmentOperationBinding` before any dispatch; validate its separate work-admission fence at
each use. `renew_claim` and `assert_current_claim` never revive invalidated authority.

Source batches atomically insert stable provider/item/revision identities and advance the
checkpoint cursor under compare-and-set. Reserved checkpoint keys are `cursor`,
`source_configuration_digest`, and `last_batch_key`. Source context is bounded and untrusted.
Recent batch receipts are bounded to 32; older source identities remain in the relational ledger.
Task plans enforce depth, fanout, dependencies, child tool attenuation and shared budgets.
Completion persists the exact task generation/result/provenance; incorporation references must
match those retained bytes. An episode cannot finish with a started action or complete an event
whose direct effect is unresolved. Unstarted reservations release when the episode yields.

Revision supersedes old unfinished events and task graphs, clears current source/finding
checkpoint fields, and archives each old task's result, digest and provenance in inspectable
`task_superseded` activity before permitting a replacement graph. These archives have references
and are not pruned as transient activity. Bounded event/action/plan history refuses new work at
capacity, requiring owner retirement or revision as applicable; it never discards replay identities.

## Effects, approval and resource admission

`put_action` records an immutable request and complete intent digest under an assignment-scoped
action key. `get_action_by_key` retrieves it without an unbounded scan. `reserve_action` durably
reserves the exact finite call/token/time maximum; parallel child work shares both lifetime and
daily totals. Currency is optional. With no currency cap, spending remains explicitly unknown;
selected caps require trusted finite unexpired quotes, including an explicit zero-cost quote for
zero caps. Actual usage overrun is recorded honestly and prevents further admission.

`start_action` commits the single-use dispatch permit, with request, permission, precondition,
operation and current epoch checks. That commit is the durable action-start boundary. A later
control cannot recall the external request; it fences every later permit and stale publication.
`record_action_outcome` accepts an exact previously issued dispatch token even after stop or
lease loss, updating only the ledger/accounting; it cannot resurrect continuation authority.
`AssignmentActionRecord.ever_started` reports whether any durable attempt received a permit
without exposing dispatch tokens. A safe replacement after control invalidation requires an
invalidated action that has never started; succeeded action identities remain reusable receipts.

Sensitive intent approval binds the complete reviewed request and immutable expiry. Interactive
only tools require a fresh `claim_for_approved_action` admission, exact action binding and the
existing attended confirmation flow. `link_interactive_proposal` verifies the actual durable remote
proposal owner, tool, agent and argument fingerprint in the same transaction that creates it.
`get_action_for_interactive_proposal` resolves the inverse association; `observe_interactive_proposal`
only reflects an actual remote decline/expiry. Control and retirement expire unstarted linked
pending/approved remote proposals before removing associations. A consumed or begun approval is
never replayed under a new worker.

Owner reconciliation of an uncertain effect records `reconciled_applied` or
`reconciled_not_applied` with the evidence reference and prior result digest. It does not recover
the external response: the public result has `result_available: false` and an empty `result`.
Even when the effect ledger is classified as succeeded, a consumer requiring that response must
hold for reconciliation until usable evidence or revised owner instructions permit continuation.
Never substitute an empty result for a completed investigation or manufacture a finding.

Lease recovery returns stale operation bindings, preserves completed results, releases unstarted
reservations, conservatively charges interrupted read-only calls and holds uncertain effects.
Failed episodes use bounded exponential retry deadlines and the durable retry count. Resume,
restart and owner check do not reset lifetime spending or turn an uncertain effect into retryable
work. Refer to [migration and recovery](migration-and-recovery.md) for deployment/restore policy.
