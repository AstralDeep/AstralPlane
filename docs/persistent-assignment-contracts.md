# Persistent assignment contracts (`079.001`)

`create_repository_catalog().assignments` and `create_assignment_repository()` expose
`AssignmentRepository`. Immutable public dataclasses live in
`astralplane.repositories.assignment_models` and are also imported by `assignments`.
Every operation uses the caller-owned Plane transaction; Deep supplies policy, authorization,
offline credentials, trusted resource quotes, ordinary dispatcher and work-admission gates.
No repository method performs external I/O or grants permission to a model.

## Additive one-shot admission (`088.001`)

`create_operation` shares the assignment initializer, action ledger and execution fences.
It creates only the `one_shot` profile; `create_assignment` remains `persistent` with
its original source/tool/offline-grant requirements. Existing row JSON and initial
definition digests are preserved by the additive migration. Legacy list and due-claim
methods explicitly select the persistent profile; a registered one-shot host handler
must deliberately use `claim_operations_for_administration`.

The host supplies a typed `AssignmentOperationSpec` and `AssignmentOperationAuthority`.
The latter is only an opaque, owner-bound reference to a current session, delegation,
framework credential or offline grant. It contains no token, role claims or permission
decision. Deep authenticates the caller and revalidates current lineage, tool/PHI/egress
permissions and the separate work-admission fence before dispatch. Plane checks closed
reference kinds, owner equality, owner retirement and database time after the owner lock.
Scheduled origin additionally requires the same current owner offline-grant reference.

One-shot definitions allow no source or external tools for ordinary model work, but
research requires a nonempty source plan. They declare no recurrence, have at most three
retries and a deadline no more than one day after acceptance. Lifetime resource ceilings
remain authoritative; an omitted daily ceiling uses the lifetime ceiling. Limits and
reference/profile fields are closed, and working authority is never reconstructed from
an API body's claims. The legacy cadence-based `request_check` refuses this profile.

Admission receives the original owner/namespace/caller key and canonical command digest.
An existing matching receipt is resolved before new definition/guidance expansion;
framework replay also binds the issuing credential reference. Receipt, new assignment,
and the caller's audit/allowance changes belong in one caller-owned transaction. Separate
one-shot capacity defaults to 25 active/paused and 256 retained tasks. At most 4096
original-key receipts per owner are retained; callers may select lower ceilings.
Reaching a ceiling refuses new admission without evicting replay or unresolved effects.

Terminal task deletion nulls the receipt's live foreign key but retains its original
logical identity: the same key returns `assignment_operation_deleted` and cannot start
another effect. A completed account retirement removes the private receipts only after
unresolved effects are settled; the owner retirement fence still prevents new admission.
This storage admission contract does not itself install a host execution handler, add
new effect authority or replace the existing ordinary chat dispatcher.

### One-shot wait, wake and controller projection

`set_event_wait` is an episode completion, not an unfenced owner-ID write. It takes
the current `AssignmentFence`, observed `state_version`, checkpoint and completion
digest plus a bounded event key and strict integer source observation revision.
The source revision is a monotonic watermark, never an ordered opaque revision string.
The existing completion transaction rejects in-flight or unresolved effects, releases
unused reservations, saves the checkpoint and clears the claim. The phase becomes
`awaiting_event` with no due time; neither worker profile treats it as cadence work.

`accept_wake` takes the owner/assignment, observed state/instruction/control vector,
original event ID, event key, source revision and host-computed event-content binding.
The host still authenticates the caller and validates current authority and the source;
these strings and digests are not permission. Owner retirement locks precede the
assignment lock. First acceptance requires the current active wait, matching key and
a strictly greater watermark, then advances the existing wake generation once and
sets database-time eligibility. Wrong-owner and missing assignment both return absent.

`operation.control` version 1 stores the current wait, at most 64 source-key watermarks
and at most 128 wake receipts keyed by the original event ID (128 UTF-8 bytes each).
A receipt binds key, source revision and content binding. Exact replay acknowledges
the prior acceptance before stale version/phase/expiry checks, without another state
change; mismatched event-ID reuse conflicts. Receipts are never evicted during the
retained operation lifetime. Capacity refuses new events while retaining inspection
and cancellation. The control envelope is at most 64 KiB and the complete assignment
remains at most 256 KiB. This uses existing private JSON; no migration bytes change.

One-shot `apply_control` requires strict `expected_state_version` in addition to the
instruction/control vector. Persistent callers and their receipt signatures retain
their existing contract. Replays of foundation one-shot receipts also remain valid.
Pause/resume preserves event waits, approval/reconciliation/authority holds and an
already scheduled retry. Source-less interactive resume validates its operation
authority and references without inventing an offline grant. Revocation cannot be
undone by pausing and resuming. Cancellation preserves started/uncertain liabilities.

One-shot `finish_episode` requires explicit due time for a nonterminal queued yield;
it never reads persistent cadence. Transient failures use 5/15/45-second bounded
retries inside the original deadline/authority lifetime and configured retry ceiling.
Exhaustion becomes an explicit terminal failure only when unresolved work permits it.
Explicit terminal completion may carry `terminal_outcome` (`completed` or `failed`)
and a bounded canonical result reference. The host remains responsible for result
ownership and approved publication. Cancellation records `cancelled`. New optional
completion fields are omitted at default when computing old receipt signatures.

`get_operation` and bounded UUID-keyset `list_operations` return an
`AssignmentOperationRead` with the existing assignment revision vector, disposition,
continuation support and explicit terminal/result metadata. They filter the profile
before pagination. Unknown positive operation/control/checkpoint versions remain
readable but non-dispatchable; claim selection excludes them before applying its
limit, so they cannot starve later supported rows. A safe stop preserves unknown
nested metadata and retires the known outer fence. Invalid identities, malformed
versions or inconsistent indexed fields remain data errors, not compatibility cases.

These are repository/controller contracts. The Deep operation service, external
owner-command adapters, current-authority stream delivery and transient action-input
disposition still require their separate integration work; they are not installed
by adding these methods.

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
`recover_expired_for_administration` selects only the persistent profile;
`recover_expired_operations_for_administration` selects only one-shot work. Both filter before
the batch limit so expired work in one profile cannot block the other's recovery. Persistent
episodes retain their bounded exponential retry deadlines. One-shot recovery uses 5/15/45-second
delays, at most its original retry allowance, and never schedules beyond the original authority
expiry or task deadline. Resume,
restart and owner check do not reset lifetime spending or turn an uncertain effect into retryable
work. Refer to [migration and recovery](migration-and-recovery.md) for deployment/restore policy.
