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
owner-command adapters and current-authority stream delivery still require their
separate integration work; they are not installed
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

For one-shot `decide_action`, `reconcile_action`, and `delete_for_owner`, pass the
observed strict integer `expected_state_version`. The persistent signatures remain
compatible. An exact prior action decision/reconciliation acknowledges its receipt
without applying a second transition; a genuinely new command must match the current
state version and instruction/control vector. Reconciliation can settle an issued
liability after stop or expiry, but cannot schedule new work under expired, revoked,
or retired authority. Once the immutable task deadline expires, a fully settled
reconciliation becomes terminal failed; any remaining liability/task reconciliation
keeps the operation held. Renewable authority loss with a live deadline remains an
authority hold. No result is incorporated or published by settlement.

One-shot and mixed-profile account cleanup must explicitly adopt
`retire_operations_for_owner`. It fences the owner and stops every owned profile in
one transaction, returning `retained_assignment_ids` as well as actual
`unresolved_action_ids`. Commit that result and defer physical account/blob purge
while **either** collection is nonempty. Outstanding usage or reconciliation tasks
can retain an assignment even without an action ID. The legacy `retire_owner`
refuses one-shot profiles and orphan holds, so older hosts that inspect only action
IDs cannot silently purge them. Deep must update its cleanup adapter when adopting
this Plane revision; the repository change alone is not that integration.

Unknown operation/control/checkpoint versions allow safe stop receipts and owner
inspection. Stop changes only understood outer fences and preserves unknown nested
bytes. Cancellation and deletion conservatively decode the current action envelope;
future/malformed actions remain unresolved even if their indexed state says they
settled. Their attempt, reservation, binding and proposal metadata is never followed
or rewritten. Physical deletion requires every action to be understood and all
issued/reserved/uncertain liabilities, outstanding usage and reconciliation tasks
to be settled. Supported neighboring assignments can still retire without waiting
for those unknown records.

## Claims, durable memory and bounded tasks

`claim_due_for_administration` uses PostgreSQL time and `FOR UPDATE SKIP LOCKED`. Claims carry
owner, instruction revision, control epoch, generation and an opaque token. Bind the ordinary
`AssignmentOperationBinding` before any dispatch; validate its separate work-admission fence at
each use. `renew_claim` and `assert_current_claim` never revive invalidated authority.

Before a host transaction performs a write requiring both execution fences, call
`assert_current_assignment_execution(transaction, fence=AssignmentFence(...),
binding=AssignmentOperationBinding(...), action_id=None, authority=None)`. This strict
public guard locks the owner retirement domain, the exact selected interactive
session when applicable, the selected owner-scoped offline grant when present,
the logical assignment, then the work-admission execution. It discovers the
authority references without an assignment row lock and refuses any changed
selection when it reloads the assignment under lock. Session validation binds
opaque encrypted credential state without decrypting it. Session deletion and
rotation contend on that session row; grant revocation contends on its grant row. After the
admission lock wait it rechecks the current assignment lease, revision/control
vector, local authority/deadline, grant expiry/revocation and admission ownership.
An approved-action claim requires its exact `action_id`; omission cannot widen it.

The returned detached `AssignmentRecord` does not renew either lease or authorize
a later transaction. Perform the subsequent bounded repository mutation in this
same transaction and still satisfy its action/state/request checks. No network,
provider callback or second pool belongs inside the transaction. The host must
refresh institutional session/delegation authority before opening it and perform
any required audit/outbox writes before commit. A missing, stale or unsupported
local context refuses the guarded write. Interactive one-shot requires an exact
`SessionExecutionObservation` in `authority`, bound to the operation's stored
session reference. A delegation reference has no supported execution observation
adapter here and is refused. Scheduled one-shot and persistent granted profiles
retain their existing checks and signatures. Framework origin is explicitly
refused until the dedicated current issuer-lineage contract is bound; its stored
credential reference alone is insufficient. These storage primitives do not enable
source-less chat, interactive ingress or a one-shot runner.

Use the named `put_action_for_execution`, `reserve_action_for_execution` and
`start_action_for_execution` wrappers for guarded action transitions. They take
the existing action arguments plus the exact `binding` and optional `authority`,
and check current authority/both fences before and after action/resource lock
waits. Each uses the caller transaction's existing savepoint: an exception rolls
back that wrapper's prepare, reservation, permit and approval-consumption writes,
even if the caller catches it and commits independent work. Failure to restore a
savepoint marks the enclosing transaction failed under the existing transaction
contract. Do not catch an authority failure and dispatch. Returned records and
permits are usable only after the enclosing transaction commits successfully;
the caller remains responsible for same-transaction required audit/outbox writes.
Raw `put_action`, `reserve_action` and `start_action` signatures remain compatible
for established callers; they do not replace these stronger execution checks.

The guard shares admission-fence validation with result settlement without changing
the latter's contract: authentic old permits can still settle usage when execution
authority has ended. Do not put that settlement-only path behind a strict current
execution guard. Existing `assert_current_claim` and persistent signatures remain
compatible. Deep must adopt this guard through its bounded transaction adapter
before enabling one-shot runner dispatch; this Plane addition does not wire or
enable those callers, and introduces no migration.

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

### Transient model input and unavailable results

`AssignmentActionIntent.transient_input=None` preserves the existing durable request
contract and canonical receipt bytes. One-shot ordinary model actions may instead
provide `AssignmentTransientInput` version 1 with `binding_key_id`, `payload_binding`,
`source_retention`, `references`, and `reconstruction_kind="model_messages"`.
`payload_binding` is a host-produced keyed attestation over the exact transient model
request; `request_digest` must equal that attestation. Plane receives neither its key
nor private messages. Never substitute an unkeyed digest of private text. The bounded
key ID is diagnostic metadata, not a key or authority token.

In this mode `request` contains only `kind="model"`, required finite
`max_output_tokens`, and optional bounded model/provider identifiers, a closed
`reasoning_effort`, or `response_format` of `{"type":"text"}` or
`{"type":"json_object"}`. Arbitrary messages, arguments, schema text and custom
fields are refused. The operation's retention policy must match the disposition;
consequential or interactive approval requests retain the existing durable reviewed
intent contract. A reconstruction reference contains only `kind` (`source`, `note`,
or `skill`), `resource_id`, and positive `revision`. Note IDs are canonical UUID4;
other IDs are bounded identifiers. References are unique by kind/ID and bounded to
32 sources, 8 notes and 20 skills, with an 8 KiB serialized ceiling. No reference
contains expanded guidance or source text.

`AssignmentActionOutcome.result_disposition` is optional for durable legacy calls.
Version 1 carries `available`, optional `reason`, `references`, and `binding_key_id`.
Unavailable results require an empty `result` and one of `retention_discarded`,
`stale_execution`, or `reconstruction_required`; available results carry neither a
reacquisition reason nor references. Transient model outcomes require a diagnostic
key ID and a host-produced keyed `result_digest`, so retained receipts cannot expose
an unkeyed hash of private output. Plane validates the envelope, not the secret-key
attestation itself. The public result projects `result_available` and, when false,
`reacquisition_reason` beside the typed disposition.
For transient outcomes, non-null `evidence_reference` is an opaque identifier of at
most 256 UTF-8 bytes, starting with an ASCII letter/digit and using only letters,
digits, `.`, `_`, `:`, `/`, `@`, or `-`. It must identify retained evidence, never
contain private excerpts or prose. Both writes and known-envelope decoding enforce
this bound; legacy durable evidence references keep their existing contract.

A successful read whose source bytes were discarded remains `succeeded` and charged.
It cannot be reserved again under the same action identity. A consumer that still
needs those bytes must resolve current owner-authorized references and perform a new
budgeted read with a new action key. The old receipt is preserved; empty content is
never a successful substitute. Deep owns retention enforcement at extraction, private
guidance expansion, attestation generation/verification, current revision resolution,
and reconstructing each new request after retry or restart. Those caller changes and
guidance storage are separate integration work.

For one-shot `record_action_outcome`, pass both `result_fence: AssignmentFence` and
`result_binding: AssignmentOperationBinding`, plus the current
`result_authority: SessionExecutionObservation` for interactive session origin.
Result usability requires the exact
issued assignment fence and admission binding, the current assignment lease/control
and instruction revisions, a current owner-scoped work-admission execution, live
local authority, and an active owner. Lock order is owner, selected session/grant,
assignment, admission, action, with database time sampled after the admission and
action lock waits. The host must refresh
external authority before this transaction and fence subsequent result incorporation
and publication. A valid old dispatch token can still settle factual usage after
either lease or owner authority is lost, even when these optional result arguments
are absent. That path discards returned payload bytes, projects `stale_execution`,
and cannot advance phase, wake generation, checkpoints or continuation. Exact replay
does not charge twice; a replay made after authority loss returns an unavailable
projection without rewriting previously accepted result bytes. Inspection of an old
receipt is not permission to incorporate it.

A missing, revoked, rotated, expired, unknown or malformed session observation
does not prevent an authentic issued permit from settling consumption. The fresh
authority check becomes false rather than raising before mandatory accounting;
success/failure charge once and uncertainty retains the reservation until explicit
settlement. This does not relax permit identity, outcome validation, settlement
replay or unresolved-liability checks. A different session ID cannot substitute for
the originally selected reference, and an old observation is refused after same-ID
ciphertext/generation replacement. The operation currently stores only that ID,
not its original credential incarnation. A newly host-issued valid observation
after same-ID replacement is therefore locally indistinguishable here. The host
must preserve and check the immutable issued-session reference before enabling
continuation; this primitive alone does not close that integration requirement.

Unknown positive input/result disposition versions remain inspectable and are refused
for execution. Safe cancellation and owner retirement retain their opaque actions and
liabilities. One-shot lease recovery holds these rows without interpreting nested
bindings, releasing reservations or rewriting action bytes, while supported neighbors
continue recovery. Resolve the hold through a qualified decoder upgrade or established
reconciliation procedure; never clear it with manual payload edits. Persistent recovery
retains its existing behavior. This addition uses the existing private JSON envelopes
and does not allocate a schema revision or change migration bytes.

Lease recovery returns stale operation bindings, preserves completed results, releases unstarted
reservations, conservatively charges interrupted read-only calls and holds uncertain effects.
`recover_expired_for_administration` selects only the persistent profile;
`recover_expired_operations_for_administration` selects only one-shot work. Both filter before
the batch limit so expired work in one profile cannot block the other's recovery. Persistent
episodes retain their bounded exponential retry deadlines. One-shot recovery uses 5/15/45-second
delays, at most its original retry allowance, and never schedules beyond the original authority
expiry or task deadline.
Supported one-shot recovery exhaustion or a deadline without unresolved liabilities
records terminal `failed`; expired authorization remains an explicit authority hold,
and uncertain effects remain in reconciliation. Resume,
restart and owner check do not reset lifetime spending or turn an uncertain effect into retryable
work. Refer to [migration and recovery](migration-and-recovery.md) for deployment/restore policy.
