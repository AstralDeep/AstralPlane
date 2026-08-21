# Work-admission and qualification-audit contracts

The `074.002` data-plane composition exposes both surfaces through the ordinary in-process
repository catalog. AstralPlane owns durable PostgreSQL mechanics; AstralDeep retains admission
policy, qualification policy, authorization, redaction, and application transaction composition.

## Work admission

`create_work_admission_repository()` returns the same `WorkAdmissionRepository` available at
`catalog.work_admission`. Every durable operation accepts a caller-owned Plane `Transaction`.
There is no connection, cursor, commit, rollback, or generic SQL escape hatch.

The repository owns persisted class configuration, hierarchical finite slots, immutable
submission replay, owner-partitioned operation lookup, queue/claim/reselection leases, phase/chat
and request-generation binding, terminalization, expiry recovery, and bounded retention. Full
operation reads that intentionally cross owners are explicitly named
`get_operation_for_administration`; ordinary status reads require an `OperationOwner`.

Configuration publication is two-phase because Plane does not own the caller's commit:

1. write or read a detached snapshot with `configure(transaction, configs)` or
   `load_existing_configs(transaction)`;
2. exit the transaction successfully; and
3. call `bind_configs(configs)` to publish that committed graph to the repository instance.

Every public boundary validates record types, UUID objects, timezone-aware timestamps, positive
retention and lease durations, bounded snake-case codes, safe summaries, terminal-state payloads,
and purge limits before issuing SQL. `bind_request_generation` is NULL-only and execution-fenced;
an exact replay returns the existing record, while a changed generation or stale fence is visible.

## Qualification audit

Revision `074.002` owns `test_runs`, `test_case_results`, `test_evidence`, `audit_entries`, and
`latex_artifacts`. `QualityAuditRepository` returns immutable typed records and keeps ordinary
reads/writes owner-partitioned. The review operation locks both the target case and the current
owner audit head, validates the expected verification status, appends the review entry, and changes
case status in one caller-owned transaction.

Legacy audit entries retain `hash_version=1` and their historical digest. New review entries use
the version-2 canonical full-record digest, which authenticates owner, run/case, action, reviewer,
rationale, timestamp, previous link, and entry identity. Migration and replay never silently
rewrite an existing history. A current revision marker and registry digest are accepted only when
the current-schema verifier also matches canonical PostgreSQL catalog structure.

## Verification and recovery

The focused unit/schema matrix currently passes 136 tests. The serial isolated-PostgreSQL
fresh/upgrade matrix passes 12 tests, including exact-repeat upgrade, injected transactional
failure and recovery, same-name structural tampering, concurrent qualification reviews, real
work-admission replay/owner denial/claim/terminalization, and caller-owned rollback.

The current migration registry digest is
`1bb948074ec378d2a74e2b74eff29e72a6f9a6be03d3ae24ec6439fcf70f1e02`. Digest-only evidence is
recorded in `provenance/checks.json`. Recovery is forward-only under closed admission as described
in `migration-and-recovery.md`; never down-migrate or rewrite audit hashes by inference.
