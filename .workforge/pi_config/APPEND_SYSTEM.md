# APPEND_SYSTEM — Always-On Enforcement Guards (highest precedence)

If any rule in AGENTS.md, skills, prompts, or chat history conflicts with this file, this file prevails. Do not reinterpret, weaken, or expand these guards.

## 1. Artifact Guard — Explicit-Request-Only

- Do not create any file solely to store or report RESULT, investigation findings, audit results, work logs, or completion reports, unless the user explicitly requests creation or update of that specific file in the current turn (regardless of extension: `.md`, `.txt`, `.json`, etc.). Return results in chat.
- Prohibited without explicit request: temporary files created solely to store findings/reports for later, survey reports, or functional documentation (`README`/spec). Use command output and chat text instead.
- Editing an existing file is allowed only when explicitly requested or directly required by the task (e.g., code/config fix); never create a new file as a side effect. If ambiguous whether to create a file, ask first.
- Not covered (permitted): implementation artifacts the task genuinely requires (source code, config files, migrations) and indispensable working pipeline intermediates.
- Authorized exception (mandatory WRITE state sync): When a task is classified WRITE under AGENTS.md, required Shared Memory synchronization under `/opt/docs` (`STATE.md`, `CHANGES.md`, `INDEX.md`, `DECISIONS.md`) is authorized as state sync, not a report artifact. This exception never authorizes RESULT files, audit reports, or survey documents.

## 2. RESULT — Chat-Only Final Status

- RESULT is not a file. It is the final status in the chat message body on task end. Never save RESULT or its contents to any file (even under another name).
- Normal end: verify actual result first, then output RESULT in chat.
- Abnormal end (abort / error / retry exhaustion): output reachable state in chat using `RESULT=ABORTED` or `RESULT=ERROR` (plus cause).

## 3. STOP — Chat-Then-Stop Only

- Fixed order: (1) verify actual result → (2) output RESULT in chat body → (3) output `STOP=YES` → (4) end there.
- Do not perform further work after STOP. Start only as a new task with new permission.
