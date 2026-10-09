/**
 * slack-notify.ts — Unified Windows + VPS Slack notify extension
 * SoT: VPS /home/deploy/.pi/agent/extensions/slack-notify.ts (synced to Windows via bootstrap)
 *
 * agent_settled (+agent_end fallback) -> Slack when idle. Webhook: env/file. PI_SLACK_NOTIFY=0 off.
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createHash } from "node:crypto";
import { readFileSync, appendFileSync, mkdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
const MAX_CHUNK = 38000;
export const OUTPUT_LIMIT = 10000;

// Diagnostic helper — file-only (PI_SLACK_NOTIFY_DIAG_CONSOLE=1 for console).
function diagLog(msg: string) {
	const line = `${new Date().toISOString()} pid=${process.pid} ${msg}\n`;
	const candidates = new Set<string>(["/tmp/slack-notify-diag.log"]);
	if (process.env.PI_CODING_AGENT_DIR) candidates.add(join(process.env.PI_CODING_AGENT_DIR, "slack-notify-diag.log"));
	for (const p of candidates) {
		try { appendFileSync(p, line); } catch {}
	}
	if (process.env.PI_SLACK_NOTIFY_DIAG_CONSOLE === "1" || process.env.SLACK_NOTIFY_DIAG_CONSOLE === "1") {
		try { console.error(`[slack-notify:diag] ${msg}`); } catch {}
	}
}


interface TextBlock {
	type?: string;
	text?: string;
}
interface RoleMessage {
	role?: unknown;
	content?: unknown;
}

function isObj(b: unknown): b is Record<string, unknown> {
	return typeof b === "object" && b !== null;
}
function isTextBlockTyped(b: unknown): b is TextBlock {
	return isObj(b) && b.type === "text" && typeof b.text === "string";
}
function isRoleMessage(m: unknown): m is RoleMessage {
	return typeof m === "object" && m !== null && "role" in m;
}

export function capText(text: string): string {
	if (text.length <= OUTPUT_LIMIT) return text;
	const notice = `\n\n[...output truncated: ${OUTPUT_LIMIT} of ${text.length} chars (hard limit)]\n\n`;
	const budget = OUTPUT_LIMIT - notice.length;
	const headLen = Math.floor(budget * 0.7);
	const tailLen = budget - headLen;
	return text.slice(0, headLen) + notice + (tailLen > 0 ? text.slice(-tailLen) : "");
}

function capAssistantMessage(msg: RoleMessage): RoleMessage | undefined {
	if (!Array.isArray(msg.content)) return undefined;
	const texts = msg.content.filter(isTextBlockTyped);
	if (texts.length === 0 || texts.reduce((n: number, t) => n + t.text.length + 2, -2) <= OUTPUT_LIMIT) return undefined;
	const capped = capText(texts.map((t) => t.text).join("\n\n"));
	let done = false;
	const content: unknown[] = [];
	for (const b of msg.content) {
		if (!isTextBlockTyped(b)) content.push(b);
		else if (!done) { done = true; content.push({ ...b, text: capped }); }
	}
	return { ...msg, content };
}

function readWebhookFromFile(path: string): string {
	try {
		const text = readFileSync(path, "utf-8");
		const m = text.match(/^SLACK_WEBHOOK_URL=(\S+)\s*$/m);
		return m ? m[1] : "";
	} catch { return ""; }
}

function loadWebhook(): string {
	if (process.env.PI_SLACK_NOTIFY === "0") return "";
	if (process.env.SLACK_WEBHOOK_URL) return process.env.SLACK_WEBHOOK_URL;
	const winPath = join(homedir(), ".pi", "secrets", "slack.env");
	const candidates = process.platform === "win32"
		? [winPath, "/etc/workforge/slack.env"]
		: ["/etc/workforge/slack.env", winPath];
	for (const p of candidates) {
		const v = readWebhookFromFile(p);
		if (v) return v;
	}
	return "";
}

function extractText(content: unknown): string {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return content.filter(isTextBlockTyped).map((b) => b.text).filter((t) => t.length > 0).join("\n");
}
function lastAssistantText(messages: readonly unknown[]): string {
	for (let i = messages.length - 1; i >= 0; i--) {
		const msg = messages[i];
		if (isRoleMessage(msg) && msg.role === "assistant") {
			const text = extractText(msg.content);
			if (text.trim()) return text;
		}
	}
	return "";
}

export function hasPiFinalResult(text: string): boolean {
	if (!text || typeof text !== "string") return false;
	const stripped = text.replace(/```[\s\S]*?```/g, "");
	const pattern = /(?:^|\r?\n)[ \t]*(?:\*\*|`|#)*[ \t]*RESULT=([A-Za-z0-9_.-]+)/;
	return pattern.test(stripped);
}

const SUCCESS_KEYS = ["MODE", "RESULT", "PROJECT", "GIT_REPOSITORY", "FINAL_SHA", "PUSHED_REF", "VERIFICATION", "STOP"];
const PLACEHOLDER = new Set(["", "NONE", "UNKNOWN", "NULL", "-", "N/A"]);
// Shape check only: is this SUCCESS backed by concrete field values?
// "verified-shape" NEVER proves the job actually succeeded. Unverifiable
// results stay deliverable but are header-flagged, never rewritten or dropped.
export function successVerdict(text: string): "verified-shape" | "unverified" | "not-success" {
	if (!text || typeof text !== "string") return "not-success";
	const stripped = text.replace(/```[\s\S]*?```/g, "");
	if (!/(?:^|\r?\n)[ \t]*(?:\*\*|`|#)*[ \t]*RESULT=SUCCESS\b/.test(stripped)) return "not-success";
	const fields = new Map<string, string>();
	for (const line of stripped.split(/\r?\n/)) {
		const m = /^[ \t]*(?:\*\*|`|#)*[ \t]*([A-Z_]{2,})=(.*)$/.exec(line.trimEnd());
		if (m) fields.set(m[1], m[2].replace(/^[*`# \t]+|[*` \t]+$/g, ""));
	}
	for (const k of SUCCESS_KEYS) if (!fields.has(k)) return "unverified";
	for (const k of ["PROJECT", "GIT_REPOSITORY", "FINAL_SHA", "PUSHED_REF"])
		if (PLACEHOLDER.has((fields.get(k) ?? "").toUpperCase())) return "unverified";
	if ((fields.get("MODE") ?? "").trim() === "") return "unverified";
	if (PLACEHOLDER.has((fields.get("VERIFICATION") ?? "").toUpperCase())) return "unverified";
	return "verified-shape";
}
export function eventId(sessionId: string, leafId: string, text: string): string {
	return createHash("sha1").update(`${sessionId}\n${leafId}\n${text}`).digest("hex");
}

function agentDir(): string {
	return process.env.PI_CODING_AGENT_DIR || join(homedir(), ".pi", "agent");
}
const notifiedStore = (): string => join(agentDir(), "slack-notified.jsonl");
const sentAuditLog = (): string => join(agentDir(), "slack-notify-sent.log");
// Audit only: ts, ids, hashes, lengths, result — never body or webhook.
function auditLog(event: string, sessionId: string, leafId: string, text: string, extra = ""): void {
	const line = `${new Date().toISOString()} pid=${process.pid} event=${event} session=${sessionId.slice(0, 8)} leaf=${leafId.slice(0, 8)} hash=${eventId(sessionId, leafId, text).slice(0, 12)} len=${text.length}${extra}\n`;
	try { appendFileSync(sentAuditLog(), line); } catch {}
}
function loadNotified(): Set<string> {
	const out = new Set<string>();
	try {
		for (const line of readFileSync(notifiedStore(), "utf-8").split("\n")) {
			try {
				const id = (JSON.parse(line) as { id?: unknown }).id;
				if (typeof id === "string" && id) out.add(id);
			} catch {}
		}
	} catch {}
	return out;
}
function persistNotified(id: string, sessionId: string, leafId: string, text: string, http: number): void {
	const row = JSON.stringify({ id, session: sessionId.slice(0, 8), leaf: leafId.slice(0, 8), hash: id.slice(0, 12), len: text.length, http, ts: new Date().toISOString(), pid: process.pid });
	try { appendFileSync(notifiedStore(), row + "\n"); } catch (err) {
		console.error("[slack-notify] store persist failed:", err instanceof Error ? err.message : "?");
	}
}
// Mutex via atomic mkdir; timeout -> proceed (at-least-once), never drop.
function acquireLock(timeoutMs = 8000): boolean {
	const dir = notifiedStore() + ".lock";
	const start = Date.now();
	while (Date.now() - start <= timeoutMs) {
		try { mkdirSync(dir); return true; } catch {}
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
	}
	return false;
}
function releaseLock(): void {
	try { rmSync(notifiedStore() + ".lock", { recursive: true, force: true }); } catch {}
}

export function buildHeader(sessionId: string, verdict: string = ""): string {
	const base = `[Pi session ${sessionId.slice(0, 8)}] result`;
	return verdict === "unverified" ? `\u26A0 UNVERIFIED-SUCCESS ${base}` : base;
}

export function chunks(text: string): string[] {
	if (text.length <= MAX_CHUNK) return [text];
	const parts: string[] = [];
	let rest = text;
	while (rest.length > 0) { parts.push(rest.slice(0, MAX_CHUNK)); rest = rest.slice(MAX_CHUNK); }
	return parts;
}
function collectMessages(ctx: { sessionManager?: { buildContextEntries?: () => unknown[] } }): unknown[] {
	const entries = ctx.sessionManager?.buildContextEntries?.() ?? [];
	const out: unknown[] = [];
	for (const e of entries) {
		if (isRoleMessage(e)) out.push(e);
		else if (isObj(e) && "message" in e && isRoleMessage((e as { message: unknown }).message)) out.push((e as { message: unknown }).message);
	}
	return out;
}

async function postToSlack(webhook: string, payload: string): Promise<number> {
	try {
		const res = await fetch(webhook, { method: "POST", headers: { "Content-Type": "application/json" }, body: payload, signal: AbortSignal.timeout(15000) });
		return res.status;
	} catch (err) {
		console.error("[slack-notify] send failed:", err instanceof Error ? err.message : String(err));
		return 0;
	}
}
function sleep(ms: number): Promise<void> { return new Promise((r) => setTimeout(r, ms)); }
async function sendWithRetry(webhook: string, payload: string): Promise<number> {
	let code = await postToSlack(webhook, payload);
	diagLog(`postToSlack http=${code}`);
	if (code < 200 || code >= 300) {
		await sleep(1100);
		code = await postToSlack(webhook, payload);
		diagLog(`postToSlack retry http=${code}`);
		if (code < 200 || code >= 300) console.error(`[slack-notify] message not delivered (http ${code})`);
	}
	return code;
}

export default function (pi: ExtensionAPI) {
	diagLog(`extension registered PID=${process.pid} PI_CODING_AGENT_DIR=${process.env.PI_CODING_AGENT_DIR ?? "(unset)"} extPath=slack-notify.ts`);
	if (process.env.PI_OUTPUT_CAP !== "0") {
		pi.on("message_end", (event) => {
			const msg = event.message;
			if (!isRoleMessage(msg) || msg.role !== "assistant") return undefined;
			const replacement = capAssistantMessage(msg);
			return replacement ? { message: replacement } : undefined;
		});
	}
	const notifiedRuns = loadNotified();
	const inFlight = new Set<string>();
	// Same handler for settled + end fallback; memory + store reject repeats.
	const handleSettled = async (_event: unknown, ctx: unknown) => {
		const c = ctx as { mode?: string; isIdle?: () => boolean; sessionManager?: unknown };
		const mode = String(c.mode ?? "unknown");
		const idle = typeof c.isIdle === "function" ? c.isIdle() : false;
		diagLog(`agent_settled reached mode=${mode} isIdle=${String(idle)} PI_SLACK_NOTIFY=${process.env.PI_SLACK_NOTIFY ?? "(unset)"} webhook_env=${process.env.SLACK_WEBHOOK_URL ? "set" : "unset"} PI_CODING_AGENT_DIR=${process.env.PI_CODING_AGENT_DIR ?? "(unset)"} sessionId=${process.env.PI_SESSION_ID ?? "(unset)"}`);
		if (process.env.PI_SLACK_NOTIFY === "0") { diagLog("guard: PI_SLACK_NOTIFY==0 -> suppressed"); return; }
		const _piDir = process.env.PI_CODING_AGENT_DIR ?? "";
		const _wfJob = (process.env as Record<string,string|undefined>)["WORKFORGE_JOB_ID"] ?? (process.env as Record<string,string|undefined>)["WORKFORGE_JOB"] ?? "";
		if (_wfJob || _piDir.includes("/workforge/workspaces") || _piDir.includes(".workforge/pi_config")) {
			diagLog();
			return;
		}
		if (!idle) { diagLog(`guard: idle guard BLOCKED isIdle=false`); return; }
		const text = lastAssistantText(collectMessages(c as never));
		if (!text.trim()) { diagLog(`guard: final text empty len=${text.length}`); return; }
		if (!hasPiFinalResult(text)) {
			diagLog(`guard: final text missing official Pi RESULT= contract -> suppressed`);
			return;
		}
		const sm = c.sessionManager as { getSessionId?: () => string; getLeafId?: () => string | null } | undefined;
		const sessionId = sm?.getSessionId?.() ?? "";
		const leafId = sm?.getLeafId?.() ?? "";
		const id = eventId(sessionId, leafId, text);
		diagLog(`id session=${sessionId.slice(0,8)} leaf=${leafId.slice(0,8)} hash=${id.slice(0,12)}`);
		if (notifiedRuns.has(id) || inFlight.has(id)) { diagLog(`dedupe REJECTED`); auditLog("duplicate_suppressed", sessionId, leafId, text); return; }
		inFlight.add(id);
		const locked = acquireLock();
		if (!locked) diagLog(`lock timeout -> proceed at-least-once`);
		let code = 0;
		try {
			// Re-check under lock: another process may have sent while waiting.
			for (const k of loadNotified()) notifiedRuns.add(k);
			if (notifiedRuns.has(id)) { diagLog(`dedupe REJECTED under lock`); auditLog("duplicate_suppressed", sessionId, leafId, text); return; }
			notifiedRuns.add(id);
			const webhook = loadWebhook();
		diagLog(`webhook loaded=${String(!!webhook)} platform=${process.platform} homedir=${homedir()}`);
		if (!webhook) { diagLog("webhook empty -> abort"); notifiedRuns.delete(id); return; }
			const verdict = successVerdict(text);
			const header = buildHeader(sessionId, verdict);
			const parts = chunks(text);
		diagLog(`send attempt parts=${parts.length}`);
		for (let i = 0; i < parts.length; i++) {
			const label = parts.length > 1 ? ` (${i + 1}/${parts.length})` : "";
			code = await sendWithRetry(webhook, JSON.stringify({ text: `*${header}*${label}\n${parts[i]}` }));
			diagLog(`send part ${i+1}/${parts.length} http=${code}`);
			if (i < parts.length - 1) await sleep(1100);
		}
		// Persist AFTER HTTP 200: crash before persist -> bounded resend. No exactly-once claim.
		if (code >= 200 && code < 300) { persistNotified(id, sessionId, leafId, text, code); auditLog("sent", sessionId, leafId, text, ` http=${code} verdict=${verdict}`); }
		else { notifiedRuns.delete(id); auditLog("failed", sessionId, leafId, text, ` http=${code}`); }
		diagLog(`HTTP result done http=${code}`);
		} finally { inFlight.delete(id); if (locked) releaseLock(); }
	};
	pi.on("agent_settled", handleSettled as never);
	pi.on("agent_end" as never, handleSettled as never);
	diagLog(`handlers registered: agent_settled + agent_end`);
}
