/**
 * runtime-generation-guard.ts — Pi Runtime Generation Guard extension
 *
 * Guarantees that long-lived Pi processes do not continue executing turns
 * with stale in-memory module caches after patches or packages are updated on disk.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent"
import fs from "node:fs"

const MARKER_PATH = "/home/deploy/.pi/agent/runtime-generation.json"

let processLoadedGeneration: string | null = null

export function readDiskMarker(): {
  generation?: string
  generationShort?: string
  status?: string
  updatedAt?: string
  pkgVersions?: Record<string, string>
  [key: string]: unknown
} | null {
  try {
    if (!fs.existsSync(MARKER_PATH)) return null
    return JSON.parse(fs.readFileSync(MARKER_PATH, "utf-8"))
  } catch {
    return null
  }
}

export function getProcessLoadedGeneration(): string | null {
  if (processLoadedGeneration === null) {
    const marker = readDiskMarker()
    processLoadedGeneration = marker?.generation ?? null
  }
  return processLoadedGeneration
}

export function checkRuntimeStale(): {
  stale: boolean
  loaded: string | null
  current: string | null
  message?: string
} {
  if (process.env.PI_FORCE_STALE_RUNTIME_TEST === "1") {
    return {
      stale: true,
      loaded: "test-loaded",
      current: "test-current",
      message: "STALE_RUNTIME_DETECTED: runtime patch changed; restart Pi required (test injection)",
    }
  }
  const loaded = getProcessLoadedGeneration()
  const current = readDiskMarker()?.generation ?? null
  if (loaded && current && loaded !== current) {
    return {
      stale: true,
      loaded,
      current,
      message: `STALE_RUNTIME_DETECTED: runtime patch changed; restart Pi required (loaded: ${loaded.slice(0, 12)}, current: ${current.slice(0, 12)})`,
    }
  }
  return {
    stale: false,
    loaded,
    current,
  }
}

export function _setProcessLoadedGenerationForTest(gen: string | null): void {
  processLoadedGeneration = gen
}

export default function (pi: ExtensionAPI) {
  // Capture process generation at extension load time
  getProcessLoadedGeneration()

  pi.on("before_agent_start", async (_event, ctx) => {
    const check = checkRuntimeStale()
    if (check.stale) {
      ctx.ui?.notify?.(check.message!, "error")
      throw new Error(check.message)
    }
  })

  pi.on("turn_start", async (_event, ctx) => {
    const check = checkRuntimeStale()
    if (check.stale) {
      ctx.ui?.notify?.(check.message!, "error")
    }
  })

  pi.registerCommand("runtime-status", {
    description: "Display current runtime patch generation and stale detection status",
    async handler(_args: string, ctx: any) {
      const check = checkRuntimeStale()
      const marker = readDiskMarker()
      const statusText = check.stale
        ? "STALE_RUNTIME_DETECTED (RESTART REQUIRED)"
        : "CURRENT (HEALTHY)"
      const lines = [
        `[Runtime Generation Guard]`,
        `Status:             ${statusText}`,
        `Process Loaded Gen: ${check.loaded ? check.loaded.slice(0, 12) : "unknown"}`,
        `Current Disk Gen:   ${check.current ? check.current.slice(0, 12) : "unknown"}`,
        `Durable Patch Disk: ${marker?.status ?? "unknown"}`,
        `Last Updated:       ${marker?.updatedAt ?? "unknown"}`,
      ]
      ctx.ui?.notify?.(lines.join("\n"), check.stale ? "error" : "info")
    },
  })
}
