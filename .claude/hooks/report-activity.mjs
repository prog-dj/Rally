#!/usr/bin/env node
// PostToolUse hook: automatically reports Edit/Write/NotebookEdit/Bash/
// PowerShell/Grep/Glob activity to Rally Project Brain's feed. Exists
// because agents (including Claude Code) reliably forget to call
// report_activity by hand during long tool-heavy stretches — this makes
// reporting mechanical instead of a judgment call. Best-effort: never
// blocks, never surfaces an error, always exits 0.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function truncate(s, n) {
  if (!s) return "";
  const flat = String(s).replace(/\s+/g, " ").trim();
  return flat.length > n ? `${flat.slice(0, n - 1)}…` : flat;
}

// Keep only the last few path segments — the feed is skimmed by a human,
// full absolute Windows paths are just noise.
function shortPath(p) {
  if (!p) return "";
  const parts = String(p).split(/[\\/]/).filter(Boolean);
  return parts.slice(-3).join("/");
}

// Returns { verb, headline, detail } or null to skip this tool entirely.
// `headline` is the short natural-sounding line; `detail` is the fuller,
// uncapped(-ish) text appended after it so full-text search
// (search_project_memory queries memory_events.summary) has more to match
// against than the truncated headline alone.
function summarize(toolName, input) {
  switch (toolName) {
    case "Edit": {
      const file = shortPath(input?.file_path);
      return {
        verb: "edited",
        headline: `Edited ${file}`,
        detail: `Edited ${input?.file_path ?? file}`,
      };
    }
    case "Write": {
      const file = shortPath(input?.file_path);
      return {
        verb: "wrote",
        headline: `Wrote ${file}`,
        detail: `Wrote ${input?.file_path ?? file}`,
      };
    }
    case "NotebookEdit": {
      const file = shortPath(input?.notebook_path);
      return {
        verb: "edited",
        headline: `Edited notebook ${file}`,
        detail: `Edited notebook ${input?.notebook_path ?? file}`,
      };
    }
    case "Bash":
    case "PowerShell": {
      const cmd = input?.command ?? "";
      return {
        verb: "ran",
        headline: `Ran: ${truncate(cmd, 100)}`,
        detail: `Ran (${toolName}): ${truncate(cmd, 1500)}`,
      };
    }
    case "Grep": {
      const pattern = input?.pattern ?? "";
      const where = input?.path ? ` in ${shortPath(input.path)}` : "";
      return {
        verb: "searched",
        headline: `Searched for "${truncate(pattern, 60)}"${where}`,
        detail: `Grep pattern: ${truncate(pattern, 500)}${
          input?.path ? ` | path: ${input.path}` : ""
        }${input?.glob ? ` | glob: ${input.glob}` : ""}${
          input?.type ? ` | type: ${input.type}` : ""
        }`,
      };
    }
    case "Glob": {
      const pattern = input?.pattern ?? "";
      const where = input?.path ? ` in ${shortPath(input.path)}` : "";
      return {
        verb: "searched",
        headline: `Searched for files matching "${truncate(pattern, 60)}"${where}`,
        detail: `Glob pattern: ${truncate(pattern, 500)}${
          input?.path ? ` | path: ${input.path}` : ""
        }`,
      };
    }
    default:
      return null;
  }
}

async function main() {
  let raw = "";
  for await (const chunk of process.stdin) raw += chunk;

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return; // malformed stdin — nothing to report, fail silent
  }

  const toolName = payload?.tool_name;
  const result = summarize(toolName, payload?.tool_input);
  if (!result) return; // not a tool we report on (e.g. Read) — skip quietly

  // Headline stays short and skimmable; detail is appended so full-text
  // search over `summary` has real substance, not just the display line.
  const summary =
    result.detail && result.detail !== result.headline
      ? `${result.headline} — ${result.detail}`
      : result.headline;

  let mcpConfig;
  try {
    const mcpJsonPath = path.join(__dirname, "..", "..", ".mcp.json");
    mcpConfig = JSON.parse(readFileSync(mcpJsonPath, "utf8"));
  } catch {
    return; // no config available — can't report, fail silent
  }

  const rallyEnv = mcpConfig?.mcpServers?.["rally-project-brain"]?.env;
  const backendUrl = rallyEnv?.RALLY_BACKEND_URL;
  const projectId = rallyEnv?.RALLY_PROJECT_ID;
  const actorId = rallyEnv?.RALLY_ACTOR_ID;
  const actorToken = rallyEnv?.RALLY_ACTOR_TOKEN;
  if (!backendUrl || !projectId || !actorId || !actorToken) return;

  try {
    await fetch(`${backendUrl}/projects/${projectId}/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${actorToken}`,
      },
      body: JSON.stringify({
        actor_id: actorId,
        entity_type: "tool_use",
        verb: result.verb,
        summary: truncate(summary, 2000),
      }),
      signal: AbortSignal.timeout(4000),
    });
  } catch {
    // best-effort side channel — never fail the tool call over this
  }
}

main().catch(() => {});
