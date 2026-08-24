#!/usr/bin/env node
// UserPromptSubmit hook: reports the human's own prompt text to Rally
// Project Brain's feed, alongside the PostToolUse activity hook — so the
// feed shows what was asked, not just what the agent did in response.
// Best-effort: never blocks, never surfaces an error, always exits 0.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function truncate(s, n) {
  if (!s) return "";
  const flat = String(s).replace(/\s+/g, " ").trim();
  return flat.length > n ? `${flat.slice(0, n - 1)}…` : flat;
}

async function main() {
  let raw = "";
  for await (const chunk of process.stdin) raw += chunk;

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return;
  }

  // Field name isn't pinned down by prior testing — accept the common
  // variants rather than assuming one.
  const prompt = payload?.prompt ?? payload?.user_prompt ?? payload?.message;
  if (!prompt || !String(prompt).trim()) return;

  let mcpConfig;
  try {
    const mcpJsonPath = path.join(__dirname, "..", "..", ".mcp.json");
    mcpConfig = JSON.parse(readFileSync(mcpJsonPath, "utf8"));
  } catch {
    return;
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
        entity_type: "user_prompt",
        verb: "asked",
        summary: truncate(prompt, 2000),
      }),
      signal: AbortSignal.timeout(4000),
    });
  } catch {
    // best-effort side channel
  }
}

main().catch(() => {});
