#!/usr/bin/env node
// Agent-agnostic activity reporting: fires on every commit, regardless of
// which tool (Claude Code, Cursor, Antigravity, a human) made it. This is
// the fallback for clients with no hook system of their own — coarser
// than Claude Code's PostToolUse (fires per-commit, not per-edit), but the
// only mechanism that works identically everywhere, since it watches the
// repo instead of any particular tool's internals.
//
// Invoked by .githooks/post-commit (the actual git hook). Requires
// `git config core.hooksPath .githooks` to be set locally — git never
// reads hooks from a versioned directory on its own; each clone must
// opt in once. Best-effort: never fails the commit, always exits 0.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { execFileSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function truncate(s, n) {
  if (!s) return "";
  const flat = String(s).replace(/\s+/g, " ").trim();
  return flat.length > n ? `${flat.slice(0, n - 1)}…` : flat;
}

function git(args) {
  return execFileSync("git", args, { encoding: "utf8" }).trim();
}

async function main() {
  let subject, author, filesRaw, statRaw, sha;
  try {
    sha = git(["rev-parse", "--short", "HEAD"]);
    subject = git(["log", "-1", "--pretty=format:%s"]);
    author = git(["log", "-1", "--pretty=format:%an"]);
    filesRaw = git(["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]);
    statRaw = git(["diff-tree", "--no-commit-id", "--shortstat", "-r", "HEAD"]);
  } catch {
    return; // not in a git repo / no commits yet — nothing to report
  }

  const files = filesRaw.split("\n").filter(Boolean);
  const fileList = files.slice(0, 12).join(", ") + (files.length > 12 ? `, +${files.length - 12} more` : "");
  const headline = `Committed: ${truncate(subject, 100)} (${files.length} file${files.length === 1 ? "" : "s"})`;
  const detail = `Commit ${sha} by ${author}: "${subject}" — ${statRaw || "no stat"} — files: ${fileList}`;
  const summary = `${headline} — ${detail}`;

  let mcpConfig;
  try {
    const mcpJsonPath = path.join(__dirname, "..", ".mcp.json");
    mcpConfig = JSON.parse(readFileSync(mcpJsonPath, "utf8"));
  } catch {
    return; // no Rally MCP config in this repo — nothing to report to
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
        entity_type: "commit",
        verb: "committed",
        summary: truncate(summary, 2000),
      }),
      signal: AbortSignal.timeout(4000),
    });
  } catch {
    // best-effort — never fail the commit over this
  }
}

main().catch(() => {});
