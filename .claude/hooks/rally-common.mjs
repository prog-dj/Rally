// Shared helpers for the Rally Project Brain hook scripts. Every hook is
// deliberately fail-open: if the backend is down or misconfigured, hooks log
// to stderr and exit 0 rather than interrupt the actual coding session.
import { readFileSync, writeFileSync, existsSync, mkdirSync, unlinkSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import os from "node:os";

export const BASE_URL = process.env.RALLY_BACKEND_URL || "http://localhost:8080";
export const PROJECT_ID = process.env.RALLY_PROJECT_ID;
export const ACTOR_ID = process.env.RALLY_ACTOR_ID;
export const ACTOR_TOKEN = process.env.RALLY_ACTOR_TOKEN;

// Who's actually driving this session — used to tell concurrent agent jobs
// apart in a job list ("Devansh|Claude Code|..." vs "Sam|Claude Code|...").
// git config is the best available source without adding new setup steps;
// falls back to the OS account name (often just a numeric profile id on
// Windows, so it's a last resort, not a first choice) and then a placeholder.
export function getPersonName() {
  try {
    const name = execFileSync("git", ["config", "user.name"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    if (name) return name;
  } catch {
    // git not installed, no user.name set, or not inside a git repo.
  }
  try {
    const username = os.userInfo().username;
    if (username) return username;
  } catch {
    // ignore
  }
  return "Someone";
}

const STATE_DIR = join(os.tmpdir(), "rally-claude-hooks");

function statePath(sessionId) {
  return join(STATE_DIR, `${sessionId}.json`);
}

export function readState(sessionId) {
  const path = statePath(sessionId);
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

export function writeState(sessionId, state) {
  mkdirSync(STATE_DIR, { recursive: true });
  writeFileSync(statePath(sessionId), JSON.stringify(state));
}

export function clearState(sessionId) {
  const path = statePath(sessionId);
  if (existsSync(path)) unlinkSync(path);
}

export async function readStdinJson() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

export async function api(method, path, body) {
  const headers = { "Content-Type": "application/json" };
  if (ACTOR_TOKEN) headers.Authorization = `Bearer ${ACTOR_TOKEN}`;
  const res = await fetch(`${BASE_URL}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`${method} ${path} -> ${res.status}: ${await res.text()}`);
  }
  return res.status === 204 ? null : res.json();
}

/// Every hook's main() is wrapped with this so a Rally outage never blocks
/// the actual Claude Code session.
export function runHook(main) {
  main().catch((err) => {
    console.error(`[rally-hook] ${err}`);
    process.exit(0);
  });
}
