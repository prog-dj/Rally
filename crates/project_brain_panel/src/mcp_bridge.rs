//! Wires a freshly-connected agent actor's credentials into Rally's own MCP
//! server (`Rally_Backend/mcp-server`) so any ACP agent Zed subsequently
//! launches picks up Rally's task/investigation/decision/feed tools
//! automatically — no dotfile, no manual MCP-client config editing.
//!
//! This reuses Zed's own settings-writer (`settings::update_settings_file`),
//! the exact call Zed's built-in "configure MCP server" modal uses
//! (`agent_ui::agent_configuration::configure_context_server_modal`) — so a
//! Rally entry shows up in Zed's settings/MCP UI exactly like any other
//! hand-configured context server would.
//!
//! Known v1 limitation: `update_settings_file` only targets the user-level
//! settings file (there is no project-scoped equivalent in this version of
//! Zed), so this upserts a single well-known `"rally"` entry — only the
//! most-recently-connected Rally project's credentials are active MCP-wide
//! per Zed install. Fine for one active connection at a time; not a
//! multi-project-simultaneously story.

use std::path::PathBuf;
use std::sync::Arc;

use collections::HashMap;
use fs::Fs;
use gpui::App;
use serde_json::{json, Value as JsonValue};
use settings::update_settings_file;
use settings_content::{ContextServerCommand, ContextServerSettingsContent};

const RALLY_CONTEXT_SERVER_ID: &str = "rally";

/// Upserts the `"rally"` context server entry in the user's Zed settings
/// with this actor's credentials, referencing it as `npx
/// rally-project-brain-mcp` — the package is published to npm, so this
/// needs no local `Rally_Backend` checkout or `RALLY_MCP_SERVER_PATH` env
/// var (an earlier version of this function required both, and silently
/// no-op'd with only a log warning if the env var wasn't set — a real
/// dead-end for anyone without a local backend checkout). `npx` resolves
/// the package fresh each launch, so this stays correct regardless of
/// local cache state, same reasoning as the external `login --agent` CLI
/// flow.
pub fn register_rally_context_server(
    cx: &App,
    backend_url: String,
    project_id: String,
    actor_id: String,
    actor_token: String,
) {
    let mut env = HashMap::default();
    env.insert("RALLY_BACKEND_URL".to_string(), backend_url);
    env.insert("RALLY_PROJECT_ID".to_string(), project_id);
    env.insert("RALLY_ACTOR_ID".to_string(), actor_id);
    env.insert("RALLY_ACTOR_TOKEN".to_string(), actor_token);

    let entry = ContextServerSettingsContent::Stdio {
        enabled: true,
        remote: false,
        command: ContextServerCommand {
            path: PathBuf::from("npx"),
            args: vec!["-y".to_string(), "rally-project-brain-mcp".to_string()],
            env: Some(env),
            timeout: None,
        },
    };

    let fs = <dyn Fs>::global(cx);
    update_settings_file(fs, cx, move |settings, _| {
        settings
            .project
            .context_servers
            .insert(Arc::from(RALLY_CONTEXT_SERVER_ID), entry);
    });
}

/// Same marker/snippet the CLI (`mcp-server login --agent`) writes — kept
/// byte-identical on purpose so a project onboarded from either path ends
/// up with the same files. Text is duplicated (Rust here, JS there) since
/// there's no shared source between the two; if you edit one, edit both.
const RALLY_MARKER: &str = "<!-- rally-project-brain:onboarding -->";
const INSTRUCTION_FILE_NAMES: &[&str] =
    &["CLAUDE.md", "AGENTS.md", "GEMINI.md", ".cursorrules", ".windsurfrules"];

fn rally_onboarding_snippet() -> String {
    format!(
        "{RALLY_MARKER}\n\
         This project is connected to Rally Project Brain. At the start of every \
         session, before anything else, call `get_project_context` to see what's \
         already been done — other agents and humans share this project's memory. \
         Starting a new task? Call `create_agent_job` ONCE, as your first action, \
         with a short specific title as `goal` formatted as \"{{the operator's \
         name, if known}}|{{your product name, e.g. Claude Code/Cursor/Gemini \
         CLI/Codex}}|{{a few words on the task}}\" (e.g. \"Devansh|Claude \
         Code|Review Rally project context\") — not the human's raw message \
         verbatim, and not one call per message: keep using the same job id for \
         every later request on the same task, and only call `create_agent_job` \
         again if the human starts a genuinely new, unrelated task later in this \
         session. If you're running as Claude Code, a session-level job was \
         already auto-registered by the SessionStart hook before you saw this — \
         its id and title prefix are in your startup context; this call \
         registers a separate, task-specific job alongside it, so a human sees \
         exactly two jobs per session (one for the session, one for the task), \
         not one per message. Then `report_activity` (short status updates — \
         pass your job id as `job_id` so it groups under it in the feed) and \
         `report_turn` (your real conversation, verbatim, not a paraphrase — \
         a user message, your own response text, or a tool call with its real \
         parameters) as you work. Before starting each new step, call \
         `check_steering_messages` with your job id — nothing else interrupts \
         you, so this is the only way you'll notice if a human redirected you. \
         Taking over someone else's unfinished job? `claim_agent_job` only \
         succeeds once they've released it or reported their session closed — \
         once it does, call `get_agent_job_turns` before doing anything else, so \
         you continue their work instead of starting cold. No tool tells you who \
         you are unprompted — call `whoami` if you need your own actor id or \
         display name (e.g. to refer to yourself in a message to another agent \
         job).\n\
         <!-- /rally-project-brain:onboarding -->"
    )
}

/// Replaces the span from `RALLY_MARKER` through the next `-->` after it
/// (the closing tag, whatever its exact text) with `snippet`. `None` if the
/// marker isn't present at all.
fn replace_marked_block(content: &str, snippet: &str) -> Option<String> {
    let start = content.find(RALLY_MARKER)?;
    let after_start = start + RALLY_MARKER.len();
    let close_offset = content[after_start..].find("-->")?;
    let end = after_start + close_offset + "-->".len();
    let mut result = String::with_capacity(content.len());
    result.push_str(&content[..start]);
    result.push_str(snippet);
    result.push_str(&content[end..]);
    Some(result)
}

/// Writes (or appends to, if one already exists and lacks the marker) the
/// same onboarding note to every known instructions-file convention in
/// `worktree_root`. Connecting an agent via this panel wires its MCP
/// *tools* automatically (`register_rally_context_server` above) — it
/// doesn't give the agent the *habit* of using them proactively, the same
/// gap the CLI login flow closes for externally-connected agents. An agent
/// launched inside Zed via "Launch in Zed now" needs the same nudge, since
/// nothing else tells it to check Rally before acting on the human's first
/// message. Best-effort: a write failure for one file is logged and
/// skipped, never fails the connection itself.
pub async fn write_onboarding_instructions(fs: Arc<dyn Fs>, worktree_root: PathBuf) {
    let snippet = rally_onboarding_snippet();
    for name in INSTRUCTION_FILE_NAMES {
        let path = worktree_root.join(name);
        let existing = if fs.is_file(&path).await {
            match fs.load(&path).await {
                Ok(content) => Some(content),
                Err(err) => {
                    log::warn!("couldn't read {} to add Rally onboarding note: {err:#}", path.display());
                    continue;
                }
            }
        } else {
            None
        };

        let new_content = match existing {
            None => format!("{snippet}\n"),
            Some(content) if !content.contains(RALLY_MARKER) => {
                format!("{}\n\n{snippet}\n", content.trim_end())
            }
            // Marker from an earlier run is already there — replace that
            // whole block in place instead of skipping. Skipping meant a
            // snippet-text change (like the check_steering_messages line
            // just added) would never reach a project onboarded before the
            // change, since "marker present" was treated as "nothing to do"
            // forever.
            Some(content) => match replace_marked_block(&content, &snippet) {
                Some(replaced) if replaced != content => replaced,
                _ => continue,
            },
        };

        if let Err(err) = fs.atomic_write(path.clone(), new_content).await {
            log::warn!("couldn't write Rally onboarding note to {}: {err:#}", path.display());
        }
    }
}

// Byte-identical to Rally_Backend/.claude/hooks/*.mjs — same duplication
// reasoning as the onboarding snippet above (no shared source between Rust
// and JS), generated by hand from those files. If you edit a hook there,
// paste the updated content in here too. Embedded directly as Rust raw
// strings rather than base64 (the JS side's approach, since npm needs a
// single-file package) — Zed ships this crate compiled, so there's no
// packaging constraint pushing toward base64 here, and raw strings stay
// diffable.
const RALLY_COMMON_MJS: &str = r#"// Shared helpers for the Rally Project Brain hook scripts. Every hook is
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
"#;

const SESSION_START_MJS: &str = r#"#!/usr/bin/env node
// SessionStart: register an agent job for this Claude Code session and hand
// back the project's shared-memory brief as additional context, so the
// session starts already aware of open tasks/investigations.
import path from "node:path";
import { readStdinJson, writeState, api, runHook, getPersonName, PROJECT_ID, ACTOR_ID } from "./rally-common.mjs";

runHook(async () => {
  const input = await readStdinJson();
  if (!PROJECT_ID || !ACTOR_ID) return;

  // "Name|Provider|Title" — the same prefix is handed to the agent below so
  // its own task-level job (created via the create_agent_job tool) matches,
  // and so two concurrent sessions in the same repo/directory are actually
  // distinguishable in a job list instead of both reading identically.
  const titlePrefix = `${getPersonName()}|Claude Code|`;
  const dirName = input.cwd ? path.basename(input.cwd) : "unknown directory";

  const job = await api("POST", `/projects/${PROJECT_ID}/agent-jobs`, {
    actor_id: ACTOR_ID,
    goal: `${titlePrefix}Session in ${dirName}`,
    context_snapshot: { source: input.source || "startup" },
  });
  await api("PATCH", `/agent-jobs/${job.id}`, { actor_id: ACTOR_ID, status: "running" });

  writeState(input.session_id, { jobId: job.id, lastSeenMessageCount: 0 });

  const brief = await api("GET", `/projects/${PROJECT_ID}/context`);
  const openTasks =
    brief.open_tasks.map((t) => `- [${t.status}] ${t.title}`).join("\n") || "(none)";
  const activeInvestigations =
    brief.active_investigations.map((i) => `- ${i.title}`).join("\n") || "(none)";

  const context = [
    "Project Brain shared memory for this project:",
    "",
    "Open tasks:",
    openTasks,
    "",
    "Active investigations:",
    activeInvestigations,
    "",
    `(This session is registered as agent job ${job.id} — file edits, commands, ` +
      "and steering messages sent via the Project Brain dashboard/Zed panel will show up here. " +
      `Its title starts with "${titlePrefix}" — reuse that exact prefix if you register a ` +
      `task-level job (call create_agent_job ONCE, as your first action, for the actual task, ` +
      `with goal formatted as "${titlePrefix}<a few words on the task>", e.g. ` +
      `"${titlePrefix}Review Rally project context" — not a call per message. For every later ` +
      "request in this same session, keep using that same task job id instead of creating " +
      "another one; only call create_agent_job again if the human starts a genuinely new, " +
      "unrelated task later in this session.)",
  ].join("\n");

  console.log(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: context },
    }),
  );
});
"#;

const USER_PROMPT_SUBMIT_MJS: &str = r#"#!/usr/bin/env node
// UserPromptSubmit: before each turn, check whether anyone sent a steering
// message via the Project Brain dashboard/Zed panel since the last turn, and
// if so, fold it into context. This is what makes claim/messages more than
// a write-only log — the agent actually sees it. Also reports the human's
// actual prompt text as a "user" transcript turn — the ground-truth
// counterpart to report_turn's self-reported user turns, straight from the
// real input, not the agent's own paraphrase of what was asked.
import { readStdinJson, readState, writeState, api, runHook, ACTOR_ID } from "./rally-common.mjs";

runHook(async () => {
  const input = await readStdinJson();
  if (!ACTOR_ID) return;

  const state = readState(input.session_id);
  if (!state) return;

  if (input.prompt) {
    await api("POST", `/agent-jobs/${state.jobId}/turns/report`, {
      actor_id: ACTOR_ID,
      role: "user",
      content: input.prompt,
    });
  }

  const detail = await api("GET", `/agent-jobs/${state.jobId}`);
  const newMessages = detail.messages.slice(state.lastSeenMessageCount);
  writeState(input.session_id, { ...state, lastSeenMessageCount: detail.messages.length });

  if (newMessages.length === 0) return;

  const context = [
    "Steering messages received via Project Brain since your last turn:",
    ...newMessages.map((m) => `- ${m.content}`),
  ].join("\n");

  console.log(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: "UserPromptSubmit", additionalContext: context },
    }),
  );
});
"#;

const POST_TOOL_USE_MJS: &str = r#"#!/usr/bin/env node
// PostToolUse (matcher: *): log real file edits and commands to Project
// Brain's generic feed (terse, for the "Live feed" panel), and separately
// report every tool call as a full-fidelity turn (untruncated tool_name +
// tool_input, plus the tool_response if present) so it renders in the same
// transcript view a self-reporting agent's report_turn calls do. This is the
// ground-truth path: real tool_input straight from Claude Code's own hook
// payload, not something the model chose to summarize.
import { readStdinJson, readState, api, runHook, PROJECT_ID, ACTOR_ID } from "./rally-common.mjs";

const FEED_TOOLS = new Set(["Edit", "Write", "MultiEdit", "Bash"]);
// Result content can be huge (a big file read, a long command's stdout) —
// capped so one tool call can't balloon the transcript.
const MAX_RESULT_CHARS = 4000;

runHook(async () => {
  const input = await readStdinJson();
  if (!PROJECT_ID || !ACTOR_ID) return;

  const state = readState(input.session_id);
  if (!state) return;

  if (FEED_TOOLS.has(input.tool_name)) {
    let entityType;
    let entityId;
    let verb;
    let summary;

    if (input.tool_name === "Bash") {
      const command = (input.tool_input?.command || "").slice(0, 200);
      entityType = "command";
      entityId = command;
      verb = "ran";
      summary = `Claude Code ran: ${command}`;
    } else {
      const filePath = input.tool_input?.file_path || "unknown file";
      entityType = "file";
      entityId = filePath;
      verb = "edited";
      summary = `Claude Code edited ${filePath}`;
    }

    await api("POST", `/projects/${PROJECT_ID}/events`, {
      actor_id: ACTOR_ID,
      entity_type: entityType,
      entity_id: entityId,
      verb,
      summary,
      agent_job_id: state.jobId,
    });

    await api("PATCH", `/agent-jobs/${state.jobId}`, {
      actor_id: ACTOR_ID,
      context_snapshot: { last_tool: input.tool_name, last_action: summary },
    });
  }

  // Full-fidelity transcript turn, for every tool call, not just the ones
  // that hit the terse feed above.
  if (input.tool_name) {
    await api("POST", `/agent-jobs/${state.jobId}/turns/report`, {
      actor_id: ACTOR_ID,
      role: "assistant",
      tool_name: input.tool_name,
      tool_use_id: input.tool_use_id,
      tool_input: input.tool_input,
    });

    if (input.tool_response !== undefined) {
      const resultText =
        typeof input.tool_response === "string"
          ? input.tool_response
          : JSON.stringify(input.tool_response);
      await api("POST", `/agent-jobs/${state.jobId}/turns/report`, {
        actor_id: ACTOR_ID,
        role: "tool",
        tool_name: input.tool_name,
        tool_use_id: input.tool_use_id,
        content: resultText.slice(0, MAX_RESULT_CHARS),
      });
    }
  }
});
"#;

const SESSION_END_MJS: &str = r#"#!/usr/bin/env node
// SessionEnd: mark this session's agent job done, report its owner session
// closed (so a different actor may claim it without needing this one to
// come back and explicitly release it), and clean up local state.
import { readStdinJson, readState, clearState, api, runHook, ACTOR_ID } from "./rally-common.mjs";

runHook(async () => {
  const input = await readStdinJson();
  if (!ACTOR_ID) return;

  const state = readState(input.session_id);
  if (!state) return;

  await api("PATCH", `/agent-jobs/${state.jobId}`, {
    actor_id: ACTOR_ID,
    status: "done",
    output_summary: `Session ended (${input.reason || "unknown"})`,
  });

  await api("POST", `/agent-jobs/${state.jobId}/close-session`, {
    actor_id: ACTOR_ID,
  });

  clearState(input.session_id);
});
"#;

/// Ships Claude Code's own hooks into `worktree_root` and wires them into
/// `.claude/settings.json` — the ground-truth transcript path (real
/// `tool_input` straight from Claude Code's hook payload) available whenever
/// the agent launched in Zed actually is Claude Code, strictly better than
/// the `report_turn` self-report fallback every other agent is stuck with.
/// Mirrors the CLI's `login --agent` flow (`mcp-server/index.mjs`) exactly,
/// so a project onboarded from either path ends up with the same hooks.
/// Best-effort: a write failure is logged and skipped, never fails the
/// connection itself.
pub async fn write_claude_code_hooks(
    fs: Arc<dyn Fs>,
    worktree_root: PathBuf,
    backend_url: String,
    project_id: String,
    actor_id: String,
    actor_token: String,
) {
    let hooks_dir = worktree_root.join(".claude").join("hooks");
    let hook_files: [(&str, &str); 5] = [
        ("rally-common.mjs", RALLY_COMMON_MJS),
        ("session-start.mjs", SESSION_START_MJS),
        ("user-prompt-submit.mjs", USER_PROMPT_SUBMIT_MJS),
        ("post-tool-use.mjs", POST_TOOL_USE_MJS),
        ("session-end.mjs", SESSION_END_MJS),
    ];
    for (name, content) in hook_files {
        let path = hooks_dir.join(name);
        if let Err(err) = fs.atomic_write(path.clone(), content.to_string()).await {
            log::warn!("couldn't write Claude Code hook {}: {err:#}", path.display());
        }
    }

    let settings_path = worktree_root.join(".claude").join("settings.json");
    let existing_raw = if fs.is_file(&settings_path).await {
        fs.load(&settings_path).await.ok()
    } else {
        None
    };
    let mut settings: JsonValue = existing_raw
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_else(|| json!({}));
    if !settings.is_object() {
        settings = json!({});
    }

    let env = settings
        .as_object_mut()
        .expect("settings is always an object here")
        .entry("env")
        .or_insert_with(|| json!({}));
    if !env.is_object() {
        *env = json!({});
    }
    let env_obj = env.as_object_mut().expect("env is always an object here");
    env_obj.insert("RALLY_BACKEND_URL".to_string(), json!(backend_url));
    env_obj.insert("RALLY_PROJECT_ID".to_string(), json!(project_id));
    env_obj.insert("RALLY_ACTOR_ID".to_string(), json!(actor_id));
    env_obj.insert("RALLY_ACTOR_TOKEN".to_string(), json!(actor_token));

    let hooks_field = settings
        .as_object_mut()
        .expect("settings is always an object here")
        .entry("hooks")
        .or_insert_with(|| json!({}));
    if !hooks_field.is_object() {
        *hooks_field = json!({});
    }
    let hooks_obj = hooks_field.as_object_mut().expect("hooks is always an object here");
    hooks_obj.insert(
        "SessionStart".to_string(),
        json!([{ "hooks": [{ "type": "command", "command": "node .claude/hooks/session-start.mjs" }] }]),
    );
    hooks_obj.insert(
        "UserPromptSubmit".to_string(),
        json!([{ "hooks": [{ "type": "command", "command": "node .claude/hooks/user-prompt-submit.mjs" }] }]),
    );
    hooks_obj.insert(
        "PostToolUse".to_string(),
        json!([{ "matcher": "*", "hooks": [{ "type": "command", "command": "node .claude/hooks/post-tool-use.mjs" }] }]),
    );
    hooks_obj.insert(
        "SessionEnd".to_string(),
        json!([{ "hooks": [{ "type": "command", "command": "node .claude/hooks/session-end.mjs" }] }]),
    );

    let serialized = match serde_json::to_string_pretty(&settings) {
        Ok(s) => s,
        Err(err) => {
            log::warn!("couldn't serialize .claude/settings.json: {err:#}");
            return;
        }
    };
    if let Err(err) = fs.atomic_write(settings_path.clone(), format!("{serialized}\n")).await {
        log::warn!("couldn't write {}: {err:#}", settings_path.display());
    }
}
