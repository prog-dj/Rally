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
use settings::update_settings_file;
use settings_content::{ContextServerCommand, ContextServerSettingsContent};

const RALLY_CONTEXT_SERVER_ID: &str = "rally";

/// Path to a local `mcp-server/index.mjs` checkout, for in-Zed ACP wiring
/// only (`register_rally_context_server` below, which spawns `node`
/// directly rather than going through `npx`). External agents don't need
/// this at all — `rally-project-brain-mcp` is published to npm, so the
/// "Connect Agent" flow's copyable command is just
/// `npx rally-project-brain-mcp login --agent`, no local checkout or path
/// required. Only relevant on a machine that has `Rally_Backend` checked
/// out with `RALLY_MCP_SERVER_PATH` pointed at it.
fn mcp_server_path() -> Option<PathBuf> {
    std::env::var("RALLY_MCP_SERVER_PATH").ok().map(PathBuf::from)
}

/// Upserts the `"rally"` context server entry in the user's Zed settings
/// with this actor's credentials. No-ops (with a log warning) if
/// `RALLY_MCP_SERVER_PATH` isn't set, since there is nothing to point the
/// entry at.
pub fn register_rally_context_server(
    cx: &App,
    backend_url: String,
    project_id: String,
    actor_id: String,
    actor_token: String,
) {
    let Some(mcp_server_path) = mcp_server_path() else {
        log::warn!(
            "RALLY_MCP_SERVER_PATH is not set — skipping MCP context-server registration for the connected agent"
        );
        return;
    };

    let mut env = HashMap::default();
    env.insert("RALLY_BACKEND_URL".to_string(), backend_url);
    env.insert("RALLY_PROJECT_ID".to_string(), project_id);
    env.insert("RALLY_ACTOR_ID".to_string(), actor_id);
    env.insert("RALLY_ACTOR_TOKEN".to_string(), actor_token);

    let entry = ContextServerSettingsContent::Stdio {
        enabled: true,
        remote: false,
        command: ContextServerCommand {
            path: PathBuf::from("node"),
            args: vec![mcp_server_path.display().to_string()],
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
         Starting a new task? Call `create_agent_job` first with the human's \
         actual request as `goal`, then `report_activity` as you work.\n\
         <!-- /rally-project-brain:onboarding -->"
    )
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
            Some(_) => continue, // marker already present, don't duplicate
        };

        if let Err(err) = fs.atomic_write(path.clone(), new_content).await {
            log::warn!("couldn't write Rally onboarding note to {}: {err:#}", path.display());
        }
    }
}
