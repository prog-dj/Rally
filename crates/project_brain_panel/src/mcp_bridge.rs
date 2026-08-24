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

/// Path to `mcp-server/index.mjs`. Pre-alpha: this only works on a machine
/// that has the `Rally_Backend` repo checked out and `RALLY_MCP_SERVER_PATH`
/// pointed at it. Packaging the MCP server as an installable/`npx`-able
/// artifact (it already declares `bin: rally-project-brain-mcp`) is out of
/// scope here — that's an alpha-launch distribution concern, not this
/// feature's.
fn mcp_server_path() -> Option<PathBuf> {
    std::env::var("RALLY_MCP_SERVER_PATH").ok().map(PathBuf::from)
}

/// Exposed so the "Connect Agent" flow can build a real, usable `.mcp.json`
/// snippet for external agents (a separate Claude Code/Cursor/etc. session,
/// not one launched inside this Zed instance) — those need the actual path
/// on disk to `mcp-server/index.mjs`, same as this module's own in-Zed
/// auto-wiring does.
pub fn mcp_server_path_display() -> Option<String> {
    mcp_server_path().map(|p| p.display().to_string())
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
