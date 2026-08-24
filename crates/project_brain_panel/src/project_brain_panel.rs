mod live_state;
mod mcp_bridge;
mod status_item;
mod welcome;

pub use live_state::{RallyLiveState, RallyLiveStateEvent};
pub use status_item::RallyStatusItem;
pub use welcome::RallyWelcome;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use chrono::{DateTime, Utc};
use editor::Editor;
use futures::StreamExt;
use gpui::{
    Action, App, AsyncApp, ClipboardItem, Context, Entity, EventEmitter, FocusHandle, Focusable,
    IntoElement, Pixels, Render, Subscription, Task, Window, px, prelude::*, WeakEntity,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use ui::{prelude::*, IconName};
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
};

/// Debug builds default to localhost so local development needs no
/// configuration; release builds (what testers actually run) default to the
/// deployed backend, mirroring how `release_channel::RELEASE_CHANNEL_NAME`
/// treats `cfg!(debug_assertions)`. Either can still be overridden with
/// `RALLY_BACKEND_URL`.
fn default_backend_base_url() -> &'static str {
    if cfg!(debug_assertions) {
        "http://localhost:8080"
    } else {
        "https://rally-backend.fly.dev"
    }
}

/// Backend base URL — override with `RALLY_BACKEND_URL` (e.g. to point at a
/// remote Project Brain instance, or a non-default port).
fn backend_base_url() -> String {
    std::env::var("RALLY_BACKEND_URL").unwrap_or_else(|_| default_backend_base_url().to_string())
}

/// Websocket URL for the live feed. Override directly with
/// `RALLY_BACKEND_WS_URL`, or let it derive from `RALLY_BACKEND_URL` by
/// swapping the scheme (http -> ws, https -> wss) so one env var covers the
/// common case.
fn backend_ws_url() -> String {
    if let Ok(url) = std::env::var("RALLY_BACKEND_WS_URL") {
        return url;
    }
    let base = backend_base_url();
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base
    }
}

fn get_project_id() -> Option<String> {
    std::env::var("RALLY_PROJECT_ID").ok()
}

/// Which actor this Zed session acts as when claiming a job or sending a
/// steering message. Mirrors RALLY_PROJECT_ID — set once per session. Once
/// an actor is created via the onboarding UI, the panel's own `actor_id`
/// field takes over; this is only the initial fallback.
fn get_actor_id() -> Option<String> {
    std::env::var("RALLY_ACTOR_ID").ok()
}

static EVENT_COUNTER: AtomicU64 = AtomicU64::new(1);

gpui::actions!(project_brain, [ToggleProjectBrainPanel]);

/// Shared Tokio runtime used to drive reqwest / websocket networking,
/// since GPUI's own async executor is not a Tokio reactor.
fn tokio_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("failed to create tokio runtime"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FeedEvent {
    #[serde(default)]
    pub id: String,
    pub project_id: Option<String>,
    pub actor_id: Option<String>,
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub verb: Option<String>,
    pub summary: String,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FeedResponse {
    pub events: Vec<FeedEvent>,
    pub next_cursor: Option<String>,
}

/// Mirrors the backend's presence snapshot entry (`presence.rs::PresenceEntry`).
/// The backend carries no display name here — `display_name_for` resolves
/// one from the fetched actor list instead.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PresenceEntry {
    pub actor_id: String,
    #[serde(default)]
    pub current_file: Option<String>,
    #[serde(default)]
    pub current_line: Option<i64>,
    #[serde(default)]
    pub current_task_id: Option<String>,
}

/// Mirrors the backend's Actor row (id/kind/display_name only) — used to
/// resolve an actor_id to a human-readable name anywhere the panel renders
/// one (presence, claimed-by, feed).
#[derive(Clone, Debug, Deserialize)]
pub struct ActorInfo {
    pub id: String,
    #[serde(default)]
    pub kind: String,
    pub display_name: String,
}

/// What the "Connect Agent" flow shows once, right after a new agent actor
/// is created — the Mosaic-style token + fixed CLI command pattern, not a
/// raw config blob the human has to hand-edit.
#[derive(Clone, Debug)]
pub struct ConnectedAgentInfo {
    pub display_name: String,
    pub token: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub command: String,
}

/// The wire shape of `POST /projects/:id/actors` — the actor row flattened
/// together with its one-time bearer token.
#[derive(Clone, Debug, Deserialize)]
pub struct CreateActorResponse {
    pub id: String,
    #[serde(default)]
    pub kind: String,
    pub display_name: String,
    pub token: String,
    #[serde(default)]
    pub token_expires_at: Option<DateTime<Utc>>,
}

/// A lightweight mirror of the backend's full AgentJob, as broadcast on
/// `agent_job_update` — only the fields this panel renders.
#[derive(Clone, Debug, Deserialize)]
pub struct LiveAgentJob {
    pub id: String,
    pub goal: String,
    pub status: String,
    #[serde(default)]
    pub claimed_by_actor_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnRole {
    User,
    Assistant,
    Tool,
}

/// Mirrors the backend's `SessionTurn` — one block (user message, assistant
/// text, tool call, or tool result) of a headless agent job's live
/// conversation, broadcast individually as it happens.
#[derive(Clone, Debug, Deserialize)]
pub struct SessionTurn {
    pub id: String,
    pub agent_job_id: String,
    pub role: TurnRole,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_use_id: Option<String>,
    #[serde(default)]
    pub tool_input: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Mirrors the backend's `presence.rs::LiveMessage` wire shape exactly —
/// internally tagged on `type`, snake_case variant names. Getting this
/// mismatched silently drops every live update of that kind (unknown fields
/// don't error; they just deserialize to `None`/defaults), so this must
/// track the backend struct field-for-field.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LiveMessage {
    PresenceUpdate {
        #[serde(default)]
        actors: Vec<PresenceEntry>,
    },
    MemoryEvent {
        event: FeedEvent,
    },
    AgentJobUpdate {
        job: LiveAgentJob,
    },
    SessionTurn {
        agent_job_id: String,
        turn: SessionTurn,
    },
    #[serde(other)]
    Other,
}

/// Mirrors a subset of the backend's Task shape — this panel only shows
/// enough to link out and identify what's active.
#[derive(Clone, Debug, Deserialize)]
pub struct BriefTask {
    pub id: String,
    pub title: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BriefInvestigation {
    pub id: String,
    pub title: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BriefAgentJob {
    pub id: String,
    pub goal: String,
    pub status: String,
    pub actor_id: String,
    #[serde(default)]
    pub claimed_by_actor_id: Option<String>,
}

/// GET /projects/:id/context — the "walk in cold" shared-memory entrypoint.
/// Unknown fields (project, recent_events — already covered by the feed) are
/// dropped by serde since this struct doesn't set deny_unknown_fields.
#[derive(Clone, Debug, Deserialize)]
pub struct ProjectContext {
    #[serde(default)]
    pub open_tasks: Vec<BriefTask>,
    #[serde(default)]
    pub active_investigations: Vec<BriefInvestigation>,
    #[serde(default)]
    pub active_agent_jobs: Vec<BriefAgentJob>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SearchResultEntry {
    pub entity_type: String,
    pub entity_id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    pub snippet: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResultEntry>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SimilarWorkMatch {
    pub title: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SimilarWorkResponse {
    pub matches: Vec<SimilarWorkMatch>,
}

/// A create request that's on hold because something similar already
/// exists — the create request itself (url + body) is captured here since
/// the composer that produced it is already cleared by the time this
/// shows, matching how every other submit_* in this panel works (clear
/// immediately, then fire the request).
#[derive(Clone)]
struct PendingDuplicate {
    label: String,
    matches: Vec<SimilarWorkMatch>,
    create_url: String,
    create_body: serde_json::Value,
}

/// Which internal view the panel is showing below the tab strip. Splitting
/// these into real switchable views (rather than stacking every section
/// vertically) is what makes the panel feel like its own product surface
/// instead of one long scrolling sidebar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PanelView {
    Feed,
    Tasks,
    Investigations,
    Decisions,
    AgentJobs,
    Projects,
}

/// Mirrors the backend's `Project` row, trimmed to what the switcher
/// needs — extra fields (created_at, the encrypted BYOK key columns, etc.)
/// are simply ignored by serde since this doesn't set deny_unknown_fields.
#[derive(Clone, Debug, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoginMode {
    Login,
    Signup,
}

pub struct ProjectBrainPanel {
    /// Held so "Connect an agent" can launch an ACP agent session in this
    /// same Zed window (via the real `AgentPanel`'s own connection store,
    /// so the session shows up there like any other agent thread) instead
    /// of only handing out a copy-paste env block.
    workspace: WeakEntity<Workspace>,
    /// Tracks the most recent in-app agent launch's connection state so the
    /// panel's status line reflects success/failure instead of freezing on
    /// "Launching…" regardless of outcome.
    _agent_connection_subscription: Option<Subscription>,
    focus_handle: FocusHandle,
    position: DockPosition,
    active_view: PanelView,
    feed_events: Vec<FeedEvent>,
    presence: Vec<PresenceEntry>,
    /// Fetched from `GET /projects/:id/actors` — resolves actor ids to
    /// display names wherever the panel renders one.
    actors: Vec<ActorInfo>,
    connection_status: ConnectionStatus,
    brief: Option<ProjectContext>,
    /// The agent job whose steering-message composer is currently expanded,
    /// if any.
    expanded_job_id: Option<String>,
    /// The agent job whose live transcript is currently expanded, if any.
    expanded_transcript_job_id: Option<String>,
    /// Session turns fetched/streamed per agent job id.
    job_turns: std::collections::HashMap<String, Vec<SessionTurn>>,
    steering_editor: Entity<Editor>,
    action_status: Option<String>,
    /// A create request held back pending "yes, create it anyway" —
    /// populated when a task/investigation/agent job looks like it might
    /// duplicate something that already exists.
    pending_duplicate: Option<PendingDuplicate>,
    search_editor: Entity<Editor>,
    search_results: Vec<SearchResultEntry>,
    search_error: Option<String>,
    creating_task: bool,
    new_task_title_editor: Entity<Editor>,
    new_task_description_editor: Entity<Editor>,
    creating_investigation: bool,
    new_investigation_title_editor: Entity<Editor>,
    creating_decision: bool,
    new_decision_title_editor: Entity<Editor>,
    new_decision_context_editor: Entity<Editor>,
    new_decision_text_editor: Entity<Editor>,
    new_decision_consequences_editor: Entity<Editor>,
    creating_agent_job: bool,
    new_agent_job_goal_editor: Entity<Editor>,
    /// Who this session acts as. Seeded from `RALLY_ACTOR_ID` if set;
    /// otherwise populated by the onboarding "Create Actor" form.
    actor_id: Option<String>,
    /// Only known when this session created the actor itself (or it was
    /// passed via `RALLY_ACTOR_TOKEN`) — the backend never returns a token
    /// after the actor is created, so there's no way to recover it later.
    actor_token: Option<String>,
    onboarding_name_editor: Entity<Editor>,
    /// Rally account session — set once the login/signup form below
    /// succeeds. Distinct from `actor_id`/`actor_token`: this identifies the
    /// *person*, which then resolves to an actor for whichever project is
    /// open (`RALLY_PROJECT_ID`) via `/projects/:id/members`, mirroring
    /// exactly what `rally_frontend`'s dashboard does on login.
    session_token: Option<String>,
    login_mode: LoginMode,
    login_email_editor: Entity<Editor>,
    login_password_editor: Entity<Editor>,
    logging_in: bool,
    login_error: Option<String>,
    /// Connecting an agent is a follow-up action a signed-in human takes,
    /// not a parallel path through the same "create actor" form — an agent
    /// doesn't decide to log in, a human provisions it. Separate state from
    /// onboarding_name_editor on purpose.
    connecting_agent: bool,
    new_agent_name_editor: Entity<Editor>,
    /// The env-var block to hand to whatever will act as the agent, once
    /// created. Shown once, same as a token — the backend never returns it
    /// again after this.
    connected_agent: Option<ConnectedAgentInfo>,
    _ws_task: Option<Task<()>>,
    /// The project this session is currently scoped to — seeded from
    /// `RALLY_PROJECT_ID` at construction, then mutable via the Projects
    /// tab's switcher. Kept as a field (rather than re-reading the env var
    /// everywhere) so switching projects can actually change it.
    project_id: Option<String>,
    my_projects: Vec<ProjectSummary>,
    loading_projects: bool,
    projects_error: Option<String>,
    new_project_name_editor: Entity<Editor>,
    creating_project: bool,
}

impl ProjectBrainPanel {
    pub fn new(workspace: &Workspace, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let steering_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Steering message…", window, cx);
            editor
        });
        let search_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Search project memory…", window, cx);
            editor
        });
        let new_task_title_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Task title…", window, cx);
            editor
        });
        let new_task_description_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Description (optional)…", window, cx);
            editor
        });
        let login_email_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Email…", window, cx);
            editor
        });
        let login_password_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Password…", window, cx);
            editor
        });
        let onboarding_name_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Your name…", window, cx);
            editor
        });
        let new_agent_name_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("e.g. Claude Code, CI Bot…", window, cx);
            editor
        });
        let new_investigation_title_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Investigation title…", window, cx);
            editor
        });
        let new_decision_title_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Decision title…", window, cx);
            editor
        });
        let new_decision_context_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Context (optional)…", window, cx);
            editor
        });
        let new_decision_text_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("The decision itself…", window, cx);
            editor
        });
        let new_decision_consequences_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Consequences (optional)…", window, cx);
            editor
        });
        let new_agent_job_goal_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("What should this agent job do?", window, cx);
            editor
        });
        let new_project_name_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("New project name…", window, cx);
            editor
        });

        let mut this = Self {
            workspace: workspace.weak_handle(),
            _agent_connection_subscription: None,
            focus_handle: cx.focus_handle(),
            position: DockPosition::Right,
            active_view: PanelView::Feed,
            feed_events: Vec::new(),
            presence: Vec::new(),
            actors: Vec::new(),
            connection_status: ConnectionStatus::Connecting,
            brief: None,
            expanded_job_id: None,
            expanded_transcript_job_id: None,
            job_turns: std::collections::HashMap::new(),
            steering_editor,
            action_status: None,
            pending_duplicate: None,
            search_editor,
            search_results: Vec::new(),
            search_error: None,
            creating_task: false,
            new_task_title_editor,
            new_task_description_editor,
            creating_investigation: false,
            new_investigation_title_editor,
            creating_decision: false,
            new_decision_title_editor,
            new_decision_context_editor,
            new_decision_text_editor,
            new_decision_consequences_editor,
            creating_agent_job: false,
            new_agent_job_goal_editor,
            actor_id: get_actor_id(),
            actor_token: std::env::var("RALLY_ACTOR_TOKEN").ok(),
            onboarding_name_editor,
            session_token: None,
            login_mode: LoginMode::Login,
            login_email_editor,
            login_password_editor,
            logging_in: false,
            login_error: None,
            connecting_agent: false,
            new_agent_name_editor,
            connected_agent: None,
            _ws_task: None,
            project_id: get_project_id(),
            my_projects: Vec::new(),
            loading_projects: false,
            projects_error: None,
            new_project_name_editor,
            creating_project: false,
        };

        RallyLiveState::global(cx).update(cx, |state, cx| {
            state.set_actor_identity(this.actor_id.clone(), this.actor_token.clone(), cx)
        });
        this.start_background_sync(cx);
        this
    }

    fn start_background_sync(&mut self, cx: &mut Context<Self>) {
        let project_id = self.project_id.clone();
        let session_token = self.session_token.clone();
        let task = cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            // 1. Fetch initial feed history (run on the Tokio runtime, since
            //    reqwest needs a real Tokio reactor for DNS/networking).
            let Some(project_id) = project_id else {
                log::error!("No project selected. Project Brain cannot connect.");
                return;
            };

            let feed_url = format!(
                "{}/projects/{}/feed?limit=50",
                backend_base_url(), project_id
            );
            let feed_result = tokio_runtime()
                .spawn(async move {
                    let client = reqwest::Client::new();
                    let res = client.get(&feed_url).send().await?;
                    res.json::<FeedResponse>().await
                })
                .await;

            if let Ok(Ok(feed_data)) = feed_result {
                let _ = this.update(cx, |panel, cx| {
                    panel.feed_events = feed_data.events.clone();
                    RallyLiveState::global(cx)
                        .update(cx, |state, cx| state.set_feed_events(feed_data.events, cx));
                    cx.notify();
                });
            }

            // 1b. Fetch the shared-memory bootstrap view — open tasks,
            //     active investigations, active agent jobs.
            let context_result = tokio_runtime()
                .spawn(fetch_project_context(project_id.clone()))
                .await;
            if let Ok(Ok(context)) = context_result {
                let _ = this.update(cx, |panel, cx| {
                    panel.brief = Some(context);
                    cx.notify();
                });
            }

            // 1c. Fetch the actor roster, so ids elsewhere can be shown as
            //     names.
            let actors_result = tokio_runtime()
                .spawn(fetch_actors(project_id.clone(), session_token.clone()))
                .await;
            if let Ok(Ok(actors)) = actors_result {
                let _ = this.update(cx, |panel, cx| {
                    panel.actors = actors.clone();
                    RallyLiveState::global(cx).update(cx, |state, cx| state.set_actors(actors, cx));
                    cx.notify();
                });
            }

            // 2. Connect to WebSocket stream & reconnect loop
            let ws_url = format!("{}/projects/{}/live", backend_ws_url(), project_id);
            loop {
                let _ = this.update(cx, |panel, cx| {
                    panel.connection_status = ConnectionStatus::Connecting;
                    RallyLiveState::global(cx)
                        .update(cx, |state, cx| state.set_connection_status(ConnectionStatus::Connecting, cx));
                    cx.notify();
                });

                // async-tungstenite's own auto-load-on-None-connector path
                // requires an explicit connector regardless of which
                // "tokio-rustls-*" feature is selected here, once features
                // unify with the rest of the workspace (client/collab/repl
                // all select tokio-rustls-manual-roots) — so build one
                // explicitly, the same way client's proxy.rs does, rather
                // than relying on the crate to load roots for us.
                let ws_url_clone = ws_url.clone();
                let connect_result = tokio_runtime()
                    .spawn(async move {
                        // async-tungstenite's `Connector` is a type alias
                        // for `tokio_rustls::TlsConnector` under the
                        // rustls-backed features — no enum variant to wrap.
                        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(
                            http_client_tls::tls_config(),
                        ));
                        async_tungstenite::tokio::connect_async_with_tls_connector(
                            &ws_url_clone,
                            Some(connector),
                        )
                        .await
                    })
                    .await;

                match connect_result {
                    Ok(Ok((ws_stream, _))) => {
                        let _ = this.update(cx, |panel, cx| {
                            panel.connection_status = ConnectionStatus::Connected;
                            RallyLiveState::global(cx)
                                .update(cx, |state, cx| state.set_connection_status(ConnectionStatus::Connected, cx));
                            cx.notify();
                        });

                        let (_, mut read) = ws_stream.split();

                        while let Some(msg_res) = read.next().await {
                            let msg = match msg_res {
                                Ok(m) => m,
                                Err(_) => break,
                            };

                            if let async_tungstenite::tungstenite::Message::Text(text) = msg {
                                if let Ok(live_msg) = serde_json::from_str::<LiveMessage>(&text) {
                                    let _ = this.update(cx, |panel, cx| {
                                        panel.handle_live_message(live_msg, cx);
                                    });
                                } else if let Ok(event) = serde_json::from_str::<FeedEvent>(&text) {
                                    let _ = this.update(cx, |panel, cx| {
                                        panel.push_feed_event(event, cx);
                                    });
                                }
                            }
                        }
                    }
                    Ok(Err(err)) => {
                        log::error!("WebSocket connection failed to {}: {err:#}", ws_url);
                    }
                    Err(join_err) => {
                        log::error!("Tokio task panicked while connecting websocket: {join_err:#}");
                    }
                }

                let _ = this.update(cx, |panel, cx| {
                    panel.connection_status = ConnectionStatus::Disconnected;
                    RallyLiveState::global(cx)
                        .update(cx, |state, cx| state.set_connection_status(ConnectionStatus::Disconnected, cx));
                    cx.notify();
                });

                // Wait 3 seconds before attempting reconnect
                cx.background_executor().timer(Duration::from_secs(3)).await;
            }
        });

        self._ws_task = Some(task);
    }

    fn handle_live_message(&mut self, msg: LiveMessage, cx: &mut Context<Self>) {
        match msg {
            LiveMessage::PresenceUpdate { actors } => {
                // The backend always broadcasts the full current snapshot
                // (see PresenceStore::snapshot_project), so a plain replace
                // is correct — anyone missing from the list is offline.
                self.presence = actors.clone();
                RallyLiveState::global(cx).update(cx, |state, cx| state.set_presence(actors, cx));
            }
            LiveMessage::MemoryEvent { event } => {
                self.push_feed_event(event, cx);
                // The brief aggregates tasks/investigations/agent jobs, so
                // any event landing could change what it should show.
                self.refresh_project_context(cx);
            }
            LiveMessage::AgentJobUpdate { job } => {
                self.refresh_project_context(cx);
                let event_id = format!("gen_{}", EVENT_COUNTER.fetch_add(1, Ordering::SeqCst));
                self.push_feed_event(
                    FeedEvent {
                        id: event_id,
                        project_id: self.project_id.clone(),
                        actor_id: job.claimed_by_actor_id.clone(),
                        entity_type: Some("agent_job".to_string()),
                        entity_id: Some(job.id.clone()),
                        verb: Some(job.status.clone()),
                        summary: job.goal.clone(),
                        created_at: Some(Utc::now()),
                    },
                    cx,
                );
            }
            LiveMessage::SessionTurn { agent_job_id, turn } => {
                self.job_turns.entry(agent_job_id).or_default().push(turn);
            }
            LiveMessage::Other => {}
        }
        cx.notify();
    }

    fn push_feed_event(&mut self, mut event: FeedEvent, cx: &mut Context<Self>) {
        if event.id.is_empty() {
            event.id = format!("gen_{}", EVENT_COUNTER.fetch_add(1, Ordering::SeqCst));
        }
        if !self.feed_events.iter().any(|e| e.id == event.id) {
            self.feed_events.insert(0, event.clone());
            RallyLiveState::global(cx).update(cx, |state, cx| state.push_feed_event(event, cx));
            cx.notify();
        }
    }

    fn refresh_project_context(&mut self, cx: &mut Context<Self>) {
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(fetch_project_context(project_id)).await;
            if let Ok(Ok(context)) = result {
                let _ = this.update(cx, |panel, cx| {
                    panel.brief = Some(context);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn claim_job(&mut self, job_id: String, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to claim jobs".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let url = format!("{}/agent-jobs/{}/claim", backend_base_url(), job_id);
        let body = serde_json::json!({ "actor_id": actor_id });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body, token)).await;
            let status = match result {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(format!("Claim failed: {err:#}")),
                Err(err) => Some(format!("Claim failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
    }

    fn toggle_job_message(&mut self, job_id: String, window: &mut Window, cx: &mut Context<Self>) {
        if self.expanded_job_id.as_deref() == Some(job_id.as_str()) {
            self.expanded_job_id = None;
        } else {
            self.expanded_job_id = Some(job_id);
            self.steering_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.steering_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn submit_steering_message(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(job_id) = self.expanded_job_id.clone() else {
            return;
        };
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to send steering messages".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let content = self
            .steering_editor
            .update(cx, |editor, cx| {
                let text = editor.text(cx);
                editor.clear(window, cx);
                text
            })
            .trim()
            .to_string();
        if content.is_empty() {
            return;
        }
        self.expanded_job_id = None;

        let url = format!("{}/agent-jobs/{}/messages", backend_base_url(), job_id);
        let body = serde_json::json!({ "actor_id": actor_id, "content": content });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body, token)).await;
            let status = match result {
                Ok(Ok(())) => Some("Steering message sent".to_string()),
                Ok(Err(err)) => Some(format!("Send failed: {err:#}")),
                Err(err) => Some(format!("Send failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
    }

    fn confirm(&mut self, _: &menu::Confirm, window: &mut Window, cx: &mut Context<Self>) {
        if self.expanded_job_id.is_some() {
            self.submit_steering_message(window, cx);
        } else if self.creating_task {
            self.submit_new_task(window, cx);
        } else {
            self.run_search(cx);
        }
    }

    fn run_search(&mut self, cx: &mut Context<Self>) {
        let query = self.search_editor.read(cx).text(cx).trim().to_string();
        if query.is_empty() {
            self.search_results.clear();
            self.search_error = None;
            cx.notify();
            return;
        }
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(fetch_search(project_id, query)).await;
            let _ = this.update(cx, |panel, cx| {
                match result {
                    Ok(Ok(response)) => {
                        panel.search_results = response.results;
                        panel.search_error = None;
                    }
                    Ok(Err(err)) => {
                        panel.search_error = Some(format!("Search failed: {err:#}"));
                    }
                    Err(err) => {
                        panel.search_error = Some(format!("Search failed: {err:#}"));
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn toggle_create_task_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.creating_task = !self.creating_task;
        if self.creating_task {
            self.new_task_title_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.new_task_description_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.new_task_title_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn submit_new_task(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to create tasks".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let title = self.new_task_title_editor.read(cx).text(cx).trim().to_string();
        if title.is_empty() {
            return;
        }
        let description = self
            .new_task_description_editor
            .read(cx)
            .text(cx)
            .trim()
            .to_string();

        self.new_task_title_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        self.new_task_description_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        self.creating_task = false;

        let url = format!("{}/projects/{}/tasks", backend_base_url(), project_id);
        let body = serde_json::json!({
            "actor_id": actor_id,
            "title": title.clone(),
            "description": description,
        });
        self.check_then_create("task", title, url, body, token, cx);
    }

    /// Checks for existing work that looks like `label` before actually
    /// creating anything. If something similar turns up, the create request
    /// is held (`pending_duplicate`) rather than fired — the composer that
    /// produced it has already been cleared by the caller, matching every
    /// other submit_* in this panel (clear immediately, then fire), so the
    /// request itself carries everything needed to finish the job later.
    fn check_then_create(
        &mut self,
        entity_type: &'static str,
        label: String,
        create_url: String,
        create_body: serde_json::Value,
        token: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let check_text = label.clone();
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let similar = tokio_runtime()
                .spawn(fetch_similar_work(project_id, entity_type, check_text))
                .await;
            // If the check itself fails (network hiccup, panic), fail open
            // rather than block creation on a broken similarity check.
            let matches = match similar {
                Ok(Ok(matches)) => matches,
                _ => Vec::new(),
            };

            if matches.is_empty() {
                let result = tokio_runtime().spawn(post_json(create_url, create_body, token)).await;
                let status = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(err)) => Some(format!("Create failed: {err:#}")),
                    Err(err) => Some(format!("Create failed: {err:#}")),
                };
                let _ = this.update(cx, |panel, cx| {
                    panel.action_status = status;
                    panel.refresh_project_context(cx);
                });
            } else {
                let _ = this.update(cx, |panel, cx| {
                    panel.pending_duplicate = Some(PendingDuplicate {
                        label,
                        matches,
                        create_url,
                        create_body,
                    });
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn confirm_pending_duplicate(&mut self, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_duplicate.take() else {
            return;
        };
        let token = self.actor_token.clone();
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime()
                .spawn(post_json(pending.create_url, pending.create_body, token))
                .await;
            let status = match result {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(format!("Create failed: {err:#}")),
                Err(err) => Some(format!("Create failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
        cx.notify();
    }

    fn cancel_pending_duplicate(&mut self, cx: &mut Context<Self>) {
        self.pending_duplicate = None;
        cx.notify();
    }

    /// Cycles a task through open -> in_progress -> blocked -> done -> open.
    /// GPUI has no native <select>, so a click-to-advance button is the
    /// simplest inline status control that fits this panel's width.
    fn cycle_task_status(&mut self, task_id: String, current_status: String, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to update tasks".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let next = next_task_status(&current_status);
        let url = format!("{}/tasks/{}", backend_base_url(), task_id);
        let body = serde_json::json!({ "actor_id": actor_id, "status": next });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(patch_json(url, body, token)).await;
            let status = match result {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(format!("Update task failed: {err:#}")),
                Err(err) => Some(format!("Update task failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
    }

    fn toggle_create_investigation_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.creating_investigation = !self.creating_investigation;
        if self.creating_investigation {
            self.new_investigation_title_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.new_investigation_title_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn submit_new_investigation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to open investigations".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let title = self
            .new_investigation_title_editor
            .read(cx)
            .text(cx)
            .trim()
            .to_string();
        if title.is_empty() {
            return;
        }
        self.new_investigation_title_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        self.creating_investigation = false;

        let url = format!("{}/projects/{}/investigations", backend_base_url(), project_id);
        let body = serde_json::json!({ "actor_id": actor_id, "title": title.clone() });
        self.check_then_create("investigation", title, url, body, token, cx);
    }

    fn resolve_investigation(&mut self, investigation_id: String, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to resolve investigations".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let url = format!("{}/investigations/{}", backend_base_url(), investigation_id);
        let body = serde_json::json!({ "actor_id": actor_id, "status": "resolved" });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(patch_json(url, body, token)).await;
            let status = match result {
                Ok(Ok(())) => Some("Investigation resolved".to_string()),
                Ok(Err(err)) => Some(format!("Resolve failed: {err:#}")),
                Err(err) => Some(format!("Resolve failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
    }

    fn toggle_create_decision_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.creating_decision = !self.creating_decision;
        if self.creating_decision {
            for editor in [
                &self.new_decision_title_editor,
                &self.new_decision_context_editor,
                &self.new_decision_text_editor,
                &self.new_decision_consequences_editor,
            ] {
                editor.update(cx, |editor, cx| editor.clear(window, cx));
            }
            self.new_decision_title_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn submit_new_decision(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to record decisions".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let title = self.new_decision_title_editor.read(cx).text(cx).trim().to_string();
        if title.is_empty() {
            return;
        }
        let context_text = self.new_decision_context_editor.read(cx).text(cx).trim().to_string();
        let decision_text = self.new_decision_text_editor.read(cx).text(cx).trim().to_string();
        let consequences = self
            .new_decision_consequences_editor
            .read(cx)
            .text(cx)
            .trim()
            .to_string();

        for editor in [
            &self.new_decision_title_editor,
            &self.new_decision_context_editor,
            &self.new_decision_text_editor,
            &self.new_decision_consequences_editor,
        ] {
            editor.update(cx, |editor, cx| editor.clear(window, cx));
        }
        self.creating_decision = false;

        let url = format!("{}/projects/{}/decisions", backend_base_url(), project_id);
        let body = serde_json::json!({
            "actor_id": actor_id,
            "title": title,
            "context": context_text,
            "decision": decision_text,
            "consequences": consequences,
        });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body, token)).await;
            let status = match result {
                Ok(Ok(())) => Some("Decision recorded".to_string()),
                Ok(Err(err)) => Some(format!("Record decision failed: {err:#}")),
                Err(err) => Some(format!("Record decision failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
    }

    fn toggle_create_agent_job_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.creating_agent_job = !self.creating_agent_job;
        if self.creating_agent_job {
            self.new_agent_job_goal_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.new_agent_job_goal_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn submit_new_agent_job(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to create agent jobs".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let goal = self.new_agent_job_goal_editor.read(cx).text(cx).trim().to_string();
        if goal.is_empty() {
            return;
        }
        self.new_agent_job_goal_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        self.creating_agent_job = false;

        let url = format!("{}/projects/{}/agent-jobs", backend_base_url(), project_id);
        let body = serde_json::json!({ "actor_id": actor_id, "goal": goal.clone() });
        self.check_then_create("agent_job", goal, url, body, token, cx);
    }

    /// Invokes the headless runtime directly — the backend calls Anthropic
    /// and runs the tool-use loop itself, unlike `submit_steering_message`
    /// which just leaves a note. Requires the active actor to be the job's
    /// current claimant.
    fn run_turn(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(job_id) = self.expanded_job_id.clone() else {
            return;
        };
        let Some(actor_id) = self.actor_id.clone() else {
            self.action_status = Some("Create an actor above to run the headless agent".into());
            cx.notify();
            return;
        };
        let token = self.actor_token.clone();
        let content = self
            .steering_editor
            .update(cx, |editor, cx| {
                let text = editor.text(cx);
                editor.clear(window, cx);
                text
            })
            .trim()
            .to_string();
        if content.is_empty() {
            return;
        }
        self.expanded_job_id = None;
        self.expanded_transcript_job_id = Some(job_id.clone());

        let url = format!("{}/agent-jobs/{}/turns", backend_base_url(), job_id);
        let body = serde_json::json!({ "actor_id": actor_id, "content": content });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body, token)).await;
            let status = match result {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(format!("Run failed: {err:#}")),
                Err(err) => Some(format!("Run failed: {err:#}")),
            };
            let turns_result = tokio_runtime().spawn(fetch_turns(job_id.clone())).await;
            let _ = this.update(cx, |panel, cx| {
                if status.is_some() {
                    panel.action_status = status;
                }
                if let Ok(Ok(turns)) = turns_result {
                    panel.job_turns.insert(job_id, turns);
                }
                panel.refresh_project_context(cx);
                cx.notify();
            });
        })
        .detach();
    }

    fn toggle_transcript(&mut self, job_id: String, cx: &mut Context<Self>) {
        if self.expanded_transcript_job_id.as_deref() == Some(job_id.as_str()) {
            self.expanded_transcript_job_id = None;
            cx.notify();
            return;
        }
        self.expanded_transcript_job_id = Some(job_id.clone());
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(fetch_turns(job_id.clone())).await;
            if let Ok(Ok(turns)) = result {
                let _ = this.update(cx, |panel, cx| {
                    panel.job_turns.insert(job_id, turns);
                    cx.notify();
                });
            }
        })
        .detach();
        cx.notify();
    }

    fn toggle_login_mode(&mut self, cx: &mut Context<Self>) {
        self.login_mode = match self.login_mode {
            LoginMode::Login => LoginMode::Signup,
            LoginMode::Signup => LoginMode::Login,
        };
        self.login_error = None;
        cx.notify();
    }

    /// Logs into (or signs up for) a Rally account, then immediately
    /// resolves this session's actor for whichever project is open —
    /// replacing the manual create/resume flow below for anyone with an
    /// account, the same way `rally_frontend`'s dashboard auto-provisions
    /// on login instead of sending you through its own /join page.
    fn submit_login(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let project_id = self.project_id.clone();
        let email = self.login_email_editor.read(cx).text(cx).trim().to_string();
        let password = self.login_password_editor.read(cx).text(cx);
        if email.is_empty() || password.is_empty() {
            self.login_error = Some("Enter an email and password".into());
            cx.notify();
            return;
        }
        let display_name = email.clone();
        let mode = self.login_mode;

        self.logging_in = true;
        self.login_error = None;
        self.login_password_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        cx.notify();

        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let auth_result = tokio_runtime()
                .spawn(async move {
                    match mode {
                        LoginMode::Login => login_request(email, password).await,
                        LoginMode::Signup => signup_request(email, password, display_name).await,
                    }
                })
                .await;

            let session_token = match auth_result {
                Ok(Ok(response)) => response.session_token,
                Ok(Err(err)) => {
                    let _ = this.update(cx, |panel, cx| {
                        panel.logging_in = false;
                        panel.login_error = Some(format!("{err:#}"));
                        cx.notify();
                    });
                    return;
                }
                Err(err) => {
                    let _ = this.update(cx, |panel, cx| {
                        panel.logging_in = false;
                        panel.login_error = Some(format!("{err:#}"));
                        cx.notify();
                    });
                    return;
                }
            };

            let _ = this.update(cx, |panel, cx| {
                panel.session_token = Some(session_token.clone());
                cx.notify();
            });

            if let Some(project_id) = project_id {
                let join_result = tokio_runtime()
                    .spawn(join_project_request(project_id, session_token.clone()))
                    .await;

                let _ = this.update(cx, |panel, cx| {
                    match join_result {
                        Ok(Ok(response)) => {
                            panel.actor_id = Some(response.actor.id.clone());
                            panel.actor_token = Some(response.actor_token.clone());
                            RallyLiveState::global(cx).update(cx, |state, cx| {
                                state.set_actor_identity(
                                    panel.actor_id.clone(),
                                    panel.actor_token.clone(),
                                    cx,
                                )
                            });
                            if !panel.actors.iter().any(|a| a.id == response.actor.id) {
                                panel.actors.push(ActorInfo {
                                    id: response.actor.id,
                                    kind: response.actor.kind,
                                    display_name: response.actor.display_name,
                                });
                            }
                            panel.action_status = Some("Signed in".into());
                        }
                        Ok(Err(err)) => {
                            panel.login_error = Some(format!("Signed in, but couldn't join this project: {err:#}"));
                        }
                        Err(err) => {
                            panel.login_error = Some(format!("Signed in, but couldn't join this project: {err:#}"));
                        }
                    }
                });
            } else {
                let _ = this.update(cx, |panel, _cx| {
                    panel.action_status = Some("Signed in — open the Projects tab to pick or create a project".into());
                });
            }

            let _ = this.update(cx, |panel, cx| {
                panel.logging_in = false;
                panel.refresh_my_projects(cx);
                cx.notify();
            });
        })
        .detach();
    }

    /// Always creates a human actor — this form is the panel's own
    /// onboarding, gone through by the human sitting at this session. An
    /// agent never goes through this; see submit_connect_agent below.
    fn submit_create_actor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let display_name = self
            .onboarding_name_editor
            .read(cx)
            .text(cx)
            .trim()
            .to_string();
        if display_name.is_empty() {
            self.action_status = Some("Enter a name before creating an actor".into());
            cx.notify();
            return;
        }
        self.onboarding_name_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        let session_token = self.session_token.clone();

        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime()
                .spawn(create_actor_request(
                    project_id,
                    display_name,
                    "human".to_string(),
                    session_token,
                ))
                .await;
            let _ = this.update(cx, |panel, cx| {
                match result {
                    Ok(Ok(response)) => {
                        panel.actor_id = Some(response.id.clone());
                        panel.actor_token = Some(response.token.clone());
                        RallyLiveState::global(cx).update(cx, |state, cx| {
                            state.set_actor_identity(
                                panel.actor_id.clone(),
                                panel.actor_token.clone(),
                                cx,
                            )
                        });
                        panel.actors.push(ActorInfo {
                            id: response.id,
                            kind: response.kind,
                            display_name: response.display_name,
                        });
                        cx.write_to_clipboard(ClipboardItem::new_string(response.token));
                        panel.action_status = Some(
                            "Actor created — token copied to clipboard, save it now (shown only once)"
                                .into(),
                        );
                    }
                    Ok(Err(err)) => {
                        panel.action_status = Some(format!("Create actor failed: {err:#}"));
                    }
                    Err(err) => {
                        panel.action_status = Some(format!("Create actor failed: {err:#}"));
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn toggle_connect_agent_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.connecting_agent = !self.connecting_agent;
        if self.connecting_agent {
            self.new_agent_name_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.new_agent_name_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    /// Connecting an agent is a human's follow-up action, not a parallel
    /// onboarding path — an agent doesn't sign in, it gets provisioned.
    /// The result isn't "you're in," it's a config block to hand to
    /// whatever will actually act as this agent (hooks, MCP, a CI secret).
    fn submit_connect_agent(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(project_id) = self.project_id.clone() else {
            return;
        };
        let name = self
            .new_agent_name_editor
            .read(cx)
            .text(cx)
            .trim()
            .to_string();
        if name.is_empty() {
            self.action_status = Some("Enter a name before connecting an agent".into());
            cx.notify();
            return;
        }
        self.new_agent_name_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        let session_token = self.session_token.clone();

        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime()
                .spawn(create_actor_request(
                    project_id.clone(),
                    name,
                    "agent".to_string(),
                    session_token,
                ))
                .await;
            let _ = this.update(cx, |panel, cx| {
                match result {
                    Ok(Ok(response)) => {
                        // Mosaic-style onboarding: hand over a token + one
                        // fixed CLI command, not a raw config blob the human
                        // has to know where to save. `login --agent` (added
                        // to the existing mcp-server bin) resolves the rest
                        // itself via /actors/whoami and writes .mcp.json.
                        let mcp_server_path = crate::mcp_bridge::mcp_server_path_display()
                            .unwrap_or_else(|| {
                                "<path to Rally_Backend/mcp-server/index.mjs on the agent's machine>"
                                    .to_string()
                            });
                        let command = format!("node \"{mcp_server_path}\" login --agent");
                        crate::mcp_bridge::register_rally_context_server(
                            cx,
                            backend_base_url(),
                            project_id.clone(),
                            response.id.clone(),
                            response.token.clone(),
                        );
                        let agent_info = ConnectedAgentInfo {
                            display_name: response.display_name.clone(),
                            token: response.token.clone(),
                            expires_at: response.token_expires_at,
                            command,
                        };
                        panel.actors.push(ActorInfo {
                            id: response.id,
                            kind: response.kind,
                            display_name: response.display_name,
                        });
                        cx.write_to_clipboard(ClipboardItem::new_string(agent_info.token.clone()));
                        panel.connected_agent = Some(agent_info);
                        panel.connecting_agent = false;
                        panel.action_status = Some(
                            "Agent connected — token copied to clipboard, shown only once".into(),
                        );
                    }
                    Ok(Err(err)) => {
                        panel.action_status = Some(format!("Connect agent failed: {err:#}"));
                    }
                    Err(err) => {
                        panel.action_status = Some(format!("Connect agent failed: {err:#}"));
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    /// ACP agent servers already configured in this Zed instance
    /// (`agent_id`, `display_name`) — whatever `Rally MCP` credentials were
    /// just registered in Step 1 apply automatically to any of these once
    /// launched, since `mcp_servers_for_project` reads from the same
    /// project-wide context-server settings regardless of which agent
    /// connects.
    fn available_agent_servers(&self, cx: &App) -> Vec<(String, String)> {
        let Some(workspace) = self.workspace.upgrade() else {
            return Vec::new();
        };
        let project = workspace.read(cx).project().clone();
        let store = project.read(cx).agent_server_store().read(cx);
        store
            .external_agents()
            .map(|id| {
                let name = store
                    .agent_display_name(id)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| id.0.to_string());
                (id.0.to_string(), name)
            })
            .collect()
    }

    /// Starts a live ACP session with `agent_id` in this same Zed window,
    /// via the real `AgentPanel`'s own `AgentConnectionStore` so the
    /// resulting thread shows up there like any other agent session — no
    /// copy-paste, no second process.
    fn launch_agent_in_zed(&mut self, agent_id: String, window: &mut Window, cx: &mut Context<Self>) {
        let Some(workspace) = self.workspace.upgrade() else {
            self.action_status = Some("No workspace available to launch an agent in".into());
            cx.notify();
            return;
        };
        let Some(agent_panel) = workspace.read(cx).panel::<agent_ui::AgentPanel>(cx) else {
            self.action_status =
                Some("Open Zed's Agent panel once first, then retry connecting".into());
            cx.notify();
            return;
        };

        let connection_store = agent_panel.read(cx).connection_store().clone();
        let key = agent_ui::Agent::Custom {
            id: project::agent_server_store::AgentId::new(agent_id.clone()),
        };
        let server: std::rc::Rc<dyn agent_servers::AgentServer> = std::rc::Rc::new(
            agent_servers::CustomAgentServer::new(project::agent_server_store::AgentId::new(
                agent_id,
            )),
        );
        // `restart_connection` (not `request_connection`) so re-clicking
        // "Launch" after a failed/stale attempt actually retries instead of
        // silently reattaching to a dead entry forever.
        let entry = connection_store.update(cx, |store, cx| {
            store.restart_connection(key, server, cx)
        });

        workspace.update(cx, |workspace, cx| {
            workspace.focus_panel::<agent_ui::AgentPanel>(window, cx);
        });

        self.action_status = Some("Launching agent in Zed…".into());
        self._agent_connection_subscription = Some(cx.observe(&entry, |this, entry, cx| {
            use agent_ui::agent_connection_store::AgentConnectionEntry;
            this.action_status = Some(match entry.read(cx) {
                AgentConnectionEntry::Connecting { .. } => {
                    "Agent connecting in Zed…".to_string()
                }
                AgentConnectionEntry::Connected(_) => {
                    "Agent connected in Zed — see the Agent panel".to_string()
                }
                AgentConnectionEntry::Error { error } => {
                    format!("Agent launch failed: {error}")
                }
            });
            cx.notify();
        }));
        cx.notify();
    }

    fn display_name_for(&self, actor_id: &str) -> String {
        self.actors
            .iter()
            .find(|a| a.id == actor_id)
            .map(|a| a.display_name.clone())
            .unwrap_or_else(|| actor_id.chars().take(8).collect())
    }

    fn set_active_view(&mut self, view: PanelView, cx: &mut Context<Self>) {
        self.active_view = view;
        if view == PanelView::Projects && self.session_token.is_some() {
            self.refresh_my_projects(cx);
        }
        cx.notify();
    }

    fn refresh_my_projects(&mut self, cx: &mut Context<Self>) {
        let Some(session_token) = self.session_token.clone() else {
            return;
        };
        self.loading_projects = true;
        self.projects_error = None;
        cx.notify();
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime()
                .spawn(list_my_projects_request(session_token))
                .await;
            let _ = this.update(cx, |panel, cx| {
                panel.loading_projects = false;
                match result {
                    Ok(Ok(projects)) => {
                        panel.my_projects = projects;
                        panel.projects_error = None;
                    }
                    Ok(Err(err)) => {
                        panel.projects_error = Some(format!("{err:#}"));
                    }
                    Err(err) => {
                        panel.projects_error = Some(format!("{err:#}"));
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn toggle_create_project_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.creating_project = !self.creating_project;
        if self.creating_project {
            self.new_project_name_editor
                .update(cx, |editor, cx| editor.clear(window, cx));
            self.new_project_name_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn submit_create_project(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(session_token) = self.session_token.clone() else {
            return;
        };
        let name = self.new_project_name_editor.read(cx).text(cx).trim().to_string();
        if name.is_empty() {
            return;
        }
        self.new_project_name_editor
            .update(cx, |editor, cx| editor.clear(window, cx));
        self.creating_project = false;
        self.loading_projects = true;
        cx.notify();

        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime()
                .spawn(create_project_request(session_token, name))
                .await;
            match result {
                Ok(Ok(created)) => {
                    let _ = this.update(cx, |panel, cx| {
                        panel.switch_to_project(
                            created.project.id,
                            created.project.name,
                            Some((created.actor.id, created.actor.kind, created.actor.display_name, created.actor_token)),
                            cx,
                        );
                    });
                }
                Ok(Err(err)) => {
                    let _ = this.update(cx, |panel, cx| {
                        panel.loading_projects = false;
                        panel.projects_error = Some(format!("Create project failed: {err:#}"));
                        cx.notify();
                    });
                }
                Err(err) => {
                    let _ = this.update(cx, |panel, cx| {
                        panel.loading_projects = false;
                        panel.projects_error = Some(format!("Create project failed: {err:#}"));
                        cx.notify();
                    });
                }
            }
        })
        .detach();
    }

    /// Tears down the panel's whole live connection for the current project
    /// and rebuilds it scoped to `new_project_id` — a project switch isn't
    /// just changing an id, it's a full reconnect. `provisioned_actor`, when
    /// given (project just created via `submit_create_project`), skips the
    /// `join_project_request` round-trip since the backend already handed
    /// back this session's actor for the new project.
    fn switch_to_project(
        &mut self,
        new_project_id: String,
        new_project_name: String,
        provisioned_actor: Option<(String, String, String, String)>,
        cx: &mut Context<Self>,
    ) {
        // Cancels the running websocket connect/reconnect loop for the old
        // project (dropping a GPUI Task cancels it).
        self._ws_task = None;

        self.feed_events.clear();
        self.presence.clear();
        self.actors.clear();
        self.connection_status = ConnectionStatus::Connecting;
        self.brief = None;
        self.expanded_job_id = None;
        self.expanded_transcript_job_id = None;
        self.job_turns.clear();
        self.pending_duplicate = None;
        self.search_results.clear();
        self.search_error = None;
        self.actor_id = None;
        self.actor_token = None;
        self.connected_agent = None;
        self.action_status = Some(format!("Switched to {new_project_name}"));

        self.project_id = Some(new_project_id.clone());

        if let Some((actor_id, actor_kind, actor_display_name, actor_token)) = provisioned_actor {
            self.actor_id = Some(actor_id.clone());
            self.actor_token = Some(actor_token);
            self.actors.push(ActorInfo {
                id: actor_id,
                kind: actor_kind,
                display_name: actor_display_name,
            });
            RallyLiveState::global(cx).update(cx, |state, cx| {
                state.set_actor_identity(self.actor_id.clone(), self.actor_token.clone(), cx)
            });
            self.loading_projects = false;
            self.start_background_sync(cx);
            cx.notify();
        } else if let Some(session_token) = self.session_token.clone() {
            cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let join_result = tokio_runtime()
                    .spawn(join_project_request(new_project_id, session_token))
                    .await;
                let _ = this.update(cx, |panel, cx| {
                    match join_result {
                        Ok(Ok(response)) => {
                            panel.actor_id = Some(response.actor.id.clone());
                            panel.actor_token = Some(response.actor_token.clone());
                            RallyLiveState::global(cx).update(cx, |state, cx| {
                                state.set_actor_identity(
                                    panel.actor_id.clone(),
                                    panel.actor_token.clone(),
                                    cx,
                                )
                            });
                            if !panel.actors.iter().any(|a| a.id == response.actor.id) {
                                panel.actors.push(ActorInfo {
                                    id: response.actor.id,
                                    kind: response.actor.kind,
                                    display_name: response.actor.display_name,
                                });
                            }
                        }
                        Ok(Err(err)) => {
                            panel.action_status = Some(format!("Switched project, but couldn't join it: {err:#}"));
                        }
                        Err(err) => {
                            panel.action_status = Some(format!("Switched project, but couldn't join it: {err:#}"));
                        }
                    }
                    panel.start_background_sync(cx);
                    cx.notify();
                });
            })
            .detach();
            self.loading_projects = false;
            cx.notify();
        } else {
            self.loading_projects = false;
            self.start_background_sync(cx);
            cx.notify();
        }
    }

    fn render_tab_strip(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let tabs: [(PanelView, &'static str); 6] = [
            (PanelView::Feed, "Feed"),
            (PanelView::Tasks, "Tasks"),
            (PanelView::Investigations, "Investigations"),
            (PanelView::Decisions, "Decisions"),
            (PanelView::AgentJobs, "Agent Jobs"),
            (PanelView::Projects, "Projects"),
        ];
        h_flex()
            .gap_1()
            .flex_wrap()
            .pb_1()
            .border_b_1()
            .border_color(cx.theme().colors().border)
            .children(tabs.into_iter().map(|(view, label)| {
                let is_active = self.active_view == view;
                Button::new(format!("panel-tab-{label}"), label)
                    .label_size(LabelSize::Small)
                    .color(if is_active { Color::Accent } else { Color::Muted })
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.set_active_view(view, cx);
                    }))
                    .into_any_element()
            }))
            .into_any_element()
    }

    fn render_action_status(&self) -> Option<gpui::AnyElement> {
        self.action_status.clone().map(|status| {
            Label::new(status)
                .size(LabelSize::XSmall)
                .color(Color::Accent)
                .into_any_element()
        })
    }

    fn render_pending_duplicate(&mut self, cx: &mut Context<Self>) -> Option<gpui::AnyElement> {
        let pending = self.pending_duplicate.clone()?;
        Some(
            v_flex()
                .gap_1p5()
                .p_2()
                .rounded_md()
                .border_1()
                .border_color(Color::Warning.color(cx))
                .bg(cx.theme().colors().editor_background)
                .child(
                    Label::new(format!("This might already exist — \"{}\" looks similar to:", pending.label))
                        .size(LabelSize::XSmall)
                        .color(Color::Warning),
                )
                .children(pending.matches.iter().map(|m| {
                    Label::new(format!("· {} ({})", m.title, m.status))
                        .size(LabelSize::XSmall)
                        .color(Color::Muted)
                        .into_any_element()
                }))
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            Button::new("cancel-duplicate", "Cancel")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.cancel_pending_duplicate(cx);
                                })),
                        )
                        .child(
                            Button::new("create-anyway", "Create Anyway")
                                .label_size(LabelSize::Small)
                                .color(Color::Accent)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirm_pending_duplicate(cx);
                                })),
                        ),
                )
                .into_any_element(),
        )
    }

    fn render_active_view(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        // Projects doesn't depend on `brief` (shared project memory) — it's
        // the view you use *before* a project is even selected, so it can't
        // gate on the same "brief loaded" check every other tab does.
        if self.active_view == PanelView::Projects {
            return self.render_projects_view(cx);
        }
        let Some(brief) = self.brief.clone() else {
            return Label::new("Loading shared memory…")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element();
        };
        match self.active_view {
            PanelView::Feed => self.render_feed_view(cx),
            PanelView::Tasks => self.render_tasks_view(&brief, cx),
            PanelView::Investigations => self.render_investigations_view(&brief, cx),
            PanelView::Decisions => self.render_decisions_view(cx),
            PanelView::AgentJobs => self.render_agent_jobs_view(&brief, cx),
            PanelView::Projects => unreachable!(),
        }
    }

    fn render_projects_view(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let Some(session_token) = self.session_token.clone() else {
            return Label::new(
                "Log in above to see and switch between your projects.",
            )
            .size(LabelSize::Small)
            .color(Color::Muted)
            .into_any_element();
        };
        let _ = session_token;

        let mut root = v_flex().gap_2();

        if self.loading_projects {
            root = root.child(
                Label::new("Loading projects…")
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            );
        }

        if let Some(error) = self.projects_error.clone() {
            root = root.child(
                Label::new(error)
                    .size(LabelSize::XSmall)
                    .color(Color::Error),
            );
        }

        if !self.loading_projects && self.my_projects.is_empty() && self.projects_error.is_none() {
            root = root.child(
                Label::new("No projects yet — create one below.")
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            );
        }

        root = root.child(v_flex().gap_1().children(self.my_projects.clone().into_iter().map(
            |project| {
                let is_current = self.project_id.as_deref() == Some(project.id.as_str());
                let project_id = project.id.clone();
                let project_name = project.name.clone();
                h_flex()
                    .id(SharedString::from(format!("project-row-{}", project.id)))
                    .w_full()
                    .justify_between()
                    .items_center()
                    .px_2()
                    .py_1()
                    .rounded_md()
                    .when(is_current, |this| {
                        this.bg(cx.theme().colors().element_selected)
                    })
                    .child(
                        h_flex()
                            .gap_1p5()
                            .items_center()
                            .child(Label::new(project.name.clone()).size(LabelSize::Small))
                            .when(is_current, |this| {
                                this.child(
                                    Label::new("current")
                                        .size(LabelSize::XSmall)
                                        .color(Color::Accent),
                                )
                            }),
                    )
                    .when(!is_current, |this| {
                        this.child(
                            Button::new(
                                SharedString::from(format!("switch-project-{project_id}")),
                                "Switch",
                            )
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.switch_to_project(
                                    project_id.clone(),
                                    project_name.clone(),
                                    None,
                                    cx,
                                );
                            })),
                        )
                    })
                    .into_any_element()
            },
        )));

        root = root.child(
            v_flex()
                .gap_1p5()
                .p_2()
                .rounded_md()
                .bg(cx.theme().colors().element_background)
                .child(
                    Label::new("NEW PROJECT")
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                )
                .children(if self.creating_project {
                    vec![
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.new_project_name_editor.clone())
                            .into_any_element(),
                        Button::new("submit-create-project", "Create")
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit_create_project(window, cx);
                            }))
                            .into_any_element(),
                    ]
                } else {
                    vec![
                        Button::new("toggle-create-project", "+ New Project")
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.toggle_create_project_form(window, cx);
                            }))
                            .into_any_element(),
                    ]
                }),
        );

        root.into_any_element()
    }

    fn render_feed_view(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        v_flex()
            .gap_2()
            .child(self.render_search_section(cx))
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        Label::new("PROJECT FEED").size(LabelSize::XSmall).color(Color::Muted),
                    )
                    .child(if self.feed_events.is_empty() {
                        v_flex().id("project-brain-feed-list").p_2().child(
                            Label::new("No feed events recorded yet.")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    } else {
                        v_flex()
                            .id("project-brain-feed-list")
                            .gap_2()
                            .children(self.feed_events.iter().map(|event| {
                                let relative_time = format_relative_time(event.created_at);
                                let verb_badge = event.verb.as_deref().unwrap_or("");

                                v_flex()
                                    .p_2()
                                    .rounded_md()
                                    .bg(cx.theme().colors().element_background)
                                    .gap_1()
                                    .child(
                                        h_flex().justify_between().items_center().child(
                                            Label::new(event.summary.clone()).size(LabelSize::Small),
                                        ),
                                    )
                                    .child(
                                        h_flex()
                                            .justify_between()
                                            .items_center()
                                            .when(!verb_badge.is_empty(), |this| {
                                                this.child(
                                                    Label::new(verb_badge.to_string())
                                                        .size(LabelSize::XSmall)
                                                        .color(Color::Muted),
                                                )
                                            })
                                            .child(
                                                Label::new(relative_time)
                                                    .size(LabelSize::XSmall)
                                                    .color(Color::Muted),
                                            ),
                                    )
                            }))
                    }),
            )
            .into_any_element()
    }

    fn render_tasks_view(&mut self, brief: &ProjectContext, cx: &mut Context<Self>) -> gpui::AnyElement {
        let mut root = v_flex().gap_2().child(
            h_flex()
                .justify_between()
                .items_center()
                .child(
                    Label::new(format!("{} open", brief.open_tasks.len()))
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                )
                .child(
                    Button::new("toggle-new-task", if self.creating_task { "Cancel" } else { "+ New Task" })
                        .label_size(LabelSize::Small)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.toggle_create_task_form(window, cx);
                        })),
                ),
        );

        if self.creating_task {
            root = root.child(self.render_task_composer(cx));
        }

        root = root.child(if brief.open_tasks.is_empty() {
            Label::new("No open tasks.").size(LabelSize::Small).color(Color::Muted).into_any_element()
        } else {
            v_flex()
                .gap_1p5()
                .children(brief.open_tasks.iter().map(|task| {
                    let task_id = task.id.clone();
                    let task_id_for_status = task.id.clone();
                    let status = task.status.clone();
                    v_flex()
                        .gap_1()
                        .p_2()
                        .rounded_md()
                        .bg(cx.theme().colors().editor_background)
                        .child(Label::new(task.title.clone()).size(LabelSize::Small).truncate())
                        .child(
                            Button::new(format!("cycle-status-{task_id}"), status.replace('_', " "))
                                .label_size(LabelSize::XSmall)
                                .color(status_color(&status))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.cycle_task_status(
                                        task_id_for_status.clone(),
                                        status.clone(),
                                        cx,
                                    );
                                })),
                        )
                        .into_any_element()
                }))
                .into_any_element()
        });

        root.into_any_element()
    }

    fn render_investigations_view(
        &mut self,
        brief: &ProjectContext,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        let mut root = v_flex()
            .gap_2()
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        Label::new(format!("{} active", brief.active_investigations.len()))
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                    )
                    .child(
                        Button::new(
                            "toggle-new-investigation",
                            if self.creating_investigation { "Cancel" } else { "+ New Investigation" },
                        )
                        .label_size(LabelSize::Small)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.toggle_create_investigation_form(window, cx);
                        })),
                    ),
            )
            .child(
                Label::new(
                    "Only open investigations are listed — there's no index of resolved ones yet.",
                )
                .size(LabelSize::XSmall)
                .color(Color::Muted),
            );

        if self.creating_investigation {
            root = root.child(
                v_flex()
                    .gap_1p5()
                    .p_2()
                    .rounded_md()
                    .bg(cx.theme().colors().editor_background)
                    .child(
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.new_investigation_title_editor.clone()),
                    )
                    .child(
                        Button::new("create-investigation", "Create")
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit_new_investigation(window, cx);
                            })),
                    ),
            );
        }

        root = root.child(if brief.active_investigations.is_empty() {
            Label::new("No open investigations.")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element()
        } else {
            v_flex()
                .gap_1p5()
                .children(brief.active_investigations.iter().map(|investigation| {
                    let investigation_id = investigation.id.clone();
                    h_flex()
                        .justify_between()
                        .items_center()
                        .gap_2()
                        .p_2()
                        .rounded_md()
                        .bg(cx.theme().colors().editor_background)
                        .child(
                            Label::new(investigation.title.clone())
                                .size(LabelSize::Small)
                                .truncate(),
                        )
                        .child(
                            Button::new(format!("resolve-investigation-{investigation_id}"), "Resolve")
                                .label_size(LabelSize::XSmall)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.resolve_investigation(investigation_id.clone(), cx);
                                })),
                        )
                        .into_any_element()
                }))
                .into_any_element()
        });

        root.into_any_element()
    }

    fn render_decisions_view(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let mut root = v_flex()
            .gap_2()
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        Label::new("Recent decisions").size(LabelSize::XSmall).color(Color::Muted),
                    )
                    .child(
                        Button::new(
                            "toggle-new-decision",
                            if self.creating_decision { "Cancel" } else { "+ New Decision" },
                        )
                        .label_size(LabelSize::Small)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.toggle_create_decision_form(window, cx);
                        })),
                    ),
            )
            .child(
                Label::new(
                    "There's no decisions index endpoint yet — this shows decisions recently seen in the feed.",
                )
                .size(LabelSize::XSmall)
                .color(Color::Muted),
            );

        if self.creating_decision {
            root = root.child(
                v_flex()
                    .gap_1p5()
                    .p_2()
                    .rounded_md()
                    .bg(cx.theme().colors().editor_background)
                    .children([
                        &self.new_decision_title_editor,
                        &self.new_decision_context_editor,
                        &self.new_decision_text_editor,
                        &self.new_decision_consequences_editor,
                    ].map(|editor| {
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(editor.clone())
                    }))
                    .child(
                        Button::new("create-decision", "Create")
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit_new_decision(window, cx);
                            })),
                    ),
            );
        }

        let recent_decisions: Vec<_> = self
            .feed_events
            .iter()
            .filter(|e| e.entity_type.as_deref() == Some("decision"))
            .take(10)
            .cloned()
            .collect();

        root = root.child(if recent_decisions.is_empty() {
            Label::new("No decisions in recent activity.")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element()
        } else {
            v_flex()
                .gap_1p5()
                .children(recent_decisions.iter().map(|event| {
                    v_flex()
                        .gap_0p5()
                        .p_2()
                        .rounded_md()
                        .bg(cx.theme().colors().editor_background)
                        .child(Label::new(event.summary.clone()).size(LabelSize::Small))
                        .child(
                            Label::new(format_relative_time(event.created_at))
                                .size(LabelSize::XSmall)
                                .color(Color::Muted),
                        )
                        .into_any_element()
                }))
                .into_any_element()
        });

        root.into_any_element()
    }

    fn render_agent_jobs_view(
        &mut self,
        brief: &ProjectContext,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        let mut root = v_flex().gap_2().child(
            h_flex()
                .justify_between()
                .items_center()
                .child(
                    Label::new(format!("{} tracked", brief.active_agent_jobs.len()))
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                )
                .child(
                    Button::new(
                        "toggle-new-agent-job",
                        if self.creating_agent_job { "Cancel" } else { "+ New Agent Job" },
                    )
                    .label_size(LabelSize::Small)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.toggle_create_agent_job_form(window, cx);
                    })),
                ),
        );

        if self.creating_agent_job {
            root = root.child(
                v_flex()
                    .gap_1p5()
                    .p_2()
                    .rounded_md()
                    .bg(cx.theme().colors().editor_background)
                    .child(
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.new_agent_job_goal_editor.clone()),
                    )
                    .child(
                        Button::new("create-agent-job", "Create")
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit_new_agent_job(window, cx);
                            })),
                    ),
            );
        }

        root = root.child(if brief.active_agent_jobs.is_empty() {
            Label::new("No agent jobs running right now.")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element()
        } else {
            v_flex()
                .gap_1p5()
                .children(
                    brief
                        .active_agent_jobs
                        .iter()
                        .map(|job| self.render_agent_job_row(job, cx)),
                )
                .into_any_element()
        });

        root.into_any_element()
    }

    fn render_task_composer(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        v_flex()
            .gap_1p5()
            .p_2()
            .rounded_md()
            .bg(cx.theme().colors().editor_background)
            .child(
                div()
                    .rounded_md()
                    .border_1()
                    .border_color(cx.theme().colors().border)
                    .px_2()
                    .py_1()
                    .child(self.new_task_title_editor.clone()),
            )
            .child(
                div()
                    .rounded_md()
                    .border_1()
                    .border_color(cx.theme().colors().border)
                    .px_2()
                    .py_1()
                    .child(self.new_task_description_editor.clone()),
            )
            .child(
                Button::new("create-task", "Create")
                    .label_size(LabelSize::Small)
                    .color(Color::Accent)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.submit_new_task(window, cx);
                    })),
            )
            .into_any_element()
    }

    fn render_search_section(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let mut section = v_flex()
            .gap_1p5()
            .child(
                Label::new("SEARCH PROJECT MEMORY")
                    .size(LabelSize::XSmall)
                    .color(Color::Muted),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .flex_1()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.search_editor.clone()),
                    )
                    .child(
                        Button::new("run-search", "Search")
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.run_search(cx);
                            })),
                    ),
            );

        if let Some(error) = self.search_error.clone() {
            section = section.child(Label::new(error).size(LabelSize::XSmall).color(Color::Error));
        } else if !self.search_results.is_empty() {
            section = section.child(
                v_flex()
                    .id("project-brain-search-results")
                    .gap_1p5()
                    .max_h(px(220.))
                    .overflow_y_scroll()
                    .children(self.search_results.iter().map(|result| {
                        v_flex()
                            .gap_0p5()
                            .p_2()
                            .rounded_md()
                            .bg(cx.theme().colors().editor_background)
                            .child(
                                Label::new(result.entity_type.replace('_', " "))
                                    .size(LabelSize::XSmall)
                                    .color(Color::Muted),
                            )
                            .child(
                                Label::new(result.snippet.clone())
                                    .size(LabelSize::Small)
                                    .truncate(),
                            )
                    })),
            );
        }

        section.into_any_element()
    }

    fn render_agent_job_row(
        &mut self,
        job: &BriefAgentJob,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        let job_id = job.id.clone();
        let claim_job_id = job_id.clone();
        let message_job_id = job_id.clone();
        let transcript_job_id = job_id.clone();
        let is_expanded = self.expanded_job_id.as_deref() == Some(job_id.as_str());
        let is_transcript_expanded =
            self.expanded_transcript_job_id.as_deref() == Some(job_id.as_str());
        let claimed_by = job
            .claimed_by_actor_id
            .as_deref()
            .map(|id| self.display_name_for(id));

        let mut row = v_flex()
            .gap_1()
            .p_2()
            .rounded_md()
            .bg(cx.theme().colors().editor_background)
            .child(
                h_flex()
                    .justify_between()
                    .items_start()
                    .gap_2()
                    .child(
                        Label::new(job.goal.clone())
                            .size(LabelSize::Small)
                            .truncate(),
                    )
                    .child(
                        Label::new(job.status.clone())
                            .size(LabelSize::XSmall)
                            .color(status_color(&job.status)),
                    ),
            )
            .child(
                Label::new(match &claimed_by {
                    Some(name) => format!("Claimed by {name}"),
                    None => "Unclaimed".to_string(),
                })
                .size(LabelSize::XSmall)
                .color(Color::Muted),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new(format!("claim-{job_id}"), "Claim")
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.claim_job(claim_job_id.clone(), cx);
                            })),
                    )
                    .child(
                        Button::new(format!("message-{job_id}"), "Message")
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.toggle_job_message(message_job_id.clone(), window, cx);
                            })),
                    )
                    .child(
                        Button::new(
                            format!("transcript-{job_id}"),
                            if is_transcript_expanded {
                                "Hide Transcript"
                            } else {
                                "Transcript"
                            },
                        )
                        .label_size(LabelSize::Small)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.toggle_transcript(transcript_job_id.clone(), cx);
                        })),
                    ),
            );

        if is_expanded {
            let send_job_id = job_id.clone();
            row = row.child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .bg(cx.theme().colors().editor_background)
                            .px_2()
                            .py_1()
                            .child(self.steering_editor.clone()),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                Button::new(format!("send-{send_job_id}"), "Send")
                                    .label_size(LabelSize::Small)
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.submit_steering_message(window, cx);
                                    })),
                            )
                            .child(
                                Button::new(format!("run-{job_id}"), "Run")
                                    .label_size(LabelSize::Small)
                                    .color(Color::Accent)
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.run_turn(window, cx);
                                    })),
                            ),
                    )
                    .child(
                        Label::new(
                            "\"Send\" leaves a steering note. \"Run\" invokes the headless agent directly (needs ANTHROPIC_API_KEY on the backend, and you must be the claimant).",
                        )
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                    ),
            );
        }

        if is_transcript_expanded {
            row = row.child(self.render_transcript(&job_id, cx));
        }

        row.into_any_element()
    }

    fn render_transcript(&self, job_id: &str, cx: &mut Context<Self>) -> gpui::AnyElement {
        let Some(turns) = self.job_turns.get(job_id) else {
            return Label::new("Loading transcript…")
                .size(LabelSize::XSmall)
                .color(Color::Muted)
                .into_any_element();
        };
        if turns.is_empty() {
            return Label::new("No turns yet.")
                .size(LabelSize::XSmall)
                .color(Color::Muted)
                .into_any_element();
        }

        v_flex()
            .id(format!("transcript-list-{job_id}"))
            .gap_1p5()
            .max_h(px(240.))
            .overflow_y_scroll()
            .p_1()
            .rounded_md()
            .bg(cx.theme().colors().editor_background)
            .children(turns.iter().map(|turn| Self::render_turn(turn)))
            .into_any_element()
    }

    fn render_turn(turn: &SessionTurn) -> gpui::AnyElement {
        let (role_label, role_color) = match turn.role {
            TurnRole::User => ("USER", Color::Accent),
            TurnRole::Assistant => ("ASSISTANT", Color::Default),
            TurnRole::Tool => ("TOOL RESULT", Color::Muted),
        };

        let body = if let Some(content) = &turn.content {
            content.clone()
        } else if let Some(tool_name) = &turn.tool_name {
            let input = turn
                .tool_input
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();
            format!("→ {tool_name}({input})")
        } else {
            String::new()
        };

        v_flex()
            .gap_0p5()
            .child(Label::new(role_label).size(LabelSize::XSmall).color(role_color))
            .child(Label::new(body).size(LabelSize::Small))
            .into_any_element()
    }

    /// "Who am I" — shows the current actor once known, or an onboarding
    /// form to create one. Nothing else in the panel that requires an actor
    /// (claiming, steering, task creation) works until this resolves.
    fn render_actor_section(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        if let Some(actor_id) = self.actor_id.clone() {
            let name = self.display_name_for(&actor_id);
            let mut root = v_flex().gap_1p5().child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        Label::new(format!("Signed in as {name}"))
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .when(self.actor_token.is_some(), |this| {
                                this.child(
                                    Button::new("copy-actor-token", "Copy Token")
                                        .label_size(LabelSize::Small)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(token) = this.actor_token.clone() {
                                                cx.write_to_clipboard(ClipboardItem::new_string(token));
                                                this.action_status =
                                                    Some("Token copied to clipboard".into());
                                                cx.notify();
                                            }
                                        })),
                                )
                            })
                            .child(
                                Button::new(
                                    "toggle-connect-agent",
                                    if self.connecting_agent { "Cancel" } else { "+ Connect Agent" },
                                )
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.toggle_connect_agent_form(window, cx);
                                })),
                            ),
                    ),
            );

            if self.connecting_agent {
                root = root.child(
                    v_flex()
                        .gap_1p5()
                        .p_2()
                        .rounded_md()
                        .bg(cx.theme().colors().editor_background)
                        .child(
                            Label::new(
                                "An agent doesn't sign in — you're provisioning it. This creates a separate agent actor and gives you a config block to hand it (hooks, MCP, or a CI secret).",
                            )
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                        )
                        .child(
                            div()
                                .rounded_md()
                                .border_1()
                                .border_color(cx.theme().colors().border)
                                .px_2()
                                .py_1()
                                .child(self.new_agent_name_editor.clone()),
                        )
                        .child(
                            Button::new("connect-agent", "Connect")
                                .label_size(LabelSize::Small)
                                .color(Color::Accent)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.submit_connect_agent(window, cx);
                                })),
                        ),
                );
            }

            if let Some(agent) = self.connected_agent.clone() {
                let expiry_label = agent
                    .expires_at
                    .map(|e| format!("expires {}", e.format("%b %-d, %Y, %-I:%M %p UTC")))
                    .unwrap_or_else(|| "no expiry".to_string());
                root = root.child(
                    v_flex()
                        .gap_1p5()
                        .p_2()
                        .rounded_md()
                        .bg(cx.theme().colors().editor_background)
                        .child(
                            Label::new(format!("@{}", agent.display_name))
                                .size(LabelSize::Small)
                                .weight(gpui::FontWeight::SEMIBOLD),
                        )
                        .child(
                            Label::new(format!("Token shown only once · {expiry_label}"))
                                .size(LabelSize::XSmall)
                                .color(Color::Warning),
                        )
                        .child(
                            Label::new("Token").size(LabelSize::XSmall).color(Color::Muted),
                        )
                        .child(
                            h_flex()
                                .gap_1()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded_md()
                                        .border_1()
                                        .border_color(cx.theme().colors().border)
                                        .px_2()
                                        .py_1()
                                        .child(
                                            Label::new(agent.token.clone())
                                                .size(LabelSize::XSmall)
                                                .color(Color::Muted),
                                        ),
                                )
                                .child(
                                    Button::new("copy-agent-token", "Copy")
                                        .label_size(LabelSize::Small)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(agent) = this.connected_agent.clone() {
                                                cx.write_to_clipboard(ClipboardItem::new_string(
                                                    agent.token,
                                                ));
                                                this.action_status =
                                                    Some("Token copied to clipboard".into());
                                                cx.notify();
                                            }
                                        })),
                                ),
                        )
                        .child(
                            Label::new("Command").size(LabelSize::XSmall).color(Color::Muted),
                        )
                        .child(
                            h_flex()
                                .gap_1()
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded_md()
                                        .border_1()
                                        .border_color(cx.theme().colors().border)
                                        .px_2()
                                        .py_1()
                                        .child(
                                            Label::new(agent.command.clone())
                                                .size(LabelSize::XSmall)
                                                .color(Color::Muted),
                                        ),
                                )
                                .child(
                                    Button::new("copy-agent-command", "Copy")
                                        .label_size(LabelSize::Small)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(agent) = this.connected_agent.clone() {
                                                cx.write_to_clipboard(ClipboardItem::new_string(
                                                    agent.command,
                                                ));
                                                this.action_status =
                                                    Some("Command copied to clipboard".into());
                                                cx.notify();
                                            }
                                        })),
                                ),
                        )
                        .child(
                            Label::new(
                                "Run that command in the agent's project directory, then paste \
                                 the token when it prompts for it — it writes .mcp.json itself.",
                            )
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                        )
                        .children(
                            crate::mcp_bridge::mcp_server_path_display().is_none().then(|| {
                                Label::new(
                                    "Note: RALLY_MCP_SERVER_PATH isn't set on this machine, so \
                                     the command above has a placeholder instead of a real path \
                                     to mcp-server/index.mjs — edit that before running it.",
                                )
                                .size(LabelSize::XSmall)
                                .color(Color::Warning)
                            }),
                        )
                        .child(
                            Button::new("connected-agent-done", "Done")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.connected_agent = None;
                                    cx.notify();
                                })),
                        )
                        .children({
                            let available = self.available_agent_servers(cx);
                            if available.is_empty() {
                                vec![
                                    Label::new(
                                        "No ACP agents configured in Zed yet — the config above still works for any external MCP client.",
                                    )
                                    .size(LabelSize::XSmall)
                                    .color(Color::Muted)
                                    .into_any_element(),
                                ]
                            } else {
                                available
                                    .into_iter()
                                    .map(|(id, name)| {
                                        Button::new(
                                            SharedString::from(format!("launch-agent-{id}")),
                                            format!("Launch {name} in Zed now"),
                                        )
                                        .label_size(LabelSize::Small)
                                        .color(Color::Accent)
                                        .on_click(cx.listener(move |this, _, window, cx| {
                                            this.launch_agent_in_zed(id.clone(), window, cx);
                                        }))
                                        .into_any_element()
                                    })
                                    .collect::<Vec<_>>()
                            }
                        }),
                );
            }

            return root.into_any_element();
        }

        v_flex()
            .gap_2()
            .child(
                v_flex()
                    .gap_1p5()
                    .p_2()
                    .rounded_md()
                    .bg(cx.theme().colors().element_background)
                    .child(
                        h_flex().justify_between().items_center().child(
                            Label::new(match self.login_mode {
                                LoginMode::Login => "LOG IN TO RALLY",
                                LoginMode::Signup => "CREATE A RALLY ACCOUNT",
                            })
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                        ).child(
                            Button::new(
                                "toggle-login-mode",
                                match self.login_mode {
                                    LoginMode::Login => "Need an account?",
                                    LoginMode::Signup => "Have an account?",
                                },
                            )
                            .label_size(LabelSize::XSmall)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_login_mode(cx);
                            })),
                        ),
                    )
                    .child(
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.login_email_editor.clone()),
                    )
                    .child(
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.login_password_editor.clone()),
                    )
                    .when_some(self.login_error.clone(), |this, error| {
                        this.child(
                            Label::new(error)
                                .size(LabelSize::XSmall)
                                .color(Color::Error),
                        )
                    })
                    .child(
                        Button::new(
                            "submit-login",
                            if self.logging_in {
                                "Please wait…"
                            } else {
                                match self.login_mode {
                                    LoginMode::Login => "Log in",
                                    LoginMode::Signup => "Create account",
                                }
                            },
                        )
                        .label_size(LabelSize::Small)
                        .color(Color::Accent)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.submit_login(window, cx);
                        })),
                    ),
            )
            .child(
                v_flex()
                    .gap_1p5()
                    .p_2()
                    .rounded_md()
                    .bg(cx.theme().colors().element_background)
                    .child(
                        Label::new("OR CONNECT WITHOUT AN ACCOUNT")
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                    )
                    .child(
                        div()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().colors().border)
                            .px_2()
                            .py_1()
                            .child(self.onboarding_name_editor.clone()),
                    )
                    .child(
                        Button::new("create-actor", "Create Actor")
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit_create_actor(window, cx);
                            })),
                    ),
            )
            .into_any_element()
    }
}

async fn fetch_search(project_id: String, query: String) -> anyhow::Result<SearchResponse> {
    let url = format!("{}/projects/{}/search", backend_base_url(), project_id);
    let client = reqwest::Client::new();
    let res = client
        .get(&url)
        .query(&[("q", query.as_str()), ("limit", "20")])
        .send()
        .await?;
    let body = res.json::<SearchResponse>().await?;
    Ok(body)
}

async fn fetch_similar_work(
    project_id: String,
    entity_type: &'static str,
    text: String,
) -> anyhow::Result<Vec<SimilarWorkMatch>> {
    let url = format!("{}/projects/{}/similar-work", backend_base_url(), project_id);
    let client = reqwest::Client::new();
    let res = client
        .get(&url)
        .query(&[("entity_type", entity_type), ("text", text.as_str())])
        .send()
        .await?;
    let body = res.json::<SimilarWorkResponse>().await?;
    Ok(body.matches)
}

async fn fetch_project_context(project_id: String) -> anyhow::Result<ProjectContext> {
    let url = format!("{}/projects/{}/context", backend_base_url(), project_id);
    let client = reqwest::Client::new();
    let res = client.get(&url).send().await?;
    let context = res.json::<ProjectContext>().await?;
    Ok(context)
}

/// Requires session auth + project membership as of this session's backend
/// changes — `session_token` must be the logged-in user's, not an actor
/// bearer token (this endpoint would reject that as an invalid session).
async fn fetch_actors(
    project_id: String,
    session_token: Option<String>,
) -> anyhow::Result<Vec<ActorInfo>> {
    let url = format!("{}/projects/{}/actors", backend_base_url(), project_id);
    let client = reqwest::Client::new();
    let mut req = client.get(&url);
    if let Some(token) = session_token {
        req = req.bearer_auth(token);
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    let actors = res.json::<Vec<ActorInfo>>().await?;
    Ok(actors)
}

async fn fetch_turns(job_id: String) -> anyhow::Result<Vec<SessionTurn>> {
    let url = format!("{}/agent-jobs/{}/turns", backend_base_url(), job_id);
    let client = reqwest::Client::new();
    let res = client.get(&url).send().await?;
    let turns = res.json::<Vec<SessionTurn>>().await?;
    Ok(turns)
}

async fn create_actor_request(
    project_id: String,
    display_name: String,
    kind: String,
    session_token: Option<String>,
) -> anyhow::Result<CreateActorResponse> {
    let url = format!("{}/projects/{}/actors", backend_base_url(), project_id);
    let client = reqwest::Client::new();
    let mut req = client
        .post(&url)
        .json(&serde_json::json!({ "kind": kind, "display_name": display_name }));
    if let Some(token) = session_token {
        req = req.bearer_auth(token);
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    let body = res.json::<CreateActorResponse>().await?;
    Ok(body)
}

/// Only the field the panel actually needs from `/auth/login` /
/// `/auth/signup` — extra fields (e.g. `user`) are ignored by default.
#[derive(Debug, Deserialize)]
struct AuthSessionResponse {
    session_token: String,
}

async fn login_request(email: String, password: String) -> anyhow::Result<AuthSessionResponse> {
    let url = format!("{}/auth/login", backend_base_url());
    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .json(&serde_json::json!({ "email": email, "password": password }))
        .send()
        .await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(res.json::<AuthSessionResponse>().await?)
}

async fn signup_request(
    email: String,
    password: String,
    display_name: String,
) -> anyhow::Result<AuthSessionResponse> {
    let url = format!("{}/auth/signup", backend_base_url());
    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .json(&serde_json::json!({
            "email": email,
            "password": password,
            "display_name": display_name,
        }))
        .send()
        .await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(res.json::<AuthSessionResponse>().await?)
}

/// Mirrors the backend's `/projects/:id/members` response shape closely
/// enough for the panel's purposes — same fields `CreateActorResponse`
/// already mirrors, just without the `token` rename.
#[derive(Debug, Deserialize)]
struct SessionActor {
    id: String,
    kind: String,
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct JoinProjectResponse {
    actor: SessionActor,
    actor_token: String,
}

/// Idempotent — get-or-create this session's actor for `project_id`, the
/// same call `rally_frontend`'s dashboard makes on login.
async fn join_project_request(
    project_id: String,
    session_token: String,
) -> anyhow::Result<JoinProjectResponse> {
    let url = format!("{}/projects/{}/members", backend_base_url(), project_id);
    let client = reqwest::Client::new();
    let res = client.post(&url).bearer_auth(session_token).send().await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(res.json::<JoinProjectResponse>().await?)
}

async fn list_my_projects_request(session_token: String) -> anyhow::Result<Vec<ProjectSummary>> {
    let url = format!("{}/users/me/projects", backend_base_url());
    let client = reqwest::Client::new();
    let res = client.get(&url).bearer_auth(session_token).send().await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(res.json::<Vec<ProjectSummary>>().await?)
}

/// Mirrors the backend's `CreateMyProjectResponse` (`POST
/// /users/me/projects`) — same shape `join_project_request` already
/// partially mirrors via `JoinProjectResponse`, plus the created project.
#[derive(Debug, Deserialize)]
struct CreateProjectResult {
    project: ProjectSummary,
    actor: SessionActor,
    actor_token: String,
}

async fn create_project_request(
    session_token: String,
    name: String,
) -> anyhow::Result<CreateProjectResult> {
    let url = format!("{}/users/me/projects", backend_base_url());
    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .bearer_auth(session_token)
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(res.json::<CreateProjectResult>().await?)
}

async fn post_json(url: String, body: serde_json::Value, token: Option<String>) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let mut req = client.post(&url).json(&body);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(())
}

async fn patch_json(url: String, body: serde_json::Value, token: Option<String>) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let mut req = client.patch(&url).json(&body);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("{status}: {text}");
    }
    Ok(())
}

impl Focusable for ProjectBrainPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ProjectBrainPanel {}

impl Panel for ProjectBrainPanel {
    fn activation_priority(&self) -> u32 {
        100 // Or any default priority number
    }

    fn persistent_name() -> &'static str {
        "ProjectBrainPanel"
    }

    fn panel_key() -> &'static str {
        "project_brain_panel"
    }

    fn position(&self, _window: &Window, _cx: &App) -> DockPosition {
        self.position
    }

    fn position_is_valid(&self, _position: DockPosition) -> bool {
        true
    }

    fn set_position(&mut self, position: DockPosition, _window: &mut Window, cx: &mut Context<Self>) {
        self.position = position;
        cx.notify();
    }

    fn default_size(&self, _window: &Window, _cx: &App) -> Pixels {
        px(320.)
    }

    fn icon(&self, _window: &Window, _cx: &App) -> Option<IconName> {
        Some(IconName::Sparkle)
    }

    fn icon_tooltip(&self, _window: &Window, _cx: &App) -> Option<&'static str> {
        Some("Project Brain Panel")
    }

    fn toggle_action(&self) -> Box<dyn Action> {
        Box::new(ToggleProjectBrainPanel)
    }
}

impl Render for ProjectBrainPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Theme-native semantic colors rather than fixed hex — matches
        // whatever Zed theme (light/dark/any accent) the user has active,
        // the same "success/warning/critical, kept separate from the
        // accent" split used by the dashboard and rally_frontend.
        let (dot_color, status_label) = match self.connection_status {
            ConnectionStatus::Connected => (Color::Success.color(cx), "Connected"),
            ConnectionStatus::Connecting => (Color::Warning.color(cx), "Connecting..."),
            ConnectionStatus::Disconnected => (Color::Error.color(cx), "Disconnected"),
        };

        v_flex()
            .key_context("ProjectBrainPanel")
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(Self::confirm))
            .size_full()
            .bg(cx.theme().colors().panel_background)
            .p_3()
            .gap_2()
            // 1. Connection Status Bar
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .pb_2()
                    .border_b_1()
                    .border_color(cx.theme().colors().border)
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(div().w_2p5().h_2p5().rounded_full().bg(dot_color))
                            .child(Label::new(status_label).size(LabelSize::Small)),
                    )
                    .child(
                        Label::new("Rally Brain")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    ),
            )
            // 2. Actor onboarding — "who am I", or a form to become someone
            .child(self.render_actor_section(cx))
            .children(self.render_action_status())
            .children(self.render_pending_duplicate(cx))
            // 3. Compact presence line — always visible regardless of which
            //    view is active, since "who's around" matters everywhere.
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        Label::new("PRESENCE").size(LabelSize::XSmall).color(Color::Muted),
                    )
                    .child(if self.presence.is_empty() {
                        Label::new("· nobody else here")
                            .size(LabelSize::XSmall)
                            .color(Color::Muted)
                            .into_any_element()
                    } else {
                        h_flex()
                            .gap_2()
                            .children(self.presence.iter().map(|p| {
                                let display_name = self.display_name_for(&p.actor_id);
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(div().w_1p5().h_1p5().rounded_full().bg(Color::Success.color(cx)))
                                    .child(Label::new(display_name).size(LabelSize::XSmall))
                            }))
                            .into_any_element()
                    }),
            )
            // 4. Tab strip — Feed / Tasks / Investigations / Decisions /
            //    Agent Jobs, as real switchable views rather than one long
            //    vertical stack.
            .child(self.render_tab_strip(cx))
            // 5. Whichever view is currently active.
            .child(
                v_flex()
                    .id("project-brain-active-view")
                    .flex_1()
                    .min_h(px(0.))
                    .overflow_y_scroll()
                    .child(self.render_active_view(cx)),
            )
    }
}

/// Semantic status coloring — queued/idle stays muted, running/in-progress
/// reads as a warning-toned "in flight", done as success, failed as error.
/// Mirrors the same split used by the dashboard and rally_frontend: status
/// color is separate from the panel's accent, never doubles as it.
fn next_task_status(current: &str) -> &'static str {
    match current {
        "open" => "in_progress",
        "in_progress" => "blocked",
        "blocked" => "done",
        _ => "open",
    }
}

fn status_color(status: &str) -> Color {
    match status {
        "running" | "in_progress" => Color::Warning,
        "done" | "resolved" | "accepted" => Color::Success,
        "failed" | "blocked" => Color::Error,
        _ => Color::Muted,
    }
}

fn format_relative_time(dt: Option<DateTime<Utc>>) -> String {
    let Some(dt) = dt else {
        return String::new();
    };
    let now = Utc::now();
    let duration = now.signed_duration_since(dt);
    if duration.num_seconds() < 60 {
        "just now".to_string()
    } else if duration.num_minutes() < 60 {
        format!("{}m ago", duration.num_minutes())
    } else if duration.num_hours() < 24 {
        format!("{}h ago", duration.num_hours())
    } else {
        format!("{}d ago", duration.num_days())
    }
}

pub fn init(cx: &mut App) {
    live_state::init(cx);
    welcome::init(cx);
    cx.observe_new(
        |workspace: &mut Workspace, window, cx: &mut Context<Workspace>| {
            workspace.register_action(|workspace, _: &ToggleProjectBrainPanel, window, cx| {
                workspace.toggle_panel_focus::<ProjectBrainPanel>(window, cx);
            });
            let Some(window) = window else { return };
            let panel = cx.new(|cx| ProjectBrainPanel::new(workspace, window, cx));
            workspace.add_panel(panel, window, cx);
        },
    )
    .detach();
}