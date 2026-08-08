use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use chrono::{DateTime, Utc};
use editor::Editor;
use futures::StreamExt;
use gpui::{
    Action, App, AsyncApp, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement,
    Pixels, Render, Task, Window, px, prelude::*, rgb, WeakEntity,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use ui::{prelude::*, IconName};
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
};

// TODO: make configurable via settings/env var
const BACKEND_BASE_URL: &str = "http://localhost:8080";
const BACKEND_WS_URL: &str = "ws://localhost:8080";

fn get_project_id() -> Option<String> {
    std::env::var("RALLY_PROJECT_ID").ok()
}

/// Which actor this Zed session acts as when claiming a job or sending a
/// steering message. Mirrors RALLY_PROJECT_ID — set once per session.
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PresenceEntry {
    pub actor_id: String,
    pub name: Option<String>,
    #[serde(default = "default_online")]
    pub online: bool,
}

fn default_online() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum LiveMessage {
    #[serde(rename = "presence_update")]
    PresenceUpdate {
        presence: Option<Vec<PresenceEntry>>,
        actor_id: Option<String>,
        name: Option<String>,
        online: Option<bool>,
    },
    #[serde(rename = "memory_event")]
    MemoryEvent {
        event: Option<FeedEvent>,
        #[serde(flatten)]
        raw_event: Option<FeedEvent>,
    },
    #[serde(rename = "agent_job_update")]
    AgentJobUpdate {
        job_id: Option<String>,
        status: Option<String>,
        summary: Option<String>,
    },
    #[serde(untagged)]
    Other(serde_json::Value),
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

pub struct ProjectBrainPanel {
    focus_handle: FocusHandle,
    position: DockPosition,
    feed_events: Vec<FeedEvent>,
    presence: Vec<PresenceEntry>,
    connection_status: ConnectionStatus,
    brief: Option<ProjectContext>,
    /// The agent job whose steering-message composer is currently expanded,
    /// if any.
    expanded_job_id: Option<String>,
    steering_editor: Entity<Editor>,
    action_status: Option<String>,
    search_editor: Entity<Editor>,
    search_results: Vec<SearchResultEntry>,
    search_error: Option<String>,
    creating_task: bool,
    new_task_title_editor: Entity<Editor>,
    new_task_description_editor: Entity<Editor>,
    _ws_task: Option<Task<()>>,
}

impl ProjectBrainPanel {
    pub fn new(_workspace: &Workspace, window: &mut Window, cx: &mut Context<Self>) -> Self {
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

        let mut this = Self {
            focus_handle: cx.focus_handle(),
            position: DockPosition::Right,
            feed_events: Vec::new(),
            presence: Vec::new(),
            connection_status: ConnectionStatus::Connecting,
            brief: None,
            expanded_job_id: None,
            steering_editor,
            action_status: None,
            search_editor,
            search_results: Vec::new(),
            search_error: None,
            creating_task: false,
            new_task_title_editor,
            new_task_description_editor,
            _ws_task: None,
        };

        this.start_background_sync(cx);
        this
    }

    fn start_background_sync(&mut self, cx: &mut Context<Self>) {
        let task = cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            // 1. Fetch initial feed history (run on the Tokio runtime, since
            //    reqwest needs a real Tokio reactor for DNS/networking).
            let Some(project_id) = get_project_id() else {
                log::error!("RALLY_PROJECT_ID is not set. Project Brain cannot connect.");
                return;
            };

            let feed_url = format!(
                "{}/projects/{}/feed?limit=50",
                BACKEND_BASE_URL, project_id
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
                    panel.feed_events = feed_data.events;
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

            // 2. Connect to WebSocket stream & reconnect loop
            let ws_url = format!("{}/projects/{}/live", BACKEND_WS_URL, project_id);
            loop {
                let _ = this.update(cx, |panel, cx| {
                    panel.connection_status = ConnectionStatus::Connecting;
                    cx.notify();
                });

                let ws_url_clone = ws_url.clone();
                let connect_result = tokio_runtime()
                    .spawn(async move { async_tungstenite::tokio::connect_async(&ws_url_clone).await })
                    .await;

                match connect_result {
                    Ok(Ok((ws_stream, _))) => {
                        let _ = this.update(cx, |panel, cx| {
                            panel.connection_status = ConnectionStatus::Connected;
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
            LiveMessage::PresenceUpdate {
                presence,
                actor_id,
                name,
                online,
            } => {
                if let Some(list) = presence {
                    self.presence = list;
                } else if let Some(actor_id) = actor_id {
                    let is_online = online.unwrap_or(true);
                    if let Some(existing) = self.presence.iter_mut().find(|p| p.actor_id == actor_id) {
                        existing.online = is_online;
                        if let Some(n) = name {
                            existing.name = Some(n);
                        }
                    } else {
                        self.presence.push(PresenceEntry {
                            actor_id,
                            name,
                            online: is_online,
                        });
                    }
                }
            }
            LiveMessage::MemoryEvent { event, raw_event } => {
                if let Some(e) = event.or(raw_event) {
                    self.push_feed_event(e, cx);
                }
                // The brief aggregates tasks/investigations/agent jobs, so
                // any event landing could change what it should show.
                self.refresh_project_context(cx);
            }
            LiveMessage::AgentJobUpdate {
                summary, status, ..
            } => {
                self.refresh_project_context(cx);
                if let Some(summary_text) = summary {
                    let status_str = status.unwrap_or_else(|| "job".to_string());
                    let event_id = format!("gen_{}", EVENT_COUNTER.fetch_add(1, Ordering::SeqCst));
                    self.push_feed_event(
                        FeedEvent {
                            id: event_id,
                            project_id: get_project_id(),
                            actor_id: Some("agent".to_string()),
                            entity_type: Some("job".to_string()),
                            entity_id: None,
                            verb: Some(status_str),
                            summary: summary_text,
                            created_at: Some(Utc::now()),
                        },
                        cx,
                    );
                }
            }
            LiveMessage::Other(_) => {}
        }
        cx.notify();
    }

    fn push_feed_event(&mut self, mut event: FeedEvent, cx: &mut Context<Self>) {
        if event.id.is_empty() {
            event.id = format!("gen_{}", EVENT_COUNTER.fetch_add(1, Ordering::SeqCst));
        }
        if !self.feed_events.iter().any(|e| e.id == event.id) {
            self.feed_events.insert(0, event);
            cx.notify();
        }
    }

    fn refresh_project_context(&mut self, cx: &mut Context<Self>) {
        let Some(project_id) = get_project_id() else {
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
        let Some(actor_id) = get_actor_id() else {
            self.action_status = Some("Set RALLY_ACTOR_ID to claim jobs".into());
            cx.notify();
            return;
        };
        let url = format!("{}/agent-jobs/{}/claim", BACKEND_BASE_URL, job_id);
        let body = serde_json::json!({ "actor_id": actor_id });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body)).await;
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
        let Some(actor_id) = get_actor_id() else {
            self.action_status = Some("Set RALLY_ACTOR_ID to send steering messages".into());
            cx.notify();
            return;
        };
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

        let url = format!("{}/agent-jobs/{}/messages", BACKEND_BASE_URL, job_id);
        let body = serde_json::json!({ "actor_id": actor_id, "content": content });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body)).await;
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
        let Some(project_id) = get_project_id() else {
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
        let Some(actor_id) = get_actor_id() else {
            self.action_status = Some("Set RALLY_ACTOR_ID to create tasks".into());
            cx.notify();
            return;
        };
        let Some(project_id) = get_project_id() else {
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

        let url = format!("{}/projects/{}/tasks", BACKEND_BASE_URL, project_id);
        let body = serde_json::json!({
            "actor_id": actor_id,
            "title": title,
            "description": description,
        });
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = tokio_runtime().spawn(post_json(url, body)).await;
            let status = match result {
                Ok(Ok(())) => Some("Task created".to_string()),
                Ok(Err(err)) => Some(format!("Create task failed: {err:#}")),
                Err(err) => Some(format!("Create task failed: {err:#}")),
            };
            let _ = this.update(cx, |panel, cx| {
                panel.action_status = status;
                panel.refresh_project_context(cx);
            });
        })
        .detach();
    }

    fn display_name_for(&self, actor_id: &str) -> String {
        self.presence
            .iter()
            .find(|p| p.actor_id == actor_id)
            .and_then(|p| p.name.clone())
            .unwrap_or_else(|| actor_id.chars().take(8).collect())
    }

    fn render_brief(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let section_label = |text: &'static str| {
            Label::new(text).size(LabelSize::XSmall).color(Color::Muted)
        };

        let Some(brief) = self.brief.clone() else {
            return v_flex()
                .gap_1()
                .child(section_label("PROJECT BRIEF"))
                .child(
                    Label::new("Loading shared memory…")
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
                .into_any_element();
        };

        let mut root = v_flex()
            .gap_2()
            .p_2()
            .rounded_md()
            .bg(cx.theme().colors().element_background)
            .child(section_label("PROJECT BRIEF — shared memory entrypoint"));

        if let Some(status) = self.action_status.clone() {
            root = root.child(Label::new(status).size(LabelSize::XSmall).color(Color::Accent));
        }

        root = root.child(
            h_flex()
                .justify_between()
                .items_center()
                .child(
                    h_flex()
                        .gap_1()
                        .child(
                            Label::new(format!("{} open tasks", brief.open_tasks.len()))
                                .size(LabelSize::XSmall)
                                .color(Color::Muted),
                        )
                        .child(Label::new("·").size(LabelSize::XSmall).color(Color::Muted))
                        .child(
                            Label::new(format!(
                                "{} active investigations",
                                brief.active_investigations.len()
                            ))
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                        ),
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

        root = root.child(
            v_flex()
                .gap_1()
                .child(section_label("AGENT JOBS"))
                .child(if brief.active_agent_jobs.is_empty() {
                    v_flex().child(
                        Label::new("No agent jobs running right now.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                } else {
                    v_flex()
                        .gap_1p5()
                        .children(
                            brief
                                .active_agent_jobs
                                .iter()
                                .map(|job| self.render_agent_job_row(job, cx)),
                        )
                }),
        );

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
        let is_expanded = self.expanded_job_id.as_deref() == Some(job_id.as_str());
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
                            .color(Color::Muted),
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
                        Button::new(format!("send-{send_job_id}"), "Send")
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.submit_steering_message(window, cx);
                            })),
                    ),
            );
        }

        row.into_any_element()
    }
}

async fn fetch_search(project_id: String, query: String) -> anyhow::Result<SearchResponse> {
    let url = format!("{}/projects/{}/search", BACKEND_BASE_URL, project_id);
    let client = reqwest::Client::new();
    let res = client
        .get(&url)
        .query(&[("q", query.as_str()), ("limit", "20")])
        .send()
        .await?;
    let body = res.json::<SearchResponse>().await?;
    Ok(body)
}

async fn fetch_project_context(project_id: String) -> anyhow::Result<ProjectContext> {
    let url = format!("{}/projects/{}/context", BACKEND_BASE_URL, project_id);
    let client = reqwest::Client::new();
    let res = client.get(&url).send().await?;
    let context = res.json::<ProjectContext>().await?;
    Ok(context)
}

async fn post_json(url: String, body: serde_json::Value) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let res = client.post(&url).json(&body).send().await?;
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
        let (dot_color, status_label) = match self.connection_status {
            ConnectionStatus::Connected => (rgb(0x22c55e), "Connected"),
            ConnectionStatus::Connecting => (rgb(0xeab308), "Connecting..."),
            ConnectionStatus::Disconnected => (rgb(0xef4444), "Disconnected"),
        };

        v_flex()
            .key_context("ProjectBrainPanel")
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(Self::confirm))
            .size_full()
            .bg(cx.theme().colors().panel_background)
            .p_3()
            .gap_3()
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
            // 2. Search
            .child(self.render_search_section(cx))
            // 3. Active Presence Section
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        Label::new("ACTIVE PRESENCE")
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
                    )
                    .child(if self.presence.is_empty() {
                        div().child(
                            Label::new("No active presence")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    } else {
                        v_flex().gap_1().children(self.presence.iter().map(|p| {
                            let dot = if p.online {
                                rgb(0x22c55e)
                            } else {
                                rgb(0x6b7280)
                            };
                            let display_name = p
                                .name
                                .clone()
                                .unwrap_or_else(|| p.actor_id.clone());

                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(div().w_2().h_2().rounded_full().bg(dot))
                                .child(Label::new(display_name).size(LabelSize::Small))
                        }))
                    }),
            )
            // 4. Project brief: shared-memory entrypoint + agent monitor
            .child(self.render_brief(cx))
            // 5. Scrolling Feed List
            .child(
                v_flex()
                    .flex_1()
                    .gap_1()
                    .child(
                        Label::new("PROJECT FEED")
                            .size(LabelSize::XSmall)
                            .color(Color::Muted),
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
                            .flex_1()
                            .min_h(px(0.))
                            .overflow_y_scroll()
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
                                        h_flex()
                                            .justify_between()
                                            .items_center()
                                            .child(
                                                Label::new(event.summary.clone())
                                                    .size(LabelSize::Small),
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