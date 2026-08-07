use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use gpui::{
    Action, App, AsyncApp, Context, EventEmitter, FocusHandle, Focusable, IntoElement, Pixels,
    Render, Task, Window, px, prelude::*, rgb, WeakEntity,
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

pub struct ProjectBrainPanel {
    focus_handle: FocusHandle,
    position: DockPosition,
    feed_events: Vec<FeedEvent>,
    presence: Vec<PresenceEntry>,
    connection_status: ConnectionStatus,
    _ws_task: Option<Task<()>>,
}

impl ProjectBrainPanel {
    pub fn new(_workspace: &Workspace, cx: &mut Context<Self>) -> Self {
        let mut this = Self {
            focus_handle: cx.focus_handle(),
            position: DockPosition::Right,
            feed_events: Vec::new(),
            presence: Vec::new(),
            connection_status: ConnectionStatus::Connecting,
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
            }
            LiveMessage::AgentJobUpdate {
                summary, status, ..
            } => {
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
            // 2. Active Presence Section
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
            // 3. Scrolling Feed List
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
                        v_flex().p_2().child(
                            Label::new("No feed events recorded yet.")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    } else {
                        v_flex()
                            .flex_1()
                            .overflow_y_hidden()
                            .overflow_x_hidden()
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
            let panel = cx.new(|cx| ProjectBrainPanel::new(workspace, cx));
            if let Some(window) = window {
                workspace.add_panel(panel, window, cx);
            }
        },
    )
    .detach();
}