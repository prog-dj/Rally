//! Always-visible status-bar surface for presence/collaboration, so "who
//! else is here, human or agent" doesn't require opening the dock panel —
//! the dock is for depth (feed, tasks, transcripts), this is for glanceable
//! ambient awareness.

use std::time::Duration;

use editor::{Editor, EditorEvent, Inlay};
use gpui::{
    App, AsyncWindowContext, Context, Entity, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled, Subscription, Task, WeakEntity, Window,
};
use project::InlayId;
use text::{Point, Selection};
use ui::{prelude::*, IconName, Tooltip};
use util::paths::PathStyle;
use workspace::{StatusItemView, item::ItemHandle};

use crate::{ConnectionStatus, RallyLiveState, RallyLiveStateEvent, ToggleProjectBrainPanel};

/// Debounce between a cursor/file change and the heartbeat it triggers, so a
/// burst of keystrokes doesn't turn into a burst of requests.
const HEARTBEAT_DEBOUNCE: Duration = Duration::from_millis(1500);

pub struct RallyStatusItem {
    live_state: Entity<RallyLiveState>,
    _subscription: Subscription,
    current_project_path: Option<String>,
    current_editor: Option<WeakEntity<Editor>>,
    _observe_active_editor: Option<Subscription>,
    _heartbeat_task: Task<()>,
    /// Inlays currently spliced into `current_editor` showing where other
    /// actors are — tracked so the next refresh can remove exactly these
    /// before inserting the new set.
    spliced_inlay_ids: Vec<InlayId>,
    next_inlay_id: usize,
}

impl RallyStatusItem {
    pub fn new(cx: &mut Context<Self>) -> Self {
        let live_state = RallyLiveState::global(cx);
        let subscription = cx.subscribe(
            &live_state,
            |this, _live_state, _event: &RallyLiveStateEvent, cx| {
                this.refresh_presence_inlays(cx);
                cx.notify();
            },
        );
        Self {
            live_state,
            _subscription: subscription,
            current_project_path: None,
            current_editor: None,
            _observe_active_editor: None,
            _heartbeat_task: Task::ready(()),
            spliced_inlay_ids: Vec::new(),
            next_inlay_id: 0,
        }
    }

    /// Shows a small inline marker at the line of every other actor whose
    /// last-reported `current_file` matches the file open in the currently
    /// active editor — the same presence data behind the status bar label
    /// above, surfaced inline instead of only as a count.
    fn refresh_presence_inlays(&mut self, cx: &mut Context<Self>) {
        let Some(editor) = self.current_editor.clone().and_then(|e| e.upgrade()) else {
            self.spliced_inlay_ids.clear();
            return;
        };

        let markers: Vec<(i64, String)> = match &self.current_project_path {
            Some(current_path) => {
                let state = self.live_state.read(cx);
                let own_actor_id = state.actor_id.as_deref();
                state
                    .presence
                    .iter()
                    .filter(|entry| {
                        Some(entry.actor_id.as_str()) != own_actor_id
                            && entry.current_file.as_deref() == Some(current_path.as_str())
                    })
                    .filter_map(|entry| {
                        let line = entry.current_line?;
                        let name = state
                            .actors
                            .iter()
                            .find(|a| a.id == entry.actor_id)
                            .map(|a| a.display_name.clone())
                            .unwrap_or_else(|| entry.actor_id.clone());
                        Some((line, name))
                    })
                    .collect()
            }
            None => Vec::new(),
        };

        let old_ids = std::mem::take(&mut self.spliced_inlay_ids);
        let mut next_id = self.next_inlay_id;
        let new_ids = editor.update(cx, |editor, cx| {
            let snapshot = editor.buffer().read(cx).snapshot(cx);
            let new_inlays: Vec<Inlay> = markers
                .into_iter()
                .map(|(line, name)| {
                    let row = (line - 1).max(0) as u32;
                    let anchor = snapshot.anchor_before(Point::new(row, 0));
                    let id = next_id;
                    next_id += 1;
                    Inlay::rally_presence(id, anchor, format!(" {name}"))
                })
                .collect();
            let new_ids: Vec<InlayId> = new_inlays.iter().map(|inlay| inlay.id).collect();
            editor.splice_inlays(&old_ids, new_inlays, cx);
            new_ids
        });
        self.next_inlay_id = next_id;
        self.spliced_inlay_ids = new_ids;
    }

    fn is_agent(&self, actor_id: &str, cx: &App) -> bool {
        self.live_state
            .read(cx)
            .actors
            .iter()
            .find(|a| a.id == actor_id)
            .is_some_and(|a| a.kind == "agent")
    }

    /// Reports this session's own file/line to the backend so other
    /// clients' presence lists (and eventually editor gutters) can show
    /// where everyone is — mirrors the file+line, not the exact selection,
    /// so it stays cheap to compute and send.
    fn schedule_heartbeat(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let current_project_path = self.current_project_path.clone();
        let current_editor = self.current_editor.clone();

        self._heartbeat_task = cx.spawn_in(window, async move |this, cx: &mut AsyncWindowContext| {
            cx.background_executor().timer(HEARTBEAT_DEBOUNCE).await;

            let current_line = match current_editor {
                Some(editor) => editor
                    .update(cx, |editor, cx| {
                        let snapshot = editor.display_snapshot(cx);
                        let mut last_selection: Option<Selection<Point>> = None;
                        for selection in editor.selections.all_adjusted(&snapshot) {
                            if last_selection
                                .as_ref()
                                .is_none_or(|last| selection.id > last.id)
                            {
                                last_selection = Some(selection);
                            }
                        }
                        last_selection.map(|s| s.head().row as i64 + 1)
                    })
                    .ok()
                    .flatten(),
                None => None,
            };

            let _ = this.update(cx, |this, cx| {
                this.send_heartbeat(current_project_path, current_line, cx);
            });
        });
    }

    fn send_heartbeat(
        &self,
        current_file: Option<String>,
        current_line: Option<i64>,
        cx: &mut Context<Self>,
    ) {
        let Some(project_id) = crate::get_project_id() else {
            return;
        };
        let state = self.live_state.read(cx);
        let Some(actor_id) = state.actor_id.clone() else {
            return;
        };
        let token = state.actor_token.clone();
        let url = format!(
            "{}/projects/{}/presence/heartbeat",
            crate::backend_base_url(),
            project_id
        );
        let body = serde_json::json!({
            "actor_id": actor_id,
            "current_file": current_file,
            "current_line": current_line,
        });
        cx.background_spawn(async move {
            let _ = crate::tokio_runtime().spawn(crate::post_json(url, body, token)).await;
        })
        .detach();
    }
}

impl Render for RallyStatusItem {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.live_state.read(cx);
        let connection_status = state.connection_status;
        let presence = state.presence.clone();

        let agent_count = presence
            .iter()
            .filter(|p| self.is_agent(&p.actor_id, cx))
            .count();
        let human_count = presence.len().saturating_sub(agent_count);

        let (dot_color, label) = match connection_status {
            Some(ConnectionStatus::Connected) if !presence.is_empty() => (
                Color::Success,
                format!(
                    "{human_count} online · {agent_count} agent{}",
                    if agent_count == 1 { "" } else { "s" }
                ),
            ),
            Some(ConnectionStatus::Connected) => (Color::Success, "Rally connected".to_string()),
            Some(ConnectionStatus::Connecting) | None => {
                (Color::Warning, "Rally connecting…".to_string())
            }
            Some(ConnectionStatus::Disconnected) => {
                (Color::Error, "Rally disconnected".to_string())
            }
        };

        h_flex()
            .id("rally-status-item")
            .gap_1()
            .px_1()
            .cursor_pointer()
            .child(
                Icon::new(IconName::UserGroup)
                    .size(IconSize::Small)
                    .color(dot_color),
            )
            .child(Label::new(label).size(LabelSize::Small))
            .tooltip(Tooltip::text("Toggle Rally project panel"))
            .on_click(|_, window, cx| {
                window.dispatch_action(Box::new(ToggleProjectBrainPanel), cx);
            })
    }
}

impl StatusItemView for RallyStatusItem {
    fn set_active_pane_item(
        &mut self,
        active_pane_item: Option<&dyn ItemHandle>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Clear any presence inlays from the editor we're switching away
        // from — otherwise they'd linger forever, since nothing else will
        // ever touch that editor entity again.
        if let Some(old_editor) = self.current_editor.take().and_then(|e| e.upgrade()) {
            if !self.spliced_inlay_ids.is_empty() {
                let old_ids = std::mem::take(&mut self.spliced_inlay_ids);
                old_editor.update(cx, |editor, cx| editor.splice_inlays(&old_ids, Vec::new(), cx));
            }
        }

        self.current_project_path = active_pane_item
            .and_then(|item| item.project_path(cx))
            .map(|path| path.path.display(PathStyle::local()).into_owned());

        let editor = active_pane_item.and_then(|item| item.act_as::<Editor>(cx));
        self.current_editor = editor.as_ref().map(Entity::downgrade);

        self._observe_active_editor = editor.map(|editor| {
            cx.subscribe_in(&editor, window, |this, _editor, event, window, cx| {
                if matches!(event, EditorEvent::SelectionsChanged { .. }) {
                    this.schedule_heartbeat(window, cx);
                }
            })
        });

        self.schedule_heartbeat(window, cx);
        self.refresh_presence_inlays(cx);
    }

    fn hide_setting(&self, _cx: &App) -> Option<workspace::HideStatusItem> {
        // Always visible — this is the always-on presence indicator, not a
        // per-file tool status.
        None
    }
}
