//! A one-time first-run tab explaining that this isn't stock Zed — it's a
//! multiplayer surface for people *and* agents. Gated on its own kvp key
//! (distinct from stock Zed's `onboarding::FIRST_OPEN`) so dismissing one
//! doesn't suppress the other, and so re-running Zed's own onboarding flow
//! doesn't bring this back.
//!
//! Deliberately not a `SerializableItem` — this tab is a one-time intro, not
//! a persistent workspace item, so there is nothing to restore across
//! restarts once it has been shown and closed.

use std::sync::atomic::{AtomicBool, Ordering};

use db::kvp::KeyValueStore;
use gpui::{
    App, Context, EventEmitter, FocusHandle, Focusable, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, SharedString, Styled, WeakEntity, Window,
};
use ui::{prelude::*, Divider, IconName};
use workspace::{
    item::{Item, ItemEvent},
    Workspace,
};

use crate::ToggleProjectBrainPanel;

const RALLY_FIRST_OPEN: &str = "rally_first_open";

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, window, cx: &mut Context<Workspace>| {
        let Some(window) = window else { return };
        maybe_show_rally_welcome(workspace, window, cx);
    })
    .detach();
}

fn maybe_show_rally_welcome(workspace: &mut Workspace, window: &mut Window, cx: &mut Context<Workspace>) {
    // Only ever attempted once per process — later windows/workspaces
    // opened in this same run shouldn't each re-check and potentially
    // re-open it in a race with the kvp write below.
    static ATTEMPTED_THIS_RUN: AtomicBool = AtomicBool::new(false);
    if ATTEMPTED_THIS_RUN.swap(true, Ordering::SeqCst) {
        return;
    }

    let kvp = KeyValueStore::global(cx);
    if !matches!(kvp.read_kvp(RALLY_FIRST_OPEN), Ok(None)) {
        return;
    }

    let welcome = cx.new(|cx| RallyWelcome::new(workspace, cx));
    workspace.add_item_to_active_pane(Box::new(welcome), None, true, window, cx);

    db::write_and_log(cx, move || async move {
        kvp.write_kvp(RALLY_FIRST_OPEN.to_string(), "false".to_string())
            .await
    });
}

pub struct RallyWelcome {
    focus_handle: FocusHandle,
    #[allow(dead_code)]
    workspace: WeakEntity<Workspace>,
}

impl RallyWelcome {
    fn new(workspace: &Workspace, cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            workspace: workspace.weak_handle(),
        }
    }
}

impl Render for RallyWelcome {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .track_focus(&self.focus_handle(cx))
            .size_full()
            .justify_center()
            .bg(cx.theme().colors().editor_background)
            .child(
                v_flex()
                    .id("rally-welcome-content")
                    .p_8()
                    .max_w_128()
                    .size_full()
                    .gap_6()
                    .justify_center()
                    .overflow_y_scroll()
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .child(
                                        Icon::new(IconName::UserGroup)
                                            .size(IconSize::Medium)
                                            .color(Color::Accent),
                                    )
                                    .child(Headline::new("Welcome to Rally")),
                            )
                            .child(
                                Label::new(
                                    "A shared workspace for people and agents to build software together — not just an editor with a chat panel bolted on.",
                                )
                                .color(Color::Muted),
                            ),
                    )
                    .child(Divider::horizontal())
                    .child(
                        v_flex()
                            .gap_3()
                            .child(
                                Label::new("Every task, investigation, decision, and agent job here is visible to everyone on the project — human or agent — in real time.")
                            )
                            .child(
                                Label::new("Connecting an agent is a follow-up step a human takes, not a separate sign-in — open the Rally panel and use \"Connect an agent\" once you're set up yourself.")
                                    .color(Color::Muted),
                            ),
                    )
                    .child(
                        Button::new("rally-welcome-open-panel", "Open Rally Panel")
                            .full_width()
                            .style(ButtonStyle::Filled)
                            .start_icon(Icon::new(IconName::UserGroup))
                            .on_click(|_, window, cx| {
                                window.dispatch_action(Box::new(ToggleProjectBrainPanel), cx);
                            }),
                    ),
            )
    }
}

impl Focusable for RallyWelcome {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<ItemEvent> for RallyWelcome {}

impl Item for RallyWelcome {
    type Event = ItemEvent;

    fn tab_content_text(&self, _detail: usize, _cx: &App) -> SharedString {
        "Welcome to Rally".into()
    }

    fn telemetry_event_text(&self) -> Option<&'static str> {
        Some("Rally Welcome Opened")
    }

    fn show_toolbar(&self) -> bool {
        false
    }

    fn to_item_events(event: &Self::Event, f: &mut dyn FnMut(ItemEvent)) {
        f(*event)
    }
}
