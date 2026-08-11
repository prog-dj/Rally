//! Presence/feed state shared outside `ProjectBrainPanel` itself. The panel
//! is still the thing that owns the websocket connection and its own
//! render-facing copies of this data (unchanged, to avoid touching every
//! render call site in that file) — this is a second, thin mirror any other
//! UI surface (status bar item, editor gutter) can read and subscribe to
//! without depending on the panel crate's `Entity<ProjectBrainPanel>`, which
//! may not exist yet (e.g. before the dock panel has been opened once).

use gpui::{App, AppContext, Context, Entity, EventEmitter, Global};

use crate::{ActorInfo, ConnectionStatus, FeedEvent, PresenceEntry};

#[derive(Default)]
pub struct RallyLiveState {
    pub presence: Vec<PresenceEntry>,
    pub connection_status: Option<ConnectionStatus>,
    pub feed_events: Vec<FeedEvent>,
    pub actors: Vec<ActorInfo>,
    /// This session's own identity — mirrored here so surfaces outside
    /// `ProjectBrainPanel` (e.g. the presence-heartbeat sender in the
    /// status bar item) can authenticate without depending on the panel's
    /// own `Entity`, which may not have been constructed yet.
    pub actor_id: Option<String>,
    pub actor_token: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum RallyLiveStateEvent {
    Updated,
}

impl EventEmitter<RallyLiveStateEvent> for RallyLiveState {}

impl RallyLiveState {
    pub fn set_presence(&mut self, presence: Vec<PresenceEntry>, cx: &mut Context<Self>) {
        self.presence = presence;
        cx.emit(RallyLiveStateEvent::Updated);
        cx.notify();
    }

    pub fn set_connection_status(&mut self, status: ConnectionStatus, cx: &mut Context<Self>) {
        self.connection_status = Some(status);
        cx.emit(RallyLiveStateEvent::Updated);
        cx.notify();
    }

    pub fn set_actors(&mut self, actors: Vec<ActorInfo>, cx: &mut Context<Self>) {
        self.actors = actors;
        cx.emit(RallyLiveStateEvent::Updated);
        cx.notify();
    }

    pub fn set_actor_identity(
        &mut self,
        actor_id: Option<String>,
        actor_token: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.actor_id = actor_id;
        self.actor_token = actor_token;
        cx.emit(RallyLiveStateEvent::Updated);
        cx.notify();
    }

    /// Bulk replace, for the initial feed-history fetch (as opposed to
    /// `push_feed_event`, used for single live events off the websocket).
    pub fn set_feed_events(&mut self, events: Vec<FeedEvent>, cx: &mut Context<Self>) {
        self.feed_events = events;
        cx.emit(RallyLiveStateEvent::Updated);
        cx.notify();
    }

    /// Mirrors `ProjectBrainPanel::push_feed_event`'s own dedupe-by-id logic
    /// so both copies stay in agreement about what counts as a new event.
    pub fn push_feed_event(&mut self, event: FeedEvent, cx: &mut Context<Self>) {
        if !self.feed_events.iter().any(|e| e.id == event.id) {
            self.feed_events.insert(0, event);
            cx.emit(RallyLiveStateEvent::Updated);
            cx.notify();
        }
    }
}

struct GlobalRallyLiveState(Entity<RallyLiveState>);

impl Global for GlobalRallyLiveState {}

/// Creates the shared live-state entity and installs it as a global. Safe to
/// call more than once (e.g. if `project_brain_panel::init` ever runs twice
/// in tests) — later calls simply replace the global with a fresh, empty
/// entity, matching how `ProjectBrainPanel` itself would start from scratch.
pub fn init(cx: &mut App) {
    let state = cx.new(|_cx| RallyLiveState::default());
    cx.set_global(GlobalRallyLiveState(state));
}

impl RallyLiveState {
    pub fn global(cx: &App) -> Entity<RallyLiveState> {
        cx.global::<GlobalRallyLiveState>().0.clone()
    }

    pub fn try_global(cx: &App) -> Option<Entity<RallyLiveState>> {
        cx.try_global::<GlobalRallyLiveState>()
            .map(|g| g.0.clone())
    }
}
