//! Proves the actual mechanism behind "an agent as a live collaborator,
//! not a side-panel transcript": a peer with no Editor, no Workspace, no
//! Window at all — nothing but Client + Project + Buffer, which is all a
//! headless agent process would ever have — can join a shared project and
//! edit a buffer such that another real peer's view of that buffer updates
//! live. This is the blueprint a real headless collab client (running
//! wherever the agent runtime lives, not inside Zed's UI) would build on:
//! Client::new/connect, ActiveCall::join_project, Project::remote,
//! BufferStore::open_buffer, Buffer::edit — all real production code
//! paths, exercised here without a GUI window via TestServer/TestClient
//! (see test_server.rs), which is exactly what Zed's own multiplayer
//! feature tests already prove works headlessly.
use crate::TestServer;
use call::ActiveCall;
use gpui::TestAppContext;
use serde_json::json;
use util::{path, rel_path::rel_path};

#[gpui::test]
async fn test_headless_peer_edits_propagate_live_to_a_real_collaborator(
    cx_human: &mut TestAppContext,
    cx_agent: &mut TestAppContext,
) {
    let executor = cx_human.executor();
    let mut server = TestServer::start(executor.clone()).await;
    let human = server.create_client(cx_human, "dana").await;
    let agent = server.create_client(cx_agent, "checkout_bot").await;
    server
        .make_contacts(&mut [(&human, cx_human), (&agent, cx_agent)])
        .await;

    let active_call_human = cx_human.read(ActiveCall::global);
    let active_call_agent = cx_agent.read(ActiveCall::global);

    human
        .fs()
        .insert_tree(
            path!("/project"),
            json!({
                "checkout.ts": "// TODO: implement checkout\n",
            }),
        )
        .await;

    // Dana (a real collaborator) shares the project, same as any human
    // collaboration session today.
    let (project_human, worktree_id) = human.build_local_project(path!("/project"), cx_human).await;
    active_call_human
        .update(cx_human, |call, cx| {
            call.invite(agent.user_id().unwrap(), Some(project_human.clone()), cx)
        })
        .await
        .unwrap();

    // "checkout_bot" joins as a peer — this is the entire connection
    // surface a headless agent runtime needs: no Editor, no Workspace, no
    // Window, just accepting the call and joining the shared project.
    let incoming_call_agent = active_call_agent.read_with(cx_agent, |call, _| call.incoming());
    executor.run_until_parked();
    let call = incoming_call_agent.borrow().clone().unwrap();
    let initial_project = call.initial_project.unwrap();
    active_call_agent
        .update(cx_agent, |call, cx| call.accept_incoming(cx))
        .await
        .unwrap();
    let project_agent = agent.join_remote_project(initial_project.id, cx_agent).await;

    executor.run_until_parked();

    let buffer_human = project_human
        .update(cx_human, |p, cx| {
            p.open_buffer((worktree_id, rel_path("checkout.ts")), cx)
        })
        .await
        .unwrap();
    let buffer_agent = project_agent
        .update(cx_agent, |p, cx| {
            p.open_buffer((worktree_id, rel_path("checkout.ts")), cx)
        })
        .await
        .unwrap();

    // The agent edits the shared buffer directly — no GUI, no Editor view,
    // exactly what a headless tool-use loop would do after deciding on an
    // edit. This is the one line that stands in for "the agent does work."
    buffer_agent.update(cx_agent, |buffer, cx| {
        buffer.edit([(0..0, "// checkout_bot: added Stripe integration\n")], None, cx)
    });

    executor.run_until_parked();

    // Dana's own view of the buffer reflects it live, with no action on her
    // part beyond having the file open — the same experience as watching a
    // human collaborator type.
    buffer_human.read_with(cx_human, |buffer, _| {
        assert!(
            buffer.text().starts_with("// checkout_bot: added Stripe integration\n"),
            "expected the agent's edit to sync to the human's view, got: {:?}",
            buffer.text()
        );
    });
}
