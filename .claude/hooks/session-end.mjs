#!/usr/bin/env node
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
