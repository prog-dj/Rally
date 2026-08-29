#!/usr/bin/env node
// UserPromptSubmit: before each turn, check whether anyone sent a steering
// message via the Project Brain dashboard/Zed panel since the last turn, and
// if so, fold it into context. This is what makes claim/messages more than
// a write-only log — the agent actually sees it. Also reports the human's
// actual prompt text as a "user" transcript turn — the ground-truth
// counterpart to report_turn's self-reported user turns, straight from the
// real input, not the agent's own paraphrase of what was asked.
import { readStdinJson, readState, writeState, api, runHook, ACTOR_ID } from "./rally-common.mjs";

runHook(async () => {
  const input = await readStdinJson();
  if (!ACTOR_ID) return;

  const state = readState(input.session_id);
  if (!state) return;

  if (input.prompt) {
    await api("POST", `/agent-jobs/${state.jobId}/turns/report`, {
      actor_id: ACTOR_ID,
      role: "user",
      content: input.prompt,
    });
  }

  const detail = await api("GET", `/agent-jobs/${state.jobId}`);
  const newMessages = detail.messages.slice(state.lastSeenMessageCount);
  writeState(input.session_id, { ...state, lastSeenMessageCount: detail.messages.length });

  if (newMessages.length === 0) return;

  const context = [
    "Steering messages received via Project Brain since your last turn:",
    ...newMessages.map((m) => `- ${m.content}`),
  ].join("\n");

  console.log(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: "UserPromptSubmit", additionalContext: context },
    }),
  );
});
