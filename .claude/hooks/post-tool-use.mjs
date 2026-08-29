#!/usr/bin/env node
// PostToolUse (matcher: *): log real file edits and commands to Project
// Brain's generic feed (terse, for the "Live feed" panel), and separately
// report every tool call as a full-fidelity turn (untruncated tool_name +
// tool_input, plus the tool_response if present) so it renders in the same
// transcript view a self-reporting agent's report_turn calls do. This is the
// ground-truth path: real tool_input straight from Claude Code's own hook
// payload, not something the model chose to summarize.
import { readStdinJson, readState, api, runHook, PROJECT_ID, ACTOR_ID } from "./rally-common.mjs";

const FEED_TOOLS = new Set(["Edit", "Write", "MultiEdit", "Bash"]);
// Result content can be huge (a big file read, a long command's stdout) —
// capped so one tool call can't balloon the transcript.
const MAX_RESULT_CHARS = 4000;

runHook(async () => {
  const input = await readStdinJson();
  if (!PROJECT_ID || !ACTOR_ID) return;

  const state = readState(input.session_id);
  if (!state) return;

  if (FEED_TOOLS.has(input.tool_name)) {
    let entityType;
    let entityId;
    let verb;
    let summary;

    if (input.tool_name === "Bash") {
      const command = (input.tool_input?.command || "").slice(0, 200);
      entityType = "command";
      entityId = command;
      verb = "ran";
      summary = `Claude Code ran: ${command}`;
    } else {
      const filePath = input.tool_input?.file_path || "unknown file";
      entityType = "file";
      entityId = filePath;
      verb = "edited";
      summary = `Claude Code edited ${filePath}`;
    }

    await api("POST", `/projects/${PROJECT_ID}/events`, {
      actor_id: ACTOR_ID,
      entity_type: entityType,
      entity_id: entityId,
      verb,
      summary,
      agent_job_id: state.jobId,
    });

    await api("PATCH", `/agent-jobs/${state.jobId}`, {
      actor_id: ACTOR_ID,
      context_snapshot: { last_tool: input.tool_name, last_action: summary },
    });
  }

  // Full-fidelity transcript turn, for every tool call, not just the ones
  // that hit the terse feed above.
  if (input.tool_name) {
    await api("POST", `/agent-jobs/${state.jobId}/turns/report`, {
      actor_id: ACTOR_ID,
      role: "assistant",
      tool_name: input.tool_name,
      tool_use_id: input.tool_use_id,
      tool_input: input.tool_input,
    });

    if (input.tool_response !== undefined) {
      const resultText =
        typeof input.tool_response === "string"
          ? input.tool_response
          : JSON.stringify(input.tool_response);
      await api("POST", `/agent-jobs/${state.jobId}/turns/report`, {
        actor_id: ACTOR_ID,
        role: "tool",
        tool_name: input.tool_name,
        tool_use_id: input.tool_use_id,
        content: resultText.slice(0, MAX_RESULT_CHARS),
      });
    }
  }
});
