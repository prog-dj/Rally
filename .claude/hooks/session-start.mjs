#!/usr/bin/env node
// SessionStart: register an agent job for this Claude Code session and hand
// back the project's shared-memory brief as additional context, so the
// session starts already aware of open tasks/investigations.
import { readStdinJson, writeState, api, runHook, PROJECT_ID, ACTOR_ID } from "./rally-common.mjs";

runHook(async () => {
  const input = await readStdinJson();
  if (!PROJECT_ID || !ACTOR_ID) return;

  const job = await api("POST", `/projects/${PROJECT_ID}/agent-jobs`, {
    actor_id: ACTOR_ID,
    goal: `Claude Code session in ${input.cwd || "unknown directory"}`,
    context_snapshot: { source: input.source || "startup" },
  });
  await api("PATCH", `/agent-jobs/${job.id}`, { actor_id: ACTOR_ID, status: "running" });

  writeState(input.session_id, { jobId: job.id, lastSeenMessageCount: 0 });

  const brief = await api("GET", `/projects/${PROJECT_ID}/context`);
  const openTasks =
    brief.open_tasks.map((t) => `- [${t.status}] ${t.title}`).join("\n") || "(none)";
  const activeInvestigations =
    brief.active_investigations.map((i) => `- ${i.title}`).join("\n") || "(none)";

  const context = [
    "Project Brain shared memory for this project:",
    "",
    "Open tasks:",
    openTasks,
    "",
    "Active investigations:",
    activeInvestigations,
    "",
    `(This session is registered as agent job ${job.id} — file edits, commands, ` +
      "and steering messages sent via the Project Brain dashboard/Zed panel will show up here.)",
  ].join("\n");

  console.log(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: context },
    }),
  );
});
