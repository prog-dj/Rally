#!/usr/bin/env node
// SessionStart: register an agent job for this Claude Code session and hand
// back the project's shared-memory brief as additional context, so the
// session starts already aware of open tasks/investigations.
import path from "node:path";
import { readStdinJson, writeState, api, runHook, getPersonName, PROJECT_ID, ACTOR_ID } from "./rally-common.mjs";

runHook(async () => {
  const input = await readStdinJson();
  if (!PROJECT_ID || !ACTOR_ID) return;

  // "Name|Provider|Title" — the same prefix is handed to the agent below so
  // its own task-level job (created via the create_agent_job tool) matches,
  // and so two concurrent sessions in the same repo/directory are actually
  // distinguishable in a job list instead of both reading identically.
  const titlePrefix = `${getPersonName()}|Claude Code|`;
  const dirName = input.cwd ? path.basename(input.cwd) : "unknown directory";

  const job = await api("POST", `/projects/${PROJECT_ID}/agent-jobs`, {
    actor_id: ACTOR_ID,
    goal: `${titlePrefix}Session in ${dirName}`,
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
      "and steering messages sent via the Project Brain dashboard/Zed panel will show up here. " +
      `Its title starts with "${titlePrefix}" — reuse that exact prefix if you register a ` +
      `task-level job (call create_agent_job ONCE, as your first action, for the actual task, ` +
      `with goal formatted as "${titlePrefix}<a few words on the task>", e.g. ` +
      `"${titlePrefix}Review Rally project context" — not a call per message. For every later ` +
      "request in this same session, keep using that same task job id instead of creating " +
      "another one; only call create_agent_job again if the human starts a genuinely new, " +
      "unrelated task later in this session.)",
  ].join("\n");

  console.log(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: context },
    }),
  );
});
