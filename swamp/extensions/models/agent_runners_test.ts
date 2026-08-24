/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Unit tests for the per-agent halves of a phase run.
 *
 * The flow's own tests drive `agent_task.ts` end to end against a fake CLI and
 * cover what a phase does with an answer. These cover the seam underneath: how
 * each agent is invoked, where its answer is found, and which of its failures
 * another attempt might survive — the four things that differ between them and
 * nothing else.
 *
 * @module
 */
import { assert, assertEquals, assertStringIncludes } from "jsr:@std/assert@1";
import {
  AmpRunner,
  ClaudeRunner,
  extractJson,
  runnerFor,
  saidOnTheWayOut,
  terminalReason,
} from "./agent_runners.ts";

const SCHEMA = {
  type: "object",
  properties: { succeeded: { type: "boolean" } },
  required: ["succeeded"],
};

function invocation(overrides: Record<string, unknown> = {}) {
  return {
    schema: SCHEMA,
    model: "",
    permissionMode: "auto",
    resumeSession: "",
    ...overrides,
  };
}

Deno.test("an unknown runner names the ones that exist", () => {
  // The alternative is a phase that starts, spawns nothing recognisable and
  // fails minutes later on whatever the shell made of an empty command.
  const error = (() => {
    try {
      runnerFor("codex");
      return null;
    } catch (e) {
      return e as Error;
    }
  })();
  assertStringIncludes(error?.message ?? "", "Unknown agent runner");
  assertStringIncludes(error?.message ?? "", "amp, claude");
});

Deno.test("Claude is handed the schema and carries none in the prompt", () => {
  const argv = ClaudeRunner.argv(invocation());

  // The CLI holds the agent to the shape, which is what `enforcesSchema` says.
  assertEquals(argv.includes("--json-schema"), true);
  assertEquals(JSON.parse(argv[argv.indexOf("--json-schema") + 1]), SCHEMA);
  assertEquals(ClaudeRunner.enforcesSchema, true);
  // So the prompt does not restate it: two copies of one fact drift apart, and
  // the copy in argv is the one that is enforced.
  assertEquals(ClaudeRunner.promptSuffix(invocation()), "");
});

Deno.test("Claude's model and session are flags", () => {
  const argv = ClaudeRunner.argv(
    invocation({ model: "claude-opus-5", resumeSession: "abc" }),
  );
  assertEquals(argv[argv.indexOf("--model") + 1], "claude-opus-5");
  assertEquals(argv[argv.indexOf("--resume") + 1], "abc");
  // An empty model leaves the CLI's own default rather than naming one.
  assertEquals(ClaudeRunner.argv(invocation()).includes("--model"), false);
});

Deno.test("Amp carries the schema in the prompt, because it has no flag for it", () => {
  const argv = AmpRunner.argv(invocation());

  assertEquals(argv.includes("--execute"), true);
  assertEquals(argv.includes("--stream-json"), true);
  assertEquals(argv.includes("--no-ide"), true);
  assertEquals(argv.includes("--json-schema"), false);
  assertEquals(AmpRunner.enforcesSchema, false);

  const suffix = AmpRunner.promptSuffix(invocation());
  assertStringIncludes(suffix, '"succeeded"');
  assertStringIncludes(suffix, "single JSON object");
});

Deno.test("a resumed Amp phase continues its thread", () => {
  // Amp has no `--resume`: the session is a thread and continuing it is a
  // subcommand. Without this the option was silently Claude-only — an Amp
  // phase asked to resume started fresh, having lost everything the session it
  // was meant to continue had decided.
  const argv = AmpRunner.argv(invocation({ resumeSession: "T-7" }));
  assertEquals(argv.slice(0, 3), ["threads", "continue", "T-7"]);
  assertEquals(argv.includes("--execute"), true);
  // And a phase that is not resuming still starts a plain execute turn.
  assertEquals(AmpRunner.argv(invocation())[0], "--execute");
});

Deno.test("a repair message is bound to its flag", () => {
  // `threads continue` takes a variadic list of threads, so a message left
  // trailing is read as another thread id and the correction is never asked
  // for.
  const argv = AmpRunner.repairArgv("T-1", "wrong shape")!;
  assertEquals(argv[argv.indexOf("--execute") + 1], "wrong shape");
  assertEquals(argv[argv.length - 1] === "wrong shape", false);
  assertEquals(argv.includes("--no-ide"), true);
});

Deno.test("Amp's model argument is a mode", () => {
  // Amp has no flag naming a model: `--mode` picks the model, the system
  // prompt and the tool set together, so that is what `model` means there.
  const argv = AmpRunner.argv(invocation({ model: "high" }));
  assertEquals(argv[argv.indexOf("--mode") + 1], "high");
  assertEquals(AmpRunner.argv(invocation()).includes("--mode"), false);
});

Deno.test("Claude's answer is the structured output, Amp's is what it said", () => {
  assertEquals(
    ClaudeRunner.answerOf({ structured_output: { succeeded: true } }),
    { succeeded: true },
  );
  // Amp prints only the last assistant message in execute mode, which is why
  // reading `result` is reading the answer and not searching a transcript.
  assertEquals(
    AmpRunner.answerOf({ result: '{"succeeded": true}' }),
    { succeeded: true },
  );
  assertEquals(AmpRunner.answerOf({ result: "I could not do it." }), null);
});

Deno.test("an answer wrapped in a fence or a sentence is still an answer", () => {
  // Asked for bare JSON an agent frequently gives a fenced block, and
  // sometimes a sentence in front of it. Neither is worth failing a
  // forty-minute phase over.
  assertEquals(
    extractJson('```json\n{"succeeded": true}\n```'),
    { succeeded: true },
  );
  assertEquals(
    extractJson('Here is the result:\n\n{"succeeded": false}\n'),
    { succeeded: false },
  );
  assertEquals(extractJson('```\n{"a": 1}\n```'), { a: 1 });
  // Prose with no object in it is not recoverable, and must not be guessed at.
  assertEquals(extractJson("The port is done."), null);
  assertEquals(extractJson(""), null);
  // A bare array is not the object shape every phase answers in.
  assertEquals(extractJson("[1, 2]"), null);
});

Deno.test("only Amp can be asked to restate its answer", () => {
  // Claude's answer matched the schema the CLI was holding it to, so there is
  // nothing to repair; Amp's was checked afterwards, so there is.
  assertEquals(ClaudeRunner.repairArgv("abc", "wrong"), null);
  const argv = AmpRunner.repairArgv("T-1", "wrong shape");
  assertEquals(argv?.slice(0, 3), ["threads", "continue", "T-1"]);
  assertEquals(argv?.includes("wrong shape"), true);
});

Deno.test("a rate limit is retryable on either agent", () => {
  // Claude says so in a field; Amp reports it in prose, with no status code to
  // read, so the two are recognised differently and mean the same thing.
  assertEquals(
    ClaudeRunner.isRetryable({ terminal_reason: "api_error" }),
    true,
  );
  assertEquals(ClaudeRunner.isRetryable({ api_error_status: 429 }), true);
  assertEquals(
    AmpRunner.isRetryable({
      subtype: "error_during_execution",
      error: "OpenAI WebSocket connection timed out after 15000ms",
    }),
    true,
  );
  assertEquals(AmpRunner.isRetryable({ api_error_status: 503 }), true);
});

Deno.test("a failure the agent produced is retryable on neither", () => {
  // The case a budget exists to stop: it ran to completion and reported that
  // it could not do the job, and another attempt is told the same thing.
  assertEquals(
    ClaudeRunner.isRetryable({
      terminal_reason: "completed",
      subtype: "error_during_execution",
    }),
    false,
  );
  assertEquals(
    AmpRunner.isRetryable({
      subtype: "error_during_execution",
      error: "I could not find the module.",
    }),
    false,
  );
});

Deno.test("the reason a run ended is named, whichever field carries it", () => {
  // Claude reports `subtype: "success"` on a run it flagged as an error, so a
  // message built from it reads "failed: success"; Amp leaves the reason in
  // `error` and reports nothing in `terminal_reason`.
  assertEquals(
    terminalReason({ subtype: "success", terminal_reason: "api_error" }),
    "api_error",
  );
  assertEquals(
    terminalReason({ subtype: "error_during_execution" }),
    "error_during_execution",
  );
  assertStringIncludes(
    saidOnTheWayOut({ error: "connection timed out" }),
    "connection timed out",
  );
  assertStringIncludes(
    saidOnTheWayOut({ result: "You've hit your session limit" }),
    "session limit",
  );
  assertEquals(saidOnTheWayOut({}), "");
});

Deno.test("Claude reports its own cost; Amp has to be asked", async () => {
  assertEquals(
    await ClaudeRunner.costOf(
      { total_cost_usd: 2.02 },
      "claude",
      ".",
      new AbortController().signal,
    ),
    2.02,
  );
  // Amp's result event carries no cost at all, so an absent one is null rather
  // than zero: a free run and an unknown one are not the same reading.
  assertEquals(
    await ClaudeRunner.costOf({}, "claude", ".", new AbortController().signal),
    null,
  );
  // Amp has to spawn for it, and takes the run's signal so that a stalled
  // lookup cannot outlive the phase whose answer is already in hand.
  assertEquals(AmpRunner.costOf.length, 4);
});
