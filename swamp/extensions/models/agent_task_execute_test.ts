/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the phase runner's `execute`.
 *
 * What the parser tests cannot reach is everything that happens around the
 * agent: that the schema and the prompt actually reach the process, that an
 * answer is refused when it does not match the schema it was given, and that
 * the transcript survives every way a phase can fail. Those are the paths that
 * matter here, because a phase that fails quietly is indistinguishable from one
 * that passed until something much later reads the result and finds it empty.
 *
 * A phase is driven against a fake `claude` in a temporary directory, so a case
 * takes milliseconds and can produce failures that are impractical to induce
 * against the real CLI.
 *
 * @module
 */
import { assert, assertEquals, assertStringIncludes } from "jsr:@std/assert@1";
import { model } from "./agent_task.ts";
import {
  captureError,
  makeContext,
  type Recorded,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

/** A well-formed answer for the review phase. */
const CLEAN_REVIEW = {
  succeeded: true,
  summary: "Nothing to report.",
  blockers: [],
  clean: true,
  findings: [],
  reviewed: ["src/union.rs"],
};

/** Global arguments pointing at a fake CLI, with every default filled in. */
function globals(agentBin: string, overrides: Record<string, unknown> = {}) {
  return {
    runner: "claude",
    agentBin,
    model: "",
    permissionMode: "auto",
    workingDir: ".",
    timeout: 30_000,
    // Retrying is off unless a case is about it, so that a case about how a
    // failure is reported does not silently run three times and record three
    // failures; and the wait is milliseconds when it is on, since what the
    // backoff is measured in is not what those cases are testing.
    maxRetries: 0,
    retryBackoffMs: 5,
    ...overrides,
  };
}

/** Method arguments with every default filled in, as swamp supplies them. */
// deno-lint-ignore no-explicit-any
function args(overrides: Record<string, unknown> = {}): any {
  return {
    task: "Port the union iterator",
    context: {},
    instructions: "",
    resumeSession: "",
    expectedTree: "",
    digestWorkflow: "verify",
    model: "",
    ignoreFailure: false,
    quiet: true,
    ...overrides,
  };
}

/**
 * Write a fake `claude` that records its argument vector and replays `events`
 * as a stream-json transcript.
 *
 * Arguments are recorded NUL-separated because the prompt is one argument
 * containing newlines — split on anything else and a test would be reading the
 * prompt's second line as the next flag.
 */
async function fakeClaude(
  dir: string,
  events: unknown[],
  options: { exitCode?: number; before?: string } = {},
): Promise<string> {
  const lines = events.map((event) => JSON.stringify(event));
  return await writeScript(
    dir,
    "fake-claude",
    [
      `printf '%s\\0' "$@" > "${dir}/argv"`,
      `cat > "${dir}/prompt"`,
      // Whatever this phase is meant to have done to the tree, so that what the
      // model derives from the checkout can be compared with what the agent
      // claims to have changed.
      options.before ?? "",
      ...lines.map((line) => `cat <<'EVENT'\n${line}\nEVENT`),
      `exit ${options.exitCode ?? 0}`,
    ].join("\n"),
  );
}

/**
 * Write a fake `claude` that answers differently on each attempt.
 *
 * For the retry cases, which are about what happens between attempts and so
 * cannot be driven by a fake that replays one transcript. The last entry
 * answers every attempt after it, so an always-failing agent is one entry.
 *
 * The attempt number is kept in a file rather than in the environment because
 * each attempt is a fresh process and the counter has to outlive it.
 */
async function fakeClaudeAttempts(
  dir: string,
  attempts: Array<
    { events: unknown[]; exitCode?: number; before?: string }
  >,
): Promise<string> {
  const branches = attempts.map((attempt, i) => {
    const last = i === attempts.length - 1;
    const body = [
      attempt.before ?? "",
      ...attempt.events.map((event) =>
        `cat <<'EVENT'\n${JSON.stringify(event)}\nEVENT`
      ),
      `exit ${attempt.exitCode ?? 0}`,
    ].filter((line) => line).join("\n");
    return `${last ? "*" : i + 1})\n${body}\n;;`;
  });
  return await writeScript(
    dir,
    "fake-claude",
    [
      `n=$(cat "${dir}/attempts" 2>/dev/null || echo 0)`,
      "n=$((n + 1))",
      `echo "$n" > "${dir}/attempts"`,
      `printf '%s\\0' "$@" > "${dir}/argv"`,
      `cat > "${dir}/prompt"`,
      'case "$n" in',
      ...branches,
      "esac",
    ].join("\n"),
  );
}

/** How many times the fake CLI was run. */
async function attemptsMade(dir: string): Promise<number> {
  try {
    return Number((await Deno.readTextFile(`${dir}/attempts`)).trim());
  } catch {
    return 0;
  }
}

/** A terminal event for a run the API refused, which another attempt may not. */
function rateLimited(): Record<string, unknown> {
  return {
    type: "result",
    subtype: "success",
    is_error: true,
    terminal_reason: "api_error",
    api_error_status: 429,
    result: "You've hit your session limit \u00b7 resets 6:50pm",
    session_id: "session-1",
    num_turns: 27,
    total_cost_usd: 2.02,
    permission_denials: [],
  };
}

/** The prompt the fake was given on stdin. */
async function promptOf(dir: string): Promise<string> {
  return await Deno.readTextFile(`${dir}/prompt`);
}

/** The argument vector the fake was called with. */
async function argv(dir: string): Promise<string[]> {
  const raw = await Deno.readTextFile(`${dir}/argv`);
  const parts = raw.split("\0");
  parts.pop();
  return parts;
}

/** A terminal result event wrapping `answer`. */
function resultEvent(
  answer: unknown,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    type: "result",
    subtype: "success",
    is_error: false,
    session_id: "session-1",
    num_turns: 4,
    total_cost_usd: 0.25,
    permission_denials: [],
    structured_output: answer,
    ...overrides,
  };
}

/** The single result a phase is expected to have written. */
function resultOf(recorded: Recorded): Record<string, unknown> {
  assertEquals(recorded.resources.length, 1);
  return recorded.resources[0].data;
}

/**
 * Assert that a phase recorded a failed attempt and no answer.
 *
 * A phase that dies writes one record, under `failure` rather than its own
 * spec: there is no answer of that shape to write, but the attempt still
 * happened and a caller counting them has to be able to see it.
 */
function failureOf(recorded: Recorded, kind: string): Record<string, unknown> {
  const specs = recorded.resources.map((r) => r.spec);
  assertEquals(specs, ["failure"], "expected only a failure record");
  const data = recorded.resources[0].data;
  assertEquals(data.phase, kind);
  return data;
}

Deno.test("a completed phase stores its answer and what the run cost", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [
      { type: "system", subtype: "init" },
      {
        type: "assistant",
        message: { content: [{ type: "text", text: "Reading the code" }] },
      },
      resultEvent(CLEAN_REVIEW, {
        permission_denials: [{ tool_name: "Bash", reason: "not allowed" }],
      }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    const handles = await model.methods.review.execute(args(), context);

    const result = resultOf(recorded);
    assertEquals(result.clean, true);
    assertEquals(result.reviewed, ["src/union.rs"]);

    // The metadata sits under its own key so that no field the agent fills in
    // can collide with one the CLI reports.
    const agent = result.agent as Record<string, unknown>;
    assertEquals(agent.sessionId, "session-1");
    assertEquals(agent.numTurns, 4);
    assertEquals(agent.costUsd, 0.25);
    assertEquals(agent.permissionMode, "auto");
    assertEquals(agent.permissionDenials, [
      { toolName: "Bash", reason: "not allowed" },
    ]);

    // The result is stored under the phase's own spec, so two phases cannot
    // overwrite each other.
    assertEquals(recorded.resources[0].spec, "review");
    assertEquals(handles.dataHandles.length, 2);

    // Every event reaches the transcript, progress ones included.
    assertEquals(recorded.files.length, 1);
    assertEquals(recorded.files[0].spec, "transcript");
    assertEquals(recorded.files[0].lines.length, 3);
  });
});

Deno.test("the prompt and the phase's schema reach the process", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context } = makeContext(dir, globals(bin));

    await model.methods.review.execute(
      args({ context: { design: { planPath: "union_plan.md" } } }),
      context,
    );

    const prompt = await promptOf(dir);
    assertStringIncludes(prompt, "Port the union iterator");
    assertStringIncludes(prompt, "union_plan.md");

    const passed = await argv(dir);

    const schema = JSON.parse(passed[passed.indexOf("--json-schema") + 1]);
    assert(schema.required.includes("clean"));
  });
});

Deno.test("a context far larger than a command-line argument still runs", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context, recorded } = makeContext(dir, globals(bin));

    // Linux caps a single argument at 128KB. The prompt therefore goes over
    // stdin, because a phase's context is the previous phases' output and grows
    // with the work — and an over-long argument does not fail as a bad answer,
    // it fails as the process never starting, which reads like the agent dying
    // for no reason.
    const huge = "x".repeat(300_000);
    await model.methods.review.execute(
      args({ context: { earlier: huge } }),
      context,
    );

    assertEquals(resultOf(recorded).clean, true);
    assertStringIncludes(await promptOf(dir), huge.slice(0, 1000));
  });
});

Deno.test("an answer that does not match the schema is refused", async () => {
  await withTempRepo(async (dir) => {
    // `clean` is what ends the review loop. An answer missing it would leave
    // the loop reading an absent field, so the phase fails instead.
    const bin = await fakeClaude(dir, [
      resultEvent({ succeeded: true, summary: "done", blockers: [] }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error!.message, "off-schema");
    assertStringIncludes(
      String(failureOf(recorded, "review").reason),
      "off-schema",
    );
    // The transcript is published anyway — the message sends the reader to it.
    assertEquals(recorded.files.length, 1);
  });
});

Deno.test("an agent that stops before answering fails the phase", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [
      resultEvent(null, {
        subtype: "error_max_turns",
        structured_output: null,
      }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    const error = await captureError(() =>
      model.methods.design.execute(args(), context)
    );

    assertStringIncludes(error!.message, "without structured output");
    assertStringIncludes(error!.message, "error_max_turns");
    failureOf(recorded, "design");
  });
});

Deno.test("an agent that dies before reading its prompt keeps its transcript", async () => {
  await withTempRepo(async (dir) => {
    // A bad flag or an expired login looks like this: the agent writes its
    // complaint and exits without draining stdin, which breaks the pipe the
    // prompt is being written to. The broken pipe is the symptom; the reason is
    // on stderr, in the transcript the error message sends the reader to.
    const bin = await writeScript(
      dir,
      "dying-claude",
      'echo "Invalid API key" >&2\nexit 1',
    );
    const { context, recorded } = makeContext(dir, globals(bin));

    const error = await captureError(() =>
      model.methods.review.execute(
        args({ context: { big: "x".repeat(300_000) } }),
        context,
      )
    );

    assert(error !== null, "the phase should fail");
    failureOf(recorded, "review");
    // The transcript is published, and it holds what the agent said on the way
    // out — which is the only place the actual cause appears.
    assertEquals(recorded.files.length, 1);
    assertStringIncludes(recorded.files[0].lines.join("\n"), "Invalid API key");
  });
});

Deno.test("a phase that cannot be launched still records the attempt", async () => {
  await withTempRepo(async (dir) => {
    // Spawning fails in ways retrying cannot fix — the binary not on PATH, a
    // working directory that is gone. Left to throw, it wrote nothing under
    // this phase's spec and nothing under `failure` either, so a scheduled
    // sweep counting attempts saw none and retried an unlaunchable phase for as
    // long as it ran.
    const { context, recorded } = makeContext(
      dir,
      globals(`${dir}/not-a-binary`),
    );

    const error = await captureError(() =>
      model.methods.review.execute(args({ label: "42:abc" }), context)
    );

    assertStringIncludes(error?.message ?? "", "could not start");
    const failure = failureOf(recorded, "review");
    // Under the caller's label, which is what the budget counts by.
    assertEquals(failure.label, "42:abc");
  });
});

Deno.test("a transcript with no result event fails the phase", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [{ type: "system", subtype: "init" }]);
    const { context, recorded } = makeContext(dir, globals(bin));

    const error = await captureError(() =>
      model.methods.design.execute(args(), context)
    );

    assertStringIncludes(error!.message, "no result event");
    failureOf(recorded, "design");
    assertEquals(recorded.files.length, 1);
  });
});

Deno.test("a CLI that reports an error fails the phase", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(
      dir,
      [resultEvent(CLEAN_REVIEW, {
        is_error: true,
        subtype: "error_during_execution",
      })],
      { exitCode: 1 },
    );
    const { context, recorded } = makeContext(dir, globals(bin));

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error!.message, "error_during_execution");
    failureOf(recorded, "review");
  });
});

Deno.test("a phase that reports its own failure fails the step", async () => {
  await withTempRepo(async (dir) => {
    // The process exits zero: the agent ran fine and decided it could not do
    // the job. Reporting that as success is exactly what the schema exists to
    // prevent.
    const bin = await fakeClaude(dir, [
      resultEvent({
        succeeded: false,
        summary: "The design contradicts the C API.",
        blockers: ["skip_to cannot return a borrow"],
        filesChanged: [],
        commits: [],
        testsAdded: [],
        notVerified: [],
        deviations: [],
      }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    const error = await captureError(() =>
      model.methods.implement.execute(args(), context)
    );

    assertStringIncludes(error!.message, "reported failure");
    assertStringIncludes(error!.message, "skip_to cannot return a borrow");
    // The result is still stored: the next step needs to see why it failed.
    assertEquals(resultOf(recorded).succeeded, false);
  });
});

Deno.test("ignoreFailure records the failure without failing the step", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [
      resultEvent({
        succeeded: false,
        summary: "Two findings stand.",
        blockers: [],
        clean: false,
        findings: [
          {
            severity: "high",
            file: "src/union.rs",
            line: 10,
            summary: "Leaks on the error path",
            detail: "The iterator is not freed when read fails.",
          },
        ],
        reviewed: ["src/union.rs"],
      }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    // A review that finds problems has done its job. Failing the step would
    // skip the revise step that exists to act on the findings.
    await model.methods.review.execute(args({ ignoreFailure: true }), context);

    const result = resultOf(recorded);
    assertEquals(result.clean, false);
    assertEquals((result.findings as unknown[]).length, 1);
  });
});

Deno.test("success reported alongside a blocker is refused", async () => {
  await withTempRepo(async (dir) => {
    // Schema-valid nonsense: a boolean and an array validate independently, so
    // nothing but this stops the two contradicting each other. It matters
    // because the gates read `succeeded` alone — an implementation saying in
    // `blockers` that it left the work unfinished would be published on the
    // strength of the flag.
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Ported the iterator.",
      blockers: ["The FFI wrapper is not written."],
      filesChanged: ["src/union.rs"],
      commits: [],
      testsAdded: [],
      notVerified: [],
      deviations: [],
    })]);
    const { context, recorded } = makeContext(dir, globals(claude));

    const error = await captureError(() =>
      model.methods.implement.execute(args(), context)
    );

    assertStringIncludes(
      error?.message ?? "",
      "reported success and blockers together",
    );
    // No answer in the store, so no gate can read the success it contradicts.
    failureOf(recorded, "implement");
  });
});

Deno.test("ignoreFailure does not excuse a contradictory answer", async () => {
  await withTempRepo(async (dir) => {
    // `ignoreFailure` forgives a phase that reported failure. This is a phase
    // whose answer cannot be read either way, which is not the same thing.
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Triaged.",
      blockers: ["Could not reach the CI logs."],
      allPreExisting: true,
      failures: [],
      ticket: null,
      recommendation: "",
    })]);
    const { context, recorded } = makeContext(dir, globals(claude));

    const error = await captureError(() =>
      model.methods.triage.execute(args({ ignoreFailure: true }), context)
    );

    assertStringIncludes(
      error?.message ?? "",
      "reported success and blockers together",
    );
    failureOf(recorded, "triage");
  });
});

Deno.test("a review is not blamed for work the checkout was already dirty on", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);
    // The state a real run starts from: a task in progress, tracked edits and
    // new files both. None of it is the reviewer's, and a gate that cannot tell
    // the difference does not block reviewers — it blocks every run started
    // from a checkout somebody was working in.
    await Deno.writeTextFile(`${dir}/README`, "base\nwork in progress\n");
    await Deno.writeTextFile(`${dir}/scratch.rs`, "fn main() {}\n");

    await model.methods.review.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsWritten, []);
    assertEquals(agent.pathsWrittenDerived, true);
    // The superset is deliberate and still reported: its consumer decides
    // whether a suite has to run, and there over-reporting is the safe error.
    assertEquals(agent.pathsTouched, ["README", "scratch.rs"]);
  });
});

Deno.test("a review that edits an already-dirty file is still refused", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The case a name comparison cannot see: the path was dirty before and is
    // dirty after, and only the content says the reviewer wrote to it.
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      before: `echo "reviewer was here" >> "${dir}/README"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);
    await Deno.writeTextFile(`${dir}/README`, "base\nwork in progress\n");

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "changed 1 path(s)");
    assertStringIncludes(error?.message ?? "", "README");
    failureOf(recorded, "review");
  });
});

Deno.test("a review that only changes a mode is refused", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // Content hashing cannot see the executable bit, which git tracks — so a
    // `chmod +x` on a file the tree was already dirty on changed the checkout
    // and left both fingerprints identical.
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      before: `chmod +x "${dir}/scratch.rs"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);
    await Deno.writeTextFile(`${dir}/scratch.rs`, "fn main() {}\n");

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "changed 1 path(s)");
    assertStringIncludes(error?.message ?? "", "scratch.rs");
    failureOf(recorded, "review");
  });
});

Deno.test("a review that advances a submodule is refused", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // A submodule's own files are another repository's, so the sentinel every
    // non-file path shared made moving one invisible: the gitlink is the whole
    // of what this checkout records about it.
    const sub = `${dir}/dep`;
    await Deno.mkdir(sub);
    await run(sub, "init", "-q");
    await run(sub, "config", "user.email", "t@t");
    await run(sub, "config", "user.name", "t");
    await run(sub, "config", "commit.gpgsign", "false");
    await Deno.writeTextFile(`${sub}/a`, "one\n");
    await run(sub, "add", "-A");
    await run(sub, "commit", "-qm", "one");

    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      before: [
        `cd "${sub}"`,
        "echo two > a",
        "git add -A",
        "git -c user.email=t@t -c user.name=t -c commit.gpgsign=false commit -qm two",
      ].join("\n"),
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "changed 1 path(s)");
    assertStringIncludes(error?.message ?? "", "dep");
    failureOf(recorded, "review");
  });
});

Deno.test("a review that edits inside a submodule is refused", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The half a gitlink cannot see: an edit inside the submodule that was
    // never committed there moves no commit, while the validation that follows
    // would run against dependency code the reviewer had changed.
    const sub = `${dir}/dep`;
    await Deno.mkdir(sub);
    await run(sub, "init", "-q");
    await run(sub, "config", "user.email", "t@t");
    await run(sub, "config", "user.name", "t");
    await run(sub, "config", "commit.gpgsign", "false");
    await Deno.writeTextFile(`${sub}/a`, "one\n");
    await run(sub, "add", "-A");
    await run(sub, "commit", "-qm", "one");

    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      before: `echo edited > "${sub}/a"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "changed 1 path(s)");
    assertStringIncludes(error?.message ?? "", "dep");
    failureOf(recorded, "review");
  });
});

Deno.test("a review that removes pre-existing work is refused", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      before: `rm "${dir}/scratch.rs"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);
    await Deno.writeTextFile(`${dir}/scratch.rs`, "fn main() {}\n");

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    // Destroying somebody else's uncommitted work is as much a write as making
    // one, and it is the direction a diff of what is present would miss.
    assertStringIncludes(error?.message ?? "", "changed 1 path(s)");
    assertStringIncludes(error?.message ?? "", "scratch.rs");
    failureOf(recorded, "review");
  });
});

Deno.test("an answer that arrived is kept when the CLI exits non-zero", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The phase answered in schema and reported no error; the process then
    // exited 1 for reasons of its own. `succeeded` is the agent's verdict by
    // design, so the exit code is not a second opinion on it — discarding the
    // answer here is how an hour of work is lost to a code nothing reads.
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      exitCode: 1,
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    const result = resultOf(recorded);
    assertEquals(result.succeeded, true);
    // Kept, but on the record, so the next disagreement is diagnosable from
    // the data rather than from a transcript that has since been collected.
    assertEquals((result.agent as Record<string, unknown>).exitCode, 1);
  });
});

Deno.test("a rate-limited run names the limit rather than reporting success", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The CLI reports `subtype: "success"` on a run it flagged as an error, so
    // a message built from `subtype` reads "failed: success" and names nothing
    // an operator can act on.
    const claude = await fakeClaude(dir, [{
      type: "result",
      subtype: "success",
      is_error: true,
      terminal_reason: "api_error",
      api_error_status: 429,
      result: "You've hit your session limit · resets 6:50pm",
      session_id: "session-1",
      num_turns: 27,
      total_cost_usd: 2.02,
      permission_denials: [],
    }], { exitCode: 1 });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    const message = error?.message ?? "";
    assertStringIncludes(message, "api_error");
    assertStringIncludes(message, "HTTP 429");
    assertStringIncludes(message, "session limit");
    assert(
      !message.includes("failed: success"),
      `the message must not report a failure as a success: ${message}`,
    );
    const failure = failureOf(recorded, "review");
    assertEquals(failure.terminalReason, "api_error");
    // What a sweep reads to decide between backing off and giving up.
    assertEquals(failure.retryable, true);
  });
});

Deno.test("a failure the agent itself produced is not retryable", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // It ran to completion and reported that it could not do the job. Running
    // it again spends an attempt to be told the same thing, which is the case
    // a retry budget exists to stop.
    const claude = await fakeClaude(dir, [{
      type: "result",
      subtype: "error_during_execution",
      is_error: true,
      terminal_reason: "completed",
      result: "I could not find the module.",
      session_id: "session-1",
      num_turns: 3,
      total_cost_usd: 0.1,
      permission_denials: [],
    }]);
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "could not find the module");
    assertEquals(failureOf(recorded, "review").retryable, false);
  });
});

Deno.test("a phase the API refused is attempted again", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The failure that killed whole runs: a rate limit or a session limit
    // reached mid-phase. Nothing about it is the agent's doing and nothing an
    // operator reads in the message can fix it, while the phases before it in
    // the flow have already been paid for and are not re-run when the run is
    // restarted from the top.
    const claude = await fakeClaudeAttempts(dir, [
      { events: [rateLimited()], exitCode: 1 },
      { events: [resultEvent(CLEAN_REVIEW)] },
    ]);
    const { context, recorded } = makeContext(
      dir,
      globals(claude, { maxRetries: 2 }),
    );
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    assertEquals(await attemptsMade(dir), 2);
    // The attempt that failed is still on the record — a budget nothing can
    // see is a budget nothing can be held to — and the answer is the one the
    // second attempt gave.
    assertEquals(recorded.resources.map((r) => r.spec), ["failure", "review"]);
    assertEquals(recorded.resources[0].data.retryable, true);
    assertEquals(recorded.resources[1].data.clean, true);
    // One transcript per attempt, each published where its message points.
    assertEquals(recorded.files.length, 2);
  });
});

Deno.test("a phase that changed the checkout is not retried", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // A transient API error says nothing about how much the agent had done
    // before it. This one committed — the CI fixer pushing is the same shape —
    // and a fresh agent on that checkout is a second phase on top of the
    // first, not another attempt at it.
    const claude = await fakeClaudeAttempts(dir, [
      {
        events: [rateLimited()],
        exitCode: 1,
        before: [
          `echo "half a port" > "${dir}/ported.rs"`,
          `git -C "${dir}" add ported.rs`,
          `git -C "${dir}" commit -qm "wip"`,
        ].join("\n"),
      },
      { events: [resultEvent(CLEAN_REVIEW)] },
    ]);
    const { context, recorded } = makeContext(
      dir,
      globals(claude, { maxRetries: 2 }),
    );

    const error = await captureError(() =>
      model.methods.implement.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "session limit");
    assertEquals(await attemptsMade(dir), 1);
    // Still recorded as retryable: it is, by a sweep that starts from a
    // checkout somebody has looked at. It is this loop that must not.
    assertEquals(failureOf(recorded, "implement").retryable, true);
  });
});

Deno.test("a failure the agent produced is not retried", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // It ran to completion and answered off-schema. Another attempt answers the
    // same way, so spending one is how a phase that will never pass is retried
    // until somebody notices.
    const claude = await fakeClaudeAttempts(dir, [
      { events: [resultEvent({ succeeded: true, summary: "done" })] },
    ]);
    const { context, recorded } = makeContext(
      dir,
      globals(claude, { maxRetries: 2 }),
    );
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "off-schema");
    assertEquals(await attemptsMade(dir), 1);
    assertEquals(failureOf(recorded, "review").retryable, false);
  });
});

Deno.test("the retry budget is spent and then the phase fails", async () => {
  await withTempRepo(async (dir) => {
    // A limit that resets in hours is not survivable by backing off inside one
    // phase. The budget is what stops it being retried for as long as the run
    // lasts, and reaching it has to fail rather than answer.
    const claude = await fakeClaudeAttempts(dir, [
      { events: [rateLimited()], exitCode: 1 },
    ]);
    const { context, recorded } = makeContext(
      dir,
      globals(claude, { maxRetries: 2 }),
    );
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "session limit");
    assertEquals(await attemptsMade(dir), 3);
    assertEquals(recorded.resources.map((r) => r.spec), [
      "failure",
      "failure",
      "failure",
    ]);
  });
});

Deno.test("a review's writes are undone and the review runs again", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // A reviewer that edits the tree fails its attempt whatever else it did —
    // a review of a tree the reviewer changed is a review of its own work. But
    // the edit is known to the byte, and undoing it puts the checkout back to
    // the tree the reviewer was given, so the run continues instead of dying
    // on it with the design and the tests it was reviewing already paid for.
    const claude = await fakeClaudeAttempts(dir, [
      {
        events: [resultEvent(CLEAN_REVIEW)],
        before: [
          `echo "fixed it" >> "${dir}/README"`,
          `echo "fn helper() {}" > "${dir}/helper.rs"`,
        ].join("\n"),
      },
      { events: [resultEvent(CLEAN_REVIEW)] },
    ]);
    const { context, recorded } = makeContext(
      dir,
      globals(claude, { maxRetries: 1 }),
    );
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    assertEquals(await attemptsMade(dir), 2);
    // Both directions of the undo: the tracked file is back to its committed
    // content, and the one the reviewer created is gone.
    assertEquals(await Deno.readTextFile(`${dir}/README`), "base\n");
    assertEquals(await exists(`${dir}/helper.rs`), false);

    const failure = recorded.resources[0].data;
    assertEquals(failure.retryable, true);
    assertStringIncludes(String(failure.reason), "have been undone");
    // The answer stored is the second attempt's, over the restored tree.
    assertEquals(recorded.resources[1].spec, "review");
    assertEquals(
      (recorded.resources[1].data.agent as Record<string, unknown>)
        .pathsWritten,
      [],
    );
  });
});

Deno.test("undoing a review's writes leaves staged work alone", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const claude = await fakeClaudeAttempts(dir, [
      {
        events: [resultEvent(CLEAN_REVIEW)],
        before: `echo "reviewer was here" >> "${dir}/README"`,
      },
      { events: [resultEvent(CLEAN_REVIEW)] },
    ]);
    const { context } = makeContext(dir, globals(claude, { maxRetries: 1 }));
    // Staged, and then put back in the working tree: the index differs from
    // the commit while the file on disk does not, so the path is not dirty by
    // content and the undo is entitled to restore it. Restoring it through
    // `git checkout` would take the index with it and throw that away — it is
    // somebody's work, and it is not the reviewer's.
    await Deno.writeTextFile(`${dir}/README`, "staged\n");
    await run(dir, "add", "README");
    await Deno.writeTextFile(`${dir}/README`, "base\n");

    await model.methods.review.execute(args(), context);

    assertEquals(await Deno.readTextFile(`${dir}/README`), "base\n");
    const staged = await new Deno.Command("git", {
      args: ["show", ":README"],
      cwd: dir,
      stdout: "piped",
      stderr: "null",
    }).output();
    assertEquals(new TextDecoder().decode(staged.stdout), "staged\n");
  });
});

Deno.test("a review's writes that cannot be undone fail the phase", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The path was already dirty when the review started, so the content it
    // had then was hashed and never written to the object database: there is
    // nothing to restore it from, and undoing it would substitute the
    // committed version for somebody's uncommitted work. That is worse than
    // the edit, so this one refuses and asks for a person.
    const claude = await fakeClaudeAttempts(dir, [
      {
        events: [resultEvent(CLEAN_REVIEW)],
        before: `echo "reviewer was here" >> "${dir}/README"`,
      },
    ]);
    const { context, recorded } = makeContext(
      dir,
      globals(claude, { maxRetries: 2 }),
    );
    await commitAll(dir);
    await Deno.writeTextFile(`${dir}/README`, "base\nwork in progress\n");

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "could not be undone");
    // Not retried, and the work that was there is still there.
    assertEquals(await attemptsMade(dir), 1);
    assertEquals(failureOf(recorded, "review").retryable, false);
    assertStringIncludes(
      await Deno.readTextFile(`${dir}/README`),
      "work in progress",
    );
  });
});

Deno.test("retrying is on by default, and only for a transient failure", async () => {
  // The defaults the workflows run with: they pass neither of these, so the
  // schema's own values are what a phase in the flow actually gets.
  const defaults = model.globalArguments.parse({});
  assertEquals(defaults.runner, "amp");
  assertEquals(defaults.workingDir, "..");
  assertEquals(defaults.maxRetries, 2);
  assertEquals(defaults.retryBackoffMs, 60_000);
});

/** Whether a path exists, for asserting that an undo removed one. */
async function exists(path: string): Promise<boolean> {
  try {
    await Deno.stat(path);
    return true;
  } catch {
    return false;
  }
}

/**
 * Write a fake `amp` that answers a phase, a repair turn and a usage query.
 *
 * One script for all three because that is how the model reaches it: the same
 * executable is invoked to run the phase, to ask a finished thread to restate
 * its answer, and to ask what the thread cost. Dispatching on the first
 * argument is what the real CLI does with them too.
 */
async function fakeAmp(
  dir: string,
  options: { said: string; corrected?: string; cost?: string },
): Promise<string> {
  const result = (text: string, session = "T-1") =>
    JSON.stringify({
      type: "result",
      subtype: "success",
      is_error: false,
      duration_ms: 10,
      num_turns: 3,
      result: text,
      session_id: session,
    });
  return await writeScript(
    dir,
    "fake-claude",
    [
      `n=$(cat "${dir}/attempts" 2>/dev/null || echo 0)`,
      "n=$((n + 1))",
      `echo "$n" > "${dir}/attempts"`,
      'case "$1" in',
      "threads)",
      `  printf '%s\0' "$@" > "${dir}/argv-$2"`,
      `  if [ "$2" = "usage" ]; then echo 'Cost: $${
        options.cost ?? "0.42"
      }'; else`,
      `    cat <<'EVENT'\n${result(options.corrected ?? "{}")}\nEVENT`,
      "  fi",
      "  ;;",
      "*)",
      `  printf '%s\0' "$@" > "${dir}/argv"`,
      `  cat > "${dir}/prompt"`,
      `  cat <<'EVENT'\n${result(options.said)}\nEVENT`,
      "  ;;",
      "esac",
    ].join("\n"),
  );
}

/** A well-formed review answer, as text an agent would have printed. */
const CLEAN_REVIEW_TEXT = JSON.stringify(CLEAN_REVIEW);

Deno.test("a phase runs on amp and records that it did", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const amp = await fakeAmp(dir, { said: CLEAN_REVIEW_TEXT, cost: "0.42" });
    const { context, recorded } = makeContext(
      dir,
      globals(amp, { runner: "amp" }),
    );
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    const result = resultOf(recorded);
    assertEquals(result.clean, true);
    const agent = result.agent as Record<string, unknown>;
    assertEquals(agent.runner, "amp");
    // The one difference a gate downstream cannot see for itself: the shape was
    // checked, not enforced.
    assertEquals(agent.schemaEnforced, false);
    // Amp's result event carries no cost, so it is asked about the thread.
    assertEquals(agent.costUsd, 0.42);
    assertEquals(agent.sessionId, "T-1");

    // The schema reaches the agent in the prompt, since there is no flag for it.
    const prompt = await promptOf(dir);
    assertStringIncludes(prompt, "single JSON object");
    assertStringIncludes(prompt, '"clean"');
  });
});

Deno.test("an amp answer wrapped in a fence is still an answer", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // Asked for bare JSON, an agent frequently gives a fenced block. Failing a
    // phase over the fence would throw away the work it describes.
    const amp = await fakeAmp(dir, {
      said: "Here is the review:\n\n```json\n" + CLEAN_REVIEW_TEXT + "\n```\n",
    });
    const { context, recorded } = makeContext(
      dir,
      globals(amp, { runner: "amp" }),
    );
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    assertEquals(resultOf(recorded).clean, true);
  });
});

Deno.test("an off-schema amp answer is repaired once, in its own thread", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // Nothing holds the agent to the shape, so this is the ordinary failure
    // there — and the phase itself is the expensive part, so it is worth one
    // round trip to have the same work restated correctly.
    const amp = await fakeAmp(dir, {
      said: JSON.stringify({ succeeded: true, summary: "done" }),
      corrected: CLEAN_REVIEW_TEXT,
    });
    const { context, recorded } = makeContext(
      dir,
      globals(amp, { runner: "amp" }),
    );
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    assertEquals(resultOf(recorded).clean, true);
    // Continued rather than re-run: the work is not redone, only restated.
    const repair = await Deno.readTextFile(`${dir}/argv-continue`);
    assertStringIncludes(repair, "continue");
    assertStringIncludes(repair, "T-1");
    assertStringIncludes(repair, "did not validate against the schema");
  });
});

Deno.test("an amp answer that cannot be repaired fails the phase", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // Twice off-schema is an agent that will be off-schema a third time, and a
    // loop here would spend a phase's worth of tokens discovering that.
    const amp = await fakeAmp(dir, {
      said: JSON.stringify({ succeeded: true, summary: "done" }),
      corrected: JSON.stringify({ succeeded: true, summary: "still wrong" }),
    });
    const { context, recorded } = makeContext(
      dir,
      globals(amp, { runner: "amp" }),
    );
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "off-schema");
    assertEquals(failureOf(recorded, "review").retryable, false);
  });
});

/** Initialise a git repository with one commit, so HEAD can be read. */
async function gitRepo(dir: string): Promise<void> {
  const run = async (...args: string[]) => {
    const { success } = await new Deno.Command("git", {
      args,
      cwd: dir,
      stdout: "null",
      stderr: "null",
    }).output();
    assert(success, `git ${args.join(" ")} failed`);
  };
  await run("init", "-q");
  await run("config", "user.email", "test@example.com");
  await run("config", "user.name", "Test");
  await run("config", "commit.gpgsign", "false");
  await Deno.writeTextFile(`${dir}/README`, "base\n");
  // The harness itself writes into the checkout — the fake CLI, and the argv
  // and prompt it records — which the real one does not. Ignored rather than
  // committed because the fake writes them while it runs, and because ignored
  // files are outside what the model reads: `--exclude-standard` leaves them
  // out, which is the same reason build output does not count as a phase having
  // touched something.
  await Deno.writeTextFile(
    `${dir}/.gitignore`,
    [
      "argv",
      "argv-continue",
      "argv-usage",
      "prompt",
      "attempts",
      "fake-claude",
      "not-a-binary",
      "remote.git",
    ]
      .join("\n") +
      "\n",
  );
  await run("add", "-A");
  await run("commit", "-qm", "base");
}

/** Run one git command in the checkout, asserting it worked. */
async function run(dir: string, ...args: string[]): Promise<void> {
  const { success } = await new Deno.Command("git", {
    args,
    cwd: dir,
    stdout: "null",
    stderr: "null",
  }).output();
  assert(success, `git ${args.join(" ")} failed`);
}

/**
 * Commit everything in the checkout, so a pinned run sees a clean tree.
 *
 * A no-op when there is nothing to commit, which is the ordinary case now that
 * the harness's own files are ignored — `git commit` exits non-zero on an empty
 * one, and a fixture that fails for having nothing to do is no use.
 */
async function commitAll(dir: string): Promise<void> {
  const { stdout } = await new Deno.Command("git", {
    args: ["status", "--porcelain"],
    cwd: dir,
    stdout: "piped",
    stderr: "null",
  }).output();
  if (new TextDecoder().decode(stdout).trim() === "") return;
  for (
    const args of [["add", "-A"], [
      "-c",
      "commit.gpgsign=false",
      "commit",
      "-qm",
      "fixture",
    ]]
  ) {
    const { success } = await new Deno.Command("git", {
      args,
      cwd: dir,
      stdout: "null",
      stderr: "null",
    }).output();
    assert(success, `git ${args.join(" ")} failed`);
  }
}

/** The hash of the checkout's HEAD tree. */
async function treeOf(dir: string): Promise<string> {
  const { stdout } = await new Deno.Command("git", {
    args: ["rev-parse", "HEAD^{tree}"],
    cwd: dir,
    stdout: "piped",
    stderr: "null",
  }).output();
  return new TextDecoder().decode(stdout).trim();
}

Deno.test("a phase refuses a tree other than the one it was promised", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The hand-off's case: a commit lands while the approval gate is suspended,
    // and every record the gate read still describes the tree from before it.
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context, recorded } = makeContext(dir, globals(claude));

    const error = await captureError(() =>
      model.methods.pr.execute(
        args({ expectedTree: "0".repeat(40) }),
        context,
      )
    );

    assertStringIncludes(error?.message ?? "", "was asked for the tree");
    // Refused before the agent was spawned, so nothing ran and nothing was
    // recorded — not even a failure, which is for a phase that started.
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("a phase runs when the tree is the one it was promised", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Opened it.",
      blockers: [],
      prUrl: "https://github.com/o/r/pull/1",
      title: "swamp: a thing",
      branch: "gd-a-thing",
      releaseNotesRequired: false,
      ticket: null,
    })]);
    const { context, recorded } = makeContext(dir, globals(claude));
    // The fake CLI is an untracked file in the checkout, and a pinned run now
    // refuses one — so the fixture commits itself first.
    await commitAll(dir);

    await model.methods.pr.execute(
      args({ expectedTree: await treeOf(dir) }),
      context,
    );

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.treeDigest, await treeOf(dir));
  });
});

Deno.test("a pinned phase refuses a checkout with uncommitted work", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);
    const tree = await treeOf(dir);
    // An edit made while the approval gate was suspended. The tree still
    // matches — a tree is what HEAD holds — and `/open-pr` sends a phase that
    // finds a dirty working copy to `/commit-guidelines`, so this would be
    // committed and pushed having been neither reviewed nor validated.
    await Deno.writeTextFile(`${dir}/README`, "an edit nobody reviewed\n");

    const error = await captureError(() =>
      model.methods.pr.execute(args({ expectedTree: tree }), context)
    );

    assertStringIncludes(error?.message ?? "", "uncommitted change");
    assertStringIncludes(error?.message ?? "", "README");
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("an untracked file counts as uncommitted work", async () => {
  await withTempRepo(async (dir) => {
    // The other half of what a phase told to sort out a dirty tree can commit.
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context } = makeContext(dir, globals(claude));
    await commitAll(dir);
    const tree = await treeOf(dir);
    await Deno.writeTextFile(`${dir}/unreviewed.rs`, "pub fn sneak() {}\n");

    const error = await captureError(() =>
      model.methods.pr.execute(args({ expectedTree: tree }), context)
    );

    assertStringIncludes(error?.message ?? "", "unreviewed.rs");
  });
});

Deno.test("an empty expected tree runs whatever is there", async () => {
  await withTempRepo(async (dir) => {
    // Which is what a record written before the model recorded a tree leaves,
    // and what every phase but the hand-off passes.
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context, recorded } = makeContext(dir, globals(claude));
    // The fake CLI is an untracked file in the checkout, and a review that
    // leaves the tree changed is now refused — so the fixture commits itself.
    await commitAll(dir);

    await model.methods.review.execute(args(), context);

    assertEquals(recorded.resources.length, 1);
  });
});

Deno.test("a review that changed the checkout is refused", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // A formatter run out of habit, a fix applied while explaining it. The
    // answer says nothing about it — which is the point: an agent that changed
    // something by accident is the one that will not report it, and the loop
    // treats the settling review as the last word on the subject, so what the
    // reviewer wrote would reach the hand-off having been read by nobody.
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)], {
      before: `echo "reviewer was here" >> "${dir}/README"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    const error = await captureError(() =>
      model.methods.review.execute(args(), context)
    );

    assertStringIncludes(error?.message ?? "", "changed 1 path(s)");
    assertStringIncludes(error?.message ?? "", "README");
    // No review record, so no loop can settle on it.
    failureOf(recorded, "review");
  });
});

Deno.test("a phase that is meant to change the checkout still may", async () => {
  await withTempRepo(async (dir) => {
    // The rule is about reviews. An implementation that touched nothing would
    // be the surprising one.
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: ["README"],
      commits: [],
      testsAdded: [],
      notVerified: [],
      deviations: [],
    })], { before: `echo "implementer was here" >> "${dir}/README"` });
    const { context, recorded } = makeContext(dir, globals(claude));
    await commitAll(dir);

    await model.methods.implement.execute(args(), context);

    assertEquals(recorded.resources.length, 1);
  });
});

Deno.test("a push reported without a commit is refused", async () => {
  await withTempRepo(async (dir) => {
    // The same shape of contradiction as success-with-blockers, in the field
    // the CI sweep reads to decide it got somewhere: `pass-made-progress`
    // accepts the round on the flag, so a claim nothing backs spends the
    // per-commit budget while the head stays where it was.
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Fixed the lint failure.",
      blockers: [],
      addressed: [],
      deferred: [],
      commitsPushed: [],
      pushed: true,
    })]);
    const { context, recorded } = makeContext(dir, globals(claude));

    const error = await captureError(() =>
      model.methods.ci.execute(args(), context)
    );

    assertStringIncludes(
      error?.message ?? "",
      "reported a push without naming a commit",
    );
    failureOf(recorded, "ci");
  });
});

Deno.test("a reviewThreadId on a non-review item is refused", async () => {
  await withTempRepo(async (dir) => {
    // `reviewThreadId` only means anything on a `source: "review"` item — a
    // non-null id elsewhere is either a mislabeled source or an invented
    // thread for work that was never a GitHub comment, and either way it
    // would otherwise reach the `resolveReviewThread` mutation with a claim
    // nothing backs.
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Fixed the build failure.",
      blockers: [],
      addressed: [{
        item: "cargo build failed on a missing feature flag",
        source: "ci",
        action: "enabled the flag in Cargo.toml",
        reviewThreadId: "PRRT_kwDOA3cIQM6a3neh",
      }],
      deferred: [],
      commitsPushed: ["abc123: fix build"],
      pushed: true,
    })]);
    const { context, recorded } = makeContext(dir, globals(claude));

    const error = await captureError(() =>
      model.methods.ci.execute(args(), context)
    );

    assertStringIncludes(
      error?.message ?? "",
      'set reviewThreadId on 1 item(s) not sourced from "review"',
    );
    failureOf(recorded, "ci");
  });
});

Deno.test("a phase that committed is recorded as having moved the revision", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // Nothing can be pushed that was never committed, so the head moving is the
    // half of a push claim the checkout can actually answer for.
    const commit = [
      `cd "${dir}"`,
      "echo more >> README",
      "git add -A",
      "git -c commit.gpgsign=false commit -qm follow-up",
    ].join(" && ");
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Pushed a follow-up.",
      blockers: [],
      addressed: [],
      deferred: [],
      commitsPushed: ["abc123: follow-up"],
      pushed: true,
    })], { before: commit });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.ci.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsDerived, true);
    assertEquals(agent.revisionMoved, true);
  });
});

Deno.test("a push that landed leaves the tracking ref level with the head", async () => {
  await withTempRepo(async (dir) => {
    // A bare repository stands in for the remote. `git push` advances the
    // tracking ref as part of succeeding, which is what makes the ref an answer
    // about the push rather than about the local branch.
    const remote = `${dir}/remote.git`;
    await gitRepo(dir);
    await run(dir, "init", "--bare", "-q", remote);
    await run(dir, "remote", "add", "origin", remote);
    await run(dir, "push", "-q", "-u", "origin", "HEAD:refs/heads/main");
    const push = [
      `cd "${dir}"`,
      "echo more >> README",
      "git add -A",
      "git -c commit.gpgsign=false commit -qm follow-up",
      "git push -q origin HEAD:refs/heads/main",
    ].join(" && ");
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Pushed a follow-up.",
      blockers: [],
      addressed: [],
      deferred: [],
      commitsPushed: ["abc123: follow-up"],
      pushed: true,
    })], { before: push });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.ci.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.remoteHead, agent.revision);
  });
});

Deno.test("a commit whose push failed leaves the tracking ref behind", async () => {
  await withTempRepo(async (dir) => {
    // The case the local revision cannot see: the branch moved, so
    // `revisionMoved` is true and the pass looks like progress, while the pull
    // request head is exactly where it was.
    const remote = `${dir}/remote.git`;
    await gitRepo(dir);
    await run(dir, "init", "--bare", "-q", remote);
    await run(dir, "remote", "add", "origin", remote);
    await run(dir, "push", "-q", "-u", "origin", "HEAD:refs/heads/main");
    const commitOnly = [
      `cd "${dir}"`,
      "echo more >> README",
      "git add -A",
      "git -c commit.gpgsign=false commit -qm follow-up",
    ].join(" && ");
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Pushed a follow-up.",
      blockers: [],
      addressed: [],
      deferred: [],
      commitsPushed: ["abc123: follow-up"],
      pushed: true,
    })], { before: commitOnly });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.ci.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.revisionMoved, true);
    assert(agent.remoteHead !== agent.revision);
    assert((agent.remoteHead as string).length > 0);
  });
});

Deno.test("a phase that committed nothing did not move the revision", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Nothing to do.",
      blockers: [],
      addressed: [],
      deferred: [],
      commitsPushed: [],
      pushed: false,
    })]);
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.ci.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsDerived, true);
    assertEquals(agent.revisionMoved, false);
  });
});

Deno.test("an assertion added to an inline test counts", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // Extending a test that exists is the third way a Rust change covers
    // itself, and the only one that adds no attribute at all.
    await Deno.mkdir(`${dir}/src`, { recursive: true });
    await Deno.writeTextFile(
      `${dir}/src/qint.rs`,
      [
        "pub fn read() -> u32 { 1 }",
        "",
        "#[cfg(test)]",
        "mod tests {",
        "    use super::*;",
        "    #[test]",
        "    fn reads() {",
        "        assert_eq!(read(), 1);",
        "    }",
        "}",
        "",
      ].join("\n"),
    );
    await run(dir, "add", "-A");
    await run(
      dir,
      "-c",
      "user.email=t@t",
      "-c",
      "user.name=t",
      "-c",
      "commit.gpgsign=false",
      "commit",
      "-qm",
      "base",
    );

    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: ["src/qint.rs"],
      commits: [],
      testsAdded: ["reads"],
      notVerified: [],
      deviations: [],
    })], {
      before:
        `sed -i 's|        assert_eq!(read(), 1);|        assert_eq!(read(), 1);\\n        assert_ne!(read(), 2);|' "${dir}/src/qint.rs"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.testsChanged, true);
  });
});

Deno.test("a production assertion is not a test", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The reason the assertion marker is scoped: library code here asserts
    // with the bare macros too — some three hundred times under
    // src/redisearch_rs — so counting one wherever it appeared let a feature
    // with no test at all satisfy the gate that exists to require one.
    await Deno.mkdir(`${dir}/src`, { recursive: true });
    await Deno.writeTextFile(
      `${dir}/src/qint.rs`,
      "pub fn read(n: u32) -> u32 { n }\n",
    );
    await run(dir, "add", "-A");
    await run(
      dir,
      "-c",
      "user.email=t@t",
      "-c",
      "user.name=t",
      "-c",
      "commit.gpgsign=false",
      "commit",
      "-qm",
      "base",
    );

    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: ["src/qint.rs"],
      commits: [],
      testsAdded: ["a test that does not exist"],
      notVerified: [],
      deviations: [],
    })], {
      before:
        `printf 'pub fn read(n: u32) -> u32 {\\n    assert!(n > 0);\\n    n\\n}\\n' > "${dir}/src/qint.rs"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsWrittenDerived, true);
    // The claim in `testsAdded` is the agent's; this is the checkout's answer,
    // and the gate reads this one.
    assertEquals(agent.testsChanged, false);
  });
});

Deno.test("a cfg(test) attribute that is not a module is not a test region", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The shape this exists for, taken from
    // src/redisearch_rs/vector_score_source/src/lib.rs: `#[cfg(test)]` on an
    // `extern crate` near the top, with production code below it. Reading that
    // as "everything after here is a test" made a production assertion count as
    // coverage in the gate meant to require one.
    await Deno.mkdir(`${dir}/src`, { recursive: true });
    await Deno.writeTextFile(
      `${dir}/src/qint.rs`,
      [
        "#[cfg(test)]",
        "extern crate redisearch_rs;",
        "",
        "pub fn read(n: u32) -> u32 { n }",
        "",
      ].join("\n"),
    );
    await run(dir, "add", "-A");
    await run(
      dir,
      "-c",
      "user.email=t@t",
      "-c",
      "user.name=t",
      "-c",
      "commit.gpgsign=false",
      "commit",
      "-qm",
      "base",
    );

    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: ["src/qint.rs"],
      commits: [],
      testsAdded: ["a test that does not exist"],
      notVerified: [],
      deviations: [],
    })], {
      before:
        `printf '#[cfg(test)]\\nextern crate redisearch_rs;\\n\\npub fn read(n: u32) -> u32 {\\n    assert!(n > 0);\\n    n\\n}\\n' > "${dir}/src/qint.rs"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.testsChanged, false);
  });
});

Deno.test("an inline Rust test counts as a test being written", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The case a path check cannot see: Rust tests live in a `mod tests` at the
    // bottom of the file they test, so nothing under tests/ is touched at all.
    const write = [
      `mkdir -p "${dir}/src"`,
      `printf '#[test]\\nfn reads_a_qint() {}\\n' > "${dir}/src/qint.rs"`,
    ].join(" && ");
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: [],
      commits: [],
      testsAdded: ["reads_a_qint"],
      notVerified: [],
      deviations: [],
    })], { before: write });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsDerived, true);
    assertEquals(agent.testsChanged, true);
  });
});

Deno.test("a file under tests/ counts whatever the diff holds", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    const write = [
      `mkdir -p "${dir}/tests/pytests"`,
      `echo "# placeholder" > "${dir}/tests/pytests/test_thing.py"`,
    ].join(" && ");
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: [],
      commits: [],
      testsAdded: ["test_thing"],
      notVerified: [],
      deviations: [],
    })], { before: write });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    assertEquals(
      (resultOf(recorded).agent as Record<string, unknown>).testsChanged,
      true,
    );
  });
});

Deno.test("code with no test in it does not count as one", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The case the gate exists for: the agent names a test it did not write,
    // which is schema-valid and reads exactly like one it did.
    const write = [
      `mkdir -p "${dir}/src"`,
      `printf 'pub fn qint() {}\\n' > "${dir}/src/qint.rs"`,
    ].join(" && ");
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: [],
      commits: [],
      testsAdded: ["reads_a_qint"],
      notVerified: [],
      deviations: [],
    })], { before: write });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsDerived, true);
    assertEquals(agent.testsChanged, false);
  });
});

Deno.test("the paths a phase touched are read from the checkout", async () => {
  await withTempRepo(async (dir) => {
    await gitRepo(dir);
    // The agent under-reports, which is schema-valid and is exactly the case
    // the derived list exists for: the cluster guard hangs off whether
    // src/coord/ was touched, and an omission there means a coordinator change
    // reaching the hand-off having never run across shards.
    const claude = await fakeClaude(dir, [resultEvent({
      succeeded: true,
      summary: "Done.",
      blockers: [],
      filesChanged: ["src/union.rs"],
      commits: [],
      testsAdded: [],
      notVerified: [],
      deviations: [],
    })], {
      before:
        `mkdir -p "${dir}/src/coord" && echo x > "${dir}/src/coord/rmr.c" && echo y >> "${dir}/README"`,
    });
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.implement.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsDerived, true);
    const touched = agent.pathsTouched as string[];
    // The untracked file the agent added and the tracked one it edited — the
    // src/coord/ path being the one its own summary left out.
    assert(touched.includes("src/coord/rmr.c"));
    assert(touched.includes("README"));
    // And not the one it claimed without touching, which is the other half of
    // the point: the list describes the tree, not the report.
    assert(!touched.includes("src/union.rs"));
  });
});

Deno.test("a checkout that cannot answer says so rather than claiming nothing", async () => {
  await withTempRepo(async (dir) => {
    // No git repository here. An empty list on its own would read as "nothing
    // was touched", which the coordinator guard would take as licence to skip
    // the cluster suite; the flag is what keeps it falling back to the agent.
    const claude = await fakeClaude(dir, [resultEvent(CLEAN_REVIEW)]);
    const { context, recorded } = makeContext(dir, globals(claude));

    await model.methods.review.execute(args(), context);

    const agent = resultOf(recorded).agent as Record<string, unknown>;
    assertEquals(agent.pathsDerived, false);
    assertEquals(agent.pathsTouched, []);
  });
});

Deno.test("a cap never discards a blocking finding", async () => {
  await withTempRepo(async (dir) => {
    // The blocker is last, behind a long tail of nits — the order a reviewer
    // naturally produces when it runs out of substantive findings. Truncating
    // in place would drop it, and the stored review would then hold nothing
    // blocking, which is exactly what the loop reads as settled.
    const findings = [
      ...Array.from({ length: 250 }, (_, index) => ({
        severity: "low",
        file: null,
        line: null,
        summary: `nit ${index}`,
        detail: "detail",
      })),
      {
        severity: "high",
        file: "src/union.rs",
        line: 1,
        summary: "leaks on the error path",
        detail: "the iterator is not freed when read fails",
      },
    ];
    const bin = await fakeClaude(dir, [
      resultEvent({ ...CLEAN_REVIEW, clean: false, findings }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    await model.methods.review.execute(args({ ignoreFailure: true }), context);

    const kept = resultOf(recorded).findings as Array<{ severity: string }>;
    assertEquals(kept.length, 200);
    assertEquals(kept[0].severity, "high");
    assertEquals(kept.filter((f) => f.severity === "high").length, 1);
  });
});

Deno.test("a runaway review has its findings capped", async () => {
  await withTempRepo(async (dir) => {
    const findings = Array.from({ length: 250 }, (_, index) => ({
      severity: "low",
      file: null,
      line: null,
      summary: `finding ${index}`,
      detail: "detail",
    }));
    const bin = await fakeClaude(dir, [
      resultEvent({ ...CLEAN_REVIEW, clean: false, findings }),
    ]);
    const { context, recorded } = makeContext(dir, globals(bin));

    await model.methods.review.execute(args({ ignoreFailure: true }), context);

    assertEquals((resultOf(recorded).findings as unknown[]).length, 200);
  });
});

Deno.test("a failed attempt is recorded under the caller's label", async () => {
  await withTempRepo(async (dir) => {
    const bin = await fakeClaude(dir, [{ type: "system", subtype: "init" }]);
    const { context, recorded } = makeContext(dir, globals(bin));

    // The label is how a scheduled caller counts attempts. An agent that dies
    // on every launch writes nothing under its own spec, so a budget counting
    // only answers would never be spent by the one failure repeating cannot
    // fix — which is the case the budget exists for.
    await captureError(() =>
      model.methods.ci.execute(args({ label: "42:abc123" }), context)
    );

    const failure = failureOf(recorded, "ci");
    assertEquals(failure.label, "42:abc123");
  });
});

Deno.test("a phase that outruns its timeout fails and keeps its transcript", async () => {
  await withTempRepo(async (dir) => {
    // `exec`, so the kill lands on the sleep rather than on a shell whose
    // child goes on holding the pipe open — which would make this case take
    // the full sleep to observe instead of the timeout.
    const bin = await writeScript(dir, "slow-claude", "exec sleep 30");
    const { context, recorded } = makeContext(
      dir,
      globals(bin, { timeout: 300 }),
    );

    const error = await captureError(() =>
      model.methods.design.execute(args(), context)
    );

    assertStringIncludes(error!.message, "timed out");
    failureOf(recorded, "design");
    assertEquals(recorded.files.length, 1);
  });
});

Deno.test("each phase writes to its own resource spec", async () => {
  await withTempRepo(async (dir) => {
    const answers: Record<string, Record<string, unknown>> = {
      tests: {
        testFiles: [],
        testsAdded: [],
        failingAsExpected: null,
        coverageNotes: "",
      },
      design: {
        planPath: "plan.md",
        overallPlanPath: null,
        subtask: null,
        decisions: [],
        alternativesRejected: [],
        openQuestions: [],
        largeChange: false,
      },
      implement: {
        filesChanged: [],
        commits: [],
        testsAdded: [],
        notVerified: [],
        deviations: [],
      },
      revise: { resolutions: [], filesChanged: [], codeChanged: false },
      pr: {
        prUrl: "https://example.invalid/pr/1",
        title: "t",
        branch: "b",
        releaseNotesRequired: true,
      },
    };
    const specs: Record<string, string> = {
      tests: "tests",
      design: "design",
      implement: "implementation",
      revise: "revision",
      pr: "pullrequest",
    };

    for (const [kind, extra] of Object.entries(answers)) {
      const bin = await fakeClaude(dir, [
        resultEvent({ succeeded: true, summary: "ok", blockers: [], ...extra }),
      ]);
      const { context, recorded } = makeContext(dir, globals(bin));

      // deno-lint-ignore no-explicit-any
      await (model.methods as any)[kind].execute(args(), context);

      assertEquals(recorded.resources[0].spec, specs[kind]);
    }
  });
});
