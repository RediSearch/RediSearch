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
import { model } from "./claude_task.ts";
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
function globals(claudeBin: string, overrides: Record<string, unknown> = {}) {
  return {
    claudeBin,
    model: "",
    permissionMode: "auto",
    workingDir: ".",
    timeout: 30_000,
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
  options: { exitCode?: number } = {},
): Promise<string> {
  const lines = events.map((event) => JSON.stringify(event));
  return await writeScript(
    dir,
    "fake-claude",
    [
      `printf '%s\\0' "$@" > "${dir}/argv"`,
      `cat > "${dir}/prompt"`,
      ...lines.map((line) => `cat <<'EVENT'\n${line}\nEVENT`),
      `exit ${options.exitCode ?? 0}`,
    ].join("\n"),
  );
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
