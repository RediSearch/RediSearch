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
    ["argv", "prompt", "fake-claude", "not-a-binary", "remote.git"].join("\n") +
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
