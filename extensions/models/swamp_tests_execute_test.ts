/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the swamp extension suite model's `execute`.
 *
 * The parser tests cover what the model makes of deno's output. These cover the
 * rest: that it invokes the deno-only target rather than the one that shells out
 * to swamp, and that a run says which of the target's two checks stopped it.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./swamp_tests.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

const run = model.methods.run;

/** A stand-in for make that reports its target and prints the given output. */
async function fakeMake(dir: string, body: string): Promise<string> {
  return await writeScript(
    dir,
    "fake-make",
    ['echo "TARGET: $@"', body].join("\n"),
  );
}

/**
 * Swamp applies the schema defaults before calling a method; this harness does
 * not, so they are spelled out here the way the other models' tests do. The
 * default itself is pinned separately, against the schema.
 */
function globals(makeBin: string, overrides: Record<string, unknown> = {}) {
  return {
    repoRoot: ".",
    makeBin,
    target: "swamp-extension-tests",
    ...overrides,
  };
}

Deno.test("defaults to the deno-only target", async () => {
  // `swamp-tests` also validates the checked-in definitions by shelling out to
  // swamp, and this model runs inside a swamp workflow.
  const defaults = model.globalArguments.parse({});

  assertEquals(defaults.target, "swamp-extension-tests");
  await Promise.resolve();
});

Deno.test("passes the configured target to make", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(dir, 'echo "ok | 3 passed | 0 failed (1s)"');
    const { context, recorded } = makeContext(dir, globals(make));

    await run.execute({ quiet: true }, context);

    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("TARGET: swamp-extension-tests"), true);

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "passed");
    assertEquals(summary.stage, null);
    assertEquals(summary.testsRun, 3);
    assertEquals(summary.summaryParsed, true);
  });
});

Deno.test("returns its handles in the shape swamp reads them from", async () => {
  await withTempRepo(async (dir) => {
    // A bare array is not a malformed result to TypeScript, so nothing here
    // would notice: the step would simply carry no handles, and the digest
    // could not pin its read of the summary to this run.
    const make = await fakeMake(dir, 'echo "ok | 1 passed | 0 failed (1s)"');
    const { context } = makeContext(dir, globals(make));

    const result = await run.execute({ quiet: true }, context) as {
      dataHandles?: unknown[];
    };

    assertEquals(Array.isArray(result?.dataHandles), true);
    assertEquals(result.dataHandles?.length, 2);
  });
});

Deno.test("names the failing tests when a test fails", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(
      dir,
      [
        'echo "some check ... FAILED (1ms)"',
        'echo " FAILURES "',
        'echo "some check => ./models/a_test.ts:12:6"',
        'echo "FAILED | 2 passed | 1 failed (1s)"',
        "exit 1",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, globals(make));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("1 test(s) failed"), true);
    const summary = summaryOf(recorded);
    assertEquals(summary.status, "failed");
    assertEquals(summary.stage, "tests");
    assertEquals(summary.failed, 1);
    assertEquals(summary.failures, [
      { test: "some check", where: "./models/a_test.ts:12:6" },
    ]);
  });
});

Deno.test("names the unformatted files when formatting fails", async () => {
  await withTempRepo(async (dir) => {
    // The formatting check runs first and stops the target, so this run never
    // reaches the tests — the counts are unknown rather than zero, and saying
    // "tests failed" would send the reader to the wrong check.
    const make = await fakeMake(
      dir,
      [
        `echo "from ${dir}/extensions/models/a.ts:"`,
        'echo "error: Found 1 not formatted file in 28 files"',
        "exit 2",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, globals(make));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("need reformatting"), true);
    const summary = summaryOf(recorded);
    assertEquals(summary.stage, "format");
    assertEquals(summary.summaryParsed, false);
    assertEquals(summary.testsRun, null);
    assertEquals(summary.unformatted, ["extensions/models/a.ts"]);
  });
});

Deno.test("records a failure the target reported in neither shape", async () => {
  await withTempRepo(async (dir) => {
    // make itself failing — a missing target, a broken Makefile — is neither
    // check, and claiming one would be a guess.
    const make = await fakeMake(dir, 'echo "make: no rule" >&2; exit 2');
    const { context, recorded } = makeContext(dir, globals(make));

    await captureError(() => run.execute({ quiet: true }, context));

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "failed");
    assertEquals(summary.stage, null);
  });
});

Deno.test("records the result without failing when told to", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(
      dir,
      'echo "FAILED | 0 passed | 1 failed (1s)"; exit 1',
    );
    const { context, recorded } = makeContext(dir, globals(make));

    await run.execute({ ignoreFailure: true, quiet: true }, context);

    assertEquals(summaryOf(recorded).status, "failed");
  });
});

Deno.test("runs the target it was configured with", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(dir, 'echo "ok | 1 passed | 0 failed (1s)"');
    const { context, recorded } = makeContext(
      dir,
      globals(make, { target: "swamp-tests" }),
    );

    await run.execute({ quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("TARGET: swamp-tests"),
      true,
    );
  });
});
