/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the C and C++ unit test model's `execute`.
 *
 * The point of these is the environment. SKIP_BUILD is read by build.sh from
 * the environment rather than its argument parser, and passing it in argv
 * silently turned it into a CMake define instead — a bug no test over argv
 * construction can see, because the argv was correct in the sense of containing
 * what it was told to. Only observing what the process actually received
 * catches it.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./c_unit_tests.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
  writeReportingScript,
  writeScript,
} from "./harness_test.ts";

const run = model.methods.run;
const GLOBALS = { repoRoot: ".", buildScript: "./build.sh", coord: "oss" };

Deno.test("puts SKIP_BUILD in the environment, not argv", async () => {
  await withTempRepo(async (dir) => {
    await writeReportingScript(dir);
    const { context, recorded } = makeContext(dir, GLOBALS);

    await run.execute(
      { debug: true, skipBuild: true, enableAssert: true, quiet: true },
      context,
    );

    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("ENV SKIP_BUILD=1"), true);
    // In argv it would fall through build.sh's catch-all branch and reach
    // CMake as -DSKIP_BUILD=1, leaving the build to run anyway.
    assertEquals(
      log.includes("ARGS: RUN_UNIT_TESTS COORD=oss DEBUG=1 ENABLE_ASSERT=1"),
      true,
    );
    assertEquals(/ARGS:[^\n]*SKIP_BUILD/.test(log), false);
  });
});

Deno.test("omits SKIP_BUILD when not asked for", async () => {
  await withTempRepo(async (dir) => {
    await writeReportingScript(dir);
    const { context, recorded } = makeContext(dir, GLOBALS);

    await run.execute(
      { debug: true, enableAssert: true, quiet: true },
      context,
    );

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV SKIP_BUILD=unset"),
      true,
    );
  });
});

Deno.test("a run with no summary is not read as a clean pass", async () => {
  await withTempRepo(async (dir) => {
    // A compile error kills the run before any test output appears.
    await writeScript(
      dir,
      "build.sh",
      ["echo 'src/foo.c:1:1: error: boom'", "exit 2"].join("\n"),
    );
    const { context, recorded } = makeContext(dir, GLOBALS);

    const error = await captureError(() =>
      run.execute({ debug: true, enableAssert: false, quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "failed");
    assertEquals(summary.summaryParsed, false);
    assertEquals(summary.testsRun, null);
    assertEquals(error?.message.includes("exited with code 2"), true);
  });
});

Deno.test("times out even when a child outlives build.sh", async () => {
  await withTempRepo(async (dir) => {
    // The background sleep inherits the pipes and is not signalled when the
    // script is, which is what a compiler or a test server does in a real run.
    // Waiting for the pipes to close would mean waiting for it, and the timeout
    // is only looked at after the readers finish.
    await writeScript(dir, "build.sh", "sleep 30 &\nexec sleep 30");
    const { context, recorded } = makeContext(dir, GLOBALS);

    const startedAt = Date.now();
    const error = await captureError(() =>
      run.execute(
        { debug: true, enableAssert: false, quiet: true, timeout: 200 },
        context,
      )
    );

    assertEquals(error?.message.includes("timed out after 200ms"), true);
    assertEquals(summaryOf(recorded).timedOut, true);
    // Generously above the timeout and far below the orphan's lifetime, so
    // this fails on a run that waited for the orphan without being flaky on a
    // loaded machine.
    assertEquals(Date.now() - startedAt < 10_000, true);
  });
});

Deno.test("a cancelled run records no summary", async () => {
  await withTempRepo(async (dir) => {
    await writeScript(dir, "build.sh", "exec sleep 30");
    const controller = new AbortController();
    const { context, recorded } = makeContext(dir, GLOBALS, controller.signal);

    const started = run.execute(
      { debug: true, enableAssert: false, quiet: true },
      context,
    );
    setTimeout(() => controller.abort(), 100);
    const error = await captureError(() => started);

    // Cancelling tested nothing, so a summary of zeroes would misrepresent it
    // as a passing run. The partial log is kept.
    assertEquals(error?.message.includes("was cancelled"), true);
    assertEquals(recorded.resources.length, 0);
    assertEquals(recorded.files.length, 1);
  });
});

Deno.test("refuses a modelled switch smuggled through extraArgs", async () => {
  await withTempRepo(async (dir) => {
    await writeScript(
      dir,
      "build.sh",
      'echo "TOTAL: 1 passed, 0 failed, 1 total"',
    );
    const { context, recorded } = makeContext(dir, GLOBALS);

    // The summary reports `testFilter` from the typed field, so this would run
    // one test and record the run as unfiltered — which the failure digest then
    // reads as licence to report the skipped blocks as problems.
    const filtered = await captureError(() =>
      run.execute({
        debug: true,
        enableAssert: false,
        quiet: true,
        extraArgs: ["TEST=some_test"],
      }, context)
    );
    assertEquals(filtered?.message.includes("extraArgs sets TEST"), true);

    // And the flavor, which decides the directory the binaries are looked for
    // in as well as what the summary names.
    const variant = await captureError(() =>
      run.execute({
        enableAssert: false,
        quiet: true,
        extraArgs: ["DEBUG=1"],
      }, context)
    );
    assertEquals(variant?.message.includes("extraArgs sets DEBUG"), true);

    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("ignoreTestFailure records the failure without throwing", async () => {
  await withTempRepo(async (dir) => {
    await writeScript(
      dir,
      "build.sh",
      [
        'echo "TOTAL: 0 passed, 1 failed, 1 total"',
        "exit 1",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, GLOBALS);

    const error = await captureError(() =>
      run.execute({
        debug: true,
        enableAssert: false,
        quiet: true,
        ignoreTestFailure: true,
      }, context)
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).status, "failed");
    assertEquals(summaryOf(recorded).testsRun, 1);
  });
});

Deno.test("ignoreTestFailure does not forgive a suite that never ran", async () => {
  await withTempRepo(async (dir) => {
    // A build that dies before ctest starts prints no TOTAL line, so nothing
    // parses. Tolerating that would record `summaryParsed: false` beside a
    // passing method, which every gate downstream reads as a green suite.
    await writeScript(dir, "build.sh", "exit 1");
    const { context, recorded } = makeContext(dir, GLOBALS);

    const error = await captureError(() =>
      run.execute({
        debug: true,
        enableAssert: false,
        quiet: true,
        ignoreTestFailure: true,
      }, context)
    );

    assertEquals(error?.message.includes("exited with code 1"), true);
    assertEquals(summaryOf(recorded).summaryParsed, false);
  });
});
