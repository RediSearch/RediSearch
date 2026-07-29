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

Deno.test("ignoreTestFailure records the failure without throwing", async () => {
  await withTempRepo(async (dir) => {
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

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).status, "failed");
  });
});
