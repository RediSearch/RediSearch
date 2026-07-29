/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the Python test model's `execute`.
 *
 * The point of these is the environment. SKIP_BUILD and PARALLEL are read by
 * build.sh from the environment rather than its argument parser, and passing
 * them in argv silently turned them into CMake defines instead — a bug no test
 * over argv construction can see, because the argv was correct in the sense of
 * containing what it was told to. Only observing what the process actually
 * received catches it.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./pytest.ts";
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

Deno.test("puts SKIP_BUILD and PARALLEL in the environment", async () => {
  await withTempRepo(async (dir) => {
    await writeReportingScript(dir);
    const { context, recorded } = makeContext(dir, GLOBALS);

    await run.execute({
      debug: true,
      skipBuild: true,
      parallel: 0,
      deployment: "standalone",
      enableAssert: true,
      quiet: true,
    }, context);

    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("ENV SKIP_BUILD=1"), true);
    // PARALLEL=0 must survive as a real value rather than being dropped for
    // being falsy — it is how a caller serialises the run.
    assertEquals(log.includes("ENV PARALLEL=0"), true);
    assertEquals(/ARGS:[^\n]*PARALLEL/.test(log), false);
  });
});

Deno.test("puts the shard count in the environment", async () => {
  await withTempRepo(async (dir) => {
    // runtests.sh reads SHARDS from the environment, not build.sh from argv, so
    // only observing the process catches a shard count that never applied.
    await writeReportingScript(dir, 'echo "ENV SHARDS=${SHARDS:-unset}"');
    const { context, recorded } = makeContext(dir, GLOBALS);

    await run.execute({
      debug: true,
      skipBuild: true,
      deployment: "cluster",
      shards: 5,
      enableAssert: true,
      quiet: true,
    }, context);

    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("ENV SHARDS=5"), true);
    assertEquals(/ARGS:[^\n]*SHARDS/.test(log), false);
    assertEquals(summaryOf(recorded).shards, 5);
  });
});

Deno.test("refuses a shard count without a cluster", async () => {
  await withTempRepo(async (dir) => {
    await writeReportingScript(dir);
    const { context, recorded } = makeContext(dir, GLOBALS);

    const error = await captureError(() =>
      run.execute({
        debug: true,
        deployment: "standalone",
        shards: 3,
        enableAssert: true,
        quiet: true,
      }, context)
    );

    // Silently ignored otherwise, which reads as a 3-shard run that never happened.
    assertEquals(error?.message.includes("only applies to a cluster"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("records an unset shard count as unset, not as one shard", async () => {
  await withTempRepo(async (dir) => {
    await writeReportingScript(dir, 'echo "ENV SHARDS=${SHARDS:-unset}"');
    const { context, recorded } = makeContext(dir, GLOBALS);

    await run.execute({
      debug: true,
      skipBuild: true,
      deployment: "cluster",
      enableAssert: true,
      quiet: true,
    }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV SHARDS=unset"),
      true,
    );
    // null means runtests.sh's own default of 3.
    assertEquals(summaryOf(recorded).shards, null);
  });
});

Deno.test("passes the deployment and filters through argv", async () => {
  await withTempRepo(async (dir) => {
    await writeReportingScript(dir);
    const { context, recorded } = makeContext(dir, GLOBALS);

    await run.execute({
      deployment: "cluster",
      test: "test_crash",
      testTimeout: 20,
      enableAssert: false,
      quiet: true,
    }, context);

    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("REDIS_STANDALONE=0"), true);
    assertEquals(log.includes("TEST_TIMEOUT=20"), true);
    assertEquals(log.includes("TEST=test_crash"), true);
  });
});

Deno.test("ignoreTestFailure records the failure without throwing", async () => {
  await withTempRepo(async (dir) => {
    await writeScript(dir, "build.sh", "exit 1");
    const { context, recorded } = makeContext(dir, GLOBALS);

    const error = await captureError(() =>
      run.execute({
        deployment: "standalone",
        enableAssert: false,
        quiet: true,
        ignoreTestFailure: true,
      }, context)
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).status, "failed");
  });
});
