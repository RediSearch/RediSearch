/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the Python behavioural test output parser and command construction.
 *
 * The fixtures are taken verbatim from a real RLTest run with deliberately
 * failing tests, so the failure shapes are observed rather than assumed: a
 * result line follows its test header on the next line, an unhandled exception
 * reports `[ERROR]` rather than `[FAIL]`, and a closing `Failed Tests Summary:`
 * section repeats the names in a different, indented form.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { buildArgv, buildEnv, parseOutput } from "./pytest.ts";

Deno.test("parses a fully passing run", () => {
  const parsed = parseOutput([
    "test_case:testCaseFunction:",
    "\t[PASS]",
    "test_case:testCaseWithComparison:",
    "\t[PASS]",
    "",
    "Test Took: 0 sec",
    "Total Tests Run: 2, Total Tests Failed: 0, Total Tests Passed: 2",
  ]);

  assertEquals(parsed.testsRun, 2);
  assertEquals(parsed.passed, 2);
  assertEquals(parsed.failed, 0);
  assertEquals(parsed.failedTests, []);
});

Deno.test("collects failed and errored tests, but not passing ones", () => {
  const parsed = parseOutput([
    "test_zzprobe:testProbeFails:",
    "\t[FAIL]",
    "test_zzprobe:testProbePasses:",
    "\t[PASS]",
    "test_zzprobe:testProbeRaises:",
    "\t[ERROR]",
    "\tUnhandled exception: deliberate probe exception",
    "Traceback (most recent call last):",
    '  File "/repo/tests/pytests/test_zzprobe.py", line 10, in testProbeRaises',
    "    raise RuntimeError('deliberate probe exception')",
    "RuntimeError: deliberate probe exception",
    "",
    "Test Took: 0 sec",
    "Total Tests Run: 3, Total Tests Failed: 2, Total Tests Passed: 1",
    "Failed Tests Summary:",
    "\ttest_zzprobe:testProbeFails",
    "\t\t❌  (FAIL):\t1 == 2\ttest_zzprobe.py:7 [deliberate probe failure]",
    "\ttest_zzprobe:testProbeRaises",
  ]);

  assertEquals(parsed.testsRun, 3);
  assertEquals(parsed.passed, 1);
  assertEquals(parsed.failed, 2);
  assertEquals(parsed.failedTests, [
    "test_zzprobe:testProbeFails",
    "test_zzprobe:testProbeRaises",
  ]);
});

Deno.test("does not double count names repeated in the failure summary", () => {
  // The closing summary lists each failing test again, indented. Those lines
  // must not register as fresh failures, or every failure would be counted
  // twice.
  const parsed = parseOutput([
    "test_a:testOne:",
    "\t[FAIL]",
    "Total Tests Run: 1, Total Tests Failed: 1, Total Tests Passed: 0",
    "Failed Tests Summary:",
    "\ttest_a:testOne",
    "\t\t❌  (FAIL):\tassertion\ttest_a.py:3",
  ]);

  assertEquals(parsed.failedTests, ["test_a:testOne"]);
  assertEquals(parsed.failedTests.length, 1);
});

Deno.test("counts skips separately from failures", () => {
  const parsed = parseOutput([
    "test_a:testOne:",
    "\t[SKIP]",
    "test_a:testTwo:",
    "\t[PASS]",
    "Total Tests Run: 2, Total Tests Failed: 0, Total Tests Passed: 1",
  ]);

  assertEquals(parsed.skipped, 1);
  assertEquals(parsed.failedTests, []);
});

Deno.test("marks the summary unparsed when the run died before testing", () => {
  // No totals line means the suite never started — a build failure, or a
  // module that would not load. Inventing zeroes here would read downstream as
  // a clean run.
  const parsed = parseOutput([
    "Error: Cannot find RediSearch module binary in /repo/bin/linux-x64-debug",
  ]);

  assertEquals(parsed.testsRun, null);
  assertEquals(parsed.passed, null);
  assertEquals(parsed.failed, null);
  assertEquals(parsed.skipped, null);
  assertEquals(parsed.failedTests, []);
});

Deno.test("falls back to counting failures when totals are missing", () => {
  // A run killed partway through still names the tests that failed before it
  // died, and that list is more useful than nothing.
  const parsed = parseOutput([
    "test_a:testOne:",
    "\t[FAIL]",
    "test_a:testTwo:",
    "\t[FAIL]",
  ]);

  assertEquals(parsed.testsRun, null);
  assertEquals(parsed.failed, 2);
  assertEquals(parsed.failedTests, ["test_a:testOne", "test_a:testTwo"]);
});

Deno.test("maps deployment onto REDIS_STANDALONE", () => {
  const standalone = buildArgv(
    { deployment: "standalone", enableAssert: false } as never,
    "oss",
  );
  const cluster = buildArgv(
    { deployment: "cluster", enableAssert: false } as never,
    "oss",
  );

  assertEquals(standalone.includes("REDIS_STANDALONE=1"), true);
  assertEquals(cluster.includes("REDIS_STANDALONE=0"), true);
});

/**
 * The controls the model clears so an inherited one cannot decide for a run
 * that did not ask. Empty is how build.sh spells "unset".
 */
const CLEARED = {
  SKIP_BUILD: "",
  SAN: "",
  COV: "",
  TEST: "",
  TEST_FILTER: "",
  TEST_TIMEOUT: "",
  QUICK: "",
  REDIS_STANDALONE: "",
  SA: "",
  SHARDS: "",
  PARALLEL: "",
  REDISEARCH_GENERATE_HEADERS: "",
  ARCHIVE_RUST_TESTS: "",
  RUN_ARCHIVED_RUST_TESTS: "",
  RUST_PARTITION: "",
};

Deno.test("clears the build.sh controls it owns", () => {
  // Setting SKIP_BUILD when asked is only half of it: a caller who exported
  // SKIP_BUILD=1 would otherwise skip the build for a run that wanted one, and
  // the suite would test whatever artifacts happened to be on disk. TEST, SAN
  // and SA are the same hazard one level along: an exported TEST narrows the
  // suite the summary says ran in full, and SA picks the topology runtests.sh
  // uses regardless of the REDIS_STANDALONE this model passes.
  const args = { deployment: "standalone", enableAssert: false } as never;

  assertEquals(buildEnv(args), CLEARED);
});

Deno.test("passes SKIP_BUILD and PARALLEL through the environment", () => {
  // build.sh reads both from the environment; in argv they would fall through
  // to the catch-all branch and become CMake defines instead.
  const args = {
    skipBuild: true,
    parallel: 0,
    deployment: "standalone",
    enableAssert: false,
  } as never;

  assertEquals(buildEnv(args), { ...CLEARED, SKIP_BUILD: "1", PARALLEL: "0" });

  const argv = buildArgv(args, "oss");
  assertEquals(argv.some((a) => a.startsWith("SKIP_BUILD")), false);
  assertEquals(argv.some((a) => a.startsWith("PARALLEL")), false);
});

Deno.test("passes the shard count through the environment", () => {
  // One level further than SKIP_BUILD: build.sh has no SHARDS argument either,
  // and it is runtests.sh that reads it from the environment. In argv it would
  // become -DSHARDS=n and the cluster would quietly keep the default of 3 —
  // which is why `./build.sh RUN_PYTEST SHARDS=3` appears to work.
  const args = {
    shards: 5,
    deployment: "cluster",
    enableAssert: false,
  } as never;

  assertEquals(buildEnv(args), { ...CLEARED, SHARDS: "5" });

  const argv = buildArgv(args, "oss");
  assertEquals(argv.some((a) => a.startsWith("SHARDS")), false);
  // The topology itself is an argument, unlike the shard count.
  assertEquals(argv.includes("REDIS_STANDALONE=0"), true);
});

Deno.test("omits the shard count when it was not asked for", () => {
  const args = { deployment: "cluster", enableAssert: false } as never;

  assertEquals(buildEnv(args), CLEARED);
});

Deno.test("omits an empty test filter rather than passing TEST=", () => {
  // The workflow passes an unset filter through as "", which must mean
  // "everything" rather than a filter matching nothing.
  const argv = buildArgv(
    { test: "", deployment: "standalone", enableAssert: false } as never,
    "oss",
  );

  assertEquals(argv.some((a) => a.startsWith("TEST=")), false);
});

Deno.test("extracts the assertion, location and message of each failure", () => {
  // Verbatim from an RLTest run with deliberately failing tests.
  const parsed = parseOutput([
    "Total Tests Run: 4, Total Tests Failed: 3, Total Tests Passed: 1",
    "Failed Tests Summary:",
    "\ttest_zzprobe:testProbeFails",
    "\t\t❌  (FAIL):\t1 == 2\ttest_zzprobe.py:7 [deliberate probe failure]",
    "\ttest_zzprobe:testProbeRaises",
    "\t\tException raised during test execution. See logs",
    "\ttest_zzprobe:testProbeMultipleFails",
    "\t\t❌  (FAIL):\t'a' == 'b'\ttest_zzprobe.py:10",
    "\t\t❌  (FAIL):\tFalse == True\ttest_zzprobe.py:11 [second assertion]",
    "Some Python tests failed. Check the test logs above for details.",
  ]);

  assertEquals(parsed.failures.length, 4);

  assertEquals(parsed.failures[0], {
    test: "test_zzprobe:testProbeFails",
    assertion: "1 == 2",
    location: "test_zzprobe.py:7",
    message: "deliberate probe failure",
    raw: "❌  (FAIL):\t1 == 2\ttest_zzprobe.py:7 [deliberate probe failure]",
  });

  // An unhandled exception has no structure to extract and points at the logs.
  assertEquals(parsed.failures[1], {
    test: "test_zzprobe:testProbeRaises",
    assertion: null,
    location: null,
    message: null,
    raw: "Exception raised during test execution. See logs",
  });

  // A test tripping several assertions yields one entry per assertion.
  assertEquals(parsed.failures[2].assertion, "'a' == 'b'");
  assertEquals(parsed.failures[2].message, null);
  assertEquals(parsed.failures[3].assertion, "False == True");
  assertEquals(parsed.failures[3].message, "second assertion");

  // The three distinct tests are still reported once each.
  assertEquals(parsed.failedTests.length, 3);
});

Deno.test("recovers a failing test that never printed a result line", () => {
  // A test killed mid-run has no [FAIL] line, but the closing section still
  // names it, so it must not be lost.
  const parsed = parseOutput([
    "Total Tests Run: 1, Total Tests Failed: 1, Total Tests Passed: 0",
    "Failed Tests Summary:",
    "\ttest_a:testDied",
    "\t\tException raised during test execution. See logs",
  ]);

  assertEquals(parsed.failedTests, ["test_a:testDied"]);
});

Deno.test("stops reading the failure section at unindented output", () => {
  const parsed = parseOutput([
    "Failed Tests Summary:",
    "\ttest_a:testOne",
    "\t\t❌  (FAIL):\t1 == 2\ttest_a.py:3",
    "One or more test suites had failures",
    "test_b:testTwo:",
    "\t[PASS]",
  ]);

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failedTests, ["test_a:testOne"]);
});

Deno.test("reports no failures for a clean run", () => {
  const parsed = parseOutput([
    "test_a:testOne:",
    "\t[PASS]",
    "Total Tests Run: 1, Total Tests Failed: 0, Total Tests Passed: 1",
  ]);

  assertEquals(parsed.failures, []);
});
