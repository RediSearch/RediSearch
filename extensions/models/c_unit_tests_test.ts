/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the C and C++ unit test output parser and command construction.
 *
 * The fixtures reproduce the exact shape printed by sbin/unit-tests: block
 * result lines padded to a fixed width, failing test names indented beneath a
 * failed block, and a grand total. The failure cases matter most — a caller
 * branches on `failedTests`, and a passing run never exercises that path.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import {
  buildArgv,
  buildEnv,
  flavorOf,
  parseOutput,
  tailOf,
} from "./c_unit_tests.ts";

/** Build a block line the way the runner's printf does, padded to 44 columns. */
function block(label: string, status: string): string {
  return `  ${label.padEnd(44)}${status}`;
}

Deno.test("parses a fully passing run", () => {
  const parsed = parseOutput([
    block("C   Unit Tests", "PASSED (15/15)"),
    block("C++ Unit Tests", "PASSED (803/803)"),
    block("C   Coordinator Unit Tests", "PASSED (5/5)"),
    block("C++ Coordinator Unit Tests", "PASSED (149/149)"),
    "  TOTAL: 972 passed, 0 failed, 972 total",
    "  STATUS: ALL TESTS PASSED",
  ]);

  assertEquals(parsed.testsRun, 972);
  assertEquals(parsed.passed, 972);
  assertEquals(parsed.failed, 0);
  assertEquals(parsed.failedTests, []);
  assertEquals(parsed.blocks.length, 4);
  // Internal runs of spaces in the label are collapsed, so `C   Unit Tests`
  // reads as a name rather than as printf padding.
  assertEquals(parsed.blocks[0], {
    name: "C Unit Tests",
    status: "passed",
    passed: 15,
    total: 15,
  });
});

Deno.test("collects failing test names from a failed block", () => {
  const parsed = parseOutput([
    block("C   Unit Tests", "PASSED (15/15)"),
    block("C++ Unit Tests", "FAILED (801/803 passed)"),
    "    - InvertedIndexTest.TestBasic",
    "    - NumericRangeTest.TestSplit",
    block("C   Coordinator Unit Tests", "[SKIPPED]"),
    block("C++ Coordinator Unit Tests", "PASSED (149/149)"),
    "  TOTAL: 965 passed, 2 failed, 967 total",
    "  STATUS: SOME TESTS FAILED",
  ]);

  assertEquals(parsed.failed, 2);
  assertEquals(parsed.failedTests, [
    "InvertedIndexTest.TestBasic",
    "NumericRangeTest.TestSplit",
  ]);
  assertEquals(parsed.blocks[1], {
    name: "C++ Unit Tests",
    status: "failed",
    passed: 801,
    total: 803,
  });
  assertEquals(parsed.blocks[2], {
    name: "C Coordinator Unit Tests",
    status: "skipped",
    passed: 0,
    total: 0,
  });
});

Deno.test("reports a skipped block when its binaries were never built", () => {
  const parsed = parseOutput([
    block("C   Unit Tests", "[SKIPPED]"),
    block("C++ Unit Tests", "[SKIPPED]"),
    block("C   Coordinator Unit Tests", "[SKIPPED]"),
    block("C++ Coordinator Unit Tests", "[SKIPPED]"),
    "  TOTAL: 0 passed, 0 failed, 0 total",
  ]);

  assertEquals(parsed.testsRun, 0);
  assertEquals(parsed.blocks.every((b) => b.status === "skipped"), true);
});

Deno.test("marks the summary unparsed when the run died before testing", () => {
  // A compile error kills the run before sbin/unit-tests prints anything, so
  // there is no total to read and the counts must not be invented.
  const parsed = parseOutput([
    "src/foo.c:12:5: error: expected ';' before '}' token",
    "make[2]: *** [CMakeFiles/rscore.dir/foo.c.o] Error 1",
  ]);

  assertEquals(parsed.testsRun, null);
  assertEquals(parsed.passed, null);
  assertEquals(parsed.blocks, []);
  assertEquals(parsed.failedTests, []);
});

Deno.test("does not mistake ordinary output for a block line", () => {
  const parsed = parseOutput([
    "Running test: test_array (log: /repo/tests/logs/test_array.log) ... PASS",
    "  Individual test results:",
    "test_array ... PASS",
  ]);

  assertEquals(parsed.blocks, []);
  assertEquals(parsed.testsRun, null);
});

Deno.test("builds the argument vector for a sanitizer run", () => {
  const argv = buildArgv(
    {
      sanitizer: "address",
      enableAssert: true,
      deployment: undefined,
    } as never,
    "oss",
  );

  assertEquals(argv, [
    "RUN_UNIT_TESTS",
    "COORD=oss",
    "SAN=address",
    "ENABLE_ASSERT=1",
  ]);
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
  QUICK: "",
  REDISEARCH_GENERATE_HEADERS: "",
  ARCHIVE_RUST_TESTS: "",
  RUN_ARCHIVED_RUST_TESTS: "",
  RUST_PARTITION: "",
};

Deno.test("clears the build.sh controls it owns", () => {
  // Setting SKIP_BUILD when asked is only half of it: a caller who exported
  // SKIP_BUILD=1 would otherwise skip the build for a run that wanted one, and
  // the suite would run whatever binaries happened to be on disk. TEST and SAN
  // are the same hazard: build.sh initialises neither, so an exported one
  // filters or reflavors a run whose summary says otherwise.
  const args = { enableAssert: false } as never;

  assertEquals(buildEnv(args), CLEARED);
});

Deno.test("passes SKIP_BUILD through the environment, not the arguments", () => {
  // build.sh reads SKIP_BUILD from the environment; in argv it would fall
  // through to the catch-all branch and become a CMake define instead.
  const args = { skipBuild: true, enableAssert: false } as never;

  assertEquals(buildEnv(args), { ...CLEARED, SKIP_BUILD: "1" });
  assertEquals(buildArgv(args, "oss").includes("SKIP_BUILD=1"), false);
});

Deno.test("mirrors build.sh's flavor cascade", () => {
  assertEquals(flavorOf({} as never), "release");
  assertEquals(flavorOf({ debug: true } as never), "debug");
  assertEquals(flavorOf({ coverage: true } as never), "debug-cov");
  assertEquals(flavorOf({ sanitizer: "address" } as never), "debug-asan");
  // A sanitizer wins over debug, matching the order of the checks in build.sh.
  assertEquals(
    flavorOf({ debug: true, sanitizer: "address" } as never),
    "debug-asan",
  );
});

Deno.test("extracts a gtest assertion with its location and body", () => {
  // Verbatim from a ctest run with a deliberately failing gtest.
  const parsed = parseOutput([
    "[ RUN      ] ZZProbeTest.Fails",
    "/repo/tests/cpptests/test_cpp_zzprobe.cpp:19: Failure",
    "Expected equality of these values:",
    "  1",
    "  2",
    "deliberate probe failure",
    "",
    "[  FAILED  ] ZZProbeTest.Fails (0 ms)",
  ], "/repo");

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0], {
    test: "ZZProbeTest.Fails",
    kind: "assertion",
    file: "tests/cpptests/test_cpp_zzprobe.cpp",
    line: 19,
    detail: [
      "Expected equality of these values:",
      "  1",
      "  2",
      "deliberate probe failure",
    ].join("\n"),
  });
});

Deno.test("records one entry per assertion when a test trips several", () => {
  const parsed = parseOutput([
    "[ RUN      ] ZZProbeTest.MultipleFails",
    "/repo/tests/cpptests/test_cpp_zzprobe.cpp:23: Failure",
    "Expected equality of these values:",
    '  "a"',
    '  "b"',
    "",
    "/repo/tests/cpptests/test_cpp_zzprobe.cpp:24: Failure",
    "Value of: false",
    "  Actual: false",
    "Expected: true",
    "second expectation",
    "",
    "[  FAILED  ] ZZProbeTest.MultipleFails (0 ms)",
  ], "/repo");

  assertEquals(parsed.failures.length, 2);
  assertEquals(parsed.failures[0].line, 23);
  assertEquals(parsed.failures[1].line, 24);
  assertEquals(parsed.failures[1].detail.endsWith("second expectation"), true);
});

Deno.test("deduplicates an assertion ctest and the runner both print", () => {
  // ctest prints the failing test's output inline, then the runner reprints
  // the same block in its details section. Without dedup every assertion
  // would be reported twice.
  const block = [
    "[ RUN      ] ZZProbeTest.Fails",
    "/repo/a.cpp:19: Failure",
    "Expected equality of these values:",
    "  1",
    "  2",
    "",
    "[  FAILED  ] ZZProbeTest.Fails (0 ms)",
  ];
  const parsed = parseOutput([
    ...block,
    "=============== FAILED TEST DETAILS ===============",
    "------- ZZProbeTest.Fails -------",
    ...block,
  ], "/repo");

  assertEquals(parsed.failures.length, 1);
});

Deno.test("reports a crashing test even though it has no assertion", () => {
  const parsed = parseOutput([
    "ZZProbeTest.Crashes ... CRASH ()",
  ], "/repo");

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0].kind, "crash");
  assertEquals(parsed.failures[0].file, null);
  // The runner leaves the reason empty when ctest phrases it unexpectedly, so
  // something meaningful has to stand in.
  assertEquals(
    parsed.failures[0].detail,
    "test crashed without reporting a reason",
  );
});

Deno.test("keeps a crash reason when the runner extracts one", () => {
  const parsed = parseOutput(["Foo.Bar ... CRASH (SEGFAULT)"], "/repo");

  assertEquals(parsed.failures[0].detail, "SEGFAULT");
});

Deno.test("reports a timed out test", () => {
  const parsed = parseOutput(["Slow.Test ... TIMEOUT"], "/repo");

  assertEquals(parsed.failures[0].kind, "timeout");
  assertEquals(parsed.failures[0].detail, "test exceeded its time limit");
});

Deno.test("attributes each assertion to the test that was running", () => {
  const parsed = parseOutput([
    "[ RUN      ] Suite.First",
    "/repo/a.cpp:1: Failure",
    "first",
    "",
    "[ RUN      ] Suite.Second",
    "/repo/b.cpp:2: Failure",
    "second",
    "",
  ], "/repo");

  assertEquals(parsed.failures.map((f) => f.test), [
    "Suite.First",
    "Suite.Second",
  ]);
});

Deno.test("leaves a path outside the repository absolute", () => {
  const parsed = parseOutput([
    "[ RUN      ] Suite.Test",
    "/elsewhere/a.cpp:1: Failure",
    "boom",
    "",
  ], "/repo");

  assertEquals(parsed.failures[0].file, "/elsewhere/a.cpp");
});

Deno.test("reports no failures for a passing run", () => {
  const parsed = parseOutput([
    block("C   Unit Tests", "PASSED (15/15)"),
    "  TOTAL: 15 passed, 0 failed, 15 total",
  ], "/repo");

  assertEquals(parsed.failures, []);
});

Deno.test("summarises the tail of a run that died before testing", () => {
  // A run aborting on a missing tool reports only an exit code otherwise, and
  // the reason is buried in a log the caller has to go and open.
  assertEquals(
    tailOf([
      "Running unit tests...",
      "",
      "/repo/build.sh: line 332: lcov: command not found",
      "",
    ]),
    "Running unit tests... | /repo/build.sh: line 332: lcov: command not found",
  );
});

Deno.test("keeps the tail short enough to read", () => {
  const lines = Array.from({ length: 50 }, (_, i) => `line ${i}`);

  assertEquals(tailOf(lines), "line 47 | line 48 | line 49");
});
