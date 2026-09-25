/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the swamp extension suite parsers.
 *
 * The fixtures are deno's own output, which is what makes them worth pinning:
 * the closing summary, the `FAILURES` block with its source locations, and the
 * `from <path>:` headers a formatting check prints. A fixture that drifted from
 * deno would leave the model reporting a clean run for a failing one.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import {
  parseFailures,
  parseTestSummary,
  parseUnformatted,
  resolve,
} from "./swamp_tests.ts";

Deno.test("reads the counts from a passing run", () => {
  const counts = parseTestSummary([
    "running 7 tests from ./models/make_fmt_test.ts",
    "reports nothing for an already formatted tree ... ok (8ms)",
    "",
    "ok | 278 passed | 0 failed (2s)",
  ]);

  assertEquals(counts, { testsRun: 278, passed: 278, failed: 0 });
});

Deno.test("reads the counts from a failing run", () => {
  const counts = parseTestSummary(["FAILED | 7 passed | 1 failed (15ms)"]);

  assertEquals(counts, { testsRun: 8, passed: 7, failed: 1 });
});

Deno.test("reads a summary carrying deno's own extra segments", () => {
  // deno appends `N ignored`, `N filtered out` and a step count of its own, and
  // is free to move them. The counts the model reads must survive that.
  assertEquals(
    parseTestSummary(["ok | 1 passed | 0 failed | 1 ignored (19ms)"]),
    { testsRun: 1, passed: 1, failed: 0 },
  );
  assertEquals(
    parseTestSummary(["ok | 3 passed (5 steps) | 0 failed (1s)"]),
    { testsRun: 3, passed: 3, failed: 0 },
  );
  assertEquals(
    parseTestSummary(["FAILED | 2 passed | 4 filtered out | 1 failed (1s)"]),
    { testsRun: 3, passed: 2, failed: 1 },
  );
});

Deno.test("reports no counts when the run never reached the tests", () => {
  // What a formatting failure looks like: the target stops before deno test,
  // so there is no summary to read and the counts are unknown rather than zero.
  const counts = parseTestSummary([
    "Checking swamp extension formatting...",
    "error: Found 1 not formatted file in 28 files",
    "make: *** [Makefile:393: swamp-extension-tests] Error 1",
  ]);

  assertEquals(counts, null);
});

Deno.test("keeps the last summary when deno printed several", () => {
  const counts = parseTestSummary([
    "ok | 3 passed | 0 failed (1s)",
    "FAILED | 1 passed | 2 failed (1s)",
  ]);

  assertEquals(counts, { testsRun: 3, passed: 1, failed: 2 });
});

Deno.test("extracts failing tests with their locations", () => {
  const failures = parseFailures([
    "deliberately failing probe ... FAILED (15ms)",
    "",
    " FAILURES ",
    "",
    "deliberately failing probe => ./models/make_fmt_test.ts:80:6",
    "another one => ./reports/failure_digest_test.ts:12:1",
    "",
    "FAILED | 7 passed | 2 failed (15ms)",
  ]);

  assertEquals(failures, [
    {
      test: "deliberately failing probe",
      where: "./models/make_fmt_test.ts:80:6",
    },
    { test: "another one", where: "./reports/failure_digest_test.ts:12:1" },
  ]);
});

Deno.test("does not read the per-test FAILED lines as the failure list", () => {
  // Those lines carry no location, and they are interleaved with the tests that
  // passed. Only the block deno prints at the end is the list.
  const failures = parseFailures([
    "some test ... FAILED (1ms)",
    "ok | 0 passed | 1 failed (1s)",
  ]);

  assertEquals(failures, []);
});

Deno.test("keeps an arrow that belongs to the test name", () => {
  const failures = parseFailures([
    " FAILURES ",
    "maps a => b correctly => ./models/a_test.ts:1:1",
    "ok | 0 passed | 1 failed (1s)",
  ]);

  assertEquals(failures[0].test, "maps a => b correctly");
  assertEquals(failures[0].where, "./models/a_test.ts:1:1");
});

Deno.test("lists the files a formatting check would rewrite", () => {
  const files = parseUnformatted([
    "Checking swamp extension formatting...",
    "from /repo/extensions/models/make_fmt_test.ts:",
    "80 | -const  unformatted   =1",
    "80 | +const unformatted = 1;",
    "from /repo/extensions/reports/failure_digest.ts:",
    "error: Found 2 not formatted files in 28 files",
  ], "/repo/extensions");

  assertEquals(files, [
    "models/make_fmt_test.ts",
    "reports/failure_digest.ts",
  ]);
});

Deno.test("reports a file once however many hunks it has", () => {
  const files = parseUnformatted([
    "from /repo/a.ts:",
    "1 | -x",
    "from /repo/a.ts:",
    "9 | -y",
  ], "/repo");

  assertEquals(files, ["a.ts"]);
});

Deno.test("leaves a path outside the repository absolute", () => {
  const files = parseUnformatted(["from /elsewhere/a.ts:"], "/repo");

  assertEquals(files, ["/elsewhere/a.ts"]);
});

Deno.test("resolves a repo-relative root without leaking dot segments", () => {
  assertEquals(resolve("/repo", "."), "/repo");
  assertEquals(resolve("/repo", "sub/dir"), "/repo/sub/dir");
  assertEquals(resolve("/repo", "/abs"), "/abs");
});
