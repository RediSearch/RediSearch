/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the cargo nextest output parser.
 *
 * The fixtures come from a real run against deliberately failing tests, so the
 * shapes are observed rather than assumed: nextest lists every failure twice —
 * once as it happens, with the panic output beneath it, and again in the recap
 * under the summary line — and it indents a test's captured output.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { parseOutput } from "./cargo_nextest.ts";

const PREFIX = "src/redisearch_rs";

Deno.test("parses a fully passing run", () => {
  const parsed = parseOutput([
    "        PASS [   0.003s] (1/2) build_utils tests::one",
    "        PASS [   0.003s] (2/2) build_utils tests::two",
    "     Summary [   0.004s] 2 tests run: 2 passed, 1 skipped",
  ], PREFIX);

  assertEquals(parsed.testsRun, 2);
  assertEquals(parsed.passed, 2);
  assertEquals(parsed.failed, 0);
  assertEquals(parsed.skipped, 1);
  assertEquals(parsed.failedTests, []);
  assertEquals(parsed.failures, []);
});

Deno.test("extracts a failed assertion with its location and values", () => {
  const parsed = parseOutput([
    "        FAIL [   0.003s] (3/4) build_utils zzprobe_tests::probe_fails",
    "  stdout ───",
    "",
    "    running 1 test",
    "    test zzprobe_tests::probe_fails ... FAILED",
    "",
    "  stderr ───",
    "",
    "    thread 'zzprobe_tests::probe_fails' (3675368) panicked at build_utils/src/lib.rs:302:9:",
    "    assertion `left == right` failed: deliberate probe failure",
    "      left: 1",
    "     right: 2",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
    "     Summary [   0.004s] 4 tests run: 2 passed, 2 failed, 1 skipped",
  ], PREFIX);

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0], {
    test: "build_utils zzprobe_tests::probe_fails",
    kind: "panic",
    // Panics report paths relative to the Cargo workspace, so the prefix makes
    // them resolvable from the repository root.
    file: "src/redisearch_rs/build_utils/src/lib.rs",
    line: 302,
    column: 9,
    detail: [
      "assertion `left == right` failed: deliberate probe failure",
      "  left: 1",
      " right: 2",
    ].join("\n"),
  });
});

Deno.test("extracts a plain panic message", () => {
  const parsed = parseOutput([
    "        FAIL [   0.004s] (4/4) build_utils zzprobe_tests::probe_panics",
    "  stderr ───",
    "",
    "    thread 'zzprobe_tests::probe_panics' (3675370) panicked at build_utils/src/lib.rs:307:9:",
    "    deliberate probe panic",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
  ], PREFIX);

  assertEquals(parsed.failures[0].detail, "deliberate probe panic");
  assertEquals(parsed.failures[0].line, 307);
});

Deno.test("keeps a panic message that opens with a libtest-looking word", () => {
  // `panic!("test failed: ...")` is ordinary phrasing, and RUST_BACKTRACE being
  // already set removes the note that would otherwise end the message. Ending
  // the capture on the word alone would report this failure with no message at
  // all — the one thing the digest needs from it.
  const parsed = parseOutput([
    "        FAIL [   0.004s] (1/1) build_utils tests::one",
    "  stderr ───",
    "",
    "    thread 'tests::one' panicked at a.rs:7:5:",
    "    test failed: the index was not rebuilt",
    "     Summary [   0.004s] 1 test run: 0 passed, 1 failed",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "panic");
  assertEquals(
    parsed.failures[0].detail,
    "test failed: the index was not rebuilt",
  );
  assertEquals(parsed.failures[0].line, 7);
});

Deno.test("still ends a panic message at libtest's own lines", () => {
  // The narrowing must not stop the markers working: these are the real forms,
  // and each has to end the capture or the message swallows the rest of the run.
  for (
    const marker of [
      "    running 1 test",
      "    test tests::two ... ok",
      "    test result: FAILED. 0 passed; 1 failed; 0 ignored",
      "    failures:",
    ]
  ) {
    const parsed = parseOutput([
      "        FAIL [   0.004s] (1/1) build_utils tests::one",
      "    thread 'tests::one' panicked at a.rs:7:5:",
      "    boom",
      marker,
    ], PREFIX);

    assertEquals(parsed.failures[0].detail, "boom", `marker: ${marker}`);
  }
});

Deno.test("does not duplicate a failure listed again in the recap", () => {
  // The recap under the summary repeats each FAIL line with no output beneath
  // it, which would otherwise overwrite the detail with an empty one.
  const parsed = parseOutput([
    "        FAIL [   0.003s] (1/1) build_utils tests::one",
    "    thread 'tests::one' panicked at a.rs:1:1:",
    "    boom",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
    "     Summary [   0.004s] 1 tests run: 0 passed, 1 failed",
    "        FAIL [   0.003s] (1/1) build_utils tests::one",
  ], PREFIX);

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0].detail, "boom");
});

Deno.test("keeps the panic message when the backtrace note is absent", () => {
  // RUST_BACKTRACE already set in the environment suppresses the note, so the
  // next marker has to end the message instead.
  const parsed = parseOutput([
    "        FAIL [   0.003s] (1/2) build_utils tests::one",
    "    thread 'tests::one' panicked at a.rs:1:1:",
    "    boom",
    "        PASS [   0.003s] (2/2) build_utils tests::two",
  ], PREFIX);

  assertEquals(parsed.failures[0].detail, "boom");
});

Deno.test("reports a test killed by a signal", () => {
  const parsed = parseOutput([
    "     SIGSEGV [   0.005s] (1/1) build_utils tests::segfault",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "signal");
  assertEquals(parsed.failures[0].file, null);
  assertEquals(parsed.failures[0].detail, "test was killed by a signal");
});

Deno.test("reports a timed out test", () => {
  const parsed = parseOutput([
    "     TIMEOUT [  60.000s] (1/1) build_utils tests::slow",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "timeout");
  assertEquals(parsed.failures[0].detail, "test exceeded its time limit");
});

Deno.test("reports a test that failed without panicking", () => {
  // A test returning Err fails with no panic to report.
  const parsed = parseOutput([
    "        FAIL [   0.003s] (1/1) build_utils tests::returns_err",
    "     Summary [   0.004s] 1 tests run: 0 passed, 1 failed",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "failure");
  assertEquals(
    parsed.failures[0].detail,
    "test failed without panicking, e.g. by returning an error",
  );
});

Deno.test("attributes each panic to the test it followed", () => {
  const parsed = parseOutput([
    "        FAIL [   0.003s] (1/2) build_utils tests::first",
    "    thread 'tests::first' panicked at a.rs:1:1:",
    "    first boom",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
    "        FAIL [   0.003s] (2/2) build_utils tests::second",
    "    thread 'tests::second' panicked at b.rs:2:2:",
    "    second boom",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
  ], PREFIX);

  assertEquals(parsed.failures.map((f) => f.detail), [
    "first boom",
    "second boom",
  ]);
  assertEquals(parsed.failures[1].file, "src/redisearch_rs/b.rs");
});

Deno.test("leaves an absolute panic path alone", () => {
  const parsed = parseOutput([
    "        FAIL [   0.003s] (1/1) build_utils tests::one",
    "    thread 'tests::one' panicked at /elsewhere/a.rs:1:1:",
    "    boom",
  ], PREFIX);

  assertEquals(parsed.failures[0].file, "/elsewhere/a.rs");
});

// The miri fixtures below are verbatim from a `cargo miri nextest run` against a
// crate written to trip each check, so the shapes are observed rather than
// assumed. Miri reports what it catches as a rustc-style diagnostic under the
// test's stderr, not as a panic, so none of the cases above would notice it.

Deno.test("extracts undefined behaviour miri caught", () => {
  const parsed = parseOutput([
    "        FAIL [   0.955s] (1/5) miriprobe ub_dangling_read",
    "  stderr ───",
    "    error: Undefined Behavior: memory access failed: attempting to access 1 byte, but got 0x1000[noalloc] which is a dangling pointer (it has no provenance)",
    "     --> src/lib.rs:4:22",
    "      |",
    "    4 |     let v = unsafe { *p };",
    "      |                      ^^ Undefined Behavior occurred here",
    "      |",
    "      = help: this indicates a bug in the program: it performed an invalid operation, and caused Undefined Behavior",
    "    error: aborting due to 1 previous error; 1 warning emitted",
    "     Summary [   1.494s] 5 tests run: 1 passed, 4 failed, 0 skipped",
  ], PREFIX);

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0], {
    test: "miriprobe ub_dangling_read",
    // A native run passes this test, so the kind has to distinguish it from an
    // ordinary failure rather than reporting it as one.
    kind: "undefined-behavior",
    file: "src/redisearch_rs/src/lib.rs",
    line: 4,
    column: 22,
    detail:
      "Undefined Behavior: memory access failed: attempting to access 1 byte, but got 0x1000[noalloc] which is a dangling pointer (it has no provenance)",
  });
});

Deno.test("ignores a miri warning preceding the error", () => {
  // Miri warns about integer-to-pointer casts before reporting what it caught.
  // The warning carries its own source location, which must not be mistaken for
  // the failure's.
  const parsed = parseOutput([
    "        FAIL [   0.955s] (1/1) miriprobe ub_dangling_read",
    "  stderr ───",
    "    warning: integer-to-pointer cast",
    "     --> src/lib.rs:3:24",
    "      |",
    "      = help: this program is using integer-to-pointer casts",
    "    error: Undefined Behavior: memory access failed",
    "     --> src/lib.rs:4:22",
  ], PREFIX);

  assertEquals(parsed.failures[0].line, 4);
  assertEquals(
    parsed.failures[0].detail,
    "Undefined Behavior: memory access failed",
  );
});

Deno.test("keeps the first location when miri reports a second one", () => {
  // Miri points at the allocation as well as the access, and the access is
  // where the bug is.
  const parsed = parseOutput([
    "        FAIL [   1.260s] (1/1) miriprobe ub_uninit",
    "  stderr ───",
    "    error: Undefined Behavior: in-bounds pointer arithmetic failed",
    "     --> src/lib.rs:12:23",
    "      |",
    "    help: alloc43562 was allocated here:",
    "     --> src/lib.rs:10:9",
  ], PREFIX);

  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0].line, 12);
  assertEquals(parsed.failures[0].column, 23);
});

Deno.test("reports a leak miri caught as a leak", () => {
  // nextest marks the test FAIL, because the leak is miri's finding rather than
  // nextest's own LEAK-FAIL detection, so the kind comes from the diagnostic.
  const parsed = parseOutput([
    "        FAIL [   1.083s] (1/1) miriprobe leaks",
    "  stdout ───",
    "    running 1 test",
    "    test leaks ... ok",
    "  stderr ───",
    "    error: memory leaked: alloc43463 (Rust heap, size: 8, align: 8), allocated here:",
    "     --> src/lib.rs:23:13",
    "    note: set `MIRIFLAGS=-Zmiri-ignore-leaks` to disable this check",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "leak");
  assertEquals(parsed.failures[0].line, 23);
  assertEquals(
    parsed.failures[0].detail,
    "memory leaked: alloc43463 (Rust heap, size: 8, align: 8), allocated here:",
  );
});

Deno.test("reports an operation miri cannot interpret as unsupported", () => {
  // Not a bug in the test: it says nothing either way and belongs behind
  // #[cfg(not(miri))], which is a different fix from a real failure.
  const parsed = parseOutput([
    "        FAIL [   0.500s] (1/1) trie_ffi tests::calls_c",
    "  stderr ───",
    "    error: unsupported operation: can't call foreign function `RediSearch_Init` on OS `linux`",
    "     --> trie_ffi/src/lib.rs:42:5",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "unsupported");
  assertEquals(
    parsed.failures[0].file,
    "src/redisearch_rs/trie_ffi/src/lib.rs",
  );
});

Deno.test("attributes each miri diagnostic to the test it followed", () => {
  const parsed = parseOutput([
    "        FAIL [   0.955s] (1/2) miriprobe first",
    "  stderr ───",
    "    error: Undefined Behavior: first finding",
    "     --> src/lib.rs:4:22",
    "        FAIL [   1.260s] (2/2) miriprobe second",
    "  stderr ───",
    "    error: Undefined Behavior: second finding",
    "     --> src/lib.rs:12:23",
  ], PREFIX);

  assertEquals(parsed.failures.map((f) => f.detail), [
    "Undefined Behavior: first finding",
    "Undefined Behavior: second finding",
  ]);
  assertEquals(parsed.failures.map((f) => f.line), [4, 12]);
});

Deno.test("prefers the panic when a test both panicked and was diagnosed", () => {
  // A panic under miri is an ordinary test failure, and miri goes on to report
  // the allocations the unwind abandoned. The panic is the proximate failure.
  const parsed = parseOutput([
    "        FAIL [   1.494s] (1/1) miriprobe plain_panic",
    "  stderr ───",
    "    thread 'plain_panic' (1001) panicked at src/lib.rs:18:5:",
    "    deliberate probe panic",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
    "    error: memory leaked: alloc1 (Rust heap, size: 8, align: 8), allocated here:",
    "     --> src/lib.rs:23:13",
  ], PREFIX);

  assertEquals(parsed.failures[0].kind, "panic");
  assertEquals(parsed.failures[0].detail, "deliberate probe panic");
  assertEquals(parsed.failures[0].line, 18);
});

Deno.test("does not attribute a compile error to the last test that failed", () => {
  // `error: aborting due to ...` follows every diagnostic, and a compile error
  // is an `error:` line too. Neither may overwrite a failure's own detail.
  const parsed = parseOutput([
    "        FAIL [   0.003s] (1/1) build_utils tests::one",
    "    thread 'tests::one' panicked at a.rs:1:1:",
    "    boom",
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
    "error[E0433]: failed to resolve: use of undeclared crate `foo`",
    "error: could not compile `build_utils` (lib test) due to 1 previous error",
  ], PREFIX);

  assertEquals(parsed.failures[0].detail, "boom");
  assertEquals(parsed.failures[0].kind, "panic");
});

Deno.test("marks the summary unparsed when the run died before testing", () => {
  // A compile error means no summary line, and inventing zeroes there would
  // read downstream as a clean run.
  const parsed = parseOutput([
    "error[E0433]: failed to resolve: use of undeclared crate `foo`",
    "error: could not compile `build_utils` (lib test) due to 1 previous error",
  ], PREFIX);

  assertEquals(parsed.testsRun, null);
  assertEquals(parsed.failed, null);
  assertEquals(parsed.failures, []);
});

Deno.test("parses the singular summary line of a one-test run", () => {
  // nextest pluralises by count, so narrowing to a single test ends "1 test
  // run". Accepting only the plural left a good run looking unparsed, and
  // rust-quick asserts tests actually ran — so it failed the run whose test had
  // just passed, in exactly the loop that workflow exists for.
  const parsed = parseOutput([
    "        PASS [   0.155s] (1/1) qint tests::one",
    "     Summary [   0.155s] 1 test run: 1 passed, 50 skipped",
  ], PREFIX);

  // testsRun stays null when the summary line goes unmatched, which is what the
  // model reports as summaryParsed: false.
  assertEquals(parsed.testsRun, 1);
  assertEquals(parsed.passed, 1);
  assertEquals(parsed.failures.length, 0);
});

Deno.test("drops a test that failed an attempt and passed on retry", () => {
  // With retries on, nextest reports every attempt as it happens, so the failed
  // one is recorded before the run is known to have succeeded. It exits 0 and
  // counts the test as passed, so reporting the attempt would name a failing
  // test on a green run.
  const parsed = parseOutput([
    "        TRY 1 FAIL [   0.005s] qint tests::flaky",
    "",
    "--- STDERR:              qint tests::flaky ---",
    "thread 'tests::flaky' panicked at qint/src/lib.rs:10:5:",
    "assertion failed",
    "",
    "        TRY 2 PASS [   0.004s] qint tests::flaky",
    "       FLAKY 2/3 [   0.021s] qint tests::flaky",
    "     Summary [   0.021s] 1 test run: 1 passed (1 flaky), 25 skipped",
  ], PREFIX);

  assertEquals(parsed.testsRun, 1);
  assertEquals(parsed.failed, 0);
  assertEquals(parsed.failedTests, []);
  assertEquals(parsed.failures.length, 0);
});

Deno.test("keeps a real failure alongside a flaky one", () => {
  // The count alone cannot separate these — two attempts failed and only one
  // test recovered — so the FLAKY line is what identifies which to drop.
  const parsed = parseOutput([
    "        TRY 1 FAIL [   0.005s] qint tests::flaky",
    "       FLAKY 2/3 [   0.021s] qint tests::flaky",
    "        FAIL [   0.003s] qint tests::broken",
    "",
    "--- STDERR:              qint tests::broken ---",
    "thread 'tests::broken' panicked at qint/src/lib.rs:20:5:",
    "really broken",
    "",
    "     Summary [   0.021s] 2 tests run: 1 passed (1 flaky), 1 failed",
  ], PREFIX);

  assertEquals(parsed.failed, 1);
  assertEquals(parsed.failedTests, ["qint tests::broken"]);
  assertEquals(parsed.failures.length, 1);
  assertEquals(parsed.failures[0].test, "qint tests::broken");
});

Deno.test("trusts a zero failure count over recorded attempts", () => {
  // The backstop for a FLAKY line that is absent or reworded: a run that says
  // it failed nothing did not fail anything.
  const parsed = parseOutput([
    "        TRY 1 FAIL [   0.005s] qint tests::flaky",
    "        TRY 2 PASS [   0.004s] qint tests::flaky",
    "     Summary [   0.021s] 1 test run: 1 passed (1 flaky), 25 skipped",
  ], PREFIX);

  assertEquals(parsed.failedTests, []);
  assertEquals(parsed.failures.length, 0);
});
