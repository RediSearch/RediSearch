/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the llvm coverage export parser and the argument vector.
 *
 * The segment fixtures are shaped like a real `cargo llvm-cov test --json` run
 * against a crate in this workspace: `[line, column, count, hasCount,
 * isRegionEntry, isGapRegion]`, with a per-file `summary.lines` block carrying
 * the counts. Getting the segment predicate wrong is the failure mode that
 * matters — it either reports every line in the file or none of them — so it is
 * covered directly.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import {
  buildArgs,
  parseExcludes,
  parseExport,
  sawTestFailure,
  toRanges,
  uncoveredLines,
} from "./rust_coverage.ts";

Deno.test("takes the lines starting a region no test entered", () => {
  const lines = uncoveredLines([
    // Entered region: has a count.
    [128, 5, 1375, true, true, false],
    // Region close: zero count, but not an entry, so not a gap in coverage.
    [128, 36, 0, false, false, false],
    // Never entered.
    [134, 9, 0, true, true, false],
    [157, 13, 0, true, true, false],
  ]);

  assertEquals(lines, [134, 157]);
});

Deno.test("lists a line whose other region was entered", () => {
  // A one-line `if` with an unexecuted arm: the line is hit, but something on it
  // was never exercised, which is what the report is for.
  const lines = uncoveredLines([
    [42, 5, 10, true, true, false],
    [42, 20, 0, true, true, false],
  ]);

  assertEquals(lines, [42]);
});

Deno.test("reads per-file counts from the export's own summary", () => {
  const parsed = parseExport(
    JSON.stringify({
      data: [{
        files: [{
          filename: "/repo/src/redisearch_rs/qint/src/lib.rs",
          summary: { lines: { count: 81, covered: 79, percent: 97.5 } },
          segments: [[134, 9, 0, true, true, false]],
        }],
      }],
    }),
    "/repo",
  );

  assertEquals(parsed?.files.length, 1);
  assertEquals(parsed?.files[0], {
    // The repository prefix is stripped, so the path reads the way the rest of
    // the repository refers to it.
    file: "src/redisearch_rs/qint/src/lib.rs",
    coveredLines: 79,
    totalLines: 81,
    percent: 97.5,
    uncovered: [134],
  });
  assertEquals(parsed?.coveredLines, 79);
  assertEquals(parsed?.totalLines, 81);
});

Deno.test("totals every file, not just the incomplete ones", () => {
  const parsed = parseExport(
    JSON.stringify({
      data: [{
        files: [
          {
            filename: "/repo/a.rs",
            summary: { lines: { count: 10, covered: 10, percent: 100 } },
            segments: [],
          },
          {
            filename: "/repo/b.rs",
            summary: { lines: { count: 10, covered: 5, percent: 50 } },
            segments: [[3, 1, 0, true, true, false]],
          },
        ],
      }],
    }),
    "/repo",
  );

  assertEquals(parsed?.coveredLines, 15);
  assertEquals(parsed?.totalLines, 20);
});

Deno.test("leaves a path alone when it is outside the repository", () => {
  const parsed = parseExport(
    JSON.stringify({
      data: [{
        files: [{
          filename: "/elsewhere/vendored.rs",
          summary: { lines: { count: 1, covered: 0, percent: 0 } },
          segments: [],
        }],
      }],
    }),
    "/repo",
  );

  assertEquals(parsed?.files[0].file, "/elsewhere/vendored.rs");
});

Deno.test("reports nothing parsed when the run produced no export", () => {
  // A compile error leaves stderr diagnostics and an empty stdout. Inventing
  // zeroes there would read as a workspace with no coverage at all.
  assertEquals(parseExport(""), null);
  assertEquals(parseExport("error[E0433]: failed to resolve"), null);
  assertEquals(parseExport(JSON.stringify({ data: [] })), null);
});

Deno.test("collapses consecutive lines into ranges", () => {
  assertEquals(toRanges([157, 158, 164, 165, 166, 134]), [
    { start: 134, end: 134 },
    { start: 157, end: 158 },
    { start: 164, end: 166 },
  ]);
});

Deno.test("reads the exclude list out of build.sh", () => {
  const excludes = parseExcludes([
    "#!/usr/bin/env bash",
    'EXCLUDE_RUST_BENCHING_CRATES_LINKING_C="--exclude varint_bencher --exclude trie_bencher"',
    "COV=0",
  ].join("\n"));

  assertEquals(excludes, ["varint_bencher", "trie_bencher"]);
});

Deno.test("returns an empty exclude list when the variable is absent", () => {
  assertEquals(parseExcludes("COV=0\n"), []);
});

Deno.test("scopes a run to one crate", () => {
  const argv = buildArgs(
    { crate: "qint", onlyIncomplete: true },
    ["bench"],
    null,
  );

  // No --workspace, and no excludes: they only apply to a workspace run.
  assertEquals(argv, [
    "llvm-cov",
    "test",
    "--quiet",
    "--json",
    "-p",
    "qint",
  ]);
});

Deno.test("scopes a run to one manifest", () => {
  const argv = buildArgs(
    { onlyIncomplete: true },
    ["bench"],
    "/repo/src/redisearch_rs/qint/Cargo.toml",
  );

  assertEquals(argv.slice(4), [
    "--manifest-path",
    "/repo/src/redisearch_rs/qint/Cargo.toml",
  ]);
});

Deno.test("excludes the crates a workspace run cannot link", () => {
  const argv = buildArgs(
    { onlyIncomplete: true },
    ["varint_bencher", "trie_bencher"],
    null,
  );

  assertEquals(argv, [
    "llvm-cov",
    "test",
    "--quiet",
    "--json",
    "--workspace",
    "--exclude",
    "varint_bencher",
    "--exclude",
    "trie_bencher",
  ]);
});

Deno.test("asks for doctests only when told to", () => {
  assertEquals(
    buildArgs({ crate: "qint", onlyIncomplete: true }, [], null).includes(
      "--doctests",
    ),
    false,
  );
  assertEquals(
    buildArgs({ crate: "qint", doctests: true, onlyIncomplete: true }, [], null)
      .includes("--doctests"),
    true,
  );
});

Deno.test("tells llvm-cov to report despite a failing test run", () => {
  // Swallowing the exit code here is not enough on its own: llvm-cov stops
  // before writing the report when the run fails, so the floor this option
  // promises has to be asked for at the tool as well.
  assertEquals(
    buildArgs({ crate: "qint", onlyIncomplete: true }, [], null).includes(
      "--ignore-run-fail",
    ),
    false,
  );
  assertEquals(
    buildArgs(
      { crate: "qint", ignoreTestFailure: true, onlyIncomplete: true },
      [],
      null,
    ).includes("--ignore-run-fail"),
    true,
  );
});

Deno.test("recognises a failing test run in the output", () => {
  // The exit code cannot answer this under --ignore-run-fail: llvm-cov reports
  // success for having written the report, however the tests went.
  assertEquals(
    sawTestFailure([
      "running 4 tests",
      "test result: FAILED. 3 passed; 1 failed; 0 ignored",
    ]),
    true,
  );
  // cargo says it too, once, after any test binary fails.
  assertEquals(
    sawTestFailure(["error: test failed, to rerun pass `-p qint --lib`"]),
    true,
  );
});

Deno.test("does not read a passing run as a failing one", () => {
  assertEquals(
    sawTestFailure([
      "test result: ok. 4 passed; 0 failed; 0 ignored",
      "    Finished report saved to target/llvm-cov",
    ]),
    false,
  );
});

Deno.test("appends extra arguments last", () => {
  const argv = buildArgs(
    { crate: "qint", extraArgs: ["--no-fail-fast"], onlyIncomplete: true },
    [],
    null,
  );

  assertEquals(argv[argv.length - 1], "--no-fail-fast");
});

Deno.test("ignores a segment that carries no count", () => {
  // hasCount=false means the region was not instrumented, so its zero says
  // nothing about what ran. Reading it as a gap sends someone off to write a
  // test for code the coverage mapping deliberately skipped.
  assertEquals(
    uncoveredLines([
      [10, 1, 0, false, true, false],
      [20, 1, 0, true, true, false],
    ]),
    [20],
  );
});

Deno.test("ignores a gap region", () => {
  // A gap region is filler between real regions rather than code, and always
  // reports zero regardless of what executed.
  assertEquals(
    uncoveredLines([
      [30, 1, 0, true, true, true],
      [40, 1, 0, true, true, false],
    ]),
    [40],
  );
});

Deno.test("selects a toolchain ahead of the cargo subcommand", () => {
  // cargo reads `+toolchain` as its first argument, so it has to precede
  // llvm-cov rather than being appended with the other flags.
  const argv = buildArgs(
    { doctests: true, onlyIncomplete: true },
    [],
    null,
    "nightly-2026-01-01",
  );

  assertEquals(argv[0], "+nightly-2026-01-01");
  assertEquals(argv[1], "llvm-cov");
  assertEquals(argv.includes("--doctests"), true);
});

Deno.test("passes no toolchain when none was resolved", () => {
  // An ordinary run stays on whatever rust-toolchain.toml selects, exactly as
  // every other cargo invocation in the repository does.
  const argv = buildArgs({ crate: "qint", onlyIncomplete: true }, [], null);

  assertEquals(argv[0], "llvm-cov");
  assertEquals(argv.some((a) => a.startsWith("+")), false);
});
