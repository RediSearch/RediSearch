/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the Rust lint output parser and command construction.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { buildArgs, parseExcludes, parseSections } from "./rust_lint.ts";

Deno.test("parses a clippy warning with its source location", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: [
      "warning: unused variable: `x`",
      "  --> src/foo.rs:10:5",
      "   |",
      "10 |     let x = 1;",
      "   |         ^ help: prefix it with an underscore",
      "   |",
    ],
  }]);

  assertEquals(findings.length, 1);
  assertEquals(findings[0], {
    level: "warning",
    code: null,
    message: "unused variable: `x`",
    file: "src/foo.rs",
    line: 10,
    column: 5,
    profile: "debug",
  });
});

Deno.test("captures the rustc diagnostic code when present", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: [
      "error[E0433]: failed to resolve: use of undeclared crate `foo`",
      "  --> src/bar.rs:3:5",
    ],
  }]);

  assertEquals(findings[0].code, "E0433");
  assertEquals(findings[0].level, "error");
  assertEquals(findings[0].file, "src/bar.rs");
});

Deno.test("deduplicates findings reported by both profiles", () => {
  const findings = parseSections([
    { profile: "debug", lines: ["warning: needless borrow", "  --> a.rs:1:1"] },
    {
      profile: "release",
      lines: ["warning: needless borrow", "  --> a.rs:1:1"],
    },
  ]);

  assertEquals(findings.length, 1);
  // The first occurrence wins, so the profile is the one that reported it first.
  assertEquals(findings[0].profile, "debug");
});

Deno.test("keeps a finding that only the release profile reports", () => {
  const findings = parseSections([
    { profile: "debug", lines: [] },
    {
      profile: "release",
      lines: ["warning: this loop never actually loops", "  --> a.rs:7:1"],
    },
  ]);

  assertEquals(findings.length, 1);
  assertEquals(findings[0].profile, "release");
});

Deno.test("keeps distinct findings that differ only by location", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: [
      "warning: needless borrow",
      "  --> a.rs:1:1",
      "warning: needless borrow",
      "  --> a.rs:9:1",
    ],
  }]);

  assertEquals(findings.length, 2);
});

Deno.test("ignores rollup lines that only restate counts", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: [
      "error: could not compile `foo` (lib) due to 2 previous errors",
      "warning: `foo` (lib) generated 3 warnings",
    ],
  }]);

  assertEquals(findings.length, 0);
});

Deno.test("does not treat indented snippet text as a diagnostic header", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: [
      "warning: unused variable: `x`",
      "  --> src/foo.rs:10:5",
      "   = note: `error: something` appears inside this snippet",
    ],
  }]);

  assertEquals(findings.length, 1);
});

Deno.test("leaves location fields null when no location follows", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: ["error: RUSTDOCFLAGS caused a failure with no span"],
  }]);

  assertEquals(findings.length, 1);
  assertEquals(findings[0].file, null);
  assertEquals(findings[0].line, null);
});

Deno.test("a clean run yields no findings", () => {
  const findings = parseSections([{
    profile: "debug",
    lines: ["    Finished `dev` profile [unoptimized] target(s) in 0.10s"],
  }]);

  assertEquals(findings.length, 0);
});

Deno.test("reads the exclude list out of build.sh", () => {
  const excluded = parseExcludes([
    "#!/bin/bash",
    'SOMETHING_ELSE="--exclude nope"',
    'EXCLUDE_RUST_BENCHING_CRATES_LINKING_C="--exclude trie_bencher --exclude triemap_ffi"',
  ].join("\n"));

  assertEquals(excluded, ["trie_bencher", "triemap_ffi"]);
});

Deno.test("returns an empty exclude list when the variable is absent", () => {
  assertEquals(parseExcludes("#!/bin/bash\necho hi\n"), []);
});

Deno.test("clippy passes lint flags after a bare --", () => {
  const argv = buildArgs("clippy", "debug", ["bencher"], {});

  assertEquals(argv, [
    "clippy",
    "--workspace",
    "--exclude",
    "bencher",
    "--",
    "-D",
    "warnings",
  ]);
});

Deno.test("doc passes rustdoc flags and no trailing --", () => {
  const argv = buildArgs("doc", "release", [], {});

  assertEquals(argv, [
    "doc",
    "--workspace",
    "--no-deps",
    "--document-private-items",
    "--release",
  ]);
});

Deno.test("extra args land before clippy's -- separator", () => {
  const argv = buildArgs("clippy", "debug", [], {
    extraArgs: ["--all-features"],
  });

  assertEquals(argv.indexOf("--all-features") < argv.indexOf("--"), true);
});
