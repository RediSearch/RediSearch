/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the formatting check parser.
 *
 * The fixtures reproduce rustfmt's `--check` output, which prints one `Diff in`
 * header per badly formatted hunk rather than one per file.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { parseCheckOutput, resolve } from "./make_fmt.ts";

const ROOT = "/repo";

Deno.test("reports nothing for an already formatted tree", () => {
  assertEquals(parseCheckOutput(["Checking code formatting..."], ROOT), []);
});

Deno.test("reports paths relative to the repository root", () => {
  const files = parseCheckOutput([
    "Checking code formatting...",
    "Diff in /repo/src/redisearch_rs/trie_rs/src/lib.rs:290:",
    "-fn   foo( a:u32 )->u32{a+1}",
    "+fn foo(a: u32) -> u32 {",
  ], ROOT);

  assertEquals(files, ["src/redisearch_rs/trie_rs/src/lib.rs"]);
});

Deno.test("deduplicates a file reported once per bad hunk", () => {
  // rustfmt emits a header per hunk, so a file with three badly formatted
  // functions appears three times and must collapse to one entry.
  const files = parseCheckOutput([
    "Diff in /repo/a.rs:10:",
    "Diff in /repo/a.rs:42:",
    "Diff in /repo/b.rs:7:",
    "Diff in /repo/a.rs:99:",
  ], ROOT);

  assertEquals(files, ["a.rs", "b.rs"]);
});

Deno.test("leaves a path outside the repository absolute", () => {
  const files = parseCheckOutput(["Diff in /elsewhere/c.rs:1:"], ROOT);

  assertEquals(files, ["/elsewhere/c.rs"]);
});

Deno.test("keeps colons that belong to the path itself", () => {
  // Only the trailing `:<line>:` is structure; anything before it is the path.
  const files = parseCheckOutput(["Diff in /repo/od:d/name.rs:12:"], ROOT);

  assertEquals(files, ["od:d/name.rs"]);
});

Deno.test("ignores lines that merely mention a diff", () => {
  const files = parseCheckOutput([
    "  Diff in /repo/a.rs:1:",
    "Some Diff in /repo/b.rs:1:",
    "Diff in /repo/c.rs:",
  ], ROOT);

  assertEquals(files, []);
});

Deno.test("resolves a repo-relative root without leaking dot segments", () => {
  assertEquals(resolve("/repo", "."), "/repo");
  assertEquals(resolve("/repo", "sub"), "/repo/sub");
  assertEquals(resolve("/repo/nested", ".."), "/repo");
  assertEquals(resolve("/repo", "/absolute"), "/absolute");
});
