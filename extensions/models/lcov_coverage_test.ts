/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the lcov trace parser.
 *
 * The fixtures follow the records a real `bin/flow_standalone.info` from this
 * repository contains — absolute `SF:` paths, `DA:line,count` per instrumented
 * line, `end_of_record` between files — so a change to the shape shows up here
 * rather than as a silently empty report.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { parseTrace, toRanges, traceName } from "./lcov_coverage.ts";

Deno.test("counts hit and unhit lines per file", () => {
  const files = parseTrace([
    "TN:",
    "SF:/repo/src/spec.c",
    "FN:12,17,Spec_Create",
    "FNDA:10,Spec_Create",
    "DA:12,10",
    "DA:13,0",
    "DA:14,3",
    "LF:3",
    "LH:2",
    "end_of_record",
  ].join("\n"));

  const spec = files.get("/repo/src/spec.c");
  assertEquals(spec, { coveredLines: 2, totalLines: 3, uncovered: [13] });
});

Deno.test("keeps files apart", () => {
  const files = parseTrace([
    "SF:/repo/src/a.c",
    "DA:1,1",
    "end_of_record",
    "SF:/repo/src/b.c",
    "DA:1,0",
    "DA:2,0",
    "end_of_record",
  ].join("\n"));

  assertEquals(files.size, 2);
  assertEquals(files.get("/repo/src/a.c")?.uncovered, []);
  assertEquals(files.get("/repo/src/b.c")?.uncovered, [1, 2]);
});

Deno.test("accumulates a file the trace lists twice", () => {
  // One file can appear once per test binary that linked it, and the records
  // add up rather than replacing each other.
  const files = parseTrace([
    "SF:/repo/src/a.c",
    "DA:1,1",
    "end_of_record",
    "SF:/repo/src/a.c",
    "DA:2,0",
    "end_of_record",
  ].join("\n"));

  assertEquals(files.get("/repo/src/a.c"), {
    coveredLines: 1,
    totalLines: 2,
    uncovered: [2],
  });
});

Deno.test("merges the same line reported by two binaries", () => {
  // Coverage is the union across binaries: a line one of them entered is
  // covered, however many others missed it. Counting the records separately
  // would report this single line as two, half covered, and name it a gap it
  // is not — sending someone off to write a test for a line already exercised.
  const files = parseTrace([
    "SF:/repo/src/a.c",
    "DA:10,0",
    "DA:11,0",
    "end_of_record",
    "SF:/repo/src/a.c",
    "DA:10,5",
    "DA:11,0",
    "end_of_record",
  ].join("\n"));

  assertEquals(files.get("/repo/src/a.c"), {
    coveredLines: 1,
    totalLines: 2,
    uncovered: [11],
  });
});

Deno.test("reports uncovered lines in order across records", () => {
  // The ranges are built from this list, and a later record can carry a line
  // number lower than an earlier one.
  const files = parseTrace([
    "SF:/repo/src/a.c",
    "DA:9,0",
    "end_of_record",
    "SF:/repo/src/a.c",
    "DA:3,0",
    "DA:4,0",
    "end_of_record",
  ].join("\n"));

  assertEquals(files.get("/repo/src/a.c")?.uncovered, [3, 4, 9]);
});

Deno.test("reads a DA record that carries a checksum", () => {
  // Some lcov versions append one, and splitting the record in two would read
  // the checksum as the hit count.
  const files = parseTrace([
    "SF:/repo/src/a.c",
    "DA:1,0,f7a9c3",
    "end_of_record",
  ].join("\n"));

  assertEquals(files.get("/repo/src/a.c")?.uncovered, [1]);
});

Deno.test("ignores records outside a file, and branch records", () => {
  const files = parseTrace([
    "DA:99,0",
    "SF:/repo/src/a.c",
    "BRDA:1,0,0,-",
    "DA:1,1",
    "end_of_record",
    "DA:100,0",
  ].join("\n"));

  assertEquals(files.size, 1);
  assertEquals(files.get("/repo/src/a.c")?.totalLines, 1);
});

Deno.test("collapses consecutive lines into ranges", () => {
  assertEquals(toRanges([3, 1, 2, 7, 9, 10, 11]), [
    { start: 1, end: 3 },
    { start: 7, end: 7 },
    { start: 9, end: 11 },
  ]);
});

Deno.test("collapses nothing when there is nothing to collapse", () => {
  assertEquals(toRanges([]), []);
  assertEquals(toRanges([5, 5]), [{ start: 5, end: 5 }]);
});

Deno.test("names the trace build.sh writes per suite", () => {
  assertEquals(traceName("flow", "standalone"), "flow_standalone.info");
  // A cluster run reaches build.sh as REDIS_STANDALONE=0, which it records as
  // `coordinator` — so the trace is not named after the word the caller used.
  assertEquals(traceName("flow", "cluster"), "flow_coordinator.info");
  // The unit trace is not per deployment, so the deployment is not in its name.
  assertEquals(traceName("unit", "standalone"), "unit.info");
});
