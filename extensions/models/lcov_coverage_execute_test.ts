/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the lcov coverage model's `execute`.
 *
 * The parser tests cover what the model makes of a trace. These cover the
 * surface around it: which trace is read, the freshness bound that stops a
 * stale one being reported as this run's, and the refusals — a Rust path, a
 * missing trace, a file the trace never mentions.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./lcov_coverage.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
} from "./harness_test.ts";

const report = model.methods.report;

/** Write a trace under bin/, with the paths a real one carries. */
async function trace(
  dir: string,
  name: string,
  records: string[],
): Promise<string> {
  await Deno.mkdir(`${dir}/bin`, { recursive: true });
  const path = `${dir}/bin/${name}`;
  await Deno.writeTextFile(path, records.join("\n"));
  return path;
}

/** A trace covering src/spec.c, with line 13 never hit. */
function specRecords(dir: string): string[] {
  return [
    "TN:",
    `SF:${dir}/src/spec.c`,
    "DA:12,10",
    "DA:13,0",
    "DA:14,3",
    "end_of_record",
    `SF:${dir}/src/query.c`,
    "DA:1,1",
    "end_of_record",
  ];
}

/** Create the source files a trace refers to, so paths canonicalise. */
async function sources(dir: string, ...files: string[]): Promise<void> {
  await Deno.mkdir(`${dir}/src`, { recursive: true });
  for (const file of files) {
    await Deno.writeTextFile(`${dir}/${file}`, "/* probe */\n");
  }
}

Deno.test("reports uncovered lines as ranges", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c", "src/query.c");
    await trace(dir, "flow_standalone.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    await report.execute({
      files: ["src/spec.c"],
      suite: "flow",
      deployment: "standalone",
      requireAllFound: true,
    }, context);

    const summary = summaryOf(recorded);
    const targets = summary.targets as Array<Record<string, unknown>>;
    assertEquals(targets.length, 1);
    assertEquals(targets[0].found, true);
    assertEquals(targets[0].coveredLines, 2);
    assertEquals(targets[0].totalLines, 3);
    assertEquals(targets[0].uncoveredRanges, [{ start: 13, end: 13 }]);
    // The whole trace is counted, not just the requested files.
    assertEquals(summary.filesInTrace, 2);
    assertEquals(summary.requested, 1);
    assertEquals(summary.found, 1);
  });
});

Deno.test("reads the trace named by suite and deployment", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c");
    // Only the cluster trace exists, so naming standalone would find nothing.
    // build.sh writes it as flow_coordinator.info, not after the word the
    // caller passed — reading flow_cluster.info would find nothing either.
    await trace(dir, "flow_coordinator.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    await report.execute({
      files: ["src/spec.c"],
      suite: "flow",
      deployment: "cluster",
      requireAllFound: true,
    }, context);

    const summary = summaryOf(recorded);
    assertEquals(
      String(summary.infoFile).endsWith("flow_coordinator.info"),
      true,
    );
    assertEquals(summary.deployment, "cluster");
  });
});

Deno.test("the unit trace is not per deployment", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c");
    await trace(dir, "unit.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    await report.execute({
      files: ["src/spec.c"],
      suite: "unit",
      deployment: "standalone",
      requireAllFound: true,
    }, context);

    const summary = summaryOf(recorded);
    assertEquals(String(summary.infoFile).endsWith("unit.info"), true);
    assertEquals(summary.deployment, null);
  });
});

Deno.test("an explicit trace path wins", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c");
    await trace(dir, "elsewhere.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    await report.execute({
      files: ["src/spec.c"],
      suite: "flow",
      deployment: "standalone",
      infoFile: "bin/elsewhere.info",
      requireAllFound: true,
    }, context);

    assertEquals(
      String(summaryOf(recorded).infoFile).endsWith("elsewhere.info"),
      true,
    );
  });
});

Deno.test("refuses a trace older than the bound", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c");
    const path = await trace(dir, "flow_standalone.info", specRecords(dir));
    await Deno.utime(path, new Date("2020-01-01"), new Date("2020-01-01"));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    const error = await captureError(() =>
      report.execute({
        files: ["src/spec.c"],
        suite: "flow",
        deployment: "standalone",
        // The suite ran after the trace was written, so the trace is not its.
        notOlderThan: "2026-07-30T00:00:00.000Z",
        requireAllFound: true,
      }, context)
    );

    assertEquals(error?.message.includes("belongs to an earlier run"), true);
    // Nothing is recorded: a stale report is worse than none.
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("accepts a trace at or after the bound", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c");
    const path = await trace(dir, "flow_standalone.info", specRecords(dir));
    await Deno.utime(path, new Date("2026-07-30"), new Date("2026-07-30"));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    await report.execute({
      files: ["src/spec.c"],
      suite: "flow",
      deployment: "standalone",
      notOlderThan: "2026-07-29T00:00:00.000Z",
      requireAllFound: true,
    }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.stale, false);
    assertEquals(summary.notOlderThan, "2026-07-29T00:00:00.000Z");
  });
});

Deno.test("refuses a Rust path", async () => {
  await withTempRepo(async (dir) => {
    await trace(dir, "flow_standalone.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    const error = await captureError(() =>
      report.execute({
        files: ["src/redisearch_rs/trie_rs/src/lib.rs"],
        suite: "flow",
        deployment: "standalone",
        requireAllFound: true,
      }, context)
    );

    // gcov cannot see Rust, so reporting it as having no data would mislead.
    assertEquals(error?.message.includes("rust-coverage model"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("says what to run when there is no trace", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c");
    const { context } = makeContext(dir, { repoRoot: ".", binDir: "bin" });

    const error = await captureError(() =>
      report.execute({
        files: ["src/spec.c"],
        suite: "flow",
        deployment: "standalone",
        requireAllFound: true,
      }, context)
    );

    assertEquals(error?.message.includes("No coverage trace at"), true);
    assertEquals(error?.message.includes("coverage"), true);
  });
});

Deno.test("fails on a file the trace never mentions", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c", "src/absent.c");
    await trace(dir, "flow_standalone.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    const error = await captureError(() =>
      report.execute({
        files: ["src/spec.c", "src/absent.c"],
        suite: "flow",
        deployment: "standalone",
        requireAllFound: true,
      }, context)
    );

    // Not compiled in, or not instrumented — either way it is not "untested".
    assertEquals(error?.message.includes("src/absent.c"), true);
    // The summary is still recorded, so the rest of the report is readable.
    assertEquals(summaryOf(recorded).found, 1);
  });
});

Deno.test("reports the rest when told not to require every file", async () => {
  await withTempRepo(async (dir) => {
    await sources(dir, "src/spec.c", "src/absent.c");
    await trace(dir, "flow_standalone.info", specRecords(dir));
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      binDir: "bin",
    });

    const error = await captureError(() =>
      report.execute({
        files: ["src/absent.c", "src/spec.c"],
        suite: "flow",
        deployment: "standalone",
        requireAllFound: false,
      }, context)
    );

    assertEquals(error, null);
    const summary = summaryOf(recorded);
    const targets = summary.targets as Array<Record<string, unknown>>;
    // Order follows the request, so a caller can zip the two lists.
    assertEquals(targets.map((t) => t.file), ["src/absent.c", "src/spec.c"]);
    assertEquals(targets[0].found, false);
    assertEquals(summary.found, 1);
    // Totals count the files that were found, not the ones that were not.
    const overall = summary.overall as Record<string, number>;
    assertEquals(overall.totalLines, 3);
  });
});
