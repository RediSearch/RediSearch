/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the build model's `execute`.
 *
 * These cover what the parser tests cannot: that the arguments reach build.sh,
 * and that the module on disk is judged correctly afterwards. The staleness
 * rule in particular was wrong on its first implementation — it flagged a
 * perfectly good incremental build — and nothing but a full workflow run
 * caught it.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./redisearch_build.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  variantDir,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

const build = model.methods.build;

/** Where the oss module lands for a given flavor. */
function modulePath(dir: string, flavor: string): string {
  return `${dir}/bin/${variantDir(flavor)}/search-community/redisearch.so`;
}

/** Place a module on disk with an mtime relative to now. */
async function placeModule(
  dir: string,
  flavor: string,
  agedMs: number,
): Promise<string> {
  const path = modulePath(dir, flavor);
  await Deno.mkdir(path.slice(0, path.lastIndexOf("/")), { recursive: true });
  await Deno.writeTextFile(path, "module");
  const when = new Date(Date.now() - agedMs);
  await Deno.utime(path, when, when);
  return path;
}

/** A repo whose build.sh succeeds, recording the argv it was called with. */
async function succeedingRepo(dir: string): Promise<void> {
  await writeScript(dir, "build.sh", 'echo "ARGS: $@"');
}

/** A repo whose build.sh fails the way a compile error does. */
async function failingRepo(dir: string): Promise<void> {
  await writeScript(
    dir,
    "build.sh",
    [
      "echo 'src/query.c:2018:37: error: expected expression before ; token'",
      "exit 2",
    ].join("\n"),
  );
}

Deno.test("passes the requested variant through to build.sh", async () => {
  await withTempRepo(async (dir) => {
    await succeedingRepo(dir);
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    await build.execute(
      { debug: true, buildTests: true, quiet: true },
      context,
    );

    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("ARGS: COORD=oss DEBUG=1 TESTS=1"), true);
    assertEquals(summaryOf(recorded).flavor, "debug");
  });
});

Deno.test("an incremental build that relinks nothing is not stale", async () => {
  await withTempRepo(async (dir) => {
    await succeedingRepo(dir);
    // The module predates the run because make found nothing to do. It is
    // still this build's output: make already decided it was up to date.
    await placeModule(dir, "debug", 60_000);
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    await build.execute({ debug: true, quiet: true }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "succeeded");
    assertEquals(summary.moduleStale, false);
    assertEquals(summary.modulePath, modulePath(dir, "debug"));
  });
});

Deno.test("a failed build reports the leftover module as stale", async () => {
  await withTempRepo(async (dir) => {
    await failingRepo(dir);
    await placeModule(dir, "debug", 60_000);
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    const error = await captureError(() =>
      build.execute({ debug: true, quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "failed");
    // The artifact exists but belongs to an earlier build, which is exactly
    // the case that would otherwise read as success.
    assertEquals(summary.moduleStale, true);
    assertEquals(summary.errorCount, 1);
    assertEquals(error?.message.includes("exited with code 2"), true);
  });
});

Deno.test("a failed first build reports no module at all", async () => {
  await withTempRepo(async (dir) => {
    await failingRepo(dir);
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    await captureError(() =>
      build.execute({ debug: true, quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.modulePath, null);
    assertEquals(summary.moduleSizeBytes, null);
    assertEquals(summary.moduleStale, false);
  });
});

Deno.test("a module written during the run is not stale", async () => {
  await withTempRepo(async (dir) => {
    const path = modulePath(dir, "release");
    await writeScript(
      dir,
      "build.sh",
      [
        `mkdir -p "$(dirname "${path}")"`,
        `printf 'fresh module' > "${path}"`,
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    await build.execute({ quiet: true }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.moduleStale, false);
    assertEquals(summary.moduleSizeBytes, 12);
  });
});

Deno.test("ignoreBuildFailure records the failure without throwing", async () => {
  await withTempRepo(async (dir) => {
    await failingRepo(dir);
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    const error = await captureError(() =>
      build.execute(
        { debug: true, quiet: true, ignoreBuildFailure: true },
        context,
      )
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).status, "failed");
  });
});

Deno.test("reports the enterprise artifact for a rlec build", async () => {
  await withTempRepo(async (dir) => {
    await succeedingRepo(dir);
    const path = `${dir}/bin/${
      variantDir("release")
    }/search-enterprise/module-enterprise.so`;
    await Deno.mkdir(path.slice(0, path.lastIndexOf("/")), { recursive: true });
    await Deno.writeTextFile(path, "module");
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "rlec",
    });

    await build.execute({ quiet: true }, context);

    assertEquals(summaryOf(recorded).modulePath, path);
  });
});

Deno.test("aborts a build that outlives its timeout", async () => {
  await withTempRepo(async (dir) => {
    await writeScript(dir, "build.sh", "exec sleep 30");
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    const error = await captureError(() =>
      build.execute({ quiet: true, timeout: 200 }, context)
    );

    assertEquals(error?.message.includes("timed out after 200ms"), true);
    // A summary is still written, so a timeout is inspectable rather than
    // leaving only an exception.
    assertEquals(summaryOf(recorded).timedOut, true);
  });
});

Deno.test("refuses to run when there is no build script", async () => {
  await withTempRepo(async (dir) => {
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    const error = await captureError(() =>
      build.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("No build script found"), true);
    // Nothing ran, so nothing is recorded.
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("rejects an invalid variant before running anything", async () => {
  await withTempRepo(async (dir) => {
    await succeedingRepo(dir);
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      buildScript: "./build.sh",
      coord: "oss",
    });

    const error = await captureError(() =>
      build.execute({ debug: true, coverage: true, quiet: true }, context)
    );

    assertEquals(error?.message.includes("cannot be combined"), true);
    assertEquals(recorded.resources.length, 0);
    assertEquals(recorded.files.length, 0);
  });
});
