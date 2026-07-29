/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the fmt model's `execute`.
 *
 * The three-way status and the decision to invoke make a second time are only
 * expressible here: neither is visible to the output parser. So is the
 * distinction between "make exited non-zero because files need formatting" and
 * "make itself broke", which rests on whether any diff headers were printed.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./make_fmt.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

const format = model.methods.format;
const check = model.methods.check;

/**
 * Stand in for make, logging each invocation's arguments so a test can tell
 * how many times it ran and in which mode.
 *
 * `body` runs with `$CHECK` resolved the way make itself resolves it: the
 * environment supplies the default and a `CHECK=<value>` argument overrides it.
 * Modelling both halves is the point — a fake that only read the argument would
 * pass whether or not the model defends against an inherited one.
 */
async function fakeMake(dir: string, body: string): Promise<string> {
  await Deno.writeTextFile(`${dir}/Makefile`, "fmt:\n\t@true\n");
  return await writeScript(
    dir,
    "fake-make",
    [
      'echo "$@" >> "$(dirname "$0")/invocations"',
      'CHECK="${CHECK:-0}"; for a in "$@"; do case "$a" in CHECK=*) CHECK="${a#CHECK=}";; esac; done',
      body,
    ].join("\n"),
  );
}

/** How many times the fake make was invoked. */
async function invocations(dir: string): Promise<string[]> {
  try {
    const text = await Deno.readTextFile(`${dir}/invocations`);
    return text.trimEnd().split("\n");
  } catch {
    return [];
  }
}

const DIFF = "Diff in REPO/src/redisearch_rs/a/src/lib.rs:290:";

Deno.test("check reports a formatted tree as clean", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(dir, "exit 0");
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });

    await check.execute({ quiet: true }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "clean");
    assertEquals(summary.files, []);
    assertEquals((await invocations(dir)).length, 1);
  });
});

Deno.test("check fails and names the files needing formatting", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(
      dir,
      [`echo "${DIFF.replace("REPO", dir)}"`, "exit 1"].join("\n"),
    );
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });

    const error = await captureError(() =>
      check.execute({ quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "unformatted");
    assertEquals(summary.files, ["src/redisearch_rs/a/src/lib.rs"]);
    assertEquals(
      error?.message,
      "1 file(s) need formatting: src/redisearch_rs/a/src/lib.rs",
    );
    // Checking must never rewrite, so make runs once and only in check mode.
    assertEquals((await invocations(dir)).length, 1);
  });
});

Deno.test("ignoreUnformatted returns the list instead of failing", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(
      dir,
      [`echo "${DIFF.replace("REPO", dir)}"`, "exit 1"].join("\n"),
    );
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });

    const error = await captureError(() =>
      check.execute({ quiet: true, ignoreUnformatted: true }, context)
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).fileCount, 1);
  });
});

Deno.test("format rewrites and reports what changed", async () => {
  await withTempRepo(async (dir) => {
    // The check pass reports work to do; the format pass then succeeds.
    const make = await fakeMake(
      dir,
      [
        'if [ "$CHECK" = 1 ]; then',
        `  echo "${DIFF.replace("REPO", dir)}"`,
        "  exit 1",
        "fi",
        "exit 0",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });

    await format.execute({ quiet: true }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "formatted");
    assertEquals(summary.files, ["src/redisearch_rs/a/src/lib.rs"]);
    // Once to learn the file list, once to apply it.
    const ran = await invocations(dir);
    assertEquals(ran.length, 2);
    assertEquals(ran[0], "fmt CHECK=1");
    assertEquals(ran[1], "fmt CHECK=");
  });
});

Deno.test("format rewrites files even with CHECK exported", async () => {
  await withTempRepo(async (dir) => {
    // The Makefile branches on `ifeq ($(CHECK),1)`, which reads the environment
    // as readily as the command line. Inherited, it would turn the second pass
    // back into a check: the run would report a failed format while leaving
    // every file exactly as it was.
    const make = await fakeMake(
      dir,
      [
        'if [ "$CHECK" = 1 ]; then',
        `  echo "${DIFF.replace("REPO", dir)}"`,
        "  exit 1",
        "fi",
        "exit 0",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });
    Deno.env.set("CHECK", "1");

    try {
      await format.execute({ quiet: true }, context);
    } finally {
      Deno.env.delete("CHECK");
    }

    assertEquals(summaryOf(recorded).status, "formatted");
  });
});

Deno.test("format does not invoke make again when nothing needs it", async () => {
  await withTempRepo(async (dir) => {
    const make = await fakeMake(dir, "exit 0");
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });

    await format.execute({ quiet: true }, context);

    assertEquals(summaryOf(recorded).status, "clean");
    assertEquals((await invocations(dir)).length, 1);
  });
});

Deno.test("a broken make is a failure, not an unformatted tree", async () => {
  await withTempRepo(async (dir) => {
    // Non-zero with no diff headers means make itself broke — a missing
    // target, a toolchain problem — rather than work to do.
    const make = await fakeMake(
      dir,
      ["echo 'make: *** No rule to make target' >&2", "exit 2"].join("\n"),
    );
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: make,
    });

    const error = await captureError(() =>
      format.execute({ quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "failed");
    assertEquals(summary.files, []);
    assertEquals(error?.message.includes("exited with code 2"), true);
    // The format pass is skipped once the check has already failed.
    assertEquals((await invocations(dir)).length, 1);
  });
});

Deno.test("refuses to run when there is no Makefile", async () => {
  await withTempRepo(async (dir) => {
    const { context, recorded } = makeContext(dir, {
      repoRoot: ".",
      makeBin: "make",
    });

    const error = await captureError(() =>
      check.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("No Makefile found"), true);
    assertEquals(recorded.resources.length, 0);
  });
});
