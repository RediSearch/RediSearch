/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Test harness for driving a model's `execute` end to end.
 *
 * The parser tests cover what a model makes of output it is handed. These
 * helpers cover the rest: that the arguments and environment actually reach the
 * process, that the filesystem is inspected correctly, and that the summary
 * written afterwards says what it should. That layer had no coverage, and it is
 * where the bugs found by hand in this repo actually were — an argument that
 * looked right but never reached build.sh, and a module reported as this run's
 * output when it belonged to the previous one.
 *
 * A model is driven against a fake script in a temporary directory rather than
 * the real build, so a case takes milliseconds and can produce failures that
 * are impractical to induce for real.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";

export interface WrittenResource {
  spec: string;
  name: string;
  data: Record<string, unknown>;
}

export interface WrittenFile {
  spec: string;
  name: string;
  lines: string[];
}

export interface Recorded {
  resources: WrittenResource[];
  files: WrittenFile[];
}

/** The context shape every model's `execute` expects from swamp. */
// deno-lint-ignore no-explicit-any
export type Context = any;

/**
 * Build a context that records what a model writes instead of persisting it.
 *
 * `quiet` is forced on by the callers, so nothing here mirrors to stderr.
 */
export function makeContext(
  repoDir: string,
  globalArgs: Record<string, unknown>,
  signal: AbortSignal = new AbortController().signal,
): { context: Context; recorded: Recorded } {
  const recorded: Recorded = { resources: [], files: [] };

  const context = {
    signal,
    repoDir,
    globalArgs,
    logger: { info: () => {} },
    writeResource: (
      spec: string,
      name: string,
      data: Record<string, unknown>,
    ) => {
      recorded.resources.push({ spec, name, data });
      return Promise.resolve({ name });
    },
    createFileWriter: (spec: string, name: string) => {
      const file: WrittenFile = { spec, name, lines: [] };
      return {
        writeLine: (line: string) => {
          file.lines.push(line);
          return Promise.resolve();
        },
        finalize: () => {
          recorded.files.push(file);
          return Promise.resolve({ name });
        },
      };
    },
  };

  return { context, recorded };
}

/** Write an executable shell script and return its path. */
export async function writeScript(
  dir: string,
  name: string,
  body: string,
): Promise<string> {
  const path = `${dir}/${name}`;
  await Deno.writeTextFile(path, `#!/usr/bin/env bash\n${body}\n`);
  // Only the test process ever runs this, so it needs no group or world bit.
  // These land under the shared temp directory, where an execute bit for
  // everyone buys nothing and is worth not handing out.
  await Deno.chmod(path, 0o700);
  return path;
}

/**
 * Write a stand-in for build.sh that reports its arguments and the environment
 * variables build.sh reads from the environment rather than argv.
 *
 * Shared because that distinction is the whole point of the suite models'
 * execute tests: an argument can look correct and still never reach the script.
 */
export function writeReportingScript(
  dir: string,
  extra = "",
): Promise<string> {
  return writeScript(
    dir,
    "build.sh",
    [
      'echo "ARGS: $@"',
      'echo "ENV SKIP_BUILD=${SKIP_BUILD:-unset}"',
      'echo "ENV PARALLEL=${PARALLEL:-unset}"',
      extra,
    ].join("\n"),
  );
}

/**
 * The build variant directory name for this machine, derived the way build.sh
 * derives it. Duplicated rather than imported so that a bug in the model's own
 * derivation shows up as a failure here.
 */
export function variantDir(flavor: string): string {
  const os = Deno.build.os === "darwin" ? "macos" : Deno.build.os;
  const arch = Deno.build.arch === "x86_64" ? "x64" : Deno.build.arch;
  return `${os}-${arch}-${flavor}`;
}

/** Run `body` against a fresh temporary directory, removing it afterwards. */
export async function withTempRepo(
  body: (dir: string) => Promise<void>,
): Promise<void> {
  const dir = await Deno.makeTempDir({ prefix: "swamp-model-test-" });
  try {
    await body(await Deno.realPath(dir));
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
}

/** Capture the error a rejected `execute` throws, or null if it resolved. */
export async function captureError(
  run: () => Promise<unknown>,
): Promise<Error | null> {
  try {
    await run();
    return null;
  } catch (error) {
    return error as Error;
  }
}

/** The single summary a run is expected to have written. */
export function summaryOf(recorded: Recorded): Record<string, unknown> {
  assertEquals(recorded.resources.length, 1);
  return recorded.resources[0].data;
}

Deno.test("the harness records what a model writes", async () => {
  await withTempRepo(async (dir) => {
    const { context, recorded } = makeContext(dir, {});

    const writer = context.createFileWriter("log", "log");
    await writer.writeLine("hello");
    await writer.finalize();
    await context.writeResource("summary", "summary", { ok: true });

    assertEquals(recorded.files, [{
      spec: "log",
      name: "log",
      lines: ["hello"],
    }]);
    assertEquals(summaryOf(recorded), { ok: true });
  });
});
