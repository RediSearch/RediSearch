/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Formats the codebase with `make fmt` and records which files were affected.
 *
 * Two methods, because they answer different questions. `check` asks whether
 * the tree is formatted and fails if it is not, which is what a verification
 * gate wants. `format` rewrites the files, which is what you want after an
 * edit.
 *
 * Both report the affected files rather than just an exit code. `make fmt`
 * prints nothing when it rewrites, so `format` runs the check first to learn
 * the file list and then applies it — a caller that just reformatted still
 * gets to see what moved.
 *
 * Scope note: `make fmt` covers Rust only, via `cargo fmt --all`. This model
 * reports what `make fmt` actually does, not what the docs imply it does.
 *
 * C and C++ are deliberately left out. They are governed by .clang-format, but
 * much of the existing C predates it, so formatting the tree wholesale would
 * bury a real change under thousands of unrelated lines. C formatting has to be
 * scoped to new or changed code — `clang-format -i` on the lines you touched —
 * which is a different operation from "format everything" and does not belong
 * behind the same method.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Default timeout: 5 minutes. Formatting the workspace takes seconds. */
const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;

const GlobalArgsSchema = z.object({
  repoRoot: z
    .string()
    .min(1)
    .default(".")
    .describe(
      "Directory holding the Makefile. Relative paths resolve against the repository root.",
    ),
  makeBin: z
    .string()
    .min(1)
    .default("make")
    .describe("The make executable to invoke."),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const CommonArgsSchema = z.object({
  timeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      `Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}).` +
        " Note that the timeout kills the script but not the commands it spawned, so a hung compiler or server can keep the run alive past it.",
    ),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default the output is mirrored to stderr as " +
        "it arrives.",
    ),
});

const FormatArgsSchema = CommonArgsSchema.extend({});

const CheckArgsSchema = CommonArgsSchema.extend({
  ignoreUnformatted: z
    .boolean()
    .optional()
    .describe(
      "Record which files need formatting without failing the method. Lets a " +
        "caller inspect the list and decide, rather than handling an error.",
    ),
});

type FormatArgs = z.infer<typeof FormatArgsSchema>;
type CheckArgs = z.infer<typeof CheckArgsSchema>;

const SummarySchema = z.object({
  command: z.string().describe("The full command line that was executed"),
  repoRoot: z.string().describe("Resolved directory make ran in"),
  mode: z
    .enum(["format", "check"])
    .describe("Whether files were rewritten or only inspected"),
  exitCode: z.number().int().describe("Exit code of make"),
  status: z
    .enum(["clean", "formatted", "unformatted", "failed"])
    .describe(
      "clean: nothing to do. formatted: files were rewritten. " +
        "unformatted: check found files needing formatting. failed: make itself errored.",
    ),
  files: z
    .array(z.string())
    .describe(
      "Repository-relative paths that were reformatted, or that need to be",
    ),
  fileCount: z.number().int().nonnegative().describe(
    "Number of affected files",
  ),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/**
 * Matches a rustfmt check diff header, e.g.
 * `Diff in /repo/src/redisearch_rs/foo/src/lib.rs:290:`.
 * The path group is greedy so it keeps any colons in the path itself, leaving
 * the final `:<line>:` to the line group.
 */
const DIFF_RE = /^Diff in (.+):(\d+):\s*$/;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and trailing carriage returns from a line. */
export function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
}

/**
 * Extract the files rustfmt reports as needing formatting, in first-seen order.
 * rustfmt prints one header per hunk, so a file with several badly formatted
 * functions appears repeatedly and has to be deduplicated.
 */
export function parseCheckOutput(lines: string[], repoRoot: string): string[] {
  const files: string[] = [];
  const seen = new Set<string>();
  const prefix = repoRoot.endsWith("/") ? repoRoot : `${repoRoot}/`;

  for (const line of lines) {
    const match = line.match(DIFF_RE);
    if (!match) continue;
    const path = match[1].startsWith(prefix)
      ? match[1].slice(prefix.length)
      : match[1];
    if (seen.has(path)) continue;
    seen.add(path);
    files.push(path);
  }

  return files;
}

/**
 * Resolve a path against the repository, collapsing `.` and `..` segments so
 * that the default `repoRoot` of "." does not leak into every reported path.
 */
export function resolve(base: string, path: string): string {
  const absolute = path.startsWith("/") ? path : `${base}/${path}`;
  const segments: string[] = [];
  for (const segment of absolute.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join("/")}`;
}

interface RunContext {
  signal: AbortSignal;
  repoDir: string;
  globalArgs: GlobalArgs;
  logger: { info: (msg: string, props?: unknown) => void };
  writeResource: (
    specName: string,
    name: string,
    data: Record<string, unknown>,
  ) => Promise<{ name: string }>;
  createFileWriter: (
    specName: string,
    name: string,
  ) => {
    writeLine: (line: string) => Promise<void>;
    finalize: () => Promise<{ name: string }>;
  };
}

interface Invocation {
  lines: string[];
  code: number;
  signal: string | null;
  success: boolean;
}

/**
 * Run make with the given arguments, streaming every line into `sink`.
 *
 * `sink` rather than a return value because both methods persist the output to
 * the same log file, and `format` invokes make twice.
 */
async function runMake(
  makeBin: string,
  args: string[],
  cwd: string,
  abort: AbortSignal,
  sink: (line: string) => Promise<void>,
): Promise<Invocation> {
  const child = new Deno.Command(makeBin, {
    args,
    cwd,
    stdout: "piped",
    stderr: "piped",
    signal: abort,
  }).spawn();

  const lines: string[] = [];

  const record = async (raw: string): Promise<void> => {
    const line = clean(raw);
    lines.push(line);
    await sink(line);
  };

  const pump = async (stream: ReadableStream<Uint8Array>): Promise<void> => {
    const decoder = new TextDecoder();
    let buffer = "";
    for await (const chunk of stream) {
      buffer += decoder.decode(chunk, { stream: true });
      const parts = buffer.split("\n");
      buffer = parts.pop() ?? "";
      for (const part of parts) await record(part);
    }
    if (buffer.length > 0) await record(buffer);
  };

  await Promise.all([pump(child.stdout), pump(child.stderr)]);
  const status = await child.status;

  return {
    lines,
    code: status.code,
    signal: status.signal ?? null,
    success: status.success,
  };
}

/** Shared body of both methods; `mode` decides whether files are rewritten. */
async function execute(
  mode: "format" | "check",
  args: FormatArgs & CheckArgs,
  context: RunContext,
): Promise<{ dataHandles: Array<{ name: string }> }> {
  const { repoRoot, makeBin } = context.globalArgs;
  const cwd = resolve(context.repoDir, repoRoot);

  try {
    await Deno.stat(`${cwd}/Makefile`);
  } catch {
    throw new Error(`No Makefile found at ${cwd}/Makefile`);
  }

  const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;
  const timeoutSignal = AbortSignal.timeout(timeoutMs);
  const abort = AbortSignal.any([context.signal, timeoutSignal]);

  const checkArgs = ["fmt", "CHECK=1"];
  // `CHECK=` rather than nothing: the Makefile branches on `ifeq ($(CHECK),1)`,
  // which reads the environment as readily as the command line, so an exported
  // CHECK=1 would turn this invocation back into a check — reporting a failed
  // format run while leaving every file exactly as it was. A command-line
  // assignment wins over the environment, and empty is what "not set" means
  // here.
  const formatArgs = ["fmt", "CHECK="];
  const command = [makeBin, ...(mode === "check" ? checkArgs : formatArgs)]
    .join(" ");

  context.logger.info("Running {command} in {cwd}", { command, cwd });

  const logWriter = context.createFileWriter("log", "log");
  const encoder = new TextEncoder();
  const sink = async (line: string): Promise<void> => {
    await logWriter.writeLine(line);
    if (!args.quiet) await Deno.stderr.write(encoder.encode(`${line}\n`));
  };

  const startedAt = Date.now();

  // Always check first: it is the only thing that names the affected files.
  // `make fmt` rewrites silently, so without this a format run could only
  // report that something happened, not what.
  const checked = await runMake(makeBin, checkArgs, cwd, abort, sink);
  const files = parseCheckOutput(checked.lines, cwd);

  // A check exits non-zero purely because files need formatting, which is not
  // a failure of make itself. Anything else — a missing target, a broken
  // toolchain — leaves no diff headers behind, and that is what distinguishes
  // the two.
  const checkFailed = !checked.success && files.length === 0;

  let applied: Invocation | null = null;
  if (mode === "format" && !checkFailed && files.length > 0) {
    applied = await runMake(makeBin, formatArgs, cwd, abort, sink);
  }

  const durationMs = Date.now() - startedAt;
  const timedOut = timeoutSignal.aborted;
  const failed = checkFailed || (applied !== null && !applied.success);
  const exitCode = applied !== null ? applied.code : checked.code;

  const logHandle = await logWriter.finalize();

  if (
    !timedOut &&
    (context.signal.aborted || checked.signal === "SIGINT" ||
      applied?.signal === "SIGINT")
  ) {
    throw new Error(
      `\`${command}\` was cancelled after ${durationMs}ms. No summary recorded.`,
    );
  }

  const status = failed
    ? "failed"
    : files.length === 0
    ? "clean"
    : mode === "format"
    ? "formatted"
    : "unformatted";

  const summaryHandle = await context.writeResource("summary", "summary", {
    command,
    repoRoot: cwd,
    mode,
    exitCode,
    status,
    files,
    fileCount: files.length,
    timedOut,
    durationMs,
    executedAt: new Date().toISOString(),
  });

  const handles = [summaryHandle, logHandle];

  if (timedOut) {
    throw new Error(
      `\`${command}\` timed out after ${timeoutMs}ms. See the log data for details.`,
    );
  }

  if (failed) {
    throw new Error(`\`${command}\` exited with code ${exitCode}.`);
  }

  if (mode === "check" && files.length > 0 && !args.ignoreUnformatted) {
    throw new Error(
      `${files.length} file(s) need formatting: ${files.join(", ")}`,
    );
  }

  return { dataHandles: handles };
}

/** Model definition wrapping `make fmt`. */
export const model = {
  type: "@gdesmott/make-fmt",
  version: "2026.08.06.1",
  description:
    "Format the codebase with make fmt, or check formatting, reporting the affected files",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed make fmt run summary",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from make fmt",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    format: {
      description: "Reformat the code in place and report which files changed",
      arguments: FormatArgsSchema,
      execute: (args: FormatArgs, context: RunContext) =>
        execute("format", args as FormatArgs & CheckArgs, context),
    },
    check: {
      description:
        "Report which files need formatting, failing if any do, without modifying them",
      arguments: CheckArgsSchema,
      execute: (args: CheckArgs, context: RunContext) =>
        execute("check", args as FormatArgs & CheckArgs, context),
    },
  },
};
