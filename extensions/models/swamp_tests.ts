/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Runs the swamp extension suite — the tests for these models themselves — and
 * records a parsed summary alongside the full log.
 *
 * The models in this directory gate every other suite, and CI's lint job runs
 * them for any pull request that touches them. Without this, a change to a
 * model could pass the pre-PR workflow and then fail that job, which is the one
 * failure mode a pre-PR gate exists to rule out.
 *
 * It wraps `make swamp-extension-tests` rather than `make swamp-tests`: the
 * latter also validates the checked-in model and workflow definitions by
 * invoking `swamp`, and this model runs *inside* a swamp workflow. That half is
 * a repository lint rather than a suite, and CI runs it.
 *
 * The target is two checks in sequence — `deno fmt --check` then `deno test` —
 * and they fail in unrelated ways, so the summary says which one stopped the
 * run rather than leaving the caller to infer it from the log.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Default timeout: 10 minutes. The suite itself takes seconds. */
const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;

/** Upper bound on failures stored in the summary. The log holds the rest. */
const MAX_FAILURES = 100;

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
  target: z
    .string()
    .min(1)
    .default("swamp-extension-tests")
    .describe(
      "Make target to run. Defaults to the deno half of `swamp-tests`; the " +
        "other half shells out to swamp, which cannot run from inside a swamp " +
        "workflow.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const RunArgsSchema = z.object({
  timeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(`Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}).`),
  ignoreFailure: z
    .boolean()
    .optional()
    .describe(
      "Record the result without failing the method when the suite fails.",
    ),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default the output is mirrored to stderr as " +
        "it arrives.",
    ),
});

type RunArgs = z.infer<typeof RunArgsSchema>;

const FailureSchema = z.object({
  test: z.string().describe("Name of the failing test"),
  where: z
    .string()
    .nullable()
    .describe("Source location deno reported for it, as `file:line:column`"),
});

const SummarySchema = z.object({
  command: z.string().describe("The full command line that was executed"),
  repoRoot: z.string().describe("Resolved directory make ran in"),
  exitCode: z.number().int().describe("Exit code of make"),
  status: z.enum(["passed", "failed"]).describe("Overall outcome of the run"),
  stage: z
    .enum(["format", "tests"])
    .nullable()
    .describe(
      "Which half of the target failed: `format` when `deno fmt --check` " +
        "found files to reformat, `tests` when a test failed. Null when the " +
        "run passed, or ended somewhere neither check reported",
    ),
  testsRun: z
    .number()
    .int()
    .nullable()
    .describe("Tests executed, or null when the summary line was not seen"),
  passed: z.number().int().nullable().describe("Tests that passed"),
  failed: z.number().int().nullable().describe("Tests that failed"),
  summaryParsed: z
    .boolean()
    .describe(
      "True when deno printed its closing summary and the counts are " +
        "trustworthy. False when the run ended before the tests ran, which a " +
        "formatting failure always does",
    ),
  failures: z
    .array(FailureSchema)
    .describe(`Failing tests with their locations, capped at ${MAX_FAILURES}`),
  unformatted: z
    .array(z.string())
    .describe(
      "Repository-relative paths `deno fmt --check` would rewrite, when that " +
        "is what failed",
    ),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/**
 * deno's closing line, e.g. `ok | 278 passed | 0 failed (2s)`.
 *
 * Lazy between the two counts rather than "anything but a pipe": deno adds
 * segments of its own to that line — `1 ignored`, `2 filtered out`, a step
 * count in parentheses — and their order is deno's to change.
 */
const SUMMARY_RE =
  /^(ok|FAILED)\s*\|\s*(\d+)\s+passed\b.*?\|\s*(\d+)\s+failed\b/;

/**
 * An entry in deno's `FAILURES` block, e.g.
 * `some test name => ./models/a_test.ts:80:6`.
 *
 * The name is greedy up to the last ` => `, because a test name is free text
 * and may contain the arrow itself.
 */
const FAILURE_RE = /^(.*) => (\S+)$/;

/** The header deno prints above the failing-test list. */
const FAILURES_HEADER = "FAILURES";

/** A file header in `deno fmt --check` output, e.g. `from /repo/a.ts:`. */
const UNFORMATTED_RE = /^from (.+):$/;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and carriage returns from a line. */
export function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
}

export interface TestCounts {
  testsRun: number;
  passed: number;
  failed: number;
}

/**
 * Read deno's closing summary.
 *
 * The last one wins: the target runs deno once, but a future target running it
 * per directory would print one line each, and the totals a caller wants are
 * then the ones that came last rather than first.
 *
 * Null when no summary was printed at all, which is what a formatting failure
 * produces — the target stops before `deno test` ever runs.
 */
export function parseTestSummary(lines: string[]): TestCounts | null {
  let counts: TestCounts | null = null;
  for (const line of lines) {
    const match = clean(line).trim().match(SUMMARY_RE);
    if (!match) continue;
    const passed = Number(match[2]);
    const failed = Number(match[3]);
    counts = { testsRun: passed + failed, passed, failed };
  }
  return counts;
}

/**
 * Read the failing tests out of deno's `FAILURES` block.
 *
 * That block, rather than the `... FAILED` line each test prints as it goes:
 * only the block carries the source location, and it lists every failure in one
 * place rather than interleaved with the output of the tests that passed.
 */
export function parseFailures(
  lines: string[],
): Array<{ test: string; where: string | null }> {
  const failures: Array<{ test: string; where: string | null }> = [];
  let inBlock = false;

  for (const raw of lines) {
    const line = clean(raw).trim();
    if (
      line.includes(FAILURES_HEADER) && line.replace(/\s/g, "").length <= 12
    ) {
      inBlock = true;
      continue;
    }
    if (!inBlock) continue;
    // The block ends at the closing summary, which is the next thing deno
    // prints after it.
    if (SUMMARY_RE.test(line)) break;
    if (line.length === 0) continue;
    const match = line.match(FAILURE_RE);
    if (match) failures.push({ test: match[1].trim(), where: match[2] });
  }
  return failures;
}

/**
 * Read the paths `deno fmt --check` would rewrite.
 *
 * Deduplicated and made repository-relative, so the list reads as a set of
 * files to fix rather than as the diff deno printed.
 */
export function parseUnformatted(lines: string[], repoRoot: string): string[] {
  const prefix = repoRoot.endsWith("/") ? repoRoot : `${repoRoot}/`;
  const paths = new Set<string>();
  for (const raw of lines) {
    const match = clean(raw).trim().match(UNFORMATTED_RE);
    if (!match) continue;
    const path = match[1];
    paths.add(path.startsWith(prefix) ? path.slice(prefix.length) : path);
  }
  return [...paths];
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

/** Model definition wrapping the repository's swamp extension suite. */
export const model = {
  type: "@gdesmott/swamp-tests",
  version: "2026.08.06.2",
  description:
    "Run the swamp extension suite and capture which tests failed, or which " +
    "files need reformatting",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed swamp extension suite run summary",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from the run",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    run: {
      description:
        "Run the swamp extension suite: deno formatting check, then the tests",
      arguments: RunArgsSchema,
      outputs: ["summary", "log"],
      // deno-lint-ignore no-explicit-any
      execute: async (args: RunArgs, context: any) => {
        const globals = context.globalArgs as GlobalArgs;
        const cwd = resolve(context.repoDir, globals.repoRoot);
        const argv = [globals.target];
        const command = [globals.makeBin, ...argv].join(" ");
        const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;
        const timeoutSignal = AbortSignal.timeout(timeoutMs);

        context.logger.info("Running {command} in {cwd}", { command, cwd });

        const logWriter = context.createFileWriter("log", "log");
        const lines: string[] = [];
        const startedAt = Date.now();

        const child = new Deno.Command(globals.makeBin, {
          args: argv,
          cwd,
          stdout: "piped",
          stderr: "piped",
          signal: AbortSignal.any([context.signal, timeoutSignal]),
        }).spawn();

        const encoder = new TextEncoder();

        /**
         * Record a line: keep it for parsing, persist it to the log file, and
         * mirror it to stderr. stderr is used because swamp emits its own JSON
         * on stdout.
         */
        const record = async (raw: string): Promise<void> => {
          const line = clean(raw);
          lines.push(line);
          await logWriter.writeLine(line);
          if (!args.quiet) {
            await Deno.stderr.write(encoder.encode(`${line}\n`));
          }
        };

        /** Decode a stream, splitting it into lines. */
        const pump = async (
          stream: ReadableStream<Uint8Array>,
        ): Promise<void> => {
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

        const durationMs = Date.now() - startedAt;
        const timedOut = timeoutSignal.aborted;
        const logHandle = await logWriter.finalize();

        // A cancelled run measured nothing, so a summary of zeroes would read
        // as a suite that passed. Keep the partial log and stop.
        if (
          !timedOut && (context.signal.aborted || status.signal === "SIGINT")
        ) {
          throw new Error(
            `\`${command}\` was cancelled after ${durationMs}ms. No summary recorded.`,
          );
        }

        const counts = parseTestSummary(lines);
        const failures = parseFailures(lines).slice(0, MAX_FAILURES);
        const unformatted = parseUnformatted(lines, cwd);
        // Formatting runs first and stops the target, so a run that failed
        // having reported unformatted files never reached the tests. Deciding
        // it this way rather than from the exit code alone is what lets the
        // digest name the check to fix instead of the target.
        const stage = status.success
          ? null
          : unformatted.length > 0
          ? "format"
          : counts !== null || failures.length > 0
          ? "tests"
          : null;

        const summaryHandle = await context.writeResource(
          "summary",
          "summary",
          {
            command,
            repoRoot: cwd,
            exitCode: status.code,
            status: status.success ? "passed" : "failed",
            stage,
            testsRun: counts?.testsRun ?? null,
            passed: counts?.passed ?? null,
            failed: counts?.failed ?? null,
            summaryParsed: counts !== null,
            failures,
            unformatted,
            timedOut,
            durationMs,
            executedAt: new Date().toISOString(),
          },
        );

        const handles = [summaryHandle, logHandle];

        if (timedOut) {
          throw new Error(
            `\`${command}\` timed out after ${timeoutMs}ms. See the log data for details.`,
          );
        }

        if (!status.success && !args.ignoreFailure) {
          throw new Error(
            `\`${command}\` exited with code ${status.code}` +
              (stage === "format"
                ? `: ${unformatted.length} file(s) need reformatting. Run \`deno fmt\` in extensions/.`
                : stage === "tests"
                ? `: ${counts?.failed ?? failures.length} test(s) failed.`
                : ". See the log data for details."),
          );
        }

        return { dataHandles: handles };
      },
    },
  },
};
