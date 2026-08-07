/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Reports which Rust lines the Rust tests did not exercise.
 *
 * Wraps `cargo llvm-cov test --json`, which builds the workspace with source
 * instrumentation, runs the tests, and exports llvm's own coverage mapping. The
 * export is parsed down to the lines no test entered, as ranges per file, sorted
 * worst-covered first — the shape you act on, rather than three megabytes of
 * segments.
 *
 * Scope is a choice with a real cost. A single crate is cheap and is what you
 * want while writing tests; the whole workspace is not, and it also has to skip
 * the bencher crates that link against the C library, because instrumenting them
 * demands C symbols at link time that a coverage build does not have. That
 * exclude list lives in build.sh and is read from there rather than copied, so
 * this model cannot drift from the build.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Default timeout: an instrumented build plus the suite is not quick. */
const DEFAULT_TIMEOUT_MS = 60 * 60 * 1000;

/** Variable in build.sh holding the crates a coverage run cannot link. */
const EXCLUDE_VAR = "EXCLUDE_RUST_BENCHING_CRATES_LINKING_C";

/** File pinning the nightly toolchain, needed to measure doctest coverage. */
const NIGHTLY_FILE = ".rust-nightly";

/** Files kept in the summary, worst covered first. */
const DEFAULT_MAX_FILES = 50;

/**
 * Lines that mean a test failed, in the output of the run behind the
 * measurement.
 *
 * The exit code cannot answer this on its own. `--ignore-run-fail`, which
 * `ignoreTestFailure` passes so that a failing run still produces a report,
 * makes llvm-cov exit 0 whenever it managed to write one — so the only account
 * of how the tests actually went is what they printed. Both forms are matched
 * because they come from different places: the first from libtest, per test
 * binary, and the second from cargo once one of them fails.
 */
const TEST_FAILURE_RE = /^(?:test result: FAILED\b|error: test failed\b)/;

const GlobalArgsSchema = z.object({
  workingDir: z
    .string()
    .min(1)
    .default("src/redisearch_rs")
    .describe(
      "Directory to invoke cargo in. Relative paths resolve against the repository root.",
    ),
  cargoBin: z
    .string()
    .min(1)
    .default("cargo")
    .describe("The cargo executable to invoke."),
  excludeFrom: z
    .string()
    .min(1)
    .default("build.sh")
    .describe(
      `Script to read the ${EXCLUDE_VAR} exclude list from, relative to the ` +
        `repository root. Only consulted for a workspace-wide run.`,
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const RunArgsSchema = z.object({
  crate: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Measure a single workspace crate (cargo `-p`). Far cheaper than the " +
        "whole workspace, and what you want while writing tests for one crate.",
    ),
  manifestPath: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Measure the crate owning this Cargo.toml, instead of naming it with " +
        "`crate`. Relative paths resolve against the repository root.",
    ),
  doctests: z
    .boolean()
    .optional()
    .describe(
      "Include doctests. cargo-llvm-cov measures those through nightly-only " +
        "features, so this selects the nightly pinned in " +
        `${NIGHTLY_FILE} unless \`toolchain\` names another. Off by default, ` +
        "which keeps the run on the repository's pinned stable toolchain.",
    ),
  toolchain: z
    .string()
    .optional()
    .describe(
      `Toolchain to run cargo with, without the leading "+". Empty takes the ` +
        `nightly pinned in ${NIGHTLY_FILE} when \`doctests\` is set and the ` +
        `repository's default otherwise, so a caller can pass an unset choice ` +
        `through without special-casing it.`,
    ),
  extraArgs: z
    .array(z.string())
    .optional()
    .describe("Additional arguments appended to the cargo invocation."),
  binDir: z
    .string()
    .optional()
    .describe(
      "Directory holding the compiled C static libraries, exported as " +
        "BINDIR. The crates that link against C read it to find them, and " +
        "without it they fall back to the conventional release layout — so a " +
        "measurement either links a stale release archive or fails outright " +
        "on a checkout that was never built release. Pass the binDir the " +
        "build reported. Empty leaves BINDIR untouched, so a caller can pass " +
        "an unset one through without special-casing it.",
    ),
  maxFiles: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      `How many files to keep in the summary, worst covered first (default ` +
        `${DEFAULT_MAX_FILES}). The totals always count every file.`,
    ),
  onlyIncomplete: z
    .boolean()
    .default(true)
    .describe(
      "Keep only files with something left to exercise: below 100% of lines, " +
        "or carrying a region no test entered. The second case matters — an " +
        "unexecuted arm of a one-line `if` leaves every line hit and the file " +
        "at 100% while still holding a real gap. A file with neither has " +
        "nothing to act on.",
    ),
  timeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(`Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}).`),
  ignoreTestFailure: z
    .boolean()
    .optional()
    .describe(
      "Report the coverage even when tests failed (`--ignore-run-fail`). The " +
        "measurement still describes what ran, but it is a floor rather than " +
        "the real figure.",
    ),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default cargo's progress is mirrored to " +
        "stderr, since an instrumented build can run for many minutes.",
    ),
});

type RunArgs = z.infer<typeof RunArgsSchema>;

const RangeSchema = z.object({
  start: z.number().int().positive().describe("First line of the run"),
  end: z
    .number()
    .int()
    .positive()
    .describe("Last line of the run; equal to `start` for a single line"),
});

const FileSchema = z.object({
  file: z
    .string()
    .describe("Source file, relative to the repository where possible"),
  coveredLines: z.number().int().nonnegative().describe(
    "Lines hit at least once",
  ),
  totalLines: z.number().int().nonnegative().describe("Instrumented lines"),
  percent: z.number().describe("Covered share of instrumented lines"),
  uncoveredCount: z
    .number()
    .int()
    .nonnegative()
    .describe("Lines starting a region no test entered"),
  uncoveredRanges: z
    .array(RangeSchema)
    .describe("Those lines, with consecutive lines collapsed into one range"),
});

const SummarySchema = z.object({
  command: z.string().describe("The full command line that was executed"),
  workingDir: z.string().describe("Resolved directory cargo ran in"),
  scope: z
    .string()
    .describe(
      "What was measured: a crate name, a manifest path, or `workspace`",
    ),
  excluded: z
    .array(z.string())
    .describe(`Crates skipped from a workspace run, per ${EXCLUDE_VAR}`),
  exitCode: z.number().int().describe("Exit code of cargo llvm-cov"),
  status: z
    .enum(["passed", "failed"])
    .describe(
      "Whether the tests behind the measurement passed. Not the exit code: " +
        "under `ignoreTestFailure` llvm-cov exits 0 whenever it managed to " +
        "write the report, however the tests went, so this is read from the " +
        "run's own output as well",
    ),
  parsed: z
    .boolean()
    .describe(
      "True when llvm's export was read. False when the run ended before " +
        "producing one, e.g. on a compile error, in which case the coverage " +
        "figures below are absent rather than zero",
    ),
  overall: z
    .object({
      coveredLines: z.number().int().nonnegative(),
      totalLines: z.number().int().nonnegative(),
      percent: z.number(),
    })
    .nullable()
    .describe("Totals across every measured file, not just the kept ones"),
  filesMeasured: z
    .number()
    .int()
    .nonnegative()
    .describe("How many files the run measured"),
  filesIncomplete: z
    .number()
    .int()
    .nonnegative()
    .describe(
      "How many of them have something left to exercise: below 100% of " +
        "lines, or holding a region no test entered while every line was hit",
    ),
  filesKept: z
    .number()
    .int()
    .nonnegative()
    .describe("How many appear in `files` below"),
  files: z
    .array(FileSchema)
    .describe("Worst covered first, capped by `maxFiles`"),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/**
 * One llvm coverage segment: `[line, column, count, hasCount, isRegionEntry,
 * isGapRegion]`.
 */
type Segment = [number, number, number, boolean, boolean, boolean];

interface ExportFile {
  filename: string;
  segments?: Segment[];
  summary?: { lines?: { count?: number; covered?: number; percent?: number } };
}

/** A file's coverage, as read from llvm's export. */
export interface FileCoverage {
  file: string;
  coveredLines: number;
  totalLines: number;
  percent: number;
  uncovered: number[];
}

/**
 * Lines that start a region no test entered.
 *
 * A segment marks where a region begins; the region was never entered when its
 * count is zero. Only region entries are considered, because the segments that
 * merely close a region also carry a zero count and would report every line in
 * the file. A line can start both an entered and an unentered region — a
 * one-line `if` with an unexecuted arm — and is listed, because the point is
 * what was not exercised rather than what the line-hit count says.
 *
 * A zero only means "never entered" when the segment actually carries a count.
 * Where `hasCount` is false the region was not instrumented, and where
 * `isGapRegion` is true it is filler between regions rather than code; both
 * report zero regardless of what ran, so counting them would send someone off
 * to write a test for code the coverage mapping deliberately skipped.
 */
export function uncoveredLines(segments: Segment[]): number[] {
  const lines = new Set<number>();
  for (const segment of segments) {
    const [line, , count, hasCount, isRegionEntry, isGapRegion] = segment;
    if (!hasCount || isGapRegion || !isRegionEntry) continue;
    if (count === 0) lines.add(line);
  }
  return [...lines].sort((a, b) => a - b);
}

/** Collapse sorted line numbers into inclusive ranges. */
export function toRanges(
  lines: number[],
): Array<{ start: number; end: number }> {
  const sorted = [...new Set(lines)].sort((a, b) => a - b);
  const ranges: Array<{ start: number; end: number }> = [];

  for (const line of sorted) {
    const last = ranges[ranges.length - 1];
    if (last && line === last.end + 1) last.end = line;
    else ranges.push({ start: line, end: line });
  }

  return ranges;
}

/**
 * Parse llvm's coverage export into per-file coverage.
 *
 * `repoDir` is stripped from the absolute filenames the export carries, so the
 * paths read the way the rest of the repository refers to them. Returns null
 * when the text is not an export at all, which is what a run that died before
 * measuring anything leaves behind.
 */
/**
 * Whether the run behind a measurement reported a failing test.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function sawTestFailure(lines: string[]): boolean {
  return lines.some((line) => TEST_FAILURE_RE.test(line.trim()));
}

export function parseExport(
  json: string,
  repoDir = "",
): { files: FileCoverage[]; coveredLines: number; totalLines: number } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    return null;
  }

  const data = (parsed as { data?: Array<{ files?: ExportFile[] }> })?.data
    ?.[0];
  if (!data?.files) return null;

  const prefix = repoDir.endsWith("/") ? repoDir : `${repoDir}/`;
  const files: FileCoverage[] = [];
  let coveredTotal = 0;
  let lineTotal = 0;

  for (const file of data.files) {
    const lines = file.summary?.lines ?? {};
    const total = lines.count ?? 0;
    const covered = lines.covered ?? 0;
    coveredTotal += covered;
    lineTotal += total;

    files.push({
      file: repoDir && file.filename.startsWith(prefix)
        ? file.filename.slice(prefix.length)
        : file.filename,
      coveredLines: covered,
      totalLines: total,
      percent: lines.percent ?? (total === 0 ? 0 : (covered / total) * 100),
      uncovered: uncoveredLines(file.segments ?? []),
    });
  }

  return { files, coveredLines: coveredTotal, totalLines: lineTotal };
}

/**
 * Read the crates a workspace-wide coverage run has to skip.
 *
 * build.sh holds the list because the same crates break its own coverage build;
 * reading it keeps this model in step rather than repeating it.
 */
export function parseExcludes(buildScript: string): string[] {
  const line = buildScript
    .split("\n")
    .find((l) => l.startsWith(`${EXCLUDE_VAR}=`));
  if (!line) return [];
  const value = line.slice(EXCLUDE_VAR.length + 1).replace(/["']/g, "");
  return value.split(/\s+/).filter((token) => token && token !== "--exclude");
}

/**
 * Decide which toolchain to invoke cargo with.
 *
 * An explicit choice is honoured as given. Otherwise only a doctest run needs
 * one: cargo-llvm-cov measures doctests through nightly-only features, so asking
 * for them on the repository's pinned stable fails before any coverage is
 * produced. Every other run takes no toolchain at all, leaving
 * rust-toolchain.toml to decide as it does for every other cargo invocation.
 */
export async function resolveToolchain(
  args: RunArgs,
  repoDir: string,
  logger: { info: (msg: string, props?: unknown) => void },
): Promise<string | null> {
  if (args.toolchain) return args.toolchain;
  if (!args.doctests) return null;

  const pin = `${repoDir}/${NIGHTLY_FILE}`;
  try {
    const toolchain = (await Deno.readTextFile(pin)).trim();
    if (toolchain) return toolchain;
  } catch {
    // Fall through to the message below.
  }

  // Whatever nightly is installed still measures doctests, so the run is worth
  // making; say which one it fell back to, since coverage that cannot be
  // reproduced against the pinned toolchain is hard to argue with.
  logger.info(
    "No toolchain pinned in {pin}, falling back to {toolchain} for doctest " +
      "coverage. Results may differ from CI.",
    { pin, toolchain: "nightly" },
  );
  return "nightly";
}

/** Build the cargo argument vector for a run. */
export function buildArgs(
  args: RunArgs,
  excluded: string[],
  manifestPath: string | null,
  toolchain: string | null = null,
): string[] {
  const argv: string[] = [];
  // Selected with a `+toolchain` argument rather than an environment variable so
  // it shows up in the recorded command line, which is what a caller reads when
  // the run behaves differently from their own shell.
  if (toolchain) argv.push(`+${toolchain}`);
  argv.push("llvm-cov", "test", "--quiet", "--json");

  if (manifestPath) argv.push("--manifest-path", manifestPath);
  else if (args.crate) argv.push("-p", args.crate);
  else {
    argv.push("--workspace");
    for (const crate of excluded) argv.push("--exclude", crate);
  }

  if (args.doctests) argv.push("--doctests");
  // Tolerating the failure here is not the same as tolerating it in cargo: left
  // to itself, llvm-cov propagates a failing test run and stops before writing
  // the report, so the floor this option promises would not exist to record.
  if (args.ignoreTestFailure) argv.push("--ignore-run-fail");
  if (args.extraArgs) argv.push(...args.extraArgs);
  return argv;
}

/** Model definition wrapping `cargo llvm-cov test`. */
export const model = {
  type: "@gdesmott/rust-coverage",
  version: "2026.08.06.2",
  description:
    "Measure Rust test coverage with cargo llvm-cov and report the uncovered lines",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Uncovered lines per Rust source file",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Progress and diagnostics from cargo llvm-cov",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    run: {
      description:
        "Measure coverage for a crate or the whole workspace and report the uncovered lines",
      arguments: RunArgsSchema,
      execute: async (
        args: RunArgs,
        context: {
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
        },
      ): Promise<{ dataHandles: Array<{ name: string }> }> => {
        if (args.crate && args.manifestPath) {
          throw new Error(
            "crate and manifestPath both name what to measure; pass one.",
          );
        }

        const { workingDir, cargoBin, excludeFrom } = context.globalArgs;
        const cwd = workingDir.startsWith("/")
          ? workingDir
          : workingDir === "."
          ? context.repoDir
          : `${context.repoDir}/${workingDir}`;

        try {
          await Deno.stat(`${cwd}/Cargo.toml`);
        } catch {
          throw new Error(`No Cargo.toml found at ${cwd}/Cargo.toml`);
        }

        const manifestPath = args.manifestPath
          ? (args.manifestPath.startsWith("/")
            ? args.manifestPath
            : `${context.repoDir}/${args.manifestPath}`)
          : null;
        if (manifestPath) {
          try {
            await Deno.stat(manifestPath);
          } catch {
            throw new Error(`No Cargo.toml found at ${manifestPath}`);
          }
        }

        // Only a workspace run needs the excludes, and only then is failing to
        // read them worth stopping for.
        let excluded: string[] = [];
        if (!manifestPath && !args.crate) {
          const excludePath = excludeFrom.startsWith("/")
            ? excludeFrom
            : `${context.repoDir}/${excludeFrom}`;
          try {
            excluded = parseExcludes(await Deno.readTextFile(excludePath));
          } catch {
            throw new Error(
              `Could not read the ${EXCLUDE_VAR} exclude list from ${excludePath}`,
            );
          }
        }

        const toolchain = await resolveToolchain(
          args,
          context.repoDir,
          context.logger,
        );
        const argv = buildArgs(args, excluded, manifestPath, toolchain);
        const command = [cargoBin, ...argv].join(" ");
        const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;
        const timeoutSignal = AbortSignal.timeout(timeoutMs);
        const scope = manifestPath ?? args.crate ?? "workspace";

        context.logger.info("Running {command} in {cwd}", { command, cwd });

        const logWriter = context.createFileWriter("log", "log");
        const startedAt = Date.now();

        const child = new Deno.Command(cargoBin, {
          args: argv,
          cwd,
          // Only set when asked, so a run without one keeps whatever BINDIR
          // the environment already provides.
          env: args.binDir ? { BINDIR: args.binDir } : {},
          stdout: "piped",
          stderr: "piped",
          signal: AbortSignal.any([context.signal, timeoutSignal]),
        }).spawn();

        const encoder = new TextEncoder();
        const decoder = new TextDecoder();

        // The export goes to stdout and the progress to stderr, so unlike the
        // other cargo models these are kept apart: merging them would corrupt
        // the JSON this run exists to read.
        const collect = async (
          stream: ReadableStream<Uint8Array>,
        ): Promise<string> => {
          let text = "";
          for await (const chunk of stream) {
            text += decoder.decode(chunk, { stream: true });
          }
          return text + decoder.decode();
        };

        // Only the lines that say a test failed are kept, not the whole
        // stream: a workspace run prints tens of thousands, and the log already
        // holds every one of them.
        const failureLines: string[] = [];
        const keep = (line: string): void => {
          if (sawTestFailure([line])) failureLines.push(line.trim());
        };

        const mirror = async (
          stream: ReadableStream<Uint8Array>,
        ): Promise<void> => {
          let buffer = "";
          const streamDecoder = new TextDecoder();
          for await (const chunk of stream) {
            buffer += streamDecoder.decode(chunk, { stream: true });
            const parts = buffer.split("\n");
            buffer = parts.pop() ?? "";
            for (const part of parts) {
              keep(part);
              await logWriter.writeLine(part);
              if (!args.quiet) {
                await Deno.stderr.write(encoder.encode(`${part}\n`));
              }
            }
          }
          if (buffer.length > 0) {
            keep(buffer);
            await logWriter.writeLine(buffer);
          }
        };

        const [exported] = await Promise.all([
          collect(child.stdout),
          mirror(child.stderr),
        ]);
        const status = await child.status;

        const durationMs = Date.now() - startedAt;
        const timedOut = timeoutSignal.aborted;
        const logHandle = await logWriter.finalize();

        // A cancelled run measured nothing, so a summary of zeroes would read as
        // a workspace with no coverage. Keep the partial log and stop.
        if (
          !timedOut && (context.signal.aborted || status.signal === "SIGINT")
        ) {
          throw new Error(
            `\`${command}\` was cancelled after ${durationMs}ms. No summary recorded.`,
          );
        }

        const parsed = parseExport(exported, context.repoDir);
        // A file is incomplete when anything in it was not exercised, which the
        // line percentage alone does not answer. An unentered region can sit on
        // a line that was otherwise hit — the unexecuted arm of a one-line `if`
        // — leaving the file at 100% with a gap that uncoveredLines went out of
        // its way to find. Filtering on the percentage alone would discard
        // exactly the case the parser exists to detect.
        //
        // A file with no instrumented lines at all reports 0%, which the
        // percentage test would read as the worst file in the workspace and
        // sort to the very front — pushing files that do have gaps out past
        // maxFiles. There is nothing to cover there, so it is only incomplete
        // if it carries an unentered region.
        const incomplete = parsed
          ? parsed.files.filter((f) =>
            (f.totalLines > 0 && f.percent < 100) || f.uncovered.length > 0
          )
          : [];
        const candidates = args.onlyIncomplete
          ? incomplete
          : parsed?.files ?? [];
        const kept = [...candidates]
          .sort((a, b) => a.percent - b.percent)
          .slice(0, args.maxFiles ?? DEFAULT_MAX_FILES)
          .map((file) => ({
            file: file.file,
            coveredLines: file.coveredLines,
            totalLines: file.totalLines,
            percent: file.percent,
            uncoveredCount: file.uncovered.length,
            uncoveredRanges: toRanges(file.uncovered),
          }));

        if (parsed && candidates.length > kept.length) {
          // Said out loud: a capped list reads as the whole picture otherwise.
          context.logger.info(
            "Kept the {kept} worst covered of {total} file(s); the rest are in the totals only",
            { kept: kept.length, total: candidates.length },
          );
        }

        const summaryHandle = await context.writeResource(
          "summary",
          "summary",
          {
            command,
            workingDir: cwd,
            scope,
            excluded,
            exitCode: status.code,
            // A clean exit is not the same as clean tests here: with
            // --ignore-run-fail, llvm-cov reports success for having written
            // the report. Presenting a floor measurement as the real figure is
            // exactly what the caller asked not to happen when it chose to
            // tolerate the failure rather than to ignore it.
            status: status.success && failureLines.length === 0
              ? "passed"
              : "failed",
            parsed: parsed !== null,
            overall: parsed
              ? {
                coveredLines: parsed.coveredLines,
                totalLines: parsed.totalLines,
                percent: parsed.totalLines === 0
                  ? 0
                  : (parsed.coveredLines / parsed.totalLines) * 100,
              }
              : null,
            filesMeasured: parsed?.files.length ?? 0,
            filesIncomplete: incomplete.length,
            filesKept: kept.length,
            files: kept,
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

        // `ignoreTestFailure` tolerates a failure it can still report around: it
        // promises a floor, and a floor needs a measurement. A run that died
        // before exporting anything — a compile error, missing llvm tooling —
        // measured nothing at all, and returning success for it would let a
        // caller read an absence as a completed run.
        if (!status.success && !(args.ignoreTestFailure && parsed !== null)) {
          throw new Error(
            `\`${command}\` exited with code ${status.code}` +
              (parsed
                ? ", so the coverage recorded is a floor rather than the real figure."
                : " before exporting any coverage. See the log data for details."),
          );
        }

        return { dataHandles: handles };
      },
    },
  },
};
