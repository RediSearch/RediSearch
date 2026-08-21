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
import { z } from "npm:zod@4.4.3";

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
  repoRoot: z
    .string()
    .min(1)
    .default(".")
    .describe(
      "The checkout this model works on, resolved against the swamp repository " +
        "directory. Everything else here is relative to it. Set it when swamp " +
        "lives in a subdirectory of the checkout rather than at its root — e.g. " +
        "`..` when the swamp files are kept under `<checkout>/swamp`.",
    ),
  workingDir: z
    .string()
    .min(1)
    .default("src/redisearch_rs")
    .describe(
      "Directory to invoke cargo in. Relative paths resolve against `repoRoot`.",
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
      `Script to read the ${EXCLUDE_VAR} exclude list from, relative to ` +
        `\`repoRoot\`. Only consulted for a workspace-wide run.`,
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
        "`crate`. Relative paths resolve against `repoRoot`.",
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
        "build reported. Empty runs with BINDIR unset — including when the " +
        "caller's environment exports one — so a run without it links what a " +
        "plain `cargo llvm-cov` would.",
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
 * Lines no test executed.
 *
 * A segment is a transition point, not a region: it says what the count becomes
 * at that position, and that count holds until the next segment replaces it. So
 * the lines a zero-count segment accounts for run from its own line up to the
 * next segment's, and reading a segment as a single line understates every
 * region longer than one — a function nothing calls was reported as one
 * uncovered line, which reads as nearly covered for code with no test at all.
 *
 * Every zero-count segment counts, not only the ones that enter a region.
 * Regions nest, and a nested one ends without ending its parent: an uncalled
 * function containing an `if` emits entries for the condition and the arms, and
 * then a segment restoring the enclosing count — still zero — for the rest of
 * the body. Taking only region entries left everything after the last nested
 * one unaccounted for, which is how a `return` at the end of an uncalled
 * function was reported as covered.
 *
 * `hasCount` is what keeps the closing segments out. A region that closes with
 * nothing enclosing it carries no count at all, and reading its zero as "never
 * executed" would report the rest of the file. Where something does enclose it,
 * the count it carries is that enclosing region's, which is the right answer to
 * use. `isGapRegion` is excluded for its own reason: a gap is filler between
 * regions rather than code, and reports zero regardless of what ran.
 *
 * The last segment has nothing after it to bound it, so it accounts for its own
 * line and no more.
 */
export function uncoveredLines(segments: Segment[]): number[] {
  const lines = new Set<number>();
  // Source order, which the export already uses, and which the spans below
  // depend on: the next segment is only where this count stops if no segment
  // between them was skipped.
  const ordered = [...segments].sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  for (let i = 0; i < ordered.length; i++) {
    const [line, , count, hasCount, , isGapRegion] = ordered[i];
    if (!hasCount || isGapRegion || count !== 0) continue;
    lines.add(line);
    const next = ordered[i + 1];
    if (!next) continue;
    for (let span = line + 1; span < next[0]; span++) lines.add(span);
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
 * `repoRoot` is stripped from the absolute filenames the export carries, so the
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
  repoRoot = "",
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

  const prefix = repoRoot.endsWith("/") ? repoRoot : `${repoRoot}/`;
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
      file: repoRoot && file.filename.startsWith(prefix)
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
  repoRoot: string,
  logger: { info: (msg: string, props?: unknown) => void },
): Promise<string | null> {
  if (args.toolchain) return args.toolchain;
  if (!args.doctests) return null;

  const pin = `${repoRoot}/${NIGHTLY_FILE}`;
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

/**
 * Resolve a path against a base, collapsing `.` and `..` segments so that the
 * default `repoRoot` of "." does not leak into every reported path.
 */
function resolve(base: string, path: string): string {
  const absolute = path.startsWith("/") ? path : `${base}/${path}`;
  const segments: string[] = [];
  for (const segment of absolute.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join("/")}`;
}

/**
 * Build the environment cargo runs with, with BINDIR decided rather than
 * inherited.
 *
 * BINDIR names the C archives the build scripts link against. Without one they
 * look in the conventional release layout, which is what a plain `cargo
 * llvm-cov` does — so a run given no `binDir` has to be given no BINDIR either.
 * An earlier debug, coverage or sanitizer build exports it, and inheriting that
 * measures a different build's archives while the summary reports an ordinary
 * run.
 *
 * Deno can override a variable but not remove one, so dropping an inherited
 * BINDIR means handing over the whole environment without it. That is done only
 * when there is one to drop: the ordinary case keeps plain inheritance rather
 * than a wholesale copy of the caller's environment.
 */
function binDirEnv(
  binDir: string | undefined,
): { env: Record<string, string>; clearEnv: boolean } {
  const dropInherited = !binDir && Deno.env.get("BINDIR") !== undefined;
  const env: Record<string, string> = dropInherited ? Deno.env.toObject() : {};
  if (dropInherited) delete env.BINDIR;
  if (binDir) env.BINDIR = binDir;
  return { env, clearEnv: dropInherited };
}

/**
 * Order files worst-covered first, with the ones that have nothing to cover
 * last.
 *
 * A file with no instrumented lines reports 0%, which by percentage alone is
 * the worst in the workspace — so a plain sort puts every one of them ahead of
 * the files that actually have gaps, and `maxFiles` then truncates the list
 * before reaching a single real one. `onlyIncomplete` filters them out earlier,
 * which is why this only bites the other path; the summary promises the worst
 * files first either way, so the ordering has to hold on its own.
 */
export function worstFirst(a: FileCoverage, b: FileCoverage): number {
  const measurable = (f: FileCoverage) => (f.totalLines > 0 ? 0 : 1);
  return measurable(a) - measurable(b) || a.percent - b.percent;
}

/** Model definition wrapping `cargo llvm-cov test`. */
export const model = {
  type: "@gdesmott/rust-coverage",
  version: "2026.08.18.2",
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

        const { repoRoot, workingDir, cargoBin, excludeFrom } =
          context.globalArgs;
        const root = resolve(context.repoDir, repoRoot);
        const cwd = workingDir.startsWith("/")
          ? workingDir
          : resolve(root, workingDir);

        try {
          await Deno.stat(`${cwd}/Cargo.toml`);
        } catch {
          throw new Error(`No Cargo.toml found at ${cwd}/Cargo.toml`);
        }

        const manifestPath = args.manifestPath
          ? (args.manifestPath.startsWith("/")
            ? args.manifestPath
            : resolve(root, args.manifestPath))
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
            : resolve(root, excludeFrom);
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
          root,
          context.logger,
        );
        const argv = buildArgs(args, excluded, manifestPath, toolchain);
        const command = [cargoBin, ...argv].join(" ");
        const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;
        const timeoutSignal = AbortSignal.timeout(timeoutMs);
        // One signal object, because the same abort has to reach two places:
        // the process, and the readers draining what it left behind.
        const abort = AbortSignal.any([context.signal, timeoutSignal]);
        const scope = manifestPath ?? args.crate ?? "workspace";

        context.logger.info("Running {command} in {cwd}", { command, cwd });

        const logWriter = context.createFileWriter("log", "log");
        const startedAt = Date.now();

        const { env, clearEnv } = binDirEnv(args.binDir);

        const child = new Deno.Command(cargoBin, {
          args: argv,
          cwd,
          env,
          clearEnv,
          stdout: "piped",
          stderr: "piped",
          signal: abort,
        }).spawn();

        const encoder = new TextEncoder();
        const decoder = new TextDecoder();

        /**
         * Take a reader that gives up once the run has been aborted.
         *
         * The abort reaches cargo and nothing else: rustc, the test binaries
         * and llvm-cov itself inherited these pipes and were signalled by no
         * one. Draining until a pipe closes therefore waits for whichever of
         * them lives longest, so a run with a hung descendant outlives the
         * timeout that was meant to end it — and the timeout is only looked at
         * after both readers return. Cancelling costs whatever those processes
         * had not written yet, which is a fair price for a run that is over
         * either way.
         *
         * Both streams need it, not just the one being mirrored: either pipe
         * left open is enough to hold the run.
         */
        const readerFor = (
          stream: ReadableStream<Uint8Array>,
        ): ReadableStreamDefaultReader<Uint8Array> => {
          const reader = stream.getReader();
          const stopDraining = () => {
            reader.cancel().catch(() => {});
          };
          if (abort.aborted) stopDraining();
          else abort.addEventListener("abort", stopDraining, { once: true });
          return reader;
        };

        // The export goes to stdout and the progress to stderr, so unlike the
        // other cargo models these are kept apart: merging them would corrupt
        // the JSON this run exists to read.
        const collect = async (
          stream: ReadableStream<Uint8Array>,
        ): Promise<string> => {
          const reader = readerFor(stream);
          let text = "";
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            text += decoder.decode(value, { stream: true });
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
          const reader = readerFor(stream);
          let buffer = "";
          const streamDecoder = new TextDecoder();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += streamDecoder.decode(value, { stream: true });
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

        const parsed = parseExport(exported, root);
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
          .sort(worstFirst)
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

        // A clean exit is not a measurement either. cargo-llvm-cov exports on
        // stdout only while nothing redirects it, and `extraArgs` is public:
        // `--output-path` writes the JSON to a file and still exits 0. Reaching
        // here with nothing parsed would record `parsed: false` alongside a
        // successful run, and a caller reading the percentage would take the
        // absence of one for zero uncovered lines.
        if (parsed === null) {
          throw new Error(
            `\`${command}\` exited cleanly without writing a coverage export ` +
              "to stdout — an `extraArgs` entry redirecting the report, such as " +
              "`--output-path`, leaves nothing to measure. See the log data for " +
              "details.",
          );
        }

        return { dataHandles: handles };
      },
    },
  },
};
