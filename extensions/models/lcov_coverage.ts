/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Reports which lines of given C sources the test suites did not exercise.
 *
 * `build.sh COV=1` instruments the C build with gcov and writes an lcov trace
 * once the tests have run: `bin/flow_<deployment>.info` for the Python suite,
 * `bin/unit.info` for the C and C++ unit tests. This model reads that trace and
 * answers the only question worth asking of it — which lines of the files I care
 * about were never hit — as line ranges rather than a wall of numbers.
 *
 * It deliberately does not build or run anything. The build and suite models
 * already do that, and separating them means asking about a different source
 * file costs a parse instead of a ten-minute suite run. What it does instead is
 * refuse to answer from a stale trace: pass `notOlderThan` with the suite run's
 * timestamp and a trace left over from an earlier run is an error rather than a
 * set of made-up gaps.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Where build.sh writes its traces, relative to the repository root. */
const DEFAULT_BIN_DIR = "bin";

/**
 * Rust sources are invisible to this trace: the coverage build instruments C
 * with gcov, and Rust coverage comes from cargo llvm-cov instead. Saying so
 * beats reporting a Rust file as having no data.
 */
const RUST_DIR = "src/redisearch_rs";

const GlobalArgsSchema = z.object({
  repoRoot: z
    .string()
    .min(1)
    .default(".")
    .describe(
      "Repository root the trace's paths are relative to. Relative paths " +
        "resolve against the repository root.",
    ),
  binDir: z
    .string()
    .min(1)
    .default(DEFAULT_BIN_DIR)
    .describe(
      "Directory build.sh writes coverage traces to, relative to `repoRoot`.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const ReportArgsSchema = z.object({
  files: z
    .array(z.string().min(1))
    .min(1)
    .describe(
      "C sources to report on, as repository-relative paths, e.g. " +
        "`src/query.c`. A path under " + RUST_DIR + " is rejected: gcov does " +
        "not see Rust, so use the rust-coverage model for those.",
    ),
  suite: z
    .enum(["flow", "unit"])
    .default("flow")
    .describe(
      "Which suite's trace to read: `flow` for the Python tests, `unit` for " +
        "the C and C++ unit tests.",
    ),
  deployment: z
    .enum(["standalone", "cluster"])
    .default("standalone")
    .describe(
      "Which flow trace to read, since build.sh names one per deployment. " +
        "Ignored when `suite` is `unit`.",
    ),
  infoFile: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Read this trace instead of the one `suite` and `deployment` name. " +
        "Relative paths resolve against the repository root.",
    ),
  notOlderThan: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Fail unless the trace was written at or after this ISO 8601 timestamp. " +
        "Pass the suite run's own `executedAt` to prove the trace belongs to " +
        "it, rather than reporting gaps from an earlier run.",
    ),
  requireAllFound: z
    .boolean()
    .default(true)
    .describe(
      "Fail when a requested file has no coverage data at all. That usually " +
        "means it is not compiled into the module, or was not rebuilt with " +
        "coverage, and reporting it as untested would be wrong either way.",
    ),
});

type ReportArgs = z.infer<typeof ReportArgsSchema>;

const RangeSchema = z.object({
  start: z.number().int().positive().describe("First line of the run"),
  end: z
    .number()
    .int()
    .positive()
    .describe("Last line of the run; equal to `start` for a single line"),
});

const TargetSchema = z.object({
  file: z.string().describe("The requested path, as given"),
  found: z
    .boolean()
    .describe(
      "Whether the trace carried data for this file. False means the file was " +
        "not compiled into what the suite exercised, not that it is untested",
    ),
  coveredLines: z.number().int().nonnegative().describe(
    "Lines hit at least once",
  ),
  totalLines: z
    .number()
    .int()
    .nonnegative()
    .describe(
      "Instrumented lines; blank lines and declarations are not counted",
    ),
  percent: z
    .number()
    .describe("Covered share of instrumented lines, 0 when there are none"),
  uncoveredCount: z.number().int().nonnegative().describe("Lines never hit"),
  uncoveredRanges: z
    .array(RangeSchema)
    .describe(
      "The never-hit lines, with consecutive lines collapsed into one range",
    ),
});

const SummarySchema = z.object({
  infoFile: z.string().describe("Trace that was read"),
  suite: z.enum(["flow", "unit"]).describe("Suite the trace came from"),
  deployment: z
    .string()
    .nullable()
    .describe("Deployment the flow trace came from, null for the unit trace"),
  capturedAt: z
    .iso
    .datetime()
    .describe("When the trace was last written, from its mtime"),
  stale: z
    .boolean()
    .describe("Whether the trace predates `notOlderThan`, when one was given"),
  notOlderThan: z
    .string()
    .nullable()
    .describe("The freshness bound the run was held to, if any"),
  filesInTrace: z
    .number()
    .int()
    .nonnegative()
    .describe("How many files the trace covers in total"),
  requested: z.number().int().positive().describe(
    "How many files were asked about",
  ),
  found: z
    .number()
    .int()
    .nonnegative()
    .describe("How many of them the trace carried data for"),
  targets: z
    .array(TargetSchema)
    .describe("One entry per requested file, in the order given"),
  overall: z
    .object({
      coveredLines: z.number().int().nonnegative(),
      totalLines: z.number().int().nonnegative(),
      percent: z.number(),
    })
    .describe("Totals across the requested files that were found"),
  executedAt: z.iso.datetime().describe("When this report was produced"),
});

/** A file's line coverage, as counted from the trace. */
export interface FileCoverage {
  coveredLines: number;
  totalLines: number;
  uncovered: number[];
}

/**
 * Parse an lcov trace into per-file line coverage, keyed by the path the trace
 * recorded.
 *
 * Only `SF:` (source file) and `DA:` (line, hit count) records matter for line
 * coverage; function and branch records are ignored. A `DA:` line may carry a
 * third checksum field, so the count is read positionally rather than by
 * splitting the record in two.
 */
export function parseTrace(trace: string): Map<string, FileCoverage> {
  // Hits are summed per source line before anything is counted. A trace can
  // list the same file more than once — once per test binary that linked it —
  // and the same line then appears once per record. Counting each record
  // separately would inflate the line total, and a line hit by one binary but
  // not another would be reported as a gap while also counting as covered.
  // Coverage is the union across binaries, so the records have to be merged
  // first and totalled after.
  const hitsByFile = new Map<string, Map<number, number>>();
  let current: Map<number, number> | null = null;

  for (const raw of trace.split("\n")) {
    const line = raw.trim();

    if (line.startsWith("SF:")) {
      const path = line.slice(3);
      current = hitsByFile.get(path) ?? new Map<number, number>();
      hitsByFile.set(path, current);
      continue;
    }

    if (line === "end_of_record") {
      current = null;
      continue;
    }

    if (!current || !line.startsWith("DA:")) continue;

    const parts = line.slice(3).split(",");
    const lineNo = Number(parts[0]);
    const hits = Number(parts[1]);
    if (!Number.isInteger(lineNo) || !Number.isFinite(hits)) continue;

    current.set(lineNo, (current.get(lineNo) ?? 0) + hits);
  }

  const files = new Map<string, FileCoverage>();
  for (const [path, hits] of hitsByFile) {
    const uncovered: number[] = [];
    let coveredLines = 0;
    for (const [lineNo, count] of hits) {
      if (count === 0) uncovered.push(lineNo);
      else coveredLines += 1;
    }
    uncovered.sort((a, b) => a - b);
    files.set(path, { coveredLines, totalLines: hits.size, uncovered });
  }

  return files;
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
 * Name the trace build.sh writes for a suite and deployment.
 *
 * The trace is named after the topology build.sh saw rather than the one asked
 * for: a cluster run sets REDIS_STANDALONE=0, which build.sh records as
 * `coordinator`. Spelling it `cluster` here would name a file that is never
 * written, so the cluster branch would fail to find the trace it just produced.
 */
export function traceName(
  suite: "flow" | "unit",
  deployment: string,
): string {
  if (suite === "unit") return "unit.info";
  const topology = deployment === "cluster" ? "coordinator" : deployment;
  return `flow_${topology}.info`;
}

/** Resolve a path against a root, leaving absolute paths alone. */
function resolve(path: string, root: string): string {
  return path.startsWith("/") ? path : `${root}/${path}`;
}

/**
 * Canonical form of a path for comparison, following symlinks when it exists.
 *
 * The trace records absolute paths as the build saw them, which need not be
 * spelled the same way as the caller's — `/var/home/...` against `/home/...`, for
 * instance, on a distribution where one is a link to the other.
 */
async function canonical(path: string): Promise<string> {
  try {
    return await Deno.realPath(path);
  } catch {
    return path;
  }
}

/** Model definition reporting uncovered lines from an lcov trace. */
export const model = {
  type: "@gdesmott/lcov-coverage",
  version: "2026.07.30.1",
  description:
    "Report which lines of given C sources the instrumented suites never hit",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Uncovered lines per requested C source file",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  methods: {
    report: {
      description:
        "Report the uncovered lines of the given C sources from a coverage trace",
      arguments: ReportArgsSchema,
      execute: async (
        args: ReportArgs,
        context: {
          repoDir: string;
          globalArgs: GlobalArgs;
          logger: { info: (msg: string, props?: unknown) => void };
          writeResource: (
            specName: string,
            name: string,
            data: Record<string, unknown>,
          ) => Promise<{ name: string }>;
        },
      ): Promise<{ dataHandles: Array<{ name: string }> }> => {
        const rustPaths = args.files.filter((file) =>
          file.startsWith(RUST_DIR) || file.endsWith(".rs")
        );
        if (rustPaths.length > 0) {
          throw new Error(
            `${
              rustPaths.join(", ")
            } is Rust: the coverage build instruments C with gcov and this ` +
              `trace cannot see it. Use the rust-coverage model instead.`,
          );
        }

        const { repoRoot, binDir } = context.globalArgs;
        const root = resolve(repoRoot === "." ? "" : repoRoot, context.repoDir)
          .replace(/\/$/, "");

        const infoFile = args.infoFile
          ? resolve(args.infoFile, context.repoDir)
          : `${resolve(binDir, root)}/${
            traceName(args.suite, args.deployment)
          }`;

        let stat: Deno.FileInfo;
        try {
          stat = await Deno.stat(infoFile);
        } catch {
          throw new Error(
            `No coverage trace at ${infoFile}. Run the suite with coverage ` +
              `enabled first: the build model with \`coverage\`, then the suite ` +
              `model with \`coverage\`.`,
          );
        }

        const capturedAt = (stat.mtime ?? new Date(0)).toISOString();
        const stale = args.notOlderThan !== undefined &&
          capturedAt < new Date(args.notOlderThan).toISOString();

        // Checked before parsing: a stale trace yields a complete-looking report
        // of gaps that belong to an earlier run, which is worse than no report.
        if (stale) {
          throw new Error(
            `The coverage trace at ${infoFile} was written at ${capturedAt}, ` +
              `before ${args.notOlderThan}. It belongs to an earlier run, so ` +
              `its gaps would not describe this one.`,
          );
        }

        context.logger.info("Reading {infoFile} for {count} file(s)", {
          infoFile,
          count: args.files.length,
        });

        const files = parseTrace(await Deno.readTextFile(infoFile));

        // The trace records absolute paths, so compare canonically rather than
        // by the spelling either side happened to use.
        const byCanonical = new Map<string, FileCoverage>();
        for (const [path, coverage] of files) {
          byCanonical.set(await canonical(path), coverage);
        }

        const targets = [];
        let coveredTotal = 0;
        let lineTotal = 0;

        for (const file of args.files) {
          const coverage = byCanonical.get(
            await canonical(resolve(file, root)),
          );

          if (!coverage) {
            targets.push({
              file,
              found: false,
              coveredLines: 0,
              totalLines: 0,
              percent: 0,
              uncoveredCount: 0,
              uncoveredRanges: [],
            });
            continue;
          }

          coveredTotal += coverage.coveredLines;
          lineTotal += coverage.totalLines;
          targets.push({
            file,
            found: true,
            coveredLines: coverage.coveredLines,
            totalLines: coverage.totalLines,
            percent: coverage.totalLines === 0
              ? 0
              : (coverage.coveredLines / coverage.totalLines) * 100,
            uncoveredCount: coverage.uncovered.length,
            uncoveredRanges: toRanges(coverage.uncovered),
          });
        }

        const found = targets.filter((t) => t.found).length;

        const handle = await context.writeResource("summary", "summary", {
          infoFile,
          suite: args.suite,
          deployment: args.suite === "unit" ? null : args.deployment,
          capturedAt,
          stale,
          notOlderThan: args.notOlderThan ?? null,
          filesInTrace: files.size,
          requested: args.files.length,
          found,
          targets,
          overall: {
            coveredLines: coveredTotal,
            totalLines: lineTotal,
            percent: lineTotal === 0 ? 0 : (coveredTotal / lineTotal) * 100,
          },
          executedAt: new Date().toISOString(),
        });

        if (args.requireAllFound && found < args.files.length) {
          const missing = targets.filter((t) => !t.found).map((t) => t.file);
          throw new Error(
            `No coverage data in ${infoFile} for ${missing.join(", ")}. ` +
              `The file is probably not compiled into the module, or the build ` +
              `was not instrumented. Pass requireAllFound=false to report on ` +
              `the rest anyway.`,
          );
        }

        return { dataHandles: [handle] };
      },
    },
  },
};
