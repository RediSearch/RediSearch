/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Runs the C and C++ unit test suites with `./build.sh RUN_UNIT_TESTS` and
 * records a parsed summary alongside the full test log.
 *
 * The `run` method covers all four blocks the suite is split into — C, C++, and
 * their coordinator counterparts — optionally narrowed to a single test with
 * `TEST=`. It parses the consolidated summary the runner prints at the end into
 * per-block counts plus the list of failing test names.
 *
 * The variant arguments mirror the build model's, because `build.sh` derives
 * the binary directory from them: running the suite against the sanitizer build
 * means asking for `sanitizer: address` here too. Pass `skipBuild` when a build
 * model run has already populated that directory, otherwise build.sh rebuilds
 * as needed first.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Default timeout for a suite run: 60 minutes, including any rebuild. */
const DEFAULT_TIMEOUT_MS = 60 * 60 * 1000;

const GlobalArgsSchema = z.object({
  repoRoot: z
    .string()
    .min(1)
    .default(".")
    .describe(
      "Directory holding build.sh. Relative paths resolve against the repository root.",
    ),
  buildScript: z
    .string()
    .min(1)
    .default("./build.sh")
    .describe("The build script to invoke, relative to `repoRoot`."),
  coord: z
    .enum(["oss", "rlec"])
    .default("oss")
    .describe(
      "Coordinator flavor: `oss` tests search-community, `rlec` tests search-enterprise.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const RunArgsSchema = z.object({
  test: z
    .string()
    .optional()
    .describe(
      "Restrict the run to one test (`TEST=`). Empty runs everything, which " +
        "lets a caller pass an unset filter through without special-casing " +
        "it. For C tests this is a binary " +
        "name such as `test_varint`; for C++ tests it is a ctest regex, which " +
        "for a gtest maps to `SuiteName.TestName`.",
    ),
  debug: z
    .boolean()
    .optional()
    .describe("Test the debug build (`DEBUG=1`), with assertions and symbols."),
  coverage: z
    .boolean()
    .optional()
    .describe(
      "Test the coverage-instrumented build (`COV=1`) and capture an lcov " +
        "tracefile into bin/unit.info." +
        " Requires lcov, which build.sh invokes to capture the tracefile; without it the run aborts before any test executes. A plain coverage build does not need it.",
    ),
  sanitizer: z
    .enum(["address"])
    .optional()
    .describe(
      "Test the sanitizer build (`SAN=<value>`). Leak and memory errors are " +
        "reported through the runner's memcheck summary.",
    ),
  enableAssert: z
    .boolean()
    .default(true)
    .describe(
      "Compile with `ENABLE_ASSERT=1` so debug assertions fire during the run.",
    ),
  skipBuild: z
    .boolean()
    .optional()
    .describe(
      "Assume the binary directory is already populated and skip the build " +
        "(`SKIP_BUILD=1`). Use after a build model run to avoid rebuilding.",
    ),
  verbose: z
    .boolean()
    .optional()
    .describe("Ask the runner for more detailed output (`VERBOSE=1`)."),
  extraArgs: z
    .array(z.string())
    .optional()
    .describe(
      "Additional arguments appended to the build.sh invocation. Anything " +
        "build.sh does not recognise is forwarded to CMake as `-D<arg>`.",
    ),
  timeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      `Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}).` +
        " Note that the timeout kills the script but not the commands it spawned, so a hung compiler or server can keep the run alive past it.",
    ),
  ignoreTestFailure: z
    .boolean()
    .optional()
    .describe("Record the result without failing the method when tests fail."),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default the output is mirrored to stderr as " +
        "it arrives, since a run including a rebuild takes minutes.",
    ),
});

type RunArgs = z.infer<typeof RunArgsSchema>;

const BlockSchema = z.object({
  name: z
    .string()
    .describe(
      "Test block, e.g. `C Unit Tests` or `C++ Coordinator Unit Tests`",
    ),
  status: z
    .enum(["passed", "failed", "skipped"])
    .describe(
      "Outcome of the block; skipped means its binaries were not built",
    ),
  passed: z.number().int().nonnegative().describe(
    "Tests that passed in this block",
  ),
  total: z.number().int().nonnegative().describe(
    "Tests that ran in this block",
  ),
});

const FailureSchema = z.object({
  test: z.string().describe("Failing test, e.g. `InvertedIndexTest.TestBasic`"),
  kind: z
    .enum(["assertion", "crash", "timeout"])
    .describe(
      "assertion: the test reported a failed check. crash: it died, so there " +
        "is no assertion to report. timeout: it exceeded its time limit",
    ),
  file: z
    .string()
    .nullable()
    .describe("Source file of the assertion, relative to the repository"),
  line: z.number().int().nullable().describe("Source line of the assertion"),
  detail: z
    .string()
    .describe(
      "What the assertion printed — expected versus actual, plus any message " +
        "the test streamed into it. For a crash or timeout, why it ended",
    ),
});

const SummarySchema = z.object({
  command: z.string().describe("The full command line that was executed"),
  repoRoot: z.string().describe("Resolved directory build.sh ran in"),
  flavor: z
    .string()
    .describe("Build flavor tested: release, debug, debug-cov or debug-asan"),
  coord: z.enum(["oss", "rlec"]).describe("Coordinator flavor that was tested"),
  variant: z.string().describe("Full build variant directory name"),
  binDir: z.string().describe("Directory holding the test binaries that ran"),
  logsDir: z
    .string()
    .describe("Directory holding the per-test log files, one per binary"),
  testFilter: z.string().nullable().describe("Test name filter, if any"),
  exitCode: z.number().int().describe("Exit code of build.sh"),
  status: z.enum(["passed", "failed"]).describe("Overall outcome of the run"),
  blocks: z.array(BlockSchema).describe("Per-block results"),
  testsRun: z.number().int().nullable().describe("Total tests executed"),
  passed: z.number().int().nullable().describe("Tests that passed"),
  failed: z.number().int().nullable().describe("Tests that failed"),
  failedTests: z.array(z.string()).describe("Names of failing tests"),
  failures: z
    .array(FailureSchema)
    .describe(
      "Why each test failed: one entry per failed assertion, so a test that " +
        "tripped several appears several times",
    ),
  summaryParsed: z
    .boolean()
    .describe(
      "True when the runner printed its summary and the counts are trustworthy. " +
        "False when the run ended before testing began, e.g. a compile error.",
    ),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/**
 * Matches a per-block result line from the runner's summary, e.g.
 * `  C++ Unit Tests                    FAILED (30/31 passed)`.
 * Block labels contain runs of spaces of their own (`C   Unit Tests`), so the
 * label group is lazy and the engine backtracks until the status word matches.
 */
const BLOCK_RE =
  /^ {2}(\S.*?) {2,}(?:PASSED \((\d+)\/(\d+)\)|FAILED \((\d+)\/(\d+) passed\)|\[SKIPPED\])\s*$/;

/** Matches a failing test name listed under a failed block, e.g. `    - test_gc`. */
const FAILED_TEST_RE = /^ {4}- (.+?)\s*$/;

/** Matches the runner's grand total, e.g. `  TOTAL: 42 passed, 1 failed, 43 total`. */
const TOTAL_RE = /^\s*TOTAL: (\d+) passed, (\d+) failed, (\d+) total\s*$/;

/** Marks which test the following gtest output belongs to. */
const RUN_RE = /^\[ RUN\s+\] (\S+)\s*$/;

/**
 * Header of a gtest assertion failure, e.g.
 * `/repo/tests/cpptests/test_cpp_foo.cpp:19: Failure`. The body follows on the
 * next lines.
 */
const GTEST_FAILURE_RE = /^(.+):(\d+): Failure\s*$/;

/**
 * A test that died rather than failing an assertion, e.g.
 * `ZZProbeTest.Crashes ... CRASH ()`. The runner leaves the reason empty when
 * ctest phrases it in a way its own extraction does not match.
 */
const CRASH_RE = /^(\S+) \.\.\. CRASH \((.*)\)\s*$/;

/** A test the runner gave up on, e.g. `SlowTest.Foo ... TIMEOUT`. */
const TIMEOUT_RE = /^(\S+) \.\.\. TIMEOUT\s*$/;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and trailing carriage returns from a line. */
function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
}

export interface Block {
  name: string;
  status: "passed" | "failed" | "skipped";
  passed: number;
  total: number;
}

export interface Failure {
  test: string;
  kind: "assertion" | "crash" | "timeout";
  file: string | null;
  line: number | null;
  detail: string;
}

export interface ParsedRun {
  blocks: Block[];
  failures: Failure[];
  testsRun: number | null;
  passed: number | null;
  failed: number | null;
  failedTests: string[];
}

/**
 * Parse the runner's output into structured results.
 *
 * `repoRoot` only shortens the absolute paths gtest prints; pass an empty
 * string to leave them as they are.
 */
export function parseOutput(lines: string[], repoRoot = ""): ParsedRun {
  const blocks: Block[] = [];
  const failedTests: string[] = [];
  let testsRun: number | null = null;
  let passed: number | null = null;
  let failed: number | null = null;

  const failures: Failure[] = [];
  // ctest prints a failing test's output inline, and the runner then reprints
  // the same block in its own details section, so every assertion is seen
  // twice. Key on the whole entry rather than the location alone, since one
  // test can trip several assertions on the same line across runs.
  const seen = new Set<string>();
  const prefix = repoRoot && !repoRoot.endsWith("/")
    ? `${repoRoot}/`
    : repoRoot;

  let currentTest: string | null = null;
  let pending: Failure | null = null;
  let body: string[] = [];

  /** Close the assertion being accumulated, if any, and record it once. */
  const flush = (): void => {
    if (!pending) return;
    const detail = body.join("\n").trimEnd();
    const entry = { ...pending, detail };
    const key = `${entry.test}\0${entry.file}\0${entry.line}\0${detail}`;
    if (!seen.has(key)) {
      seen.add(key);
      failures.push(entry);
    }
    pending = null;
    body = [];
  };

  for (const line of lines) {
    const run = line.match(RUN_RE);
    if (run) {
      flush();
      currentTest = run[1];
      continue;
    }

    const gtestFailure = line.match(GTEST_FAILURE_RE);
    if (gtestFailure && currentTest) {
      flush();
      const file = gtestFailure[1].startsWith(prefix) && prefix
        ? gtestFailure[1].slice(prefix.length)
        : gtestFailure[1];
      pending = {
        test: currentTest,
        kind: "assertion",
        file,
        line: Number(gtestFailure[2]),
        detail: "",
      };
      continue;
    }

    if (pending) {
      // A blank line or any gtest marker ends the body. Everything else is
      // part of the expected/actual report the assertion printed.
      if (line.trim() === "" || line.startsWith("[")) flush();
      else body.push(line);
      if (pending) continue;
    }

    const crash = line.match(CRASH_RE);
    if (crash) {
      const detail = crash[2].trim() ||
        "test crashed without reporting a reason";
      const key = `${crash[1]}\0crash`;
      if (!seen.has(key)) {
        seen.add(key);
        failures.push({
          test: crash[1],
          kind: "crash",
          file: null,
          line: null,
          detail,
        });
      }
      continue;
    }

    const timeout = line.match(TIMEOUT_RE);
    if (timeout) {
      const key = `${timeout[1]}\0timeout`;
      if (!seen.has(key)) {
        seen.add(key);
        failures.push({
          test: timeout[1],
          kind: "timeout",
          file: null,
          line: null,
          detail: "test exceeded its time limit",
        });
      }
      continue;
    }

    const total = line.match(TOTAL_RE);
    if (total) {
      passed = Number(total[1]);
      failed = Number(total[2]);
      testsRun = Number(total[3]);
      continue;
    }

    const block = line.match(BLOCK_RE);
    if (block) {
      const name = block[1].replace(/\s+/g, " ");
      if (block[2] !== undefined) {
        blocks.push({
          name,
          status: "passed",
          passed: Number(block[2]),
          total: Number(block[3]),
        });
      } else if (block[4] !== undefined) {
        blocks.push({
          name,
          status: "failed",
          passed: Number(block[4]),
          total: Number(block[5]),
        });
      } else {
        blocks.push({ name, status: "skipped", passed: 0, total: 0 });
      }
      continue;
    }

    // Failing test names are only ever printed indented under a failed block,
    // so they need no additional context to be recognised.
    const failure = line.match(FAILED_TEST_RE);
    if (failure) failedTests.push(failure[1]);
  }

  flush();

  return { blocks, failures, testsRun, passed, failed, failedTests };
}

/**
 * The last few meaningful lines of output, for a run that failed before it
 * reported anything parseable.
 *
 * Without this the caller is told only the exit code — a coverage run aborting
 * on a missing lcov reported "exited with code 127" while the reason sat unread
 * in the log.
 */
export function tailOf(lines: string[], count = 3): string {
  const meaningful = lines.filter((line) => line.trim() !== "");
  return meaningful.slice(-count).join(" | ");
}

/** Build the build.sh argument vector for a run. */
export function buildArgv(args: RunArgs, coord: GlobalArgs["coord"]): string[] {
  const argv = ["RUN_UNIT_TESTS", `COORD=${coord}`];
  if (args.debug) argv.push("DEBUG=1");
  if (args.coverage) argv.push("COV=1");
  if (args.sanitizer) argv.push(`SAN=${args.sanitizer}`);
  if (args.enableAssert) argv.push("ENABLE_ASSERT=1");
  if (args.verbose) argv.push("VERBOSE=1");
  if (args.test) argv.push(`TEST=${args.test}`);
  if (args.extraArgs) argv.push(...args.extraArgs);
  return argv;
}

/**
 * build.sh controls this model decides, which it reads from the environment.
 *
 * Setting one is only half the job: left alone, an inherited value decides for
 * a run that did not ask. `SKIP_BUILD=1` in the caller's environment would skip
 * the build for a run that wanted one, `SAN=address` would send it looking for
 * the binaries in a flavor directory this model did not select, and `TEST`
 * would filter the suite while the summary records `testFilter: null` — which
 * the failure digest reads as licence to report the resulting skipped blocks as
 * problems. build.sh initialises none of those three before use.
 *
 * Deliberately not listed: `INLINE_LSE_ATOMICS`, `BUILD_INTEL_SVS_OPT`,
 * `RUST_DYN_CRT`, `SCCACHE_PATH`. Those describe the machine rather than
 * contradict the run, and a developer who exported one meant it.
 */
const OWNED_ENV = [
  "SKIP_BUILD",
  "SAN",
  "COV",
  "TEST",
  "TEST_FILTER",
  "QUICK",
  "REDISEARCH_GENERATE_HEADERS",
  "ARCHIVE_RUST_TESTS",
  "RUN_ARCHIVED_RUST_TESTS",
  "RUST_PARTITION",
];

/**
 * Environment for the run. build.sh reads SKIP_BUILD from the environment
 * rather than its argument parser, so passing it in argv would silently become
 * a CMake define instead and the build would not be skipped.
 *
 * Every control this model owns is cleared first, so an inherited one can never
 * decide what a run did not. Empty rather than a value: build.sh reads them as
 * `${VAR:-<default>}`, and that form treats empty exactly as unset, so clearing
 * one restores build.sh's own default without naming it a second time here.
 */
export function buildEnv(args: RunArgs): Record<string, string> {
  const env: Record<string, string> = Object.fromEntries(
    OWNED_ENV.map((name) => [name, ""]),
  );
  if (args.skipBuild) env.SKIP_BUILD = "1";
  return env;
}

/**
 * Mirror build.sh's flavor cascade. Order matters: the first match wins.
 * Duplicated from the build model rather than shared, because every file under
 * extensions/models is loaded as a model in its own right.
 */
export function flavorOf(args: RunArgs): string {
  if (args.sanitizer === "address") return "debug-asan";
  if (args.debug) return "debug";
  if (args.coverage) return "debug-cov";
  return "release";
}

/** Mirror build.sh's OS and architecture normalisation. */
function platform(): { os: string; arch: string } {
  const os = Deno.build.os === "darwin" ? "macos" : Deno.build.os;
  const arch = Deno.build.arch === "x86_64" ? "x64" : Deno.build.arch;
  return { os, arch };
}

/**
 * Resolve a path against the repository, collapsing `.` and `..` segments so
 * that the default `repoRoot` of "." does not leak into every reported path.
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

/** Reject variant combinations that build.sh resolves in a surprising way. */
function validate(args: RunArgs): void {
  if (args.coverage && args.sanitizer) {
    throw new Error(
      "coverage cannot be combined with a sanitizer: the two instrumentations " +
        `conflict and the run would use the ${args.sanitizer} build directory ` +
        "while still trying to capture coverage.",
    );
  }
  if (args.coverage && args.debug) {
    throw new Error(
      "coverage cannot be combined with debug: the run would look for the " +
        "binaries in the plain `debug` directory rather than the " +
        "coverage-instrumented one. Coverage builds are already unoptimised.",
    );
  }
}

/** Model definition wrapping the RediSearch C and C++ unit test suites. */
export const model = {
  type: "@gdesmott/c-unit-tests",
  version: "2026.08.05.2",
  description:
    "Run the C and C++ unit tests with build.sh and capture a parsed summary",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed C and C++ unit test run summary",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from the unit test run",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    run: {
      description:
        "Run the unit test suites, optionally scoped to a single test and against a specific build variant",
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
        validate(args);

        const { repoRoot, buildScript, coord } = context.globalArgs;
        const cwd = resolve(context.repoDir, repoRoot);
        const script = resolve(cwd, buildScript);
        try {
          await Deno.stat(script);
        } catch {
          throw new Error(`No build script found at ${script}`);
        }

        const argv = buildArgv(args, coord);
        const command = [buildScript, ...argv].join(" ");
        const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;
        const timeoutSignal = AbortSignal.timeout(timeoutMs);

        const { os, arch } = platform();
        const flavor = flavorOf(args);
        const variant = `${os}-${arch}-${flavor}`;
        const outDir = coord === "oss"
          ? "search-community"
          : "search-enterprise";
        const binDir = `${cwd}/bin/${variant}/${outDir}`;

        context.logger.info("Running {command} in {cwd}", { command, cwd });

        // A single, stable data instance rather than one per build variant:
        // `data.latest()` in a workflow takes the instance name, so a
        // variant-derived name could only be referenced by hardcoding the
        // platform. Every record carries `variant`, so a specific variant is
        // still reachable with `swamp data query`.
        const logWriter = context.createFileWriter("log", "log");
        const lines: string[] = [];
        const startedAt = Date.now();

        const child = new Deno.Command(script, {
          args: argv,
          cwd,
          env: buildEnv(args),
          stdout: "piped",
          stderr: "piped",
          signal: AbortSignal.any([context.signal, timeoutSignal]),
        }).spawn();

        const encoder = new TextEncoder();

        /**
         * Record a line: keep it for parsing, persist it to the log file, and
         * mirror it to stderr so long runs show progress. stderr is used
         * because swamp emits its own JSON on stdout.
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
            for (const part of parts) {
              await record(part);
            }
          }
          if (buffer.length > 0) {
            await record(buffer);
          }
        };

        await Promise.all([pump(child.stdout), pump(child.stderr)]);
        const status = await child.status;

        const durationMs = Date.now() - startedAt;
        const timedOut = timeoutSignal.aborted;
        const parsed = parseOutput(lines, cwd);
        const failedRun = !status.success;

        const logHandle = await logWriter.finalize();

        // A cancelled run tested nothing, so recording a "failed" summary with
        // empty counts would misrepresent it as a test failure. Keep the
        // partial log, but write no summary.
        if (
          !timedOut && (context.signal.aborted || status.signal === "SIGINT")
        ) {
          throw new Error(
            `\`${command}\` was cancelled after ${durationMs}ms. No summary recorded.`,
          );
        }

        const summaryHandle = await context.writeResource(
          "summary",
          "summary",
          {
            command,
            repoRoot: cwd,
            flavor,
            coord,
            variant,
            binDir,
            logsDir: `${cwd}/tests/logs`,
            testFilter: args.test || null,
            exitCode: status.code,
            status: failedRun ? "failed" : "passed",
            blocks: parsed.blocks,
            testsRun: parsed.testsRun,
            passed: parsed.passed,
            failed: parsed.failed,
            failedTests: parsed.failedTests,
            failures: parsed.failures,
            summaryParsed: parsed.testsRun !== null,
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

        if (failedRun && !args.ignoreTestFailure) {
          const detail = parsed.failures.length > 0
            ? ` ${
              parsed.failures.slice(0, 3).map((f) =>
                `${f.test}${f.file ? ` (${f.file}:${f.line})` : ""}: ${
                  f.detail.split("\n")[0]
                }`
              ).join("; ")
            }`
            : parsed.failedTests.length > 0
            ? ` Failing tests: ${parsed.failedTests.join(", ")}`
            // Nothing was parsed, so the run died before testing began. The
            // tail of the output is the only clue to why.
            : ` ${tailOf(lines)}`;
          throw new Error(
            `\`${command}\` exited with code ${status.code}.${detail}`,
          );
        }

        return { dataHandles: handles };
      },
    },
  },
};
