/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Runs the end-to-end Python behavioural tests with `./build.sh RUN_PYTEST` and
 * records a parsed summary alongside the full test log.
 *
 * The `run` method drives the RLTest suite under tests/pytests against a real
 * redis-server loading the built module, optionally narrowed with `TEST=` to a
 * file, a single test, or several files. It parses RLTest's per-test results
 * and closing totals into structured counts plus the list of failing tests.
 *
 * The variant arguments mirror the build model's, because `build.sh` derives
 * the module path from them: testing the sanitizer build means asking for
 * `sanitizer: address` here too. Pass `skipBuild` when a build model run has
 * already populated that directory, otherwise build.sh rebuilds as needed
 * first.
 *
 * @module
 */
import { z } from "npm:zod@4.4.3";

/** Default timeout for a suite run: 3 hours. The full suite is long. */
const DEFAULT_TIMEOUT_MS = 3 * 60 * 60 * 1000;

const GlobalArgsSchema = z.object({
  repoRoot: z
    .string()
    .min(1)
    .default(".")
    .describe(
      "Directory holding build.sh. " +
        "Relative paths resolve against the swamp repository " +
        "directory, so a checkout whose swamp files live in a subdirectory " +
        "wants `..`.",
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
      "Restrict the run (`TEST=`). Empty runs everything, which lets a caller " +
        "pass an unset filter through without special-casing it. A file " +
        "without its extension runs that " +
        "file (`test_crash`), `file:name` runs one test " +
        "(`test_crash:test_query_thread_crash`), and a space-separated list " +
        "runs several files (`test_crash test_gc`).",
    ),
  deployment: z
    .enum(["standalone", "cluster"])
    .default("standalone")
    .describe(
      "Run against a standalone server or a coordinator cluster " +
        "(`REDIS_STANDALONE=1` or `0`).",
    ),
  shards: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      "Number of OSS coordinator shards to run against (default 3). Only " +
        "meaningful for a cluster deployment, and rejected without one. Note " +
        "that `SHARDS=n` on build.sh's command line does not work: build.sh has " +
        "no such argument, so it becomes a CMake define and runtests.sh never " +
        "sees it.",
    ),
  testTimeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      "Per-test timeout in seconds (`TEST_TIMEOUT=`, default 300). Set 20 for " +
        "a quick verification run; slow tests will be terminated.",
    ),
  quick: z
    .boolean()
    .optional()
    .describe("Run only a subset of the tests (`QUICK=1`)."),
  parallel: z
    .number()
    .int()
    .nonnegative()
    .optional()
    .describe(
      "Number of parallel test processes (`PARALLEL=`). 0 disables " +
        "parallelism, which makes interleaved failures easier to read.",
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
        "tracefile into bin/flow_<deployment>.info." +
        " Requires lcov, which build.sh invokes to capture the tracefile; without it the run aborts before any test executes. A plain coverage build does not need it.",
    ),
  sanitizer: z
    .enum(["address"])
    .optional()
    .describe(
      "Test the sanitizer build (`SAN=<value>`). Requires a redis-server built " +
        "with the same sanitizer toolchain: an ordinary one dies as it loads " +
        "the instrumented module, and every test then fails with a connection " +
        "error rather than anything describing the real cause. CI builds and " +
        "ships a matching server for this reason.",
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
    .describe("Ask RLTest for more detailed output (`VERBOSE=1`)."),
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
      `Timeout for the whole run, in milliseconds (default ${DEFAULT_TIMEOUT_MS}). ` +
        "Distinct from `testTimeout`, which bounds a single test." +
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
        "it arrives, since the suite runs for a long time.",
    ),
});

type RunArgs = z.infer<typeof RunArgsSchema>;

const FailureSchema = z.object({
  test: z.string().describe("Failing test, as `file:test`"),
  assertion: z
    .string()
    .nullable()
    .describe("The comparison that failed, e.g. `1 == 2`"),
  location: z
    .string()
    .nullable()
    .describe("Source location of the assertion, as `file.py:line`"),
  message: z
    .string()
    .nullable()
    .describe("Message the test supplied with the assertion, if any"),
  raw: z
    .string()
    .describe(
      "The detail line as printed. Carries failures with no structure to " +
        "extract, such as an unhandled exception, which point at the server logs",
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
  modulePath: z.string().describe("Module the tests loaded into redis-server"),
  logsDir: z
    .string()
    .describe(
      "Directory holding the per-test server logs, named in the runner output",
    ),
  deployment: z
    .enum(["standalone", "cluster"])
    .describe("Topology the tests ran against"),
  shards: z
    .number()
    .int()
    .nullable()
    .describe(
      "Shard count the cluster ran with, or null when unset — which means " +
        "runtests.sh's own default of 3, not one shard",
    ),
  testFilter: z.string().nullable().describe("Test name filter, if any"),
  exitCode: z.number().int().describe("Exit code of build.sh"),
  status: z.enum(["passed", "failed"]).describe("Overall outcome of the run"),
  testsRun: z.number().int().nullable().describe("Total tests executed"),
  passed: z.number().int().nullable().describe("Tests that passed"),
  failed: z.number().int().nullable().describe("Tests that failed"),
  skipped: z.number().int().nullable().describe("Tests that were skipped"),
  failedTests: z
    .array(z.string())
    .describe("Names of failing tests, as `file:test`"),
  failures: z
    .array(FailureSchema)
    .describe(
      "Why each test failed: one entry per failed assertion, so a test that " +
        "tripped several appears several times",
    ),
  summaryParsed: z
    .boolean()
    .describe(
      "True when RLTest printed its totals and the counts are trustworthy. " +
        "False when the run ended before testing began, e.g. a compile error.",
    ),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/**
 * Matches RLTest's closing totals, e.g.
 * `Total Tests Run: 10, Total Tests Failed: 0, Total Tests Passed: 10`.
 */
const TOTALS_RE =
  /Total Tests Run: (\d+), Total Tests Failed: (\d+), Total Tests Passed: (\d+)/;

/**
 * Matches the header RLTest prints before each test, e.g. `test_case:testFoo:`.
 * The result follows on the next line, so the name is carried forward.
 */
const TEST_NAME_RE = /^(\S+:\S+):\s*$/;

/** Matches the indented result line following a test header, e.g. `\t[PASS]`. */
const RESULT_RE = /^\s+\[(PASS|FAIL|SKIP|ERROR|EXCEPTION)\]/;

/**
 * Header of the section RLTest prints after the totals, listing every failing
 * test again with the assertions that failed underneath it.
 */
const FAILURE_SECTION_RE = /^Failed Tests Summary:\s*$/;

/** A test name in that section: exactly one tab of indentation. */
const FAILURE_TEST_RE = /^\t([^\t].*?)\s*$/;

/** A detail line under a test name: exactly two tabs of indentation. */
const FAILURE_DETAIL_RE = /^\t\t(.*?)\s*$/;

/**
 * Splits the tail of an assertion detail into its location and optional
 * message, e.g. `test_crash.py:7 [deliberate failure]`. A location never
 * contains spaces, so the first token is the location and any bracketed
 * remainder is the message the test supplied.
 */
const LOCATION_RE = /^(\S+)(?:\s+\[(.*)\])?$/;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and trailing carriage returns from a line. */
function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
}

export interface Failure {
  test: string;
  assertion: string | null;
  location: string | null;
  message: string | null;
  raw: string;
}

export interface ParsedRun {
  testsRun: number | null;
  passed: number | null;
  failed: number | null;
  skipped: number | null;
  failedTests: string[];
  failures: Failure[];
}

/**
 * Turn one detail line into a structured failure.
 *
 * Two shapes occur. A failed assertion is tab-separated into a marker, the
 * comparison, and a location with an optional bracketed message. Anything else
 * — notably `Exception raised during test execution. See logs` — has no
 * structure to extract, so it survives as `raw` alone and the caller falls back
 * to the server logs.
 */
export function parseFailureDetail(test: string, detail: string): Failure {
  const parts = detail.split("\t");

  if (parts.length >= 3 && parts[0].includes("(FAIL)")) {
    const tail = parts[2].match(LOCATION_RE);
    return {
      test,
      assertion: parts[1],
      location: tail ? tail[1] : parts[2],
      message: tail?.[2] ?? null,
      raw: detail,
    };
  }

  return { test, assertion: null, location: null, message: null, raw: detail };
}

/** Parse RLTest's output into structured counts and failing test names. */
export function parseOutput(lines: string[]): ParsedRun {
  const failedTests = new Set<string>();
  let testsRun: number | null = null;
  let passed: number | null = null;
  let failed: number | null = null;
  let skipped = 0;
  let currentTest: string | null = null;

  const failures: Failure[] = [];
  let inFailureSection = false;
  let failureTest: string | null = null;

  for (const line of lines) {
    // The closing failure section is indentation-structured, so it is handled
    // before anything else: its lines must not also be read as test results.
    if (FAILURE_SECTION_RE.test(line)) {
      inFailureSection = true;
      failureTest = null;
      continue;
    }

    if (inFailureSection) {
      const detail = line.match(FAILURE_DETAIL_RE);
      if (detail && failureTest) {
        failures.push(parseFailureDetail(failureTest, detail[1]));
        continue;
      }

      const test = line.match(FAILURE_TEST_RE);
      if (test) {
        failureTest = test[1];
        continue;
      }

      // Anything unindented ends the section — it is back to ordinary output.
      inFailureSection = false;
      failureTest = null;
    }

    const totals = line.match(TOTALS_RE);
    if (totals) {
      testsRun = Number(totals[1]);
      failed = Number(totals[2]);
      passed = Number(totals[3]);
      continue;
    }

    const name = line.match(TEST_NAME_RE);
    if (name) {
      currentTest = name[1];
      continue;
    }

    const result = line.match(RESULT_RE);
    if (result) {
      if (result[1] === "SKIP") skipped += 1;
      else if (result[1] !== "PASS" && currentTest) {
        failedTests.add(currentTest);
      }
      currentTest = null;
    }
  }

  // The failure section names every failing test, so it also covers the case
  // where a test died without ever printing its own result line.
  for (const failure of failures) failedTests.add(failure.test);

  return {
    testsRun,
    passed,
    // RLTest's totals omit skips, so they are counted from the result lines.
    failed: failed ?? (failedTests.size > 0 ? failedTests.size : null),
    skipped: testsRun === null && skipped === 0 ? null : skipped,
    failedTests: [...failedTests],
    failures,
  };
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
  const argv = ["RUN_PYTEST", `COORD=${coord}`];
  if (args.debug) argv.push("DEBUG=1");
  if (args.coverage) argv.push("COV=1");
  if (args.sanitizer) argv.push(`SAN=${args.sanitizer}`);
  if (args.enableAssert) argv.push("ENABLE_ASSERT=1");
  if (args.verbose) argv.push("VERBOSE=1");
  argv.push(`REDIS_STANDALONE=${args.deployment === "standalone" ? 1 : 0}`);
  if (args.testTimeout !== undefined) {
    argv.push(`TEST_TIMEOUT=${args.testTimeout}`);
  }
  if (args.quick) argv.push("QUICK=1");
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
 * the module in a flavor directory this model did not select, and `TEST` would
 * narrow the suite to one file while the summary reports the whole of it. Those
 * three build.sh never initialises before use, so an exported value survives
 * untouched.
 *
 * `SA` is the subtle one: the topology reaches build.sh as `REDIS_STANDALONE`,
 * but runtests.sh reads `SA`, which build.sh defaults from `REDIS_STANDALONE`
 * only when it is unset. Inherited, it decides the topology on its own and the
 * summary records the other one.
 *
 * The four alongside `TEST` in runtests.sh's own "Test selection" block are the
 * same trap it is, and are owned for the same reason: `TESTFILE` and
 * `FAILEDFILE` reach RLTest as `-f` and `-F`, `LIST` turns the run into
 * `--collect-only`, and `RLTEST_ARGS` and `RLTEST_TEST_ARGS` are both appended
 * to what RLTest is given, so `--test` can be smuggled in through either. The
 * second is the one that block builds `TEST` into, and like the first it is
 * only ever appended to, never initialised. None appears in the summary — this
 * model reports the filter it was given, so a suite narrowed by any of them is
 * recorded as having run in full.
 *
 * `ENV_ONLY`, `HELP` and `NOP` each stop the suite from running at all —
 * respectively by starting an environment and exiting, printing the usage and
 * exiting, and turning every command into an `echo`. None is assigned or
 * defaulted anywhere in runtests.sh, so an exported one is read as an
 * instruction the caller never gave, and the run this model reports on never
 * happened. They are the only members of this list whose failure is loud: a run
 * that tested nothing parses no summary and fails. That is still a run failing
 * on the state of somebody's shell rather than on the code, which is what
 * owning the environment is for.
 *
 * `CLEAR_LOGS` at 0 keeps the previous run's logs, and the summary points at
 * that directory for anything reading the failures afterwards — so a passing
 * run can be read alongside an earlier run's output.
 *
 * `VERBOSE` and `REJSON_MODULE` are here because this model already speaks for
 * both: `verbose` is a typed field, and `REJSON_MODULE` names the RedisJSON
 * build the xredis path loads, which is the hazard `REJSON_PATH` is owned for
 * one branch along.
 *
 * The RLTest configuration file is assembled from several variables that
 * runtests.sh only ever reads or assigns conditionally, and each is a way into
 * the same file: `RLTEST_COV_ARGS` is never assigned at all, `RLTEST_VG_ARGS`
 * neither, `RLTEST_SAN_ARGS` and `RLTEST_PARALLEL_ARG` only on branches this
 * model may not take, and `CONFIG_FILE` names a file whose contents are
 * appended wholesale. Anything RLTest accepts can arrive through any of them,
 * `--test` included, so an inherited one narrows the suite exactly as `TEST`
 * would and with as little trace in the summary.
 *
 * `NO_LIBEXT` decides whether the extension-loading test runs at all.
 * `includes.py` reads it straight from the environment and `test_ext.py` skips
 * on it, so an inherited `NO_LIBEXT=1` drops that coverage while the suite still
 * reports a positive total and this model still records an unfiltered run.
 *
 * The `REJSON` family decides whether the suite covers JSON at all. `REJSON=0`
 * skips loading RedisJSON, and the tests marked `no_json` are then skipped
 * rather than failed — RLTest still reports a positive total, this model still
 * records an unfiltered passing run, and a gate reading it vouches for JSON
 * behaviour nothing exercised. `REJSON_PATH` is the same hazard aimed
 * differently: it names the module to load, so the suite would run against
 * whichever RedisJSON build that path holds. Cleared rather than pinned, since
 * build.sh reads the two that have defaults as `${VAR:-<default>}` and the
 * other two mean "work it out" when empty.
 *
 * `RLEC` is the one that changes what was tested rather than how it ran.
 * runtests.sh reads it as `${RLEC:-0}`, and at 1 it takes the external
 * enterprise path — clearing `REDIS_STANDALONE` on the way, so the topology
 * this model asked for is discarded too — and runs the suite against a database
 * that is already up. The module exercised is then whichever one that database
 * loaded, while the summary reports the `modulePath` this run built: the same
 * failure `EXT` is pinned against in {@linkcode PINNED_ENV}, reached by a
 * different variable.
 *
 * Deliberately not listed: `INLINE_LSE_ATOMICS`, `BUILD_INTEL_SVS_OPT`,
 * `RUST_DYN_CRT` and `SCCACHE_PATH`, which describe the machine rather than
 * contradict the run. From runtests.sh, the same judgement leaves `TMPDIR`,
 * `LOG`, `RLTEST_DEBUG`, `GITHUB_ACTIONS`, `KEEP`, `RANDPORTS`, `REDIS_PORT`,
 * `UNIX` and `VG_OPTIONS`: each says where to work, how much to say, or how to
 * connect, and none decides which tests ran or which module they ran against.
 * `BASH_SOURCE`, `FULL_VARIANT` and `JSON_BIN_PATH` are not the caller's to set
 * — the shell and the scripts runtests.sh sources assign them.
 *
 * The `EXT` family is owned too, but through {@linkcode PINNED_ENV} rather than
 * here: it cannot be cleared. build.sh reads it as `${VAR-<default>}`, and that
 * form — unlike `${VAR:-<default>}` — takes an empty string as a real value, so
 * an emptied `EXT_HOST` would reach runtests.sh as a host of "".
 */
const OWNED_ENV = [
  "SKIP_BUILD",
  "SAN",
  "COV",
  "TEST",
  "TEST_FILTER",
  "TESTFILE",
  "FAILEDFILE",
  "LIST",
  "RLTEST_ARGS",
  "RLTEST_TEST_ARGS",
  "RLTEST_COV_ARGS",
  "RLTEST_VG_ARGS",
  "RLTEST_SAN_ARGS",
  "RLTEST_PARALLEL_ARG",
  "CONFIG_FILE",
  "ENV_ONLY",
  "HELP",
  "NOP",
  "CLEAR_LOGS",
  "VERBOSE",
  "REJSON_MODULE",
  "TEST_TIMEOUT",
  "QUICK",
  "REDIS_STANDALONE",
  "SA",
  "RLEC",
  "REJSON",
  "REJSON_BRANCH",
  "REJSON_PATH",
  "REJSON_ARGS",
  "NO_LIBEXT",
  "SHARDS",
  "PARALLEL",
  "REDISEARCH_GENERATE_HEADERS",
  "ARCHIVE_RUST_TESTS",
  "RUN_ARCHIVED_RUST_TESTS",
  "RUST_PARTITION",
];

/**
 * Controls this model owns that have to be set to a value rather than cleared,
 * given here as build.sh's own defaults.
 *
 * `EXT` decides what the suite is run against. `EXT=1` means "test whatever is
 * already listening on EXT_HOST:EXT_PORT", which is the one inherited value
 * that changes what a run measured rather than how it ran: the module under
 * test would be whichever one that server loaded, while the summary reports the
 * `modulePath` this run built. A validation gate reading that summary would be
 * vouching for a build nothing had exercised.
 *
 * Pinned to the default instead of cleared because build.sh reads these as
 * `${VAR-<default>}`, where an empty string is a value and not an absence.
 * `RUN` is build.sh's own default and is not runtests.sh's `run` — the
 * comparison there is case-sensitive, so this is the inert value, which is the
 * point: it selects the ordinary path of starting a server for the tests.
 */
const PINNED_ENV: Record<string, string> = {
  EXT: "RUN",
  EXT_HOST: "127.0.0.1",
  EXT_PORT: "6379",
};

/**
 * Environment for the run. build.sh reads SKIP_BUILD and PARALLEL from the
 * environment rather than its argument parser, so passing them in argv would
 * silently become CMake defines instead.
 *
 * SHARDS is the same trap one level down: build.sh does not know the name
 * either, and it is runtests.sh that reads it from the environment, defaulting
 * to 3. Passed in argv it would become `-DSHARDS=n` and the shard count would
 * quietly stay at the default.
 *
 * Every control this model owns is neutralised first, so an inherited one can
 * never decide what a run did not. Most are cleared rather than set: build.sh
 * reads them as `${VAR:-<default>}`, and that form treats empty exactly as
 * unset, so clearing one restores build.sh's own default without naming it a
 * second time here. The exceptions are in {@linkcode PINNED_ENV}, which are
 * read in the form where empty is a value and so have to be named.
 */
export function buildEnv(args: RunArgs): Record<string, string> {
  const env: Record<string, string> = {
    ...Object.fromEntries(OWNED_ENV.map((name) => [name, ""])),
    ...PINNED_ENV,
  };
  if (args.skipBuild) env.SKIP_BUILD = "1";
  if (args.parallel !== undefined) env.PARALLEL = String(args.parallel);
  if (args.shards !== undefined) env.SHARDS = String(args.shards);
  return env;
}

/**
 * Mirror build.sh's flavor cascade. Order matters: the first match wins.
 * Duplicated from the build model rather than shared, because every file under
 * swamp/extensions/models is loaded as a model in its own right.
 */
function flavorOf(args: RunArgs): string {
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
/**
 * build.sh switches this model has a typed field for, and the field to use.
 *
 * The same split the build model guards against, one level along. `extraArgs`
 * is appended to the argument vector, but the summary — the flavor, the module
 * path, the deployment, and the `testFilter` a gate reads to decide whether the
 * suite ran in full — is derived from the typed fields alone.
 * `extraArgs: ["TEST=x"]` with `test` unset runs one test and records the run
 * as unfiltered; `["REDIS_STANDALONE=1"]` runs standalone while the summary
 * says cluster, which is the whole of what the cluster gate reads. Anything
 * build.sh forwards to CMake, and any switch the summary does not speak for, is
 * what `extraArgs` is for and is left alone.
 *
 * `SKIP_BUILD`, `SHARDS` and `PARALLEL` are here as well as in
 * {@linkcode OWNED_ENV}: build.sh reads them from the environment rather than
 * its argument parser, so through `extraArgs` they would not even take effect —
 * they would fall through to CMake as defines, and the run would quietly use
 * the default while the caller believed otherwise.
 */
const MODELLED_SWITCHES: Record<string, string> = {
  COORD: "the `coord` global argument",
  DEBUG: "`debug`",
  COV: "`coverage`",
  SAN: "`sanitizer`",
  ENABLE_ASSERT: "`enableAssert`",
  VERBOSE: "`verbose`",
  REDIS_STANDALONE: "`deployment`",
  TEST_TIMEOUT: "`testTimeout`",
  QUICK: "`quick`",
  TEST: "`test`",
  SKIP_BUILD: "`skipBuild`",
  SHARDS: "`shards`",
  PARALLEL: "`parallel`",
};

/**
 * The switch an `extraArgs` entry sets, i.e. `TEST` from `TEST=x`.
 *
 * Upper-cased, because build.sh is: `parse_arguments` folds every argument to
 * upper case before matching it, so `test=x` reaches the same branch `TEST=x`
 * does. A case-sensitive lookup here would let the lower-cased spelling through
 * a check the upper-cased one fails, which is worse than no check at all — it
 * reads as a rule that holds.
 */
function switchName(arg: string): string {
  const equals = arg.indexOf("=");
  return (equals === -1 ? arg : arg.slice(0, equals)).toUpperCase();
}

function validate(args: RunArgs): void {
  if (args.coverage && args.sanitizer) {
    throw new Error(
      "coverage cannot be combined with a sanitizer: the two instrumentations " +
        `conflict and the run would load the module from the ${args.sanitizer} ` +
        "build directory while still trying to capture coverage.",
    );
  }
  if (args.coverage && args.debug) {
    throw new Error(
      "coverage cannot be combined with debug: the run would load the module " +
        "from the plain `debug` directory rather than the coverage-instrumented " +
        "one. Coverage builds are already unoptimised.",
    );
  }
  if (args.shards !== undefined && args.deployment === "standalone") {
    throw new Error(
      "shards only applies to a cluster deployment: runtests.sh reads it when " +
        "starting an oss-cluster environment, and a standalone run would " +
        "silently ignore it.",
    );
  }
  for (const arg of args.extraArgs ?? []) {
    const owned = MODELLED_SWITCHES[switchName(arg)];
    if (owned) {
      throw new Error(
        `extraArgs sets ${switchName(arg)}, which this model derives its ` +
          `summary from: pass ${owned} instead. Set through extraArgs it would ` +
          `change what the run did without changing what is recorded for it.`,
      );
    }
  }
}

/**
 * Most output lines kept in memory for parsing.
 *
 * Everything is streamed to the log file as it arrives, so this bounds only
 * what is parsed at the end. A test or a server that spins printing until the
 * timeout would otherwise be retained in full, and the process dies of memory
 * exhaustion before the timeout it was heading for can record a diagnostic —
 * the one failure that leaves nothing behind to read.
 *
 * The tail is what is kept, because that is where a run's totals are, and a
 * marker replaces what was dropped so that output built from this cannot read
 * as a whole run.
 */
export const MAX_RETAINED_LINES = 200_000;

/**
 * Keep `line` for parsing, dropping the oldest half once the cap is reached.
 *
 * Half at a time rather than one line at a time: dropping the front of an array
 * this long moves every element that follows it, so a line-for-line window
 * would cost a copy of the whole buffer per line of output — which is the same
 * runaway suite paid for differently. Halving amortises it to a copy per
 * hundred thousand lines, and keeping between half the cap and the cap is
 * bounded, which is the whole requirement.
 */
export function retain(lines: string[], line: string): void {
  if (lines.length >= MAX_RETAINED_LINES) {
    lines.splice(0, Math.floor(MAX_RETAINED_LINES / 2));
    if (lines[0] !== TRUNCATION_MARKER) lines.unshift(TRUNCATION_MARKER);
  }
  lines.push(line);
}

/** Stands in the retained output for the lines the cap dropped. */
export const TRUNCATION_MARKER =
  "[swamp] earlier output dropped: only the tail is parsed";

/** Model definition wrapping the RediSearch Python behavioural test suite. */
export const model = {
  type: "@gdesmott/pytest",
  version: "2026.08.17.2",
  description:
    "Run the end-to-end Python tests with build.sh and capture a parsed summary",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed Python behavioural test run summary",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from the Python test run",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    run: {
      description:
        "Run the Python tests, optionally scoped to a file or test and against a specific build variant",
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
        // One signal object, because the same abort has to reach two places:
        // the process, and the readers draining what it left behind.
        const abort = AbortSignal.any([context.signal, timeoutSignal]);

        const { os, arch } = platform();
        const flavor = flavorOf(args);
        const variant = `${os}-${arch}-${flavor}`;
        const outDir = coord === "oss"
          ? "search-community"
          : "search-enterprise";
        const moduleName = coord === "oss"
          ? "redisearch.so"
          : "module-enterprise.so";
        const modulePath = `${cwd}/bin/${variant}/${outDir}/${moduleName}`;

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
          signal: abort,
        }).spawn();

        const encoder = new TextEncoder();

        /**
         * Record a line: keep it for parsing, persist it to the log file, and
         * mirror it to stderr so long runs show progress. stderr is used
         * because swamp emits its own JSON on stdout.
         */
        const record = async (raw: string): Promise<void> => {
          const line = clean(raw);
          retain(lines, line);
          await logWriter.writeLine(line);
          if (!args.quiet) {
            await Deno.stderr.write(encoder.encode(`${line}\n`));
          }
        };

        /**
         * Decode a stream, splitting it into lines, and give up on it once the run has
         * been aborted.
         *
         * The abort reaches the process this model started and nothing else: the
         * compilers, test servers and helpers it spawned in turn inherited this pipe
         * and were signalled by no one. Draining until the pipe closes therefore waits
         * for whichever of them lives longest, so a run with a hung grandchild outlives
         * the timeout that was meant to end it — and the timeout is only looked at
         * after this returns. Cancelling costs whatever those processes had not written
         * yet, which is a fair price for a run that is over either way.
         */
        const pump = async (
          stream: ReadableStream<Uint8Array>,
        ): Promise<void> => {
          const reader = stream.getReader();
          const stopDraining = () => {
            reader.cancel().catch(() => {});
          };
          if (abort.aborted) stopDraining();
          else abort.addEventListener("abort", stopDraining, { once: true });
          const decoder = new TextDecoder();
          let buffer = "";
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              buffer += decoder.decode(value, { stream: true });
              const parts = buffer.split("\n");
              buffer = parts.pop() ?? "";
              for (const part of parts) await record(part);
            }
          } finally {
            abort.removeEventListener("abort", stopDraining);
          }
          if (buffer.length > 0) await record(buffer);
        };

        await Promise.all([pump(child.stdout), pump(child.stderr)]);
        const status = await child.status;

        const durationMs = Date.now() - startedAt;
        const timedOut = timeoutSignal.aborted;
        const parsed = parseOutput(lines);
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
            modulePath,
            logsDir: `${cwd}/tests/pytests/logs`,
            deployment: args.deployment,
            shards: args.shards ?? null,
            testFilter: args.test || null,
            exitCode: status.code,
            status: failedRun ? "failed" : "passed",
            testsRun: parsed.testsRun,
            passed: parsed.passed,
            failed: parsed.failed,
            skipped: parsed.skipped,
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

        // `ignoreTestFailure` forgives tests that failed; it cannot forgive a
        // suite that never ran. A run that died in compilation or setup parses
        // no summary at all, and returning success for it would record
        // `summaryParsed: false` beside a passing method — an absence read as a
        // green suite by every gate downstream.
        if (
          failedRun && !(args.ignoreTestFailure && parsed.testsRun !== null)
        ) {
          const detail = parsed.failures.length > 0
            ? ` ${
              parsed.failures.slice(0, 3).map((f) =>
                `${f.test}: ${f.assertion ?? f.raw}${
                  f.location ? ` (${f.location})` : ""
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
