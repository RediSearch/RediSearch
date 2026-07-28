/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Runs a Rust test suite with `cargo nextest` and records a parsed summary
 * alongside the full test log.
 *
 * The `run` method wraps `cargo nextest run`, optionally scoped to a single
 * crate (`-p <crate>`) and/or a nextest filter expression. It streams the
 * combined stdout/stderr into a log file and parses nextest's summary line
 * into structured counts plus the list of failing test names.
 *
 * Passing `miri` runs the suite under the interpreter instead, as
 * `cargo +<nightly> miri nextest run`. Miri reports undefined behaviour as a
 * diagnostic rather than a panic, so those are parsed too and land in the same
 * `failures` list, distinguished by their `kind`.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Default timeout for a test run: 30 minutes. */
const DEFAULT_TIMEOUT_MS = 30 * 60 * 1000;

/**
 * Default timeout for a miri run: 4 hours. Interpreting the suite is orders of
 * magnitude slower than running it natively, and a whole-workspace miri run
 * comfortably outlives the native timeout. CI only keeps it tolerable by
 * sharding the run three ways.
 */
const DEFAULT_MIRI_TIMEOUT_MS = 4 * 60 * 60 * 1000;

/**
 * File holding the nightly toolchain the project pins for miri, relative to the
 * repository root. Reading it keeps a local run on the same toolchain as CI,
 * which matters because miri's diagnostics change between nightlies.
 */
const NIGHTLY_FILE = ".rust-nightly";

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
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const RunArgsSchema = z.object({
  crate: z
    .string()
    .optional()
    .describe(
      "Restrict the run to a single workspace crate (cargo `-p`). Empty runs " +
        "the whole workspace, which lets a caller pass an unset scope through " +
        "without special-casing it.",
    ),
  test: z
    .string()
    .optional()
    .describe(
      "nextest filter, matched against test names (positional filter). Empty " +
        "runs every test, for the same reason.",
    ),
  extraArgs: z
    .array(z.string())
    .optional()
    .describe("Additional arguments appended to the nextest invocation."),
  noFailFast: z
    .boolean()
    .optional()
    .describe("Run every test even after the first failure."),
  miri: z
    .boolean()
    .optional()
    .describe(
      "Interpret the tests under miri to catch undefined behaviour, as " +
        "`cargo miri nextest run`. Far slower than a native run, so scope it " +
        "with `crate` or `test` while iterating. Tests that cannot run under " +
        "the interpreter, such as those calling into C, are expected to be " +
        "excluded with `#[cfg(not(miri))]` in the source.",
    ),
  toolchain: z
    .string()
    .optional()
    .describe(
      `Toolchain to run cargo with, without the leading "+". Empty defaults to ` +
        `the nightly pinned in ${NIGHTLY_FILE} under miri, and to the ` +
        `repository's default toolchain otherwise, so a caller can pass an unset ` +
        `choice through without special-casing it. Miri needs a nightly, so ` +
        `override this only when the pinned one lacks the miri component.`,
    ),
  miriFlags: z
    .array(z.string())
    .optional()
    .describe(
      "Value for MIRIFLAGS, e.g. `-Zmiri-ignore-leaks` or " +
        "`-Zmiri-strict-provenance`. Replaces any inherited MIRIFLAGS rather " +
        "than adding to it, so pass every flag the run needs. A miri run with " +
        "none given clears the variable instead of inheriting it: an " +
        "interpreter flag left in the caller's shell would otherwise decide " +
        "what the run checks, without appearing anywhere in the summary.",
    ),
  cargoProfile: z
    .string()
    .optional()
    .describe(
      "Cargo profile to build the test binaries with, as nextest's " +
        "`--cargo-profile`. Empty leaves nextest on its own default, which " +
        "is not the profile a preceding build used unless that build was a " +
        "debug one — so a release run would rebuild the workspace and test " +
        "artifacts other than the ones just built. Pass the rustProfile the " +
        "build reported.",
    ),
  binDir: z
    .string()
    .optional()
    .describe(
      "Directory holding the compiled C static libraries, exported as " +
        "BINDIR. The crates that link against C read it to find them, and " +
        "without it they fall back to the conventional release layout — so a " +
        "run following a debug build links the wrong archive, or none at all " +
        "on a checkout that was never built release. Pass the binDir the " +
        "build reported. Empty leaves BINDIR untouched, so a caller can pass " +
        "an unset one through without special-casing it.",
    ),
  timeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      `Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}, or ` +
        `${DEFAULT_MIRI_TIMEOUT_MS} under miri).`,
    ),
  ignoreTestFailure: z
    .boolean()
    .optional()
    .describe("Record the result without failing the method when tests fail."),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default cargo's output is mirrored to stderr " +
        "as it arrives, since a cold workspace build can run for minutes.",
    ),
});

type RunArgs = z.infer<typeof RunArgsSchema>;

const FailureSchema = z.object({
  test: z.string().describe("Failing test, as `<binary> <test path>`"),
  kind: z
    .enum([
      "panic",
      "signal",
      "timeout",
      "leak",
      "undefined-behavior",
      "unsupported",
      "failure",
    ])
    .describe(
      "panic: the test panicked, so a location and message are available. " +
        "signal: it was killed. timeout: it ran too long. leak: it leaked. " +
        "undefined-behavior: miri caught the test doing something the language " +
        "does not define, which is a real bug even if the native run passes. " +
        "unsupported: miri cannot interpret an operation the test performs, so " +
        "the test says nothing either way and belongs behind #[cfg(not(miri))]. " +
        "failure: it failed without panicking, e.g. by returning an error",
    ),
  file: z
    .string()
    .nullable()
    .describe(
      "Source file the panic or miri diagnostic came from, relative to the " +
        "repository",
    ),
  line: z.number().int().nullable().describe("Source line of the failure"),
  column: z.number().int().nullable().describe("Source column of the failure"),
  detail: z
    .string()
    .describe(
      "The panic message, including the left/right values an assertion " +
        "reported. For a miri diagnostic, the category and what it caught. " +
        "For a test that did not panic, why it ended",
    ),
});

const SummarySchema = z.object({
  command: z.string().describe("The full command line that was executed"),
  workingDir: z.string().describe("Resolved directory cargo ran in"),
  crate: z.string().nullable().describe("Crate the run was scoped to, if any"),
  testFilter: z.string().nullable().describe("Test name filter, if any"),
  miri: z
    .boolean()
    .describe("Whether the tests were interpreted under miri"),
  toolchain: z
    .string()
    .nullable()
    .describe(
      "Toolchain cargo was invoked with, or null for the repository default",
    ),
  exitCode: z.number().int().describe("Exit code of cargo nextest"),
  status: z
    .enum(["passed", "failed"])
    .describe("Overall outcome of the test run"),
  testsRun: z.number().int().nullable().describe("Total tests executed"),
  passed: z.number().int().nullable().describe("Tests that passed"),
  failed: z.number().int().nullable().describe("Tests that failed"),
  skipped: z.number().int().nullable().describe("Tests that were skipped"),
  failedTests: z
    .array(z.string())
    .describe("Fully qualified names of failing tests"),
  failures: z
    .array(FailureSchema)
    .describe("Why each test failed: one entry per failing test"),
  summaryParsed: z
    .boolean()
    .describe(
      "True when nextest printed a summary line and the counts are trustworthy. " +
        "False when the run ended before testing began (e.g. a compile error).",
    ),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/**
 * Matches nextest's final `Summary [ 0.035s] 24 tests run: ...` line.
 *
 * The noun is pluralised by count, so a run narrowed to one test ends
 * `1 test run:` instead. Accepting only the plural made a perfectly good
 * single-test run parse as no summary at all — and since `rust-quick` asserts
 * that tests actually ran, it failed the run whose test had just passed. That is
 * the shape of the loop this workflow exists for, so it is the case that matters
 * most, not an edge one.
 */
const SUMMARY_RE = /Summary \[\s*[\d.]+s\]\s+(\d+) tests? run:\s*(.*)$/;

/**
 * Matches a per-test failure line, e.g.
 * `        FAIL [   0.005s] ( 3/24) varint::varint test_u32`.
 * The optional `TRY <n> ` prefix appears when nextest retries a test.
 */
const FAILURE_RE =
  /^\s*(?:TRY \d+ )?(FAIL|SIGSEGV|SIGABRT|SIGTERM|ABORT|TIMEOUT|LEAK-FAIL)\s+\[[^\]]*\]\s+(?:\(\s*\d+\/\d+\)\s+)?(\S+)\s+(\S.*?)\s*$/;

/**
 * Matches the header of a panic report, e.g.
 * `thread 'varint::test_u32' (3675368) panicked at build_utils/src/lib.rs:302:9:`.
 * The thread id is absent on older toolchains. The message follows on the next
 * lines.
 */
const PANIC_RE =
  /^\s*thread '([^']*)'(?: \(\d+\))? panicked at (.+):(\d+):(\d+):\s*$/;

/**
 * Matches the header of a miri diagnostic, e.g.
 * `error: Undefined Behavior: memory access failed: attempting to access 1 byte`.
 * Miri reports what it catches as a rustc-style diagnostic rather than a panic,
 * so nothing in the panic path picks these up.
 *
 * The categories are listed rather than matching any `error:` line because miri
 * also prints `error: aborting due to 1 previous error` afterwards, and a
 * compile error would otherwise be attributed to whichever test ran last.
 */
const MIRI_ERROR_RE =
  /^\s*error: (Undefined Behavior|memory leaked|unsupported operation|deadlock|abnormal termination)\s*:\s*(.*)$/;

/**
 * Matches the source location under a rustc-style diagnostic, e.g.
 * `  --> trie_rs/src/lib.rs:12:23`.
 */
const MIRI_LOCATION_RE = /^\s*-->\s+(.+?):(\d+):(\d+)\s*$/;

/** Miri's category names, mapped onto the kind of failure each represents. */
const MIRI_KINDS: Record<string, Failure["kind"]> = {
  "Undefined Behavior": "undefined-behavior",
  "memory leaked": "leak",
  "unsupported operation": "unsupported",
  deadlock: "failure",
  "abnormal termination": "failure",
};

/**
 * Matches cargo's complaint that miri is not installed for the toolchain, which
 * exits before any test runs and so leaves no summary to explain it.
 */
const MIRI_MISSING_RE = /'cargo-miri' is not installed for the toolchain/;

/** The hint rustc appends after a panic message, which ends the message. */
const BACKTRACE_NOTE_RE = /^\s*note: run with `RUST_BACKTRACE=/;

/**
 * Any nextest or libtest marker that ends a panic message, for the case where
 * the backtrace note is absent because RUST_BACKTRACE was already set.
 *
 * The libtest forms are matched in full rather than by their first word. A
 * panic message is arbitrary text, and `panic!("test failed: ...")` is ordinary
 * phrasing — matched on the prefix alone it would end the capture on its own
 * first line, and the failure would be reported as one with no message at all.
 * So `test` has to be followed by libtest's own shape, and `running` by the
 * count it always carries.
 */
const MARKER_RE = new RegExp(
  "^\\s*(?:" +
    "(?:PASS|FAIL|FLAKY|SIGSEGV|SIGABRT|SIGTERM|ABORT|TIMEOUT|LEAK-FAIL|TRY|Summary|Starting)\\b" +
    "|stdout|stderr" +
    // `running 4 tests`
    "|running\\s+\\d+\\s+tests?\\b" +
    // `test result: FAILED. 3 passed; 1 failed; ...`
    "|test\\s+result:" +
    // `test tests::one ... ok`
    "|test\\s+\\S+\\s+\\.\\.\\." +
    // The header above libtest's list of failing tests.
    "|failures:" +
    ")",
);

/**
 * Matches nextest's `FLAKY 2/3 [ 0.021s] qint tests::one` line.
 *
 * A retried test reports every attempt as it happens, so a run with retries
 * enabled prints `TRY 1 FAIL` for an attempt that later succeeded. nextest
 * counts such a test as passed — the run exits 0 and the summary reads
 * `1 passed (1 flaky)` — but the failed attempt has already been recorded by
 * then. This line is how nextest says the test ultimately passed, and it is what
 * takes the earlier attempt back out.
 */
const FLAKY_RE =
  /^\s*FLAKY\s+\d+\/\d+\s+\[[^\]]*\]\s+(?:\(\s*\d+\/\d+\)\s+)?(\S+)\s+(\S.*?)\s*$/;

/** Depth nextest indents a test's captured output by. */
const OUTPUT_INDENT = 4;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and trailing carriage returns from a line. */
function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
}

/** Extract a named count (e.g. `12 passed`) from nextest's summary tail. */
function count(tail: string, label: string): number | null {
  const match = tail.match(new RegExp(`(\\d+) ${label}`));
  return match ? Number(match[1]) : null;
}

export interface Failure {
  test: string;
  kind:
    | "panic"
    | "signal"
    | "timeout"
    | "leak"
    | "undefined-behavior"
    | "unsupported"
    | "failure";
  file: string | null;
  line: number | null;
  column: number | null;
  detail: string;
}

interface ParsedRun {
  testsRun: number | null;
  passed: number | null;
  failed: number | null;
  skipped: number | null;
  failedTests: string[];
  failures: Failure[];
}

/** Map a nextest result marker onto the kind of failure it represents. */
function kindOf(marker: string): Failure["kind"] {
  if (marker === "TIMEOUT") return "timeout";
  if (marker === "LEAK-FAIL") return "leak";
  if (marker === "FAIL") return "failure";
  return "signal";
}

/**
 * Rebase a path a diagnostic reported onto the repository.
 *
 * Both panics and miri report paths relative to the Cargo workspace, so the
 * prefix makes them resolvable from the repository root. Absolute paths, and
 * paths reported when there is no prefix, are left alone.
 */
function resolvePath(file: string, pathPrefix: string): string {
  return file.startsWith("/") || !pathPrefix ? file : `${pathPrefix}/${file}`;
}

/** Remove nextest's output indentation, keeping the panic message's own. */
function dedent(line: string): string {
  return line.startsWith(" ".repeat(OUTPUT_INDENT))
    ? line.slice(OUTPUT_INDENT)
    : line.trimStart();
}

/**
 * Parse nextest's human-readable output into structured counts and failures.
 *
 * `pathPrefix` is prepended to the relative paths panics and miri diagnostics
 * report, which are relative to the Cargo workspace rather than the repository.
 * Pass an empty string to leave them alone.
 */
export function parseOutput(lines: string[], pathPrefix = ""): ParsedRun {
  const failedTests = new Set<string>();
  let testsRun: number | null = null;
  let passed: number | null = null;
  let failed: number | null = null;
  let skipped: number | null = null;

  // Keyed by test, because nextest lists every failure twice: once as it
  // happens and again in the recap under the summary line. The first mention
  // is the one followed by the panic output.
  const failures = new Map<string, Failure>();
  let current: Failure | null = null;
  let capturing = false;
  let detail: string[] = [];
  // Tests whose failure a miri diagnostic already explained. A test gets one
  // diagnostic, but miri prints further locations under it — the allocation
  // site of a leak, for instance — and those must not overwrite it.
  const diagnosed = new Set<string>();
  let awaitingLocation = false;

  /**
   * Attach the accumulated panic message to the failure it belongs to, and end
   * the diagnostic context, so that a location left unclaimed by one failure is
   * never picked up by the next.
   */
  const flush = (): void => {
    if (current && capturing) {
      current.detail = detail.join("\n").trimEnd();
    }
    capturing = false;
    awaitingLocation = false;
    detail = [];
  };

  for (const line of lines) {
    const panic = line.match(PANIC_RE);
    if (panic && current) {
      flush();
      current.kind = "panic";
      current.file = resolvePath(panic[2], pathPrefix);
      current.line = Number(panic[3]);
      current.column = Number(panic[4]);
      capturing = true;
      continue;
    }

    // A test that panicked has already said where it failed, and miri prints
    // its own diagnostics after an unwind — a leak from the abandoned
    // allocations, say. The panic is the proximate failure, so it wins.
    const miri = line.match(MIRI_ERROR_RE);
    if (
      miri && current && current.kind !== "panic" &&
      !diagnosed.has(current.test)
    ) {
      flush();
      const [, category, message] = miri;
      current.kind = MIRI_KINDS[category];
      current.detail = `${category}: ${message}`.trimEnd();
      diagnosed.add(current.test);
      awaitingLocation = true;
      continue;
    }

    if (awaitingLocation && current) {
      const location = line.match(MIRI_LOCATION_RE);
      if (location) {
        current.file = resolvePath(location[1], pathPrefix);
        current.line = Number(location[2]);
        current.column = Number(location[3]);
        awaitingLocation = false;
        continue;
      }
    }

    if (capturing) {
      if (BACKTRACE_NOTE_RE.test(line) || MARKER_RE.test(line)) flush();
      else {
        detail.push(dedent(line));
        continue;
      }
    }

    const summary = line.match(SUMMARY_RE);
    if (summary) {
      flush();
      // Everything after the summary is a recap of failures already seen.
      current = null;
      testsRun = Number(summary[1]);
      passed = count(summary[2], "passed");
      failed = count(summary[2], "failed") ?? 0;
      skipped = count(summary[2], "skipped");
      continue;
    }

    const flaky = line.match(FLAKY_RE);
    if (flaky) {
      flush();
      // The attempts that failed are no longer failures: the test passed.
      const test = `${flaky[1]} ${flaky[2]}`;
      failedTests.delete(test);
      failures.delete(test);
      current = null;
      continue;
    }

    const failure = line.match(FAILURE_RE);
    if (failure) {
      flush();
      const test = `${failure[2]} ${failure[3]}`;
      failedTests.add(test);
      if (failures.has(test)) {
        // The recap, or a retry of a test that already failed.
        current = null;
        continue;
      }
      current = {
        test,
        kind: kindOf(failure[1]),
        file: null,
        line: null,
        column: null,
        detail: "",
      };
      failures.set(test, current);
    }
  }

  flush();

  // Second line of defence against retry artifacts. The FLAKY lines above are
  // the precise signal and handle a run that mixes a flaky test with a real
  // failure; this catches the case where they are absent or reworded, since the
  // count is a far more stable part of nextest's output than any marker. A run
  // that says it failed nothing did not fail anything, whatever the attempts
  // along the way reported.
  if (testsRun !== null && failed === 0) {
    failedTests.clear();
    failures.clear();
  }

  // A test killed by a signal or a timeout prints no panic, so say what
  // happened rather than leaving the detail empty.
  for (const entry of failures.values()) {
    if (entry.detail) continue;
    entry.detail = entry.kind === "timeout"
      ? "test exceeded its time limit"
      : entry.kind === "signal"
      ? "test was killed by a signal"
      : entry.kind === "leak"
      ? "test leaked memory or file descriptors"
      : "test failed without panicking, e.g. by returning an error";
  }

  return {
    testsRun,
    passed,
    failed: failed ?? (failedTests.size > 0 ? failedTests.size : null),
    skipped,
    failedTests: [...failedTests],
    failures: [...failures.values()],
  };
}

/**
 * Build the cargo argument vector for a run.
 *
 * A toolchain has to come first, before any subcommand, and `miri` wraps the
 * nextest subcommand rather than taking a flag.
 */
function buildArgs(args: RunArgs, toolchain: string | null): string[] {
  const argv: string[] = [];
  if (toolchain) argv.push(`+${toolchain}`);
  if (args.miri) argv.push("miri");
  argv.push("nextest", "run", "--color", "never");
  // An empty crate or filter means "unset", so neither reaches argv. An empty
  // positional filter in particular is not the same as no filter: nextest would
  // match it against every test name and run nothing.
  if (args.crate) argv.push("-p", args.crate);
  if (args.cargoProfile) argv.push(`--cargo-profile=${args.cargoProfile}`);
  if (args.noFailFast) argv.push("--no-fail-fast");
  if (args.extraArgs) argv.push(...args.extraArgs);
  if (args.test) argv.push(args.test);
  return argv;
}

/**
 * Decide which toolchain to invoke cargo with.
 *
 * An explicit choice is honoured as given. Otherwise a miri run takes the
 * nightly the project pins, so that a local run reproduces CI: miri only exists
 * on nightly, and both what it accepts and what it reports move between
 * nightlies. A native run takes no toolchain at all, leaving rust-toolchain.toml
 * to decide as it does for every other cargo invocation.
 */
async function resolveToolchain(
  args: RunArgs,
  repoDir: string,
  logger: { info: (msg: string, props?: unknown) => void },
): Promise<string | null> {
  if (args.toolchain) return args.toolchain;
  if (!args.miri) return null;

  const pin = `${repoDir}/${NIGHTLY_FILE}`;
  try {
    const toolchain = (await Deno.readTextFile(pin)).trim();
    if (toolchain) return toolchain;
  } catch {
    // Fall through to the message below.
  }

  // Running miri on whatever nightly is installed is still worth doing, but say
  // so: a diagnostic that only reproduces on one nightly is confusing enough
  // without being silent about which one ran.
  logger.info(
    "No toolchain pinned in {pin}, falling back to {toolchain}. " +
      "Miri diagnostics may differ from CI.",
    { pin, toolchain: "nightly" },
  );
  return "nightly";
}

/** Model definition wrapping `cargo nextest run`. */
export const model = {
  type: "@gdesmott/cargo-nextest",
  version: "2026.08.06.1",
  description:
    "Run Rust tests with cargo nextest, natively or under miri, and capture a parsed summary",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed cargo nextest run summary",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from cargo nextest",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    run: {
      description:
        "Run the test suite, natively or under miri, optionally scoped to a " +
        "crate and/or a test filter",
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
        if (args.test && args.test.startsWith("-")) {
          throw new Error(
            `Invalid test filter ${
              JSON.stringify(args.test)
            }: filters must not start with "-". ` +
              "Use extraArgs to pass flags.",
          );
        }

        const { workingDir, cargoBin } = context.globalArgs;
        const cwd = workingDir.startsWith("/")
          ? workingDir
          : `${context.repoDir}/${workingDir}`;

        const manifest = `${cwd}/Cargo.toml`;
        try {
          await Deno.stat(manifest);
        } catch {
          throw new Error(`No Cargo.toml found at ${manifest}`);
        }

        if (args.miriFlags && !args.miri) {
          throw new Error(
            "miriFlags was given without miri, so nothing would read it.",
          );
        }

        const toolchain = await resolveToolchain(
          args,
          context.repoDir,
          context.logger,
        );
        const argv = buildArgs(args, toolchain);
        const command = [cargoBin, ...argv].join(" ");
        const timeoutMs = args.timeout ??
          (args.miri ? DEFAULT_MIRI_TIMEOUT_MS : DEFAULT_TIMEOUT_MS);
        const timeoutSignal = AbortSignal.timeout(timeoutMs);

        context.logger.info("Running {command} in {cwd}", { command, cwd });

        // A single, stable data instance rather than one per crate:
        // `data.latest()` in a workflow takes the instance name, so a name
        // derived from the arguments could not be referenced without knowing
        // them. Every record carries the crate it was scoped to.
        const logWriter = context.createFileWriter("log", "log");
        const lines: string[] = [];
        const startedAt = Date.now();

        const child = new Deno.Command(cargoBin, {
          args: argv,
          cwd,
          env: {
            // A miri run owns MIRIFLAGS outright, because the flags decide what
            // the interpreter will even look for: `-Zmiri-ignore-leaks` or
            // `-Zmiri-disable-isolation` left in a developer's shell would turn
            // the pre-PR gate green on exactly what CI still fails on, and the
            // summary records the toolchain but not the flags, so nothing would
            // say why. Empty when none were asked for, which is what CI runs
            // with. A non-miri run has no use for the variable either way.
            ...(args.miri
              ? { MIRIFLAGS: (args.miriFlags ?? []).join(" ") }
              : {}),
            // Only set when asked, so a run that leaves it out keeps whatever
            // the environment already provides.
            ...(args.binDir ? { BINDIR: args.binDir } : {}),
          },
          stdout: "piped",
          stderr: "piped",
          signal: AbortSignal.any([context.signal, timeoutSignal]),
        }).spawn();

        const encoder = new TextEncoder();

        /**
         * Record a line: keep it for parsing, persist it to the log file, and
         * mirror it to stderr so long builds show progress. stderr is used
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
        const parsed = parseOutput(lines, workingDir);
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
            workingDir: cwd,
            // Empty means unset, so it is recorded as unset rather than as an
            // empty filter that never existed.
            crate: args.crate || null,
            testFilter: args.test || null,
            miri: args.miri ?? false,
            toolchain,
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

        // Not a test failure but a missing component: cargo exits before
        // interpreting anything, and `ignoreTestFailure` should not swallow it,
        // because nothing was tested. The component is per-toolchain, so a
        // machine with miri on some other nightly still lands here.
        if (failedRun && lines.some((line) => MIRI_MISSING_RE.test(line))) {
          throw new Error(
            `Miri is not installed for the \`${toolchain}\` toolchain. ` +
              `Install it with \`rustup component add --toolchain ${toolchain} miri\`, ` +
              `or point the run at a nightly that has it with the \`toolchain\` argument.`,
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
            : "";
          throw new Error(
            `\`${command}\` exited with code ${status.code}.${detail}`,
          );
        }

        return { dataHandles: handles };
      },
    },
  },
};
