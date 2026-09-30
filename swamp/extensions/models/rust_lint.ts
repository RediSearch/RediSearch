/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Runs one Rust lint tool — clippy or rustdoc — across the debug and release
 * profiles, and records the diagnostics it produced.
 *
 * The repository's `make lint` target chains both tools across both profiles in
 * a single opaque step. This model runs one tool per instance so a failure
 * names the tool and profile that produced it, mirroring the Makefile recipe
 * otherwise: the same workspace, the same excluded crates (read from
 * `build.sh`, so the two cannot drift), and warnings escalated to errors.
 *
 * @module
 */
import { z } from "npm:zod@4.4.3";

/** Default timeout for a lint run: 45 minutes. */
const DEFAULT_TIMEOUT_MS = 45 * 60 * 1000;

/** Upper bound on findings stored in the summary resource. */
const MAX_FINDINGS = 500;

/** The shell variable in `build.sh` listing crates excluded from linting. */
const EXCLUDE_VAR = "EXCLUDE_RUST_BENCHING_CRATES_LINKING_C";

/** Where regen_headers.sh writes the generated C headers, relative to the repo. */
const HEADERS_DIR = "src/redisearch_rs/headers";

const GlobalArgsSchema = z.object({
  tool: z
    .enum(["clippy", "doc"])
    .describe("Which lint tool this model runs: cargo clippy or cargo doc."),
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
  profiles: z
    .array(z.enum(["debug", "release"]))
    .min(1)
    .default(["debug", "release"])
    .describe("Cargo profiles to lint, run in the order given."),
  excludeFrom: z
    .string()
    .default("build.sh")
    .describe(
      `Script to read the ${EXCLUDE_VAR} exclude list from, relative to \`repoRoot\`.`,
    ),
  regenerateHeaders: z
    .boolean()
    .default(true)
    .describe(
      "Run regen_headers.sh first, as the Makefile's lint target does. " +
        "This writes to src/redisearch_rs/headers/ and can modify the working tree.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const RunArgsSchema = z.object({
  jobs: z
    .number()
    .int()
    .positive()
    .optional()
    .describe("Parallel cargo jobs (`--jobs`)."),
  extraArgs: z
    .array(z.string())
    .optional()
    .describe("Additional arguments appended to the cargo invocation."),
  binDir: z
    .string()
    .optional()
    .describe(
      "Directory holding the compiled C static libraries, exported as " +
        "BINDIR. Linting runs the build scripts, and the ones binding C " +
        "symbols panic when the archive is missing rather than degrading to a " +
        "warning — so without this a lint on a checkout that was never built " +
        "release fails before reporting a single finding. Pass the binDir the " +
        "build reported. Empty runs with BINDIR unset — including when the " +
        "caller's environment exports one — so a run without it links what a " +
        "plain `cargo clippy` would.",
    ),
  failFast: z
    .boolean()
    .optional()
    .describe(
      "Stop after the first failing profile, as make does. Defaults to true; " +
        "set false to lint every profile and collect all findings.",
    ),
  timeout: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(`Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}).`),
  ignoreFailure: z
    .boolean()
    .optional()
    .describe("Record the result without failing the method when lint fails."),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default cargo's output is mirrored to stderr " +
        "as it arrives, since a full lint run takes minutes.",
    ),
});

type RunArgs = z.infer<typeof RunArgsSchema>;

const FindingSchema = z.object({
  level: z.enum(["error", "warning"]).describe("Diagnostic severity"),
  code: z
    .string()
    .nullable()
    .describe("Diagnostic code, e.g. E0433, when rustc emitted one"),
  message: z.string().describe("The diagnostic message"),
  file: z.string().nullable().describe(
    "Source file, when a location was given",
  ),
  line: z.number().int().nullable().describe("Line number of the diagnostic"),
  column: z.number().int().nullable().describe("Column of the diagnostic"),
  profile: z
    .string()
    .describe("Cargo profile that reported it: debug or release"),
});

const SummarySchema = z.object({
  tool: z.string().describe("The lint tool that was run: clippy or doc"),
  commands: z
    .array(z.string())
    .describe("The cargo command lines that were executed, in order"),
  workingDir: z.string().describe("Resolved directory cargo ran in"),
  excluded: z
    .array(z.string())
    .describe("Crates excluded from the workspace lint"),
  profilesRun: z
    .array(z.string())
    .describe("Profiles actually linted before the run ended"),
  headersRegenerated: z
    .boolean()
    .describe("Whether regen_headers.sh ran before linting"),
  exitCode: z
    .number()
    .int()
    .describe("Exit code of the first failing profile, or 0"),
  status: z.enum(["passed", "failed"]).describe("Overall outcome"),
  errorCount: z.number().int().describe("Distinct error-level findings"),
  warningCount: z.number().int().describe("Distinct warning-level findings"),
  findings: z
    .array(FindingSchema)
    .describe(`Deduplicated diagnostics, capped at ${MAX_FINDINGS}`),
  truncated: z
    .boolean()
    .describe("True when findings were capped and some were dropped"),
  totalFindings: z
    .number()
    .int()
    .describe("Distinct findings before the cap was applied"),
  failedProfile: z
    .string()
    .nullable()
    .describe("Profile whose cargo invocation failed, if any"),
  timedOut: z.boolean().describe("Whether the run was aborted by the timeout"),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the run completed"),
});

/** A diagnostic header, e.g. `error[E0433]: failed to resolve`. */
const DIAGNOSTIC_RE = /^(error|warning)(?:\[([^\]]+)\])?: (.+)$/;

/** A source location line, e.g. `  --> src/lib.rs:10:5`. */
const LOCATION_RE = /^\s*-->\s+(.+?):(\d+):(\d+)\s*$/;

/**
 * Rollup lines that restate counts rather than report a distinct problem, e.g.
 * `error: could not compile \`foo\` due to 2 previous errors`.
 */
const AGGREGATE_RE =
  /^(?:could not compile|aborting due to|build failed|generated \d+ warnings?|.*generated \d+ warnings?$)/;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and carriage returns from a line. */
function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
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

type Finding = z.infer<typeof FindingSchema>;

/** One profile's captured output. */
export interface LintSection {
  profile: string;
  lines: string[];
}

/**
 * Parse each profile's output into deduplicated findings.
 *
 * Debug and release report largely the same problems, so findings are keyed on
 * level, location and message; the first occurrence wins and keeps the profile
 * that reported it.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function parseSections(sections: LintSection[]): Finding[] {
  const byKey = new Map<string, Finding>();

  for (const section of sections) {
    let pending: Finding | null = null;

    /** Store a finished finding, keeping the first occurrence of a duplicate. */
    const flush = (): void => {
      if (!pending) return;
      const key = [
        pending.level,
        pending.file ?? "",
        pending.line ?? "",
        pending.column ?? "",
        pending.message,
      ].join("|");
      if (!byKey.has(key)) byKey.set(key, pending);
      pending = null;
    };

    for (const line of section.lines) {
      const diagnostic = line.match(DIAGNOSTIC_RE);
      if (diagnostic) {
        flush();
        const message = diagnostic[3].trim();
        if (AGGREGATE_RE.test(message)) continue;
        pending = {
          level: diagnostic[1] as "error" | "warning",
          code: diagnostic[2] ?? null,
          message,
          file: null,
          line: null,
          column: null,
          profile: section.profile,
        };
        continue;
      }

      const location = line.match(LOCATION_RE);
      if (location && pending && pending.file === null) {
        pending.file = location[1];
        pending.line = Number(location[2]);
        pending.column = Number(location[3]);
      }
    }
    flush();
  }

  return [...byKey.values()];
}

/**
 * Read the excluded-crate list out of `build.sh`.
 *
 * The Makefile greps the same variable rather than duplicating the list, so
 * reading it at runtime keeps this model in step with the build.
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
 * The `extraArgs` entries this model refuses, and what to use instead.
 *
 * The same split the suite models guard against. `extraArgs` is appended to the
 * cargo invocation, but `excluded` and `profilesRun` — what a reader takes as
 * the scope the lint actually covered — are derived from build.sh and the
 * global arguments alone. `extraArgs: ["--exclude", "qint"]` therefore lints
 * everything but `qint` while the summary lists only build.sh's own exclusions,
 * and a narrowed run reads as the whole workspace on a clean result.
 *
 * `--` is here because the model appends `-- -D warnings` itself: a caller's
 * own `--` puts lint levels in front of that, and `-A warnings` accepted there
 * is a lint gate that certifies anything.
 */
const REFUSED_SWITCHES: Record<string, string> = {
  "-p": "nothing — this model lints the workspace build.sh defines",
  "--package": "nothing — this model lints the workspace build.sh defines",
  "--workspace": "nothing — the model passes it",
  "--exclude":
    "`excludeFrom`, which is where the recorded exclusions come from",
  "--all": "nothing — this model lints the workspace build.sh defines",
  "--lib": "nothing — the summary speaks for the whole workspace",
  "--bin": "nothing — the summary speaks for the whole workspace",
  "--bins": "nothing — the summary speaks for the whole workspace",
  "--test": "nothing — the summary speaks for the whole workspace",
  "--tests": "nothing — the summary speaks for the whole workspace",
  "--example": "nothing — the summary speaks for the whole workspace",
  "--examples": "nothing — the summary speaks for the whole workspace",
  "--release": "`profiles`, which is what `profilesRun` is recorded from",
  "--profile": "`profiles`, which is what `profilesRun` is recorded from",
  "--jobs": "`jobs`",
  "-j": "`jobs`",
  "--no-deps": "nothing — the model passes it for rustdoc",
  "--document-private-items": "nothing — the model passes it for rustdoc",
  "--": "nothing — the model appends the lint flags after it itself",
};

/** The switch an `extraArgs` entry sets, i.e. `-p` from `-p=qint`. */
export function switchName(arg: string): string {
  const equals = arg.indexOf("=");
  return equals === -1 ? arg : arg.slice(0, equals);
}

/**
 * Refuse `extraArgs` that would change what was linted without changing what
 * the summary says was linted.
 *
 * Positionals are refused rather than matched against a list: cargo reads a
 * bare entry as a spec or a path depending on the subcommand, and either
 * narrows the lint without changing the scope recorded for it.
 */
export function checkExtraArgs(extraArgs: string[] | undefined): void {
  const extra = extraArgs ?? [];
  for (const [i, arg] of extra.entries()) {
    const refused = REFUSED_SWITCHES[switchName(arg)];
    if (refused) {
      throw new Error(
        `extraArgs sets ${switchName(arg)}, which this model's summary does ` +
          `not reflect: pass ${refused} instead. Set through extraArgs it ` +
          `would narrow what was linted while the recorded scope still reads ` +
          `as the whole workspace.`,
      );
    }
    // An entry directly after a switch written without `=` is that switch's
    // value, which is how a caller ordinarily passes one. What that leaves
    // uncaught is a selector written after a boolean flag; writing values as
    // `--flag=value` closes even that, which is why the message asks for it.
    const previous = i === 0 ? "" : extra[i - 1];
    if (
      !arg.startsWith("-") &&
      !(previous.startsWith("-") && !previous.includes("="))
    ) {
      throw new Error(
        `extraArgs contains the positional entry ${JSON.stringify(arg)}. ` +
          "cargo reads a bare entry as a package or path selector, which " +
          "narrows the lint without changing the scope the summary records. " +
          'Attach a flag\'s value with "=" (--jobs=4) so that a value is ' +
          "never mistaken for a selector.",
      );
    }
  }
}

/** Build the cargo argument vector for one tool/profile combination. */
export function buildArgs(
  tool: "clippy" | "doc",
  profile: string,
  excluded: string[],
  args: RunArgs,
): string[] {
  const argv: string[] = [tool, "--workspace"];
  for (const crate of excluded) argv.push("--exclude", crate);
  if (tool === "doc") argv.push("--no-deps", "--document-private-items");
  if (profile === "release") argv.push("--release");
  if (args.jobs) argv.push("--jobs", String(args.jobs));
  if (args.extraArgs) argv.push(...args.extraArgs);
  // clippy takes lint flags after `--`; rustdoc takes them via RUSTDOCFLAGS.
  if (tool === "clippy") argv.push("--", "-D", "warnings");
  return argv;
}

/**
 * The variables this model decides rather than inherits.
 *
 * BINDIR names the C archives the build scripts link against. Without one they
 * look in the conventional release layout, which is what a plain `cargo clippy`
 * does — so a run given no `binDir` has to be given no BINDIR either. An
 * earlier debug, coverage or sanitizer build exports it, and inheriting that
 * lints against a different build's archives while the summary reports an
 * ordinary run.
 *
 * The flag variables are worse, because they can defeat the gate rather than
 * redirect it: rustc's lint cap outranks a command line, so an inherited
 * `RUSTFLAGS=--cap-lints allow` turns the `-D warnings` this model appends into
 * nothing and every profile comes back clean. `CARGO_ENCODED_RUSTFLAGS` is
 * listed beside it because cargo prefers it when both are set, so dropping only
 * `RUSTFLAGS` would leave the same cap in place. `RUSTDOCFLAGS` is set
 * explicitly for rustdoc runs and dropped for the rest, so that a stale one
 * cannot decide a clippy run either.
 */
const DECIDED_ENV = [
  "BINDIR",
  "RUSTFLAGS",
  "CARGO_ENCODED_RUSTFLAGS",
  "RUSTDOCFLAGS",
];

/**
 * Build the base environment every command in a run inherits, with the
 * variables in `DECIDED_ENV` decided rather than inherited.
 *
 * Deno can override a variable but not remove one, so dropping an inherited one
 * means handing over the whole environment without it. That is done only when
 * there is something to drop: the ordinary case keeps plain inheritance rather
 * than a wholesale copy of the caller's environment.
 */
function binDirEnv(
  binDir: string | undefined,
): { env: Record<string, string>; clearEnv: boolean } {
  const dropped = DECIDED_ENV.filter((name) =>
    // A BINDIR that was asked for is set below rather than dropped; the rest
    // are never taken from the caller.
    (name !== "BINDIR" || !binDir) && Deno.env.get(name) !== undefined
  );
  const clearEnv = dropped.length > 0;
  const env: Record<string, string> = clearEnv ? Deno.env.toObject() : {};
  for (const name of dropped) delete env[name];
  if (binDir) env.BINDIR = binDir;
  return { env, clearEnv };
}

/** Model definition wrapping a single Rust lint tool. */
export const model = {
  type: "@gdesmott/rust-lint",
  version: "2026.08.17.2",
  description:
    "Run one Rust lint tool (clippy or rustdoc) and capture findings",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed lint run summary with structured findings",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from the lint run",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    run: {
      description: "Lint each configured profile and record the diagnostics",
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
        const {
          tool,
          repoRoot,
          workingDir,
          cargoBin,
          profiles,
          excludeFrom,
          regenerateHeaders,
        } = context.globalArgs;

        checkExtraArgs(args.extraArgs);

        const root = resolve(context.repoDir, repoRoot);
        const cwd = workingDir.startsWith("/")
          ? workingDir
          : resolve(root, workingDir);

        try {
          await Deno.stat(`${cwd}/Cargo.toml`);
        } catch {
          throw new Error(`No Cargo.toml found at ${cwd}/Cargo.toml`);
        }

        const excludePath = excludeFrom.startsWith("/")
          ? excludeFrom
          : resolve(root, excludeFrom);
        let excluded: string[] = [];
        try {
          excluded = parseExcludes(await Deno.readTextFile(excludePath));
        } catch {
          throw new Error(
            `Could not read the ${EXCLUDE_VAR} exclude list from ${excludePath}`,
          );
        }

        const timeoutMs = args.timeout ?? DEFAULT_TIMEOUT_MS;
        const timeoutSignal = AbortSignal.timeout(timeoutMs);
        const signal = AbortSignal.any([context.signal, timeoutSignal]);
        const failFast = args.failFast ?? true;

        const logWriter = context.createFileWriter("log", "log");
        const encoder = new TextEncoder();
        const startedAt = Date.now();

        /**
         * Record a line: persist it to the log file and mirror it to stderr so
         * long runs show progress. stderr is used because swamp emits its own
         * JSON on stdout.
         */
        const record = async (raw: string, sink: string[]): Promise<void> => {
          const line = clean(raw);
          sink.push(line);
          await logWriter.writeLine(line);
          if (!args.quiet) {
            await Deno.stderr.write(encoder.encode(`${line}\n`));
          }
        };

        /**
         * Abandon the run, keeping what was written to the log.
         *
         * Every early exit here happens after output has already been streamed,
         * and several of the messages send the reader to that log — so throwing
         * without publishing it first would point at nothing. No summary is
         * written on these paths by design: there is nothing to summarise.
         */
        const abort = async (message: string): Promise<never> => {
          await logWriter.finalize();
          throw new Error(message);
        };

        const { env: baseEnv, clearEnv } = binDirEnv(args.binDir);

        /** Spawn a command, streaming its output into `sink`. */
        const spawn = async (
          bin: string,
          argv: string[],
          spawnCwd: string,
          env: Record<string, string>,
          sink: string[],
        ): Promise<Deno.CommandStatus> => {
          const child = new Deno.Command(bin, {
            args: argv,
            cwd: spawnCwd,
            env: { ...baseEnv, ...env },
            clearEnv,
            stdout: "piped",
            stderr: "piped",
            signal,
          }).spawn();

          /**
           * Decode a stream, splitting it into lines, and give up on it once the run has
           * been aborted.
           *
           * The abort reaches the process this model started and nothing else: the
           * compilers, test servers and helpers it spawned in turn inherited this pipe and
           * were signalled by no one. Draining until the pipe closes therefore waits for
           * whichever of them lives longest, so a run with a hung grandchild outlives the
           * timeout that was meant to end it — and the timeout is only looked at after this
           * returns. Cancelling costs whatever those processes had not written yet, which
           * is a fair price for a run that is over either way.
           */
          const pump = async (
            stream: ReadableStream<Uint8Array>,
          ): Promise<void> => {
            const reader = stream.getReader();
            const stopDraining = () => {
              reader.cancel().catch(() => {});
            };
            if (signal.aborted) stopDraining();
            else signal.addEventListener("abort", stopDraining, { once: true });
            const decoder = new TextDecoder();
            let buffer = "";
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const parts = buffer.split("\n");
                buffer = parts.pop() ?? "";
                for (const part of parts) await record(part, sink);
              }
            } finally {
              signal.removeEventListener("abort", stopDraining);
            }
            if (buffer.length > 0) await record(buffer, sink);
          };

          await Promise.all([pump(child.stdout), pump(child.stderr)]);
          return await child.status;
        };

        /** True when the run was interrupted rather than completed. */
        const cancelled = (status: Deno.CommandStatus): boolean =>
          !timeoutSignal.aborted &&
          (context.signal.aborted || status.signal === "SIGINT");

        /**
         * What the generated headers currently differ from the commit on, one
         * porcelain line per path, or null when git did not answer.
         *
         * `git status --porcelain` rather than `git diff`, because a new FFI
         * crate makes cheadergen write a header that was never committed. git
         * diff compares tracked files against the index and says nothing about
         * an untracked one, so it would pass while leaving a required header
         * uncommitted. status reports both.
         */
        const headerDirt = async (sink: string[]): Promise<string[] | null> => {
          const from = sink.length;
          const status = await spawn(
            "git",
            [
              "status",
              "--porcelain",
              "--untracked-files=all",
              "--",
              HEADERS_DIR,
            ],
            root,
            {},
            sink,
          );
          if (cancelled(status)) {
            await abort(
              "Header regeneration was cancelled. No summary recorded.",
            );
          }
          if (!status.success) return null;
          return sink.slice(from).filter((entry) => entry.trim().length > 0);
        };

        if (regenerateHeaders) {
          const scratch: string[] = [];
          await record("=== regen_headers.sh ===", scratch);
          // What the headers already differed on before anything was
          // regenerated. Subtracted from the same reading afterwards it leaves
          // what regeneration itself introduced, which is the question being
          // asked — and it is not the same question as whether the tree is
          // dirty. A change that regenerated its headers and has not committed
          // them yet is the ordinary state for the pre-commit `verify` this
          // model serves, and failing there tells the author to run the command
          // they have already run.
          const beforeRegen = await headerDirt(scratch);
          const status = await spawn(
            `${root}/src/redisearch_rs/regen_headers.sh`,
            [],
            root,
            {},
            scratch,
          );
          if (cancelled(status)) {
            await abort(
              "Header regeneration was cancelled. No summary recorded.",
            );
          }
          if (!status.success) {
            await abort(
              `regen_headers.sh exited with code ${status.code}; skipped linting. See its log data for the diagnostics.`,
            );
          }

          // Regeneration succeeding is not the same as the headers having been
          // up to date: it rewrites them in place, so stale committed headers
          // come back as a dirty tree rather than as a failure.
          const afterRegen = await headerDirt(scratch);
          // Only compare when git answered on both sides. A non-zero exit is
          // git failing to run at all — no repository, no git on PATH — which is
          // not evidence that the headers are stale and must not be reported as
          // such. Linting is still worth doing, so say so and carry on.
          if (beforeRegen !== null && afterRegen !== null) {
            const introduced = afterRegen.filter((entry) =>
              !beforeRegen.includes(entry)
            );
            if (introduced.length > 0) {
              await abort(
                `Regenerating headers changed files under ${HEADERS_DIR}: ` +
                  `${introduced.join(", ")}. The committed headers are stale ` +
                  "— run 'make generate-rust-headers' and commit the result, " +
                  "or CI's generated-files check will fail.",
              );
            }
            if (afterRegen.length > 0) {
              // Regeneration changed nothing, so the headers on disk match the
              // Rust that produced them and only the commit is missing. That is
              // a `/commit-guidelines` matter — the headers belong in the same
              // revision as the change — and not something to fail a lint over.
              context.logger.info(
                "The headers under {dir} are up to date but uncommitted: {n} " +
                  "path(s) were already dirty before regeneration and " +
                  "regeneration changed nothing. Commit them with the change " +
                  "they belong to; CI's generated-files check reads what is " +
                  "committed.",
                { dir: HEADERS_DIR, n: afterRegen.length },
              );
            }
          } else {
            context.logger.info(
              "Could not check whether regeneration changed {dir}: git did " +
                "not answer. Continuing to lint; CI's generated-files check " +
                "still covers this.",
              { dir: HEADERS_DIR },
            );
          }
        }

        const sections: LintSection[] = [];
        const commands: string[] = [];
        const profilesRun: string[] = [];
        let failedProfile: string | null = null;
        let exitCode = 0;

        for (const profile of profiles) {
          const argv = buildArgs(tool, profile, excluded, args);
          const command = [cargoBin, ...argv].join(" ");
          commands.push(command);

          const lines: string[] = [];
          await record(`=== ${tool} (${profile}) ===`, lines);
          context.logger.info("Running {command} in {cwd}", { command, cwd });

          const status = await spawn(
            cargoBin,
            argv,
            cwd,
            {
              // rustdoc takes its lint flags through the environment. BINDIR is
              // decided once for every command this method runs, in binDirEnv.
              ...(tool === "doc" ? { RUSTDOCFLAGS: "-Dwarnings" } : {}),
            },
            lines,
          );

          sections.push({ profile, lines });
          profilesRun.push(profile);

          if (cancelled(status)) {
            await abort(
              `\`${command}\` was cancelled after ${
                Date.now() - startedAt
              }ms. No summary recorded.`,
            );
          }

          if (!status.success) {
            failedProfile ??= profile;
            exitCode = exitCode === 0 ? status.code : exitCode;
            if (failFast) break;
          }
        }

        const durationMs = Date.now() - startedAt;
        const timedOut = timeoutSignal.aborted;
        const logHandle = await logWriter.finalize();

        const all = parseSections(sections);
        const kept = all.slice(0, MAX_FINDINGS);
        const errorCount = all.filter((f) => f.level === "error").length;
        const warningCount = all.filter((f) => f.level === "warning").length;
        const failedRun = failedProfile !== null;

        const summaryHandle = await context.writeResource(
          "summary",
          "summary",
          {
            tool,
            commands,
            workingDir: cwd,
            excluded,
            profilesRun,
            headersRegenerated: regenerateHeaders,
            exitCode,
            status: failedRun ? "failed" : "passed",
            errorCount,
            warningCount,
            findings: kept,
            truncated: all.length > kept.length,
            totalFindings: all.length,
            failedProfile,
            timedOut,
            durationMs,
            executedAt: new Date().toISOString(),
          },
        );

        const handles = [summaryHandle, logHandle];

        if (timedOut) {
          throw new Error(
            `cargo ${tool} timed out after ${timeoutMs}ms. See the log data for details.`,
          );
        }

        if (failedRun && !args.ignoreFailure) {
          throw new Error(
            `cargo ${tool} failed on the ${failedProfile} profile with code ${exitCode}. ` +
              `${errorCount} error(s), ${warningCount} warning(s).`,
          );
        }

        return { dataHandles: handles };
      },
    },
  },
};
