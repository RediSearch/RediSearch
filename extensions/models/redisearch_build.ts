/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Builds the RediSearch module (C, C++ and Rust) with `./build.sh` and records
 * a parsed summary alongside the full build log.
 *
 * The `build` method wraps the repository's `build.sh`, exposing the build
 * variants that matter for development: debug, release, coverage (`COV=1`),
 * AddressSanitizer (`SAN=address`) and profiling. It streams the combined
 * stdout/stderr into a log file, derives the build flavor and output directory
 * the same way `build.sh` does, and reports the resulting module artifact.
 *
 * This model deliberately never runs tests. `build.sh` can run C/C++, Rust and
 * Python suites, but those are covered by dedicated models so that each suite
 * gets its own summary, filters and failure reporting. Only `buildTests` is
 * exposed here, which compiles the test binaries that those models consume.
 *
 * All build variants share the Cargo build-directory lock under
 * `src/redisearch_rs/target`, so every variant is a method on this single model
 * rather than a model per variant: swamp's per-model lock then serialises them.
 *
 * @module
 */
import { z } from "npm:zod@4";

/** Default timeout for a build: 60 minutes. A cold full build is slow. */
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
      "Coordinator flavor: `oss` builds search-community, `rlec` builds search-enterprise.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const BuildArgsSchema = z.object({
  debug: z
    .boolean()
    .optional()
    .describe("Build with debug symbols and assertions (`DEBUG=1`)."),
  coverage: z
    .boolean()
    .optional()
    .describe(
      "Build instrumented for coverage (`COV=1`). Uses the pinned nightly " +
        "toolchain and lands in the `debug-cov` flavor directory.",
    ),
  sanitizer: z
    .enum(["address"])
    .optional()
    .describe(
      "Build with a sanitizer (`SAN=<value>`). Uses the pinned nightly " +
        "toolchain and lands in the `debug-asan` flavor directory.",
    ),
  profile: z
    .boolean()
    .optional()
    .describe(
      "Build a release binary with profiling enabled (`PROFILE=1`). " +
        "Cannot be combined with debug, coverage or a sanitizer.",
    ),
  lto: z
    .boolean()
    .optional()
    .describe(
      "Enable Rust/C link-time optimisation (`LTO=1`). Linux only, and " +
        "requires clang and lld matching the LLVM version rustc was built with.",
    ),
  force: z
    .boolean()
    .optional()
    .describe("Discard previous artifacts and rebuild from scratch (`FORCE`)."),
  verbose: z
    .boolean()
    .optional()
    .describe("Echo the underlying compiler commands (`VERBOSE=1`)."),
  buildTests: z
    .boolean()
    .optional()
    .describe(
      "Also compile the C/C++ and Rust test binaries (`TESTS=1`) without " +
        "running them. Required before the test models can run a suite.",
    ),
  generateHeaders: z
    .boolean()
    .optional()
    .describe(
      "Regenerate the C headers for Rust modules from source " +
        "(`REDISEARCH_GENERATE_HEADERS`). Defaults to build.sh's own default " +
        "of enabled; set false to use the committed headers.",
    ),
  rustProfile: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Override the Cargo profile used for Rust code (`RUST_PROFILE=`). " +
        "By default build.sh picks one from the build variant.",
    ),
  rustDenyWarns: z
    .boolean()
    .optional()
    .describe("Turn Rust compiler warnings into errors (`RUST_DENY_WARNS=1`)."),
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
  ignoreBuildFailure: z
    .boolean()
    .optional()
    .describe(
      "Record the result without failing the method when the build fails.",
    ),
  quiet: z
    .boolean()
    .optional()
    .describe(
      "Suppress live output. By default the build output is mirrored to " +
        "stderr as it arrives, since a cold build runs for many minutes.",
    ),
});

type BuildArgs = z.infer<typeof BuildArgsSchema>;

const SummarySchema = z.object({
  command: z.string().describe("The full command line that was executed"),
  repoRoot: z.string().describe("Resolved directory build.sh ran in"),
  flavor: z
    .string()
    .describe(
      "Build flavor selected by build.sh: release, debug, debug-cov, " +
        "debug-asan or release-profile",
    ),
  coord: z.enum(["oss", "rlec"]).describe("Coordinator flavor that was built"),
  variant: z
    .string()
    .describe("Full build variant directory name, e.g. linux-x64-debug"),
  binDir: z
    .string()
    .describe("Directory build.sh writes the module and test binaries into"),
  rustProfile: z
    .string()
    .nullable()
    .describe(
      "Cargo profile the Rust half was built with, read from the cmake " +
        "command build.sh echoes rather than derived a second time here. " +
        "Null when the line was not seen, which a SKIP_BUILD run never " +
        "prints. The test models need it: nextest otherwise defaults to its " +
        "own profile and rebuilds instead of using these artifacts",
    ),
  modulePath: z
    .string()
    .nullable()
    .describe("Path to the built module shared object, if it exists"),
  moduleSizeBytes: z
    .number()
    .int()
    .nonnegative()
    .nullable()
    .describe("Size of the built module, useful for spotting bloat over time"),
  moduleStale: z
    .boolean()
    .describe(
      "True when the build failed and the module on disk is left over from an " +
        "earlier build. Check this before treating modulePath as this run's " +
        "output. A successful build is never stale, including an incremental " +
        "one that had nothing to relink",
    ),
  coverage: z.boolean().describe(
    "Whether the build was instrumented for coverage",
  ),
  sanitizer: z
    .string()
    .nullable()
    .describe("Sanitizer the build was instrumented with, if any"),
  testsBuilt: z.boolean().describe("Whether test binaries were compiled"),
  exitCode: z.number().int().describe("Exit code of build.sh"),
  status: z.enum(["succeeded", "failed"]).describe(
    "Overall outcome of the build",
  ),
  errorCount: z.number().int().nonnegative().describe(
    "Compiler error lines seen",
  ),
  warningCount: z
    .number()
    .int()
    .nonnegative()
    .describe("Compiler warning lines seen"),
  errors: z
    .array(z.string())
    .describe("First few compiler or CMake error lines, for triage"),
  timedOut: z.boolean().describe(
    "Whether the build was aborted by the timeout",
  ),
  durationMs: z.number().int().nonnegative().describe("Wall-clock duration"),
  executedAt: z.iso.datetime().describe("Timestamp when the build completed"),
});

/** How many error lines to keep in the summary; the log holds the rest. */
const MAX_ERRORS = 20;

/**
 * A diagnostic's prefix, before the `error:` or `warning:` itself.
 *
 * Two forms, and a link failure only ever prints the second. A compiler names
 * the source position it is complaining about — `src/spec.c:42:5:` — but the
 * linker has no position to name and prints its own name instead: `ld.lld:`,
 * `collect2:`, `/usr/bin/ld:`, `clang:`. A single token with no spaces, which
 * is what keeps this from swallowing prose that happens to contain a colon.
 */
const PREFIX = String.raw`(?:.*?:\d+:\d+:|[^\s:]+:)\s*`;

/** Matches a compiler, linker or rustc error line. */
const ERROR_RE = new RegExp(
  `^(?:${PREFIX})?(?:fatal error|error(?:\\[[^\\]]+\\])?):\\s`,
  "i",
);

/** Matches a compiler or linker warning line. */
const WARNING_RE = new RegExp(
  `^(?:${PREFIX})?warning(?:\\[[^\\]]+\\])?:\\s`,
  "i",
);

/**
 * Matches the header line of a CMake diagnostic.
 *
 * A configure-time failure never reaches a compiler, so it says nothing in the
 * `file:line:col: error:` form the matchers above expect — it says
 * `CMake Error at CMakeLists.txt:12 (message):` (or a bare `CMake Error:`, or
 * either with a `(dev)` marker), and puts the message itself on the indented
 * lines that follow.
 */
const CMAKE_RE = /^CMake (Error|Warning)\b/;

/** Matches a continuation line of a CMake diagnostic, or its blank separator. */
const CMAKE_CONT_RE = /^(?:\s*$|\s)/;

/** How many continuation lines of a CMake diagnostic to fold into the entry. */
const MAX_CMAKE_CONT = 3;

const ANSI_RE = /\x1b\[[0-9;]*m/g;

/** Strip ANSI colour escapes and trailing carriage returns from a line. */
function clean(line: string): string {
  return line.replace(ANSI_RE, "").replace(/\r/g, "");
}

export interface ParsedBuild {
  errorCount: number;
  warningCount: number;
  errors: string[];
}

/**
 * Read the cargo profile out of the cmake command build.sh echoes.
 *
 * build.sh derives the profile from DEBUG, TESTS, COV, SAN and miri together
 * and passes it as `-DRUST_PROFILE=`. Recovering it from the command it prints
 * keeps that derivation in one place; repeating the rules here would leave two
 * copies to drift apart. Null when no such line appeared.
 */
export function parseRustProfile(lines: string[]): string | null {
  for (const line of lines) {
    const match = line.match(/-DRUST_PROFILE=(\S+)/);
    if (match) return match[1];
  }
  return null;
}

/** Parse build output into error and warning counts plus sample error lines. */
export function parseOutput(lines: string[]): ParsedBuild {
  const errors: string[] = [];
  let errorCount = 0;
  let warningCount = 0;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const cmake = line.match(CMAKE_RE);
    if (cmake) {
      // The header alone names a file and not the problem, so fold the indented
      // message underneath it into the same entry. Bounded on both ends: a long
      // CMake backtrace would otherwise crowd out the rest of the list.
      const parts = [line.trim()];
      let kept = 0;
      while (
        i + 1 < lines.length && CMAKE_CONT_RE.test(lines[i + 1]) &&
        kept < MAX_CMAKE_CONT
      ) {
        i += 1;
        const text = lines[i].trim();
        if (text.length === 0) continue;
        parts.push(text);
        kept += 1;
      }
      if (cmake[1] === "Error") {
        errorCount += 1;
        if (errors.length < MAX_ERRORS) errors.push(parts.join(" "));
      } else {
        warningCount += 1;
      }
    } else if (ERROR_RE.test(line)) {
      errorCount += 1;
      if (errors.length < MAX_ERRORS) errors.push(line.trim());
    } else if (WARNING_RE.test(line)) {
      warningCount += 1;
    }
  }

  return { errorCount, warningCount, errors };
}

/**
 * Reject variant combinations that build.sh either refuses outright or
 * silently resolves in a surprising way.
 *
 * The flavor cascade in build.sh picks the first match of asan, miri, debug,
 * cov, profile. So `debug` plus `coverage` produces an instrumented build that
 * lands in the plain `debug` directory, where a later ordinary debug build
 * would reuse and overwrite it. Catching that here beats debugging it later.
 */
export function validate(args: BuildArgs): void {
  if (args.profile && (args.debug || args.coverage || args.sanitizer)) {
    throw new Error(
      "profile cannot be combined with debug, coverage or a sanitizer: " +
        "build.sh rejects this combination.",
    );
  }
  if (args.coverage && args.sanitizer) {
    throw new Error(
      "coverage cannot be combined with a sanitizer: the two instrumentations " +
        `conflict and the build would land in the ${args.sanitizer} flavor ` +
        "directory while still being coverage-instrumented.",
    );
  }
  if (args.coverage && args.debug) {
    throw new Error(
      "coverage cannot be combined with debug: the build would be " +
        "coverage-instrumented but land in the plain `debug` flavor directory, " +
        "where an ordinary debug build would overwrite it. Coverage builds are " +
        "already unoptimised.",
    );
  }
}

/**
 * build.sh controls that decide what gets built, which it reads from the
 * environment.
 *
 * `SKIP_BUILD` and `SAN` are the two that build.sh never initialises before
 * use, so an exported value survives untouched: a caller who exported
 * `SKIP_BUILD=1` — a CI shell, an earlier test-only invocation — would have
 * this model report a build it never ran, and an exported `SAN=address` would
 * instrument a build the summary calls `release` and leave it in a flavor
 * directory this model does not name. The rest do have an argument form, so a
 * run that sets one is already safe; a run that does not would inherit it.
 *
 * Deliberately not listed: `INLINE_LSE_ATOMICS`, `BUILD_INTEL_SVS_OPT`,
 * `RUST_DYN_CRT`, `SCCACHE_PATH`. Those describe the machine rather than
 * contradict the summary, and a developer who exported one meant it.
 */
const OWNED_ENV = [
  "SKIP_BUILD",
  "SAN",
  "COV",
  "REDISEARCH_GENERATE_HEADERS",
  "ARCHIVE_RUST_TESTS",
  "RUN_ARCHIVED_RUST_TESTS",
  "RUST_PARTITION",
];

/**
 * Neutralise the inherited build.sh controls this model owns.
 *
 * Empty rather than a value: build.sh reads each of these as `${VAR:-<default>}`
 * or not at all, and that form treats empty exactly as unset. So clearing one
 * restores build.sh's own default without naming it a second time here, where
 * it could drift, and the argument vector still overrides it for the controls a
 * run actually asked for. Anything build.sh reads as `${VAR-<default>}` — the
 * dash without a colon, as `EXT` and its companions do — would take the empty
 * string as a real value, so none of those belong on this list.
 */
export function buildEnv(): Record<string, string> {
  return Object.fromEntries(OWNED_ENV.map((name) => [name, ""]));
}

/** Build the build.sh argument vector for a run. */
export function buildArgv(
  args: BuildArgs,
  coord: GlobalArgs["coord"],
): string[] {
  const argv: string[] = [`COORD=${coord}`];
  if (args.debug) argv.push("DEBUG=1");
  if (args.coverage) argv.push("COV=1");
  if (args.sanitizer) argv.push(`SAN=${args.sanitizer}`);
  if (args.profile) argv.push("PROFILE=1");
  if (args.lto) argv.push("LTO=1");
  if (args.force) argv.push("FORCE");
  if (args.verbose) argv.push("VERBOSE=1");
  if (args.buildTests) argv.push("TESTS=1");
  if (args.generateHeaders !== undefined) {
    argv.push(`REDISEARCH_GENERATE_HEADERS=${args.generateHeaders ? 1 : 0}`);
  }
  if (args.rustProfile) argv.push(`RUST_PROFILE=${args.rustProfile}`);
  if (args.rustDenyWarns) argv.push("RUST_DENY_WARNS=1");
  if (args.extraArgs) argv.push(...args.extraArgs);
  return argv;
}

/** Mirror build.sh's flavor cascade. Order matters: the first match wins. */
export function flavorOf(args: BuildArgs): string {
  if (args.sanitizer === "address") return "debug-asan";
  if (args.debug) return "debug";
  if (args.coverage) return "debug-cov";
  if (args.profile) return "release-profile";
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

/** Model definition wrapping the RediSearch `build.sh` script. */
export const model = {
  type: "@gdesmott/redisearch-build",
  version: "2026.08.05.2",
  description:
    "Build the RediSearch module with build.sh and capture a parsed summary",
  globalArguments: GlobalArgsSchema,
  resources: {
    summary: {
      description: "Parsed build.sh run summary",
      schema: SummarySchema,
      lifetime: "infinite",
      garbageCollection: 20,
    },
  },
  files: {
    log: {
      description: "Combined stdout and stderr from build.sh",
      contentType: "text/plain",
      lifetime: "30d",
      garbageCollection: 20,
      streaming: true,
    },
  },
  methods: {
    build: {
      description:
        "Build the module, optionally as a debug, coverage, sanitizer or profiling variant",
      arguments: BuildArgsSchema,
      execute: async (
        args: BuildArgs,
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
        const moduleName = coord === "oss"
          ? "redisearch.so"
          : "module-enterprise.so";

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
          env: buildEnv(),
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
        const parsed = parseOutput(lines);
        const failedBuild = !status.success;

        const logHandle = await logWriter.finalize();

        // Report the artifact only if it is actually on disk, and say whether
        // it can be trusted as this run's output. A failed build leaves the
        // previous module in place, so a path alone would read as success —
        // the module is there, it is just not this build's.
        //
        // Only a failed build can leave a stale artifact. An incremental build
        // that succeeded without relinking leaves an mtime older than the run,
        // and that module is current by definition: make already decided
        // nothing needed rebuilding.
        let modulePath: string | null = null;
        let moduleSizeBytes: number | null = null;
        let moduleStale = false;
        try {
          const info = await Deno.stat(`${binDir}/${moduleName}`);
          modulePath = `${binDir}/${moduleName}`;
          moduleSizeBytes = info.size;
          moduleStale = failedBuild && (info.mtime?.getTime() ?? 0) < startedAt;
        } catch {
          // No artifact at all: the first build of this variant failed.
        }

        // A cancelled run built nothing meaningful, so recording a "failed"
        // summary would misrepresent it as a compile failure. Keep the partial
        // log, but write no summary.
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
            rustProfile: parseRustProfile(lines),
            modulePath,
            moduleSizeBytes,
            moduleStale,
            coverage: args.coverage ?? false,
            sanitizer: args.sanitizer ?? null,
            testsBuilt: args.buildTests ?? false,
            exitCode: status.code,
            status: failedBuild ? "failed" : "succeeded",
            errorCount: parsed.errorCount,
            warningCount: parsed.warningCount,
            errors: parsed.errors,
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

        if (failedBuild && !args.ignoreBuildFailure) {
          const detail = parsed.errors.length > 0
            ? ` First errors: ${parsed.errors.slice(0, 3).join(" | ")}`
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
