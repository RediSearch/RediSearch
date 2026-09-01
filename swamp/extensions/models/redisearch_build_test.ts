/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the build output parser, command construction and variant guards.
 *
 * The failure fixtures are the ones that matter: a caller branches on
 * `errorCount` and `errors`, and a successful build never produces either.
 *
 * @module
 */
import { assertEquals, assertThrows } from "jsr:@std/assert@1";
import {
  buildArgv,
  buildEnv,
  flavorOf,
  parseOutput,
  parseRustProfile,
  validate,
} from "./redisearch_build.ts";

Deno.test("counts nothing for a clean build", () => {
  const parsed = parseOutput([
    "[ 66%] Building C object src/CMakeFiles/rscore.dir/query.c.o",
    "[100%] Built target redisearch",
    "Build complete. Artifacts in /repo/bin/linux-x64-debug/search-community",
  ]);

  assertEquals(parsed.errorCount, 0);
  assertEquals(parsed.warningCount, 0);
  assertEquals(parsed.errors, []);
});

Deno.test("extracts gcc and clang error lines with their locations", () => {
  const parsed = parseOutput([
    "src/query.c:120:5: error: expected ';' before '}' token",
    "src/spec.c:44:1: warning: unused variable 'x' [-Wunused-variable]",
    "src/doc_table.c:9:3: fatal error: missing.h: No such file or directory",
    "make[2]: *** [CMakeFiles/rscore.dir/query.c.o] Error 1",
  ]);

  assertEquals(parsed.errorCount, 2);
  assertEquals(parsed.warningCount, 1);
  assertEquals(parsed.errors, [
    "src/query.c:120:5: error: expected ';' before '}' token",
    "src/doc_table.c:9:3: fatal error: missing.h: No such file or directory",
  ]);
});

Deno.test("recognises a rustc diagnostic code as an error", () => {
  const parsed = parseOutput([
    "error[E0433]: failed to resolve: use of undeclared crate `foo`",
    "warning[unused_imports]: unused import: `std::mem`",
  ]);

  assertEquals(parsed.errorCount, 1);
  assertEquals(parsed.warningCount, 1);
});

Deno.test("extracts a link failure named by its tool", () => {
  const parsed = parseOutput([
    "ld.lld: error: undefined symbol: RediSearch_Init",
    "/usr/bin/ld: warning: relocation against `x' in read-only section",
    "collect2: error: ld returned 1 exit status",
    "make[2]: *** [CMakeFiles/redisearch.dir/link.txt] Error 1",
  ]);

  // A link failure has no source position to name, so it prints the tool's
  // name instead — the one shape the compiler-location prefix cannot match.
  assertEquals(parsed.errorCount, 2);
  assertEquals(parsed.warningCount, 1);
  assertEquals(
    parsed.errors[0],
    "ld.lld: error: undefined symbol: RediSearch_Init",
  );
});

Deno.test("does not read make's own failure line as an error", () => {
  // The tool prefix must stay narrow enough that make's summary, which every
  // failed build ends with, is not counted as a second failure.
  const parsed = parseOutput([
    "make[2]: *** [CMakeFiles/rscore.dir/query.c.o] Error 1",
    "make: *** [all] Error 2",
    "-- Configuring incomplete, errors occurred!",
  ]);

  assertEquals(parsed.errorCount, 0);
});

Deno.test("extracts a configure-time CMake error with its message", () => {
  const parsed = parseOutput([
    "-- Configuring RediSearch",
    "CMake Error at CMakeLists.txt:112 (message):",
    "  VectorSimilarity submodule is missing.  Run git submodule update.",
    "",
    "",
    "-- Configuring incomplete, errors occurred!",
  ]);

  // A configure failure never reaches a compiler, so this is the only thing
  // the digest has to go on.
  assertEquals(parsed.errorCount, 1);
  assertEquals(parsed.errors, [
    "CMake Error at CMakeLists.txt:112 (message): " +
    "VectorSimilarity submodule is missing.  Run git submodule update.",
  ]);
});

Deno.test("counts a CMake warning without listing it as an error", () => {
  const parsed = parseOutput([
    "CMake Warning (dev) at deps/CMakeLists.txt:4 (find_package):",
    "  Policy CMP0148 is not set.",
    "CMake Error:",
    "  Could not find CMAKE_ROOT!!!",
  ]);

  assertEquals(parsed.warningCount, 1);
  assertEquals(parsed.errorCount, 1);
  assertEquals(parsed.errors, ["CMake Error: Could not find CMAKE_ROOT!!!"]);
});

Deno.test("does not double-count compiler-shaped lines inside a CMake block", () => {
  const parsed = parseOutput([
    "CMake Error at cmake/Toolchain.cmake:9 (message):",
    "  the probe reported: error: no such flag",
    "src/query.c:1:1: error: real one",
  ]);

  // The indented line belongs to the CMake diagnostic above it; counting it
  // again would inflate errorCount past the number of actual failures.
  assertEquals(parsed.errorCount, 2);
  assertEquals(parsed.errors[1], "src/query.c:1:1: error: real one");
});

Deno.test("keeps at most a sample of errors, leaving the rest to the log", () => {
  const parsed = parseOutput(
    Array.from({ length: 50 }, (_, i) => `src/f${i}.c:1:1: error: boom ${i}`),
  );

  assertEquals(parsed.errorCount, 50);
  // The count stays exact even though only the first few are carried in the
  // summary, so a caller can tell the sample is partial.
  assertEquals(parsed.errors.length, 20);
  assertEquals(parsed.errors[0], "src/f0.c:1:1: error: boom 0");
});

Deno.test("clears the build.sh controls it decides", () => {
  // A caller who exported SKIP_BUILD=1 — a CI shell, an earlier test-only
  // invocation — would otherwise have this model report a build it never ran,
  // and every later gate would test whatever was already on disk. SKIP_BUILD
  // has no argument form, so argv cannot answer it.
  const env = buildEnv();

  assertEquals(env.SKIP_BUILD, "");
  assertEquals(env.COV, "");
  // SAN is the other one build.sh never initialises: exported, it instruments
  // a build the summary calls release and puts it somewhere this model does
  // not name.
  assertEquals(env.SAN, "");
  // Empty is how build.sh spells unset: every one of these is read as
  // `${VAR:-<default>}`, so clearing restores its default rather than a value
  // repeated here.
  assertEquals(env.REDISEARCH_GENERATE_HEADERS, "");
  // The machine's own settings are left alone: a developer who exported one
  // meant it, and none of them contradicts the summary.
  assertEquals("INLINE_LSE_ATOMICS" in env, false);
  assertEquals("BUILD_INTEL_SVS_OPT" in env, false);
});

Deno.test("lets an argument override the control it cleared", () => {
  // Clearing COV must not fight the request: build.sh applies the environment
  // as a default and then parses argv over it.
  assertEquals(buildEnv().COV, "");
  assertEquals(
    buildArgv({ coverage: true } as never, "oss").includes("COV=1"),
    true,
  );
});

Deno.test("mirrors build.sh's flavor cascade", () => {
  assertEquals(flavorOf({} as never), "release");
  assertEquals(flavorOf({ debug: true } as never), "debug");
  assertEquals(flavorOf({ coverage: true } as never), "debug-cov");
  assertEquals(flavorOf({ sanitizer: "address" } as never), "debug-asan");
  assertEquals(flavorOf({ profile: true } as never), "release-profile");
});

Deno.test("builds the argument vector for a coverage build", () => {
  assertEquals(buildArgv({ coverage: true } as never, "oss"), [
    "COORD=oss",
    "COV=1",
  ]);
});

Deno.test("passes the header generation flag only when set explicitly", () => {
  // Unset must leave build.sh's own default alone rather than forcing either
  // value.
  assertEquals(
    buildArgv({} as never, "oss").some((a) =>
      a.startsWith("REDISEARCH_GENERATE_HEADERS")
    ),
    false,
  );
  assertEquals(
    buildArgv({ generateHeaders: false } as never, "oss").includes(
      "REDISEARCH_GENERATE_HEADERS=0",
    ),
    true,
  );
});

Deno.test("rejects variant combinations build.sh resolves surprisingly", () => {
  // build.sh checks DEBUG before COV, so this would land a coverage build in
  // the plain debug directory where an ordinary debug build would overwrite it.
  assertThrows(
    () => validate({ debug: true, coverage: true } as never),
    Error,
    "coverage cannot be combined with debug",
  );
  assertThrows(
    () => validate({ coverage: true, sanitizer: "address" } as never),
    Error,
    "coverage cannot be combined with a sanitizer",
  );
  assertThrows(
    () => validate({ profile: true, debug: true } as never),
    Error,
    "profile cannot be combined",
  );
});

Deno.test("allows a sanitizer together with debug", () => {
  // Both resolve to the same debug-asan directory, so there is nothing to
  // clobber and no reason to refuse it.
  validate({ debug: true, sanitizer: "address" } as never);
});

Deno.test("reads the cargo profile out of the echoed cmake command", () => {
  // build.sh derives the profile from DEBUG, TESTS, COV, SAN and miri together
  // and prints it as part of the cmake command. Reading it back is what keeps
  // the derivation in one place.
  assertEquals(
    parseRustProfile([
      "Configuring CMake...",
      "cmake /repo -DCMAKE_BUILD_TYPE=Release -DRUST_PROFILE=optimised_test -DBUILD_SEARCH_UNIT_TESTS=ON",
      "[100%] Built target redisearch",
    ]),
    "optimised_test",
  );
});

Deno.test("reports no cargo profile when the command was never printed", () => {
  // A SKIP_BUILD run configures nothing, so there is no profile to report and
  // guessing one would be worse than saying so.
  assertEquals(parseRustProfile(["[100%] Built target redisearch"]), null);
});
