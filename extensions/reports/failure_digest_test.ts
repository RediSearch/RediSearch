/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for the failure digest report.
 *
 * The report's whole job is to read each suite's own summary shape correctly, so
 * the fixtures here are the shapes those models really write — the same fields
 * their SummarySchema declares. A fixture that drifted from a model's schema
 * would make this suite pass while the digest reported nothing, so the field
 * names are the contract under test.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { report } from "./failure_digest.ts";

interface Step {
  stepName: string;
  modelName?: string;
  modelType?: string;
  modelId?: string;
  status: "succeeded" | "failed" | "skipped";
  dataHandles?: Array<{ name: string; version?: number }>;
}

/**
 * Build a context whose data repository serves the given summaries, keyed by
 * model id, and record what was asked for.
 *
 * `owners` maps a model id to the workflow run its summary records as its
 * writer, and adds the `findByName` the report asks that question through.
 * Omitting it leaves the repository without one, which is how the timestamp
 * fallback gets exercised — the two paths have to keep working independently.
 */
function makeContext(
  steps: Step[],
  summaries: Record<string, unknown> = {},
  workflowStatus: "succeeded" | "failed" = "failed",
  owners?: Record<string, string | null>,
) {
  const requested: Array<{ modelId: string; version?: number }> = [];

  const findByName = owners === undefined ? {} : {
    findByName: (_type: string, modelId: string) => {
      const runId = owners[modelId];
      return Promise.resolve(
        runId === undefined || runId === null
          // Present but recording no run: the report has to fall through rather
          // than read the absence as "not this run".
          ? { ownerDefinition: {} }
          : { ownerDefinition: { workflowRunId: runId } },
      );
    },
  };

  const context = {
    workflowName: "verify",
    workflowRunId: "run-1",
    workflowStatus,
    stepExecutions: steps,
    logger: { info: () => {} },
    dataRepository: {
      getContent: (
        _type: string,
        modelId: string,
        _dataName: string,
        version?: number,
      ) => {
        requested.push({ modelId, version });
        const summary = summaries[modelId];
        return Promise.resolve(
          summary === undefined
            ? null
            : new TextEncoder().encode(JSON.stringify(summary)),
        );
      },
      ...findByName,
    },
  };

  // deno-lint-ignore no-explicit-any
  return { context: context as any, requested };
}

/** A failed step wired to a summary, with the handle the model would return. */
function failedStep(
  stepName: string,
  modelName: string,
  modelType: string,
  version = 3,
): Step {
  return {
    stepName,
    modelName,
    modelType,
    modelId: modelName,
    status: "failed",
    dataHandles: [{ name: "summary", version }, { name: "log", version }],
  };
}

Deno.test("says nothing to fix for a run that succeeded", async () => {
  const { context } = makeContext(
    [{ stepName: "fmt-check", modelName: "fmt", status: "succeeded" }],
    {},
    "succeeded",
  );

  const result = await report.execute(context);

  assertEquals(result.json.findingCount, 0);
  assertEquals(result.json.firstFailedStep, null);
  assertEquals(result.markdown.includes("nothing to fix"), true);
});

Deno.test("names the steps that never ran, when it is told of any", async () => {
  // The verify chain is linear, so one failure leaves every later suite
  // unproven. Swamp reports no skipped step today — it omits them from
  // stepExecutions entirely — so this covers the shape rather than a run.
  const { context } = makeContext([
    { stepName: "fmt-check", modelName: "fmt", status: "succeeded" },
    failedStep("c-unit-tests", "c-unit-tests", "@gdesmott/c-unit-tests"),
    { stepName: "rust-tests", modelName: "rust-tests", status: "skipped" },
    { stepName: "pytest", modelName: "pytest", status: "skipped" },
  ], {
    "c-unit-tests": { failures: [], blocks: [] },
  });

  const result = await report.execute(context);

  assertEquals(result.json.firstFailedStep, "c-unit-tests");
  assertEquals(result.json.stepsNotRun, ["rust-tests", "pytest"]);
  assertEquals(result.markdown.includes("2 later steps did not run"), true);
});

Deno.test("states what a workflow report cannot see", async () => {
  // A digest that looked complete would be worse than one that says where it
  // stops: assert failures and never-run steps are not reported to it.
  const { context } = makeContext(
    [failedStep("build", "build", "@gdesmott/redisearch-build")],
    { build: { errors: ["boom"] } },
  );

  const result = await report.execute(context);

  assertEquals(
    result.markdown.includes("Only steps that ran as model methods"),
    true,
  );
  assertEquals((result.json.limits as string[]).length, 2);
  assertEquals(result.json.stepsReported, 1);
});

Deno.test("pins the read to the version a step reported", async () => {
  // A succeeded step carries handles, so the summary can be tied to this run
  // rather than to whatever ran last.
  const { context, requested } = makeContext([{
    stepName: "fmt-check",
    modelName: "fmt",
    modelType: "@gdesmott/make-fmt",
    modelId: "fmt",
    status: "failed",
    dataHandles: [{ name: "summary", version: 7 }],
  }], { fmt: { mode: "check", files: ["a.rs"], fileCount: 1 } });

  const result = await report.execute(context);

  assertEquals(requested, [{ modelId: "fmt", version: 7 }]);
  assertEquals(result.json.unpinnedReads, []);
});

Deno.test("falls back to the latest summary when a step reports no handles", async () => {
  // This is the normal case for a failed step: swamp hands the report an empty
  // dataHandles, so pinning is impossible and reading nothing would leave the
  // digest empty exactly when it is needed.
  const { context, requested } = makeContext([{
    stepName: "build",
    modelName: "build",
    modelType: "@gdesmott/redisearch-build",
    modelId: "build",
    status: "failed",
    dataHandles: [],
  }], { build: { errors: ["src/spec.c:1:1: error: boom"] } });

  const result = await report.execute(context);

  assertEquals(requested, [{ modelId: "build", version: undefined }]);
  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "build-error");
  // Said out loud, so the read is never mistaken for proof.
  assertEquals(result.json.unpinnedReads, ["build"]);
  assertEquals(
    result.markdown.includes(
      "Read from the latest summary rather than this run's",
    ),
    true,
  );
});

Deno.test("reports each build error line", async () => {
  const { context } = makeContext(
    [failedStep("build", "build", "@gdesmott/redisearch-build")],
    {
      build: {
        errorCount: 2,
        errors: [
          "src/spec.c:42:5: error: implicit declaration of function 'foo'",
          "src/spec.c:43:5: error: expected ';'",
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 2);
  assertEquals(findings[0].kind, "build-error");
  assertEquals(
    findings[0].detail,
    "src/spec.c:42:5: error: implicit declaration of function 'foo'",
  );
});

Deno.test("reports a C test assertion with its location", async () => {
  const { context } = makeContext(
    [failedStep("c-unit-tests", "c-unit-tests", "@gdesmott/c-unit-tests")],
    {
      "c-unit-tests": {
        failures: [{
          test: "InvertedIndexTest.TestBasic",
          kind: "assertion",
          file: "tests/cpptests/test_cpp_index.cpp",
          line: 120,
          detail: "Expected equality of these values:\n  1\n  2",
        }],
        blocks: [{
          name: "C Unit Tests",
          status: "failed",
          passed: 3,
          total: 4,
        }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings[0], {
    step: "c-unit-tests",
    model: "c-unit-tests",
    kind: "test-assertion",
    what: "InvertedIndexTest.TestBasic",
    where: "tests/cpptests/test_cpp_index.cpp:120",
    // Only the first line: the rest is in the log.
    detail: "Expected equality of these values:",
  });
});

Deno.test("reports a skipped test block alongside the failures", async () => {
  // A block whose binaries were never built is a different problem from a
  // failing assertion, and the easiest one to miss next to one.
  const { context } = makeContext(
    [failedStep("c-unit-tests", "c-unit-tests", "@gdesmott/c-unit-tests")],
    {
      "c-unit-tests": {
        failures: [],
        blocks: [
          { name: "C Unit Tests", status: "passed", passed: 4, total: 4 },
          {
            name: "C++ Coordinator Unit Tests",
            status: "skipped",
            passed: 0,
            total: 0,
          },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "block-skipped");
  assertEquals(findings[0].what, "C++ Coordinator Unit Tests");
});

Deno.test("keeps a miri diagnostic's own kind", async () => {
  // Undefined behaviour is not a test failure in the ordinary sense — the native
  // run passes — so flattening it into one would hide what it is.
  const { context } = makeContext(
    [failedStep("miri", "rust-tests", "@gdesmott/cargo-nextest")],
    {
      "rust-tests": {
        miri: true,
        failures: [
          {
            test: "trie_rs tests::insert",
            kind: "undefined-behavior",
            file: "src/redisearch_rs/trie_rs/src/lib.rs",
            line: 42,
            column: 9,
            detail: "Undefined Behavior: memory access failed",
          },
          {
            test: "trie_rs tests::other",
            kind: "panic",
            file: "src/redisearch_rs/trie_rs/src/lib.rs",
            line: 50,
            column: 5,
            detail: "boom",
          },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings[0].kind, "undefined-behavior");
  assertEquals(findings[0].where, "src/redisearch_rs/trie_rs/src/lib.rs:42");
  // An ordinary panic under miri is still an ordinary failure, but saying it
  // happened under the interpreter saves reproducing it natively in vain.
  assertEquals(findings[1].kind, "miri-panic");
});

Deno.test("marks a native test failure as such", async () => {
  const { context } = makeContext(
    [failedStep("rust-tests", "rust-tests", "@gdesmott/cargo-nextest")],
    {
      "rust-tests": {
        miri: false,
        failures: [{
          test: "varint tests::test_u32",
          kind: "panic",
          file: "src/redisearch_rs/varint/src/lib.rs",
          line: 10,
          column: 1,
          detail: "assertion `left == right` failed",
        }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings[0].kind, "test-panic");
});

Deno.test("keeps lint errors and drops lint warnings", async () => {
  // clippy fails the run on errors; a warning it also printed is not what broke
  // the build and would bury the error it sits next to.
  const { context } = makeContext(
    [failedStep("clippy", "clippy", "@gdesmott/rust-lint")],
    {
      clippy: {
        findings: [
          {
            level: "warning",
            code: "clippy::redundant_clone",
            message: "redundant clone",
            file: "a.rs",
            line: 1,
            column: 1,
            profile: "debug",
          },
          {
            level: "error",
            code: "clippy::unwrap_used",
            message: "used `unwrap()` on a `Result` value",
            file: "src/redisearch_rs/trie_rs/src/lib.rs",
            line: 7,
            column: 3,
            profile: "debug",
          },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "lint-error");
  assertEquals(findings[0].what, "clippy::unwrap_used");
  assertEquals(findings[0].where, "src/redisearch_rs/trie_rs/src/lib.rs:7");
});

Deno.test("reports a pytest assertion, falling back to its raw detail", async () => {
  const { context } = makeContext(
    [failedStep("pytest", "pytest", "@gdesmott/pytest")],
    {
      pytest: {
        failures: [
          {
            test: "test_search:test_basic",
            assertion: "1 == 2",
            location: "tests/pytests/test_search.py:42",
            message: null,
            raw: "assert 1 == 2",
          },
          {
            test: "test_crash:test_thread",
            assertion: null,
            location: null,
            message: null,
            raw: "redis.exceptions.ConnectionError: Connection closed",
          },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings[0].where, "tests/pytests/test_search.py:42");
  assertEquals(findings[0].detail, "1 == 2");
  // A failure with no structure to extract still has to say something.
  assertEquals(
    findings[1].detail,
    "redis.exceptions.ConnectionError: Connection closed",
  );
});

Deno.test("reports the extension tests that failed", async () => {
  const { context } = makeContext(
    [failedStep("swamp-tests", "swamp-tests", "@gdesmott/swamp-tests")],
    {
      "swamp-tests": {
        status: "failed",
        stage: "tests",
        summaryParsed: true,
        testsRun: 3,
        failed: 1,
        unformatted: [],
        failures: [
          { test: "some check", where: "./models/a_test.ts:12:6" },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "test-failure");
  assertEquals(findings[0].what, "some check");
  assertEquals(findings[0].where, "./models/a_test.ts:12:6");
});

Deno.test("reports the extension files that need reformatting", async () => {
  // The target's two checks fail in unrelated ways and the formatting one runs
  // first, so a run stopped by it has no test failures to report — naming one
  // would send the reader to a check that is passing.
  const { context } = makeContext(
    [failedStep("swamp-tests", "swamp-tests", "@gdesmott/swamp-tests")],
    {
      "swamp-tests": {
        status: "failed",
        stage: "format",
        summaryParsed: false,
        testsRun: null,
        failures: [],
        unformatted: ["models/a.ts", "reports/b.ts"],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 2);
  assertEquals(findings[0].kind, "unformatted");
  assertEquals(findings.map((f) => f.where), ["models/a.ts", "reports/b.ts"]);
});

Deno.test("reports each unformatted file", async () => {
  const { context } = makeContext(
    [failedStep("fmt-check", "fmt", "@gdesmott/make-fmt")],
    { fmt: { mode: "check", files: ["a.rs", "b.rs"], fileCount: 2 } },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 2);
  assertEquals(findings[0].kind, "unformatted");
  assertEquals(findings[0].where, "a.rs");
  assertEquals(findings[0].what, "formatting");
});

Deno.test("reports a step that succeeded while its suite failed", async () => {
  // A suite run with ignoreTestFailure — so a later step can still use its
  // output — succeeds as a step and reports failure in its summary. Keying only
  // on the step status drops exactly the failures the caller chose not to stop
  // for, which is how a coverage run can look clean with a red test in it.
  const { context } = makeContext([{
    stepName: "pytest",
    modelName: "pytest",
    modelType: "@gdesmott/pytest",
    modelId: "pytest",
    status: "succeeded",
    dataHandles: [{ name: "summary", version: 2 }],
  }], {
    pytest: {
      status: "failed",
      testsRun: 2158,
      failed: 1,
      failures: [{
        test: "test_vecsim_svs:test_queries_sanity_LVQ8_FLOAT32_L2_async",
        assertion: null,
        location: null,
        message: null,
        raw: "Exception raised during test execution. See logs",
      }],
    },
  });

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(
    findings[0].what,
    "test_vecsim_svs:test_queries_sanity_LVQ8_FLOAT32_L2_async",
  );
  assertEquals(result.json.failedSteps, []);
  assertEquals(result.json.stepsPassedWithFailures, ["pytest"]);
  assertEquals(result.json.firstFailedStep, "pytest");
  assertEquals(
    result.markdown.includes("reported failures without stopping the run"),
    true,
  );
});

Deno.test("stays quiet about a step that succeeded cleanly", async () => {
  const { context } = makeContext(
    [{
      stepName: "build",
      modelName: "build",
      modelType: "@gdesmott/redisearch-build",
      modelId: "build",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 1 }],
    }],
    { build: { status: "succeeded", errors: [] } },
    "succeeded",
  );

  const result = await report.execute(context);

  assertEquals(result.json.findingCount, 0);
  assertEquals(result.json.stepsPassedWithFailures, []);
});

Deno.test("names each file the coverage trace had no data for", async () => {
  // Not "untested": the file is not compiled into what ran, which is a build
  // problem rather than a testing one.
  const { context } = makeContext(
    [failedStep("flow-coverage", "flow-coverage", "@gdesmott/lcov-coverage")],
    {
      "flow-coverage": {
        targets: [
          { file: "src/query.c", found: true, uncoveredCount: 850 },
          { file: "src/disk_spec.c", found: false, uncoveredCount: 0 },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "no-coverage-data");
  assertEquals(findings[0].what, "src/disk_spec.c");
});

Deno.test("separates a missing coverage export from failing tests", async () => {
  const noExport = makeContext(
    [failedStep("rust-coverage", "rust-coverage", "@gdesmott/rust-coverage")],
    { "rust-coverage": { scope: "workspace", parsed: false } },
  );
  const testsFailed = makeContext(
    [failedStep("rust-coverage", "rust-coverage", "@gdesmott/rust-coverage")],
    { "rust-coverage": { scope: "trie_rs", parsed: true } },
  );

  const first = await report.execute(noExport.context);
  const second = await report.execute(testsFailed.context);

  const a = first.json.findings as Array<Record<string, unknown>>;
  const b = second.json.findings as Array<Record<string, unknown>>;
  // Nothing measured is a different problem from a measurement that is a floor.
  assertEquals(a[0].kind, "no-coverage-export");
  assertEquals(b[0].kind, "tests-failed-under-coverage");
  assertEquals(b[0].what, "trie_rs");
});

Deno.test("reports a failed assert step as a gate", async () => {
  // An assert step has no model and so no summary. Skipping it would drop the
  // one check that catches a suite passing without running anything.
  const { context } = makeContext([
    { stepName: "pytest-ran", status: "failed" },
  ]);

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "gate");
  assertEquals(findings[0].what, "pytest-ran");
  assertEquals(findings[0].model, null);
});

Deno.test("keeps a failed step that reported no individual failure", async () => {
  const { context } = makeContext(
    [failedStep("pytest", "pytest", "@gdesmott/pytest")],
    { pytest: { failures: [], timedOut: true } },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "step-failed");
  assertEquals(String(findings[0].detail).includes("timeout"), true);
});

Deno.test("keeps a failed step whose summary is missing", async () => {
  // No handle, so nothing to read — the step still failed.
  const { context } = makeContext([{
    stepName: "build",
    modelName: "build",
    modelType: "@gdesmott/redisearch-build",
    modelId: "build",
    status: "failed",
    dataHandles: [],
  }]);

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "step-failed");
});

Deno.test("keeps a failed step of an unknown model type", async () => {
  // A model added later degrades to a coarser entry rather than vanishing.
  const { context } = makeContext(
    [failedStep("coverage", "coverage", "@gdesmott/future-model")],
    { coverage: { status: "failed" } },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(
    String(findings[0].detail).includes("@gdesmott/future-model"),
    true,
  );
});

Deno.test("says how many findings the table left out", async () => {
  // A truncated table with no note reads as the whole story.
  const failures = Array.from({ length: 30 }, (_, i) => ({
    test: `varint tests::t${i}`,
    kind: "panic",
    file: "a.rs",
    line: i,
    column: 1,
    detail: "boom",
  }));
  const { context } = makeContext(
    [failedStep("rust-tests", "rust-tests", "@gdesmott/cargo-nextest")],
    { "rust-tests": { miri: false, failures } },
  );

  const result = await report.execute(context);

  assertEquals(result.json.findingCount, 30);
  assertEquals(result.markdown.includes("5 further findings omitted"), true);
  assertEquals(result.markdown.includes("all 30"), true);
});

Deno.test("escapes a detail that would break the table", async () => {
  const { context } = makeContext(
    [failedStep("rust-tests", "rust-tests", "@gdesmott/cargo-nextest")],
    {
      "rust-tests": {
        miri: false,
        failures: [{
          test: "a | b",
          kind: "panic",
          file: "a.rs",
          line: 1,
          column: 1,
          detail: "left | right",
        }],
      },
    },
  );

  const result = await report.execute(context);

  const row = result.markdown.split("\n").find((l) => l.includes("a \\| b"));
  assertEquals(row?.includes("left \\| right"), true);
});

Deno.test("reports skipped blocks from a suite that exited cleanly", async () => {
  // The C suite whose binaries were never built skips every block and still
  // exits 0, so both the step and its summary say success. The workflow catches
  // it with an assert step, but stepExecutions omits failed asserts — so if the
  // digest keyed on failure alone, the run would fail with nothing to act on
  // while the explanation sat in the summary beside it.
  const { context } = makeContext(
    [{
      stepName: "c-unit-tests",
      modelName: "c-unit-tests",
      modelType: "@gdesmott/c-unit-tests",
      modelId: "c-unit-tests",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 3 }],
    }],
    {
      "c-unit-tests": {
        status: "passed",
        testsRun: 0,
        failures: [],
        blocks: [
          { name: "C tests", status: "skipped" },
          { name: "C++ tests", status: "skipped" },
        ],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 2);
  assertEquals(findings[0].kind, "block-skipped");
  assertEquals(findings[0].what, "C tests");
  // It succeeded, so it belongs with the steps that passed while reporting
  // something rather than among the failures.
  assertEquals(result.json.failedSteps, []);
  assertEquals(result.json.stepsPassedWithFailures, ["c-unit-tests"]);
});

Deno.test("stays silent about steps that passed with nothing to report", async () => {
  // Translating every step that ran only works if a clean summary yields
  // nothing. The build and coverage translators are the ones at risk: their
  // failure mode is an absence — no error lines, no export — which reads the
  // same on a healthy run.
  const { context } = makeContext(
    [
      {
        stepName: "build",
        modelName: "build",
        modelType: "@gdesmott/redisearch-build",
        modelId: "build",
        status: "succeeded",
        dataHandles: [{ name: "summary", version: 1 }],
      },
      {
        stepName: "rust-coverage",
        modelName: "rust-coverage",
        modelType: "@gdesmott/rust-coverage",
        modelId: "rust-coverage",
        status: "succeeded",
        dataHandles: [{ name: "summary", version: 1 }],
      },
    ],
    {
      build: { status: "succeeded", errors: [] },
      "rust-coverage": { status: "passed", scope: "workspace", parsed: true },
    },
  );

  const result = await report.execute(context);

  assertEquals(result.json.findingCount, 0);
  assertEquals(result.json.stepsPassedWithFailures, []);
  assertEquals(result.json.firstFailedStep, null);
});

Deno.test("keeps legitimate skips out of a filtered unit run", async () => {
  // cTestFilter deliberately leaves the other blocks unrun, and the workflow's
  // own assert makes the same exception. Reporting them would turn a filtered
  // run's intended skips into problems — and, since it ran first, would name
  // c-unit-tests as where it went wrong while the real failure came later.
  const { context } = makeContext(
    [
      {
        stepName: "c-unit-tests",
        modelName: "c-unit-tests",
        modelType: "@gdesmott/c-unit-tests",
        modelId: "c-unit-tests",
        status: "succeeded",
        dataHandles: [{ name: "summary", version: 1 }],
      },
      failedStep("clippy", "clippy", "@gdesmott/rust-lint"),
    ],
    {
      "c-unit-tests": {
        status: "passed",
        testFilter: "test_blkalloc",
        testsRun: 1,
        failures: [],
        failedTests: [],
        blocks: [
          { name: "C tests", status: "passed" },
          { name: "C++ tests", status: "skipped" },
        ],
      },
      clippy: {
        findings: [{ level: "error", code: "E0433", message: "boom" }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.every((f) => f.kind !== "block-skipped"), true);
  assertEquals(result.json.firstFailedStep, "clippy");
});

Deno.test("names a failing C binary that carried no structured failure", async () => {
  // A plain C binary reports only its name under a failed block. Reading
  // `failures` alone dropped it and fell back to a generic step failure, losing
  // the one identifier the summary had.
  const { context } = makeContext(
    [failedStep("c-unit-tests", "c-unit-tests", "@gdesmott/c-unit-tests")],
    {
      "c-unit-tests": {
        status: "failed",
        testFilter: null,
        failures: [],
        failedTests: ["test_blkalloc"],
        blocks: [{ name: "C tests", status: "failed" }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].what, "test_blkalloc");
  assertEquals(findings[0].kind, "test-failure");
});

Deno.test("names a pytest failure that had no detail row", async () => {
  // RLTest can list a test as failed without printing a detail line for it —
  // an error rather than an assertion. The name is still the useful part.
  const { context } = makeContext(
    [failedStep("pytest", "pytest", "@gdesmott/pytest")],
    {
      "pytest": {
        status: "failed",
        failures: [{ test: "test_a", location: "t.py:1", assertion: "boom" }],
        failedTests: ["test_a", "test_b"],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 2);
  // The one with details keeps them; the bare name is reported rather than lost.
  assertEquals(findings[0].detail, "boom");
  assertEquals(findings[1].what, "test_b");
});

Deno.test("reports a filtered suite that matched no test", async () => {
  // rust-quick with a filter matching nothing: nextest exits 0, records
  // testsRun 0 and no failures. The tests-ran assert fails the workflow, but
  // assert steps never reach this report — so without this the run gates on
  // exactly this condition and arrives with nothing to act on.
  const { context } = makeContext(
    [{
      stepName: "rust-tests",
      modelName: "rust-tests",
      modelType: "@gdesmott/cargo-nextest",
      modelId: "rust-tests",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 2 }],
    }],
    {
      "rust-tests": {
        status: "passed",
        testsRun: 0,
        failures: [],
        failedTests: [],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "no-tests-ran");
  assertEquals(findings[0].what, "rust tests");
});

Deno.test("reports a suite whose summary line was never recognised", async () => {
  // The same silence as a filter matching nothing, from the opposite cause: the
  // suite exited 0 and printed something the parser did not recognise, so
  // testsRun is unknown rather than zero. The tests-ran assert catches it and
  // then never reaches this report, so the digest has to say it.
  const { context } = makeContext(
    [{
      stepName: "pytest",
      modelName: "pytest",
      modelType: "@gdesmott/pytest",
      modelId: "pytest",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 2 }],
    }],
    {
      pytest: {
        status: "passed",
        summaryParsed: false,
        testsRun: null,
        failures: [],
        failedTests: [],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "summary-unparsed");
  assertEquals(findings[0].what, "pytest");
});

Deno.test("keeps the failed-step reason for a suite that never reached a summary", async () => {
  // The same unparsed summary from the opposite cause: nextest failed to
  // compile, so there was no summary line to recognise. Saying the suite
  // "exited successfully" would be false, and it would displace the fallback
  // below, which is the only thing that names a timeout as one.
  const { context } = makeContext(
    [{
      stepName: "rust-tests",
      modelName: "rust-tests",
      modelType: "@gdesmott/cargo-nextest",
      modelId: "rust-tests",
      status: "failed",
      dataHandles: [{ name: "summary", version: 2 }],
    }],
    {
      "rust-tests": {
        status: "failed",
        summaryParsed: false,
        testsRun: null,
        timedOut: true,
        failures: [],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "step-failed");
  assertEquals(
    String(findings[0].detail).includes("aborted by its timeout"),
    true,
  );
});

Deno.test("stays quiet about a suite that parsed and ran tests", async () => {
  // The guard is keyed on summaryParsed, so a summary that simply predates the
  // field must not be read as a parse failure.
  const { context } = makeContext(
    [{
      stepName: "rust-tests",
      modelName: "rust-tests",
      modelType: "@gdesmott/cargo-nextest",
      modelId: "rust-tests",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 2 }],
    }],
    { "rust-tests": { status: "passed", testsRun: 412, failures: [] } },
  );

  const result = await report.execute(context);

  assertEquals(result.json.findingCount, 0);
});

Deno.test("prefers the specific reason over saying no tests ran", async () => {
  // The C suite reports both: zero tests, and the skipped blocks that explain
  // why. The blocks are the actionable half, so they win.
  const { context } = makeContext(
    [{
      stepName: "c-unit-tests",
      modelName: "c-unit-tests",
      modelType: "@gdesmott/c-unit-tests",
      modelId: "c-unit-tests",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 1 }],
    }],
    {
      "c-unit-tests": {
        status: "passed",
        testFilter: null,
        testsRun: 0,
        failures: [],
        failedTests: [],
        blocks: [{ name: "C tests", status: "skipped" }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "block-skipped");
});

Deno.test("keeps tolerated missing coverage files out of the digest", async () => {
  // requireAllFound: false asks the model to report on what it found and
  // succeed regardless, so the missing targets are there by consent. The
  // summary does not record the choice, but the step succeeding encodes it.
  const { context } = makeContext(
    [{
      stepName: "coverage-report",
      modelName: "flow-coverage",
      modelType: "@gdesmott/lcov-coverage",
      modelId: "flow-coverage",
      status: "succeeded",
      dataHandles: [{ name: "summary", version: 1 }],
    }],
    {
      "flow-coverage": {
        targets: [
          { file: "src/spec.c", found: true },
          { file: "src/absent.c", found: false },
        ],
      },
    },
  );

  const result = await report.execute(context);

  assertEquals(result.json.findingCount, 0);
  assertEquals(result.json.firstFailedStep, null);
});

Deno.test("still names missing files when the coverage step failed", async () => {
  const { context } = makeContext(
    [failedStep("coverage-report", "flow-coverage", "@gdesmott/lcov-coverage")],
    {
      "flow-coverage": {
        targets: [{ file: "src/absent.c", found: false }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "no-coverage-data");
  assertEquals(findings[0].what, "src/absent.c");
});

Deno.test("discards a summary the timestamps show predates this run", async () => {
  // A model that throws before writing a summary — the lint model rejecting
  // stale headers — leaves the previous run's summary as the model's latest.
  // A failed step carries no data handles, so the read falls back to exactly
  // that, and translating it would report an earlier run's findings as this
  // run's. The build step succeeded, so its pinned summary dates the run.
  const { context } = makeContext(
    [
      {
        stepName: "build",
        modelName: "build",
        modelType: "@gdesmott/redisearch-build",
        modelId: "build",
        status: "succeeded",
        dataHandles: [{ name: "summary", version: 4 }],
      },
      // A real failed step carries no data handles at all, which is what makes
      // the read fall back to the model's latest.
      {
        stepName: "clippy",
        modelName: "clippy",
        modelType: "@gdesmott/rust-lint",
        modelId: "clippy",
        status: "failed",
      },
    ],
    {
      build: {
        status: "succeeded",
        errors: [],
        executedAt: "2026-08-03T10:00:00.000Z",
      },
      clippy: {
        status: "failed",
        executedAt: "2026-08-02T09:00:00.000Z",
        findings: [{ level: "error", code: "E0433", message: "yesterday" }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  // The stale diagnostic is gone; the step is still reported, from itself.
  assertEquals(findings.length, 1);
  assertEquals(findings[0].step, "clippy");
  assertEquals(findings[0].kind, "step-failed");
  assertEquals(
    String(findings[0].detail).includes("belongs to an earlier run"),
    true,
  );
  assertEquals(result.json.staleReads, ["clippy"]);
  assertEquals(result.json.unpinnedReads, []);
});

Deno.test("keeps an unpinned summary recorded during this run", async () => {
  // The common case the unpinned read exists for: the model did write a summary,
  // it is simply unpinned because a failed step reports no handles. Discarding
  // it would gut the report.
  const { context } = makeContext(
    [
      {
        stepName: "build",
        modelName: "build",
        modelType: "@gdesmott/redisearch-build",
        modelId: "build",
        status: "succeeded",
        dataHandles: [{ name: "summary", version: 4 }],
      },
      {
        stepName: "clippy",
        modelName: "clippy",
        modelType: "@gdesmott/rust-lint",
        modelId: "clippy",
        status: "failed",
      },
    ],
    {
      build: {
        status: "succeeded",
        errors: [],
        executedAt: "2026-08-03T10:00:00.000Z",
      },
      clippy: {
        status: "failed",
        executedAt: "2026-08-03T10:05:00.000Z",
        findings: [{ level: "error", code: "E0433", message: "this run" }],
      },
    },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].detail, "this run");
  assertEquals(result.json.staleReads, []);
  assertEquals(result.json.unpinnedReads, ["clippy"]);
});

/** The one failed step is the only one that ran: nothing to date it against. */
function loneFailure(): Step[] {
  return [{
    stepName: "clippy",
    modelName: "clippy",
    modelType: "@gdesmott/rust-lint",
    modelId: "clippy",
    status: "failed",
  }];
}

/** A lint summary from a run that is not this one. */
const YESTERDAYS_LINT = {
  clippy: {
    status: "failed",
    executedAt: "2026-08-02T09:00:00.000Z",
    findings: [{ level: "error", code: "E0433", message: "yesterday" }],
  },
};

Deno.test("discards an earlier run's summary when it is the only step that ran", async () => {
  // The timestamp floor cannot reach this: it is built from the summaries of
  // steps that succeeded, and here nothing succeeded. The ownership record is
  // the only thing that knows, and without it the digest reports an earlier
  // run's findings as this run's.
  const { context } = makeContext(
    loneFailure(),
    YESTERDAYS_LINT,
    "failed",
    { clippy: "run-0" },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "step-failed");
  assertEquals(result.json.staleReads, ["clippy"]);
});

Deno.test("keeps a lone failed step's summary when it names this run", async () => {
  // The mirror image, and the far commoner one: the model did write during this
  // run. Treating an undatable summary as stale would empty the digest for
  // every run whose first step is the one that failed.
  const { context } = makeContext(
    loneFailure(),
    YESTERDAYS_LINT,
    "failed",
    { clippy: "run-1" },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "lint-error");
  assertEquals(result.json.staleReads, []);
  assertEquals(result.json.unpinnedReads, ["clippy"]);
});

Deno.test("reports a lone failed step when nothing records a run", async () => {
  // Ownership metadata that names no run answers nothing, and there is no floor
  // either. The existing behaviour has to stand: report it, and say the read was
  // unpinned rather than silently dropping the only evidence there is.
  const { context } = makeContext(
    loneFailure(),
    YESTERDAYS_LINT,
    "failed",
    { clippy: null },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "lint-error");
  assertEquals(result.json.staleReads, []);
  assertEquals(result.json.unpinnedReads, ["clippy"]);
});

Deno.test("prefers the ownership record over the timestamp floor", async () => {
  // A summary older than the floor, but the data says this run wrote it — a
  // clock skew, or a step whose model started before the one that dated the
  // run. The direct answer wins over the approximation.
  const { context } = makeContext(
    [
      {
        stepName: "build",
        modelName: "build",
        modelType: "@gdesmott/redisearch-build",
        modelId: "build",
        status: "succeeded",
        dataHandles: [{ name: "summary", version: 4 }],
      },
      ...loneFailure(),
    ],
    {
      build: {
        status: "succeeded",
        errors: [],
        executedAt: "2026-08-03T10:00:00.000Z",
      },
      ...YESTERDAYS_LINT,
    },
    "failed",
    { build: "run-1", clippy: "run-1" },
  );

  const result = await report.execute(context);

  const findings = result.json.findings as Array<Record<string, unknown>>;
  assertEquals(findings.length, 1);
  assertEquals(findings[0].kind, "lint-error");
  assertEquals(result.json.staleReads, []);
});
