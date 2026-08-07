/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Turns a failed workflow run into one list of things to fix.
 *
 * Every suite model writes a well-structured summary, but each writes its own
 * shape: the build reports error lines, the C tests report blocks and
 * assertions, nextest reports panics and miri diagnostics, clippy reports
 * findings, pytest reports assertions, fmt reports paths. Reading a failure
 * therefore means knowing which model failed, which of those shapes it produces,
 * and where in the data it lives — before any of it can be acted on.
 *
 * This report does that walk once and emits a single normalised list, plus the
 * step that failed first.
 *
 * Two limits come from what a workflow-scope report is given, not from choices
 * here, and both are stated in the output rather than papered over:
 *
 * - `stepExecutions` holds only steps that ran as model methods. A failed assert
 *   step, and a step that never ran, are absent — so the digest cannot count
 *   them. The workflow's own output reports both.
 * - A *failed* step arrives with an empty `dataHandles`, even though its model
 *   did write a summary. Pinning the read to the version this run produced is
 *   therefore impossible for exactly the steps this report exists for, so those
 *   reads fall back to the model's latest summary and are named in the output.
 *   The data's own ownership record still says which run wrote it, so a summary
 *   left behind by an earlier run is discarded rather than reported as this
 *   run's — but that record is not guaranteed to be there, so the fallback is a
 *   timestamp comparison against the earliest summary this run is known to have
 *   written.
 *
 * @module
 */

/** Rows kept in the markdown table. The JSON carries every finding. */
const MAX_ROWS = 25;

/** One thing to fix, normalised across the suites' summary shapes. */
interface Finding {
  /** Workflow step that produced it. */
  step: string;
  /** Model instance that ran, or null for a step with no model. */
  model: string | null;
  /**
   * What kind of problem it is, in the vocabulary of the suite that found it:
   * a compiler error, a failing test, undefined behaviour miri caught, a lint
   * finding, an unformatted file, or a failed assertion on the run itself.
   */
  kind: string;
  /** What it concerns: a test name, a source file, a lint profile. */
  what: string;
  /** Source location as `file:line`, when the suite reported one. */
  where: string | null;
  /** What the suite said about it. */
  detail: string;
}

/** The subset of a step execution this report reads. */
interface StepExecution {
  stepName: string;
  modelName?: string;
  modelType?: string;
  modelId?: string;
  status: "succeeded" | "failed" | "skipped";
  dataHandles?: Array<{ name: string; version?: number }>;
}

/** The subset of the report context this report reads. */
interface Context {
  workflowName: string;
  workflowRunId: string;
  workflowStatus: "succeeded" | "failed";
  stepExecutions: StepExecution[];
  logger: { info: (msg: string, props?: unknown) => void };
  dataRepository: {
    getContent: (
      type: string,
      modelId: string,
      dataName: string,
      version?: number,
    ) => Promise<Uint8Array | null>;
    /**
     * Metadata for the same data, which carries who wrote it. Optional so a
     * context that offers only `getContent` still works: the ownership check
     * below degrades to the timestamp comparison rather than failing.
     */
    findByName?: (
      type: string,
      modelId: string,
      dataName: string,
      version?: number,
    ) => Promise<{ ownerDefinition?: { workflowRunId?: string } } | null>;
  };
}

/** A summary read from a step, with unknown fields until narrowed per type. */
type Summary = Record<string, unknown>;

/** Read a field as an array, tolerating a summary that predates it. */
function list(summary: Summary, field: string): Record<string, unknown>[] {
  const value = summary[field];
  return Array.isArray(value) ? value : [];
}

/** Read a field as a string array, tolerating a summary that predates it. */
function strings(summary: Summary, field: string): string[] {
  const value = summary[field];
  return Array.isArray(value) ? value.filter((v) => typeof v === "string") : [];
}

/** Read a nullable string field. */
function str(entry: Record<string, unknown>, field: string): string | null {
  const value = entry[field];
  return typeof value === "string" && value.length > 0 ? value : null;
}

/** Format a file and line as `file:line`, dropping either if absent. */
function location(
  file: string | null | undefined,
  line: unknown,
): string | null {
  if (typeof file !== "string" || file.length === 0) return null;
  return typeof line === "number" ? `${file}:${line}` : file;
}

/** Keep a detail to its first line: the rest belongs in the log. */
function firstLine(detail: unknown): string {
  return typeof detail === "string" ? detail.split("\n")[0] : "";
}

/**
 * A finding for a suite that ran no tests, or nothing when it ran some.
 *
 * A filter matching nothing is the quiet failure every suite here shares: the
 * runner exits 0 having done no work, so neither the step nor its summary says
 * anything is wrong. Each verify-family workflow guards it with a `*-ran`
 * assert, but a failed assert step never reaches this report — so unless the
 * translator says it, a run that gated on exactly this arrives with nothing to
 * act on.
 *
 * A summary that was never parsed is the same silence with a different cause,
 * and gets its own finding: the runner exited cleanly having printed something
 * the parser did not recognise, so the counts are not zero but unknown. Keyed
 * on `summaryParsed` rather than on a null `testsRun`, so a summary written
 * before either field existed is not read as a parse failure.
 *
 * `failed` is why that case is not reported on its own. A summary also goes
 * unparsed when the run never got as far as a summary — a compile error, a
 * runner that aborted on startup, a timeout — and there the step's own failure
 * is the finding. Claiming a clean exit for it would be wrong, and it would
 * displace the caller's fallback, which distinguishes a timeout from the rest.
 */
function noTestsRan(
  summary: Summary,
  base: (f: Partial<Finding>) => Finding,
  what: string,
  failed: boolean,
): Finding[] {
  if (summary.summaryParsed === false) {
    return failed ? [] : [base({
      kind: "summary-unparsed",
      what,
      detail: "the suite exited successfully but its summary line was not " +
        "recognised, so what it ran is unknown; see the log",
    })];
  }
  if (summary.testsRun !== 0) return [];
  return [base({
    kind: "no-tests-ran",
    what,
    detail:
      "the suite exited successfully without running a test; the filter " +
      "probably matched nothing",
  })];
}

/**
 * Translate one suite's summary into findings.
 *
 * Keyed by model type, because that is what decides the shape. A type with no
 * translator still yields a finding from the step itself, so a model added later
 * degrades to a coarser entry rather than disappearing from the digest.
 *
 * A translator is asked about every step that ran, not only the ones that
 * failed, because a suite can exit cleanly having done nothing worth calling a
 * success. So the contract is: **enumerate what is wrong and return nothing when
 * nothing is**. Emitting a finding unconditionally would turn every green run
 * into a page of phantom failures.
 *
 * `failed` says whether the step is already known to have gone wrong, which the
 * caller determines and a translator must not try to re-derive. Most do not need
 * it — a list of failing tests describes itself. The two whose failure mode is an
 * *absence* do: no error lines and no coverage figures read identically on a
 * healthy run and a broken one.
 */
const TRANSLATORS: Record<
  string,
  (
    summary: Summary,
    base: (f: Partial<Finding>) => Finding,
    failed: boolean,
  ) => Finding[]
> = {
  "@gdesmott/redisearch-build": (summary, base, failed) => {
    // The build keeps only the first error lines, unstructured, so each becomes
    // a finding as printed. errorCount says how many there really were.
    const errors = strings(summary, "errors");
    if (errors.length > 0) {
      return errors.map((error) =>
        base({ kind: "build-error", what: "build", detail: error })
      );
    }
    // No error lines means one of two opposite things, and only the step's
    // outcome tells them apart: a build that failed in a way the parser did not
    // recognise, or a build that simply worked.
    return failed
      ? [
        base({
          kind: "build-failure",
          what: "build",
          detail:
            "the build failed without printing a recognisable error; see the log",
        }),
      ]
      : [];
  },

  "@gdesmott/c-unit-tests": (summary, base, failed) => {
    const findings = list(summary, "failures").map((failure) =>
      base({
        kind: `test-${str(failure, "kind") ?? "failure"}`,
        what: str(failure, "test") ?? "unknown test",
        where: location(str(failure, "file"), failure.line),
        detail: firstLine(failure.detail),
      })
    );
    // A plain C binary reports its failure as a name under a failed block and
    // nothing else — only the gtest blocks produce a structured entry. Naming
    // those too keeps the digest from throwing away a test the summary already
    // identified and falling back to "something failed, read the log".
    const detailed = new Set(
      list(summary, "failures").map((failure) => str(failure, "test")),
    );
    for (const test of strings(summary, "failedTests")) {
      if (detailed.has(test)) continue;
      findings.push(base({
        kind: "test-failure",
        what: test,
        detail:
          "reported as failing by its block; see the log for the assertion",
      }));
    }
    // A skipped block means its binaries were never built, which is a different
    // problem from a failing assertion and easy to miss next to one — unless the
    // run was filtered, where skipping the blocks the filter excluded is the
    // whole point. The workflow's own assert makes the same exception, and
    // without it a filtered run reports its intended skips as problems and
    // becomes the first one listed.
    if (str(summary, "testFilter") === null) {
      for (const block of list(summary, "blocks")) {
        if (str(block, "status") !== "skipped") continue;
        findings.push(base({
          kind: "block-skipped",
          what: str(block, "name") ?? "unknown block",
          detail: "block did not run; its binaries were probably not built",
        }));
      }
    }
    // Only when nothing more specific was found: the skipped blocks above say
    // why no test ran, which is more use than saying that none did.
    return findings.length > 0
      ? findings
      : noTestsRan(summary, base, "C unit tests", failed);
  },

  "@gdesmott/cargo-nextest": (summary, base, failed) => {
    const miri = summary.miri === true;
    const failures = list(summary, "failures").map((failure) => {
      const kind = str(failure, "kind") ?? "failure";
      return base({
        // Miri's findings are not test failures in the ordinary sense — the
        // native run passes — so they keep their own kind all the way out.
        kind: kind === "undefined-behavior" || kind === "unsupported"
          ? kind
          : miri
          ? `miri-${kind}`
          : `test-${kind}`,
        what: str(failure, "test") ?? "unknown test",
        where: location(str(failure, "file"), failure.line),
        detail: firstLine(failure.detail),
      });
    });
    // A miri run legitimately interprets nothing when every crate in scope is
    // excluded with #[cfg(not(miri))], but the workflow asserts against that
    // too, so it is reported the same way.
    return failures.length > 0
      ? failures
      : noTestsRan(summary, base, miri ? "miri" : "rust tests", failed);
  },

  "@gdesmott/rust-lint": (summary, base) =>
    list(summary, "findings")
      .filter((finding) => str(finding, "level") !== "warning")
      .map((finding) =>
        base({
          kind: `lint-${str(finding, "level") ?? "error"}`,
          what: str(finding, "code") ?? str(finding, "profile") ?? "lint",
          where: location(str(finding, "file"), finding.line),
          detail: firstLine(finding.message),
        })
      ),

  "@gdesmott/pytest": (summary, base, failed) => {
    const findings = list(summary, "failures").map((failure) =>
      base({
        kind: "test-failure",
        what: str(failure, "test") ?? "unknown test",
        where: str(failure, "location"),
        detail: str(failure, "assertion") ?? str(failure, "message") ??
          firstLine(failure.raw),
      })
    );
    // RLTest can name a test as failed without printing a detail line for it —
    // an error rather than an assertion, or a message that only says to read the
    // logs. The name is still the most useful thing the run produced, so it is
    // reported rather than left to a generic step failure.
    const detailed = new Set(
      list(summary, "failures").map((failure) => str(failure, "test")),
    );
    for (const test of strings(summary, "failedTests")) {
      if (detailed.has(test)) continue;
      findings.push(base({
        kind: "test-failure",
        what: test,
        detail: "reported as failing without a detail line; see the log",
      }));
    }
    return findings.length > 0
      ? findings
      : noTestsRan(summary, base, "pytest", failed);
  },

  "@gdesmott/lcov-coverage": (summary, base, failed) =>
    // A requested file with no coverage data is not compiled into what ran — a
    // different problem from being untested, and the one worth naming per file.
    //
    // Only when the step failed, though. `requireAllFound: false` asks the model
    // to report on whatever it found and succeed regardless, so the same missing
    // targets are there by consent. The summary does not record that choice, but
    // the step's outcome already encodes it: the model fails exactly when the
    // caller demanded every file and did not get them.
    !failed ? [] : list(summary, "targets")
      .filter((target) => target.found === false)
      .map((target) =>
        base({
          kind: "no-coverage-data",
          what: str(target, "file") ?? "unknown file",
          where: str(target, "file"),
          detail:
            "not in the coverage trace; the file is probably not compiled into " +
            "the module, or the build was not instrumented",
        })
      ),

  "@gdesmott/rust-coverage": (summary, base, failed) => {
    // Exporting nothing is a failure however the run exited: there is no
    // measurement to act on either way.
    if (summary.parsed === false) {
      return [
        base({
          kind: "no-coverage-export",
          what: String(summary.scope ?? "workspace"),
          detail:
            "the run ended before exporting any coverage, so nothing was measured; see its log",
        }),
      ];
    }
    // Coverage was measured, so anything left is in the tests behind it, and
    // cargo llvm-cov's own output is where they are named. A run whose tests
    // passed has nothing to report — the coverage figures are the point, and
    // they are not a failure.
    return failed
      ? [
        base({
          kind: "tests-failed-under-coverage",
          what: String(summary.scope ?? "workspace"),
          detail:
            "tests failed, so the measured coverage is a floor rather than the real figure; see its log",
        }),
      ]
      : [];
  },

  "@gdesmott/swamp-tests": (summary, base, failed) => {
    // The target is two checks in sequence, and the model already decided which
    // one stopped the run. Reporting the wrong one sends the reader to a check
    // that is passing.
    if (str(summary, "stage") === "format") {
      return strings(summary, "unformatted").map((file) =>
        base({
          kind: "unformatted",
          what: "formatting",
          where: file,
          detail: "needs reformatting; run `deno fmt` in extensions/",
        })
      );
    }
    const failures = list(summary, "failures").map((failure) =>
      base({
        kind: "test-failure",
        what: str(failure, "test") ?? "unknown test",
        where: str(failure, "where"),
        detail: "the extension suite reported it as failing; see the log",
      })
    );
    return failures.length > 0
      ? failures
      : noTestsRan(summary, base, "swamp extension tests", failed);
  },

  "@gdesmott/make-fmt": (summary, base) =>
    strings(summary, "files").map((file) =>
      base({
        kind: "unformatted",
        // The path is the location, so naming it twice would only pad the row.
        what: "formatting",
        where: file,
        detail: "needs reformatting; run `make fmt`",
      })
    ),
};

/**
 * Findings for a step whose model type has no translator, or whose summary is
 * missing. Coarse on purpose: it says which step failed without claiming to know
 * why, rather than reporting nothing.
 */
function fallback(step: StepExecution, reason: string): Finding[] {
  return [{
    step: step.stepName,
    model: step.modelName ?? null,
    kind: step.modelName ? "step-failed" : "gate",
    what: step.modelName ?? step.stepName,
    where: null,
    detail: reason,
  }];
}

/** When a summary says it was recorded, in epoch milliseconds. */
function timestamp(summary: Summary | null): number | null {
  const value = summary?.executedAt;
  if (typeof value !== "string") return null;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

/** A summary and how confidently it was tied to this run. */
interface SummaryRead {
  summary: Summary | null;
  /** True when the version came from the step's own handle. */
  pinned: boolean;
  /**
   * Whether the data's own ownership record names this workflow run: true for
   * this run, false for a different one, null when nothing answered — no
   * `findByName`, no metadata, or a writer that recorded no run.
   */
  ofThisRun: boolean | null;
}

/**
 * Ask the data itself which workflow run wrote it.
 *
 * Every artifact carries an ownership record, and a workflow step's names the
 * run. That is a direct answer to the question the timestamps below can only
 * approximate, and unlike them it needs no other step to have succeeded first.
 * Null whenever nothing answered, which leaves the caller on the timestamps.
 */
async function writtenByThisRun(
  context: Context,
  step: StepExecution,
): Promise<boolean | null> {
  const find = context.dataRepository.findByName;
  if (!find || !step.modelType || !step.modelId) return null;
  try {
    // No version: the same latest the content was read from, so the two cannot
    // describe different versions.
    const data = await find(step.modelType, step.modelId, "summary");
    const runId = data?.ownerDefinition?.workflowRunId;
    return typeof runId === "string" ? runId === context.workflowRunId : null;
  } catch {
    // Metadata is an optimisation over the timestamps, never a requirement.
    return null;
  }
}

/**
 * Read the summary a step wrote.
 *
 * A handle pins the version this run produced, which is what a succeeded step
 * provides. A failed step provides no handles at all, so its summary can only be
 * read as the model's latest — usually this run's, because the per-model lock
 * serialises runs and nothing else has run since, but not when the model threw
 * before writing one. So the read also asks the data who wrote it, and is
 * reported as unpinned either way so it is never mistaken for proof.
 */
async function readSummary(
  context: Context,
  step: StepExecution,
): Promise<SummaryRead> {
  const missing = { summary: null, pinned: false, ofThisRun: null };
  if (!step.modelType || !step.modelId) return missing;

  const handle = (step.dataHandles ?? []).find((h) => h.name === "summary");
  const pinned = handle !== undefined;

  try {
    const bytes = await context.dataRepository.getContent(
      step.modelType,
      step.modelId,
      "summary",
      handle?.version,
    );
    if (!bytes) return { summary: null, pinned, ofThisRun: null };
    const parsed = JSON.parse(new TextDecoder().decode(bytes));
    return {
      summary: typeof parsed === "object" && parsed !== null
        ? parsed as Summary
        : null,
      pinned,
      // A pinned read is already this run's by construction; only the fallback
      // to the model's latest has anything to establish, and it read the latest
      // rather than a version.
      ofThisRun: pinned ? true : await writtenByThisRun(context, step),
    };
  } catch (error) {
    // An unreadable summary must not lose the failure it described.
    context.logger.info("Could not read the summary for step {step}: {error}", {
      step: step.stepName,
      error: error instanceof Error ? error.message : String(error),
    });
    return { summary: null, pinned, ofThisRun: null };
  }
}

/** Escape the pipes and newlines that would break a markdown table row. */
function cell(value: string | null): string {
  if (!value) return "—";
  return value.replace(/\|/g, "\\|").replace(/\n/g, " ");
}

/** Render the digest as markdown. */
function render(
  context: Context,
  findings: Finding[],
  failed: StepExecution[],
  notRun: StepExecution[],
  unpinned: string[],
  stale: string[],
): string {
  const lines: string[] = [`# ${context.workflowName} failures`, ""];

  if (context.workflowStatus === "succeeded" && findings.length === 0) {
    lines.push("Every step succeeded — nothing to fix.");
    return lines.join("\n");
  }

  const first = failed[0];
  const position = first
    ? context.stepExecutions.findIndex((s) => s.stepName === first.stepName) + 1
    : 0;
  lines.push(
    first
      ? `${
        first.status === "failed"
          ? `Failed at step **${first.stepName}**`
          // Told to carry on, so the run continued past it — but it still found
          // something, and that is what the list below is.
          : `Step **${first.stepName}** reported failures without stopping the run`
      } (${position} of ${context.stepExecutions.length} reported).` +
        (notRun.length > 0
          ? ` ${notRun.length} later step${
            notRun.length === 1 ? "" : "s"
          } did not run, so ${
            notRun.length === 1 ? "it has" : "they have"
          } proved nothing.`
          : "")
      : "No step reported a failure, so what failed is not in this digest.",
    "",
  );

  if (findings.length > 0) {
    lines.push("## What to fix", "");
    lines.push("| Where | What | Kind | Detail |", "| --- | --- | --- | --- |");
    for (const finding of findings.slice(0, MAX_ROWS)) {
      lines.push(
        `| ${cell(finding.where)} | ${cell(finding.what)} | ${
          cell(finding.kind)
        } | ${cell(finding.detail)} |`,
      );
    }
    if (findings.length > MAX_ROWS) {
      // Say what was dropped: a truncated table reads as the whole story.
      lines.push(
        "",
        `${findings.length - MAX_ROWS} further finding${
          findings.length - MAX_ROWS === 1 ? "" : "s"
        } omitted here; the JSON output carries all ${findings.length}.`,
      );
    }
    lines.push("");
  }

  lines.push("## Steps", "");
  lines.push("| Step | Model | Status |", "| --- | --- | --- |");
  for (const step of context.stepExecutions) {
    lines.push(
      `| ${cell(step.stepName)} | ${cell(step.modelName ?? null)} | ${
        cell(step.status)
      } |`,
    );
  }

  // Both caveats are properties of what a workflow report is given. Leaving them
  // implicit would let this table read as the whole run.
  lines.push(
    "",
    "Only steps that ran as model methods appear above: a failed assertion " +
      "step, and a step that never ran, are not reported to this digest. See " +
      "the workflow's own output for those.",
  );
  if (unpinned.length > 0) {
    lines.push(
      "",
      `Read from the latest summary rather than this run's, because a failed ` +
        `step reports no data handles: ${unpinned.join(", ")}.`,
    );
  }
  if (stale.length > 0) {
    lines.push(
      "",
      `Discarded a summary that belongs to an earlier run, so these steps are ` +
        `reported from the step itself rather than from data describing a ` +
        `different run: ${stale.join(", ")}.`,
    );
  }

  return lines.join("\n");
}

/** Report definition: a single list of what to fix after a failed run. */
export const report = {
  name: "@gdesmott/failure-digest",
  description:
    "Collect every failure a workflow run produced into one normalised list of " +
    "what to fix, with the source location each suite reported",
  scope: "workflow" as const,
  labels: ["verification", "failures"],
  execute: async (context: Context): Promise<{
    markdown: string;
    json: Record<string, unknown>;
  }> => {
    const notRun = context.stepExecutions.filter((s) => s.status === "skipped");
    const unpinned: string[] = [];
    const stale: string[] = [];
    // Steps with something to report, in the order they ran.
    const problems: StepExecution[] = [];

    // Read every summary first, so the pinned ones can date the run.
    const reads = new Map<string, SummaryRead>();
    for (const step of context.stepExecutions) {
      if (step.status === "skipped") continue;
      reads.set(step.stepName, await readSummary(context, step));
    }

    // The earliest summary that definitely belongs to this run.
    //
    // An unpinned read returns the model's *latest* summary, which is this run's
    // only while the model actually wrote one. A model that threw before writing
    // — the lint model rejecting stale headers, say — leaves the previous run's
    // summary as latest, and translating that reports yesterday's failures as
    // today's. A succeeded step's summary is pinned to this run and gives a
    // floor to compare against: a summary older than the first thing this run
    // recorded cannot have come from it.
    //
    // Only a fallback, and a partial one: it needs an earlier step to have
    // succeeded, so it says nothing about the case where the first step to run
    // is the one that failed. `ofThisRun` answers that from the data's own
    // ownership record and is preferred wherever it is available.
    const floor = Math.min(
      ...[...reads.values()]
        .filter((r) => r.pinned)
        .map((r) => timestamp(r.summary))
        .filter((t): t is number => t !== null),
    );
    const runFloor = Number.isFinite(floor) ? floor : null;

    const findings: Finding[] = [];
    for (const step of context.stepExecutions) {
      if (step.status === "skipped") continue;

      const read = reads.get(step.stepName) ??
        { summary: null, pinned: false, ofThisRun: null };
      const { pinned, ofThisRun } = read;
      // Only discard a summary that is provably not this run's.
      //
      // The ownership record settles it outright when it answered. Otherwise
      // fall back to the timestamps, which can only say "older than something
      // this run recorded" and need an earlier step to have succeeded for even
      // that. With neither answering, the existing behaviour stands: report it
      // and say it was unpinned.
      const when = pinned ? null : timestamp(read.summary);
      const isStale = ofThisRun !== null
        ? !ofThisRun
        : runFloor !== null && when !== null && when < runFloor;
      if (isStale) stale.push(step.stepName);
      const summary = isStale ? null : read.summary;

      // A step can succeed while what it ran did not, in two different ways.
      //
      // It can be told to ignore test failures — so a later step can still use
      // its output — and then reports success here and failure in its summary.
      //
      // Or it can exit cleanly having run nothing: the C suite whose binaries
      // were never built skips every block and still exits 0. The workflow
      // catches that with an assert step, but stepExecutions omits failed
      // asserts, so keying on failure alone left the digest silent about a run
      // that had a perfectly good explanation sitting in the summary next to it.
      //
      // So every step that ran is translated, and what the translator finds
      // decides whether there is anything to report. `knownBad` then only
      // governs the fallbacks: a step already known to have gone wrong has to
      // appear even when nothing could be parsed out of it, while a step that
      // passed and yielded no findings is simply fine and says so by silence.
      const knownBad = step.status === "failed" || summary?.status === "failed";

      const base = (fields: Partial<Finding>): Finding => ({
        step: step.stepName,
        model: step.modelName ?? null,
        kind: "failure",
        what: step.stepName,
        where: null,
        detail: "",
        ...fields,
      });

      /** Report a step, but only when it actually produced something. */
      const record = (produced: Finding[]): void => {
        if (produced.length === 0) return;
        problems.push(step);
        if (summary && !pinned) unpinned.push(step.stepName);
        findings.push(...produced);
      };

      if (!summary) {
        // A step with no model — an assert step, if a future swamp reports one —
        // has no summary to read, and the step name is the whole finding. With
        // nothing to read there is also nothing to distinguish a healthy step
        // from a broken one, so only a known-bad one is worth naming.
        if (knownBad) {
          record(fallback(
            step,
            isStale
              ? "the model failed before recording a summary — the one on file " +
                "belongs to an earlier run and describes a different one; see " +
                "its log"
              : step.modelName
              ? "the model recorded no summary for this run; see its log"
              : "a workflow assertion on this run did not hold",
          ));
        }
        continue;
      }

      const translate = step.modelType
        ? TRANSLATORS[step.modelType]
        : undefined;
      if (!translate) {
        if (knownBad) {
          record(fallback(
            step,
            `no translator for model type ${step.modelType}; read its summary directly`,
          ));
        }
        continue;
      }

      const translated = translate(summary, base, knownBad);
      if (translated.length > 0) {
        record(translated);
        continue;
      }

      // A model can fail while reporting no individual finding — a run killed by
      // its timeout, say. The step still has to appear.
      if (knownBad) {
        record(fallback(
          step,
          summary.timedOut === true
            ? "the run was aborted by its timeout before it could report"
            : "the step failed without reporting an individual failure; see its log",
        ));
      }
    }

    const failed = problems.filter((s) => s.status === "failed");
    const passedWithFailures = problems.filter((s) => s.status !== "failed");

    return {
      markdown: render(context, findings, problems, notRun, unpinned, stale),
      json: {
        workflow: context.workflowName,
        workflowRunId: context.workflowRunId,
        workflowStatus: context.workflowStatus,
        // Where it first went wrong, whether or not that stopped the step.
        firstFailedStep: problems[0]?.stepName ?? null,
        failedSteps: failed.map((s) => s.stepName),
        // Steps that succeeded and still had something to report: either they
        // were told to carry on past test failures, or they exited cleanly
        // without doing the work — a suite that skipped every block for want of
        // binaries. Their findings are in the list either way.
        stepsPassedWithFailures: passedWithFailures.map((s) => s.stepName),
        // Named rather than counted: in a linear chain these are the suites
        // whose result is still unknown, not suites that passed. Empty whenever
        // swamp reports no skipped step, which is the case today.
        stepsNotRun: notRun.map((s) => s.stepName),
        // Steps whose summary could not be tied to this run's version.
        unpinnedReads: unpinned,
        // Steps whose summary on file belongs to an earlier run, so it was
        // discarded rather than reported as this run's findings.
        staleReads: stale,
        stepsReported: context.stepExecutions.length,
        // Machine-readable form of the caveats in the markdown, so a consumer
        // does not have to infer completeness from a count.
        limits: [
          "stepExecutions omits failed assert steps and steps that never ran",
          "a failed step reports no data handles, so its summary is read unpinned",
        ],
        findingCount: findings.length,
        findings,
      },
    };
  },
};
