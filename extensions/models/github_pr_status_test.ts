/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for reducing a pull request's checks and reviews to a verdict.
 *
 * This is the part worth testing hardest, because the loop that reads it ends
 * on the answer. A verdict of `passing` over a run that had not finished, or an
 * approval credited to a bot that had not approved, both end the loop early and
 * neither looks wrong afterwards.
 *
 * @module
 */
import { assert, assertEquals } from "jsr:@std/assert@1";
import { approval, normaliseChecks, verdict } from "./github_pr_status.ts";

Deno.test("check runs and commit statuses reduce to one shape", () => {
  const checks = normaliseChecks([
    {
      __typename: "CheckRun",
      name: "build",
      status: "COMPLETED",
      conclusion: "SUCCESS",
      detailsUrl: "https://ci.invalid/1",
    },
    {
      __typename: "StatusContext",
      context: "legacy/lint",
      state: "SUCCESS",
      targetUrl: "https://ci.invalid/2",
    },
  ]);

  assertEquals(checks[0], {
    name: "build",
    status: "COMPLETED",
    conclusion: "SUCCESS",
    url: "https://ci.invalid/1",
  });
  // A commit status has no status of its own: it exists only once it has a
  // result, so it is reported as finished rather than as never having started.
  assertEquals(checks[1], {
    name: "legacy/lint",
    status: "COMPLETED",
    conclusion: "SUCCESS",
    url: "https://ci.invalid/2",
  });
});

Deno.test("an unfinished run is pending even when something already failed", () => {
  // Pending has to beat failing: while anything can still change, the answer is
  // not known, and reporting `failing` sends an agent off to fix a job that had
  // not finished.
  assertEquals(
    verdict(normaliseChecks([
      { name: "a", status: "COMPLETED", conclusion: "FAILURE" },
      { name: "b", status: "IN_PROGRESS", conclusion: "" },
    ])),
    "pending",
  );
});

Deno.test("skipped and neutral checks do not fail the run", () => {
  // A skipped job is how a conditional CI workflow says it had nothing to do.
  // Counting it as a failure would leave the loop trying to fix a job that
  // never ran.
  assertEquals(
    verdict(normaliseChecks([
      { name: "a", status: "COMPLETED", conclusion: "SUCCESS" },
      { name: "b", status: "COMPLETED", conclusion: "SKIPPED" },
      { name: "c", status: "COMPLETED", conclusion: "NEUTRAL" },
    ])),
    "passing",
  );
});

Deno.test("every way a check can end badly counts as failing", () => {
  for (
    const conclusion of [
      "FAILURE",
      "TIMED_OUT",
      "CANCELLED",
      "ACTION_REQUIRED",
      "STARTUP_FAILURE",
      // Ran against something that is no longer the head, so it is not a pass —
      // and a required check left stale would otherwise read as green.
      "STALE",
    ]
  ) {
    assertEquals(
      verdict(normaliseChecks([
        { name: "ok", status: "COMPLETED", conclusion: "SUCCESS" },
        { name: "bad", status: "COMPLETED", conclusion },
      ])),
      "failing",
      conclusion,
    );
  }
});

Deno.test("an unsettled commit status is pending, not passing", () => {
  // A commit status carries progress and outcome in one field. Folded into the
  // conclusion, PENDING reads as "finished, and not a failure" — success, while
  // a required status is still outstanding.
  for (const state of ["PENDING", "EXPECTED"]) {
    assertEquals(
      verdict(normaliseChecks([
        {
          __typename: "CheckRun",
          name: "build",
          status: "COMPLETED",
          conclusion: "SUCCESS",
        },
        { __typename: "StatusContext", context: "external/required", state },
      ])),
      "pending",
      state,
    );
  }
});

Deno.test("a commit status that errored is a failure", () => {
  // ERROR belongs to the status vocabulary, not the check-run one. Missing it
  // means an errored required status reads as green.
  assertEquals(
    verdict(normaliseChecks([
      {
        __typename: "StatusContext",
        context: "external/required",
        state: "ERROR",
      },
    ])),
    "failing",
  );
});

Deno.test("no checks at all is its own answer", () => {
  // Distinct from passing: a pull request whose CI never started has not been
  // validated, and treating it as green would let the loop finish on nothing.
  assertEquals(verdict([]), "none");
});

Deno.test("with no required approvers, GitHub's own decision is used", () => {
  const yes = approval([], "APPROVED", []);
  assertEquals(yes.approved, true);
  assertEquals(approval([], "REVIEW_REQUIRED", []).approved, false);
  assertEquals(approval([], "CHANGES_REQUESTED", []).approved, false);
});

Deno.test("a required approver is matched without knowing how the bot is spelled", () => {
  const result = approval(
    [{ author: { login: "codex[bot]" }, state: "APPROVED" }],
    "REVIEW_REQUIRED",
    ["codex"],
  );

  // GitHub's decision still says review required — a bot approval does not
  // satisfy branch protection — but the flow's own requirement is met.
  assertEquals(result.approved, true);
  assertEquals(result.approvedBy, ["codex[bot]"]);
  assertEquals(result.missing, []);
});

Deno.test("a similarly named account does not satisfy a required approver", () => {
  // `codex` matching `codex[bot]` is the point of normalising the suffix. It is
  // not licence to accept `codex-helper`, which is a different account.
  const result = approval(
    [{
      author: { login: "codex-helper" },
      state: "APPROVED",
      commit: { oid: "h" },
    }],
    "REVIEW_REQUIRED",
    ["codex"],
    "h",
  );

  assertEquals(result.approved, false);
  assertEquals(result.missing, ["codex"]);
});

Deno.test("a required approver who has not approved is reported as missing", () => {
  const result = approval(
    [{ author: { login: "alice" }, state: "APPROVED" }],
    "APPROVED",
    ["codex"],
  );

  // Approved by a human and green as far as GitHub is concerned, yet the
  // requirement this flow was given is unmet — so it must not read as ready.
  assertEquals(result.approved, false);
  assertEquals(result.missing, ["codex"]);
  assert(result.approvedBy.includes("alice"));
});

Deno.test("a request for changes blocks even when the required approver approved", () => {
  // Naming required approvers says who must say yes. It does not say whose no
  // can be ignored — and GitHub will not merge over an outstanding request for
  // changes, so a flow that read this as ready would stop on a stuck PR.
  const result = approval(
    [
      { author: { login: "codex[bot]" }, state: "APPROVED" },
      { author: { login: "alice" }, state: "CHANGES_REQUESTED" },
    ],
    "CHANGES_REQUESTED",
    ["codex"],
  );

  assertEquals(result.approved, false);
  assertEquals(result.missing, []);
  assert(result.approvedBy.includes("codex[bot]"));
});

Deno.test("an approval of an earlier commit does not count", () => {
  // The CI fixer pushes follow-ups. An approval is of a commit, so one given
  // before the last push says nothing about what is there now — and counting it
  // stops the sweep on code the required reviewer never saw.
  const result = approval(
    [{
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "old" },
    }],
    "REVIEW_REQUIRED",
    ["codex"],
    "head",
  );

  assertEquals(result.approved, false);
  assertEquals(result.missing, ["codex"]);
  assertEquals(result.stale, ["codex[bot]"]);
});

Deno.test("an approval of the current head counts", () => {
  const result = approval(
    [{
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "head" },
    }],
    "REVIEW_REQUIRED",
    ["codex"],
    "head",
  );

  assertEquals(result.approved, true);
  assertEquals(result.stale, []);
});

Deno.test("a stale non-approving review does not count", () => {
  const result = approval(
    [
      { author: { login: "codex[bot]" }, state: "CHANGES_REQUESTED" },
      { author: { login: "bob" }, state: "COMMENTED" },
    ],
    "CHANGES_REQUESTED",
    ["codex"],
  );

  assertEquals(result.approved, false);
  assertEquals(result.approvedBy, []);
});
