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
import {
  approval,
  isReady,
  latestPerAuthor,
  normaliseChecks,
  repositoryOf,
  reviewedHead,
  verdict,
} from "./github_pr_status.ts";

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

Deno.test("keeps each author's most recent review", () => {
  const latest = latestPerAuthor([
    {
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "old" },
      submittedAt: "2026-08-01T00:00:00Z",
    },
    {
      author: { login: "bob" },
      state: "COMMENTED",
      submittedAt: "2026-08-02T00:00:00Z",
    },
    {
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "head" },
      submittedAt: "2026-08-03T00:00:00Z",
    },
  ]);

  assertEquals(latest.length, 2);
  const codex = latest.find((r) => r.author?.login === "codex[bot]");
  assertEquals(codex?.commit?.oid, "head");
});

Deno.test("orders by submittedAt rather than by position", () => {
  // GitHub decides the order of the array; an approval read as the older of the
  // two would be discarded as stale and the flow would never see the review.
  const latest = latestPerAuthor([
    {
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "head" },
      submittedAt: "2026-08-03T00:00:00Z",
    },
    {
      author: { login: "codex[bot]" },
      state: "COMMENTED",
      submittedAt: "2026-08-01T00:00:00Z",
    },
  ]);

  assertEquals(latest.length, 1);
  assertEquals(latest[0].state, "APPROVED");
});

Deno.test("drops reviews with no author to attribute them to", () => {
  assertEquals(latestPerAuthor([{ state: "APPROVED" }]), []);
});

Deno.test("an approval carrying no commit is not the current head's", () => {
  // What `gh pr view --json latestReviews` returns for every entry: the field
  // is printed but never populated. Read from there, every approval would be
  // compared against "" and rejected, and the flow could never report ready.
  const result = approval(
    [{
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "" },
    }],
    "REVIEW_REQUIRED",
    ["codex"],
    "head",
  );

  assertEquals(result.approved, false);
  assertEquals(result.stale, ["codex[bot]"]);
});

Deno.test("a later comment does not withdraw an approval", () => {
  // GitHub does not dismiss an approval because its author spoke again, and
  // reading the newest review outright would report the approver as missing —
  // sending the fixer at an already-approved pull request every sweep.
  const latest = latestPerAuthor([
    {
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T10:00:00Z",
    },
    {
      author: { login: "codex[bot]" },
      state: "COMMENTED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T11:00:00Z",
    },
  ]);

  assertEquals(latest.length, 1);
  assertEquals(latest[0].state, "APPROVED");
  assertEquals(
    approval(latest, "REVIEW_REQUIRED", ["codex"], "head").approved,
    true,
  );
});

Deno.test("a later decision does replace an earlier one", () => {
  const latest = latestPerAuthor([
    {
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T10:00:00Z",
    },
    {
      author: { login: "codex[bot]" },
      state: "CHANGES_REQUESTED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T12:00:00Z",
    },
  ]);

  assertEquals(latest.length, 1);
  assertEquals(latest[0].state, "CHANGES_REQUESTED");
});

Deno.test("a dismissed approval does not survive as one", () => {
  const latest = latestPerAuthor([
    {
      author: { login: "codex[bot]" },
      state: "APPROVED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T10:00:00Z",
    },
    {
      author: { login: "codex[bot]" },
      state: "DISMISSED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T12:00:00Z",
    },
  ]);

  assertEquals(latest[0].state, "DISMISSED");
  assertEquals(
    approval(latest, "REVIEW_REQUIRED", ["codex"], "head").approved,
    false,
  );
});

Deno.test("an author with only comments is still listed", () => {
  const latest = latestPerAuthor([
    {
      author: { login: "bob" },
      state: "COMMENTED",
      submittedAt: "2026-08-18T10:00:00Z",
    },
  ]);

  assertEquals(latest.length, 1);
  assertEquals(latest[0].state, "COMMENTED");
});

Deno.test("reviewedHead names who looked at the current commit", () => {
  const reviews = [
    {
      author: { login: "codex[bot]" },
      state: "COMMENTED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T10:00:00Z",
    },
    {
      author: { login: "alice" },
      state: "APPROVED",
      commit: { oid: "old" },
      submittedAt: "2026-08-17T10:00:00Z",
    },
  ];

  assertEquals(reviewedHead(reviews, "head"), ["codex[bot]"]);
  // Nothing is known about a head nobody named, so nobody reviewed it.
  assertEquals(reviewedHead(reviews, ""), []);
});

Deno.test("an approval on the head is not feedback to address", () => {
  // With required approvers configured, somebody else's approval leaves
  // `approved` false. Counting it here made "has anyone reviewed this commit"
  // true, and the sweep sent an agent to address feedback that did not exist —
  // which found nothing, pushed nothing, and spent an attempt every firing on a
  // pull request that was only waiting for the named reviewer.
  const reviews = [
    {
      author: { login: "alice" },
      state: "APPROVED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T10:00:00Z",
    },
    {
      author: { login: "bob" },
      state: "DISMISSED",
      commit: { oid: "head" },
      submittedAt: "2026-08-18T10:00:00Z",
    },
  ];

  assertEquals(reviewedHead(reviews, "head"), []);
});

Deno.test("a request for changes on the head is", () => {
  const reviews = [{
    author: { login: "carol" },
    state: "CHANGES_REQUESTED",
    commit: { oid: "head" },
    submittedAt: "2026-08-18T10:00:00Z",
  }];

  assertEquals(reviewedHead(reviews, "head"), ["carol"]);
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

/** A pull request that is green, approved, and mergeable. */
function mergeable(overrides: Record<string, unknown> = {}) {
  return {
    state: "OPEN",
    isDraft: false,
    checksState: "passing",
    approved: true,
    mergeStateStatus: "CLEAN",
    ...overrides,
  } as never;
}

Deno.test("a green, approved, mergeable pull request is ready", () => {
  assertEquals(isReady(mergeable()), true);
  // UNSTABLE is a passing rollup with a non-required check red; HAS_HOOKS is
  // clean with a hook configured. Both merge.
  assertEquals(isReady(mergeable({ mergeStateStatus: "unstable" })), true);
  assertEquals(isReady(mergeable({ mergeStateStatus: "HAS_HOOKS" })), true);
});

Deno.test("a mergeability nobody has computed is not a pass", () => {
  // The state may still be blocked, behind or conflicted. Reading the absence
  // as ready stopped the sweep polling a pull request that could not land,
  // which is the one outcome the mergeability check exists to prevent.
  assertEquals(isReady(mergeable({ mergeStateStatus: "" })), false);
  // And the value GitHub actually reports while it is still working it out.
  assertEquals(isReady(mergeable({ mergeStateStatus: "UNKNOWN" })), false);
});

Deno.test("a pull request GitHub says cannot merge is not ready", () => {
  for (const state of ["BLOCKED", "BEHIND", "DIRTY", "DRAFT"]) {
    assertEquals(isReady(mergeable({ mergeStateStatus: state })), false, state);
  }
});

Deno.test("the other three conditions each stand on their own", () => {
  assertEquals(isReady(mergeable({ state: "CLOSED" })), false);
  assertEquals(isReady(mergeable({ isDraft: true })), false);
  assertEquals(isReady(mergeable({ checksState: "failing" })), false);
  assertEquals(isReady(mergeable({ approved: false })), false);
});

Deno.test("a pull request URL names the repository it belongs to", () => {
  assertEquals(
    repositoryOf("https://github.com/RediSearch/RediSearch/pull/10702"),
    "RediSearch/RediSearch",
  );
  // The case the head SHA cannot distinguish: a fork shares every commit id
  // with its parent, so a link to one comes back found and with the right head.
  assertEquals(
    repositoryOf("https://github.com/someone/RediSearch/pull/1"),
    "someone/RediSearch",
  );
  // GitHub Enterprise, which is a different host and the same shape.
  assertEquals(
    repositoryOf("https://git.example.com/o/r/pull/7"),
    "o/r",
  );
});

Deno.test("anything that is not a pull request URL names no repository", () => {
  // Empty rather than a guess, which is what makes the comparison skippable:
  // a URL that does not parse leaves the check to the other two conditions
  // rather than failing a hand-off over a shape nobody anticipated.
  for (
    const url of [
      "",
      "not a url",
      "https://github.com/RediSearch/RediSearch",
      "https://github.com/RediSearch/RediSearch/issues/10702",
    ]
  ) {
    assertEquals(repositoryOf(url), "", url);
  }
});
