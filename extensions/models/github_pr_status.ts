/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Reports whether a pull request is green and approved, as structured data a
 * workflow guard can read.
 *
 * This exists to keep an agent out of the polling loop. Whether CI passed is a
 * fact with a known shape, and asking a language model to read a check list
 * costs tokens on every poll and can answer "green" about a run that is still
 * queued — which, in a flow whose whole job is to gate on that answer, is the
 * one error that matters. So the checks are read with `gh` and reduced here,
 * and the agent is only spawned once something is actually broken.
 *
 * Absence is reported rather than raised. A scheduled sweep runs whether or not
 * there is a pull request to look at, and a run that fails because there was
 * nothing to do is indistinguishable from one that failed because something
 * went wrong — so `found` is false and the workflow's guards skip everything.
 *
 * @module
 */
import { z } from "npm:zod@4.4.3";

/** Default budget for the `gh` calls: half a minute. */
const DEFAULT_TIMEOUT_MS = 30 * 1000;

/**
 * Check conclusions that mean the check is broken.
 *
 * `SKIPPED` and `NEUTRAL` are deliberately absent: a skipped job is how a
 * conditional workflow reports that it had nothing to do, and treating it as a
 * failure would leave the loop trying to fix a job that never ran.
 *
 * `ERROR` is here for the legacy commit statuses, whose vocabulary is not the
 * check-run one: a status that errored has failed, however differently GitHub
 * words it.
 */
const FAILING_CONCLUSIONS = new Set([
  "FAILURE",
  "ERROR",
  "TIMED_OUT",
  "CANCELLED",
  "ACTION_REQUIRED",
  "STARTUP_FAILURE",
  // A stale check ran against something that is no longer the head. Its result
  // is not a pass, and GitHub will not count it as one — so neither can this,
  // or a required check could be stale and the pull request read as ready.
  "STALE",
]);

/**
 * Legacy commit-status states that mean the status has not settled.
 *
 * A commit status is not like a check run: it has one `state` covering both
 * progress and outcome, and two of its values are neither. Folded into the
 * conclusion as though they were results, `PENDING` and `EXPECTED` read as
 * "finished, and not a failure" — which is to say, as success, while a required
 * status is still outstanding.
 */
const UNSETTLED_STATUS_STATES = new Set(["PENDING", "EXPECTED"]);

const GlobalArgsSchema = z.object({
  ghBin: z
    .string()
    .min(1)
    .default("gh")
    .describe("The GitHub CLI executable to invoke."),
  workingDir: z
    .string()
    .default(".")
    .describe(
      "Directory to run `gh` in, which is how it works out the repository. " +
        "Relative paths resolve against the repository root.",
    ),
  timeout: z
    .number()
    .int()
    .positive()
    .default(DEFAULT_TIMEOUT_MS)
    .describe(`Timeout in milliseconds (default ${DEFAULT_TIMEOUT_MS}).`),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const CheckArgsSchema = z.object({
  pr: z
    .string()
    .default("")
    .describe(
      "Pull request number or URL. Empty asks `gh` for the one belonging to " +
        "the current branch, which is the usual case right after opening it.",
    ),
  requiredApprovers: z
    .array(z.string())
    .default([])
    .describe(
      "Logins that must each have approved, matched case-insensitively as a " +
        "substring so that `codex` matches `codex[bot]`. Empty accepts " +
        "GitHub's own review decision, which is what branch protection uses.",
    ),
});

type CheckArgs = z.infer<typeof CheckArgsSchema>;

const CheckSchema = z.object({
  name: z.string().describe("Check or status context name"),
  status: z
    .string()
    .describe("QUEUED, IN_PROGRESS or COMPLETED; empty for status contexts"),
  conclusion: z.string().describe("SUCCESS, FAILURE, SKIPPED, … when finished"),
  url: z.string().nullable().describe("Where to read the run"),
});

const StatusSchema = z.object({
  found: z
    .boolean()
    .describe(
      "A pull request was found. False is a normal answer, not an error: a " +
        "sweep that runs when none is open has nothing to do",
    ),
  reason: z
    .string()
    .describe(
      "Why nothing was found, empty when something was. A sweep that " +
        "quietly does nothing forever looks identical whether there is no " +
        "pull request yet or the checkout is somewhere `gh` cannot ask about",
    ),
  number: z.number().int().nullable().describe("Pull request number"),
  url: z.string().nullable().describe("Pull request URL"),
  title: z.string().nullable().describe("Pull request title"),
  headSha: z.string().nullable().describe("Commit the checks ran against"),
  state: z.string().describe("OPEN, MERGED or CLOSED; empty when not found"),
  isDraft: z.boolean().describe("Whether it is still a draft"),
  checksState: z
    .enum(["passing", "failing", "pending", "none", "unknown"])
    .describe(
      "`pending` while anything is queued or running, `failing` if anything " +
        "finished badly, `none` when no checks are attached at all, and " +
        "`unknown` when there is no pull request to ask about",
    ),
  checks: z.array(CheckSchema).describe("Every check, as reported"),
  failedChecks: z
    .array(z.string())
    .describe(
      "Names of the checks that failed, for deciding what to reproduce",
    ),
  approved: z
    .boolean()
    .describe("Whether the review requirement is satisfied"),
  reviewDecision: z
    .string()
    .describe("GitHub's own decision: APPROVED, CHANGES_REQUESTED or empty"),
  mergeStateStatus: z
    .string()
    .describe(
      "GitHub's own mergeability: CLEAN, UNSTABLE, BLOCKED, BEHIND, DIRTY…",
    ),
  staleApprovals: z
    .array(z.string())
    .describe(
      "Required approvers who approved an earlier commit than the current head",
    ),
  approvedBy: z.array(z.string()).describe("Logins that have approved"),
  missingApprovers: z
    .array(z.string())
    .describe("Required logins that have not approved yet"),
  ready: z
    .boolean()
    .describe(
      "Open, not draft, every check passing, and approved. This is the one " +
        "field the loop ends on",
    ),
  executedAt: z.iso.datetime().describe("When the status was read"),
});

/** One entry of `gh`'s statusCheckRollup, in either of its two shapes. */
export interface RollupEntry {
  __typename?: string;
  name?: string;
  context?: string;
  status?: string;
  state?: string;
  conclusion?: string;
  detailsUrl?: string;
  targetUrl?: string;
}

/** One entry of `gh`'s latestReviews. */
export interface ReviewEntry {
  author?: { login?: string };
  state?: string;
  commit?: { oid?: string };
}

/**
 * Merge states in which GitHub would let the pull request through.
 *
 * `UNSTABLE` is here because it means a non-required check failed, which does
 * not block a merge. Everything else — blocked on a required check that never
 * reported, behind the base, conflicted, or not yet computed — is a pull
 * request that cannot merge, and a flow that called it ready would stop
 * polling on one that still needs work.
 */
const MERGEABLE_STATES = new Set(["CLEAN", "UNSTABLE", "HAS_HOOKS"]);

type Check = z.infer<typeof CheckSchema>;

/**
 * Normalise the rollup into one shape.
 *
 * GitHub reports two different things through this field: check runs, which
 * have a name, a status and a conclusion, and the older commit statuses, which
 * have a context and a single state. Left as they come, every consumer would
 * have to know the difference.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function normaliseChecks(rollup: RollupEntry[]): Check[] {
  return rollup.map((entry) => {
    const isContext = entry.context !== undefined;
    const state = (entry.state ?? "").toUpperCase();
    // A commit status carries progress and outcome in the same field, so its
    // state decides both here: unsettled ones stay unfinished rather than being
    // reported as a conclusion that happens not to be a failure.
    const unsettled = isContext && UNSETTLED_STATUS_STATES.has(state);
    return {
      name: entry.name ?? entry.context ?? "unnamed",
      status: isContext
        ? (unsettled ? "IN_PROGRESS" : "COMPLETED")
        : (entry.status ?? ""),
      conclusion:
        (isContext ? (unsettled ? "" : entry.state) : entry.conclusion) ??
          "",
      url: entry.detailsUrl ?? entry.targetUrl ?? null,
    };
  });
}

/**
 * Reduce the checks to a single verdict.
 *
 * Pending beats failing on purpose: while anything is still running the answer
 * is not yet known, and reporting `failing` early would send the loop off to
 * fix a job that had not finished. A failure is only a failure once nothing
 * else can still change it.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function verdict(
  checks: Check[],
): "passing" | "failing" | "pending" | "none" {
  if (checks.length === 0) return "none";
  if (checks.some((check) => check.status && check.status !== "COMPLETED")) {
    return "pending";
  }
  if (
    checks.some((check) =>
      FAILING_CONCLUSIONS.has(check.conclusion.toUpperCase())
    )
  ) {
    return "failing";
  }
  return "passing";
}

/**
 * Work out whether the review requirement is met.
 *
 * With no required approvers this defers to GitHub's own decision, which is
 * what branch protection enforces. With them, each has to have approved in
 * their own right — a login is matched as a case-insensitive substring so that
 * `codex` finds `codex[bot]` without the caller having to know how the bot is
 * spelled — but as whole logins, so a different account whose name merely
 * contains the required one does not satisfy it.
 *
 * An outstanding request for changes blocks either way. Naming required
 * approvers says who must say yes; it does not say whose no can be ignored, and
 * a bot approval sitting alongside a human asking for changes is not a pull
 * request anyone can merge.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function approval(
  reviews: ReviewEntry[],
  reviewDecision: string,
  required: string[],
  headSha = "",
): {
  approved: boolean;
  approvedBy: string[];
  missing: string[];
  stale: string[];
} {
  const approvals = reviews.filter((review) =>
    (review.state ?? "").toUpperCase() === "APPROVED"
  );
  // An approval is of a commit, not of a pull request. The CI fixer pushes
  // follow-ups, so an approval given before the last push says nothing about
  // what is there now — and counting it would stop the sweep on code the
  // required reviewer never saw.
  const covers = (review: ReviewEntry): boolean =>
    headSha === "" || (review.commit?.oid ?? "") === headSha;
  const approvedBy = approvals
    .filter(covers)
    .map((review) => review.author?.login ?? "")
    .filter((login) => login.length > 0);
  const stale = approvals
    .filter((review) => !covers(review))
    .map((review) => review.author?.login ?? "")
    .filter((login) => login.length > 0);

  // Changes requested blocks regardless of who else has approved. GitHub will
  // not merge over it, so a flow that read itself as ready would stop with the
  // pull request stuck and nobody addressing the review.
  const blocked = reviewDecision.toUpperCase() === "CHANGES_REQUESTED";

  if (required.length === 0) {
    return {
      approved: !blocked && reviewDecision.toUpperCase() === "APPROVED",
      approvedBy,
      missing: [],
      stale,
    };
  }

  // Compared as whole logins, with GitHub's `[bot]` suffix removed so that
  // `codex` matches `codex[bot]` — which is the only reason this was ever a
  // substring test. As a substring it also matched `codex-helper`, and an
  // approval from a different account with a similar name is not the approval
  // that was asked for.
  const lowered = approvedBy.map((login) =>
    login.toLowerCase().replace(/\[bot\]$/, "")
  );
  const missing = required.filter((name) =>
    !lowered.includes(name.toLowerCase().replace(/\[bot\]$/, ""))
  );
  return {
    approved: !blocked && missing.length === 0,
    approvedBy,
    missing,
    stale,
  };
}

/** The context swamp hands a method's `execute`. */
interface ExecuteContext {
  signal: AbortSignal;
  repoDir: string;
  globalArgs: GlobalArgs;
  logger: { info: (msg: string, props?: unknown) => void };
  writeResource: (
    specName: string,
    name: string,
    data: Record<string, unknown>,
  ) => Promise<{ name: string }>;
}

/** The fields to ask `gh` for, in one call. */
const FIELDS = [
  "number",
  "mergeStateStatus",
  "url",
  "title",
  "state",
  "isDraft",
  "headRefOid",
  "statusCheckRollup",
  "latestReviews",
  "reviewDecision",
].join(",");

/** Model definition reporting a pull request's checks and approvals. */
export const model = {
  type: "@gdesmott/github-pr-status",
  version: "2026.08.10.1",
  description:
    "Report a pull request's check results and review approvals as structured data",
  globalArguments: GlobalArgsSchema,
  resources: {
    status: {
      description: "Whether the pull request is green and approved",
      schema: StatusSchema,
      lifetime: "infinite",
      garbageCollection: 50,
    },
  },
  methods: {
    check: {
      description: "Read the pull request's checks and reviews",
      arguments: CheckArgsSchema,
      execute: async (
        args: CheckArgs,
        context: ExecuteContext,
      ): Promise<{ dataHandles: Array<{ name: string }> }> => {
        const { ghBin, workingDir, timeout } = context.globalArgs;
        const cwd = workingDir.startsWith("/")
          ? workingDir
          : workingDir === "."
          ? context.repoDir
          : `${context.repoDir}/${workingDir}`;

        const signal = AbortSignal.any([
          context.signal,
          AbortSignal.timeout(timeout),
        ]);

        const argv = ["pr", "view"];
        if (args.pr) argv.push(args.pr);
        argv.push("--json", FIELDS);

        context.logger.info("Reading pull request status with {bin}", {
          bin: ghBin,
        });

        const result = await new Deno.Command(ghBin, {
          args: argv,
          cwd,
          stdin: "null",
          stdout: "piped",
          stderr: "piped",
          signal,
        }).output();

        const stdout = new TextDecoder().decode(result.stdout);
        const stderr = new TextDecoder().decode(result.stderr);

        /** Record "there is nothing to look at" as an answer. */
        const absent = async (reason: string): Promise<{
          dataHandles: Array<{ name: string }>;
        }> => {
          const handle = await context.writeResource("status", "status", {
            found: false,
            reason,
            number: null,
            url: null,
            title: null,
            headSha: null,
            state: "",
            isDraft: false,
            checksState: "unknown",
            checks: [],
            failedChecks: [],
            approved: false,
            reviewDecision: "",
            mergeStateStatus: "",
            staleApprovals: [],
            approvedBy: [],
            missingApprovers: args.requiredApprovers,
            ready: false,
            executedAt: new Date().toISOString(),
          });
          return { dataHandles: [handle] };
        };

        if (!result.success) {
          // No pull request for this branch is the expected state for most of
          // a task's life, and gh says so on stderr rather than with its own
          // exit code. Anything else is a real failure — no gh, no auth, no
          // network — and must not be reported as "nothing to do".
          if (/no pull requests found|no open pull requests/i.test(stderr)) {
            return await absent("no pull request for this branch");
          }
          // A colocated jj checkout sits on a detached HEAD, so `gh` cannot work
          // out which branch to ask about — and never will here. That is a
          // configuration to report, not a crash to propagate: pass the pull
          // request explicitly instead of relying on the branch.
          if (
            /could not determine current branch|not on any branch/i.test(stderr)
          ) {
            return await absent(
              "the checkout is not on a branch, so gh cannot infer the pull " +
                "request — pass one explicitly",
            );
          }
          throw new Error(
            `\`${ghBin} ${argv.join(" ")}\` failed with code ${result.code}: ` +
              stderr.trim(),
          );
        }

        let payload: {
          number?: number;
          url?: string;
          title?: string;
          state?: string;
          isDraft?: boolean;
          headRefOid?: string;
          statusCheckRollup?: RollupEntry[];
          latestReviews?: ReviewEntry[];
          reviewDecision?: string;
          mergeStateStatus?: string;
        };
        try {
          payload = JSON.parse(stdout);
        } catch {
          throw new Error(
            `\`${ghBin} pr view\` did not return JSON: ${stdout.slice(0, 200)}`,
          );
        }

        if (payload.number === undefined) {
          return await absent("gh returned no pull request");
        }

        const checks = normaliseChecks(payload.statusCheckRollup ?? []);
        const checksState = verdict(checks);
        const reviewDecision = payload.reviewDecision ?? "";
        const headSha = payload.headRefOid ?? "";
        const { approved, approvedBy, missing, stale } = approval(
          payload.latestReviews ?? [],
          reviewDecision,
          args.requiredApprovers,
          headSha,
        );
        const mergeStateStatus = payload.mergeStateStatus ?? "";
        const state = payload.state ?? "";
        const isDraft = payload.isDraft ?? false;

        const handle = await context.writeResource("status", "status", {
          found: true,
          reason: "",
          number: payload.number,
          url: payload.url ?? null,
          title: payload.title ?? null,
          headSha: payload.headRefOid ?? null,
          state,
          isDraft,
          checksState,
          checks,
          failedChecks: checks
            .filter((check) =>
              FAILING_CONCLUSIONS.has(check.conclusion.toUpperCase())
            )
            .map((check) => check.name),
          approved,
          reviewDecision,
          mergeStateStatus,
          staleApprovals: stale,
          approvedBy,
          missingApprovers: missing,
          // GitHub's own mergeability as well as the checks it reports. A
          // required check that never reports is invisible in the rollup and
          // still blocks the merge, so a rollup that looks green is not on its
          // own a pull request anyone can land.
          ready: state === "OPEN" && !isDraft && checksState === "passing" &&
            approved &&
            (mergeStateStatus === "" ||
              MERGEABLE_STATES.has(mergeStateStatus.toUpperCase())),
          executedAt: new Date().toISOString(),
        });

        return { dataHandles: [handle] };
      },
    },
  },
};
