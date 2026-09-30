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
        "Relative paths resolve against the swamp repository directory, so a " +
        "checkout whose swamp files live in a subdirectory wants `..`.",
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
      "Logins that must each have approved, compared case-insensitively as " +
        "whole logins with GitHub's `[bot]` suffix ignored — so `codex` " +
        "matches `codex[bot]`, and a partial login like `code` matches " +
        "nothing. Empty accepts GitHub's own review decision, which is what " +
        "branch protection uses.",
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
  repository: z
    .string()
    .describe(
      "The `owner/name` the checkout itself points at, empty when it could " +
        "not be read. Recorded so a caller can tell a pull request opened here " +
        "from one that merely exists somewhere",
    ),
  localHead: z
    .string()
    .describe(
      "The commit the checkout is on, empty when it could not be read. " +
        "Recorded so a caller about to change the checkout can establish that " +
        "it is the pull request being reported on and not some other branch",
    ),
  prRepository: z
    .string()
    .describe(
      "The `owner/name` the pull request that was found belongs to, read out " +
        "of its own URL and empty when there is none. A fork shares its " +
        "parent's commits, so a head SHA on its own does not say which " +
        "repository a pull request was opened against",
    ),
  unresolvedThreads: z
    .number()
    .int()
    .describe(
      "How many review threads are still open, whatever commit they were left " +
        "on. Zero when there are none and equally when GitHub could not be " +
        "asked — a caller acting on this treats it as 'nothing known to do', " +
        "which is what it did before the field existed",
    ),
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
  reviewedHead: z
    .array(z.string())
    .describe(
      "Logins that left feedback — a comment or a request for changes — on " +
        "the commit now at the head. An approval is not feedback and is not " +
        "counted. Empty with checks green means there is nothing to address " +
        "on this commit, which is waiting rather than work",
    ),
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

/** One entry of `gh`'s reviews. */
export interface ReviewEntry {
  author?: { login?: string };
  state?: string;
  commit?: { oid?: string };
  submittedAt?: string;
}

/**
 * Review states that decide where an author stands. A comment is not one of
 * them: GitHub does not dismiss an approval because its author said something
 * afterwards, and neither does this.
 */
const DECISIVE = ["APPROVED", "CHANGES_REQUESTED", "DISMISSED"];

/**
 * Where each author stands, as their latest decisive review.
 *
 * GitHub's own `latestReviews` field is not used, and cannot be: `gh pr view
 * --json latestReviews` returns `commit.oid` as an empty string for every entry
 * — the field is in the shape gh prints but not in the query it sends — while
 * `--json reviews` populates it. Since an approval is of a commit, a coverage
 * test built on `latestReviews` compares against "" and rejects every approval
 * ever given, so the whole history is fetched and reduced here instead.
 *
 * Reducing to each author's newest review outright would be wrong for the same
 * reason `latestReviews` would be misleading: a reviewer who approves and then
 * leaves a comment has not withdrawn the approval, but their newest review is
 * the comment, and the flow would report them as not having approved — sending
 * the fixer at an already-approved pull request until the retry budget runs
 * out. So a comment never displaces a decision. An author with nothing but
 * comments is still represented by their newest one, which carries no weight in
 * `approval` but keeps the list a faithful account of who has spoken.
 *
 * Ordering is by `submittedAt`, not by position: the array's order is GitHub's
 * business, and an approval wrongly treated as the older of two decides whether
 * the flow believes the pull request is reviewed.
 */
export function latestPerAuthor(reviews: ReviewEntry[]): ReviewEntry[] {
  const decisive = new Map<string, ReviewEntry>();
  const anything = new Map<string, ReviewEntry>();
  const newer = (a: ReviewEntry, b?: ReviewEntry) =>
    !b || (a.submittedAt ?? "") >= (b.submittedAt ?? "");
  for (const review of reviews) {
    const login = review.author?.login ?? "";
    if (!login) continue;
    if (newer(review, anything.get(login))) anything.set(login, review);
    if (!DECISIVE.includes((review.state ?? "").toUpperCase())) continue;
    if (newer(review, decisive.get(login))) decisive.set(login, review);
  }
  return [...anything.keys()].map((login) =>
    decisive.get(login) ?? anything.get(login)!
  );
}

/**
 * Review states that leave something to address.
 *
 * An approval does not. Nor does a dismissed review, which is one somebody has
 * already decided no longer applies, or a pending one, which has not been
 * submitted and is visible to nobody but its author.
 */
const FEEDBACK_STATES = new Set(["CHANGES_REQUESTED", "COMMENTED"]);

/**
 * Logins that left feedback on the commit now at the head.
 *
 * This is the difference between a pull request with feedback waiting to be
 * addressed and one that is merely waiting to be looked at. Both report
 * `approved: false` with every check green, and only the first is something an
 * agent can act on — so the sweep reads this before spending an attempt.
 *
 * Which is why an approval is not counted, though it is a review of the head.
 * With required approvers configured, somebody else's approval leaves
 * `approved` false while making "has anyone reviewed this commit" true, and the
 * sweep would then send an agent to address feedback that does not exist. It
 * finds nothing, records no push, fails the progress check, and every sweep
 * after it spends another attempt on a pull request that was only ever waiting
 * for the named reviewer.
 */
export function reviewedHead(
  reviews: ReviewEntry[],
  headSha: string,
): string[] {
  if (headSha === "") return [];
  // Each author's *last* word on this commit, not every word they said about
  // it. Somebody who commented and then approved the same commit has nothing
  // outstanding, and counting the comment anyway sends the fixer at feedback
  // that was already answered — it finds nothing, pushes nothing, and spends
  // an attempt on a pull request that was only waiting for another reviewer.
  //
  // Not {@linkcode latestPerAuthor}, which prefers the latest *decisive*
  // review over the latest of any kind. That is right for counting approvals
  // and wrong here: it would let an older approval hide a newer comment, which
  // is the direction that loses live feedback rather than the one that invents
  // it.
  const last = new Map<string, ReviewEntry>();
  for (const review of reviews) {
    if ((review.commit?.oid ?? "") !== headSha) continue;
    const login = review.author?.login ?? "";
    if (!login) continue;
    const previous = last.get(login);
    if (
      !previous || (review.submittedAt ?? "") >= (previous.submittedAt ?? "")
    ) {
      last.set(login, review);
    }
  }
  const logins = new Set<string>();
  for (const [login, review] of last) {
    if (FEEDBACK_STATES.has((review.state ?? "").toUpperCase())) {
      logins.add(login);
    }
  }
  return [...logins];
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

/**
 * The `owner/name` a pull request URL names, or "" when it names none.
 *
 * Read out of the URL rather than asked for as a field, because `gh pr view`
 * offers the *head* repository and not the one the pull request was opened
 * against — and on an ordinary branch those are the same, which is exactly the
 * case where the distinction does not matter.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
/**
 * The `host/owner/name` a repository URL names, or "" when it names none.
 *
 * The checkout's own side of the identity, read from the URL `gh` reports for
 * it rather than from `nameWithOwner` — which answers `owner/name` and so
 * cannot tell this repository from an Enterprise mirror of it.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function identityOf(url: string): string {
  const match = url.trim().match(
    /^https?:\/\/([^/]+)\/([^/]+)\/([^/?#]+)/,
  );
  return match
    ? `${match[1]}/${match[2]}/${match[3].replace(/\.git$/, "")}`
    : "";
}

export function repositoryOf(url: string): string {
  const match = url.match(
    /^https?:\/\/([^/]+)\/([^/]+)\/([^/]+)\/pull\/\d+/,
  );
  // Host included, because `owner/name` is not an identity: an Enterprise
  // mirror of this repository answers to the same one, and a fork of it shares
  // every commit id — so a pull request there passes both the repository
  // comparison and the head comparison while belonging to somewhere else
  // entirely. `gh` makes the same distinction in its own `[HOST/]OWNER/REPO`.
  return match ? `${match[1]}/${match[2]}/${match[3]}` : "";
}

/**
 * Whether the pull request is one anybody could merge right now.
 *
 * GitHub's own mergeability as well as the checks it reports, because a
 * required check that never reports is invisible in the rollup and still blocks
 * the merge — a rollup that looks green is not on its own a pull request anyone
 * can land.
 *
 * An absent `mergeStateStatus` is not a pass, which is the half that used to be
 * wrong: it contradicted the rule {@linkcode MERGEABLE_STATES} states, since a
 * state nobody has computed may still be blocked, behind or conflicted, and
 * calling that ready stops the sweep polling a pull request that cannot land.
 * GitHub reports "UNKNOWN" while it is computing, so an empty string means the
 * field was missing from the payload altogether — rare, and worth a person
 * looking rather than a flow deciding.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function isReady(status: {
  state: string;
  isDraft: boolean;
  checksState: string;
  approved: boolean;
  mergeStateStatus: string;
}): boolean {
  const mergeState = status.mergeStateStatus.toUpperCase();
  // `UNSTABLE` is GitHub saying the checks that failed are not required, so
  // demanding `passing` as well made that state unreachable — and the two
  // halves of this function then contradicted each other, since
  // {@linkcode MERGEABLE_STATES} admits it. What that cost is a sweep: a pull
  // request anybody could merge read as not ready, and `phase-ci` triaged the
  // optional failure once per firing until its attempt budget ran out.
  //
  // Only `failing`, not every non-passing state: `pending` is a run that has
  // not reported yet, which says nothing about whether what it will report is
  // required.
  const checksAllow = status.checksState === "passing" ||
    (mergeState === "UNSTABLE" && status.checksState === "failing");
  return status.state === "OPEN" && !status.isDraft &&
    checksAllow && status.approved &&
    MERGEABLE_STATES.has(mergeState);
}

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
 * their own right — compared case-insensitively as whole logins with GitHub's
 * `[bot]` suffix ignored, so that `codex` matches `codex[bot]` without the
 * caller having to know how the bot is spelled, while a different account whose
 * name merely contains the required one does not satisfy it.
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
const ResolveArgsSchema = z.object({
  pr: z
    .string()
    .min(1)
    .describe(
      "The pull request whose threads are being resolved, as its URL. " +
        "Required and not inferred: this method mutates, and a mutation aimed " +
        "at whatever branch the checkout happens to be on is the failure it " +
        "would cause rather than the one it would prevent.",
    ),
  threadIds: z
    .array(z.string())
    .default([])
    .describe(
      "The review threads to resolve, as GraphQL node ids. Each is checked " +
        "against the pull request's own unresolved threads before anything is " +
        "mutated, so an id that names nothing there is reported rather than " +
        "sent.",
    ),
});

type ResolveArgs = z.infer<typeof ResolveArgsSchema>;

const ThreadsSchema = z.object({
  pr: z.string().describe("The pull request whose threads these are"),
  requested: z
    .array(z.string())
    .describe("Every thread id the caller asked to have resolved"),
  resolved: z
    .array(z.string())
    .describe("Thread ids this run resolved"),
  alreadyResolved: z
    .array(z.string())
    .describe(
      "Thread ids that were resolved before this run, which is not a failure: " +
        "a re-run of the same pass asks for the same threads",
    ),
  rejected: z
    .array(z.string())
    .describe(
      "Thread ids that name no thread on this pull request, and were " +
        "therefore not sent. An id an agent invented, or one belonging to " +
        "another pull request, lands here rather than in a mutation",
    ),
  executedAt: z.iso.datetime().describe("When the threads were resolved"),
});

/**
 * The `owner`, `name` and `number` in a pull request URL, or null.
 *
 * The mutation needs all three to list the threads it is allowed to touch, and
 * a URL is what every caller here has: the status record's own `url`, which
 * GitHub wrote.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function pullRequestRef(
  url: string,
): { owner: string; name: string; number: number } | null {
  const match = url.match(/^https?:\/\/[^/]+\/([^/]+)\/([^/]+)\/pull\/(\d+)/);
  return match
    ? { owner: match[1], name: match[2], number: Number(match[3]) }
    : null;
}

/**
 * Every review thread on a pull request, as id to whether it is resolved, or
 * null when GitHub could not be asked.
 *
 * `--paginate` follows the cursor because a pull request under review for a
 * while has hundreds: a first-page listing would undercount the unresolved ones
 * and reject valid ids as belonging to nothing.
 */
async function listThreads(
  ghBin: string,
  cwd: string,
  signal: AbortSignal,
  ref: { owner: string; name: string; number: number },
): Promise<Map<string, boolean> | null> {
  try {
    const { success, stdout } = await new Deno.Command(ghBin, {
      args: [
        "api",
        "graphql",
        "--paginate",
        "-F",
        `owner=${ref.owner}`,
        "-F",
        `name=${ref.name}`,
        "-F",
        `number=${ref.number}`,
        "-f",
        `query=query($owner: String!, $name: String!, $number: Int!, $endCursor: String) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) {
              reviewThreads(first: 100, after: $endCursor) {
                pageInfo { hasNextPage endCursor }
                nodes { id isResolved }
              }
            }
          }
        }`,
        "-q",
        ".data.repository.pullRequest.reviewThreads.nodes[] | [.id, (.isResolved|tostring)] | @tsv",
      ],
      cwd,
      stdin: "null",
      stdout: "piped",
      stderr: "null",
      signal,
    }).output();
    if (!success) return null;
    const threads = new Map<string, boolean>();
    for (const line of new TextDecoder().decode(stdout).split("\n")) {
      const [id, resolved] = line.trim().split("\t");
      if (id) threads.set(id, resolved === "true");
    }
    return threads;
  } catch {
    return null;
  }
}

/**
 * Sort the requested ids into what may be resolved, what already is, and what
 * names nothing on this pull request.
 *
 * The whole point of doing this in the model rather than in the agent: an id is
 * checked against the threads the pull request actually has *before* anything is
 * mutated. An agent that matched a thread to the wrong fix, or invented an id,
 * otherwise resolves live feedback inside its own subprocess, where no later
 * validation of its answer can undo it.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function sortThreads(
  requested: string[],
  threads: Map<string, boolean>,
): { toResolve: string[]; alreadyResolved: string[]; rejected: string[] } {
  const toResolve: string[] = [];
  const alreadyResolved: string[] = [];
  const rejected: string[] = [];
  for (const id of requested) {
    if (!threads.has(id)) rejected.push(id);
    else if (threads.get(id)) alreadyResolved.push(id);
    else toResolve.push(id);
  }
  return { toResolve, alreadyResolved, rejected };
}

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
  // `reviews` rather than `latestReviews`: only this one carries the commit
  // each review was given on. See latestPerAuthor.
  "reviews",
  "reviewDecision",
].join(",");

/**
 * Resolve a path against a base, collapsing `.` and `..` segments so that a
 * `workingDir` pointing outside the swamp repository does not leak `..` into
 * every recorded path.
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

/** Model definition reporting a pull request's checks and approvals. */
export const model = {
  type: "@gdesmott/github-pr-status",
  version: "2026.08.18.2",
  description:
    "Report a pull request's check results and review approvals as structured data",
  globalArguments: GlobalArgsSchema,
  resources: {
    status: {
      description: "Whether the pull request is green and approved",
      schema: StatusSchema,
      lifetime: "infinite",
      // Deep enough that a sweep threshold can be counted out of it. This is
      // one record per sweep and the instance is shared between pull requests,
      // so retention is what bounds how far back a workflow counting "the same
      // commit seen pending N times" can actually see: at 50 a threshold above
      // that was unreachable, and the escalation it was there to trigger never
      // fired. `phase-ci`'s `maxPendingSweeps` names this number, and its
      // description carries the arithmetic.
      garbageCollection: 500,
    },
    threads: {
      description: "Which review threads a pass resolved, and which it refused",
      schema: ThreadsSchema,
      lifetime: "infinite",
      garbageCollection: 200,
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
          : resolve(context.repoDir, workingDir);

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

        // Which repository the checkout is, so a caller can compare it with the
        // one the pull request belongs to. Asked separately because `gh pr
        // view` answers about the pull request it was given and not about where
        // it was asked from — and given a URL it will happily answer about a
        // different repository altogether. Empty when it cannot be read, which
        // leaves the comparison to be skipped rather than to fail.
        const repository = await (async () => {
          try {
            const { success, stdout: out } = await new Deno.Command(ghBin, {
              args: [
                "repo",
                "view",
                "--json",
                "url",
                "-q",
                ".url",
              ],
              cwd,
              stdin: "null",
              stdout: "piped",
              stderr: "null",
              signal,
            }).output();
            if (!success) return "";
            return identityOf(new TextDecoder().decode(out));
          } catch {
            return "";
          }
        })();

        // The commit the checkout is on. Read here rather than left to a
        // caller because it has to be read where `gh` was asked — a phase that
        // fixes a pull request works in this directory, and whether that is the
        // pull request being reported on is exactly the question. Empty when it
        // cannot be read, which leaves the comparison to be skipped rather
        // than to fail.
        const localHead = await (async () => {
          try {
            const { success, stdout: out } = await new Deno.Command("git", {
              args: ["rev-parse", "HEAD"],
              cwd,
              stdin: "null",
              stdout: "piped",
              stderr: "null",
              signal,
            }).output();
            if (!success) return "";
            return new TextDecoder().decode(out).trim();
          } catch {
            return "";
          }
        })();

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
            repository,
            localHead,
            unresolvedThreads: 0,
            prRepository: "",
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
            reviewedHead: [],
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
          reviews?: ReviewEntry[];
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
          latestPerAuthor(payload.reviews ?? []),
          reviewDecision,
          args.requiredApprovers,
          headSha,
        );
        const mergeStateStatus = payload.mergeStateStatus ?? "";
        const state = payload.state ?? "";
        const isDraft = payload.isDraft ?? false;

        // How much review feedback is still open, counted from the threads
        // themselves rather than inferred from the reviews. A `COMMENTED`
        // review whose thread nobody answered stops being visible in
        // `reviewDecision` and `reviewedHead` the moment another commit is
        // pushed — the decision is not CHANGES_REQUESTED and the review no
        // longer names the head — so a sweep reading only those treats a pull
        // request with outstanding feedback as one merely awaiting review, and
        // does nothing about it for ever.
        //
        // Zero when GitHub could not be asked, which leaves a caller where it
        // was before this existed rather than sending an agent at a number
        // nobody established.
        const threadRef = pullRequestRef(payload.url ?? "");
        const allThreads = threadRef === null
          ? null
          : await listThreads(ghBin, cwd, signal, threadRef);
        const unresolvedThreads = allThreads === null
          ? 0
          : [...allThreads.values()].filter((resolved) => !resolved).length;

        const handle = await context.writeResource("status", "status", {
          found: true,
          reason: "",
          number: payload.number,
          url: payload.url ?? null,
          title: payload.title ?? null,
          headSha: payload.headRefOid ?? null,
          repository,
          localHead,
          unresolvedThreads,
          prRepository: repositoryOf(payload.url ?? ""),
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
          reviewedHead: reviewedHead(payload.reviews ?? [], headSha),
          missingApprovers: missing,
          ready: isReady({
            state,
            isDraft,
            checksState,
            approved,
            mergeStateStatus,
          }),
          executedAt: new Date().toISOString(),
        });

        return { dataHandles: [handle] };
      },
    },
    resolve: {
      description:
        "Resolve review threads by id, refusing any that name nothing on the " +
        "pull request",
      arguments: ResolveArgsSchema,
      execute: async (
        args: ResolveArgs,
        context: ExecuteContext,
      ): Promise<{ dataHandles: Array<{ name: string }> }> => {
        const { ghBin, workingDir, timeout } = context.globalArgs;
        const cwd = workingDir.startsWith("/")
          ? workingDir
          : resolve(context.repoDir, workingDir);
        const signal = AbortSignal.any([
          context.signal,
          AbortSignal.timeout(timeout),
        ]);

        const ref = pullRequestRef(args.pr);
        if (ref === null) {
          throw new Error(
            `${JSON.stringify(args.pr)} is not a pull request URL, and the ` +
              "threads that may be resolved are the ones belonging to a " +
              "particular pull request. Pass the URL the status record holds.",
          );
        }

        const gh = async (argv: string[]): Promise<string | null> => {
          const { success, stdout } = await new Deno.Command(ghBin, {
            args: argv,
            cwd,
            stdin: "null",
            stdout: "piped",
            stderr: "piped",
            signal,
          }).output();
          return success ? new TextDecoder().decode(stdout) : null;
        };

        const threads = await listThreads(ghBin, cwd, signal, ref);
        if (threads === null) {
          throw new Error(
            `Could not list the review threads of ${args.pr}. Nothing was ` +
              "resolved: an id is only safe to send once it has been matched " +
              "against the threads the pull request actually has.",
          );
        }

        const sorted = sortThreads(args.threadIds, threads);
        const resolved: string[] = [];
        for (const id of sorted.toResolve) {
          const done = await gh([
            "api",
            "graphql",
            "-F",
            `id=${id}`,
            "-f",
            `query=mutation($id: ID!) {
              resolveReviewThread(input: {threadId: $id}) { thread { id isResolved } }
            }`,
          ]);
          if (done === null) {
            throw new Error(
              `Resolving thread ${id} on ${args.pr} failed. ` +
                `${resolved.length} thread(s) were resolved before it: ` +
                `${resolved.join(", ") || "none"}.`,
            );
          }
          resolved.push(id);
        }

        context.logger.info(
          "Resolved {n} review thread(s) on {pr}; {already} already resolved, " +
            "{rejected} rejected",
          {
            n: resolved.length,
            pr: args.pr,
            already: sorted.alreadyResolved.length,
            rejected: sorted.rejected.length,
          },
        );

        const handle = await context.writeResource("threads", "threads", {
          pr: args.pr,
          requested: args.threadIds,
          resolved,
          alreadyResolved: sorted.alreadyResolved,
          rejected: sorted.rejected,
          executedAt: new Date().toISOString(),
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
