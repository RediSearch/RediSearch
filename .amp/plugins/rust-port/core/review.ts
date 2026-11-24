/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * Reviewer threads: what they are told, how a stage's reviewers run, and the proposals they reply
 * with, which {@link confirmProposals} then tries.
 */
import { sizeClass } from './diff.ts';
import type { FileRef, Repro } from './job.ts';
import { describePlan, type Plan } from './plan.ts';
import type { Host, Profile, Stage } from './ports.ts';

const ROLE = `You are an independent, read-only reviewer.

Please review these changes carefully. They were written by my intern, who is a bad programmer
and was often drunk or high while working. Their code has broken production before. Do not
trust any of it: assume every line is wrong until you have checked it yourself. Anything you
miss ships.

The plugin runs the checks and judges their results; do not review check logs or build output.
Read the diff and the code it touches; follow callers and other modules only as far as a finding
needs, with Grep before Read, so your context holds the change rather than the codebase.
You cannot run commands; the plugin runs your reproductions for you.`;

export interface Reviewer {
  name: string;
  instructions: string;
}

/** A finding as a reviewer reported it, before the plugin checks its evidence. */
export interface Proposal {
  location: string;
  scenario: string;
  correction: string;
  refs: FileRef[];
  repro?: Repro;
  deletion?: { patch: string };
}

export interface Review {
  label: string;
  thread?: string;
  error?: string;
  proposals: Proposal[];
}

/** The runners a reproduction may use, as the reviewers and the worker read them. */
export const runnerList = (profile: Pick<Profile, 'testRunners'>) =>
  profile.testRunners.map((r) => `\`${r}\``).join(', ');

const replyFormat = (profile: Pick<Profile, 'testRunners' | 'ruleFiles'>) => `
Reply with only a JSON object:
{"findings": [{"location": "file:line or symbol",
  "scenario": "the input or state, and the wrong result it produces",
  "correction": "the smallest change that fixes it; prefer deleting code to adding it",
  "refs": [{"path": "file relative to the workspace root", "line": 12}],
  "repro": {"path": "test file", "test": "test name", "command": "runs that test from the root",
    "patch": "unified diff, as git apply takes it, adding the failing test"},
  "deletion": {"patch": "unified diff removing code the stage does not need"}}]}
\`refs\` cites where the defect is and, for a broken rule, the rule's line in ${profile.ruleFiles};
cite deleted code by its line in the diff file. A finding whose refs do not hold is discarded. A
finding blocks only when the plugin confirms it: its repro patch, touching tests only, applies and
its command (one of ${runnerList(profile)}) then fails; its deletion patch, touching no test, removes more than
it adds and every test check still passes; or it cites a rule. Anything else is a suggestion. Report each problem once; return
{"findings": []} when there is nothing to report.`;

/** How findings, logs and thread labels name a reviewer, e.g. `swap / Correctness`. */
export const reviewerLabel = (stage: Stage, reviewer: Reviewer) => `${stage} / ${reviewer.name}`;

/** What the stage's reviewers read first. */
export function reviewBrief<X extends object>(
  reviewer: Reviewer,
  o: {
    plan: Plan<X>;
    profile: Profile<X>;
    stage: Stage;
    candidate: string;
    base: string;
    diff: string;
    stat: string;
    lines: number;
  },
): string {
  const { deliverable, paths, checks } = o.plan.stages[o.stage];
  const stages = o.profile.stages.map((s) => s.name);
  const number = stages.indexOf(o.stage) + 1;
  const big = sizeClass(o.lines);
  return `# ${reviewerLabel(o.stage, reviewer)} review, stage ${number} of ${stages.length}

## This stage must deliver

${deliverable}

It may change only ${paths.map((p) => `\`${p}\``).join(', ')}, and adds ${o.lines} lines.${big ? ` ${big.note}` : ''}
Later stages: ${
    stages
      .slice(number)
      .map((s) => `\`${s}\`: ${o.plan.stages[s].deliverable}`)
      .join(' ') || 'none'
  }

## Candidate

Revision \`${o.candidate}\` is checked out; read files from the working tree. Base \`${o.base}\`.
The full diff is in \`${o.diff}\`.

\`\`\`
${o.stat}
\`\`\`

These checks passed: ${checks.map((c) => `\`${c.command}\``).join(', ')}.

Repository rules are in ${o.profile.ruleFiles}.

## The plan

${describePlan(o.plan, o.profile)}
`;
}

/**
 * What a reviewer is asked once the author answered it: the change since, not the stage again, so
 * findings do not trickle in round after round. `delta` is absent after a rebase.
 */
export function followUpBrief(o: {
  previous: string;
  candidate: string;
  diff: string;
  delta?: string;
}) {
  const [previous, candidate] = [o.previous, o.candidate].map((c) => c.slice(0, 10));
  const change = o.delta
    ? `What changed since: \`${o.delta}\`. The whole stage diff: \`${o.diff}\`.`
    : `It was rebased, so compare its whole diff, \`${o.diff}\`, with the version you reviewed.`;
  return `# Re-review: ${candidate}

The author answered your review of ${previous} and claims ${candidate}. ${change}

Review the change, not the whole stage again:
- Check that each fix does what it says; raise it again only if it does not, and say why.
- Raise a new finding only on lines the change touches, unless it crashes, is undefined behavior,
  breaks memory safety or loses data: raise those wherever they are.
- If a fix draws a new problem in the same hunk, raise it once as a design decision, naming the
  trade-off, rather than a third variation of the same concern.

Reply in the same JSON format. An empty list is a good outcome.
`;
}

/** The proposals in a reviewer's reply; throws if it holds no readable verdict. */
export function parseProposals(text: string): Proposal[] {
  const json = text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1);
  // Malformed proposals are kept for the plugin to drop and log, so one does not fail the rest.
  return JSON.parse(json).findings.map((f: Proposal): Proposal => {
    const { path, test, patch, command } = f.repro ?? {};
    const given = (v: unknown) => typeof v === 'string' && v !== '';
    const repro = [path, test, patch, command].every(given) ? f.repro : undefined;
    const deletion = given(f.deletion?.patch) ? f.deletion : undefined;
    return { ...f, refs: Array.isArray(f.refs) ? f.refs : [], repro, deletion };
  });
}

interface Options {
  reviewer: Reviewer;
  /** What the reviewer's thread is told it is. */
  role: string;
  stage: Stage;
  owner: string;
  brief: string;
  /** The reviewer's earlier thread and what to ask it, to review the change since. */
  resume?: { thread: string; message: string };
  signal: AbortSignal;
  timeoutMs: number;
}

/**
 * Runs a read-only reviewer thread, or resumes its earlier one, and parses what it proposes. A
 * failed thread is retried once with a fresh one, unless the review was cancelled.
 */
export async function runReviewer(host: Host, o: Options): Promise<Review> {
  if (o.resume) {
    const resumed = await tryReviewer(host, o, o.resume);
    if (!resumed.error || o.signal.aborted) return resumed;
  }
  const first = await tryReviewer(host, o);
  return first.error && !o.signal.aborted ? tryReviewer(host, o) : first;
}

async function tryReviewer(host: Host, o: Options, resume?: Options['resume']): Promise<Review> {
  const name = reviewerLabel(o.stage, o.reviewer);
  let thread = resume?.thread;
  try {
    const slug = o.reviewer.name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
    thread ??= await host.startThread({ parent: o.owner, labels: [o.stage, slug], role: o.role });
    const text = await host.ask(thread, resume?.message ?? o.brief, o);
    return { label: name, thread, proposals: parseProposals(text) };
  } catch (error) {
    return { label: name, thread, error: String(error), proposals: [] };
  }
}

/**
 * The stage's reviews: fresh threads on the first review under the plan, then the same threads
 * asked about the change since `prior`. A reviewer that fails twice throws, because that says
 * nothing about the candidate.
 */
export async function reviewStage<X extends object>(
  host: Host,
  o: {
    plan: Plan<X>;
    profile: Profile<X>;
    stage: Stage;
    owner: string;
    candidate: string;
    base: string;
    diff: string;
    delta?: string;
    stat: string;
    lines: number;
    prior?: { candidate: string; reviewers: { reviewer: string; thread?: string }[] };
    signal: AbortSignal;
    timeoutMs: number;
    debug: (message: string) => void;
  },
): Promise<Review[]> {
  const { prior, stage, signal, timeoutMs, profile } = o;
  const message = prior && followUpBrief({ ...o, previous: prior.candidate });
  const reviews = await Promise.all(
    profile.reviewers.map(async (reviewer) => {
      const thread = prior?.reviewers.find(
        (r) => r.reviewer === reviewerLabel(stage, reviewer),
      )?.thread;
      const r = await runReviewer(host, {
        ...{ reviewer, stage, owner: o.owner, signal, timeoutMs, brief: reviewBrief(reviewer, o) },
        role: `${ROLE}\n\n${reviewer.instructions}\n${replyFormat(profile)}`,
        resume: thread && message ? { thread, message } : undefined,
      });
      o.debug(
        `${r.label} reviewer ${r.thread ?? ''} ${r.error ? `failed: ${r.error}` : `${r.proposals.length} proposals`}`,
      );
      return r;
    }),
  );
  const failed = reviews.filter((r) => r.error);
  if (failed.length && !signal.aborted)
    throw new Error(
      `${failed.map((r) => `${r.label} reviewer failed (${r.error})`).join('; ')}. Nothing was charged; claim again.`,
    );
  return reviews;
}
