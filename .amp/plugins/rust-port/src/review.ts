/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import type { PluginAPI, ThreadID } from '@ampcode/plugin';
import { RUNS } from './confirm.ts';
import { sizeClass } from './gates.ts';
import { describe, STAGES, type Plan, type Stage } from './plan.ts';
import { GUIDES, RULE_FILES } from './redisearch.ts';
import type { FileRef, Repro } from './state.ts';

const ROLE = `You are an independent, read-only reviewer.

Please review these changes carefully. They were written by my intern, who is a bad programmer
and was often drunk or high while working. Their code has broken production before. Do not
trust any of it: assume every line is wrong until you have checked it yourself. Anything you
miss ships.

The plugin runs the checks and judges their results; do not review check logs or build output.
You cannot run commands; the plugin runs your reproductions for you.`;

const FORMAT = `
Reply with only a JSON object:
{"findings": [{"location": "file:line or symbol",
  "scenario": "the input or state, and the wrong result it produces",
  "correction": "the smallest change that fixes it; prefer deleting code to adding it",
  "refs": [{"path": "file relative to the workspace root", "line": 12}],
  "repro": {"path": "test file", "test": "test name", "command": "runs that test from the root",
    "patch": "unified diff, as git apply takes it, adding the failing test"},
  "deletion": {"patch": "unified diff removing code the stage does not need"}}]}
\`refs\` cites where the defect is and, for a broken rule, the rule's line in ${RULE_FILES};
cite deleted code by its line in the diff file. A finding whose refs do not hold is discarded. A
finding blocks only when the plugin confirms it: its repro patch, touching tests only, applies and
its command (one of ${RUNS}) then fails; its deletion patch, touching no test, removes more than
it adds and every test check still passes; or it cites a rule. Anything else is a suggestion. Report each problem once; return
{"findings": []} when there is nothing to report.`;

export interface Reviewer {
  name: string;
  instructions: string;
}

export const REVIEWERS: Reviewer[] = [
  {
    name: 'Correctness',
    instructions: `Review correctness against the plan: its contract, its divergences from C, what
replaces each C function, the stage's deliverable and the recorded interfaces. C behavior the
contract does not list may change, provided the plan lists the divergence.
Look for inputs or states that break a contract item, behave differently from C without a
listed divergence, crash on user input or are undefined behavior; for a deliverable that is
incomplete or departs from the recorded interfaces; and for build scripts, lint exclusions and
CI paths the change breaks. Prove each behavior defect with a failing test as its repro.
Check that each contract item has tests named for the behavior they assert, and that an item
holding over a range of inputs, such as a conversion, a bound or an ordering, has a property
test over generated inputs rather than a few hand-picked values; cite the repository rule.
At the FFI boundary, check every pointer for three things separately, because one does not imply
another: initialization (writable memory may be uninitialized, so write through the raw pointer
instead of making a reference to it), aliasing (no reference may overlap another live one or
outlive what it points to), and ownership (who frees it, with which allocator, exactly once).`,
  },
  {
    name: 'Idiomatic Rust',
    instructions: `Review only the Rust: idiomatic, small, readable code, documentation and tests.
Make two passes. First bugs and bloat, which matter most: look hardest for code the stage does
not need, and give each such finding a deletion patch. Then design and house style: go through
${GUIDES}, checking the diff against each rule rather than waiting for one to stand out. Cite the
line of the rule a finding breaks, in ${RULE_FILES}. Matters of taste cite no rule.
A property test must state an invariant the implementation does not spell out, such as a round
trip, an ordering, or agreement with a simpler model. One that restates the implementation's
expression proves nothing: give it a deletion patch.`,
  },
];

/** What the stage's reviewers read first. */
export function brief(
  reviewer: Reviewer,
  o: {
    plan: Plan;
    stage: Stage;
    candidate: string;
    base: string;
    diff: string;
    stat: string;
    lines: number;
  },
): string {
  const { deliverable, paths, checks } = o.plan.stages[o.stage];
  const number = STAGES.indexOf(o.stage) + 1;
  const big = sizeClass(o.lines);
  return `# ${label(o.stage, reviewer)} review, stage ${number} of ${STAGES.length}

## This stage must deliver

${deliverable}

It may change only ${paths.map((p) => `\`${p}\``).join(', ')}, and adds ${o.lines} lines.${big ? ` ${big.note}` : ''}
Later stages: ${
    STAGES.slice(number)
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

The C being ported, ${o.plan.module.map((p) => `\`${p}\``).join(', ')}, is in the working tree
until \`swap\` deletes it. Repository rules are in ${RULE_FILES}.

## The plan

${describe(o.plan)}
`;
}

/**
 * What a reviewer is asked once the author answered it: the change since, not the stage again, so
 * findings do not trickle in round after round. `delta` is absent after a rebase.
 */
export function followUp(o: { previous: string; candidate: string; diff: string; delta?: string }) {
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

/** How findings, logs and thread labels name a reviewer, e.g. `swap / Correctness`. */
export const label = (stage: Stage, reviewer: Reviewer) => `${stage} / ${reviewer.name}`;

interface Options {
  reviewer: Reviewer;
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
export async function review(amp: PluginAPI, o: Options): Promise<Review> {
  if (o.resume) {
    const resumed = await attempt(amp, o, o.resume);
    if (!resumed.error || o.signal.aborted) return resumed;
  }
  const first = await attempt(amp, o);
  return first.error && !o.signal.aborted ? attempt(amp, o) : first;
}

/**
 * The stage's reviews: fresh threads on the first review under the plan, then the same threads
 * asked about the change since `prior`. A reviewer that fails twice throws, because that says
 * nothing about the candidate.
 */
export async function reviewStage(
  amp: PluginAPI,
  o: {
    plan: Plan;
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
  const { prior, stage, signal, timeoutMs } = o;
  const message = prior && followUp({ ...o, previous: prior.candidate });
  const reviews = await Promise.all(
    REVIEWERS.map(async (reviewer) => {
      const thread = prior?.reviewers.find((r) => r.reviewer === label(stage, reviewer))?.thread;
      const r = await review(amp, {
        ...{ reviewer, stage, owner: o.owner, signal, timeoutMs, brief: brief(reviewer, o) },
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

async function attempt(amp: PluginAPI, o: Options, resume?: Options['resume']): Promise<Review> {
  const name = label(o.stage, o.reviewer);
  let thread = resume?.thread;
  try {
    const child = resume ? amp.threads.get(resume.thread as ThreadID) : await create(amp, o);
    thread = child.id;
    const cancel = () => void child.cancel().catch(() => {});
    o.signal.addEventListener('abort', cancel, { once: true });
    // An abort while the thread was being created or looked up fired before the listener.
    if (o.signal.aborted) cancel();
    const response = child.waitForResponse({ timeoutMs: o.timeoutMs });
    response.catch(() => {}); // Observed below; this keeps an early failure from going unhandled.
    await child.appendUserMessage({ type: 'user-message', content: resume?.message ?? o.brief });
    const message = await response.finally(() => o.signal.removeEventListener('abort', cancel));
    const text = message.content.map((b) => (b.type === 'text' ? b.text : '')).join('\n');
    const json = text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1);
    // Malformed proposals are kept for the plugin to drop and log, so one does not fail the rest.
    const proposals = JSON.parse(json).findings.map((f: Proposal): Proposal => {
      const { path, test, patch, command } = f.repro ?? {};
      const text = (v: unknown) => typeof v === 'string' && v !== '';
      const repro = [path, test, patch, command].every(text) ? f.repro : undefined;
      const deletion = text(f.deletion?.patch) ? f.deletion : undefined;
      return { ...f, refs: Array.isArray(f.refs) ? f.refs : [], repro, deletion };
    });
    return { label: name, thread, proposals };
  } catch (error) {
    return { label: name, thread, error: String(error), proposals: [] };
  }
}

async function create(amp: PluginAPI, o: Options) {
  const agent = amp.createAgent({
    extends: 'high',
    instructions: `${ROLE}\n\n${o.reviewer.instructions}\n${FORMAT}`,
    tools: { include: ['Read', 'Grep', 'glob'], exclude: ['tool_search', 'code_exec'] },
  });
  const child = await agent.createThread({ parentThreadID: o.owner as ThreadID });
  // Amp generates thread titles itself; labels are what a plugin can set to name the reviewer.
  const slug = o.reviewer.name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
  await child.addLabels(['rust-port', o.stage, slug]).catch(() => {});
  return child;
}
