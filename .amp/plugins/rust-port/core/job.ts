/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * The job and everything it remembers: the plan, the evidence of each assessment, and the
 * findings, which are everything left to do. Functions here only read and change that state.
 */
import { createHash } from 'node:crypto';
import type { CheckResult } from './checks.ts';
import type { Size } from './diff.ts';
import type { Plan } from './plan.ts';
import type { PR, Stage } from './ports.ts';

/** Assessments a job may run before an operator grants more. */
export const BUDGET = 30;

export const TARGETS = ['plugin', 'policy', 'skills', 'checks', 'reviewers'] as const;
/** Each retro list and the text fields its items need, besides their evidence. */
export const RETRO = {
  mistakes: ['what', 'cost', 'prevention'],
  cuts: ['what', 'lines'],
  improvements: ['problem', 'proposal', 'target'],
} as const;

export interface State<X = {}> {
  job: Job<X>;
  evidence: Record<string, Evidence>;
  findings: Record<string, Finding>;
}

export interface Job<X = {}> {
  owner: string;
  /** Worker threads that owned the job before {@link Job.owner}: one per stage handed on. */
  former?: string[];
  plan: Plan<X>;
  /** Evidence and admissions hold only under the plan they were gathered under. */
  planHash: string;
  started: string;
  admitted: Partial<Record<Stage, { candidate: string; base: string; plan: string }>>;
  prs: Partial<Record<Stage, PR>>;
  /** Assessments run; grants extend {@link BUDGET}. */
  used: number;
  grants: { amount: number; at: string; by: string }[];
  /** Number of the last finding created, and of the last one shown to the worker. */
  created: number;
  delivered: number;
  /** IDs of the comments the plugin posted, which feedback collection leaves out. */
  posted: number[];
  /** A blocker the worker reported; its next claim clears it. */
  blocked?: string;
  /** {@link progressMark} at the last automatic continuation. */
  continued?: string;
  retro?: Retro;
}

/** What one assessment of a candidate showed, bound to its stage, base and plan. */
export interface Evidence {
  stage: Stage;
  candidate: string;
  base: string;
  plan: string;
  /** {@link patchId} of the stage's diff: equal for the same change rebased. */
  patch: string;
  checks: CheckResult[];
  /** Empty when a check failed, because then the reviewers do not run. */
  reviewers: { reviewer: string; thread?: string }[];
  size: Size;
}

/** A file in the workspace, a line of it, or a test it names, as evidence the plugin checks. */
export interface FileRef {
  path: string;
  line?: number;
  test?: string;
}
export type Ref = FileRef | { finding: string };

/** A failing test as a patch adding it to `path`, and the command that runs it. */
export interface Repro {
  path: string;
  test: string;
  patch: string;
  command: string;
}

/**
 * Who closes a finding: the worker closes reviewer findings and review comments; failed checks and
 * CI close themselves once they pass.
 */
export type FindingSource = 'review' | 'comment' | 'check' | 'ci';

export interface Finding {
  readonly id: string;
  /** Identity of what was reported; exact duplicates share it. */
  readonly key: string;
  readonly source: FindingSource;
  readonly stage: Stage;
  /** The revision it was raised against. */
  readonly revision: string;
  readonly blocking: boolean;
  readonly title: string;
  readonly body: string;
  /** The failing test that reproduced it, which every later claim re-runs. */
  readonly repro?: Repro;
  links: string[];
  /** The forge comments to answer and threads to resolve; each is dropped once done. */
  replies?: { pr: number; comment?: number; thread?: string }[];
  /** The PRs a reply was posted on. */
  answered?: number[];
  closed?: {
    /** `superseded`: the same failure was reported again on a newer revision. */
    resolution: 'fixed' | 'no-change' | 'passed' | 'superseded';
    reason?: string;
    evidence?: FileRef[];
  };
}
export type NewFinding = Omit<Finding, 'id'>;

/** A postmortem about the next iteration: what went wrong, what to cut, what to change. */
export interface Retro {
  mistakes: { what: string; cost: string; prevention: string; evidence: Ref[] }[];
  cuts: { what: string; lines: string; evidence: Ref[] }[];
  improvements: {
    problem: string;
    proposal: string;
    target: (typeof TARGETS)[number];
    evidence: Ref[];
  }[];
}

/** A short digest of `parts`, independent of the order of keys in any object among them. */
export function hash(...parts: unknown[]): string {
  const sorted = (_: string, value: unknown) =>
    value && typeof value === 'object' && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)))
      : value;
  return createHash('sha256').update(JSON.stringify(parts, sorted)).digest('hex').slice(0, 16);
}

export function newState<X extends object>(owner: string, plan: Plan<X>): State<X> {
  const job: Job<X> = {
    ...{ owner, plan, planHash: hash(plan), started: new Date().toISOString() },
    ...{ admitted: {}, prs: {}, used: 0, grants: [], created: 0, delivered: 0, posted: [] },
  };
  return { job, evidence: {}, findings: {} };
}

export const budget = (job: Job) => job.grants.reduce((sum, g) => sum + g.amount, BUDGET);

/** Why the job may not go on without a human, if it may not. */
export function stopReason(job: Job): string | undefined {
  if (job.blocked) return `blocked: ${job.blocked}`;
  if (job.used >= budget(job)) return `the assessment budget (${budget(job)}) is used up`;
}

/** Changes whenever a turn moved the job: an assessment, a finding raised or closed, an admission. */
export function progressMark(state: State): string {
  const closed = Object.values(state.findings).filter((f) => f.closed).length;
  const { used, created, admitted, retro, planHash } = state.job;
  return hash(used, created, closed, admitted, !!retro, planHash);
}

/** The plan's stages, in the profile's order, which the engine keeps them in. */
export const stageOrder = (job: Job): Stage[] => Object.keys(job.plan.stages);

export const previousStage = (job: Job, stage: Stage): Stage | undefined =>
  stageOrder(job)[stageOrder(job).indexOf(stage) - 1];

/** A stage stays admitted while the plan and the admitted stage it was built on are unchanged. */
export function isAdmitted(job: Job, stage: Stage): boolean {
  const admission = job.admitted[stage];
  const prev = previousStage(job, stage);
  if (admission?.plan !== job.planHash) return false;
  return !prev || (isAdmitted(job, prev) && admission.base === job.admitted[prev]!.candidate);
}

export const passed = (evidence: Evidence) => evidence.checks.every((c) => !c.problem);

/** The stage's reviewed evidence under the current plan, oldest first. */
export const reviewedEvidence = (state: State, stage: Stage) =>
  Object.values(state.evidence).filter(
    (e) => e.stage === stage && e.plan === state.job.planHash && e.reviewers.length,
  );

/**
 * The replies due on review threads: a no-change answer at once, a fix once its stage's PR moved
 * past the revision the finding was raised on, quoting the head that carries it.
 */
export function repliesDue(state: State): { finding: Finding; reply: string }[] {
  return Object.values(state.findings).flatMap((finding) => {
    const { closed, replies, stage, revision } = finding;
    if (!closed || !replies?.length) return [];
    if (closed.resolution !== 'fixed') return [{ finding, reply: closed.reason! }];
    const head = state.job.prs[stage]?.head;
    return head && head !== revision
      ? [{ finding, reply: `Fixed in ${head.slice(0, 10)}: ${closed.reason}` }]
      : [];
  });
}

export function outstanding(state: State): Finding[] {
  const stages = stageOrder(state.job);
  return Object.values(state.findings)
    .filter((f) => !f.closed)
    .sort((a, b) => stages.indexOf(a.stage) - stages.indexOf(b.stage) || seq(a) - seq(b));
}

/** CI runs only after publication, so its findings cannot hold back the admission that publishes. */
export const blocksAdmission = (f: Finding, stage: Stage) =>
  f.stage === stage && f.blocking && f.source !== 'ci';

/** Every stage admitted, nothing outstanding and every reply posted: the retrospective is next. */
export function done(state: State): boolean {
  return (
    stageOrder(state.job).every((s) => isAdmitted(state.job, s)) &&
    !outstanding(state).length &&
    !Object.values(state.findings).some((f) => f.closed && f.replies?.length)
  );
}

const seq = (f: Finding) => Number(f.id.slice(1));

function insert(state: State, finding: NewFinding) {
  const id = `F${++state.job.created}`;
  state.findings[id] = { id, ...finding };
}

/**
 * Adds findings whose key is new. A duplicate merges into the existing finding: an open one gains
 * its links, and either gains its replies, so a comment on two stacked PRs is answered on both.
 */
export function addFindings(state: State, findings: NewFinding[]): void {
  for (const finding of findings) {
    const existing = Object.values(state.findings).find((f) => f.key === finding.key);
    if (!existing) {
      insert(state, finding);
      continue;
    }
    // The same comment again on a PR it was answered on is new feedback, such as a regression.
    const again = finding.replies?.find(
      (r) => existing.closed && existing.answered?.includes(r.pr),
    );
    if (again) {
      const key = `${finding.key}:${again.thread ?? again.comment}`;
      if (!Object.values(state.findings).some((f) => f.key === key))
        insert(state, { ...finding, key });
      continue;
    }
    if (!existing.closed) existing.links = [...new Set([...existing.links, ...finding.links])];
    for (const reply of finding.replies ?? [])
      if (!existing.replies?.some((r) => r.comment === reply.comment))
        existing.replies = [...(existing.replies ?? []), reply];
  }
}

/**
 * Replaces the open findings of one stage and source with the current failures. A failure reported
 * again on another revision, or with other details, supersedes the open one with a new finding,
 * so the worker hears of it.
 */
export function replaceFindings(
  state: State,
  stage: Stage,
  source: FindingSource,
  failures: NewFinding[],
) {
  const open = Object.values(state.findings).filter(
    (f) => !f.closed && f.stage === stage && f.source === source,
  );
  for (const finding of open)
    if (!failures.some((f) => f.key === finding.key)) finding.closed = { resolution: 'passed' };
  for (const failure of failures) {
    const existing = open.find((f) => f.key === failure.key);
    if (existing?.revision === failure.revision && existing.body === failure.body) continue;
    if (existing) existing.closed = { resolution: 'superseded' };
    insert(state, failure);
  }
}

/** Closes a finding the worker may close. On a review thread the reason becomes the reply. */
export function closeFinding(
  state: State,
  id: string,
  resolution: 'fixed' | 'no-change',
  reason: string | undefined,
  evidence: FileRef[],
): Finding {
  const finding = state.findings[id];
  if (!finding || finding.closed) throw new Error(`${id} is not an outstanding finding`);
  if (finding.source === 'check' || finding.source === 'ci')
    throw new Error(`${id} closes itself once it passes`);
  if (!evidence.length) throw new Error('Cite evidence: the files, lines or tests that show it');
  if (resolution === 'no-change' && !reason?.trim())
    throw new Error('A no-change resolution needs the reason it does not apply');
  if (
    finding.replies?.length &&
    !(reason?.trim() && reason.length <= 280 && !reason.includes('\n'))
  )
    throw new Error(`${id} is a review thread: give a one-line reason of at most 280 characters`);
  finding.closed = { resolution, reason, evidence };
  return finding;
}

/** Outstanding findings the worker has not been shown yet; marks them shown. */
export function takeNewFindings(state: State): Finding[] {
  const fresh = outstanding(state).filter((f) => seq(f) > state.job.delivered);
  state.job.delivered = state.job.created;
  return fresh;
}

/** The findings as a numbered to-do list, then how to close them with `closeTool`. */
export function formatFindings(findings: Finding[], closeTool: string): string {
  if (!findings.length) return '';
  const indent = (text: string) => text.replace(/^/gm, '   ');
  const items = findings.map((f, i) =>
    [
      `${i + 1}. ${f.id} · ${f.stage} · ${f.title}${f.blocking ? '' : ' · suggestion'}`,
      indent(f.body),
      indent(f.links.join(' ')),
    ]
      .filter((line) => line.trim())
      .join('\n'),
  );
  const any = (test: (f: Finding) => unknown) => findings.some(test);
  const how = [
    any((f) => f.source === 'review' || f.source === 'comment') &&
      `Close reviewer findings and comments with ${closeTool}: fixed, or no-change with the reason, citing the files, lines or tests that show it.`,
    any((f) => f.repro) &&
      "Every later claim re-runs a reproduced finding's test; commit it only if it is a regression test that belongs in the suite.",
    any((f) => f.replies?.length) &&
      'For a PR comment the reason is posted as the reply: one line of at most 280 characters.',
    any((f) => f.source === 'check' || f.source === 'ci') &&
      'Failed checks and CI close themselves once they pass.',
    any((f) => !f.blocking) && 'Suggestions do not hold admission but need closing.',
  ];
  return [...items, how.filter(Boolean).join(' ')].join('\n\n');
}

/** The run as a human reviewing its PRs reads it: its cost, its findings and its retrospective. */
export function runReport(state: State): string {
  const { job } = state;
  const findings = Object.values(state.findings);
  const tally = (key: (f: Finding) => string | undefined, keys: string[]) =>
    keys.map((k) => `${findings.filter((f) => key(f) === k).length} ${k}`).join(', ');
  const assessed = stageOrder(job).map((s) => {
    const claims = Object.values(state.evidence).filter((e) => e.stage === s).length;
    const at = isAdmitted(job, s) ? job.admitted[s]!.candidate.slice(0, 10) : undefined;
    return `\`${s}\` assessed ${claims} times${at ? `, admitted at ${at}` : ''}`;
  });
  const confirmed = findings.filter((f) => f.source === 'review' && f.blocking).length;
  const reviewed = findings.filter((f) => f.source === 'review').length;
  const lines = [
    '### Run',
    '',
    `- Started ${job.started}; ${job.used} of ${budget(job)} assessments used, ${job.grants.length} grants, ${(job.former?.length ?? 0) + 1} worker threads.`,
    `- ${assessed.join('; ')}.`,
    `- ${findings.length} findings: ${tally((f) => f.source, ['review', 'comment', 'check', 'ci'])}. ` +
      `${confirmed} of ${reviewed} reviewer findings confirmed or citing a rule.`,
    `- Closed: ${tally((f) => f.closed?.resolution, ['fixed', 'no-change', 'passed', 'superseded'])}.`,
  ];
  const { retro } = job;
  if (retro)
    lines.push(
      '',
      '### Retrospective',
      '',
      ...retro.mistakes.map(
        (m) => `- Mistake: ${m.what} Cost: ${m.cost} Prevention: ${m.prevention}`,
      ),
      ...retro.cuts.map((c) => `- Cut: ${c.what} (${c.lines} lines)`),
      ...retro.improvements.map(
        (i) => `- Improve ${i.target}: ${i.problem} Proposal: ${i.proposal}`,
      ),
    );
  return lines.join('\n');
}

/** Every list has an item, every item its fields and evidence, every improvement a target. */
export function validateRetro(retro: Retro): string[] {
  const errors: string[] = [];
  for (const [list, fields] of Object.entries(RETRO)) {
    const items: any[] = Array.isArray((retro as any)?.[list]) ? (retro as any)[list] : [];
    if (!items.length) errors.push(`${list} needs at least one item`);
    for (const [i, item] of items.entries()) {
      for (const field of fields)
        if (typeof item?.[field] !== 'string' || !item[field].trim())
          errors.push(`${list}[${i}] needs ${field}`);
      if (!item?.evidence?.length) errors.push(`${list}[${i}] needs evidence`);
    }
  }
  for (const [i, item] of (retro?.improvements ?? []).entries())
    if (!TARGETS.includes(item?.target))
      errors.push(`improvements[${i}].target must be one of ${TARGETS.join(', ')}`);
  return errors;
}
