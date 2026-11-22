/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * Confirming reviewer proposals by trying them: a proposal blocks only when its failing test
 * fails, its deletion keeps every test passing, or it cites a repository rule.
 */
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { readLog, runChecks, type Check, type CheckResult, type CheckEnv } from './checks.ts';
import { parseDiff } from './diff.ts';
import { hash, type NewFinding, type Repro } from './job.ts';
import type { Profile, Stage } from './ports.ts';
import { resolveLocation, checkRefs } from './refs.ts';
import { runnerList, type Proposal, type Review } from './review.ts';

/** Deletions tried per assessment; each runs the stage's test checks. */
const MAX_DELETIONS = 3;

type Rules = Pick<
  Profile,
  'classify' | 'stripTests' | 'disabledTestPattern' | 'testRunners' | 'isTestCommand'
>;

/** The finding a checked proposal becomes; `verdict` says what the plugin confirmed. */
export function toFinding(
  r: Review,
  p: Proposal,
  o: {
    stage: Stage;
    candidate: string;
    blocking: boolean;
    verdict: string;
    proof?: string;
    repro?: Repro;
    link?: string;
  },
): NewFinding {
  return {
    // Identical findings from several reviewers share a key, so they merge into one.
    key: `review:${hash(o.stage, o.candidate, p.location, p.scenario, p.correction)}`,
    source: 'review',
    stage: o.stage,
    revision: o.candidate,
    blocking: o.blocking,
    title: `${r.label} · ${p.location}`,
    body: [`Problem: ${p.scenario}`, `Fix: ${p.correction}`, `Evidence: ${o.verdict}`, o.proof]
      .filter(Boolean)
      .join('\n'),
    repro: o.repro,
    links: o.link ? [o.link] : [],
  };
}

/** What trying a reviewer's patch showed, and what the plugin ran, for the worker to reuse. */
interface Trial {
  confirmed: boolean;
  outcome: string;
  proof?: string;
}

/** Applies a patch, runs `checks` with it, and takes it out again; a string if it does not apply. */
async function withPatch(
  run: CheckEnv,
  patch: string,
  checks: Check[],
): Promise<CheckResult[] | string> {
  const text = patch.endsWith('\n') ? patch : `${patch}\n`;
  if (!(await run.vcs.applyPatch(text))) return 'its patch does not apply';
  try {
    return await runChecks(run, checks);
  } finally {
    if (!(await run.vcs.applyPatch(text, { reverse: true })))
      throw new Error('A reviewer patch could not be taken out again');
  }
}

/** Runs a reproduction's command, with its test patched in unless the candidate already has it. */
export async function runReproduction(run: CheckEnv, repro: Repro, kind: string) {
  const check = [{ kind, command: repro.command }];
  // Present only verbatim: a weakened test of the same name must not stand in for it.
  const added = parseDiff(repro.patch).find((f) => f.path === repro.path)?.added ?? [];
  const text = await readFile(join(run.root, repro.path), 'utf8').catch(() => undefined);
  const present = text !== undefined && added.length > 0 && added.every((l) => text.includes(l));
  return present
    ? (await runChecks(run, check))[0]
    : withPatch(run, repro.patch, check).then((r) => (typeof r === 'string' ? r : r[0]));
}

/**
 * Confirms a finding by its failing test: one test runner whose command passes without the test
 * and fails with it, naming it. Anything else may be the patch failing to compile or an unrelated
 * test.
 */
async function confirmReproduction(
  run: CheckEnv,
  rules: Rules,
  repro: Repro,
  n: number,
): Promise<Trial> {
  const files = parseDiff(repro.patch).map((f) => f.path);
  if (!files.includes(repro.path) || files.some((f) => rules.classify(f) !== 'test'))
    return {
      confirmed: false,
      outcome: `its patch must add ${repro.path} and touch only test sources`,
    };
  if (!rules.isTestCommand(repro.command))
    return { confirmed: false, outcome: `its command must be one of ${runnerList(rules)}` };
  const [before] = await runChecks(run, [{ kind: `repro-${n}-before`, command: repro.command }]);
  if (before.exitCode !== 0)
    return { confirmed: false, outcome: `its command fails without the test (${before.log})` };
  const after = await runReproduction(run, repro, `repro-${n}`);
  if (typeof after === 'string') return { confirmed: false, outcome: after };
  if (after.exitCode === null) return { confirmed: false, outcome: 'its command timed out' };
  if (after.exitCode === 0) return { confirmed: false, outcome: 'its test passes' };
  if (!(await readLog(after.log)).includes(repro.test))
    return {
      confirmed: false,
      outcome: `its command fails, but its log never names ${repro.test}`,
    };
  return {
    confirmed: true,
    outcome: `reproduced: \`${repro.command}\` exits ${after.exitCode} (${after.log})`,
    proof: `Reproduction (${repro.command}):\n${repro.patch}`,
  };
}

/**
 * Whether a deletion removes or disables tests, which proves nothing: a test file, an added line
 * that switches a test off, or a line of a source that appears only inside its tests. `read`
 * gives the candidate's text of a file.
 */
export async function deletesTests(
  rules: Pick<Rules, 'classify' | 'stripTests' | 'disabledTestPattern'>,
  patch: string,
  read: (path: string) => Promise<string>,
) {
  for (const f of parseDiff(patch)) {
    if (
      rules.classify(f.path) === 'test' ||
      f.added.some((line) => rules.disabledTestPattern.test(line))
    )
      return true;
    const text = await read(f.path).catch(() => '');
    const production = rules.stripTests?.(f.path, text);
    if (production === undefined) continue;
    if (f.removed.some((line) => line.trim() && text.includes(line) && !production.includes(line)))
      return true;
  }
  return false;
}

/** Confirms that code is not needed: every test check passes without it. */
async function confirmDeletion(
  run: CheckEnv,
  rules: Rules,
  patch: string,
  tests: Check[],
  n: number,
): Promise<Trial> {
  const net = parseDiff(patch).reduce((sum, f) => sum + f.removed.length - f.added.length, 0);
  const read = (path: string) => readFile(join(run.root, path), 'utf8');
  if (net <= 0 || (await deletesTests(rules, patch, read)))
    return { confirmed: false, outcome: 'its patch must remove more than it adds, outside tests' };
  const results = await withPatch(
    run,
    patch,
    tests.map((c) => ({ ...c, kind: `${c.kind}-deletion-${n}` })),
  );
  if (typeof results === 'string') return { confirmed: false, outcome: results };
  const failed = results.find((r) => r.problem);
  if (failed) return { confirmed: false, outcome: `\`${failed.command}\` fails without it` };
  return {
    confirmed: true,
    outcome: `every test check passes without it (−${net} lines)`,
    proof: `Deletion:\n${patch}`,
  };
}

/**
 * Turns proposals into findings. Those whose references do not hold are dropped; the rest block
 * only if their test fails, their deletion keeps the tests passing, or they cite a rule. Proposals
 * resolving to one line are one finding, and a confirmed one replaces a suggestion there.
 */
export async function confirmProposals(
  run: CheckEnv,
  rules: Rules,
  reviews: Review[],
  o: { stage: Stage; candidate: string; tests: Check[]; link: (thread: string) => string },
): Promise<NewFinding[]> {
  const findings: NewFinding[] = [];
  const kept = new Map<string, number>();
  let n = 0;
  let deletions = 0;
  for (const r of reviews)
    for (const p of r.proposals) {
      const problems = !(typeof p.location === 'string' && p.location && p.scenario)
        ? ['it has no location or scenario']
        : p.refs.length
          ? await checkRefs(run.root, p.refs)
          : ['it cites nothing'];
      if (problems.length) {
        run.debug(`${r.label}: dropped ${p.location}: ${problems.join('; ')}`);
        continue;
      }
      const id = await resolveLocation(run.root, p.location);
      const earlier = kept.get(id);
      if (earlier !== undefined && findings[earlier].blocking) continue;
      let result: Trial | undefined;
      if (p.repro) result = await confirmReproduction(run, rules, p.repro, ++n);
      else if (p.deletion && deletions++ < MAX_DELETIONS)
        result = await confirmDeletion(run, rules, p.deletion.patch, o.tests, ++n);
      const rule = p.refs.find((ref) => rules.classify(ref.path) === 'rule');
      const blocking = !!result?.confirmed || !!rule;
      const verdict = result?.confirmed
        ? result.outcome
        : rule
          ? `breaks the rule at ${rule.path}${rule.line ? `:${rule.line}` : ''}`
          : `unconfirmed${result ? `, ${result.outcome}` : ''}`;
      const finding = toFinding(r, p, {
        ...{ stage: o.stage, candidate: o.candidate, blocking, verdict },
        proof: result?.confirmed ? result.proof : undefined,
        repro: result?.confirmed ? p.repro : undefined,
        link: r.thread && o.link(r.thread),
      });
      if (earlier === undefined) kept.set(id, findings.push(finding) - 1);
      else if (blocking) findings[earlier] = finding;
    }
  return findings;
}
