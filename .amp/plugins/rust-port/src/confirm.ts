/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { git, runChecks, type Check, type CheckResult } from './checks.ts';
import { parseDiff } from './gates.ts';
import type { Stage } from './plan.ts';
import { isRule, isTest, RUNNERS } from './redisearch.ts';
import { anchor, defines, verify } from './refs.ts';
import type { Proposal, Review } from './review.ts';
import { nonTest } from './rules.ts';
import { hash, type NewFinding, type Repro } from './state.ts';

/** Confirming reviewer proposals by trying them: running their failing test, or their deletion. */

/** The runners a reproduction may use, as the reviewers read them. */
export const RUNS = RUNNERS.map((r) => `\`${r}\``).join(', ');

/** Deletions tried per assessment; each runs the stage's test checks. */
const MAX_DELETIONS = 3;

/**
 * Whether a reproduction command only runs tests: one of {@link RUNNERS}, whose arguments select
 * packages and tests, with no shell syntax and no option such as `--config` or `-Z` that makes
 * Cargo run something else. Reviewer output is model text that read untrusted code.
 */
export function runnable(command: string): boolean {
  const pattern = (r: string) =>
    new RegExp(`^${r.replace(/[.]/g, '\\.').replace('*', '\\w+')}(?=\\s|$)`);
  const runner = RUNNERS.map((r) => command.match(pattern(r))?.[0]).find(Boolean);
  if (!runner) return false;
  const args = command.slice(runner.length).trim().split(/\s+/).filter(Boolean);
  const name = /^[\w:./-]+$/;
  // A script takes its options as variables.
  if (!runner.startsWith('cargo ')) return args.every((a) => /^[A-Z_]+=[\w:./-]+$/.test(a));
  const valued = ['-p', '--package', '--test', '--features', '-F', '--manifest-path'];
  const flags = ['--lib', '--tests', '--exact', '--nocapture', '--no-fail-fast', '--release', '--'];
  for (let i = 0; i < args.length; i++)
    if (
      valued.includes(args[i])
        ? !name.test(args[++i] ?? '')
        : !flags.includes(args[i]) && (args[i].startsWith('-') || !name.test(args[i]))
    )
      return false;
  return true;
}

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

/** Where trials run: the checkout, a directory for their logs, and the claim's signal and environment. */
export interface Run {
  root: string;
  dir: string;
  signal: AbortSignal;
  vars: Record<string, string>;
  debug: (message: string) => void;
}

/** What trying a reviewer's patch showed, and what the plugin ran, for the worker to reuse. */
interface Trial {
  confirmed: boolean;
  outcome: string;
  proof?: string;
}

/** Applies a patch, runs `checks` with it, and takes it out again; a string if it does not apply. */
async function trial(run: Run, patch: string, checks: Check[]): Promise<CheckResult[] | string> {
  const text = patch.endsWith('\n') ? patch : `${patch}\n`;
  // Hunk line counts written by a model are often off; --recount ignores them.
  const applied = await git(run.root, ['apply', '--recount'], text).then(
    () => true,
    () => false,
  );
  if (!applied) return 'its patch does not apply';
  try {
    return await runChecks(run.root, run.dir, checks, run.signal, run.debug, run.vars);
  } finally {
    await git(run.root, ['apply', '--recount', '-R'], text);
  }
}

/** Runs a reproduction's command, with its test patched in unless the candidate already has it. */
export async function reproduction(run: Run, repro: Repro, kind: string) {
  const check = [{ kind, command: repro.command }];
  const present = await readFile(join(run.root, repro.path), 'utf8').then(
    (text) => defines(text, repro.test),
    () => false,
  );
  return present
    ? (await runChecks(run.root, run.dir, check, run.signal, run.debug, run.vars))[0]
    : trial(run, repro.patch, check).then((r) => (typeof r === 'string' ? r : r[0]));
}

/**
 * Confirms a finding by its failing test: one test runner (see {@link runnable}) whose command
 * passes without the test and fails with it, naming it. Anything else may be the patch failing to
 * compile or an unrelated test.
 */
async function reproduce(run: Run, repro: Repro, n: number): Promise<Trial> {
  const files = parseDiff(repro.patch).map((f) => f.path);
  if (
    !files.includes(repro.path) ||
    files.some((f) => !isTest(f) || /(^|\/)(Cargo\.toml|build\.rs)$/.test(f))
  )
    return {
      confirmed: false,
      outcome: `its patch must add ${repro.path} and touch only test sources`,
    };
  if (!runnable(repro.command))
    return { confirmed: false, outcome: `its command must be one of ${RUNS}` };
  const [before] = await runChecks(
    run.root,
    run.dir,
    [{ kind: `repro-${n}-before`, command: repro.command }],
    run.signal,
    run.debug,
    run.vars,
  );
  if (before.exitCode !== 0)
    return { confirmed: false, outcome: `its command fails without the test (${before.log})` };
  const after = await reproduction(run, repro, `repro-${n}`);
  if (typeof after === 'string') return { confirmed: false, outcome: after };
  if (after.exitCode === null) return { confirmed: false, outcome: 'its command timed out' };
  if (after.exitCode === 0) return { confirmed: false, outcome: 'its test passes' };
  if (!(await readFile(after.log, 'utf8').catch(() => '')).includes(repro.test))
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
 * Whether a deletion removes tests, which proves nothing: a test file, or a line of a Rust source
 * that appears only inside its `#[cfg(test)]` items. `read` gives the candidate's text of a file.
 */
export async function deletesTests(patch: string, read: (path: string) => Promise<string>) {
  for (const f of parseDiff(patch)) {
    if (isTest(f.path)) return true;
    if (!f.path.endsWith('.rs')) continue;
    const text = await read(f.path).catch(() => '');
    const production = nonTest(text);
    if (f.removed.some((line) => line.trim() && text.includes(line) && !production.includes(line)))
      return true;
  }
  return false;
}

/** Confirms that code is not needed: every test check passes without it. */
async function remove(run: Run, patch: string, tests: Check[], n: number): Promise<Trial> {
  const net = parseDiff(patch).reduce((sum, f) => sum + f.removed.length - f.added.length, 0);
  if (net <= 0 || (await deletesTests(patch, (path) => readFile(join(run.root, path), 'utf8'))))
    return { confirmed: false, outcome: 'its patch must remove more than it adds, outside tests' };
  const results = await trial(
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
export async function confirm(
  run: Run,
  reviews: Review[],
  o: { stage: Stage; candidate: string; tests: Check[]; threadURL: (thread: string) => string },
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
          ? await verify(run.root, p.refs)
          : ['it cites nothing'];
      if (problems.length) {
        run.debug(`${r.label}: dropped ${p.location}: ${problems.join('; ')}`);
        continue;
      }
      const id = await anchor(run.root, p.location);
      const earlier = kept.get(id);
      if (earlier !== undefined && findings[earlier].blocking) continue;
      let result: Trial | undefined;
      if (p.repro) result = await reproduce(run, p.repro, ++n);
      else if (p.deletion && deletions++ < MAX_DELETIONS)
        result = await remove(run, p.deletion.patch, o.tests, ++n);
      const rule = p.refs.find((ref) => isRule(ref.path));
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
        link: r.thread && o.threadURL(r.thread),
      });
      if (earlier === undefined) kept.set(id, findings.push(finding) - 1);
      else if (blocking) findings[earlier] = finding;
    }
  return findings;
}
