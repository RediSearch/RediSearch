/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import { execa } from 'execa';
import { readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { stripVTControlCharacters } from 'node:util';
import { WORKSPACE } from './redisearch.ts';

const env = { GIT_EDITOR: 'true', GIT_TERMINAL_PROMPT: '0', NO_COLOR: '1' };

/** How much slower than C, in percent, Rust may be in any benchmark. */
export const SLOWDOWN_PERCENT = 5;

const ESTIMATES = /^(.+)[\\/](?:c|rust)[\\/]new[\\/]estimates\.json$/;

export interface Check {
  kind: string;
  command: string;
}

export interface CheckResult extends Check {
  exitCode: number | null;
  /** Milliseconds since the epoch. */
  started: number;
  ms: number;
  log: string;
  /** Why the check failed: its exit code, or what its log shows. */
  problem?: string;
}

export async function git(cwd: string, args: string[], input = ''): Promise<string> {
  return (await execa('git', args, { cwd, env, input })).stdout;
}

/**
 * Runs checks one at a time, because builds share the cargo lock, and stops at the first failure,
 * which already rejects the candidate. Each result carries its `problem`, if it has one.
 */
export async function runChecks(
  cwd: string,
  dir: string,
  checks: Check[],
  signal: AbortSignal,
  debug: (message: string) => void,
  vars: Record<string, string> = {},
): Promise<CheckResult[]> {
  const results: CheckResult[] = [];
  for (const [index, check] of checks.entries()) {
    const log = join(dir, `check-${index}-${check.kind}.log`);
    const started = Date.now();
    const exitCode = signal.aborted ? null : await run(cwd, check.command, log, signal, vars);
    const result: CheckResult = { ...check, exitCode, started, ms: Date.now() - started, log };
    result.problem =
      exitCode === null
        ? 'timed out'
        : exitCode !== 0
          ? `exited ${exitCode}`
          : await judge(result, cwd);
    debug(`check \`${check.command}\` ${result.problem ?? 'passed'} in ${result.ms}ms: ${log}`);
    results.push(result);
    if (result.problem) break;
  }
  return results;
}

async function run(
  cwd: string,
  command: string,
  log: string,
  signal: AbortSignal,
  vars: Record<string, string>,
) {
  // Both streams append to one log, so their output interleaves as it happened.
  await writeFile(log, '');
  // A process group of its own, so aborting also kills the redis-servers and compilers it starts.
  const child = execa('bash', ['-o', 'pipefail', '-c', command], {
    cwd,
    env: { ...env, ...vars },
    detached: true,
    reject: false,
    stdin: 'ignore',
    stdout: { file: log, append: true },
    stderr: { file: log, append: true },
  });
  const kill = () => {
    try {
      process.kill(-child.pid!, 'SIGKILL');
    } catch {}
  };
  signal.addEventListener('abort', kill, { once: true });
  // An abort before the listener existed would otherwise leave the check running.
  if (signal.aborted) kill();
  const { exitCode } = await child;
  signal.removeEventListener('abort', kill);
  return signal.aborted ? null : (exitCode ?? null);
}

/**
 * Whether checks' logs show the test `name` ran: a line naming it, module path included, that
 * does not report it ignored or skipped. Another check may still skip it, say under Miri.
 */
export function ran(logs: string, name: string): boolean {
  const named = new RegExp(`(?<!\\w)${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?!\\w)`);
  return logs
    .split('\n')
    .some((line) => named.test(line) && !/\b(ignored|skip(ped)?)\b/i.test(line));
}

/** Why a check that exited 0 still failed, judged from its log or its output files. */
async function judge(check: CheckResult, root: string): Promise<string | undefined> {
  if (check.kind === 'bench') return judgeBenchmarks(await criterion(root), check.started);
  const log = stripVTControlCharacters(await readFile(check.log, 'utf8').catch(() => ''));
  if (check.kind === 'miri') return judgeMiri(log);
  if (check.kind === 'mutants') return judgeMutants(log);
}

/**
 * Fails a Miri run that ran nothing, such as `true # miri`, or had a test nextest marked slow:
 * CI's runners are slower and time it out.
 */
export function judgeMiri(log: string): string | undefined {
  if (!Number(log.match(/Summary \[[^\]]*\] (\d+) tests? run/)?.[1]))
    return 'ran no tests under Miri: nextest reported no summary of tests run';
  const slow = new Set(
    [...log.matchAll(/ SLOW \[[^\]]*\] \([^)]*\) (.+)$/gm)].map((m) => m[1].trim()),
  );
  if (slow.size)
    return (
      `has tests nextest marked slow, which CI's slower runners time out: ${[...slow].join(', ')}. ` +
      'Shrink their inputs; never raise the timeout.'
    );
}

/** Fails a mutation run that found nothing to mutate, because it then covers nothing. */
export function judgeMutants(log: string): string | undefined {
  if (!/Found [1-9]\d* mutants? to test/.test(log))
    return (
      'found no mutants in the stage diff, so it shows no test coverage. Point `-p` and ' +
      '`--in-diff` at the Rust the stage adds; a swap that changes only `unsafe extern` exports, ' +
      'which cargo mutants skips, needs no mutants check.'
    );
}

/**
 * Where Criterion may write: `criterion` in the Cargo target directory. Cargo run from the
 * repository root and from the workspace disagree on it, so both are asked, one after the other
 * because Cargo commands contend for the workspace lock.
 */
export async function criterion(root: string): Promise<string[]> {
  const manifest = join(root, WORKSPACE, 'Cargo.toml');
  const args = ['metadata', '--format-version', '1', '--no-deps', '--manifest-path', manifest];
  const targets = [];
  for (const cwd of [root, join(root, WORKSPACE)]) {
    const { stdout } = await execa('cargo', args, { cwd });
    targets.push(join(JSON.parse(stdout).target_directory, 'criterion'));
  }
  return [...new Set(targets)];
}

/** The median Criterion estimated in `file`, unless it was written before `since`. */
async function median(file: string, since: number): Promise<number | undefined> {
  const written = await stat(file).catch(() => undefined);
  if (!written || written.mtimeMs < since) return;
  const estimate = await readFile(file, 'utf8')
    .then((text) => JSON.parse(text).median.point_estimate)
    .catch(() => NaN);
  return estimate > 0 ? estimate : NaN;
}

/**
 * Judges each Criterion group measured since `since` by the ratio of its `rust` and `c` medians.
 * A group with neither side fresh is an earlier run's, left in the shared target directory.
 */
export async function judgeBenchmarks(
  dirs: string[],
  since: number,
  slowdownPercent = SLOWDOWN_PERCENT,
): Promise<string | undefined> {
  const ratios: { name: string; ratio: number }[] = [];
  const halves: string[] = [];
  for (const dir of dirs) {
    const files = await readdir(dir, { recursive: true }).catch(() => [] as string[]);
    const names = new Set(files.map((f) => f.match(ESTIMATES)?.[1]).filter((n) => n !== undefined));
    for (const name of names) {
      const [c, rust] = await Promise.all(
        ['c', 'rust'].map((impl) => median(join(dir, name, impl, 'new', 'estimates.json'), since)),
      );
      if (c !== undefined && rust !== undefined) ratios.push({ name, ratio: rust / c });
      else if (c !== undefined || rust !== undefined) halves.push(name);
    }
  }
  if (halves.length)
    return `measured only one of \`c\` and \`rust\` for: ${halves.sort().join(', ')}`;
  if (!ratios.length)
    return `measured no Criterion group with \`c\` and \`rust\` functions in ${dirs.join(' or ')}`;
  const unreadable = ratios.filter((r) => Number.isNaN(r.ratio)).map((r) => r.name);
  if (unreadable.length) return `left unreadable Criterion estimates for: ${unreadable.join(', ')}`;
  const limit = 1 + slowdownPercent / 100;
  const slow = ratios.filter((r) => r.ratio > limit).map((r) => `${r.name} ${r.ratio.toFixed(2)}x`);
  if (slow.length) return `is slower than C beyond ${limit.toFixed(2)}x: ${slow.join(', ')}`;
}
