/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * What checks in a Cargo workspace need: judges of Miri, cargo-mutants and Criterion, and the
 * checks of commands and paths a plan names.
 */
import { execa } from 'execa';
import { readdir, readFile, stat } from 'node:fs/promises';
import { join } from 'node:path';
import type { Judge } from '../core/ports.ts';

const ESTIMATES = /^(.+)[\\/](?:c|rust)[\\/]new[\\/]estimates\.json$/;

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
 * Where Criterion may write: `criterion` in the Cargo target directory of the workspace at
 * `workspace` under `root`. Cargo run from the repository root and from the workspace disagree on
 * it, so both are asked, one after the other because Cargo commands contend for the workspace lock.
 */
export async function criterionDirs(root: string, workspace: string): Promise<string[]> {
  const manifest = join(root, workspace, 'Cargo.toml');
  const args = ['metadata', '--format-version', '1', '--no-deps', '--manifest-path', manifest];
  const targets = [];
  for (const cwd of [root, join(root, workspace)]) {
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
 * Judges each Criterion group measured since `since` by the ratio of its `rust` and `c` medians,
 * which may exceed 1 by at most `slowdownPercent`. A group with neither side fresh is an earlier
 * run's, left in the shared target directory.
 */
export async function judgeBenchmarks(
  dirs: string[],
  since: number,
  slowdownPercent: number,
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

/**
 * The judges of Miri, mutation and benchmark checks in the workspace at `workspace`, where Rust may
 * be at most `slowdownPercent` slower than C.
 */
export const cargoJudges = (workspace: string, slowdownPercent: number): Record<string, Judge> => ({
  miri: ({ log }) => judgeMiri(log),
  mutants: ({ log }) => judgeMutants(log),
  bench: async ({ check, root }) =>
    judgeBenchmarks(await criterionDirs(root, workspace), check.started, slowdownPercent),
});

/** What is missing to run and judge the checks: Cargo subcommands, and Cargo's metadata. */
export async function missingCargoTools(root: string, workspace: string): Promise<string[]> {
  const problems: string[] = [];
  for (const tool of ['mutants', 'nextest'])
    await execa('cargo', [tool, '--version'], { cwd: root }).catch(() =>
      problems.push(`cargo ${tool} is missing: cargo install --locked cargo-${tool}`),
    );
  await criterionDirs(root, workspace).catch((error) =>
    problems.push(`cargo metadata fails, so benchmarks cannot be read: ${error.shortMessage}`),
  );
  return problems;
}

/**
 * What in a `cargo bench` command selects only some benchmarks: a bench target, or a filter
 * passed to Criterion. Benchmarks a run skips leave stale results the judge ignores.
 */
export function benchFilter(command: string): string | undefined {
  const args =
    command
      .match(/cargo bench\b([^;&|]*)/)?.[1]
      .trim()
      .split(/\s+/) ?? [];
  const valued = ['-p', '--package', '--manifest-path', '--features', '-F', '--profile'];
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--' || args[i] === '--bench' || args[i].startsWith('--bench=')) return args[i];
    if (valued.includes(args[i]) || args[i] === '--target-dir') i++;
    else if (args[i] && !args[i].startsWith('-')) return args[i];
  }
}

/** Paths holding several crates: a stage names each crate it changes, so spreading shows. */
export function multiCratePaths(paths: string[], files: string[]): string[] {
  return paths.flatMap((path) => {
    const dir = path.replace(/\/*$/, '/');
    const count = files.filter((f) => f.startsWith(dir) && f.endsWith('/Cargo.toml')).length;
    return count > 1 ? [`${path} holds ${count} crates; list each one the stage changes`] : [];
  });
}

/**
 * Whether a reproduction command only runs tests: one of `runners`, a `*` standing for a word,
 * whose arguments select packages and tests, with no shell syntax and no option such as
 * `--config` or `-Z` that makes Cargo run something else. Reviewer output is model text that read
 * untrusted code.
 */
export function isTestCommand(runners: string[], command: string): boolean {
  const pattern = (r: string) =>
    new RegExp(`^${r.replace(/[.]/g, '\\.').replace('*', '\\w+')}(?=\\s|$)`);
  const runner = runners.map((r) => command.match(pattern(r))?.[0]).find(Boolean);
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
