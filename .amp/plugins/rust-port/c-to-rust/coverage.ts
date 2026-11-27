/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** Line coverage of the C being ported, read from the lcov tracefile a test run wrote. */
import { readFile, realpath, stat } from 'node:fs/promises';
import { relative, resolve } from 'node:path';
import type { CheckResult } from '../core/checks.ts';

/** Ranges listed per file before the report elides the rest; the tracefile holds them all. */
const MAX_RANGES = 30;

/** Hits per instrumented line. */
export type LineHits = Map<number, number>;

/**
 * Each source file of an lcov tracefile, as it names it, with its hits per line. A file in several
 * records, one per test, adds up its hits.
 */
export function parseLcov(text: string): Map<string, LineHits> {
  const files = new Map<string, LineHits>();
  let current: LineHits | undefined;
  for (const line of text.split('\n')) {
    if (line.startsWith('SF:')) {
      const path = line.slice(3).trim();
      current = files.get(path) ?? new Map();
      files.set(path, current);
    } else if (line.startsWith('DA:') && current) {
      const [number, hits] = line.slice(3).split(',').map(Number);
      current.set(number, (current.get(number) ?? 0) + hits);
    } else if (line.startsWith('end_of_record')) current = undefined;
  }
  return files;
}

/** `[1, 2, 3, 7]` as `1-3, 7`. */
export function lineRanges(lines: number[]): string[] {
  const ranges: string[] = [];
  for (let i = 0; i < lines.length; ) {
    let j = i;
    while (j + 1 < lines.length && lines[j + 1] === lines[j] + 1) j++;
    ranges.push(i === j ? `${lines[i]}` : `${lines[i]}-${lines[j]}`);
    i = j + 1;
  }
  return ranges;
}

/**
 * Judges a test run that wrote the lcov tracefile `info`: it must have written it since `since`,
 * and covered every one of `sources`. Reports each source's coverage and the lines left uncovered,
 * which the port then reads as the behavior no test pins.
 */
export async function judgeCoverage(
  info: string,
  sources: string[],
  since: number,
  root: string,
): Promise<Pick<CheckResult, 'problem' | 'report'>> {
  const written = await stat(info).catch(() => undefined);
  if (!written || written.mtimeMs < since)
    return { problem: `wrote no fresh coverage to ${relative(root, info)}` };
  // Tracefiles name sources by absolute path, which may run through a symlink the root does not.
  const top = await realpath(root);
  const files = new Map<string, LineHits>();
  for (const [path, hits] of parseLcov(await readFile(info, 'utf8'))) {
    const real = await realpath(resolve(root, path)).catch(() => resolve(root, path));
    const key = relative(top, real);
    const merged = files.get(key) ?? new Map<number, number>();
    files.set(key, merged);
    for (const [line, n] of hits) merged.set(line, (merged.get(line) ?? 0) + n);
  }
  const missing = sources.filter((s) => !files.get(s)?.size);
  if (missing.length)
    return { problem: `has no coverage of ${missing.join(', ')}: is it compiled into the module?` };
  const rows = sources.map((s) => {
    const lines = [...files.get(s)!].sort(([a], [b]) => a - b);
    const total = lines.length;
    const uncovered = lines.filter(([, n]) => n === 0).map(([line]) => line);
    const covered = total - uncovered.length;
    const ranges = lineRanges(uncovered);
    const listed = ranges.slice(0, MAX_RANGES).join(', ');
    const more = ranges.length > MAX_RANGES ? ` and ${ranges.length - MAX_RANGES} more` : '';
    return `| \`${s}\` | ${covered}/${total} | ${((100 * covered) / total).toFixed(1)}% | ${listed || 'none'}${more} |`;
  });
  return {
    report: `### Flow test coverage

Lines of the C being ported that the Python flow tests reach, from this candidate's coverage run.

| File | Lines covered | Coverage | Uncovered lines |
| --- | ---: | ---: | --- |
${rows.join('\n')}`,
  };
}
