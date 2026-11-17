/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * What a change is: the files of a diff, the lines each adds and removes, and the measures the
 * gates take of them.
 */
import { hash } from './job.ts';
import type { Profile } from './ports.ts';

const GIT_FILE = 'diff --git ';

export interface FileChange {
  path: string;
  added: string[];
  removed: string[];
  /** The file before and after, where the caller read them. */
  texts?: [before: string | undefined, after: string | undefined];
}

/**
 * The files a unified diff changes, with the lines it adds and removes in each. A file starts at
 * its `---`/`+++` pair; hunk line counts are ignored, because a model's are often wrong. Where the
 * diff has `diff --git` lines, a file starts at its own, so one without hunks, binary or empty,
 * still counts, and a `---`/`+++` pair is a header only before the file's first hunk, so hunk
 * content that looks like one is not one.
 */
export function parseDiff(diff: string): FileChange[] {
  const files: FileChange[] = [];
  const lines = diff.split('\n');
  const fromGit = lines.some((line) => line.startsWith(GIT_FILE));
  const clean = (path: string) => path.replace(/^[ab]\//, '');
  let file: FileChange | undefined;
  let inHeader = false;
  const start = (path: string) => {
    file = { path: clean(path), added: [], removed: [] };
    files.push(file);
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (fromGit && line.startsWith(GIT_FILE)) {
      // Both sides name the same path, `a/<path> b/<path>`, unless the file was renamed, when a
      // `rename to` or `+++` line below names it.
      const sides = line.slice(GIT_FILE.length);
      start(sides.slice(0, (sides.length - 1) / 2));
      inHeader = true;
    } else if (fromGit && inHeader && line.startsWith('rename to ')) {
      file!.path = line.slice('rename to '.length);
    } else if (
      (!fromGit || inHeader) &&
      line.startsWith('--- ') &&
      lines[i + 1]?.startsWith('+++ ')
    ) {
      const [from, to] = [line, lines[++i]].map((l) => l.slice(4).split('\t')[0].trim());
      const path = to === '/dev/null' ? from : to;
      if (fromGit) file!.path = clean(path);
      else start(path);
    } else if (line.startsWith('@@')) inHeader = false;
    else if (file && !inHeader && line[0] === '+') file.added.push(line.slice(1));
    else if (file && !inHeader && line[0] === '-') file.removed.push(line.slice(1));
  }
  return files;
}

/** Equal for the same change rebased: line numbers and blob IDs aside, whitespace kept. */
export const patchId = (diff: string) =>
  hash(diff.split('\n').filter((line) => !/^(index |@@)/.test(line)));

/** Lines added and removed per file, as reviewers skim a change. */
export const diffStat = (files: FileChange[]) =>
  files.map((f) => `${f.path} | +${f.added.length} -${f.removed.length}`).join('\n');

export interface Size {
  /** Lines added, Markdown and `.gitignore` aside as the PR size label counts. */
  lines: number;
  /**
   * Lines of code added minus deleted, tests and generated files aside: the growth floor. In a
   * file whose texts are known and whose language holds tests in sources, only production lines
   * count.
   */
  code: number;
  files: number;
}

export function measureSize(
  files: FileChange[],
  profile: Pick<Profile, 'classify' | 'stripTests'>,
): Size {
  const size = { lines: 0, code: 0, files: 0 };
  const lines = (text: string) => text.split('\n').filter((line) => line.trim()).length;
  for (const { path, added, removed, texts } of files) {
    if (path.endsWith('.md') || path.endsWith('.gitignore')) continue;
    size.lines += added.length;
    size.files++;
    const kind = profile.classify(path);
    if (kind === 'test' || kind === 'generated') continue;
    const [before, after] = (texts ?? []).map((t) => profile.stripTests?.(path, t ?? ''));
    size.code +=
      texts && before !== undefined && after !== undefined
        ? lines(after) - lines(before)
        : added.length - removed.length;
  }
  return size;
}

/**
 * Pushback by lines added, which grows with them; nothing up to 250. The classes end where the PR
 * size label's do, and XL blocks. Deletions do not count: the classes counter bloat.
 */
export function sizeClass(lines: number): { label: string; note: string } | undefined {
  if (lines <= 250) return;
  if (lines <= 500)
    return {
      label: 'M',
      note: 'Every line costs a reviewer attention; before claiming again, delete what the port does not need.',
    };
  if (lines <= 1000)
    return {
      label: 'L',
      note: 'Reviewers will push back on a stage this big. Stop adding: cut first, and question every file the stage touches.',
    };
  return {
    label: 'XL',
    note: 'Too large to review: the stage cannot be admitted until it adds at most 1000 lines.',
  };
}

/** Files changed only in blank lines; other whitespace may matter, in a string or in Python. */
export function blankOnlyFiles(files: FileChange[]): string[] {
  return files
    .filter((f) => {
      const lines = [...f.added, ...f.removed];
      return lines.length && lines.every((line) => !line.trim());
    })
    .map((f) => f.path);
}

/** The files that lie outside every one of `paths`, each a file or a directory. */
export function filesOutside(files: string[], paths: string[]): string[] {
  const dirs = paths.map((p) => p.replace(/\/*$/, '/'));
  return files.filter((f) => !paths.includes(f) && !dirs.some((d) => f.startsWith(d)));
}
