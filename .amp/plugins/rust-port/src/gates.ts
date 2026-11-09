/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import { cited, crates, type Plan, type Stage } from './plan.ts';
import { verify } from './refs.ts';
import { HEADERS, isGenerated, isPure, isTest, RULES, WORKSPACE } from './redisearch.ts';
import { bodies, nonTest, violations } from './rules.ts';
import { reviewed, type State } from './state.ts';

type Git = (args: string[]) => Promise<string>;

export interface FileChange {
  path: string;
  added: string[];
  removed: string[];
  /** The file before and after, where the caller read them. */
  texts?: [before: string | undefined, after: string | undefined];
}

/**
 * The files a unified diff changes, with the lines it adds and removes in each. A file starts at
 * its `---`/`+++` pair; hunk line counts are ignored, because a model's are often wrong. In git's
 * own output the pair follows an `index` line, so hunk content that looks like one is not one.
 */
export function parseDiff(diff: string): FileChange[] {
  const files: FileChange[] = [];
  const lines = diff.split('\n');
  const fromGit = lines.some((line) => line.startsWith('diff --git '));
  let file: FileChange | undefined;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const header = !fromGit || lines[i - 1]?.startsWith('index ');
    if (header && line.startsWith('--- ') && lines[i + 1]?.startsWith('+++ ')) {
      const [from, to] = [line, lines[++i]].map((l) => l.slice(4).split('\t')[0].trim());
      const path = to === '/dev/null' ? from : to;
      file = { path: path.replace(/^[ab]\//, ''), added: [], removed: [] };
      files.push(file);
    } else if (file && line[0] === '+') file.added.push(line.slice(1));
    else if (file && line[0] === '-') file.removed.push(line.slice(1));
  }
  return files;
}

export interface Size {
  /** Lines added, Markdown and `.gitignore` aside as the PR size label counts. */
  lines: number;
  /**
   * Lines of code added minus deleted, tests and generated files aside: the growth floor. In a
   * Rust file whose texts are known, only lines outside `#[cfg(test)]` items count.
   */
  code: number;
  files: number;
}

export function measure(files: FileChange[]): Size {
  const size = { lines: 0, code: 0, files: 0 };
  const code = (text = '') =>
    nonTest(text)
      .split('\n')
      .filter((line) => line.trim()).length;
  for (const { path, added, removed, texts } of files) {
    if (path.endsWith('.md') || path.endsWith('.gitignore')) continue;
    size.lines += added.length;
    size.files++;
    if (isTest(path) || isGenerated(path)) continue;
    size.code +=
      texts && path.endsWith('.rs')
        ? code(texts[1]) - code(texts[0])
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
export function cosmetic(files: FileChange[]): string[] {
  return files
    .filter((f) => {
      const lines = [...f.added, ...f.removed];
      return lines.length && lines.every((line) => !line.trim());
    })
    .map((f) => f.path);
}

/** The files that lie outside every one of `paths`, each a file or a directory. */
export function outside(files: string[], paths: string[]): string[] {
  const dirs = paths.map((p) => p.replace(/\/*$/, '/'));
  return files.filter((f) => !paths.includes(f) && !dirs.some((d) => f.startsWith(d)));
}

/**
 * Why the candidate cannot be the stage, and its size: everything that rejects a claim before
 * anything runs, so a rejection costs no assessment.
 */
export async function gates(
  git: Git,
  root: string,
  state: State,
  stage: Stage,
  base: string,
  candidate: string,
): Promise<{ problems: string[]; size: Size }> {
  const { plan } = state.job;
  // Without rename detection a moved file names both its paths, so neither escapes the scope.
  const diff = (...args: string[]) => git(['diff', '--no-renames', ...args, base, candidate]);
  const { paths } = plan.stages[stage];
  const stray = outside(
    (await diff('--name-only')).split('\n').filter((f) => f && !isGenerated(f)),
    paths,
  );
  if (stray.length)
    return {
      problems: [`it changes ${stray.join(', ')}, outside ${paths.join(', ')}; revert or amend`],
      size: { lines: 0, code: 0, files: 0 },
    };
  const show = (rev: string, path: string) =>
    git(['show', `${rev}:${path}`]).catch(() => undefined);
  const files = parseDiff(await diff('--no-ext-diff'));
  for (const f of files) f.texts = [await show(base, f.path), await show(candidate, f.path)];
  const size = measure(files);
  const problems = cosmetic(files.filter((f) => !isGenerated(f.path))).map(
    (path) => `${path} changes only blank lines; restore it`,
  );
  const tooBig = sizeClass(size.lines);
  if (tooBig?.label === 'XL') problems.push(`it adds ${size.lines} lines: ${tooBig.note}`);
  // Once reviewed, a stage's code may not grow: a fix pays for itself in deleted code.
  const floor = Math.min(...reviewed(state, stage).map((e) => e.size.code));
  if (size.code > floor)
    problems.push(
      `it adds ${size.code} net lines of code (tests aside), its reviewed version ${floor}: ` +
        'a fix must delete as much code as it adds',
    );
  for (const { path, texts } of files) {
    const [before, after] = texts!;
    if (after !== undefined) problems.push(...violations(RULES, path, before, after));
  }
  problems.push(
    ...crates(paths, (await git(['ls-tree', '-r', '--name-only', candidate])).split('\n')),
  );
  const tests = cited(plan, stage).map((t) => ({ path: t.path, test: t.name }));
  problems.push(...(await verify(root, tests)), ...(await unfixed(git, state, stage, candidate)));
  if (stage === 'swap') problems.push(...(await swapped(git, plan, candidate, files)));
  return { problems, size };
}

/**
 * The stage's findings closed as fixed whose fix the candidate lacks: it changes none of the files
 * the fix cites since the revision the finding was raised on.
 */
async function unfixed(git: Git, state: State, stage: Stage, candidate: string) {
  const problems = [];
  for (const f of Object.values(state.findings)) {
    if (f.stage !== stage || f.closed?.resolution !== 'fixed') continue;
    const paths = (f.closed.evidence ?? []).map((r) => r.path);
    // A revision this checkout never had cannot be compared, and is not held against it.
    const changed = await git(['diff', '--name-only', f.revision, candidate, '--', ...paths]).catch(
      () => '?',
    );
    if (!changed)
      problems.push(
        `${f.id} was closed as fixed, but nothing it cites changed since ${f.revision.slice(0, 10)}`,
      );
  }
  return problems;
}

/**
 * What the swap leaves undone: a Rust replacement or generated export missing, a module file
 * left, an export no production code calls, or Rust outside the FFI crate without a mutants check.
 * A C definition left elsewhere fails to link against the Rust export of its name.
 */
async function swapped(git: Git, plan: Plan, candidate: string, files: FileChange[]) {
  const problems: string[] = [];
  const found = (args: string[]) =>
    git(args).then(
      () => true,
      () => false,
    );
  const grep = (flags: string, pattern: string, ...paths: string[]) =>
    found(['grep', flags, pattern, candidate, '--', ...paths]);
  // Exports are not mutation candidates, but other Rust the swap adds is.
  const pure = files.map((f) => f.path).filter((f) => isPure(f) && !isTest(f));
  if (pure.length && !plan.stages.swap.checks.some((c) => c.kind === 'mutants'))
    problems.push(
      `swap changes Rust outside its FFI crate (${pure.join(', ')}): add a mutants check`,
    );
  for (const s of plan.symbols) {
    if (!(await definesRust(git, candidate, s.rust)))
      problems.push(`${s.rust}, which replaces ${s.c}, is not defined`);
    if (!(await grep('-qw', s.c, HEADERS))) problems.push(`no generated header exports ${s.c}`);
  }
  for (const path of plan.module)
    if (await found(['cat-file', '-e', `${candidate}:${path}`]))
      problems.push(`${path} still exists; the swap deletes the module, headers included`);
  for (const name of await orphans(git, files, candidate))
    problems.push(
      `${name} is exported, but no production code calls it: delete it, and point any test ` +
        'that calls it at the Rust API',
    );
  return problems;
}

/**
 * Whether the crate a Rust path starts with defines each item after it, so `geo::RadiusFilter::new`
 * needs `fn new` in a file of the `geo` crate that implements `RadiusFilter`, not any `fn new`.
 */
async function definesRust(git: Git, candidate: string, path: string): Promise<boolean> {
  const [crate, ...items] = path.split('::');
  const manifests = await git([
    'grep',
    '-l',
    `^name = "${crate}"`,
    candidate,
    '--',
    `${WORKSPACE}**Cargo.toml`,
  ]).catch(() => '');
  const manifest = manifests.split('\n')[0]?.split(':').at(-1);
  if (!manifest || !items.length) return false;
  const dir = manifest.replace(/Cargo\.toml$/, '');
  const files = async (pattern: string) =>
    (await git(['grep', '-lwE', pattern, candidate, '--', dir]).catch(() => ''))
      .split('\n')
      .filter(Boolean);
  for (const item of items.slice(0, -1))
    if (!(await files(`(struct|enum|trait|type|mod) ${item}`)).length) return false;
  const [owner, item] = [items.at(-2), items.at(-1)!];
  const defs = await files(`(fn|struct|enum|trait|type|const|static|mod) ${item}`);
  if (!owner) return defs.length > 0;
  // A method lives in an impl block of its type; a function in its module's file or block.
  const header = new RegExp(
    `^\\s*(impl\\b[^{;]*\\b${owner}\\b[^{;]*|(pub(\\([^)]*\\))?\\s+)?mod\\s+${owner}\\s*)\\{`,
    'gm',
  );
  const defined = new RegExp(`\\b(fn|const|type|struct|enum)\\s+${item}\\b`);
  for (const hit of defs) {
    if (hit.endsWith(`/${owner}.rs`) || hit.endsWith(`/${owner}/mod.rs`)) return true;
    if (bodies(await git(['show', hit]), header).some((b) => defined.test(b))) return true;
  }
  return false;
}

/**
 * C ABI exports without a production caller: those the stage adds, and those whose caller it
 * deleted, that no C or Rust code outside tests, generated headers and comments mentions. An
 * export only tests call keeps C ABI alive for nothing; its tests belong on the Rust API.
 */
export async function orphans(git: Git, files: FileChange[], candidate: string): Promise<string[]> {
  const grep = (...args: string[]) => git(['grep', ...args]).catch(() => '');
  const touched = new Set(files.flatMap((f) => f.removed.join('\n').match(/\w+/g) ?? []));
  for (const f of files)
    for (const m of f.added.join('\n').matchAll(/#\[unsafe\(no_mangle\)\][^{;]*?\bfn\s+(\w+)/g))
      touched.add(m[1]);
  const exports = await grep('-h', '-A4', '#\\[unsafe(no_mangle)\\]', candidate, '--', '*.rs');
  const names = new Set(
    [...exports.matchAll(/\bfn\s+(\w+)/g)].map((m) => m[1]).filter((n) => touched.has(n)),
  );
  const sources = ['*.c', '*.h', '*.cpp', '*.cc', '*.hpp', '*.rs', `:!${HEADERS}`];
  const skip = [':!tests', ':!**/tests/**', ':!**/benches/**'];
  const result = [];
  for (const name of names) {
    const word = new RegExp(`\\b${name}\\b`);
    const call = (line: string) =>
      word.test(line) && !/^(\/\/|\/?\*)/.test(line.trim()) && !/\bfn\s/.test(line);
    let used = false;
    for (const hit of (await grep('-lw', name, candidate, '--', ...sources, ...skip)).split('\n')) {
      const path = hit.slice(candidate.length + 1);
      if (!path || used) continue;
      // A Rust file's own `#[cfg(test)]` items are tests too.
      const text = await git(['show', `${candidate}:${path}`]);
      used = (path.endsWith('.rs') ? nonTest(text) : text).split('\n').some(call);
    }
    if (!used) result.push(name);
  }
  return result;
}
