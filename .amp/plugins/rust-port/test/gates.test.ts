/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { git } from '../src/checks.ts';
import { CRATE_LINTS } from '../src/redisearch.ts';
import { cosmetic, gates, measure, outside, parseDiff, sizeClass } from '../src/gates.ts';
import type { Plan } from '../src/plan.ts';
import { newState } from '../src/state.ts';

test('a diff is read file by file, ignoring hunk counts and lines that look like headers', () => {
  const read = (diff: string) =>
    parseDiff(diff).map(({ path, added, removed }) => ({ path, added, removed }));
  const git = `diff --git a/src/a.rs b/src/a.rs
index 1..2 100644
--- a/src/a.rs
+++ b/src/a.rs
@@ -1,9 +1,9 @@
 fn a() {}
--- b/fake.md
+++ b/fake.md
diff --git a/src/old.c b/src/old.c
deleted file mode 100644
index 1..0
--- a/src/old.c
+++ /dev/null
@@ -1 +0,0 @@
-int old(void);
`;
  assert.deepEqual(read(git), [
    { path: 'src/a.rs', added: ['++ b/fake.md'], removed: ['-- b/fake.md'] },
    { path: 'src/old.c', added: [], removed: ['int old(void);'] },
  ]);
  // A reviewer's patch has no `diff --git` lines and often wrong hunk counts.
  const model = `--- a/src/a.rs
+++ b/src/a.rs
@@ -1,9 +1,9 @@
-fn b() {}
+fn c() {}
--- /dev/null
+++ b/tests/new.py
@@ -0,0 +5 @@
+def test(): pass
`;
  assert.deepEqual(read(model), [
    { path: 'src/a.rs', added: ['fn c() {}'], removed: ['fn b() {}'] },
    { path: 'tests/new.py', added: ['def test(): pass'], removed: [] },
  ]);
});

test('deleting code lowers net code and counts nothing towards the size classes', () => {
  const files = [
    { path: 'src/ffi/geo.rs', added: ['}'], removed: Array(105).fill('fn old() {}') },
    { path: 'tests/geo.rs', added: ['assert!(x);'], removed: [] },
    { path: 'README.md', added: ['words'], removed: [] },
  ];
  assert.deepEqual(measure(files), { lines: 2, code: -104, files: 2 });
});

test('size classes push back from 250 added lines on, and only XL blocks', () => {
  assert.deepEqual(
    [10, 250, 251, 500, 501, 1000, 1001].map((n) => sizeClass(n)?.label),
    [undefined, undefined, 'M', 'M', 'L', 'L', 'XL'],
  );
});

test('a path is outside the scope unless a listed file or directory holds it', () => {
  assert.deepEqual(outside(['src/geo/a.rs', 'src/geo.rs', 'build.sh'], ['src/geo', 'build.sh']), [
    'src/geo.rs',
  ]);
});

test('a file changed only in blank lines is cosmetic; any other whitespace may matter', () => {
  const diff = `--- a/Cargo.toml
+++ b/Cargo.toml
@@ -1,2 +1,1 @@
 [package]
-
--- a/a.rs
+++ b/a.rs
@@ -1 +1 @@
-fn a() {
+fn  a() {
--- a/b.rs
+++ b/b.rs
@@ -1 +1,2 @@
+fn b() {}
--- a/query_eval/Cargo.toml
+++ b/query_eval/Cargo.toml
@@ -1,2 +1,3 @@
 [dependencies]
+geo.workspace = true
 ffi.workspace = true
@@ -9,3 +10,2 @@
 [dev-dependencies]
-geo.workspace = true
 query.workspace = true
`;
  assert.deepEqual(cosmetic(parseDiff(diff)), ['Cargo.toml']);
});

/** A repository whose commits write `files`; returns each commit's revision. */
async function repository() {
  const root = await mkdtemp(join(tmpdir(), 'gates-'));
  const sh = (args: string[]) => git(root, args);
  await sh(['init', '-q']);
  const commit = async (files: Record<string, string>) => {
    for (const [path, text] of Object.entries(files)) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), text);
    }
    await sh(['add', '.']);
    await sh(['-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '-qm', 'c']);
    return sh(['rev-parse', 'HEAD']);
  };
  return { root, sh, commit };
}

test('the gates report every problem of an in-scope claim, and only the scope otherwise', async () => {
  const crate = 'src/redisearch_rs/geo/';
  const { root, sh, commit } = await repository();
  const lints = `#![warn(\n${CRATE_LINTS.map((l) => `    ${l},\n`).join('')})]\n`;
  const base = await commit({
    [`${crate}src/filter.rs`]: 'pub fn f() {}\n',
    [`${crate}src/lib.rs`]: lints,
  });
  const plan = {
    contract: [
      { behavior: 'b', tests: [{ path: `${crate}tests/a.rs`, name: 'radius', stage: 'rust' }] },
    ],
    divergences: [],
    stages: { rust: { paths: [crate] } },
  } as unknown as Plan;
  const state = newState('T-1', plan);
  const stray = await commit({ 'src/geo.c': 'int f;\n' });
  assert.deepEqual((await gates(sh, root, state, 'rust', base, stray)).problems, [
    `it changes src/geo.c, outside ${crate}; revert or amend`,
  ]);

  // Its new test module costs no code, and the lint it drops is caught though nothing is added.
  const candidate = await commit({
    [`${crate}src/filter.rs`]:
      'pub fn f() { x.unwrap(); }\npub fn g() {}\npub fn h() {}\n#[cfg(test)]\nmod tests {\n    fn t() {}\n}\n',
    [`${crate}src/lib.rs`]: lints.replace('    clippy::todo,\n', ''),
  });
  state.evidence.reviewed = {
    ...{ stage: 'rust', candidate: base, base, plan: state.job.planHash, patch: 'p', checks: [] },
    ...{ reviewers: [{ reviewer: 'R' }], size: { lines: 0, code: 0, files: 0 } },
  };
  const { problems } = await gates(sh, root, state, 'rust', stray, candidate);
  assert.deepEqual(
    problems.map((p) => p.split(/[:;]/)[0]),
    [
      'it adds 1 net lines of code (tests aside), its reviewed version 0',
      `${crate}src/filter.rs adds \`unwrap()\` outside tests`,
      `${crate}src/lib.rs lacks lints of the crate lint block`,
      `${crate}tests/a.rs does not exist`,
    ],
  );
});
