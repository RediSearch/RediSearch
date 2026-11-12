/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  blankOnlyFiles,
  measureSize,
  filesOutside,
  parseDiff,
  patchId,
  sizeClass,
} from '../diff.ts';

const KINDS = {
  classify: (path: string) => (/(^|\/)tests\//.test(path) ? 'test' : 'source'),
} as const;
const read = (diff: string) =>
  parseDiff(diff).map(({ path, added, removed }) => ({ path, added, removed }));

test('a diff is read file by file, ignoring hunk counts and lines that look like headers', () => {
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

test('a file without hunks, binary, empty or only changing its mode, is still a file', () => {
  const git = `diff --git a/img/logo.png b/img/logo.png
new file mode 100644
index 0000000..1111111
Binary files /dev/null and b/img/logo.png differ
diff --git a/src/empty.rs b/src/empty.rs
new file mode 100644
index 0000000..e69de29
diff --git a/build.sh b/build.sh
old mode 100644
new mode 100755
`;
  assert.deepEqual(
    read(git).map((f) => f.path),
    ['img/logo.png', 'src/empty.rs', 'build.sh'],
  );
});

test('a reviewer patch with git file lines but no index line has headers, not content', () => {
  const patch = `diff --git a/t/x.rs b/t/x.rs
--- /dev/null
+++ b/t/x.rs
@@ -0,0 +1 @@
+fn x() {}
`;
  assert.deepEqual(read(patch), [{ path: 't/x.rs', added: ['fn x() {}'], removed: [] }]);
});

test('a renamed file is named by its new path', () => {
  const git = `diff --git a/src/old.rs b/src/new_name.rs
similarity index 100%
rename from src/old.rs
rename to src/new_name.rs
`;
  assert.deepEqual(
    read(git).map((f) => f.path),
    ['src/new_name.rs'],
  );
});

test('a patch keeps its ID when rebased, and loses it when any line changes', () => {
  const diff = (at: number, line: string) => `diff --git a/a.rs b/a.rs
index ${at}..${at + 1} 100644
--- a/a.rs
+++ b/a.rs
@@ -${at},2 +${at},2 @@ fn a()
 fn a() {
-    old();
+    ${line}
`;
  assert.equal(patchId(diff(1, 'new();')), patchId(diff(40, 'new();')));
  assert.notEqual(patchId(diff(1, 'new();')), patchId(diff(1, 'new(); ')));
});

test('deleting code lowers net code and counts nothing towards the size classes', () => {
  const files = [
    { path: 'src/ffi/geo.rs', added: ['}'], removed: Array(105).fill('fn old() {}') },
    { path: 'tests/geo.rs', added: ['assert!(x);'], removed: [] },
    { path: 'README.md', added: ['words'], removed: [] },
  ];
  assert.deepEqual(measureSize(files, KINDS), { lines: 2, code: -104, files: 2 });
});

test('size classes push back from 250 added lines on, and only XL blocks', () => {
  assert.deepEqual(
    [10, 250, 251, 500, 501, 1000, 1001].map((n) => sizeClass(n)?.label),
    [undefined, undefined, 'M', 'M', 'L', 'L', 'XL'],
  );
});

test('a path is outside the scope unless a listed file or directory holds it', () => {
  assert.deepEqual(
    filesOutside(['src/geo/a.rs', 'src/geo.rs', 'build.sh'], ['src/geo', 'build.sh']),
    ['src/geo.rs'],
  );
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
  assert.deepEqual(blankOnlyFiles(parseDiff(diff)), ['Cargo.toml']);
});
