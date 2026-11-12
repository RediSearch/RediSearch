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
import { deletesTests } from '../../core/confirm.ts';
import { ruleViolations } from '../../core/gates.ts';
import { CRATE_LINTS, profile, RULES } from '../profile.ts';

const FFI = 'src/redisearch_rs/c_entrypoint/geo_ffi/src/lib.rs';
const PURE = 'src/redisearch_rs/geo/src/filter.rs';
const rules = (path: string, before: string | undefined, after: string) =>
  ruleViolations(RULES, path, before, after).map((v) =>
    v
      .slice(path.length + 1)
      .split(' ')
      .slice(0, 3)
      .join(' '),
  );

test('a rule is a ratchet: only violations the change adds count', () => {
  const old = 'let a = x.unwrap();\n';
  assert.deepEqual(rules(PURE, old, old), []);
  assert.deepEqual(rules(PURE, old, `${old}let b = y.unwrap();\n`), ['adds `unwrap()` outside']);
});

test('tests may unwrap, panic and use contract-free names freely', () => {
  const inline = 'fn f() {}\n#[cfg(test)]\nmod tests { fn t() { x.unwrap(); } }\n';
  assert.deepEqual(rules(PURE, '', inline), []);
  assert.deepEqual(rules('src/redisearch_rs/geo/tests/a.rs', '', 'x.unwrap();'), []);
});

test('FFI code dereferences through methods and asserts through named helpers', () => {
  const after = `
fn debug_assert_non_null<T>(p: *const T) { debug_assert!(!p.is_null()); }
pub unsafe extern "C" fn f(gf: *const G) {
    debug_assert!(!gf.is_null());
    let g = unsafe { &*gf };
}`;
  assert.deepEqual(rules(FFI, '', after), ['dereferences a raw', 'adds a bare']);
});

test('a # Safety section numbers its pre-conditions', () => {
  const doc = (body: string) => `/// Frees.\n/// # Safety\n${body}pub unsafe fn f() {}\n`;
  assert.deepEqual(rules(FFI, '', doc('/// `p` must be valid.\n')), ['adds a `#']);
  assert.deepEqual(rules(FFI, '', doc('/// 1. `p` must be valid.\n/// 2. Unaliased.\n')), []);
});

test('allocation, errors, allocators, test names and C ABI items follow the house rules', () => {
  assert.deepEqual(rules(FFI, '', 'unsafe { RedisModule_Alloc.unwrap() }'), [
    'adds `unwrap()` outside',
    'calls the Redis',
  ]);
  assert.deepEqual(rules(PURE, '', "fn new() -> Result<Self, &'static str> {}"), [
    "returns a `&'static",
  ]);
  assert.deepEqual(rules('src/redisearch_rs/geo/tests/a.rs', '', '#[global_allocator]'), [
    'adds an allocator;',
  ]);
  assert.deepEqual(rules('tests/pytests/test_geo.py', '', 'def testGeoFilterContract(env):'), [
    'adds a test',
  ]);
  assert.deepEqual(rules('tests/cpptests/test_q.cpp', '', 'TEST_F(Q, testGeoFilterABI) {'), [
    'adds a test',
  ]);
  const tests = 'src/redisearch_rs/query_eval/tests/integration/geo.rs';
  assert.deepEqual(rules(tests, '', 'unsafe extern "C" { fn isWithinRadius(); }'), [
    'declares C functions',
  ]);
  assert.deepEqual(rules(PURE, '', 'unsafe extern "C" { fn f(); }'), ['adds C ABI']);
  assert.deepEqual(rules(FFI.replace('src/lib.rs', 'src/x.rs'), '', '#[unsafe(no_mangle)]'), []);
});

test('a new crate opts into the lints the workspace does not enable yet, and none drops one', () => {
  const root = 'src/redisearch_rs/quantile/src/lib.rs';
  const block = `#![warn(${CRATE_LINTS.join(', ')})]`;
  assert.deepEqual(rules(root, undefined, ''), ['lacks lints of']);
  assert.deepEqual(rules(root, undefined, block), []);
  assert.deepEqual(rules(root, 'pub fn f() {}', 'pub fn f() {}'), []);
  assert.deepEqual(rules(root, block, block.replace('clippy::todo, ', '')), ['lacks lints of']);
});

test('only test items are skipped, so production code after one is still checked', () => {
  const text = `#[cfg(test)]
fn helper() -> u8 { x.unwrap() }
#[cfg(test)]
mod tests;
pub fn new() -> u8 { y.unwrap() }
#[cfg(test)]
mod more { fn t() { z.unwrap(); } }`;
  assert.deepEqual(rules(PURE, '', text), ['adds `unwrap()` outside']);
});

test('a deletion that removes tests proves nothing, wherever the tests live', async () => {
  const source =
    'pub fn keep() {}\npub fn spare() {}\n#[cfg(test)]\nmod tests {\n    fn checks_spare() {}\n}\n';
  const read = async () => source;
  const patch = (path: string, line: string) =>
    `--- a/${path}\n+++ b/${path}\n@@ -1 +0,0 @@\n-${line}\n`;
  const cases = [
    ['production code', patch('src/geo.rs', 'pub fn spare() {}'), false],
    ['a test item beside the code', patch('src/geo.rs', '    fn checks_spare() {}'), true],
    ['a test file', patch('geo/tests/a.rs', 'fn t() {}'), true],
    [
      'production code, with a test switched off',
      `${patch('src/geo.rs', 'pub fn spare() {}')}+#[ignore]\n`,
      true,
    ],
  ] as const;
  for (const [name, diff, expected] of cases)
    assert.equal(await deletesTests(profile, diff, read), expected, name);
});
