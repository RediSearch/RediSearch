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
import { deletesTests, runnable } from '../src/confirm.ts';

test('a reproduction runs one test runner, with no shell syntax or Cargo configuration', () => {
  for (const command of [
    'cargo test --manifest-path src/redisearch_rs/Cargo.toml -p geo --test integration radius_nan -- --exact',
    'cargo nextest run -p quantile insert_after_query',
    './build.sh RUN_PYTEST TEST=test_geo:testUnits',
  ])
    assert.equal(runnable(command), true, command);
  for (const command of [
    `cargo test --config 'build.rustc-wrapper="tests/evil.sh"'`,
    'cargo test -Z unstable-options',
    'cargo run --manifest-path tests/evil/Cargo.toml',
    'cargo test -p geo; rm -rf /',
    './build.sh RUN_PYTEST TEST=$(evil)',
  ])
    assert.equal(runnable(command), false, command);
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
  ] as const;
  for (const [name, diff, expected] of cases)
    assert.equal(await deletesTests(diff, read), expected, name);
});
