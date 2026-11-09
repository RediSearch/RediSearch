/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { benchFilter, judgeBenchmarks, judgeMiri, judgeMutants, isTestCommand } from '../cargo.ts';

const RUNNERS = ['cargo test', 'cargo nextest run', './build.sh RUN_*'];

test('a Miri run fails on a test nextest marked slow, naming each once, or on no tests', () => {
  const summary = '     Summary [  23.052s] 9 tests run: 9 passed (4 slow), 0 skipped';
  const log = `    PASS [   2.101s] (1/9) quantile::integration exact_uncompressed_ranks
  TRY 1 SLOW [> 10.000s] (───) quantile::integration streaming_quantiles::case_1_single
  TRY 1 SLOW [> 20.000s] (───) quantile::integration streaming_quantiles::case_1_single
    PASS [  31.502s] (2/9) quantile::integration streaming_quantiles::case_1_single
${summary}`;
  assert.match(judgeMiri(log)!, /: quantile::integration streaming_quantiles::case_1_single\. /);
  assert.equal(judgeMiri(`${log.split('\n')[0]}\n${summary}`), undefined);
  // A command that never reached nextest, such as `true # miri`.
  assert.match(judgeMiri('')!, /ran no tests/);
});

test('a mutation run that finds no mutants covers nothing, so it fails', () => {
  assert.match(judgeMutants(' INFO No mutants to filter')!, /found no mutants/);
  assert.match(judgeMutants('')!, /found no mutants/);
  assert.equal(
    judgeMutants('Found 67 mutants to test\n67 mutants tested in 82s: 67 caught'),
    undefined,
  );
});

async function criterion(results: Record<string, string>) {
  const dir = await mkdtemp(join(tmpdir(), 'criterion-'));
  for (const [path, estimates] of Object.entries(results)) {
    await mkdir(join(dir, path, 'new'), { recursive: true });
    await writeFile(join(dir, path, 'new', 'estimates.json'), estimates);
  }
  return dir;
}
const median = (ns: number) => JSON.stringify({ median: { point_estimate: ns } });

test('each group is judged by the ratio of its Rust and C medians', async () => {
  const dir = await criterion({
    'parse/c': median(100),
    'parse/rust': median(104),
    'geo/radius/c': median(10),
    'geo/radius/rust': median(12),
  });
  assert.equal(
    await judgeBenchmarks([dir], 0, 5),
    'is slower than C beyond 1.05x: geo/radius 1.20x',
  );
  assert.equal(await judgeBenchmarks([dir], 0, 25), undefined);
});

test('results from before the check, missing pairs and unreadable estimates fail', async () => {
  const stale = await criterion({ 'parse/c': median(1), 'parse/rust': median(1) });
  for (const impl of ['c', 'rust'])
    await utimes(join(stale, `parse/${impl}/new/estimates.json`), 0, 0);
  const missing = /^measured no Criterion group with `c` and `rust` functions in /;
  assert.match((await judgeBenchmarks([stale], Date.now() - 60_000, 5))!, missing);
  // One complete pair does not excuse a group measured on one side only.
  const half = await criterion({
    'parse/c': median(1),
    'parse/rust': median(1),
    'scan/rust': median(1),
    'sort/c': median(1),
    'sort/rs': median(1),
  });
  assert.equal(
    await judgeBenchmarks([half], 0, 5),
    'measured only one of `c` and `rust` for: scan, sort',
  );
  assert.match((await judgeBenchmarks([join(stale, 'nowhere')], 0, 5))!, missing);
  const broken = await criterion({ 'parse/c': median(1), 'parse/rust': '{"mean"' });
  assert.equal(
    await judgeBenchmarks([broken], 0, 5),
    'left unreadable Criterion estimates for: parse',
  );
});

test('a reproduction runs one test runner, with no shell syntax or Cargo configuration', () => {
  for (const command of [
    'cargo test --manifest-path src/redisearch_rs/Cargo.toml -p geo --test integration radius_nan -- --exact',
    'cargo nextest run -p quantile insert_after_query',
    './build.sh RUN_PYTEST TEST=test_geo:testUnits',
  ])
    assert.equal(isTestCommand(RUNNERS, command), true, command);
  for (const command of [
    `cargo test --config 'build.rustc-wrapper="tests/evil.sh"'`,
    'cargo test -Z unstable-options',
    'cargo run --manifest-path tests/evil/Cargo.toml',
    'cargo test -p geo; rm -rf /',
    './build.sh RUN_PYTEST TEST=$(evil)',
  ])
    assert.equal(isTestCommand(RUNNERS, command), false, command);
});

test('a bench check runs every benchmark: no bench target and no Criterion filter', () => {
  assert.deepEqual(
    [
      'cd src/redisearch_rs && cargo bench -p quantile_bencher',
      'cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p geo_bencher',
      'cargo bench -p quantile_bencher small',
      'cargo bench -p quantile_bencher -- small',
      'cargo bench -p quantile_bencher --bench stream',
    ].map(benchFilter),
    [undefined, undefined, 'small', '--', '--bench'],
  );
});
