/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { testRan, runChecks, type CheckEnv } from '../checks.ts';
import type { Vcs } from '../ports.ts';

async function run(judges: CheckEnv['judges'] = {}): Promise<CheckEnv> {
  const dir = await mkdtemp(join(tmpdir(), 'checks-'));
  const vcs = {} as Vcs;
  return {
    root: dir,
    dir,
    vcs,
    signal: new AbortController().signal,
    vars: {},
    judges,
    debug: () => {},
  };
}

test('checks stop at the first that exits non-zero', async () => {
  const checks = ['true', 'exit 3', 'true'].map((command) => ({ kind: 'test', command }));
  const results = await runChecks(await run(), checks);
  assert.deepEqual(
    results.map((r) => r.exitCode),
    [0, 3],
  );
});

test('a check that exits 0 is judged by its kind from its log, and stops the rest on a problem', async () => {
  const judges = {
    miri: ({ log }: { log: string }) => (log.includes('0 tests') ? 'ran nothing' : undefined),
  };
  const checks = [
    { kind: 'test', command: 'echo 0 tests' },
    { kind: 'miri', command: 'echo 0 tests' },
    { kind: 'lint', command: 'true' },
  ];
  const results = await runChecks(await run(judges), checks);
  assert.deepEqual(
    results.map((r) => r.problem),
    [undefined, 'ran nothing'],
  );
});

test('a test ran if some check ran it, not if every check ignored or skipped it', () => {
  const ignored = 'test geo::radius_nan ... ignored';
  const skipped = '    SKIP [   0.000s] geo::integration radius_nan';
  assert.equal(testRan(`${ignored}\n${skipped}`, 'radius_nan'), false);
  assert.equal(testRan('test_geo.py::testUnits SKIPPED (cluster only)', 'testUnits'), false);
  assert.equal(testRan(`${skipped}\ntest geo::radius_nan ... ok`, 'radius_nan'), true);
  assert.equal(testRan('test other::radius_nan ... ok', 'target::radius_nan'), false);
  assert.equal(testRan('test geo::target::radius_nan ... ok', 'target::radius_nan'), true);
});
