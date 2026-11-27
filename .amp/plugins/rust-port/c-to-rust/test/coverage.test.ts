/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { judgeCoverage, lineRanges } from '../coverage.ts';

const record = (path: string, lines: [number, number][]) =>
  `SF:${path}\n${lines.map(([l, n]) => `DA:${l},${n}`).join('\n')}\nend_of_record\n`;

async function checkout() {
  const root = await mkdtemp(join(tmpdir(), 'coverage-'));
  await mkdir(join(root, 'src'));
  await writeFile(join(root, 'src/geo.c'), '');
  return root;
}

test('uncovered lines read as ranges', () => {
  assert.deepEqual(lineRanges([1, 2, 3, 7, 9, 10]), ['1-3', '7', '9-10']);
  assert.deepEqual(lineRanges([]), []);
});

test('coverage adds up the records of a source, however its path is spelled', async () => {
  const root = await checkout();
  const link = `${root}-link`;
  await symlink(root, link);
  const info = join(root, 'cov.info');
  await writeFile(
    info,
    record(join(root, 'src/geo.c'), [
      [1, 0],
      [2, 3],
      [3, 0],
      [4, 0],
    ]) +
      record(join(link, 'src/geo.c'), [
        [3, 1],
        [4, 0],
      ]) +
      record(join(root, 'src/other.c'), [[1, 0]]),
  );
  const { problem, report } = await judgeCoverage(info, ['src/geo.c'], 0, root);
  assert.equal(problem, undefined);
  assert.match(report!, /\| `src\/geo\.c` \| 2\/4 \| 50\.0% \| 1, 4 \|/);
  assert.doesNotMatch(report!, /other\.c/);
});

test('a run that wrote no fresh tracefile, or missed a source, fails', async () => {
  const root = await checkout();
  const info = join(root, 'cov.info');
  assert.equal(
    (await judgeCoverage(info, ['src/geo.c'], 0, root)).problem,
    'wrote no fresh coverage to cov.info',
  );
  await writeFile(info, record(join(root, 'src/geo.c'), [[1, 1]]));
  assert.equal(
    (await judgeCoverage(info, ['src/geo.c'], Date.now() + 60_000, root)).problem,
    'wrote no fresh coverage to cov.info',
  );
  assert.equal(
    (await judgeCoverage(info, ['src/geo.c', 'src/rest.c'], 0, root)).problem,
    'has no coverage of src/rest.c: is it compiled into the module?',
  );
});
