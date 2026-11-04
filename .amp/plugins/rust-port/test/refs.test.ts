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
import { join } from 'node:path';
import { test } from 'node:test';
import { anchor, verify } from '../src/refs.ts';

test('a location resolves to its line, whether named by line, range or symbol', async () => {
  const root = await mkdtemp(join(tmpdir(), 'refs-'));
  await mkdir(join(root, 'src'));
  const source =
    'use x;\n\n/// Frees.\npub extern "C" fn Free(f: *mut u8) {}\nint New(double lon) {\n';
  await writeFile(join(root, 'src/geo.rs'), source);
  const resolved = await Promise.all(
    [
      'src/geo.rs:4',
      'src/geo.rs:4-6',
      'src/geo.rs:Free',
      'src/geo.rs:geo::New',
      'src/geo.rs:Gone',
    ].map((location) => anchor(root, location)),
  );
  assert.deepEqual(resolved, [
    'src/geo.rs:4',
    'src/geo.rs:4',
    'src/geo.rs:4',
    'src/geo.rs:5',
    'src/geo.rs:Gone',
  ]);
});

test('a malformed reference is reported, not thrown', async () => {
  assert.deepEqual(await verify(tmpdir(), [null, 'x']), [
    'null is not a reference',
    '"x" is not a reference',
  ]);
  const root = await mkdtemp(join(tmpdir(), 'refs-'));
  await writeFile(join(root, 'lib.rs'), 'fn f() {}\n');
  assert.deepEqual(
    await verify(root, [
      { path: 'lib.rs', test: 1 },
      { path: 'lib.rs', line: '2' },
    ]),
    ['lib.rs: test must be a name', 'lib.rs: line must be a number'],
  );
});
