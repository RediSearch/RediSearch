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
import { git, gitVcs } from '../../core/git.ts';
import { orphanedExports, fileReader, type PortPlan } from '../../c-to-rust/port.ts';
import { parseDiff } from '../../core/diff.ts';
import { checkClaim } from '../../core/gates.ts';
import { newState } from '../../core/job.ts';
import type { Plan } from '../../core/plan.ts';
import { CRATE_LINTS, layout, profile } from '../profile.ts';

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
  const { root, commit } = await repository();
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
  } as unknown as Plan<PortPlan>;
  const state = newState('T-1', plan);
  const stray = await commit({ 'src/geo.c': 'int f;\n' });
  assert.deepEqual(
    (
      await checkClaim({
        root,
        vcs: gitVcs(root),
        profile,
        state,
        stage: 'rust',
        base,
        candidate: stray,
      })
    ).problems,
    [`it changes src/geo.c, outside ${crate}; revert or amend`],
  );

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
  const { problems } = await checkClaim({
    root,
    vcs: gitVcs(root),
    profile,
    state,
    stage: 'rust',
    base: stray,
    candidate,
  });
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

test('an export only a test module beside it calls is an orphan', async () => {
  const { root, sh, commit } = await repository();
  const ffi = 'src/redisearch_rs/c_entrypoint/geo_ffi/src/lib.rs';
  const exported = '#[unsafe(no_mangle)]\npub extern "C" fn Geo_Free() {}\n';
  const base = await commit({ 'src/geo.c': 'void f(void) { Geo_Free(); }\n', [ffi]: exported });
  await sh(['rm', '-q', 'src/geo.c']);
  const candidate = await commit({
    [ffi]: `${exported}#[cfg(test)]\nmod tests {\n    fn frees() { super::Geo_Free(); }\n}\n`,
  });
  const files = parseDiff(await sh(['diff', base, candidate]));
  const tracked = await gitVcs(root).listFiles(candidate);
  assert.deepEqual(await orphanedExports(layout, files, tracked, fileReader(root)), ['Geo_Free']);
});
