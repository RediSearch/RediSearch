/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { git, gitVcs } from '../git.ts';
import { engine } from '../engine.ts';
import type { State } from '../job.ts';
import type { Host } from '../ports.ts';
import { fakeForge, fakeHost, plan, profile } from './fakes.ts';

const WORKER = 'T-worker';

/** A checkout on `main` whose commits write files, and a job on it, kept outside it. */
async function setup(host: Host = fakeHost().host) {
  const root = await mkdtemp(join(tmpdir(), 'engine-'));
  const dir = await mkdtemp(join(tmpdir(), 'engine-state-'));
  const sh = (args: string[]) => git(root, args);
  await sh(['init', '-q', '-b', 'main']);
  const commit = async (files: Record<string, string>) => {
    for (const [path, text] of Object.entries(files)) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), text);
    }
    await sh(['add', '.']);
    await sh(['-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '-qm', 'c']);
    return sh(['rev-parse', 'HEAD']);
  };
  await commit({ README: 'r\n' });
  // The work goes on a branch of its own, so `main` stays the base the first stage diffs against.
  await sh(['checkout', '-q', '-b', 'work']);
  const { forge, published } = fakeForge();
  const port = engine({
    ...{ root, dir, profile, host, vcs: gitVcs(root), forge: () => forge },
    ...{ policy: ['core.md', 'host.md'], log: () => {}, disposed: new AbortController().signal },
  });
  const run = (name: string, input: Record<string, unknown>, thread = WORKER) =>
    port.operations.find((o) => o.name === name)!.run(input, thread, new AbortController().signal);
  const state = async (): Promise<State> =>
    JSON.parse(await readFile(join(dir, 'state.json'), 'utf8'));
  await run('plan', { plan });
  return { port, run, commit, state, published, dir };
}

test('admitting the first stage publishes it and hands the job to a fresh worker', async () => {
  const { host, started, sent } = fakeHost();
  const { port, run, commit, state, published } = await setup(host);
  const a = await commit({ 'a/t.txt': 'pins\n' });

  const result = await run('claim', { stage: 'a', revision: a });
  assert.match(result, /Stage a admitted at/);
  assert.match(result, /continues in a fresh thread, https:\/\/host\/T-2/);
  assert.deepEqual(
    published.map((p) => [p.branch, p.base, p.candidate]),
    [['p-a', 'main', a]],
  );
  // The reviewer ran under the worker, then the next worker was started under it and briefed.
  assert.deepEqual(
    started.map((s) => [s.id, s.parent, s.labels]),
    [
      ['T-1', WORKER, ['a', 'reviewer']],
      ['T-2', WORKER, ['b']],
    ],
  );
  const briefing = sent.find((s) => s.thread === 'T-2')!.message;
  assert.match(briefing, /You take over a port job at stage `b`/);
  assert.match(briefing, /Follow `core\.md` and `host\.md`/);
  assert.match(briefing, /subscribe to the CI and comments of https:\/\/forge\/pull\/1\./);
  const { job } = await state();
  assert.deepEqual([job.owner, job.former, job.used], ['T-2', [WORKER], 1]);

  // The former worker owns nothing, and is told so when it wakes.
  await assert.rejects(run('findings', {}), /does not own a port job/);
  assert.match((await port.startTurn(WORKER))!, /handed the job to https:\/\/host\/T-2/);

  const b = await commit({ 'b/x.txt': 'x\n' });
  assert.match(await run('claim', { stage: 'b', revision: b }, 'T-2'), /Stage b admitted/);
  assert.deepEqual(
    published.map((p) => [p.branch, p.base]),
    [
      ['p-a', 'main'],
      ['p-b', 'p-a'],
    ],
  );
  assert.equal((await state()).job.owner, 'T-2');
});

test('a state recorded with its stages out of order is read in the profile order', async () => {
  const { run, commit, dir } = await setup();
  const file = join(dir, 'state.json');
  const saved = JSON.parse(await readFile(file, 'utf8'));
  const { a, b } = saved.job.plan.stages;
  saved.job.plan.stages = { b, a };
  await writeFile(file, JSON.stringify(saved));
  const revision = await commit({ 'a/t.txt': 'pins\n' });
  assert.match(await run('claim', { stage: 'a', revision }), /Stage a admitted/);
});

test('a claim the gates reject costs no assessment', async () => {
  const { run, commit, state } = await setup();
  const stray = await commit({ 'a/t.txt': 'pins\n', 'c/x.txt': 'x\n' });
  await assert.rejects(
    run('claim', { stage: 'a', revision: stray }),
    /outside a\/; revert or amend/,
  );
  assert.equal((await state()).job.used, 0);
});

test('a failed handoff keeps the worker, and passing evidence is reused for free', async () => {
  const { host } = fakeHost();
  const failing: Host = {
    ...host,
    startThread: (o) => (o.role ? host.startThread(o) : Promise.reject(new Error('no agent'))),
  };
  const { run, commit, state } = await setup(failing);
  const a = await commit({ 'a/t.txt': 'pins\n' });
  assert.doesNotMatch(await run('claim', { stage: 'a', revision: a }), /fresh thread/);
  assert.match(await run('claim', { stage: 'a', revision: a }), /Stage a admitted/);
  const { job } = await state();
  assert.deepEqual([job.owner, job.former, job.used], [WORKER, undefined, 1]);
});

test('the worker continues while it moves the job, and stops when blocked or stuck', async () => {
  const { port, run } = await setup();
  assert.equal(await port.endTurn('T-other'), undefined);
  assert.deepEqual(await port.endTurn(WORKER), { continue: 'Continue the port job.' });
  assert.deepEqual(await port.endTurn(WORKER), {
    stop: 'the last continued turn made no progress',
  });
  await run('blocked', { reason: 'no push access' });
  assert.deepEqual(await port.endTurn(WORKER), { stop: 'blocked: no push access' });
});
