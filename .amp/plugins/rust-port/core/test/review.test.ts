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
import { toFinding } from '../confirm.ts';
import { followUpBrief, parseProposals, runReviewer } from '../review.ts';
import { fakeHost } from './fakes.ts';

const signal = new AbortController().signal;
const o = {
  reviewer: { name: 'Plan adherence', instructions: 'Review only the plan.' },
  role: 'the role',
  stage: 'swap',
  owner: 'T-0',
  brief: 'the brief',
  signal,
  timeoutMs: 1000,
};

test('a reviewer thread starts under the worker, labelled, briefed, and its proposals parsed', async () => {
  const reply = {
    location: 'lib.rs:1',
    scenario: 's',
    correction: 'c',
    refs: [{ path: 'lib.rs' }],
  };
  const { host, started, sent } = fakeHost(() => `Done.\n${JSON.stringify({ findings: [reply] })}`);
  const result = await runReviewer(host, o);

  assert.deepEqual(started, [
    { id: 'T-1', parent: 'T-0', labels: ['swap', 'plan-adherence'], role: 'the role' },
  ]);
  assert.deepEqual(sent, [{ thread: 'T-1', message: 'the brief' }]);
  assert.equal(result.error, undefined);
  assert.deepEqual(result.proposals, [{ ...reply, repro: undefined, deletion: undefined }]);
  const confirmed = { stage: 'swap', candidate: 'c', blocking: true, verdict: 'reproduced' };
  const finding = toFinding(result, result.proposals[0], { ...confirmed, link: 'l' });
  assert.equal(finding.title, 'swap / Plan adherence · lib.rs:1');
  assert.match(finding.body, /Evidence: reproduced/);
});

test('an unreadable verdict is a reviewer failure after one fresh retry, not an empty review', async () => {
  const { host, started } = fakeHost(() => 'Looks fine to me.');
  const result = await runReviewer(host, o);
  assert.ok(result.error);
  assert.deepEqual(result.proposals, []);
  assert.equal(started.length, 2);
});

test('a resumed reviewer that fails is replaced by a fresh one', async () => {
  const { host, started, sent } = fakeHost((thread) =>
    thread === 'T-old' ? 'garbled' : '{"findings": []}',
  );
  const result = await runReviewer(host, {
    ...o,
    resume: { thread: 'T-old', message: 'the delta' },
  });
  assert.equal(result.error, undefined);
  assert.equal(result.thread, 'T-1');
  assert.deepEqual(
    sent.map((s) => [s.thread, s.message]),
    [
      ['T-old', 'the delta'],
      ['T-1', 'the brief'],
    ],
  );
  assert.equal(started.length, 1);
});

test('a re-review asks about the change since, or the whole diff after a rebase', () => {
  const o = { previous: 'a'.repeat(40), candidate: 'b'.repeat(40), diff: 'run/diff.patch' };
  const after = followUpBrief({ ...o, delta: 'run/delta.patch' });
  assert.match(after, /changed since: `run\/delta\.patch`/);
  assert.match(after, /review of a{10} and claims b{10}/);
  assert.match(followUpBrief(o), /rebased, so compare its whole diff, `run\/diff\.patch`/);
});

test('reviewer evidence with non-string fields is dropped, not kept to fail later', () => {
  const reply = {
    location: 'lib.rs:1',
    scenario: 's',
    correction: 'c',
    refs: [{ path: 'lib.rs' }],
    repro: { path: 1, test: 1, patch: 1, command: 1 },
    deletion: { patch: 2 },
  };
  const [parsed] = parseProposals(JSON.stringify({ findings: [reply] }));
  assert.equal(parsed.repro, undefined);
  assert.equal(parsed.deletion, undefined);
});
