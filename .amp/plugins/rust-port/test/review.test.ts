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
import { toFinding } from '../src/confirm.ts';
import { followUp, review } from '../src/review.ts';

test('a reviewer thread is labelled, briefed, and its proposals parsed', async () => {
  const sent: { labels?: string[]; message?: string; instructions?: string } = {};
  const reply = {
    location: 'lib.rs:1',
    scenario: 's',
    correction: 'c',
    refs: [{ path: 'lib.rs' }],
  };
  const thread = {
    id: 'T-r',
    addLabels: async (labels: string[]) => (sent.labels = labels),
    appendUserMessage: async (m: { content: string }) => void (sent.message = m.content),
    waitForResponse: async () => ({
      content: [{ type: 'text', text: `Done.\n${JSON.stringify({ findings: [reply] })}` }],
    }),
    cancel: async () => {},
  };
  const amp = {
    system: { ampURL: new URL('https://ampcode.com') },
    createAgent: (config: { instructions: string }) => {
      sent.instructions = config.instructions;
      return { createThread: async () => thread };
    },
  } as any;
  const result = await review(amp, {
    reviewer: { name: 'Plan adherence', instructions: 'Review only the plan.' },
    stage: 'swap',
    owner: 'T-1',
    brief: 'the brief',
    signal: new AbortController().signal,
    timeoutMs: 1000,
  });

  assert.deepEqual(sent.labels, ['rust-port', 'swap', 'plan-adherence']);
  assert.equal(sent.message, 'the brief');
  assert.ok(sent.instructions?.includes('Review only the plan.'));
  assert.equal(result.error, undefined);
  assert.deepEqual(result.proposals, [{ ...reply, repro: undefined, deletion: undefined }]);
  const confirmed = {
    stage: 'swap' as const,
    candidate: 'c',
    blocking: true,
    verdict: 'reproduced',
  };
  const finding = toFinding(result, result.proposals[0], { ...confirmed, link: 'l' });
  assert.equal(finding.title, 'swap / Plan adherence · lib.rs:1');
  assert.match(finding.body, /Evidence: reproduced/);
});

test('an unreadable verdict is a reviewer failure, not an empty review', async () => {
  const thread = {
    id: 'T-r',
    addLabels: async () => {},
    appendUserMessage: async () => {},
    waitForResponse: async () => ({ content: [{ type: 'text', text: 'Looks fine to me.' }] }),
    cancel: async () => {},
  };
  const amp = {
    system: { ampURL: new URL('https://ampcode.com') },
    createAgent: () => ({ createThread: async () => thread }),
  } as any;
  const reviewer = { name: 'Correctness', instructions: '' };
  const signal = new AbortController().signal;
  const o = { reviewer, stage: 'rust' as const, owner: 'T-1' };
  const result = await review(amp, { ...o, brief: 'b', signal, timeoutMs: 1000 });
  assert.ok(result.error);
  assert.deepEqual(result.proposals, []);
});

test('a re-review asks about the change since, or the whole diff after a rebase', () => {
  const o = { previous: 'a'.repeat(40), candidate: 'b'.repeat(40), diff: 'run/diff.patch' };
  const after = followUp({ ...o, delta: 'run/delta.patch' });
  assert.match(after, /changed since: `run\/delta\.patch`/);
  assert.match(after, /review of a{10} and claims b{10}/);
  assert.match(followUp(o), /rebased, so compare its whole diff, `run\/diff\.patch`/);
});

test('reviewer evidence with non-string fields is dropped, not kept to fail later', async () => {
  const reply = {
    location: 'lib.rs:1',
    scenario: 's',
    correction: 'c',
    refs: [{ path: 'lib.rs' }],
    repro: { path: 1, test: 1, patch: 1, command: 1 },
    deletion: { patch: 2 },
  };
  const thread = {
    id: 'T-r',
    addLabels: async () => {},
    appendUserMessage: async () => {},
    waitForResponse: async () => ({
      content: [{ type: 'text', text: JSON.stringify({ findings: [reply] }) }],
    }),
    cancel: async () => {},
  };
  const amp = {
    system: { ampURL: new URL('https://ampcode.com') },
    createAgent: () => ({ createThread: async () => thread }),
  } as any;
  const reviewer = { name: 'Correctness', instructions: '' };
  const result = await review(amp, {
    ...{ reviewer, stage: 'rust', owner: 'T-1', brief: 'b' },
    ...{ signal: new AbortController().signal, timeoutMs: 1000 },
  });
  assert.equal(result.proposals[0].repro, undefined);
  assert.equal(result.proposals[0].deletion, undefined);
});
