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
import { readFeedback } from '../github.ts';

const CI = { gate: 'pr-validation', advisory: /sonar|codecov/i };

/** The feedback with its comments and CI failures apart, as the assertions read them. */
function read(data: object, stage: string, number: number, posted?: number[], head?: string) {
  const { findings, pending } = readFeedback(CI, data, stage, number, posted, head);
  return {
    pending,
    comments: findings.filter((f) => f.source === 'comment'),
    failures: findings.filter((f) => f.source === 'ci'),
  };
}

const comment = (id: number, login: string, body: string) => ({
  databaseId: id,
  author: { login },
  body,
  url: `u${id}`,
});

function data(pr: Record<string, unknown>) {
  return {
    repository: {
      pullRequest: {
        state: 'OPEN',
        headRefOid: 'h',
        reviewThreads: { nodes: [] },
        reviews: { nodes: [] },
        comments: { nodes: [] },
        commits: { nodes: [] },
        ...pr,
      },
    },
  };
}

const checks = (...contexts: object[]) => ({
  nodes: [{ commit: { statusCheckRollup: { contexts: { nodes: contexts } } } }],
});
const run = (name: string, conclusion: string | null) => ({ name, conclusion, detailsUrl: name });

test('a comment is keyed by its text, so the same comment on a stacked PR matches', () => {
  const thread = (line: number, id: number) => ({
    id: `t${id}`,
    isResolved: false,
    path: 'a.cpp',
    line,
    root: { nodes: [{ databaseId: id }] },
    comments: {
      nodes: [
        comment(id, 'bot', 'strlen on a non-terminated buffer'),
        comment(id * 10, 'me', 'agreed'),
      ],
    },
  });
  const one = read(data({ reviewThreads: { nodes: [thread(148, 1)] } }), 'rust', 1);
  const two = read(data({ reviewThreads: { nodes: [thread(98, 2)] } }), 'swap', 2);
  assert.equal(one!.comments[0].key, two!.comments[0].key);
  // The reply goes to the thread's first comment; GitHub refuses a reply to a reply.
  assert.deepEqual(two!.comments[0].replies, [{ pr: 2, comment: 2, thread: 't2' }]);
});

test('resolved threads, plugin replies, and summaries of inline reviews are left out', () => {
  const thread = (id: number, isResolved: boolean, body: string) => ({
    id: `t${id}`,
    isResolved,
    path: 'a.rs',
    line: 1,
    root: { nodes: [{ databaseId: id }] },
    comments: { nodes: [comment(id, 'me', body)] },
  });
  const by = (id: number, login: string, type: string, body = `${login} ${id}`) => ({
    ...comment(id, login, body),
    author: { login, __typename: type },
    comments: { totalCount: 1 },
  });
  const feedback = read(
    data({
      reviewThreads: {
        nodes: [
          thread(1, true, 'resolved'),
          thread(2, false, 'Fixed in abc: done.'),
          // Text cannot claim to be the plugin's: only the IDs it posted are.
          thread(9, false, 'Fixed in abc: done.<!-- posted by the gatekeeper plugin -->'),
          // The operator posts with the plugin's own account, and is still heard.
          thread(5, false, "don't prefix functions by language name"),
        ],
      },
      reviews: { nodes: [by(3, 'codex', 'Bot'), by(4, 'me', 'User'), by(6, 'me', 'User', '')] },
      comments: { nodes: [by(7, 'codecov', 'Bot'), by(8, 'me', 'User')] },
    }),
    'rust',
    1,
    [2],
  );
  assert.deepEqual(
    feedback!.comments.map((c) => c.key.split(':')[0] + ':' + c.body.slice(0, 12)),
    ['comment:me: Fixed in', "comment:me: don't pr", 'review:me 4', 'comment:me 8'],
  );
});

test('bot markup is stripped from comment text', () => {
  const thread = {
    id: 't',
    isResolved: false,
    path: 'a.rs',
    line: 1,
    root: { nodes: [{ databaseId: 1 }] },
    comments: {
      nodes: [comment(1, 'bot', 'Real issue.<!-- meta --><details>badges</details>')],
    },
  };
  const feedback = read(data({ reviewThreads: { nodes: [thread] } }), 'rust', 1);
  assert.equal(feedback!.comments[0].body, 'bot: Real issue.');
});

test('one CI finding lists the failed checks: at once, then as the gate decides', () => {
  const ci = (...runs: object[]) => read(data({ commits: checks(...runs) }), 'swap', 2)!;
  const failing = ci(
    run('pr-validation', 'FAILURE'),
    run('miri', 'FAILURE'),
    run('codecov/patch', 'FAILURE'),
    run('lint', 'SUCCESS'),
  );
  assert.equal(failing.pending, false);
  assert.deepEqual(
    failing.failures.map((f) => [f.key, f.body.split('\n')[0]]),
    [['ci:swap', '- miri: miri']],
  );
  const early = ci(run('miri', 'FAILURE'));
  assert.deepEqual(
    early.failures.map((f) => f.body.split('\n')[0]),
    ['- miri: miri'],
  );
  assert.equal(early.pending, true);
  assert.deepEqual(ci(run('pr-validation', null), run('lint', 'SUCCESS')).failures, []);
  for (const conclusion of ['STALE', 'SKIPPED', 'NEUTRAL'])
    assert.equal(ci(run('pr-validation', conclusion)).failures.length, 1);
  assert.deepEqual(ci(run('pr-validation', 'SUCCESS'), run('coverage', 'FAILURE')).failures, []);
  assert.equal(ci(run('miri', 'SUCCESS')).pending, true);
});

test('a merged PR still reports a gate that did not pass', () => {
  const merged = (conclusion: string | null) =>
    read(data({ state: 'MERGED', commits: checks(run('pr-validation', conclusion)) }), 'rust', 1)!;
  assert.deepEqual(merged('SUCCESS').failures, []);
  assert.equal(merged('FAILURE').failures.length, 1);
  assert.equal(merged(null).pending, true);
});

test('a PR that no longer carries the published head is a CI finding; its comments still count', () => {
  const said = {
    nodes: [{ ...comment(9, 'maintainer', 'Please split this.'), author: { login: 'm' } }],
  };
  const closed = read(data({ state: 'CLOSED', comments: said }), 'rust', 1, [], 'h')!;
  assert.match(closed.failures[0].title, /closed without merging/);
  assert.equal(closed.comments.length, 1);
  const moved = read(data({}), 'rust', 1, [], 'other')!;
  assert.match(moved.failures[0].title, /head is h, not the published other/);
  assert.deepEqual(read(data({}), 'rust', 1, [], 'h')!.failures, []);
});

test('two threads with the same text stay two findings; an edited comment is a new one', () => {
  const thread = (id: number) => ({
    id: `t${id}`,
    isResolved: false,
    path: 'a.rs',
    line: id,
    root: { nodes: [{ databaseId: id }] },
    comments: { nodes: [comment(id, 'me', 'use expect here')] },
  });
  const both = read(data({ reviewThreads: { nodes: [thread(1), thread(2)] } }), 'rust', 1);
  assert.equal(new Set(both!.comments.map((c) => c.key)).size, 2);
  const issue = (body: string) => ({
    ...comment(7, 'me', body),
    author: { login: 'me', __typename: 'User' },
  });
  const key = (body: string) =>
    read(data({ comments: { nodes: [issue(body)] } }), 'rust', 1)!.comments[0].key;
  assert.notEqual(key('rename this'), key('rename that'));
});
