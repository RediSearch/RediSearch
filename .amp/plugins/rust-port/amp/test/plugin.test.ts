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
import { ampHost } from '../plugin.ts';

/** An Amp whose threads record what they were sent, and reply with `text`. */
function amp(text: string) {
  const seen: { labels?: string[]; messages: string[]; instructions?: string; parent?: string } = {
    messages: [],
  };
  const thread = {
    id: 'T-r',
    addLabels: async (labels: string[]) => (seen.labels = labels),
    appendUserMessage: async (m: { content: string }) => void seen.messages.push(m.content),
    waitForResponse: async () => ({ content: [{ type: 'text', text }] }),
    cancel: async () => {},
  };
  const agent = {
    createThread: async (o: { parentThreadID: string }) => (
      (seen.parent = o.parentThreadID),
      thread
    ),
  };
  const api = {
    system: { ampURL: new URL('https://ampcode.com') },
    createAgent: (config: { instructions: string }) => (
      (seen.instructions = config.instructions),
      agent
    ),
    threads: { get: () => ({ ...thread, agent: async () => agent }) },
  } as any;
  return { api, seen };
}

test('a reviewer is a labelled read-only thread under the worker that answers what it is asked', async () => {
  const { api, seen } = amp('{"findings": []}');
  const threads = ampHost(api, 'rust-port');
  const id = await threads.startThread({
    parent: 'T-1',
    labels: ['swap', 'correctness'],
    role: 'Review.',
  });
  const reply = await threads.ask(id, 'the brief', {
    signal: new AbortController().signal,
    timeoutMs: 1000,
  });
  assert.deepEqual(
    [id, seen.parent, seen.labels, seen.instructions],
    ['T-r', 'T-1', ['rust-port', 'swap', 'correctness'], 'Review.'],
  );
  assert.deepEqual([seen.messages, reply], [['the brief'], '{"findings": []}']);
  assert.equal(threads.threadUrl(id), 'https://ampcode.com/threads/T-r');
  assert.equal(threads.toolName('claim'), 'port_claim');
});

test('a worker thread is started with the parent worker agent, not a reviewer', async () => {
  const { api, seen } = amp('');
  const threads = ampHost(api, 'rust-port');
  await threads.startThread({ parent: 'T-1', labels: ['swap'] });
  assert.equal(seen.instructions, undefined);
  assert.deepEqual(seen.labels, ['rust-port', 'swap']);
  await assert.rejects(threads.sendMessage('nope', 'hi'), /not an Amp thread/);
});
