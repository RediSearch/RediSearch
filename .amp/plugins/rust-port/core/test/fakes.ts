/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import type { NewFinding } from '../job.ts';
import type { Plan } from '../plan.ts';
import type { Forge, Host, PR, Profile } from '../ports.ts';

/** A host whose threads are numbered, and whose reviewers answer with `reply`. */
export function fakeHost(
  reply: (thread: string, message: string) => string = () => '{"findings": []}',
) {
  const started: { id: string; parent: string; labels: string[]; role?: string }[] = [];
  const sent: { thread: string; message: string }[] = [];
  const host: Host = {
    threadUrl: (thread) => `https://host/${thread}`,
    toolName: (operation) => `port_${operation}`,
    async startThread(o) {
      const id = `T-${started.length + 1}`;
      started.push({ id, ...o });
      return id;
    },
    async sendMessage(thread, message) {
      sent.push({ thread, message });
    },
    async ask(thread, message) {
      sent.push({ thread, message });
      return reply(thread, message);
    },
  };
  return { host, started, sent };
}

/** A forge that records publications and reports `feedback` per PR number. */
export function fakeForge() {
  const published: (Parameters<Forge['publish']>[0] & { number: number })[] = [];
  const feedback = new Map<number, { findings: NewFinding[]; pending: boolean }>();
  const forge: Forge = {
    checkRemote: async () => [],
    async publish(o) {
      const number = o.existing?.number ?? published.length + 1;
      published.push({ ...o, number });
      return { number, url: `https://forge/pull/${number}`, head: o.candidate } satisfies PR;
    },
    fetchFeedback: async ({ number }) => feedback.get(number) ?? { findings: [], pending: false },
    replyToComment: async () => 1,
    resolveThread: async () => {},
  };
  return { forge, published, feedback };
}

/** Two stages, `a` and `b`, each needing a test check, reviewed by one reviewer. */
export const profile: Profile = {
  name: 'port',
  stages: [
    { name: 'a', checks: { test: [] } },
    { name: 'b', checks: { test: [] } },
  ],
  languages: { from: 'Old', to: 'New' },
  classify: (path) => (/(^|\/)tests\//.test(path) ? 'test' : 'source'),
  disabledTestPattern: /@skip/,
  rules: [],
  ruleFiles: 'RULES.md',
  testRunners: ['true'],
  isTestCommand: (command) => command === 'true',
  judges: {},
  reviewers: [{ name: 'Reviewer', instructions: 'Review it.' }],
};

/** A plan for {@link profile}, publishing from the checkout itself onto `main`. */
export const plan: Plan = {
  repository: 'o/r',
  remote: '.',
  baseBranch: 'main',
  branchPrefix: 'p',
  contract: [{ behavior: 'It pins.', tests: [{ path: 'a/t.txt', name: 'pins', stage: 'a' }] }],
  divergences: [],
  structure: 's',
  interfaces: 'i',
  stages: Object.fromEntries(
    ['a', 'b'].map((s) => [
      s,
      {
        deliverable: `${s} deliverable`,
        paths: [`${s}/`],
        checks: [{ kind: 'test', command: 'echo pins' }],
        title: `t ${s}`,
        body: `b ${s}`,
      },
    ]),
  ),
};
