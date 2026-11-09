/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * The Amp plugin: the {@link Host} on Amp threads, and the engine's operations and turn hooks
 * registered with Amp.
 */
import type { PluginAPI, ThreadID } from '@ampcode/plugin';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gitVcs } from '../core/git.ts';
import { github } from '../core/github.ts';
import { engine } from '../core/engine.ts';
import type { Host } from '../core/ports.ts';
import { ci, profile } from '../redisearch/profile.ts';

/** Where the plugin lives in the workspace, and the policy documents of each layer, in order. */
const PLUGIN = '.amp/plugins/rust-port/';
const POLICY = ['core', 'amp', 'redisearch'].map((layer) => `${PLUGIN}${layer}/policy.md`);

const isThread = (id: string): id is ThreadID => id.startsWith('T-');

function threadID(id: string): ThreadID {
  if (!isThread(id)) throw new Error(`${id} is not an Amp thread`);
  return id;
}

/** Workers and reviewers as Amp threads, labelled `name`, with tools named `port_<operation>`. */
export function ampHost(
  amp: Pick<PluginAPI, 'system' | 'threads' | 'createAgent'>,
  name: string,
): Host {
  return {
    threadUrl: (thread) => new URL(`/threads/${thread}`, amp.system.ampURL).toString(),
    toolName: (operation) => `port_${operation}`,

    async startThread({ parent, labels, role }) {
      const agent = role
        ? amp.createAgent({
            extends: 'high',
            instructions: role,
            tools: { include: ['Read', 'Grep', 'glob'], exclude: ['tool_search', 'code_exec'] },
          })
        : await amp.threads.get(threadID(parent)).agent();
      const child = await agent.createThread({ parentThreadID: threadID(parent) });
      // Amp generates thread titles itself; labels are what a plugin can set to name the reviewer.
      await child.addLabels([name, ...labels]).catch(() => {});
      return child.id;
    },

    async sendMessage(thread, message) {
      await amp.threads
        .get(threadID(thread))
        .appendUserMessage({ type: 'user-message', content: message });
    },

    async ask(thread, message, { signal, timeoutMs }) {
      const child = amp.threads.get(threadID(thread));
      const cancel = () => void child.cancel().catch(() => {});
      signal.addEventListener('abort', cancel, { once: true });
      // An abort while the thread was being created or looked up fired before the listener.
      if (signal.aborted) cancel();
      const response = child.waitForResponse({ timeoutMs });
      response.catch(() => {}); // Observed below; this keeps an early failure from going unhandled.
      await child.appendUserMessage({ type: 'user-message', content: message });
      const reply = await response.finally(() => signal.removeEventListener('abort', cancel));
      return reply.content.map((b) => (b.type === 'text' ? b.text : '')).join('\n');
    },
  };
}

export default function (amp: PluginAPI) {
  if (!amp.system.workspaceRoot) return;
  const root = amp.helpers.filePathFromURI(amp.system.workspaceRoot);
  const disposed = new AbortController();
  amp.onDispose(() => disposed.abort());
  // Experimental in Amp, so optional: the job's state at a glance, next to the prompt.
  const statusItem = amp.experimental?.createStatusItem();
  const threads = ampHost(amp, profile.name);
  const port = engine({
    ...{ root, dir: join(root, '.amp', profile.name), profile, host: threads, vcs: gitVcs(root) },
    forge: (repository) => github({ root, repository, ci }),
    policy: POLICY,
    log: (message) => amp.logger.log(message),
    saved: (summary) => statusItem?.update({ text: `${profile.name}: ${summary}` }),
    disposed: disposed.signal,
  });

  // Names the running build, so a fix can be confirmed loaded before a retry.
  readFile(fileURLToPath(import.meta.url))
    .then((code) =>
      port.debug(`plugin loaded: ${createHash('sha1').update(code).digest('hex').slice(0, 10)}`),
    )
    .catch(() => {});

  for (const operation of port.operations)
    amp.registerTool({
      name: threads.toolName(operation.name),
      description: operation.description,
      inputSchema: operation.schema,
      execute: (input, ctx) => operation.run(input, ctx.thread.id, ctx.signal),
    });

  amp.registerCommand(
    'grant',
    {
      title: 'Grant assessments',
      category: profile.name,
      description: 'Operator-only budget grant',
    },
    async (ctx) => {
      const summary = await port.summary();
      if (!summary) return ctx.ui.notify(`No ${profile.name} job in this checkout`);
      const amount = Number(
        await ctx.ui.input({
          title: 'Grant how many more assessments?',
          helpText: summary,
          requireHuman: true,
        }),
      );
      if (amount > 0) await port.grant(amount, amp.system.user?.email ?? 'unknown');
    },
  );

  amp.on('agent.start', async (event) => {
    const content = await port.startTurn(event.thread.id);
    return content ? { message: { content, display: true } } : {};
  });

  amp.on('agent.end', async (event) => {
    if (event.status !== 'done') return;
    const next = await port.endTurn(event.thread.id);
    if (next && 'stop' in next)
      return void amp.ui.notify(`${profile.name} stopped: ${next.stop}`).catch(() => {});
    // The progress check is the bound; Amp's default cap of 5 would stop a healthy job.
    if (next) return { action: 'continue', userMessage: next.continue, maxContinuations: 1000 };
  });
}
