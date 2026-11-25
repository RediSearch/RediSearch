/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * The job's directory: `state.json`, which every change reaches through one queue, and
 * `debug.log`, which records each step.
 */
import { appendFile, mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { State } from './job.ts';

export function stateFile<X>(o: {
  dir: string;
  /** Names the job in the error a thread that does not own it gets. */
  name: string;
  log: (message: string) => void;
  /** Brings a loaded state up to what the engine expects of it. */
  prepare: (state: State<X>) => void;
  saved?: (state: State<X>) => void;
}) {
  const file = join(o.dir, 'state.json');

  /** Goes to the host's log and to `debug.log`, which is easier to follow. */
  function debug(message: string) {
    o.log(message);
    mkdir(o.dir, { recursive: true })
      .then(() => appendFile(join(o.dir, 'debug.log'), `${new Date().toISOString()} ${message}\n`))
      .catch(() => {});
  }

  async function load(): Promise<State<X> | undefined> {
    const text = await readFile(file, 'utf8').catch((error) => {
      if (error.code === 'ENOENT') return undefined;
      throw error;
    });
    if (text === undefined) return;
    const state = JSON.parse(text);
    if (typeof state?.job?.planHash !== 'string')
      throw new Error(`${file} was not written by this version; move it aside to start a job`);
    o.prepare(state);
    return state;
  }

  async function save(state: State<X>) {
    // Written aside and renamed, so a crash never leaves a half-written state.
    await mkdir(o.dir, { recursive: true });
    await writeFile(`${file}.tmp`, JSON.stringify(state, null, 2));
    await rename(`${file}.tmp`, file);
    o.saved?.(state);
  }

  // Every state transition goes through this queue. Hooks check ownership before queueing, so
  // other threads never wait behind an assessment.
  let tail: Promise<unknown> = Promise.resolve();
  function enqueue<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const run = tail.then(async () => {
      const start = Date.now();
      try {
        const result = await fn();
        debug(`${name}: done in ${Date.now() - start}ms`);
        return result;
      } catch (error) {
        debug(`${name}: failed in ${Date.now() - start}ms: ${error}`);
        throw error;
      }
    });
    tail = run.catch(() => {});
    return run;
  }

  return {
    debug,
    load,
    save,
    enqueue,

    /** Hooks must not fail the turn, so an unreadable state means no job. */
    loadQuietly: () =>
      load().catch((error) => {
        debug(`ignoring the state: ${error}`);
        return undefined;
      }),

    /** Runs `fn` in the queue on the job `thread` owns, and saves it after. */
    withOwnedJob<T>(name: string, thread: string, fn: (state: State<X>) => Promise<T>) {
      return enqueue(name, async () => {
        const state = await load();
        if (state?.job.owner !== thread)
          throw new Error(`This thread does not own a ${o.name} job`);
        const result = await fn(state);
        await save(state);
        return result;
      });
    },
  };
}
