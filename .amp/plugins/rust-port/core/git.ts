/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** The {@link Vcs} on a git checkout. */
import { execa } from 'execa';
import { ENV } from './checks.ts';
import type { Vcs } from './ports.ts';

export async function git(cwd: string, args: string[], input = ''): Promise<string> {
  return (await execa('git', args, { cwd, env: ENV, input })).stdout;
}

export function gitVcs(root: string): Vcs {
  const sh = (args: string[], input?: string) => git(root, args, input);
  const dirty = async () => !!(await sh(['status', '--porcelain']));
  return {
    async currentRevision() {
      if (await dirty()) throw new Error('The working tree is dirty');
      return sh(['rev-parse', 'HEAD']);
    },
    resolveRevision: (revision) => sh(['rev-parse', '--verify', `${revision}^{commit}`]),
    async fetchBranch(remote, branch) {
      await sh(['fetch', remote, branch]);
      return sh(['rev-parse', 'FETCH_HEAD']);
    },
    mergeBase: (a, b) => sh(['merge-base', a, b]),
    diff: (from, to, o) =>
      sh([
        ...['diff', '--no-ext-diff', o?.renames ? '--find-renames' : '--no-renames', from, to],
        ...(o?.paths ? ['--', ...o.paths] : []),
      ]),
    readFileAt: (revision, path) => sh(['show', `${revision}:${path}`]).catch(() => undefined),
    listFiles: async (revision) =>
      (await sh(['ls-tree', '-r', '--name-only', revision])).split('\n').filter(Boolean),
    // Hunk line counts written by a model are often off; --recount ignores them.
    applyPatch: (patch, o) =>
      sh(['apply', '--recount', ...(o?.reverse ? ['-R'] : [])], patch).then(
        () => true,
        () => false,
      ),
    async discardChanges() {
      if (!(await dirty())) return false;
      await sh(['restore', '--source=HEAD', '--staged', '--worktree', '--', '.']);
      await sh(['clean', '-fd']);
      return true;
    },
  };
}
