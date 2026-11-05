/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import { Octokit } from '@octokit/rest';
import { git } from './checks.ts';
import type { Stage } from './plan.ts';
import { ADVISORY, CI_GATE } from './redisearch.ts';
import { hash, type NewFinding, type Source } from './state.ts';

// STALE is a check GitHub gave up on: it never passed.
const FAILED = [
  'FAILURE',
  'ERROR',
  'TIMED_OUT',
  'CANCELLED',
  'ACTION_REQUIRED',
  'STARTUP_FAILURE',
  'STALE',
];

const FEEDBACK = `query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) { pullRequest(number: $number) {
    state headRefOid url
    reviewThreads(first: 100, after: $after) { pageInfo { hasNextPage endCursor }
      nodes { id isResolved path line
      comments(last: 20) { nodes { databaseId author { login } body url } } } }
    reviews(last: 50) { nodes { databaseId author { login __typename } body url
      comments { totalCount } } }
    comments(last: 50) { nodes { databaseId author { login __typename } body url } }
    commits(last: 1) { nodes { commit { statusCheckRollup { contexts(first: 100) { nodes {
      ... on CheckRun { name conclusion detailsUrl }
      ... on StatusContext { context state targetUrl } } } } } } }
  } }
}`;

const RESOLVE = `mutation($id: ID!) {
  resolveReviewThread(input: { threadId: $id }) { clientMutationId }
}`;

/** Bots wrap their comments in HTML comments and collapsed sections the worker never needs. */
function trim(body: string, max = 1500): string {
  const text = body
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<details>[\s\S]*?<\/details>/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return text.length > max ? `${text.slice(0, max)}… (full text at the link)` : text;
}

/** Amp configures git's credential helper in Orbs, so this needs nothing beyond git itself. */
async function token(cwd: string): Promise<string> {
  const env = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
  if (env) return env;
  const fill = await git(cwd, ['credential', 'fill'], 'protocol=https\nhost=github.com\n\n');
  const password = fill.match(/^password=(.*)$/m)?.[1];
  if (!password) throw new Error('git has no credentials for github.com');
  return password;
}

/**
 * The review comments and the CI verdict of a stage PR, from the {@link FEEDBACK} query; nothing
 * once it merged. `posted` are the comments the plugin wrote; `head` is the revision it published.
 */
export function readFeedback(
  data: any,
  stage: Stage,
  number: number,
  posted: number[] = [],
  head?: string,
) {
  const pr = data.repository.pullRequest;
  // The plugin posts with the operator's token, so only the IDs it recorded tell its replies apart.
  const ours = (comment: { databaseId: number }) => posted.includes(comment.databaseId);
  const finding = (
    source: Source,
    key: string,
    title: string,
    body: string,
    url: string,
  ): NewFinding => ({
    ...{ source, key, title, body, stage, revision: pr.headRefOid },
    ...{ blocking: true, links: [url] },
  });
  // A PR closed unmerged, or one whose head moved past what the job published, no longer carries
  // the assessed candidate, so its CI says nothing about it. Its comments are still heard.
  const ci = (title: string, body: string) => finding('ci', `ci:${stage}`, title, body, pr.url);
  const stale =
    pr.state === 'CLOSED'
      ? ci(`PR #${number} was closed without merging`, 'Reopen it or publish the stage again.')
      : head && pr.headRefOid !== head
        ? ci(
            `PR #${number} head is ${pr.headRefOid.slice(0, 10)}, not the published ${head.slice(0, 10)}`,
            'Something else pushed to its branch; claim the stage again.',
          )
        : undefined;

  const comments: NewFinding[] = [];
  const keys = new Set<string>();
  for (const t of pr.reviewThreads.nodes) {
    const thread = t.comments.nodes;
    const last = thread.at(-1);
    if (t.isResolved || !last || ours(last)) continue;
    const body = thread.map((c: any) => `${c.author?.login}: ${trim(c.body)}`).join('\n');
    // Keyed by content, not ID: a stacked PR repeats its ancestors' comments, on other lines. Two
    // threads on one PR with the same text are still two.
    let key = `comment:${hash(
      t.path,
      thread.filter((c: any) => !ours(c)).map((c: any) => c.body),
    )}`;
    if (keys.has(key)) key = `${key}:${t.id}`;
    keys.add(key);
    comments.push({
      ...finding(
        'comment',
        key,
        `review comment on PR #${number} · ${t.path}:${t.line}`,
        body,
        last.url,
      ),
      replies: [{ pr: number, comment: last.databaseId, thread: t.id }],
    });
  }
  // People are always heard. Bots are heard in review threads only: their review bodies summarize
  // those threads, and their conversation comments are reports CI already covers. The body is part
  // of the key, so an edited comment is a new finding.
  const human = (c: any) => c.body.trim() && !ours(c) && c.author?.__typename !== 'Bot';
  for (const [kind, nodes] of [
    ['review', pr.reviews.nodes],
    ['comment', pr.comments.nodes],
  ] as const)
    for (const c of nodes.filter(human))
      comments.push(
        finding(
          'comment',
          `${kind}:${c.databaseId}:${hash(c.body)}`,
          `${kind} by ${c.author?.login} on PR #${number}`,
          trim(c.body),
          c.url,
        ),
      );

  if (stale) return { comments, pending: false, failures: [stale] };

  // The gate reports after the slowest check, so until then a failed check is reported at once
  // under the gate; once it reports, only an explicit success passes.
  const runs = (pr.commits.nodes[0]?.commit.statusCheckRollup?.contexts.nodes ?? []).map(
    (c: any) => ({
      name: c.name ?? c.context,
      result: c.conclusion ?? c.state,
      url: c.detailsUrl ?? c.targetUrl,
    }),
  );
  const gate = runs.find((r: any) => r.name === CI_GATE);
  const pending = !gate || gate.result == null || ['PENDING', 'EXPECTED'].includes(gate.result);
  const failed = runs.filter(
    (r: any) => r !== gate && FAILED.includes(r.result) && !ADVISORY.test(r.name),
  );
  const failing = pending ? failed.length > 0 : gate.result !== 'SUCCESS';
  const list =
    failed.map((r: any) => `- ${r.name}: ${r.url}`).join('\n') || 'No failed check is listed.';
  const failures = failing
    ? [
        finding(
          'ci',
          `ci:${stage}`,
          `CI ${CI_GATE} failed on PR #${number}`,
          `${list}\nRead the run log to find which stage the fix belongs in.`,
          gate?.url ?? failed[0]?.url ?? pr.url,
        ),
      ]
    : [];
  return { comments, pending, failures };
}

export async function github(cwd: string, repository: string) {
  const [owner, repo] = repository.split('/');
  const octokit = new Octokit({ auth: await token(cwd) });

  return {
    async feedback(stage: Stage, number: number, posted: number[], head: string) {
      const query = (after?: string) =>
        octokit.graphql<any>(FEEDBACK, { owner, name: repo, number, after });
      const data = await query();
      // Only review threads are paged in full: they are where unresolved feedback lives.
      const threads = data.repository.pullRequest.reviewThreads;
      for (let page = threads; page.pageInfo.hasNextPage; ) {
        page = (await query(page.pageInfo.endCursor)).repository.pullRequest.reviewThreads;
        threads.nodes.push(...page.nodes);
      }
      return readFeedback(data, stage, number, posted, head);
    },

    /**
     * Pushes `candidate` to `branch` and opens the PR from it into `base`, or updates it. An
     * existing PR keeps its base: GitHub refuses to change it within a stack.
     */
    async publish(o: {
      remote: string;
      branch: string;
      base: string;
      candidate: string;
      title: string;
      body: string;
      existing?: { number: number; head: string };
    }) {
      const fetched = o.existing
        ? (await octokit.rest.pulls.get({ owner, repo, pull_number: o.existing.number })).data
        : undefined;
      // A merged or closed PR cannot carry a new candidate: a new one replaces it.
      const current = fetched?.state === 'open' ? fetched : undefined;
      // The lease refuses to overwrite anything but this job's last publication, or, with none, a
      // branch that exists at all. A retry after a successful push, including one whose PR
      // creation then failed, needs only the unfinished metadata update.
      const pushed = current
        ? current.head.sha
        : (await git(cwd, ['ls-remote', o.remote, `refs/heads/${o.branch}`])).split('\t')[0];
      if (pushed !== o.candidate) {
        const expected = pushed ? o.existing?.head : '';
        if (expected === undefined)
          throw new Error(
            `${o.branch} has commits this job did not push: choose another branchPrefix or delete it`,
          );
        const lease = `--force-with-lease=${o.branch}:${expected}`;
        await git(cwd, ['push', lease, o.remote, `${o.candidate}:refs/heads/${o.branch}`]);
      }
      const fields = { owner, repo, title: o.title, body: o.body };
      // A PR created before the plugin could record it, say by a run that died, is adopted.
      const open =
        current ??
        (
          await octokit.rest.pulls.list({
            owner,
            repo,
            state: 'open',
            head: `${owner}:${o.branch}`,
          })
        ).data[0];
      const { data: pr } = open
        ? await octokit.rest.pulls.update({ ...fields, pull_number: open.number })
        : await octokit.rest.pulls.create({ ...fields, base: o.base, head: o.branch });
      return { number: pr.number, url: pr.html_url, head: o.candidate };
    },

    reply: (pr: number, comment: number, body: string) =>
      octokit.rest.pulls.createReplyForReviewComment({
        owner,
        repo,
        pull_number: pr,
        comment_id: comment,
        body,
      }),

    resolve: (thread: string) => octokit.graphql(RESOLVE, { id: thread }),
  };
}

export type GitHub = Awaited<ReturnType<typeof github>>;
