/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** The {@link Forge} on GitHub: stage PRs pushed from the checkout, their review threads and CI. */
import { Octokit } from '@octokit/rest';
import { hash, type NewFinding, type FindingSource } from './job.ts';
import type { Forge, Stage } from './ports.ts';
import { git } from './git.ts';

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

/** The start of a GitHub remote URL, before the repository: HTTPS, SSH or scp-style. */
const REMOTE =
  /^(https:\/\/([^@/]+@)?github\.com\/|ssh:\/\/([^@/]+@)?github\.com\/|[^@/:]+@github\.com:)/i;

const FEEDBACK = `query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) { pullRequest(number: $number) {
    state headRefOid url
    reviewThreads(first: 100, after: $after) { pageInfo { hasNextPage endCursor }
      nodes { id isResolved path line
      root: comments(first: 1) { nodes { databaseId } }
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

/** How the repository's CI reports: the check whose verdict decides, and checks that never block. */
export interface CI {
  gate: string;
  advisory: RegExp;
}

/** Bots wrap their comments in HTML comments and collapsed sections the worker never needs. */
function trimComment(body: string, max = 1500): string {
  const text = body
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<details>[\s\S]*?<\/details>/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return text.length > max ? `${text.slice(0, max)}… (full text at the link)` : text;
}

/** Amp configures git's credential helper in Orbs, so this needs nothing beyond git itself. */
async function githubToken(cwd: string): Promise<string> {
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
  ci: CI,
  data: any,
  stage: Stage,
  number: number,
  posted: number[] = [],
  head?: string,
): { findings: NewFinding[]; pending: boolean } {
  const pr = data.repository.pullRequest;
  // The plugin posts with the operator's token, so only the IDs it recorded tell its replies apart.
  const ours = (comment: { databaseId: number }) => posted.includes(comment.databaseId);
  const finding = (
    source: FindingSource,
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
  const failed = (title: string, body: string) => finding('ci', `ci:${stage}`, title, body, pr.url);
  const stale =
    pr.state === 'CLOSED'
      ? failed(`PR #${number} was closed without merging`, 'Reopen it or publish the stage again.')
      : head && pr.headRefOid !== head
        ? failed(
            `PR #${number} head is ${pr.headRefOid.slice(0, 10)}, not the published ${head.slice(0, 10)}`,
            'Something else pushed to its branch; claim the stage again.',
          )
        : undefined;

  const findings: NewFinding[] = [];
  const keys = new Set<string>();
  for (const t of pr.reviewThreads.nodes) {
    const thread = t.comments.nodes;
    const last = thread.at(-1);
    if (t.isResolved || !last || ours(last)) continue;
    const body = thread.map((c: any) => `${c.author?.login}: ${trimComment(c.body)}`).join('\n');
    // Keyed by content, not ID: a stacked PR repeats its ancestors' comments, on other lines. Two
    // threads on one PR with the same text are still two.
    let key = `comment:${hash(
      t.path,
      thread.filter((c: any) => !ours(c)).map((c: any) => c.body),
    )}`;
    if (keys.has(key)) key = `${key}:${t.id}`;
    keys.add(key);
    findings.push({
      ...finding(
        'comment',
        key,
        `review comment on PR #${number} · ${t.path}:${t.line}`,
        body,
        last.url,
      ),
      // GitHub takes replies only to a thread's first comment.
      replies: [{ pr: number, comment: t.root.nodes[0].databaseId, thread: t.id }],
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
      findings.push(
        finding(
          'comment',
          `${kind}:${c.databaseId}:${hash(c.body)}`,
          `${kind} by ${c.author?.login} on PR #${number}`,
          trimComment(c.body),
          c.url,
        ),
      );

  if (stale) return { findings: [...findings, stale], pending: false };

  // The gate reports after the slowest check, so until then a failed check is reported at once
  // under the gate; once it reports, only an explicit success passes.
  const runs = (pr.commits.nodes[0]?.commit.statusCheckRollup?.contexts.nodes ?? []).map(
    (c: any) => ({
      name: c.name ?? c.context,
      result: c.conclusion ?? c.state,
      url: c.detailsUrl ?? c.targetUrl,
    }),
  );
  const gate = runs.find((r: any) => r.name === ci.gate);
  const pending = !gate || gate.result == null || ['PENDING', 'EXPECTED'].includes(gate.result);
  const failures = runs.filter(
    (r: any) => r !== gate && FAILED.includes(r.result) && !ci.advisory.test(r.name),
  );
  const failing = pending ? failures.length > 0 : gate.result !== 'SUCCESS';
  const list =
    failures.map((r: any) => `- ${r.name}: ${r.url}`).join('\n') || 'No failed check is listed.';
  if (failing)
    findings.push(
      finding(
        'ci',
        `ci:${stage}`,
        `CI ${ci.gate} failed on PR #${number}`,
        `${list}\nRead the run log to find which stage the fix belongs in.`,
        gate?.url ?? failures[0]?.url ?? pr.url,
      ),
    );
  return { findings, pending };
}

/** The stage PRs of `repository` on GitHub, pushed from the checkout at `root`. */
export function github(o: { root: string; repository: string; ci: CI }): Forge {
  const { root, repository, ci } = o;
  const [owner, repo] = repository.split('/');
  // Made on first use, so inspecting a plan needs no credentials.
  let client: Promise<Octokit> | undefined;
  const api = () => (client ??= githubToken(root).then((auth) => new Octokit({ auth })));
  const sh = (args: string[]) => git(root, args);

  return {
    async checkRemote({ remote, branches }) {
      const problems: string[] = [];
      const url = await sh(['remote', 'get-url', remote]).catch(() => '');
      const path = url.replace(REMOTE, '');
      if (
        path === url ||
        !new RegExp(`^${repository.replace(/\./g, '\\.')}(\\.git)?/?$`, 'i').test(path)
      )
        problems.push(`remote ${remote} is not github.com/${repository}, where the PRs open`);
      for (const branch of branches)
        await sh(['check-ref-format', '--branch', branch]).catch(() =>
          problems.push(`${branch} is not a valid branch name`),
        );
      return problems;
    },

    async fetchFeedback({ stage, number, posted, head }) {
      const octokit = await api();
      const query = (after?: string) =>
        octokit.graphql<any>(FEEDBACK, { owner, name: repo, number, after });
      const data = await query();
      // Only review threads are paged in full: they are where unresolved feedback lives.
      const threads = data.repository.pullRequest.reviewThreads;
      for (let page = threads; page.pageInfo.hasNextPage; ) {
        page = (await query(page.pageInfo.endCursor)).repository.pullRequest.reviewThreads;
        threads.nodes.push(...page.nodes);
      }
      return readFeedback(ci, data, stage, number, posted, head);
    },

    /** An existing PR keeps its base: GitHub refuses to change it within a stack. */
    async publish(o) {
      const octokit = await api();
      const fetched = o.existing
        ? (await octokit.rest.pulls.get({ owner, repo, pull_number: o.existing.number })).data
        : undefined;
      // A merged or closed PR cannot carry a new candidate: a new one replaces it. A merged one
      // still takes a new description for the candidate it merged, such as the retrospective.
      const current =
        fetched?.state === 'open' || (fetched?.merged && o.existing!.head === o.candidate)
          ? fetched
          : undefined;
      // The lease refuses to overwrite anything but this job's last publication, or, with none, a
      // branch that exists at all. A retry after a successful push, including one whose PR
      // creation then failed, needs only the unfinished metadata update.
      const pushed = current
        ? current.head.sha
        : (await sh(['ls-remote', o.remote, `refs/heads/${o.branch}`])).split('\t')[0];
      if (pushed !== o.candidate) {
        const expected = pushed ? o.existing?.head : '';
        if (expected === undefined)
          throw new Error(
            `${o.branch} has commits this job did not push: choose another branchPrefix or delete it`,
          );
        const lease = `--force-with-lease=${o.branch}:${expected}`;
        await sh(['push', lease, o.remote, `${o.candidate}:refs/heads/${o.branch}`]);
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

    replyToComment: async (pr, comment, body) =>
      (
        await (
          await api()
        ).rest.pulls.createReplyForReviewComment({
          owner,
          repo,
          pull_number: pr,
          comment_id: comment,
          body,
        })
      ).data.id,

    resolveThread: async (thread) => void (await (await api()).graphql(RESOLVE, { id: thread })),
  };
}
