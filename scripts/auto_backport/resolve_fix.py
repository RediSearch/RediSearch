#!/usr/bin/env python3
"""Resolve PR context, eligibility, and failure logs for the auto-backport
fix flow. See .github/workflows/task-backport_pr-agent-fix.yml.

Invoked from the fix workflow after the master checkout (which is why
this lives in `scripts/` — the file has to exist on the working tree).
Verifies the trigger comment landed on an actual auto-backport PR,
pulls the most recent failed `Pull Request Flow` run on the current
HEAD, tails the failed-step logs, and gathers human-supplied
`/backport-agent-context` hints. Writes a context JSON file to
$RUNNER_TEMP that the Codex agent consumes via
`BACKPORT_FIX_CONTEXT_FILE`.

Env contract (set by the workflow):
- GH_TOKEN, GH_REPO, GITHUB_REPOSITORY -- consumed by `gh` / api paths.
- RUNNER_TEMP, GITHUB_OUTPUT -- GitHub Actions standard.
- PR_NUMBER_FROM_ISSUE -- the backport PR number.
- COMMENT_BODY -- the `/backport-agent-fix [<inline context>]` comment.

Exit codes mirror resolve_create: 0 with skip=true on every "nothing to
do" outcome, 0 with skip=false on success, non-zero only for genuine
programming errors.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


ORIGINAL_PR_RE = re.compile(r"Backport of #(\d+)")
ORIGINAL_SHA_RE = re.compile(r"Merge commit: `([0-9a-f]{7,})`")
BRANCH_PREFIX = "backport-agent/"
LOG_TAIL_LINES = 200

TRUSTED_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

# The login the workflow gives the bot (git identity + the App token's author).
# Every dedup decision keyed off the bot must verify `.user.login == BOT_LOGIN`:
# the marker text below is public, so an untrusted commenter could otherwise
# forge it to suppress or resurface feedback.
BOT_LOGIN = "redis-pr-app[bot]"

# When the bot replies to a general PR comment or a review body it addressed, it
# embeds this marker (see pr-backport-auto-fix.md) naming the exact item it
# handled, e.g. `<!-- backport-agent-addressed: comment:123 -->`. Repeated fix
# runs dedup on these markers rather than a coarse timestamp cutoff: only an item
# the bot actually replied to is filtered out, so feedback an applied fix left
# open (out of scope, or whose reply failed) is preserved for the next run
# instead of being silently dropped once an "applied" summary is posted.
ADDRESSED_MARKER_RE = re.compile(
    r"<!--\s*backport-agent-addressed:\s*(comment|review):(\d+)\s*-->"
)


# ---- helpers -----------------------------------------------------------------


# The first line must be exactly `/backport-agent-fix`, optionally followed by
# whitespace and inline context. Anchored so longer words don't match.
FIX_COMMAND_RE = re.compile(r"^/backport-agent-fix(\s|$)")


def is_fix_command(comment_body: str) -> bool:
    """True iff the first line's command token is exactly `/backport-agent-fix`.

    The workflow's `if:` gate uses `startsWith(body, '/backport-agent-fix')`,
    which also matches longer words like `/backport-agent-fixes`. Those are not
    our command; without this check `strip_inline_context` would strip the
    `/backport-agent-fix` prefix and feed the mangled remainder (`es ...`) to
    the agent as inline context. Require an exact command token instead.
    """
    if not comment_body:
        return False
    return FIX_COMMAND_RE.match(comment_body.splitlines()[0]) is not None


def strip_inline_context(comment_body: str) -> str:
    """Return everything after `/backport-agent-fix` on the first line."""
    if not comment_body:
        return ""
    first_line = comment_body.splitlines()[0]
    return re.sub(r"^/backport-agent-fix\s*", "", first_line)


def parse_canonical_backport_refs(body: str) -> tuple[int | None, str]:
    """Extract (original_pr, original_sha) from a backport PR body.

    The create workflow always writes:
        Backport of #<n> to `<target>`.
        ...
        Merge commit: `<sha>`
    so we anchor on those literals.
    """
    m_pr = ORIGINAL_PR_RE.search(body or "")
    m_sha = ORIGINAL_SHA_RE.search(body or "")
    original_pr = int(m_pr.group(1)) if m_pr else None
    original_sha = m_sha.group(1) if m_sha else ""
    return original_pr, original_sha


def fetch_failed_run(branch: str, head_sha: str) -> dict | None:
    """Most recent `Pull Request Flow` run on (branch, head_sha), iff it
    actually failed.

    Filtering by `--commit head_sha` (not just `--branch`) is important:
    after a new commit lands, the previous failed run is stale and we
    don't want to feed its logs to the agent.

    We deliberately do NOT pre-filter with `--status failure`. A failed
    run that was later rerun successfully on the same commit would still
    match `--status failure --limit 1`, which would feed the agent stale
    logs and tempt it to push an unnecessary "fix" on top of a now-green
    branch. Instead, fetch the latest run regardless of conclusion, then
    return it only if its `conclusion` is `failure` (and `status` is
    `completed`). Anything else — in-progress, success, cancelled,
    timed_out — means there's no failure for us to act on.
    """
    out = common.gh(
        "run", "list",
        "--branch", branch,
        "--commit", head_sha,
        "--workflow", "Pull Request Flow",
        "--limit", "1",
        "--json", "databaseId,url,status,conclusion",
        "--jq", ".[0] // empty",
    )
    s = out.strip()
    if not s:
        return None
    run = json.loads(s)
    if run.get("status") != "completed" or run.get("conclusion") != "failure":
        return None
    # Keep the shape stable for callers — they only need databaseId/url.
    return {"databaseId": run["databaseId"], "url": run["url"]}


def fetch_failed_jobs_and_excerpts(run_id: int) -> tuple[list[str], list[dict]]:
    """Return (failed_job_names, [{job, step, tail}]) for `run_id`.

    Best-effort: gh API hiccups give us an empty list rather than an abort.
    Note this resolve-time capture is the *only* CI-log source the agent
    gets: it runs on the default GITHUB_TOKEN (which has actions:read),
    whereas the agent's App token does not, so the agent cannot re-fetch
    logs itself via `gh run view --log-failed`.

    `actions/runs/{id}/jobs` is paginated (30 per page by default), and
    `Pull Request Flow` runs many matrix jobs. Without `--paginate`,
    failures on later pages are dropped from both `failed_jobs` and
    `log_excerpts`, which would silently hide the actual failing job.
    Use `gh_paginated_array` (the same helper we use for
    /backport-agent-context comments) to stitch pages.
    """
    repo = os.environ["GITHUB_REPOSITORY"]
    jobs = common.gh_paginated_array(
        "api", "-X", "GET", f"repos/{repo}/actions/runs/{run_id}/jobs",
        "--jq", '[.jobs[] | select(.conclusion=="failure") | {name, id}]',
    )

    failed_jobs = [j["name"] for j in jobs]
    excerpts: list[dict] = []
    for j in jobs:
        try:
            log_text = common.gh(
                "api", f"repos/{repo}/actions/jobs/{j['id']}/logs", check=False,
            )
        except Exception:
            continue
        if not log_text:
            continue
        tail = "\n".join(log_text.splitlines()[-LOG_TAIL_LINES:])
        excerpts.append({"job": j["name"], "step": "(see log)", "tail": tail})
    return failed_jobs, excerpts


def fetch_trusted_context_comments(pr: int) -> list[str]:
    """All `/backport-agent-context <text>` bodies authored by write-level
    commenters, stripped of the command prefix.

    The author-association filter (OWNER/MEMBER/COLLABORATOR) is the
    prompt-injection gate — see the trust section of pr-backport-auto-fix.md.
    `gh_paginated_array` quietly returns [] on transient gh failure so a
    bad-network moment doesn't kill the fix run.
    """
    repo = os.environ["GITHUB_REPOSITORY"]
    bodies = common.gh_paginated_array(
        "api", "-X", "GET", f"repos/{repo}/issues/{pr}/comments",
        "--jq",
        "[ .[] "
        '| select(.body | startswith("/backport-agent-context")) '
        "| select(.author_association == \"OWNER\" "
        '     or .author_association == "MEMBER" '
        '     or .author_association == "COLLABORATOR") '
        "| .body ]",
    )
    return [
        re.sub(r"^/backport-agent-context\s*", "", b, count=1)
        for b in bodies if b
    ]


# A general comment starting with this is a slash-command, not reviewer
# feedback: `/backport-agent` / `/backport-agent-fix` are triggers, and
# `/backport-agent-context` is already collected into `context[]`. The bot's own
# output (summaries, `🤖 Re:` replies) is filtered by AUTHOR (== BOT_LOGIN), not
# by body prefix — a `🤖`/`Re:` content check would also drop a maintainer's
# comment that happens to quote the bot's heading.
_COMMAND_PREFIX = "/backport-agent"


# `reviewThreads` is paginated (100 per page); a busy PR can exceed one page.
# `$after` is a nullable cursor — omitted (→ null) on the first call by
# `gh_graphql`, then set from `pageInfo.endCursor` on subsequent pages.
#
# The nested `comments(last:100)` fetches the *most recent* 100 comments of each
# thread rather than the first 100. On a thread with >100 comments that keeps
# `comments[-1]` accurate (it is genuinely the latest comment, so the
# "did the bot already reply?" check below is correct) and keeps the freshest
# reviewer feedback in view; older buried comments matter less for a fix run.
REVIEW_THREADS_QUERY = """
query($owner:String!, $repo:String!, $pr:Int!, $after:String) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$pr) {
      reviewThreads(first:100, after:$after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          comments(last:100) {
            nodes { author { login } authorAssociation body createdAt }
          }
        }
      }
    }
  }
}
"""


def _owner_repo() -> tuple[str, str]:
    """Split `owner/repo` from GITHUB_REPOSITORY."""
    repo = os.environ["GITHUB_REPOSITORY"]
    owner, _, name = repo.partition("/")
    return owner, name


def addressed_feedback(pr: int) -> dict[str, str]:
    """`{"comment:123": "<ack-time>", ...}` — for each feedback item a prior fix
    run replied to, the timestamp of the (latest) bot reply that acknowledged
    it, read back from the acknowledgement markers the bot embedded in its own
    replies.

    Only markers in comments authored by `BOT_LOGIN` count: the marker text is
    public, so gating on the bot author stops an untrusted commenter from
    forging a marker to hide (or, by omission, resurface) feedback. Per-item
    identity is what makes this precise — a single applied fix that handled some
    comments but left others open records markers only for the ones it replied
    to, so the rest survive to the next run.

    The timestamp lets the caller detect a comment that was *edited after* it
    was addressed (its `updated_at` moves past the ack): that revised feedback
    is surfaced again rather than staying hidden behind the stale marker.
    Best-effort: {} on gh failure.
    """
    repo = os.environ["GITHUB_REPOSITORY"]
    rows = common.gh_paginated_array(
        "api", "-X", "GET", f"repos/{repo}/issues/{pr}/comments",
        "--jq",
        f'[ .[] | select(.user.login == "{BOT_LOGIN}") '
        "| {created_at, body} ]",
    )
    acked: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        created = row.get("created_at") or ""
        for kind, num in ADDRESSED_MARKER_RE.findall(row.get("body") or ""):
            token = f"{kind}:{num}"
            if created > acked.get(token, ""):
                acked[token] = created
    return acked


def fetch_unresolved_review_threads(pr: int) -> list[dict]:
    """Unresolved inline review threads whose root comment was authored by a
    write-level user (OWNER/MEMBER/COLLABORATOR), across ALL pages.

    Returns one entry per thread: its GraphQL node id (needed by the agent to
    call `resolveReviewThread`), the `path`/`line` it anchors to, and the
    write-level comment bodies in the thread. The author-association filter is
    the same prompt-injection gate used for `/backport-agent-context` — threads
    started by non-write-level users are dropped here and reach the agent only
    (if at all) as untrusted evidence, never as actionable input.

    A thread qualifies if it holds **any** trusted (write-level) non-empty
    comment — not only when its ROOT comment is trusted. That keeps threads a
    non-write-level user (or a review bot) opened but a maintainer later
    replied to with actionable feedback; the per-comment trust filter still
    drops the untrusted comments themselves.

    Each entry carries `bot_replied_last`: true when the thread's most recent
    comment is the bot's own reply. Such a thread was already addressed by a
    prior run whose `resolveReviewThread` did not stick (a clean resolve would
    have flipped `isResolved`), so it is surfaced as **resolution-only** work
    rather than skipped — the agent retries the resolve without posting a
    duplicate reply (see pr-backport-auto-fix.md). Best-effort: any gh failure
    returns the pages gathered so far rather than aborting the run.
    """
    owner, repo = _owner_repo()
    threads: list[dict] = []
    after: str | None = None
    while True:
        data = common.gh_graphql(
            REVIEW_THREADS_QUERY, owner=owner, repo=repo, pr=pr, after=after,
        )
        try:
            conn = data["repository"]["pullRequest"]["reviewThreads"]
            nodes = conn["nodes"]
        except (TypeError, KeyError):
            break

        for node in nodes or []:
            if node.get("isResolved"):
                continue
            comments = ((node.get("comments") or {}).get("nodes")) or []
            if not comments:
                continue
            bodies = [
                {
                    "author": ((c.get("author") or {}).get("login")) or "",
                    "body": c.get("body") or "",
                }
                for c in comments
                if c.get("authorAssociation") in TRUSTED_ASSOCIATIONS and c.get("body")
            ]
            if not bodies:
                continue
            bot_replied_last = (
                ((comments[-1].get("author") or {}).get("login")) == BOT_LOGIN
            )
            # Newest comment timestamp in this snapshot. The agent re-checks the
            # thread just before resolving and refuses to resolve if a newer
            # non-bot comment has appeared since — otherwise a reviewer follow-up
            # that lands after this snapshot would be auto-closed unseen.
            latest_comment_at = max(
                (c.get("createdAt") or "" for c in comments), default=""
            )
            threads.append({
                "thread_id": node.get("id"),
                "path": node.get("path"),
                "line": node.get("line"),
                "is_outdated": bool(node.get("isOutdated")),
                "bot_replied_last": bot_replied_last,
                "latest_comment_at": latest_comment_at,
                "comments": bodies,
            })

        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        after = page.get("endCursor")
        if not after:
            break
    return threads


def fetch_general_pr_comments(pr: int, acked: dict[str, str]) -> list[dict]:
    """Write-level general (issue-style) PR comments, excluding the bot's own
    output and `/backport-agent*` command comments.

    Same author-association trust gate as the review-thread / context
    collectors. The bot's own comments are dropped by AUTHOR (`== BOT_LOGIN`),
    not by body prefix, so a maintainer's comment that quotes the bot's
    `🤖 …` heading is still surfaced. `/backport-agent*` command comments are
    skipped by prefix (they're triggers, and `/backport-agent-context` is
    already collected into `context[]`). Each entry keeps its `id` and `kind`
    ("comment") so the agent can stamp the acknowledgement marker when it
    replies.

    An item the bot already replied to (its `comment:<id>` token is in `acked`)
    is filtered out — UNLESS the comment was edited after that reply
    (`updated_at` is later than the ack), in which case the revised feedback is
    surfaced again rather than staying hidden behind the stale marker.
    """
    repo = os.environ["GITHUB_REPOSITORY"]
    raw = common.gh_paginated_array(
        "api", "-X", "GET", f"repos/{repo}/issues/{pr}/comments",
        "--jq",
        "[ .[] "
        "| select(.author_association == \"OWNER\" "
        '     or .author_association == "MEMBER" '
        '     or .author_association == "COLLABORATOR") '
        "| {id: .id, author: .user.login, body: .body, updated_at: .updated_at} ]",
    )
    out: list[dict] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        body = c.get("body") or ""
        if not body or (c.get("author") or "") == BOT_LOGIN:
            continue
        if body.startswith(_COMMAND_PREFIX):
            continue
        ack_ts = acked.get(f"comment:{c.get('id')}")
        if ack_ts is not None and (c.get("updated_at") or "") <= ack_ts:
            continue
        out.append({"id": c.get("id"), "kind": "comment",
                    "author": c.get("author") or "", "body": body})
    return out


def fetch_review_bodies(pr: int, acked: dict[str, str]) -> list[dict]:
    """Write-level top-level PR *review* bodies (the summary text of a review,
    e.g. a "Request changes" with no inline comment).

    Such feedback is stored as a pull-request review, not an issue comment or a
    review thread, so neither of the other collectors sees it. Same trust gate
    and per-item `acked` dedup as `fetch_general_pr_comments`; the bot's own
    reviews are dropped by author (`== BOT_LOGIN`). There is no thread to
    resolve — the agent replies via a normal PR comment, exactly as for general
    comments, so entries share the same shape with `kind` set to "review".

    Unlike issue comments, the reviews endpoint exposes no edit timestamp, so a
    review whose `review:<id>` token is in `acked` is suppressed on presence
    alone (review bodies are seldom edited after submission).

    `DISMISSED` (and not-yet-submitted `PENDING`) reviews are excluded: a
    dismissed review is feedback the reviewer explicitly withdrew. A review body
    is also dropped when the same author later submitted an `APPROVED` review
    (GitHub keeps the superseded record un-dismissed): the approval means the
    reviewer is satisfied, so an earlier request-changes/comment body is stale
    and must not be re-applied. A request-changes submitted *after* an approval
    (a fresh review round) is newer than that approval and is kept.
    """
    repo = os.environ["GITHUB_REPOSITORY"]
    # Fetch state + submitted_at too, and do NOT drop empty bodies in jq: an
    # empty-body APPROVED review still supersedes that author's earlier bodies,
    # so we need to see it here.
    raw = common.gh_paginated_array(
        "api", "-X", "GET", f"repos/{repo}/pulls/{pr}/reviews",
        "--jq",
        "[ .[] "
        "| select(.author_association == \"OWNER\" "
        '     or .author_association == "MEMBER" '
        '     or .author_association == "COLLABORATOR") '
        '| select(.state != "DISMISSED" and .state != "PENDING") '
        "| {id: .id, author: .user.login, body: (.body // \"\"), "
        "   state: .state, submitted_at: .submitted_at} ]",
    )
    rows = [r for r in raw if isinstance(r, dict)]

    # Latest APPROVED submission time per author.
    approved_at: dict[str, str] = {}
    for r in rows:
        if r.get("state") == "APPROVED":
            author = r.get("author") or ""
            ts = r.get("submitted_at") or ""
            if ts > approved_at.get(author, ""):
                approved_at[author] = ts

    out: list[dict] = []
    for r in rows:
        body = r.get("body") or ""
        author = r.get("author") or ""
        if not body or author == BOT_LOGIN:
            continue
        if f"review:{r.get('id')}" in acked:
            continue
        # Superseded by a later (or same-time) approval from this author.
        if r.get("state") != "APPROVED" and author in approved_at \
                and (r.get("submitted_at") or "") <= approved_at[author]:
            continue
        out.append({"id": r.get("id"), "kind": "review",
                    "author": author, "body": body})
    return out


# ---- main --------------------------------------------------------------------


def main() -> int:
    pr = (os.environ.get("PR_NUMBER_FROM_ISSUE") or "").strip()
    if not pr:
        common.log("No PR number from issue_comment event; skipping.")
        common.set_output("skip", "true")
        return 0

    comment_body = os.environ.get("COMMENT_BODY", "")
    if not is_fix_command(comment_body):
        # The workflow `if:` gate is a cheap `startsWith` pre-filter that also
        # admits siblings like `/backport-agent-fixes`. Reject anything whose
        # command token isn't exactly `/backport-agent-fix` so we never spin up
        # Codex on a mistyped/unrelated command.
        common.skip("Comment is not exactly the /backport-agent-fix command; skipping.")

    inline_context = strip_inline_context(comment_body)

    pr_data = common.fetch_pr(pr, [
        "number", "headRefName", "baseRefName", "state",
        "title", "body", "headRefOid", "isCrossRepository",
    ])

    state = pr_data.get("state")
    if state != "OPEN":
        common.skip(f"PR #{pr} is not open (state={state}); skipping.")

    if pr_data.get("isCrossRepository"):
        # Our auto-backport PRs are always opened by the App with the
        # head branch in this repo. Refuse cross-repo PRs to avoid
        # checking out fork-controlled refs in a privileged context.
        common.skip(
            f"PR #{pr} is cross-repository; the auto-fix flow does not "
            "touch fork branches. Skipping."
        )

    branch = pr_data["headRefName"]
    base = pr_data["baseRefName"]
    head_sha = pr_data["headRefOid"]

    # The `backport-agent/` branch namespace is the authoritative signal that
    # this PR was opened by our create flow: only that flow, holding the App
    # token, ever pushes these branches — and it does so only after confirming
    # the source PR was merged and refusing fork-sourced PRs. We intentionally
    # do NOT also hard-require the `auto-backport` label: that label is
    # provisioned out-of-band and a create run may legitimately fail to attach
    # it (e.g. the label definition is missing from the repo). Gating on a label
    # the create flow couldn't apply would lock a human out of
    # `/backport-agent-fix` on an otherwise-valid backport PR, which is exactly
    # the case where the fix flow is most useful.
    if not branch.startswith(BRANCH_PREFIX):
        common.skip(
            f"PR #{pr} branch ({branch}) is outside the backport-agent/ "
            "namespace; skipping."
        )

    original_pr, original_sha = parse_canonical_backport_refs(pr_data.get("body") or "")

    run = fetch_failed_run(branch, head_sha)
    run_id = run.get("databaseId") if run else None
    run_url = run.get("url") if run else ""

    failed_jobs: list[str] = []
    excerpts: list[dict] = []
    if run_id is not None:
        failed_jobs, excerpts = fetch_failed_jobs_and_excerpts(run_id)

    context_bodies = fetch_trusted_context_comments(int(pr))
    if inline_context:
        context_bodies.append(inline_context)

    # Write-level reviewer feedback the agent should try to address alongside the
    # CI-failure fix: unresolved inline review threads (which it can mark
    # resolved), plus general PR comments and top-level review bodies (which it
    # can only reply to). Comments/reviews the bot already replied to in a prior
    # run are filtered out per-item via its acknowledgement markers, so a
    # repeated run neither re-replies to handled feedback nor drops feedback it
    # left open; review threads self-dedup via their resolved state and the
    # bot-replied-last flag.
    acked = addressed_feedback(int(pr))
    review_threads = fetch_unresolved_review_threads(int(pr))
    pr_comments = (fetch_general_pr_comments(int(pr), acked)
                   + fetch_review_bodies(int(pr), acked))

    runner_temp = os.environ["RUNNER_TEMP"]
    context_file = os.path.join(runner_temp, "auto-backport-fix-context.json")
    common.write_context(context_file, {
        "pr": int(pr),
        "branch": branch,
        "base_branch": base,
        "head_sha": head_sha,
        "original_pr": original_pr,
        "original_sha": original_sha,
        "run_id": run_id,
        "run_url": run_url,
        "failed_jobs": failed_jobs,
        "log_excerpts": excerpts,
        "context": context_bodies,
        "review_threads": review_threads,
        "pr_comments": pr_comments,
    })

    common.set_output("skip", "false")
    common.set_output("pr", str(pr))
    common.set_output("branch", branch)
    common.set_output("base", base)
    common.set_output("original_sha", original_sha)
    common.set_output("context_file", context_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
