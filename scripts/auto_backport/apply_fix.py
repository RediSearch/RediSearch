#!/usr/bin/env python3
"""Deterministically apply the auto-backport FIX agent's manifest.

Privilege separation companion to apply_create.py. The Codex agent runs with no
GitHub token and no command network: it only makes the fix commit in the pre-made
clone and describes what it did (and which feedback it addressed) in a manifest.
This script holds the write token and performs every mutation — pushing the fix
commit, posting the summary/replies, resolving review threads, editing the PR
body.

The manifest is prompt-injectable, so it is treated as data:
- the push is a plain fast-forward of the exact backport branch (a non-ff /
  history-rewrite push is refused, so the original cherry-pick stays intact);
- a thread can only be replied-to/resolved if its `thread_id` was in the context
  the resolve step handed the agent (it can't touch arbitrary threads);
- a general comment can only be replied-to if its `<kind>:<id>` was in context;
- before resolving, the live thread is re-checked so a reviewer follow-up that
  arrived after the snapshot is never auto-closed unseen.

Env contract (set by task-backport_pr-agent-fix.yml):
- GH_TOKEN, GH_REPO, GITHUB_REPOSITORY -- scoped App token + repo.
- BACKPORT_FIX_CONTEXT_FILE -- resolve_fix.py's context JSON.
- BACKPORT_FIX_MANIFEST_FILE -- the agent's manifest JSON.
- BACKPORT_WORK -- the pre-made clone the agent committed the fix in.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import resolve_fix  # noqa: E402  (BOT_LOGIN, marker format)

BRANCH_PREFIX = "backport-agent/"


def git(work: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", work, *args],
                          capture_output=True, text=True, check=check)


def configure_push_auth(work: str, token: str) -> None:
    header = "AUTHORIZATION: basic " + base64.b64encode(
        f"x-access-token:{token}".encode()).decode()
    git(work, "config", "http.https://github.com/.extraheader", header)


def push_fix(work: str, branch: str) -> tuple[bool, str]:
    """Fast-forward-push the agent's fix commit. Returns (pushed, detail)."""
    if git(work, "rev-parse", "--verify", f"refs/heads/{branch}",
           check=False).returncode != 0:
        return False, "branch missing in clone"
    ahead = git(work, "rev-list", "--count", f"origin/{branch}..{branch}", check=False)
    if ahead.returncode != 0 or ahead.stdout.strip() == "0":
        return False, "no new commit to push"
    # Refuse anything that isn't a clean fast-forward — the original cherry-pick
    # (and any prior fix commits) must stay intact; no history rewrite.
    if git(work, "merge-base", "--is-ancestor", f"origin/{branch}", branch,
           check=False).returncode != 0:
        return False, "refusing non-fast-forward push (history rewrite)"
    push = git(work, "push", "origin", f"refs/heads/{branch}:refs/heads/{branch}",
               check=False)
    if push.returncode != 0:
        return False, f"push failed: {push.stderr.strip()[:200]}"
    short = git(work, "rev-parse", "--short", branch, check=False).stdout.strip()
    return True, short


THREAD_LATEST_QUERY = """
query($id:ID!) {
  node(id:$id) { ... on PullRequestReviewThread {
    comments(last:20) { nodes { createdAt author { login } } }
  } }
}
"""


def newest_non_bot_comment(thread_id: str) -> str:
    data = common.gh_graphql(THREAD_LATEST_QUERY, id=thread_id)
    try:
        nodes = data["node"]["comments"]["nodes"]
    except (TypeError, KeyError):
        return ""
    stamps = [c.get("createdAt") or "" for c in (nodes or [])
              if ((c.get("author") or {}).get("login")) != resolve_fix.BOT_LOGIN]
    return max(stamps) if stamps else ""


def resolve_thread_if_unchanged(thread_id: str, snapshot_ts: str) -> str:
    """Resolve the thread only if no newer non-bot comment appeared since the
    snapshot. Returns a short status for the log."""
    newest = newest_non_bot_comment(thread_id)
    if newest and snapshot_ts and newest > snapshot_ts:
        return "left open (newer reviewer comment since snapshot)"
    common.gh_graphql(
        "mutation($t:ID!){ resolveReviewThread(input:{threadId:$t}){ thread { isResolved } } }",
        t=thread_id)
    return "resolved"


def reply_thread(thread_id: str, body: str) -> None:
    common.gh_graphql(
        "mutation($t:ID!,$b:String!){ addPullRequestReviewThreadReply("
        "input:{pullRequestReviewThreadId:$t, body:$b}){ comment { id } } }",
        t=thread_id, b=body)


def post_pr_comment(pr: int, body: str) -> None:
    f = os.path.join(os.environ["RUNNER_TEMP"], f"fix-comment-{abs(hash(body)) % 10**8}.md")
    Path(f).write_text(body)
    common.gh("pr", "comment", str(pr), "--body-file", f, check=False)


def append_caveats(pr: int, caveats_md: str) -> None:
    cur = common.gh("pr", "view", str(pr), "--json", "body", "--jq", ".body", check=False)
    new = os.path.join(os.environ["RUNNER_TEMP"], "fix-pr-body.md")
    Path(new).write_text((cur.rstrip() + "\n\n" + caveats_md).strip() + "\n")
    common.gh("pr", "edit", str(pr), "--body-file", new, check=False)


def main() -> int:
    ctx = json.loads(Path(os.environ["BACKPORT_FIX_CONTEXT_FILE"]).read_text())
    manifest_path = os.environ["BACKPORT_FIX_MANIFEST_FILE"]
    work = os.environ["BACKPORT_WORK"]
    token = os.environ["GH_TOKEN"]
    pr = int(ctx["pr"])
    branch = ctx["branch"]
    run_url = ctx.get("run_url", "")

    if not os.path.exists(manifest_path):
        common.log("No manifest produced by the agent; nothing to apply.")
        return 0
    m = json.loads(Path(manifest_path).read_text())

    # Manifest must reference the exact backport branch from context.
    if m.get("branch") != branch or not branch.startswith(BRANCH_PREFIX):
        common.log(f"Manifest branch {m.get('branch')!r} != context {branch!r}; aborting.")
        return 1

    # Sets the agent is allowed to act on — anything else is ignored.
    valid_threads = {t.get("thread_id"): (t.get("latest_comment_at") or "")
                     for t in ctx.get("review_threads", []) if t.get("thread_id")}
    valid_comments = {(c.get("kind"), c.get("id")) for c in ctx.get("pr_comments", [])}

    if m.get("action") == "decline":
        d = m.get("decline") or {}
        post_pr_comment(pr, "\n".join([
            "🤖 Auto-backport fix declined", "",
            f"**What I observed:** {d.get('observed', '')}",
            f"**Specific obstacle:** {d.get('obstacle', '')}",
            f"**What the reviewer needs to decide:** {d.get('reviewer_needs', '')}",
            "", f"Failed run: {run_url}"]))
        common.log("Agent declined; posted decline comment.")
        return 0

    configure_push_auth(work, token)
    pushed, detail = push_fix(work, branch)
    if not pushed and detail not in ("no new commit to push",):
        common.log(f"Fix push not applied: {detail}")

    # Summary comment (built here from manifest data, not agent-authored prose-as-command).
    if pushed:
        post_pr_comment(pr, "\n".join([
            "🤖 Auto-backport fix attempt", "",
            f"**Root cause:** {m.get('root_cause', '')}",
            f"**Change:** {m.get('change_summary', '')}",
            f"**Files touched:** {', '.join(m.get('files_touched') or [])}",
            f"**Kind of fix:** {m.get('kind', '')}",
            "", f"Pushed as {detail} on top of the existing branch. CI will re-run.",
            f"Failed run: {run_url}"]))
        if m.get("kind") == "scope-adapting" and m.get("caveats_markdown"):
            append_caveats(pr, m["caveats_markdown"])

    # Review threads the agent addressed: reply (agent text) + resolve with guard.
    for tr in m.get("thread_replies") or []:
        tid = tr.get("thread_id")
        if tid not in valid_threads:
            common.log(f"Ignoring thread_reply for out-of-context thread {tid!r}")
            continue
        reply_thread(tid, f"🤖 {tr.get('body', '').strip()}")
        common.log(f"thread {tid}: {resolve_thread_if_unchanged(tid, valid_threads[tid])}")

    # Threads a prior run replied to but didn't resolve — retry resolve only.
    for tid in m.get("resolve_only_threads") or []:
        if tid not in valid_threads:
            continue
        common.log(f"thread {tid} (resolution-only): {resolve_thread_if_unchanged(tid, valid_threads[tid])}")

    # General comments / review bodies the agent addressed: reply + ack marker.
    for cr in m.get("comment_replies") or []:
        kind, cid = cr.get("kind"), cr.get("id")
        if (kind, cid) not in valid_comments:
            common.log(f"Ignoring comment_reply for out-of-context item {kind}:{cid}")
            continue
        marker = f"<!-- backport-agent-addressed: {kind}:{cid} -->"
        post_pr_comment(pr, f"🤖 {cr.get('body', '').strip()}\n\n{marker}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
