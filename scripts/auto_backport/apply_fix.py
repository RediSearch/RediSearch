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
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import resolve_fix  # noqa: E402  (BOT_LOGIN, marker format)

BRANCH_PREFIX = "backport-agent/"

# APPLY_DRY_RUN=1 prints the push / comment / resolve it WOULD do without any
# write (read-only checks still run). See apply_create.py.
DRY_RUN = os.environ.get("APPLY_DRY_RUN") == "1"


def git(work: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", work, *args],
                          capture_output=True, text=True, check=check,
                          env=common.SAFE_GIT_ENV)


def configure_push_auth(work: str, token: str) -> None:
    if DRY_RUN:
        return
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
    short = git(work, "rev-parse", "--short", branch, check=False).stdout.strip()
    if DRY_RUN:
        common.log(f"[dry-run] would fast-forward-push {branch} ({short})")
        return True, short
    # `--no-verify` + the sanitized `.git` (done in main) keep any agent-installed
    # hook/config from running during this token-holding push.
    push = git(work, "push", "--no-verify", "origin",
               f"refs/heads/{branch}:refs/heads/{branch}", check=False)
    if push.returncode != 0:
        return False, f"push failed: {push.stderr.strip()[:200]}"
    return True, short


THREAD_LATEST_QUERY = """
query($id:ID!) {
  node(id:$id) { ... on PullRequestReviewThread {
    comments(last:20) { nodes { createdAt author { login } } }
  } }
}
"""


def newest_non_bot_comment(thread_id: str) -> str | None:
    """Timestamp of the thread's newest NON-bot comment. `""` means the thread
    genuinely has no non-bot comment; `None` means the live read failed or came
    back malformed (unverifiable) — the caller must fail closed on `None`."""
    data = common.gh_graphql(THREAD_LATEST_QUERY, id=thread_id)
    try:
        nodes = data["node"]["comments"]["nodes"]
    except (TypeError, KeyError):
        return None
    stamps = [c.get("createdAt") or "" for c in (nodes or [])
              if ((c.get("author") or {}).get("login")) != resolve_fix.BOT_LOGIN]
    return max(stamps) if stamps else ""


def resolve_thread_if_unchanged(thread_id: str, snapshot_ts: str) -> str:
    """Resolve the thread only if we can VERIFY no newer non-bot comment appeared
    since the snapshot. Fails closed: an unverifiable live read leaves it open."""
    newest = newest_non_bot_comment(thread_id)
    if newest is None:
        return "left open (could not verify latest comment)"
    if newest and snapshot_ts and newest > snapshot_ts:
        return "left open (newer reviewer comment since snapshot)"
    if DRY_RUN:
        return "[dry-run] would resolve"
    common.gh_graphql(
        "mutation($t:ID!){ resolveReviewThread(input:{threadId:$t}){ thread { isResolved } } }",
        t=thread_id)
    return "resolved"


def reply_thread(thread_id: str, body: str) -> None:
    if DRY_RUN:
        common.log(f"[dry-run] would reply on thread {thread_id}: {body[:80]}")
        return
    common.gh_graphql(
        "mutation($t:ID!,$b:String!){ addPullRequestReviewThreadReply("
        "input:{pullRequestReviewThreadId:$t, body:$b}){ comment { id } } }",
        t=thread_id, b=body)


def _tmp_body_file(text: str) -> str:
    """Write `text` to a fresh temp file with an OS-chosen name (no path built
    from user-controlled data) and return the path."""
    fd, path = tempfile.mkstemp(suffix=".md", dir=os.environ.get("RUNNER_TEMP") or None)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    return path


def post_pr_comment(pr: int, body: str) -> None:
    if DRY_RUN:
        common.log(f"[dry-run] would comment on PR #{pr}:\n{body}")
        return
    common.gh("pr", "comment", str(pr), "--body-file", _tmp_body_file(body), check=False)


def append_caveats(pr: int, caveats_md: str) -> None:
    if DRY_RUN:
        common.log(f"[dry-run] would append caveats to PR #{pr} body")
        return
    # Abort if the current body can't be read: check=True raises on failure so a
    # transient `gh pr view` error can't turn into overwriting the whole PR
    # description with only the caveats text.
    try:
        cur = common.gh("pr", "view", str(pr), "--json", "body", "--jq", ".body", check=True)
    except Exception:
        common.log("Could not read current PR body; skipping caveats append.")
        return
    common.gh("pr", "edit", str(pr), "--body-file",
              _tmp_body_file((cur.rstrip() + "\n\n" + caveats_md).strip() + "\n"), check=False)


def main() -> int:
    ctx = json.loads(Path(os.environ["BACKPORT_FIX_CONTEXT_FILE"]).read_text())
    manifest_path = os.environ["BACKPORT_FIX_MANIFEST_FILE"]
    work = os.environ["BACKPORT_WORK"]
    token = os.environ.get("GH_TOKEN", "")
    pr = int(ctx["pr"])
    branch = ctx["branch"]
    run_url = ctx.get("run_url", "")

    if not os.path.exists(manifest_path):
        # Past the skip gate but no manifest — a model/tool interruption or
        # output-contract miss. Fail loudly rather than a green no-op.
        common.log("ERROR: agent produced no manifest; failing the run.")
        return 1
    m = json.loads(Path(manifest_path).read_text())

    # Manifest must reference the exact backport branch from context.
    if m.get("branch") != branch or not branch.startswith(BRANCH_PREFIX):
        common.log(f"Manifest branch {m.get('branch')!r} != context {branch!r}; aborting.")
        return 1

    # Sets the agent is allowed to act on — anything else is ignored. The
    # resolution-only allow-list is built ONLY from threads the context marked
    # `bot_replied_last`, so the agent can't resolve an unaddressed thread by
    # listing it under resolve_only_threads.
    valid_threads = {t.get("thread_id"): (t.get("latest_comment_at") or "")
                     for t in ctx.get("review_threads", []) if t.get("thread_id")}
    resolve_only_allowed = {t.get("thread_id")
                            for t in ctx.get("review_threads", [])
                            if t.get("thread_id") and t.get("bot_replied_last")}
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

    # Treat the agent's clone `.git` as hostile before the token-holding push.
    if not DRY_RUN:
        common.sanitize_git_dir(work, os.environ["GITHUB_REPOSITORY"])
    configure_push_auth(work, token)
    pushed, detail = push_fix(work, branch)
    if not pushed:
        common.log(f"Fix not pushed: {detail}")

    # Feedback that claims "addressed in code" is only honored if the code
    # actually reached the PR (the push succeeded); otherwise we'd resolve
    # threads / stamp acks for changes that were never pushed.
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

        for tr in m.get("thread_replies") or []:
            tid = tr.get("thread_id")
            if tid not in valid_threads:
                common.log(f"Ignoring thread_reply for out-of-context thread {tid!r}")
                continue
            reply_thread(tid, f"🤖 {tr.get('body', '').strip()}")
            common.log(f"thread {tid}: {resolve_thread_if_unchanged(tid, valid_threads[tid])}")

        for cr in m.get("comment_replies") or []:
            kind, cid = cr.get("kind"), cr.get("id")
            if (kind, cid) not in valid_comments:
                common.log(f"Ignoring comment_reply for out-of-context item {kind}:{cid}")
                continue
            marker = f"<!-- backport-agent-addressed: {kind}:{cid} -->"
            post_pr_comment(pr, f"🤖 {cr.get('body', '').strip()}\n\n{marker}")

    # Resolution-only retries are independent of this run's push (they finish a
    # PRIOR run's already-pushed fix), but are restricted to bot-replied threads.
    for tid in m.get("resolve_only_threads") or []:
        if tid not in resolve_only_allowed:
            common.log(f"Ignoring resolve_only for non-bot-replied/out-of-context thread {tid!r}")
            continue
        common.log(f"thread {tid} (resolution-only): {resolve_thread_if_unchanged(tid, valid_threads[tid])}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
