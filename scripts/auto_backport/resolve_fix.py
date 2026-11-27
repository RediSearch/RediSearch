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

    Best-effort: gh API hiccups give us an empty list rather than an
    abort — the agent can call `gh run view "$run_id" --log-failed`
    itself if needed.

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
        "number", "headRefName", "baseRefName", "labels", "state",
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

    labels = [(l.get("name") or "") for l in (pr_data.get("labels") or [])]
    if "auto-backport" not in labels:
        common.skip(f"PR #{pr} does not have the auto-backport label; skipping.")

    branch = pr_data["headRefName"]
    base = pr_data["baseRefName"]
    head_sha = pr_data["headRefOid"]

    # Belt-and-suspenders: branches the agent edits must be ours.
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
