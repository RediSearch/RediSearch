#!/usr/bin/env python3
"""Deterministically apply the auto-backport CREATE agent's manifest.

Privilege separation: the Codex agent runs with **no GitHub token and no command
network** — all it does is cherry-pick each target onto a local branch in the
pre-made clone and describe the result in a manifest JSON. This script is the
only component that holds the write token; it performs every mutation (push,
`gh pr create`, labels, the summary comment) from that manifest.

Because the manifest is authored by a prompt-injectable agent, every field is
treated as **data**, never as a command: branch names must match the exact
`backport-agent/pr-<pr>-to-<target>` shape, each target must be one the resolve
step actually resolved, PR title/body are built here from templates (the agent
only supplies the free-text conflict log, which lands in the PR body), and every
`git`/`gh` call is made with argument arrays — never shell-interpolated strings.

Env contract (set by task-backport_pr-agent.yml):
- GH_TOKEN, GH_REPO, GITHUB_REPOSITORY -- the scoped App token + repo, for `gh`.
- BACKPORT_CONTEXT_FILE -- the resolve step's context JSON (pr, sha, title, url,
  body, targets).
- BACKPORT_MANIFEST_FILE -- the agent's manifest JSON.
- BACKPORT_WORK -- the pre-made git clone the agent cherry-picked in.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

# APPLY_DRY_RUN=1 makes this print the push / PR / label / comment it WOULD do
# without performing any write (read-only pre-flight still runs). Lets the whole
# resolve -> cherry-pick -> apply pipeline be exercised locally, and is a safe
# operational plan mode.
DRY_RUN = os.environ.get("APPLY_DRY_RUN") == "1"

# Same release-branch shape the resolve step validates targets against; a branch
# name is only ever `backport-agent/pr-<pr>-to-<target>` with a valid target.
TARGET_RE = re.compile(r"^\d{1,4}\.\d{1,4}(?:-[A-Za-z0-9._-]{1,64})?$")
VALID_STATUSES = {"clean", "conflicts", "skipped"}


def git(work: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run `git -C <work> <args>` with an argument array (no shell), under the
    sanitized env that ignores agent-writable global/system git config."""
    return subprocess.run(["git", "-C", work, *args],
                          capture_output=True, text=True, check=check,
                          env=common.SAFE_GIT_ENV)


def configure_push_auth(work: str, token: str) -> None:
    """Wire the App token as the push credential for the clone's origin.

    GitHub App tokens must be presented as HTTP basic `x-access-token:<token>`;
    a bearer header is rejected. Done here (not in the agent) so the token never
    enters the agent's environment.
    """
    if DRY_RUN:
        return
    import base64
    header = "AUTHORIZATION: basic " + base64.b64encode(
        f"x-access-token:{token}".encode()).decode()
    git(work, "config", "http.https://github.com/.extraheader", header)


def branch_for(pr: int, target: str) -> str:
    return f"backport-agent/pr-{pr}-to-{target}"


def release_notes_block(original_body: str) -> str:
    """Replicate the original PR's release-notes checkbox state (exactly one is
    checked, per the PR template). Falls back to 'requires' — the conservative
    choice that forces a human to reconsider — if neither line is found checked.
    """
    requires = "- [ ] This PR requires release notes"
    does_not = "- [ ] This PR does not require release notes"
    body = original_body or ""
    if re.search(r"^\s*-\s*\[[xX]\]\s*This PR requires release notes", body, re.M):
        requires = "- [x] This PR requires release notes"
    elif re.search(r"^\s*-\s*\[[xX]\]\s*This PR does not require release notes", body, re.M):
        does_not = "- [x] This PR does not require release notes"
    else:
        requires = "- [x] This PR requires release notes"
    return f"{requires}\n{does_not}"


def pr_body(ctx: dict, entry: dict) -> str:
    """Build the backport PR body from templates + the agent's conflict log."""
    pr, sha = ctx["pr"], ctx["sha"]
    lines = [
        f"Backport of #{pr} to `{entry['target']}`.",
        "",
        "## Original PR",
        f"- Title: {ctx.get('title', '')}",
        f"- Link: {ctx.get('url', '')}",
        f"- Merge commit: `{sha}`",
        "",
        "## Cherry-pick result",
    ]
    cl = entry.get("conflict_log")
    conflict_log = cl if isinstance(cl, list) else []
    if entry.get("status") == "conflicts" and conflict_log:
        lines.append(f"- Resolved {len(conflict_log)} conflict(s) — see Conflict Log below.")
        lines += ["", "## Conflict Log"]
        for c in conflict_log:
            if not isinstance(c, dict):
                continue
            lines += [
                f"### `{c.get('path', '')}`",
                f"- **Conflict:** {c.get('conflict', '')}",
                f"- **Why it conflicted:** {c.get('why', '')}",
                f"- **Resolution:** {c.get('resolution', '')}",
                f"- **Rationale:** {c.get('rationale', '')}",
            ]
    else:
        lines.append("- Clean cherry-pick — no conflicts.")
    lines += ["", "## Release notes", "", release_notes_block(ctx.get("body", "")),
              "", "🤖 Generated by the auto-backport workflow."]
    return "\n".join(lines)


def existing_pr(branch: str) -> str | None:
    """`<state> <url>` of a same-repo PR already on this head branch, else None.

    Filters to `isCrossRepository == false` so a fork PR reusing the predictable
    branch name can't make us treat the slot as taken (or skip a real backport).
    """
    out = common.gh(
        "pr", "list", "--head", branch, "--state", "all", "--limit", "100",
        "--json", "url,state,isCrossRepository",
        "--jq", '[.[] | select(.isCrossRepository == false)] | .[0] | select(.) '
                '| "\\(.state) \\(.url)"',
        check=False,
    )
    return out.strip() or None


def apply_target(ctx: dict, work: str, entry: dict) -> dict:
    """Push + open the PR for one manifest entry. Returns a summary row."""
    pr = ctx["pr"]
    target = entry.get("target")
    status = entry.get("status")
    # `error` = a validation/apply failure that must fail the run (and is NOT a
    # valid coverage outcome); `skipped` is reserved for legit no-ops (an explicit
    # agent skip, a missing target branch, an already-open PR). Type-check the
    # (agent-authored) entry so a malformed field is an error row, never a crash
    # and never a silently-"covered" target.
    row = {"target": target if isinstance(target, str) else str(target),
           "status": "error", "detail": ""}

    if not isinstance(target, str) or not TARGET_RE.match(target) or target not in ctx["targets"]:
        row["detail"] = f"invalid/unknown target {target!r}"
        return row
    if not isinstance(status, str) or status not in VALID_STATUSES:
        row["detail"] = f"invalid status {status!r}"
        return row
    if status == "skipped":
        row["status"] = "skipped"
        row["detail"] = entry.get("reason", "skipped by agent")
        return row

    branch = branch_for(pr, target)
    if entry.get("branch") != branch:
        row["detail"] = f"branch {entry.get('branch')!r} != expected {branch!r}"
        return row

    # The agent must actually have produced the local branch.
    if git(work, "rev-parse", "--verify", f"refs/heads/{branch}", check=False).returncode != 0:
        row["detail"] = "agent did not produce the branch"
        return row
    # Target must still exist on origin — a legit skip, not an error.
    if git(work, "ls-remote", "--exit-code", "--heads", "origin", target,
           check=False).returncode != 0:
        row["status"] = "skipped"
        row["detail"] = f"no such branch {target}"
        return row
    # Idempotency: never re-open / force over an existing backport PR — legit skip.
    if (ex := existing_pr(branch)):
        row["status"] = "skipped"
        row["detail"] = f"already {ex}"
        return row

    title = f"[{target}] {ctx.get('title', '')}"
    labels = ["auto-backport"] + (["auto-backport-conflicts"] if status == "conflicts" else [])
    cl = entry.get("conflict_log")
    n_conflicts = len(cl) if isinstance(cl, list) else 0
    status_label = f"conflicts({n_conflicts})" if status == "conflicts" else "clean"

    if DRY_RUN:
        row["status"] = status_label
        row["detail"] = f"[dry-run] would push {branch}, open PR '{title}', labels {labels}"
        return row

    # Push the agent's branch — plain (non-force) push of a fresh ref only.
    # `--no-verify` + the sanitized `.git` (done once in main) mean no
    # agent-installed hook/config runs during this token-holding push.
    push = git(work, "push", "--no-verify", "origin",
               f"refs/heads/{branch}:refs/heads/{branch}", check=False)
    if push.returncode != 0:
        row["detail"] = f"push failed: {push.stderr.strip()[:200]}"
        return row
    pushed_sha = git(work, "rev-parse", branch, check=False).stdout.strip()

    body_file = os.path.join(os.environ["RUNNER_TEMP"], f"backport-body-{target}.md")
    with open(body_file, "w") as f:
        f.write(pr_body(ctx, entry))
    created = common.gh("pr", "create", "--base", target, "--head", branch,
                        "--title", title, "--body-file", body_file, check=False)
    url = created.strip().splitlines()[-1] if created.strip() else ""
    if not url.startswith("http"):
        # PR creation failed after the push — delete the just-created remote ref
        # so it isn't left orphaned. Lease the delete to the SHA we pushed
        # (`--force-with-lease`) so that if something updated the branch in the
        # meantime the delete is refused rather than dropping that newer commit.
        deleted = git(work, "push", "--no-verify",
                      f"--force-with-lease=refs/heads/{branch}:{pushed_sha}",
                      "origin", f":refs/heads/{branch}", check=False)
        row["status"] = "error"
        row["detail"] = ("gh pr create failed; pushed branch deleted"
                         if deleted.returncode == 0
                         else "gh pr create failed; branch changed concurrently, left in place")
        return row

    for label in labels:
        common.gh("pr", "edit", url, "--add-label", label, check=False)

    row["status"] = status_label
    row["detail"] = url
    return row


def summary_comment(pr: int, sha: str, rows: list[dict]) -> str:
    lines = ["🤖 Auto-backport summary", "", "| Target | Status | Backport PR |",
             "|---|---|---|"]
    for r in rows:
        detail = r["detail"]
        cell = f"#{detail.rsplit('/', 1)[-1]}" if detail.startswith("http") else detail
        lines.append(f"| `{r['target']}` | {r['status']} | {cell} |")
    lines += ["", f"(PR #{pr}, `{sha[:9]}` — generated by the auto-backport workflow.)"]
    return "\n".join(lines)


def main() -> int:
    ctx = json.loads(Path(os.environ["BACKPORT_CONTEXT_FILE"]).read_text())
    manifest_path = os.environ["BACKPORT_MANIFEST_FILE"]
    work = os.environ["BACKPORT_WORK"]
    token = os.environ.get("GH_TOKEN", "")

    if not os.path.exists(manifest_path):
        # The agent step ran (we're past the skip gate) but produced no manifest
        # — a model/tool interruption or output-contract miss. Fail loudly rather
        # than reporting a green no-op that silently opened no backports.
        common.log("ERROR: agent produced no manifest; failing the run.")
        return 1
    manifest = json.loads(Path(manifest_path).read_text())
    entries = manifest.get("targets") or []
    if not isinstance(entries, list):
        common.log("Malformed manifest: 'targets' is not a list.")
        return 1

    # Treat the agent's clone `.git` as hostile before the token-holding push.
    if not DRY_RUN:
        common.sanitize_git_dir(work, os.environ["GITHUB_REPOSITORY"])
    configure_push_auth(work, token)

    rows = [apply_target(ctx, work, e) for e in entries if isinstance(e, dict)]

    # Every requested target must be accounted for. A target the manifest omits
    # entirely (a truncated/empty agent output) would otherwise be a silent
    # green no-op — surface it as an error row so it appears in the summary and
    # fails the run.
    covered = {r["target"] for r in rows}
    omitted = [t for t in ctx["targets"] if t not in covered]
    rows += [{"target": t, "status": "error", "detail": "omitted from manifest"}
             for t in omitted]

    if rows and not DRY_RUN:
        comment_file = os.path.join(os.environ["RUNNER_TEMP"], "backport-summary.md")
        with open(comment_file, "w") as f:
            f.write(summary_comment(ctx["pr"], ctx["sha"], rows))
        common.gh("pr", "comment", str(ctx["pr"]), "--body-file", comment_file, check=False)
    elif rows:
        common.log("[dry-run] would comment the summary on the original PR:")
        common.log(summary_comment(ctx["pr"], ctx["sha"], rows))

    common.log("Auto-backport apply summary:")
    for r in rows:
        common.log(f"  {r['target']:<12} {r['status']:<14} {r['detail']}")
    # Fail the run if any requested target was omitted or errored, so a partial /
    # empty manifest isn't reported as success.
    return 1 if any(r["status"] == "error" for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
