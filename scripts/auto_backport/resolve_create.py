#!/usr/bin/env python3
"""Resolve PR context and target branches for the auto-backport create flow.

Invoked from .github/workflows/task-backport_pr-agent.yml after the master
checkout. Reads the triggering event metadata from env vars, queries gh
for PR data, derives the list of target release branches to back-port to,
and writes a context JSON file in $RUNNER_TEMP that the Codex agent
consumes via the `BACKPORT_CONTEXT_FILE` env var.

Exits 0 in one of two ways:
- `skip=true` output -> the workflow's later steps short-circuit (the
  `if:` on the Codex step gates on this).
- `skip=false` + `context_file=<path>` -> the workflow continues.

Non-zero exit is reserved for genuine programming errors (unrecognized
event, missing env, gh blowing up).

Env contract (set by the workflow):
- GH_TOKEN, GH_REPO -- consumed by `gh`.
- RUNNER_TEMP, GITHUB_OUTPUT -- GitHub Actions standard.
- EVENT_NAME, EVENT_ACTION, LABEL_NAME, COMMENT_BODY -- event payload bits.
- PR_NUMBER_FROM_PR -- pull_request event's PR number (may be empty).
- PR_NUMBER_FROM_ISSUE -- issue_comment event's PR/issue number (may be empty).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


# `backport-<branch>-agent` is the trigger-label shape. The middle group
# is the release branch to backport to.
LABEL_RE = re.compile(r"^backport-(.+)-agent$")


def resolve_pr_number(event_name: str) -> str | None:
    if event_name == "pull_request_target":
        return (os.environ.get("PR_NUMBER_FROM_PR") or "").strip() or None
    if event_name == "issue_comment":
        return (os.environ.get("PR_NUMBER_FROM_ISSUE") or "").strip() or None
    return None


def parse_comment_args(comment_body: str) -> list[str]:
    """`/backport-agent 8.6, 8.2` -> ["8.6", "8.2"].

    Only the first line of the comment is considered. Anything after the
    command (whitespace- or comma-separated) becomes a target. Returns
    [] when there are no args (e.g. plain `/backport-agent`) or
    separator-only args (e.g. `/backport-agent ,`).
    """
    if not comment_body:
        return []
    first_line = comment_body.splitlines()[0]
    stripped = re.sub(r"^/backport-agent\s*", "", first_line)
    if not stripped.strip():
        return []
    return [t for t in re.split(r"[\s,]+", stripped) if t]


def resolve_targets(event_name: str, event_action: str,
                    label_name: str, comment_body: str,
                    pr_data: dict) -> list[str]:
    """Derive the deduplicated target-branch list from event + PR state."""
    targets: list[str] = []

    # 1) `/backport-agent <list>` overrides labels for the run.
    if event_name == "issue_comment":
        targets = parse_comment_args(comment_body)

    # 2) `pull_request_target: labeled` -> just the one label that fired.
    if not targets and event_name == "pull_request_target" and event_action == "labeled":
        m = LABEL_RE.match(label_name or "")
        if m:
            targets = [m.group(1)]

    # 3) Fallback: every matching label on the PR.
    if not targets:
        for label in pr_data.get("labels", []) or []:
            m = LABEL_RE.match(label.get("name", ""))
            if m:
                targets.append(m.group(1))

    # Dedup, preserve order.
    seen: set[str] = set()
    out: list[str] = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def main() -> int:
    event_name = os.environ.get("EVENT_NAME", "")
    event_action = os.environ.get("EVENT_ACTION", "")
    label_name = os.environ.get("LABEL_NAME", "")
    comment_body = os.environ.get("COMMENT_BODY", "")

    pr = resolve_pr_number(event_name)
    if not pr:
        common.log(f"Unhandled or missing PR number for event {event_name!r}")
        return 1

    # `gh pr view --json` exposes `state` (OPEN/CLOSED/MERGED); the
    # boolean `merged` field doesn't exist in current gh CLI.
    pr_data = common.fetch_pr(pr, ["title", "mergeCommit", "labels", "state", "url"])
    state = pr_data.get("state")
    if state != "MERGED":
        common.skip(f"PR #{pr} is not merged (state={state}); skipping.")

    # Defensive: state=MERGED but mergeCommit can be null briefly
    # (API caching, certain fast-forward / merge-queue sequences). The
    # agent would otherwise try to cherry-pick the literal "null".
    merge_commit = pr_data.get("mergeCommit") or {}
    sha = (merge_commit.get("oid") or "").strip()
    if not sha or sha == "null":
        common.skip(
            f"PR #{pr} is MERGED but mergeCommit.oid is not available yet; "
            "skipping. Re-trigger later."
        )

    targets = resolve_targets(event_name, event_action, label_name, comment_body, pr_data)
    if not targets:
        common.skip(f"No backport-<branch>-agent targets resolved for PR #{pr}; nothing to do.")

    # Context goes to $RUNNER_TEMP (not the workspace) so the agent's
    # `git add -A` during cherry-pick conflict resolution can't stage
    # the context JSON into the backport commit.
    runner_temp = os.environ["RUNNER_TEMP"]
    context_file = os.path.join(runner_temp, "auto-backport-context.json")
    common.write_context(context_file, {
        "pr": int(pr),
        "sha": sha,
        "title": pr_data.get("title", ""),
        "url": pr_data.get("url", ""),
        "targets": targets,
    })

    common.set_output("skip", "false")
    common.set_output("context_file", context_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
