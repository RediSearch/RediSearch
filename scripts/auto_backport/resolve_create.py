#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Resolve authorized requests for task-backport_pr.yml.

Both label namespaces and both create commands share this resolver. Explicit
comment targets override labels; version floors expand through the release
registry. The context is stored in RUNNER_TEMP, outside the agent's writable
checkout, and remains the publication allow-list throughout the run.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


LABEL_RE = re.compile(r"^(?:backport ([^ ]+)|backport-(.+)-agent)$")
# Bound version digits to avoid pathological int conversions from comment text.
TARGET_RE = re.compile(r"^[0-9]{1,4}\.[0-9]{1,4}(?:-[A-Za-z0-9._-]{1,64})?$")
COMMENT_COMMAND_RE = re.compile(r"^/backport(?:-agent)?(\s|$)")

# A `>=<version>` token in the comment args: backport to that release line and
# every newer one. `>= 2.10` is normalized to `>=2.10` before splitting (see
# parse_comment_args), so only the no-space form needs matching here.
FLOOR_RE = re.compile(r"^>=([0-9]{1,4}\.[0-9]{1,4}(?:-[A-Za-z0-9._-]{1,64})?)$")

# The registry of currently-active release branches that `>=` expands over.
RELEASE_BRANCHES_FILE = (
    Path(__file__).resolve().parents[2] / ".github" / "release-branches.json"
)


def version_key(branch: str) -> tuple[int, int]:
    """`"8.10"` / `"8.10-rse"` -> `(8, 10)`, for numeric release-line ordering.

    A variant suffix sorts with its base line, so `8.6-rse` and `8.6` compare
    equal. Comparing numerically matters: lexically `"8.10" < "8.2"`, which
    would make `>= 8.9` silently skip 8.10.
    """
    major, _, rest = branch.partition(".")
    minor = re.match(r"\d+", rest)
    return (int(major), int(minor.group()) if minor else 0)


def load_release_branches() -> list[str]:
    """Read `.github/release-branches.json` -> the active release-branch list.

    Returns [] (after logging) when the file is missing or malformed. A `>=`
    token then resolves to nothing rather than aborting the run, so any
    explicitly-listed targets in the same comment still get backported --
    same philosophy as the malformed-target handling in resolve_targets.
    """
    try:
        with open(RELEASE_BRANCHES_FILE) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        common.log(f"Could not read {RELEASE_BRANCHES_FILE}: {e}")
        return []
    branches = data.get("release_branches") if isinstance(data, dict) else None
    if not isinstance(branches, list) or not all(isinstance(b, str) for b in branches):
        common.log(f"{RELEASE_BRANCHES_FILE}: 'release_branches' must be a list of strings")
        return []
    return branches


def expand_floor(floor: str) -> list[str]:
    """`">=8.6"` -> every registered release branch at or above 8.6.

    Order follows the registry (oldest first); the agent sorts targets
    newest-to-oldest itself. Returns [] for an unparsable floor or an empty
    registry, and logs why.
    """
    m = FLOOR_RE.match(floor)
    if not m:
        common.log(f"Ignoring malformed version floor: {floor}")
        return []
    base = m.group(1)
    if not TARGET_RE.fullmatch(base):
        common.log(f"Ignoring malformed version floor: {floor}")
        return []
    branches = load_release_branches()
    expanded = [b for b in branches if TARGET_RE.fullmatch(b) and version_key(b) >= version_key(base)]
    if not expanded:
        common.log(
            f"Version floor {floor} matched no active release branch "
            f"(registry: {', '.join(branches) if branches else 'unavailable'})"
        )
    else:
        common.log(f"Version floor {floor} expanded to: {', '.join(expanded)}")
    return expanded


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
    [] when the first line isn't exactly the `/backport-agent` command
    (e.g. a typo like `/backport-agentcontext`), when there are no args
    (plain `/backport-agent`), or for separator-only args
    (`/backport-agent ,`). An empty result falls back to the PR's labels.

    A `>=<version>` token survives as a single arg -- `/backport-agent >= 2.10`
    yields [">=2.10"] -- which resolve_targets expands over the active release
    branches. The whitespace after `>=` is folded first so the natural
    `>= 2.10` spelling doesn't split into two args.
    """
    if not comment_body:
        return []
    first_line = comment_body.splitlines()[0]
    if not COMMENT_COMMAND_RE.match(first_line):
        return []
    stripped = re.sub(r"^/backport(?:-agent)?\s*", "", first_line)
    if not stripped.strip():
        return []
    stripped = re.sub(r">=\s+", ">=", stripped)
    return [t for t in re.split(r"[\s,]+", stripped) if t]


def resolve_targets(event_name: str, event_action: str,
                    label_name: str, comment_body: str,
                    pr_data: dict, diagnostics: list[str] | None = None) -> list[str]:
    """Derive the deduplicated target-branch list from event + PR state."""
    targets: list[str] = []
    diagnostics = diagnostics if diagnostics is not None else []

    # Explicit intent must stay explicit, even if every argument is invalid.
    comment_args: list[str] = []
    if event_name == "issue_comment":
        comment_args = parse_comment_args(comment_body)
        if not COMMENT_COMMAND_RE.match(comment_body.splitlines()[0] if comment_body else ""):
            return []
        for arg in comment_args:
            expanded = expand_floor(arg) if arg.startswith(">=") else [arg]
            if not expanded:
                diagnostics.append(f"Version expression {arg!r} matched no release branches")
            targets.extend(expanded)

    # Note the guard is on `comment_args`, not `targets`: a comment that DID
    # carry args but whose `>=` floor expanded to nothing must stay empty rather
    # than silently inheriting the PR's labels.
    if not comment_args and not targets:
        # 2) On a `labeled` event, seed the just-fired label
        #    (`github.event.label.name`) first, as a guard against the
        #    `gh pr view` label snapshot lagging the webhook event.
        if event_name == "pull_request_target" and event_action == "labeled":
            m = LABEL_RE.fullmatch(label_name or "")
            if m:
                targets.append(m.group(1) or m.group(2))

        # Scan the complete label set so adding several labels is idempotent.
        for label in pr_data.get("labels", []) or []:
            m = LABEL_RE.fullmatch(label.get("name", ""))
            if m:
                targets.append(m.group(1) or m.group(2))

    # Dedup (preserve order) and drop anything that isn't a well-formed release
    # branch name — see TARGET_RE. Malformed targets are logged and skipped
    # rather than aborting the run: one bad comment token or stray label must
    # not stop the valid targets from being backported.
    seen: set[str] = set()
    out: list[str] = []
    dropped: list[str] = []
    for t in targets:
        if t in seen:
            continue
        seen.add(t)
        if TARGET_RE.fullmatch(t):
            out.append(t)
        else:
            dropped.append(t)
    if dropped:
        diagnostics.append(f"Invalid target(s): {', '.join(dropped)}")
        common.log(diagnostics[-1])
    return out


def main() -> int:
    event_name = os.environ.get("EVENT_NAME", "")
    event_action = os.environ.get("EVENT_ACTION", "")
    label_name = os.environ.get("LABEL_NAME", "")
    comment_body = os.environ.get("COMMENT_BODY", "")

    if event_name == "issue_comment" and not COMMENT_COMMAND_RE.match(comment_body):
        common.skip("Not a backport creation command")
    if event_name == "issue_comment" or event_action == "labeled":
        if not common.has_write_permission(os.environ.get("GITHUB_ACTOR", "")):
            common.skip("Backport commands and labels require repository write permission")

    pr = resolve_pr_number(event_name)
    if not pr:
        common.log(f"Unhandled or missing PR number for event {event_name!r}")
        return 1

    # `gh pr view --json` exposes `state` (OPEN/CLOSED/MERGED); the
    # boolean `merged` field doesn't exist in current gh CLI.
    pr_data = common.fetch_pr(pr, [
        "title", "body", "mergeCommit", "labels", "state", "url", "isCrossRepository", "author",
    ])
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

    diagnostics: list[str] = []
    targets = resolve_targets(event_name, event_action, label_name, comment_body, pr_data, diagnostics)
    if not targets and not diagnostics:
        common.skip(f"No backport targets resolved for PR #{pr}; nothing to do.")

    # Context goes to $RUNNER_TEMP (not the workspace) so the agent's
    # `git add -A` during cherry-pick conflict resolution can't stage
    # the context JSON into the backport commit.
    runner_temp = os.environ["RUNNER_TEMP"]
    context_file = os.path.join(runner_temp, "auto-backport-context.json")
    # `body` is pre-fetched here so the agent needs no `gh` (hence no token and
    # no network): the applier reads it to replicate the release-notes checkbox,
    # and the agent may read it as untrusted evidence about the change's intent.
    common.write_context(context_file, {
        "pr": int(pr),
        "sha": sha,
        "title": pr_data.get("title", ""),
        "body": pr_data.get("body", ""),
        "url": pr_data.get("url", ""),
        "targets": targets,
        "diagnostics": diagnostics,
        "agent_allowed": not pr_data.get("isCrossRepository"),
        "author": (pr_data.get("author") or {}).get("login", ""),
        "labels": [label["name"] for label in pr_data.get("labels", [])
                   if not LABEL_RE.fullmatch(label["name"])],
    })

    common.set_output("sha", sha)
    common.set_output("pr", pr)
    common.set_output("skip", "false")
    common.set_output("context_file", context_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
