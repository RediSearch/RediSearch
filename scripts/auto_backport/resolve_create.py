#!/usr/bin/env python3
"""Resolve PR context and target branches for the auto-backport create flow.

Invoked from .github/workflows/task-backport_pr-agent.yml after the master
checkout. Reads the triggering event metadata from env vars, queries gh
for PR data, derives the list of target release branches to back-port to,
and writes a context JSON file in $RUNNER_TEMP that the Codex agent
consumes via the `BACKPORT_CONTEXT_FILE` env var.

Targets come from `backport-<branch>-agent` labels or from an explicit
`/backport-agent <list>` comment. A `>= <version>` arg in that comment expands
to every active release branch at or above that version, read from
.github/release-branches.json (the repo's registry of live release lines).

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

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


# `backport-<branch>-agent` is the trigger-label shape. The middle group
# is the release branch to backport to.
LABEL_RE = re.compile(r"^backport-(.+)-agent$")

# A valid target is a release-branch name: `MAJOR.MINOR` optionally followed by
# a variant suffix (e.g. `8.6`, `8.10`, `8.6-rse`, `2.8`). This is the gate that
# keeps malformed targets out of the run — whether they come from a user-typed
# `/backport-agent <list>` comment (`8.6x`, `foo`, path/injection-ish tokens) or
# a mis-provisioned label (`backport-experimental-agent`). A well-formed target
# that simply doesn't exist as a branch is still caught later by the agent's
# per-target `git ls-remote` pre-flight; this only rejects things that could
# never be a branch, before they reach branch-name construction.
TARGET_RE = re.compile(r"^\d+\.\d+(?:-[A-Za-z0-9._-]+)?$")

# The command token must be exactly `/backport-agent`, optionally followed by
# whitespace and a target list. Anchored with a trailing boundary so mistyped
# siblings like `/backport-agentcontext` don't get their suffix parsed as a
# branch target. (The workflow `if:` gate already excludes the dash-prefixed
# `/backport-agent-fix` / `/backport-agent-context` commands; this guards the
# no-dash typos that still slip through `startsWith`.)
COMMENT_COMMAND_RE = re.compile(r"^/backport-agent(\s|$)")

# A `>=<version>` token in the comment args: backport to that release line and
# every newer one. `>= 2.10` is normalized to `>=2.10` before splitting (see
# parse_comment_args), so only the no-space form needs matching here.
FLOOR_RE = re.compile(r"^>=(\d+\.\d+(?:-[A-Za-z0-9._-]+)?)$")

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
    branches = data.get("release_branches")
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
    if not TARGET_RE.match(base):
        common.log(f"Ignoring malformed version floor: {floor}")
        return []
    branches = load_release_branches()
    expanded = [b for b in branches if TARGET_RE.match(b) and version_key(b) >= version_key(base)]
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
    stripped = re.sub(r"^/backport-agent\s*", "", first_line)
    if not stripped.strip():
        return []
    stripped = re.sub(r">=\s+", ">=", stripped)
    return [t for t in re.split(r"[\s,]+", stripped) if t]


def resolve_targets(event_name: str, event_action: str,
                    label_name: str, comment_body: str,
                    pr_data: dict) -> list[str]:
    """Derive the deduplicated target-branch list from event + PR state."""
    targets: list[str] = []

    # 1) An explicit `/backport-agent <list>` comment overrides labels for the
    #    run. A *plain* `/backport-agent` (no args) returns [] here and falls
    #    through to the label scan below — the documented "backport to whatever
    #    labels are on the PR" behavior.
    #
    #    `>=<version>` args are expanded here, in place, into the registered
    #    release branches at or above that version — `>= 2.10` is the usual
    #    "this line and everything newer" backport. Expansion happens before the
    #    dedup/validate pass below, so mixing forms (`>= 8.8 2.10`) just unions.
    #    A `>=` that expands to nothing does NOT fall back to labels: the
    #    comment stated an explicit intent, and quietly backporting somewhere
    #    else would be worse than doing nothing.
    comment_args: list[str] = []
    if event_name == "issue_comment":
        comment_args = parse_comment_args(comment_body)
        targets = [
            expanded
            for arg in comment_args
            for expanded in (expand_floor(arg) if arg.startswith(">=") else [arg])
        ]

    # Note the guard is on `comment_args`, not `targets`: a comment that DID
    # carry args but whose `>=` floor expanded to nothing must stay empty rather
    # than silently inheriting the PR's labels.
    if not comment_args and not targets:
        # 2) On a `labeled` event, seed the just-fired label
        #    (`github.event.label.name`) first, as a guard against the
        #    `gh pr view` label snapshot lagging the webhook event.
        if event_name == "pull_request_target" and event_action == "labeled":
            m = LABEL_RE.match(label_name or "")
            if m:
                targets.append(m.group(1))

        # 3) Fall back to EVERY matching label currently on the PR. This runs
        #    for a plain `/backport-agent` comment, a `labeled` event, and a
        #    `closed`-merged event alike — never just the one label that fired.
        #
        #    Resolving all labels (rather than only the fired one) is what makes
        #    multi-label backports reliable: adding several
        #    `backport-<branch>-agent` labels fires a separate `labeled` run per
        #    label, and the per-PR concurrency group (cancel-in-progress: false)
        #    keeps only the LATEST pending run, cancelling the intermediates. If
        #    each run resolved only its own fired label, the cancelled runs'
        #    targets would be silently dropped. Having whichever run survives
        #    resolve the full current label set means no target is lost, and the
        #    agent's per-target idempotency (it skips any target whose backport
        #    PR already exists) makes re-processing an already-handled label a
        #    no-op.
        for label in pr_data.get("labels", []) or []:
            m = LABEL_RE.match(label.get("name", ""))
            if m:
                targets.append(m.group(1))

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
        if TARGET_RE.match(t):
            out.append(t)
        else:
            dropped.append(t)
    if dropped:
        common.log(f"Ignoring malformed backport target(s): {', '.join(dropped)}")
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
    pr_data = common.fetch_pr(pr, [
        "title", "mergeCommit", "labels", "state", "url", "isCrossRepository",
    ])
    state = pr_data.get("state")
    if state != "MERGED":
        common.skip(f"PR #{pr} is not merged (state={state}); skipping.")

    # Refuse cross-repo (fork-sourced) PRs even after merge. The cherry-pick
    # itself would be safe — the merge SHA lives on master in our own repo —
    # but the agent reads the original PR title/body as evidence about the
    # change, and that text is attacker-authored on fork PRs. Send those to
    # the manual `/pr-backport` skill so a human reviews the input. Mirrors
    # the same gate in resolve_fix.py.
    if pr_data.get("isCrossRepository"):
        common.skip(
            f"PR #{pr} is cross-repository; the auto-backport flow does not "
            "operate on fork-sourced PRs. Skipping."
        )

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
