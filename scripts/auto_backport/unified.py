# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Deterministic orchestration for the action-first backport workflow.

The action owns the initial comment; report() takes ownership after it finishes.
Results and the authorized agent context live outside the agent's writable roots.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import subprocess
import sys

import apply_create
import common


def saved(name: str) -> Path:
    return Path(os.environ["RUNNER_TEMP"], f"backport-{name}.json")


def read(path: str | Path) -> dict:
    return json.loads(Path(path).read_text())


def write(name: str, value: dict) -> None:
    saved(name).write_text(json.dumps(value))


def api_pages(endpoint: str) -> list:
    # Unlike the feedback collectors, publication dedup must fail on API errors.
    return common.decode_concatenated_json_arrays(common.gh("api", endpoint, "--paginate"))


def backports(ctx: dict, target: str) -> list[dict]:
    repo = os.environ["GITHUB_REPOSITORY"]
    owner = repo.split("/")[0]
    found = []
    for branch in (apply_create.branch_for(ctx["pr"], target),
                   f"backport-{ctx['pr']}-to-{target}"):
        for pr in api_pages(f"repos/{repo}/pulls?state=all&head={owner}:{branch}&base={target}&per_page=100"):
            if ((pr.get("head", {}).get("repo") or {}).get("full_name", "").lower() == repo.lower() and (
                    pr["head"]["ref"] == branch and pr["base"]["ref"] == target)):
                found.append(pr)
    return found


def existing_row(ctx: dict, target: str, created: set[int] | None = None) -> dict | None:
    prs = backports(ctx, target)
    if not prs:
        return None
    # A merged or open replacement takes precedence over a historical closed PR.
    pr = min(prs, key=lambda p: (not bool(p.get("merged_at")), p["state"] != "open"))
    status = "already merged" if pr.get("merged_at") else "already open"
    if pr["state"] == "closed" and not pr.get("merged_at"):
        status = "closed — manual intervention"
    elif pr["number"] in (created or set()):
        status = "clean"
    return {"target": target, "status": status, "detail": pr["html_url"]}


def git(work: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", work, *args], text=True, capture_output=True, check=check)


def preflight(ctx: dict) -> None:
    rows = {t: {"target": t, "status": "error", "detail": "action did not complete"}
            for t in ctx["targets"]}
    state = {"rows": rows, "comment_ids": [], "bot": os.environ["BACKPORT_BOT_LOGIN"],
             "action_targets": [], "agent_targets": []}
    write("results", state)
    for target in ctx["targets"]:
        row = existing_row(ctx, target)
        if row:
            rows[target] = row
        else:
            state["action_targets"].append(target)
        write("results", state)
    repo = os.environ["GITHUB_REPOSITORY"]
    comments = api_pages(f"repos/{repo}/issues/{ctx['pr']}/comments?per_page=100")
    state["comment_ids"] = [c["id"] for c in comments]
    write("results", state)
    if state["action_targets"]:
        work = os.environ["BACKPORT_ACTION_WORK"]
        subprocess.run(["git", "clone", "--no-hardlinks", os.environ["GITHUB_WORKSPACE"], work], check=True)
        git(work, "remote", "set-url", "origin", f"https://github.com/{repo}")
        # The classic action needs push authentication, isolated from the trusted checkout.
        header = "AUTHORIZATION: basic " + base64.b64encode(
            f"x-access-token:{os.environ['GH_TOKEN']}".encode()).decode()
        if git(work, "config", "http.https://github.com/.extraheader", header, check=False).returncode:
            # Do not raise CalledProcessError with an auth header in its argv.
            raise ValueError("Could not configure classic checkout authentication")
    common.set_output("targets", " ".join(state["action_targets"]))


def selected_commits(ctx: dict, work: str) -> list[str]:
    """Mirror the pinned action's auto selection and merge_commits=skip.

    Keep parity with korthout/backport-action src/resolve-commits.ts and
    github.ts when updating its pin. Both sides use PR association to distinguish
    squash from rebase, rather than guessing from commit messages.
    """
    repo, pr, sha = os.environ["GITHUB_REPOSITORY"], ctx["pr"], ctx["sha"]
    commits = api_pages(f"repos/{repo}/pulls/{pr}/commits?per_page=100")
    git(work, "fetch", "--no-tags", "origin", f"refs/pull/{pr}/head", sha)
    parents = git(work, "show", "-s", "--format=%P", sha).stdout.split()
    selected = [c["sha"] for c in commits]
    if len(parents) == 1:
        if len(commits) == 1:
            selected = [sha]
        else:
            def associated(commit: str) -> bool:
                return any(p["number"] == pr for p in api_pages(
                    f"repos/{repo}/commits/{commit}/pulls?per_page=100"))
            parent_associated, merge_associated = associated(parents[0]), associated(sha)
            if merge_associated:
                selected = (git(work, "rev-list", "--reverse", f"{sha}~{len(commits)}..{sha}").stdout.split()
                            if parent_associated else [sha])
    return [c for c in selected if len(git(work, "show", "-s", "--format=%P", c).stdout.split()) <= 1]


def replay(work: str, target: str, commits: list[str], branch: str | None = None) -> dict:
    """Reproduce a failure without model calls; preserve enough evidence to resolve it."""
    base = git(work, "rev-parse", f"refs/remotes/origin/{target}").stdout.strip()
    git(work, "checkout", "--detach", base)
    failure = None
    skipped = []
    try:
        for sha in commits:
            result = git(work, "cherry-pick", "-x", sha, check=False)
            if result.returncode:
                paths = git(work, "diff", "--name-only", "--diff-filter=U").stdout.splitlines()
                if not paths and git(work, "diff", "--quiet", "HEAD", check=False).returncode == 0:
                    # Empty picks are already applied; keep checking the rest of the range.
                    git(work, "cherry-pick", "--skip")
                    skipped.append(sha)
                    continue
                failure = {"kind": "conflict" if paths else "error", "paths": paths,
                           "stderr": result.stderr[-6000:], "commit": sha, "base_sha": base}
                break
        if failure:
            return failure
        unchanged = git(work, "diff", "--quiet", base, "HEAD", check=False).returncode == 0
        result = {"kind": "already applied" if unchanged else "clean", "base_sha": base,
                  "skipped_commits": skipped}
        if skipped and not unchanged and branch:
            git(work, "branch", branch, "HEAD")
            result["head_sha"] = git(work, "rev-parse", branch).stdout.strip()
        return result
    finally:
        git(work, "cherry-pick", "--abort", check=False)
        git(work, "reset", "--hard", base)


def collect(ctx: dict) -> None:
    state = read(saved("results"))
    created = {int(n) for n in os.environ.get("CREATED_PULL_NUMBERS", "").split() if n.isdigit()}
    outcomes = {}
    for line in os.environ.get("SUCCESS_BY_TARGET", "").splitlines():
        target, sep, value = line.partition("=")
        if sep and target in state["action_targets"] and value in {"true", "false"}:
            outcomes[target] = value
    pending = []
    for target in state["action_targets"]:
        row = existing_row(ctx, target, created)
        if row:
            state["rows"][target] = row
        elif outcomes.get(target) == "true":
            state["rows"][target]["detail"] = "action reported success but no backport PR was found"
        else:
            pending.append(target)
        write("results", state)
    if not pending:
        common.set_output("run_agent", "false")
        return

    work = os.environ["BACKPORT_WORK"]
    subprocess.run(["git", "clone", "--no-hardlinks", os.environ["GITHUB_WORKSPACE"], work], check=True)
    git(work, "remote", "set-url", "origin", f"https://github.com/{os.environ['GITHUB_REPOSITORY']}")
    # Use process-scoped read auth: no credential survives in the clone for the agent.
    header = "AUTHORIZATION: basic " + base64.b64encode(
        f"x-access-token:{os.environ['GH_TOKEN']}".encode()).decode()
    os.environ.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
                      GIT_CONFIG_VALUE_0=header)
    try:
        commits = selected_commits(ctx, work)
        if not commits:
            raise ValueError("No non-merge commits selected by the action's merge policy")
        failures = {}
        clean_recoveries = {}
        for target in pending:
            try:
                fetched = git(work, "fetch", "--no-tags", "origin",
                              f"+refs/heads/{target}:refs/remotes/origin/{target}", check=False)
                if fetched.returncode:
                    state["rows"][target]["detail"] = "target fetch failed; check branch existence and repository access"
                else:
                    branch = apply_create.branch_for(ctx["pr"], target)
                    result = replay(work, target, commits, branch)
                    if result["kind"] == "conflict" and ctx["agent_allowed"]:
                        failures[target] = result
                        state["rows"][target]["detail"] = "agent did not complete conflict resolution"
                    elif result["kind"] == "already applied":
                        state["rows"][target] = {"target": target, "status": "already applied", "detail": "no changes needed"}
                    elif result["kind"] == "clean" and result.get("head_sha"):
                        clean_recoveries[target] = {**result, "target": target, "branch": branch,
                                                    "status": "clean"}
                        state["rows"][target]["detail"] = "clean remainder awaiting publication"
                    elif result["kind"] == "conflict":
                        state["rows"][target]["detail"] = "fork-sourced PR conflict requires manual backport"
                    else:
                        state["rows"][target]["detail"] = "no reproducible conflict; check action logs and retry (publication or infrastructure failure)"
            except (OSError, ValueError, subprocess.CalledProcessError) as error:
                state["rows"][target]["detail"] = f"failure reproduction stopped: {error}"
            write("results", state)
        agent_ctx = {**ctx, "targets": sorted(failures, key=lambda t: tuple(map(int, t.split('-')[0].split('.'))), reverse=True),
                     "commits": commits, "failures": failures}
        write("agent-context", agent_ctx)
        state["clean_recoveries"] = clean_recoveries
        state["agent_targets"] = agent_ctx["targets"]
        write("results", state)
        common.set_output("apply_needed", "true" if failures or clean_recoveries else "false")
        common.set_output("run_agent", "true" if failures else "false")
        common.set_output("agent_context", str(saved("agent-context")))
    finally:
        for key in ("GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"):
            os.environ.pop(key, None)


def apply(ctx: dict) -> None:
    state = read(saved("results"))
    clean_recoveries = state.get("clean_recoveries", {})
    if clean_recoveries:
        privileged = common.PrivilegedGit(os.environ["BACKPORT_WORK"], os.environ["GITHUB_REPOSITORY"],
                                         os.environ["GH_TOKEN"])
        for target, entry in clean_recoveries.items():
            try:
                row = existing_row(ctx, target)
                if row is None:
                    branch = apply_create.branch_for(ctx["pr"], target)
                    if privileged("rev-parse", branch).stdout.strip() != entry["head_sha"]:
                        raise ValueError("prepared clean remainder was modified after triage")
                    remote = privileged("ls-remote", "--exit-code", "origin", f"refs/heads/{target}").stdout.split()
                    if not remote or remote[0] != entry["base_sha"]:
                        raise ValueError("target advanced after triage; retry")
                    row = apply_create.apply_target(ctx, privileged, entry)
                    if row["status"] == "skipped":
                        row["status"] = "error"
                state["rows"][target] = row
            except (OSError, ValueError, subprocess.CalledProcessError) as error:
                state["rows"][target]["detail"] = f"clean remainder publication failed: {error}"
            write("results", state)
    if not state["agent_targets"]:
        return
    agent_ctx = read(saved("agent-context"))
    try:
        manifest = read(os.environ["BACKPORT_MANIFEST_FILE"])
        entries = manifest.get("targets")
        if not isinstance(entries, list):
            raise ValueError("manifest targets must be a list")
        by_target = {}
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("target"), str):
                raise ValueError("invalid manifest entry")
            target = entry["target"]
            if target not in state["agent_targets"] or target in by_target:
                raise ValueError("unknown or duplicate manifest target")
            by_target[target] = entry
    except (OSError, ValueError, AttributeError) as error:
        for target in state["agent_targets"]:
            state["rows"][target]["detail"] = f"agent output unavailable or invalid: {error}"
        write("results", state)
        return

    privileged = common.PrivilegedGit(os.environ["BACKPORT_WORK"], os.environ["GITHUB_REPOSITORY"],
                                     os.environ["GH_TOKEN"])
    for target in state["agent_targets"]:
        try:
            if target not in by_target:
                raise ValueError("target omitted from agent manifest")
            row = existing_row(ctx, target)
            if row is None:
                validate_branch(agent_ctx, privileged, by_target[target])
                row = apply_create.apply_target(agent_ctx, privileged, by_target[target])
                if row["status"] == "skipped":
                    row["status"] = "error"
            state["rows"][target] = row
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            state["rows"][target]["detail"] = f"apply failed: {error}"
        write("results", state)


def validate_branch(ctx: dict, git: common.PrivilegedGit, entry: dict) -> None:
    if entry.get("status") == "skipped":
        return
    if entry.get("status") != "conflicts" or not isinstance(entry.get("conflict_log"), list) or not entry["conflict_log"]:
        raise ValueError("resolved conflicts require a conflict log")
    target = entry["target"]
    branch = apply_create.branch_for(ctx["pr"], target)
    if entry.get("branch") != branch:
        raise ValueError("agent branch does not match the authorized target")
    base = ctx["failures"][target]["base_sha"]
    if git("merge-base", "--is-ancestor", base, branch, check=False).returncode:
        raise ValueError("agent branch does not descend from the authorized base")
    count = int(git("rev-list", "--count", f"{base}..{branch}").stdout)
    if not 0 < count <= len(ctx["commits"]):
        raise ValueError("agent branch has an unexpected commit count")
    if git("rev-list", "--merges", f"{base}..{branch}").stdout.strip():
        raise ValueError("agent introduced merge commits")
    remote = git("ls-remote", "--exit-code", "origin", f"refs/heads/{target}").stdout.split()
    if not remote or remote[0] != base:
        raise ValueError("target advanced during conflict resolution; retry against the new base")


def report(ctx: dict) -> int:
    state = read(saved("results")) if saved("results").exists() else {
        "rows": {}, "comment_ids": [], "bot": os.environ["BACKPORT_BOT_LOGIN"]}
    rows = [state["rows"].get(t, {"target": t, "status": "error", "detail": "workflow stopped before backporting"})
            for t in ctx["targets"]]
    created = {int(n) for n in os.environ.get("CREATED_PULL_NUMBERS", "").split() if n.isdigit()}
    # Reporting also reconciles partial runs (e.g. the token revocation failed
    # after PR creation). A failed collector must not hide already-created PRs.
    for index, row in enumerate(rows):
        if row["status"] == "error":
            try:
                rows[index] = existing_row(ctx, row["target"], created) or row
            except (OSError, ValueError, subprocess.CalledProcessError):
                pass
    body = apply_create.summary_comment(ctx["pr"], ctx["sha"], rows)
    run_url = f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    marker = f"<!-- unified-backport:{os.environ['GITHUB_RUN_ID']}:{os.environ['GITHUB_RUN_ATTEMPT']} -->"
    body += f"\n\n[Workflow run]({run_url}) · Retry unfinished targets with `/backport <branches>`.\n{marker}"
    if ctx.get("diagnostics"):
        body += "\n\n" + "\n".join(apply_create.summary_cell(d) for d in ctx["diagnostics"])
    saved("summary").with_suffix(".md").write_text(body)
    repo = os.environ["GITHUB_REPOSITORY"]
    comments = api_pages(f"repos/{repo}/issues/{ctx['pr']}/comments?per_page=100")
    candidates = [c for c in comments if c["user"]["login"] == state["bot"] and (
        marker in c["body"] or (c["id"] not in state["comment_ids"] and
        f"]({run_url})" in c["body"] and c["body"].startswith("[Backport-action]")))]
    if len(candidates) > 1:
        raise ValueError("ambiguous summary comment; refusing to create a duplicate")
    endpoint = (f"repos/{repo}/issues/comments/{candidates[0]['id']}" if candidates
                else f"repos/{repo}/issues/{ctx['pr']}/comments")
    common.gh("api", endpoint, "--method", "PATCH" if candidates else "POST", "-f", f"body={body}")
    return int(bool(ctx.get("diagnostics")) or any(
        r["status"] == "error" or r["status"].startswith("closed") for r in rows))


def main() -> int:
    ctx = read(os.environ["BACKPORT_CONTEXT_FILE"])
    command = sys.argv[1]
    if command == "report":
        return report(ctx)
    {"preflight": preflight, "collect": collect, "apply": apply}[command](ctx)
    return 0


if __name__ == "__main__":
    sys.exit(main())
