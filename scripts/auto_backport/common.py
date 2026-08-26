# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Shared helpers for the auto-backport resolve and apply scripts.

These scripts are invoked from .github/workflows/task-backport_pr-agent.yml
and .github/workflows/task-backport_pr-agent-fix.yml. Each workflow's
resolve step calls one of resolve_create.py or resolve_fix.py, which both
rely on this module for gh CLI access, $GITHUB_OUTPUT writing, and PR
fetching; each apply step drives its writes through `PrivilegedGit` below,
which is the single owner of the token-holding git path.

The scripts assume:
- `gh` is installed and authenticated via env (GH_TOKEN, GH_REPO).
- `RUNNER_TEMP` and `GITHUB_OUTPUT` are set by GitHub Actions.
- Python 3.8+ (so the deprecated `Optional[X]` style works without
  __future__ imports; we use `from __future__ import annotations`
  in the callers anyway).

Keep this module deliberately tiny, and stdlib-only: it runs on the runner's
system python3 with no `pip install` step, which is deliberate — the apply
steps that import it hold the GitHub App write token, and a third-party
dependency there would be a supply-chain surface on the most privileged step
in the flow. These helpers exist so the callers read as logic rather than
subprocess plumbing.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
from typing import Any, Iterable, NoReturn


# ---- privileged git in an agent-produced clone -------------------------------
#
# The auto-backport agent runs with write access to its working clone, so before
# the (token-holding) apply step runs any git command there, the clone's `.git`
# must be treated as hostile: an injected agent could have installed hooks or set
# config (credential.helper, url.insteadOf, core.fsmonitor/pager/sshCommand,
# uploadpack.*) that would execute code during an otherwise-innocent push.

# Env that neutralizes global/system git config and interactive prompts for the
# privileged git calls; callers also pass a sanitized local `.git` (below).
SAFE_GIT_ENV = {
    **os.environ,
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_ALLOWED_PROTOCOLS": "https",
}


def sanitize_git_dir(work: str, repo: str) -> None:
    """Strip agent-controllable code paths from a clone's `.git` before a
    privileged git operation: remove all hooks and rewrite the local config to a
    minimal trusted `origin` (dropping any credential.helper / insteadOf / hook
    settings the agent may have added). Refs and objects are untouched, so the
    agent's produced branch is preserved. Auth (extraheader) is re-added by the
    caller afterwards.
    """
    gitdir = os.path.join(work, ".git")
    hooks = os.path.join(gitdir, "hooks")
    shutil.rmtree(hooks, ignore_errors=True)
    os.makedirs(hooks, exist_ok=True)
    with open(os.path.join(gitdir, "config"), "w") as f:
        f.write(
            "[core]\n\trepositoryformatversion = 0\n\tbare = false\n"
            f'[remote "origin"]\n\turl = https://github.com/{repo}\n'
            "\tfetch = +refs/heads/*:refs/remotes/origin/*\n"
        )


class PrivilegedGit:
    """The only sanctioned way to run git in an agent-produced clone while
    holding the write token.

    Both appliers used to carry their own copy of this plumbing — a `git()`
    wrapper plus a `configure_push_auth()` — and relied on each call site
    remembering to sanitize `.git` first and to pass `--no-verify` on every
    push. That contract lived in comments, which is exactly the kind of
    invariant that rots. Here it is structural instead:

    - **Constructing** the object *is* the security boundary: it sanitizes the
      clone (hooks + local config, see `sanitize_git_dir`) and only then installs
      the push credential, so no caller can reach a push without both having
      happened.
    - `push_ref` / `delete_ref` are the only push paths, and they hard-code
      `--no-verify` and a fully-qualified refspec. The generic `__call__`
      *refuses* `push`, so a future edit can't quietly add an unhardened one.
    - Every invocation is an argument array under `SAFE_GIT_ENV` — never a
      shell-interpolated string, since the branch/target names originate in
      agent-authored JSON.

    Deliberately no GitPython: these scripts run on the runner's system
    `python3` with zero third-party deps, and a `pip install` inside the
    token-holding step would add a supply-chain surface to the one step this
    whole design exists to harden. The security properties here are all *exact
    invocation control* (`GIT_CONFIG_GLOBAL`/`SYSTEM`, `GIT_ALLOWED_PROTOCOLS`,
    `--no-verify`, `--force-with-lease=<ref>:<sha>`, `extraheader` basic auth) —
    precisely what an SDK abstracts away and makes harder to audit.

    `dry_run=True` skips the mutating setup (sanitize + auth) and refuses pushes,
    so the read-only pre-flight still exercises the real code path.
    """

    def __init__(self, work: str, repo: str, token: str, *,
                 dry_run: bool = False) -> None:
        self.work = work
        self.dry_run = dry_run
        if not dry_run:
            sanitize_git_dir(work, repo)
            self._configure_push_auth(token)

    def __call__(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run `git -C <work> <args>` under the sanitized env.

        Pushes are rejected: they must go through `push_ref` / `delete_ref` so
        the `--no-verify` and refspec hardening can't be bypassed.
        """
        if args and args[0] == "push":
            raise ValueError("use PrivilegedGit.push_ref()/delete_ref() for pushes")
        return self._run(*args, check=check)

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", self.work, *args],
                              capture_output=True, text=True, check=check,
                              env=SAFE_GIT_ENV)

    def _configure_push_auth(self, token: str) -> None:
        """Wire the App token as the push credential for the clone's origin.

        GitHub App tokens must be presented as HTTP basic `x-access-token:<token>`;
        a bearer header is rejected. Done here rather than in the agent's step so
        the token never enters the agent's environment.
        """
        header = "AUTHORIZATION: basic " + base64.b64encode(
            f"x-access-token:{token}".encode()).decode()
        self._run("config", "http.https://github.com/.extraheader", header)

    # -- the only two push paths ---------------------------------------------

    def push_ref(self, branch: str) -> subprocess.CompletedProcess:
        """Plain (non-force) push of `branch` to the same name on origin.

        `--no-verify` plus the sanitized `.git` mean no agent-installed hook or
        config runs during this token-holding push. The refspec is spelled out
        in full so an agent-supplied name can't be resolved as anything but a
        branch.
        """
        self._refuse_in_dry_run("push")
        return self._run("push", "--no-verify", "origin",
                         f"refs/heads/{branch}:refs/heads/{branch}", check=False)

    def delete_ref(self, branch: str, expect_sha: str) -> subprocess.CompletedProcess:
        """Delete `branch` on origin, leased to `expect_sha`.

        Used to clean up a ref we just pushed when the follow-up PR creation
        failed. `--force-with-lease` means that if anything updated the branch in
        the meantime the delete is refused rather than dropping that newer commit.
        """
        self._refuse_in_dry_run("delete")
        return self._run("push", "--no-verify",
                         f"--force-with-lease=refs/heads/{branch}:{expect_sha}",
                         "origin", f":refs/heads/{branch}", check=False)

    def _refuse_in_dry_run(self, what: str) -> None:
        if self.dry_run:
            raise AssertionError(f"dry run attempted a real {what}")


# ---- workflow log / outputs --------------------------------------------------


def log(msg: str) -> None:
    """Print one line to the workflow log."""
    print(msg, flush=True)


def set_output(name: str, value: str) -> None:
    """Append `name=value` to $GITHUB_OUTPUT.

    Single-line scalars only — the auto-backport scripts don't need
    multi-line outputs (those would require a heredoc delimiter).
    """
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        # Useful for local testing — fall back to a clearly-marked log line.
        log(f"[GITHUB_OUTPUT not set] {name}={value}")
        return
    with open(out_path, "a") as f:
        f.write(f"{name}={value}\n")


def skip(reason: str) -> NoReturn:
    """Log the reason, emit skip=true, and exit 0."""
    log(reason)
    set_output("skip", "true")
    sys.exit(0)


# ---- gh CLI ------------------------------------------------------------------


def gh(*args: str, check: bool = True) -> str:
    """Run `gh <args>` and return stdout (text).

    Raises CalledProcessError on non-zero exit when `check=True`. When
    `check=False`, returns whatever stdout was produced (possibly empty).
    Stderr is captured and surfaced in the exception or — on quiet failure
    — discarded; callers that care about stderr should run the subprocess
    directly.
    """
    cmd = ["gh", *args]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    except subprocess.CalledProcessError as e:
        log(f"gh command failed: {' '.join(cmd)}\nstderr: {e.stderr.strip()}")
        raise
    return result.stdout


def gh_graphql(query: str, **variables: Any) -> Any:
    """Run a GraphQL query via `gh api graphql` and return the decoded `data`.

    String variables are passed with `-f` (raw string): `-F` would apply gh's
    type inference and, worse, treat an `@`-prefixed value as a filename — so an
    agent reply of `@reviewer …` would fail or read a local file. Non-string
    values (int/bool, e.g. a PR number for `$pr:Int!`) still use `-F` so their
    GraphQL type is preserved. A `None` value is omitted so a nullable variable
    resolves to `null` (e.g. a first-page pagination cursor). Returns the `data`
    object, or `None` on any failure (best-effort, like `gh_paginated_array`).
    """
    args = ["api", "graphql", "-f", f"query={query}"]
    for name, value in variables.items():
        if value is None:
            continue
        if isinstance(value, str):
            args += ["-f", f"{name}={value}"]     # raw string: no @file / type magic
        else:
            args += ["-F", f"{name}={value}"]     # typed (Int/Boolean/…)
    try:
        out = gh(*args, check=False)
    except Exception:
        return None
    s = out.strip()
    if not s:
        return None
    try:
        parsed = json.loads(s)
    except json.JSONDecodeError:
        return None
    return parsed.get("data")


def gh_json(*args: str) -> Any:
    """`gh <args>` with stdout decoded as a single JSON value.

    Returns `None` for empty stdout. Do not use this for `--paginate` calls
    that emit multiple JSON documents — use `gh_paginated_array` for that.
    """
    out = gh(*args)
    s = out.strip()
    if not s:
        return None
    return json.loads(s)


def gh_paginated_array(*args: str) -> list:
    """Run a `--paginate` gh call whose per-page `--jq` emits a JSON array,
    and stitch the pages into one flat list.

    Returns `[]` on any gh failure — these calls are best-effort
    (transient network glitches shouldn't kill a backport run).
    """
    try:
        out = gh(*args, "--paginate", check=False)
    except Exception:
        return []
    return decode_concatenated_json_arrays(out)


def decode_concatenated_json_arrays(text: str) -> list:
    """Parse a stream of consecutive JSON values into one flat list.

    Each value should be a JSON array (one per page from `gh --paginate`);
    they're concatenated. Empty / whitespace-only input → []. Non-array
    values (rare; only seen with malformed --jq) are appended as scalars.
    """
    s = text.strip()
    if not s:
        return []
    decoder = json.JSONDecoder()
    out: list = []
    idx = 0
    while idx < len(s):
        while idx < len(s) and s[idx].isspace():
            idx += 1
        if idx >= len(s):
            break
        value, end = decoder.raw_decode(s, idx)
        if isinstance(value, list):
            out.extend(value)
        elif value is not None:
            out.append(value)
        idx = end
    return out


# ---- PR helpers --------------------------------------------------------------


def fetch_pr(pr_number: int | str, fields: Iterable[str]) -> dict:
    """`gh pr view <pr> --json <fields>` -> decoded dict."""
    out = gh("pr", "view", str(pr_number), "--json", ",".join(fields))
    return json.loads(out)


# ---- context JSON ------------------------------------------------------------


def write_context(path: str, payload: dict) -> None:
    """Write `payload` as a single-line JSON document to `path` and echo
    a *compact summary* (not the full content) to the workflow log.

    Echoing the full file is undesirable for the fix flow: `log_excerpts[]`
    carries tails of failed CI step logs, which may include non-masked
    sensitive output (env values, runner internals, etc.). The agent reads
    the file directly via `$BACKPORT_*_CONTEXT_FILE`, so the log echo is
    purely for human traceability — the path + a summary of the keys is
    enough; full content stays in the file in $RUNNER_TEMP. GitHub Actions
    auto-redacts any value that came through `${{ secrets.* }}` regardless
    of where it appears in the log, but we keep CI output off this surface
    on principle.
    """
    with open(path, "w") as f:
        json.dump(payload, f)

    # Build a redacted summary for the log: keep small scalar/list fields,
    # replace bulky bytes-of-CI fields with a one-line digest.
    summary: dict = {}
    for k, v in payload.items():
        if k == "log_excerpts" and isinstance(v, list):
            summary[k] = f"<{len(v)} entries; tails omitted from log>"
        elif k in ("context", "review_threads", "pr_comments") and isinstance(v, list):
            # Human hints / reviewer feedback are short and useful to see, but
            # replace them with a count digest so a reviewer pasting a wall of
            # text can't bloat the workflow log.
            summary[k] = f"<{len(v)} entries>"
        else:
            summary[k] = v

    log(f"Context written to {path}")
    log(json.dumps(summary))
