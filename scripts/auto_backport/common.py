"""Shared helpers for the auto-backport resolve scripts.

These scripts are invoked from .github/workflows/task-backport_pr-agent.yml
and .github/workflows/task-backport_pr-agent-fix.yml. Each workflow's
resolve step calls one of resolve_create.py or resolve_fix.py, which both
rely on this module for gh CLI access, $GITHUB_OUTPUT writing, and PR
fetching.

The scripts assume:
- `gh` is installed and authenticated via env (GH_TOKEN, GH_REPO).
- `RUNNER_TEMP` and `GITHUB_OUTPUT` are set by GitHub Actions.
- Python 3.8+ (so the deprecated `Optional[X]` style works without
  __future__ imports; we use `from __future__ import annotations`
  in the callers anyway).

Keep this module deliberately tiny — these helpers exist so the resolve
scripts read as logic rather than subprocess plumbing.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any, Iterable, NoReturn


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
    it back to the workflow log for traceability."""
    with open(path, "w") as f:
        json.dump(payload, f)
    log(f"Context written to {path}:")
    with open(path) as f:
        log(f.read())
