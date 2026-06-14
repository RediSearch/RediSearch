# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""GitHub ``pull_request`` event parsing for the event-driven path (design §5.1).

A GitHub Actions workflow (``pull_request_target``) hands us the event payload.
This module turns that payload into a normalized :class:`PrEvent` and extracts
the MOD issue key(s) the PR references — the entry point for resolving which
ticket(s) to act on.

The ticket↔PR association of record is still the Jira development panel (design
§6); the scheduled reconciliation path uses it directly. The event path uses
the PR's own issue-key references (branch / title / body — the same signal the
GitHub-for-Jira app keys on) as a low-latency entry point, then applies the
identical per-PR matching against authoritative GitHub data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# MOD issue keys, e.g. "MOD-16249". Case-insensitive on input; normalized upper.
_ISSUE_KEY_RE = re.compile(r"\bMOD-\d+\b", re.IGNORECASE)


def extract_issue_keys(*texts: str) -> list[str]:
    """Unique MOD issue keys found across the given texts, order-preserving."""
    seen: dict[str, None] = {}
    for text in texts:
        if not text:
            continue
        for m in _ISSUE_KEY_RE.findall(text):
            seen.setdefault(m.upper(), None)
    return list(seen)


@dataclass
class PrEvent:
    repo: str  # bare repo name, e.g. "RediSearch"
    number: int
    action: str  # opened | synchronize | edited | reopened | closed
    head_ref: str
    base_ref: str
    title: str
    body: str
    html_url: str
    state: str  # OPEN | CLOSED | MERGED
    is_fork: bool  # head repo differs from base repo


def parse_pr_event(payload: dict) -> Optional[PrEvent]:
    """Normalize a GitHub ``pull_request`` webhook payload.

    Returns ``None`` if the payload is not a pull-request event.
    """
    pr = payload.get("pull_request")
    if not isinstance(pr, dict):
        return None

    head = pr.get("head") or {}
    base = pr.get("base") or {}
    head_repo = (head.get("repo") or {}).get("full_name", "")
    base_repo = (base.get("repo") or {}).get("full_name", "")
    is_fork = bool(head_repo and base_repo and head_repo != base_repo)

    state = "MERGED" if pr.get("merged") else (pr.get("state") or "").upper()

    return PrEvent(
        repo=(payload.get("repository") or {}).get("name", ""),
        number=int(pr.get("number") or 0),
        action=payload.get("action", ""),
        head_ref=(head.get("ref") or ""),
        base_ref=(base.get("ref") or ""),
        title=(pr.get("title") or ""),
        body=(pr.get("body") or ""),
        html_url=(pr.get("html_url") or ""),
        state=state,
        is_fork=is_fork,
    )
