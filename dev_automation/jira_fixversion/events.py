# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Parse a GitHub ``pull_request`` event payload for the event path (design §5.1)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .github_client import is_fork

_ISSUE_KEY_RE = re.compile(r"\bMOD-\d+\b", re.IGNORECASE)


def extract_issue_keys(*texts: str) -> list[str]:
    """Unique MOD issue keys across the given texts, order-preserving, upper-cased."""
    seen: dict[str, None] = {}
    for text in texts:
        for m in _ISSUE_KEY_RE.findall(text or ""):
            seen.setdefault(m.upper(), None)
    return list(seen)


@dataclass
class PrEvent:
    repo: str  # bare repo name, e.g. "RediSearch"
    number: int
    action: str
    head_ref: str
    base_ref: str
    title: str
    html_url: str
    state: str  # OPEN | CLOSED | MERGED
    is_fork: bool


def parse_pr_event(payload: dict) -> Optional[PrEvent]:
    """Normalize a pull_request payload, or None if it isn't one."""
    pr = payload.get("pull_request")
    if not isinstance(pr, dict):
        return None
    head, base = pr.get("head") or {}, pr.get("base") or {}
    head_repo = (head.get("repo") or {}).get("full_name", "")
    base_repo = (base.get("repo") or {}).get("full_name", "")
    return PrEvent(
        repo=(payload.get("repository") or {}).get("name", ""),
        number=int(pr.get("number") or 0),
        action=payload.get("action", ""),
        head_ref=head.get("ref") or "",
        base_ref=base.get("ref") or "",
        title=pr.get("title") or "",
        html_url=pr.get("html_url") or "",
        state="MERGED" if pr.get("merged") else (pr.get("state") or "").upper(),
        is_fork=is_fork(head_repo, base_repo),
    )
