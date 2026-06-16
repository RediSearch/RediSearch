# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Pure matching: map a PR's version to Jira release(s) (design §8).

Side-effect-free — handlers take the project versions + a PullRequest and return
Lookup decisions; the caller applies fixVersions / alerts. Fully unit-testable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .version_parse import MASTER_SENTINEL, parse_version_h

REPO_REDISEARCH = "RediSearch"
MASTER_BRANCH = "master"


@dataclass(frozen=True)
class ReleaseTemplates:
    """Exact release-name template. Overridable so tests can target a
    differently-named release (e.g. a "... dummy" one)."""

    redisearch: str = "RediSearch v{X}.{Y}.{Z}"


DEFAULT_TEMPLATES = ReleaseTemplates()


@dataclass
class PullRequest:
    repo: str
    head_branch: str
    base_branch: str
    pr_number: int
    state: str = ""
    head_sha: str = ""
    url: str = ""


@dataclass
class Lookup:
    """One expected release. `release` is the matched version dict, or None
    (which triggers an alert). `searched_name`/`rule` are for alert context."""

    searched_name: str
    rule: str
    release: Optional[dict]


def is_version_branch(name: str) -> bool:
    return re.fullmatch(r"\d+\.\d+", name) is not None


def find_release_exact(versions: list[dict], name: str) -> Optional[dict]:
    return next((v for v in versions if v.get("name") == name), None)


def _active(v: dict) -> bool:
    """A version usable as a fix version: neither released nor archived."""
    return not v.get("released", False) and not v.get("archived", False)


def open_source_minor_unreleased(versions: list[dict], X: int, Y: int) -> Optional[dict]:
    """The active two-part `Open Source X.Y` (OSS releases drop the patch)."""
    return next((v for v in versions
                 if re.fullmatch(rf"Open Source {X}\.{Y}", v.get("name", "")) and _active(v)),
                None)


def open_source_minor_exists(versions: list[dict], X: int, Y: int) -> bool:
    """Whether any `Open Source X.Y` exists, regardless of released/archived."""
    return any(re.fullmatch(rf"Open Source {X}\.{Y}", v.get("name", "")) for v in versions)


def open_source_highest_version(versions: list[dict]) -> Optional[dict]:
    """The highest active two-part `Open Source X.Y` (master/sentinel case)."""
    cands = [v for v in versions
             if re.fullmatch(r"Open Source \d+\.\d+", v.get("name", "")) and _active(v)]
    return max(cands, key=lambda v: tuple(map(int, v["name"].split()[-1].split("."))),
               default=None)


def handle_redisearch(pr: PullRequest, versions: list[dict], version_text: str,
                      templates: ReleaseTemplates = DEFAULT_TEMPLATES) -> list[Lookup]:
    """RediSearch rule (design §8.2): base must be a version branch or master."""
    base = pr.base_branch
    if not (is_version_branch(base) or base == MASTER_BRANCH):
        return []
    X, Y, Z = parse_version_h(version_text)

    if base == MASTER_BRANCH:  # master -> highest unreleased Open Source X.Y
        rel = open_source_highest_version(versions)
        return [Lookup(rel["name"] if rel else "Open Source <highest unreleased X.Y>",
                       "RediSearch/master -> highest unreleased Open Source X.Y", rel)]

    # Version branch. The sentinel here means the head was not rebased onto the
    # release line (e.g. a backport branched off master) -> mismatch; alert rather
    # than assign the wrong line. (The master rule is gated on base == master above.)
    if (X, Y, Z) == MASTER_SENTINEL:
        return [Lookup(f"<version.h is 99.99.99 on base {base}>",
                       "RediSearch/version-branch -> head carries master sentinel (mismatch)", None)]

    # Concrete version: exact RediSearch vX.Y.Z + the two-part Open Source X.Y.
    rs = templates.redisearch.format(X=X, Y=Y, Z=Z)
    oss_rule = "RediSearch/version-branch -> unreleased Open Source X.Y"
    lookups = [Lookup(rs, "RediSearch/version-branch -> exact RediSearch vX.Y.Z",
                      find_release_exact(versions, rs))]
    oss = open_source_minor_unreleased(versions, X, Y)
    if oss:
        lookups.append(Lookup(oss["name"], oss_rule, oss))
    elif not open_source_minor_exists(versions, X, Y):
        # Genuinely missing -> alert. If it exists but is released/archived it has
        # already shipped (nothing to assign), so we neither add nor alert — this
        # avoids spurious alerts when reconciliation revisits historical tickets.
        lookups.append(Lookup(f"Open Source {X}.{Y}", oss_rule, None))
    return lookups


def handlers_for_repo(repo: str):
    """The handler for `repo`, or None if it has no rule.

    Only RediSearch is handled here; RediSearchEnterprise is implemented
    separately in its own repository.
    """
    return handle_redisearch if repo == REPO_REDISEARCH else None
