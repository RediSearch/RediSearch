# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Pure matching logic mapping a PR's version to Jira release(s) (design §8).

These functions are side-effect-free: they take the list of MOD project
versions and a :class:`PullRequest` and return :class:`Lookup` decisions. The
caller is responsible for applying ``fixVersions`` and raising Slack alerts, so
the matching engine can be unit-tested entirely offline.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from .version_parse import MASTER_SENTINEL, parse_version_h

# Repository names that have a defined rule (design §2). The repository decides
# which rule applies; any other repo is a silent no-op.
REPO_REDISEARCH = "RediSearch"
REPO_ENTERPRISE = "RediSearchEnterprise"

# Master is the development branch; version.h declares the 99.99.99 sentinel.
MASTER_BRANCH = "master"


@dataclass(frozen=True)
class ReleaseTemplates:
    """Format strings for the exact-name release lookups (design §8.2/§8.3).

    Defaults follow the design exactly. They are configurable so a test run can
    point at a differently-named release (e.g. a ``... dummy`` test release)
    without changing production matching semantics. Each template is formatted
    with ``X``, ``Y``, ``Z`` keyword arguments.
    """

    redisearch: str = "RediSearch v{X}.{Y}.{Z}"
    enterprise: str = "RediSearchEnterprise v{X}.{Y}.{Z}"


DEFAULT_TEMPLATES = ReleaseTemplates()


@dataclass
class PullRequest:
    """A normalized PR linked to a ticket via the Jira development panel."""

    repo: str
    head_branch: str
    base_branch: str
    pr_number: int
    state: str = ""
    head_sha: str = ""
    url: str = ""


@dataclass
class Lookup:
    """The result of looking up one expected release for a PR.

    ``release`` is the matched MOD project version (a dict with at least
    ``id``, ``name``, ``released``) or ``None`` when no such release exists —
    the latter is what triggers a Slack alert.
    """

    searched_name: str  # human-readable description of what was searched for
    rule: str  # which rule path produced this lookup (for alert context)
    release: Optional[dict]


@dataclass
class TicketDecisions:
    """All lookups produced for a single (ticket, PR) pair."""

    pr: PullRequest
    lookups: list[Lookup] = field(default_factory=list)


def is_version_branch(name: str) -> bool:
    """True for a branch named ``X.Y`` (e.g. ``2.8``, ``8.0``)."""
    return re.fullmatch(r"\d+\.\d+", name) is not None


def find_release_exact(versions: list[dict], name: str) -> Optional[dict]:
    return next((v for v in versions if v.get("name") == name), None)


def open_source_minor_unreleased(versions: list[dict], X: int, Y: int) -> Optional[dict]:
    """The unreleased two-part ``Open Source X.Y`` release.

    Open Source releases are named by major.minor only (e.g. ``Open Source 6.6``),
    so a concrete RediSearch ``X.Y.Z`` maps to ``Open Source X.Y`` — the patch is
    dropped. There is at most one such release.
    """
    return next(
        (
            v
            for v in versions
            if re.fullmatch(rf"Open Source {X}\.{Y}", v.get("name", ""))
            and not v.get("released", False)
        ),
        None,
    )


def open_source_highest_version(versions: list[dict]) -> Optional[dict]:
    """The highest unreleased two-part ``Open Source X.Y`` release (master case)."""
    cands = [
        v
        for v in versions
        if re.fullmatch(r"Open Source \d+\.\d+", v.get("name", ""))
        and not v.get("released", False)
    ]
    return max(
        cands,
        key=lambda v: tuple(map(int, v["name"].split()[-1].split("."))),
        default=None,
    )


def handle_redisearch(pr: PullRequest, versions: list[dict], version_text: str,
                      templates: ReleaseTemplates = DEFAULT_TEMPLATES) -> list[Lookup]:
    """RediSearch repository rule (design §8.2)."""
    base = pr.base_branch
    if not (is_version_branch(base) or base == MASTER_BRANCH):
        return []

    X, Y, Z = parse_version_h(version_text)

    if (X, Y, Z) == MASTER_SENTINEL:  # PR targets master
        rel = open_source_highest_version(versions)
        searched = rel["name"] if rel else "Open Source <highest unreleased X.Y>"
        return [Lookup(searched, "RediSearch/master -> highest unreleased Open Source X.Y", rel)]

    lookups: list[Lookup] = []

    rs_name = templates.redisearch.format(X=X, Y=Y, Z=Z)
    lookups.append(
        Lookup(rs_name, "RediSearch/version-branch -> exact RediSearch vX.Y.Z",
               find_release_exact(versions, rs_name))
    )

    oss = open_source_minor_unreleased(versions, X, Y)
    oss_searched = oss["name"] if oss else f"Open Source {X}.{Y}"
    lookups.append(
        Lookup(oss_searched, "RediSearch/version-branch -> unreleased Open Source X.Y", oss)
    )
    return lookups


def handle_enterprise(pr: PullRequest, versions: list[dict], version_text: str,
                      templates: ReleaseTemplates = DEFAULT_TEMPLATES) -> list[Lookup]:
    """RediSearchEnterprise repository rule (design §8.3): version branches only."""
    if not is_version_branch(pr.base_branch):  # master excluded
        return []

    X, Y, Z = parse_version_h(version_text)
    name = templates.enterprise.format(X=X, Y=Y, Z=Z)
    return [Lookup(name, "RediSearchEnterprise/version-branch -> exact RediSearchEnterprise vX.Y.Z",
                   find_release_exact(versions, name))]


def handlers_for_repo(repo: str):
    """Return the handler for ``repo``, or ``None`` if the repo has no rule."""
    if repo == REPO_REDISEARCH:
        return handle_redisearch
    if repo == REPO_ENTERPRISE:
        return handle_enterprise
    return None
