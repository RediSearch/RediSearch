# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Environment-driven configuration (design §11.3)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

MOD_PROJECT_KEY = "MOD"
ELIGIBLE_COMPONENTS = frozenset({"RedisAI", "RediSearch", "VectorSimilarity"})

# Repositories with a matching rule; anything else is a silent no-op.
# (RediSearchEnterprise is handled separately in its own repo.)
REPO_REDISEARCH = "RediSearch"
RULED_REPOS = frozenset({REPO_REDISEARCH})

VERSION_FILE_PATH = "src/version.h"


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    return default if not raw else raw in ("1", "true", "yes", "on")


@dataclass
class Config:
    jira_base_url: str
    jira_email: str
    jira_token: str
    github_token: str
    github_org: str = REPO_REDISEARCH

    # Slack Workflow Builder webhook; empty -> stub mode (log only).
    slack_webhook_url: str = ""

    project_key: str = MOD_PROJECT_KEY
    eligible_components: frozenset = field(default_factory=lambda: ELIGIBLE_COMPONENTS)

    # Skip fork PRs in both paths so external contributors can't write fixVersions
    # with the bot's credentials by referencing a MOD key.
    skip_fork_prs: bool = True

    # RediSearch release-name template; overridable for tests (e.g. "... dummy").
    redisearch_template: str = "RediSearch v{X}.{Y}.{Z}"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            jira_base_url=_require("JIRA_BASE_URL").rstrip("/"),
            jira_email=_require("JIRA_USER_EMAIL"),
            jira_token=_require("JIRA_API_TOKEN"),
            github_token=(os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN", "")).strip(),
            github_org=os.environ.get("GITHUB_ORG", "").strip() or REPO_REDISEARCH,
            slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL_FIX_VERSION", "").strip(),
            skip_fork_prs=_env_bool("JFV_SKIP_FORK_PRS", True),
            redisearch_template=(os.environ.get("JFV_REDISEARCH_RELEASE_TEMPLATE", "").strip()
                                 or "RediSearch v{X}.{Y}.{Z}"),
        )
