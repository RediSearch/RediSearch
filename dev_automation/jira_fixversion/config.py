# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Configuration for the Jira Fix-Version Automation Agent (design §11.3).

Everything is driven by environment variables so the same code runs locally,
in a GitHub Actions workflow, or in a serverless function. Secrets are never
read from files committed to the repo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


# --- Static configuration (design §11.3) ------------------------------------

MOD_PROJECT_KEY = "MOD"
ELIGIBLE_COMPONENTS = frozenset({"RedisAI", "RediSearch", "VectorSimilarity"})

# Repositories with a defined matching rule. Anything else is a silent no-op.
REPO_REDISEARCH = "RediSearch"
REPO_ENTERPRISE = "RediSearchEnterprise"
RULED_REPOS = frozenset({REPO_REDISEARCH, REPO_ENTERPRISE})

VERSION_FILE_PATH = "src/version.h"


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Config:
    """Runtime configuration assembled from the environment."""

    # Jira (GH Actions secrets: JIRA_BASE_URL / JIRA_USER_EMAIL / JIRA_API_TOKEN)
    jira_base_url: str
    jira_email: str
    jira_token: str

    # GitHub
    github_token: str
    github_org: str = REPO_REDISEARCH  # owner org for both repos

    # Slack alerting (design §9). Preferred: a Slack Workflow Builder incoming
    # webhook (SLACK_WEBHOOK_URL_FIX_VERSION) whose body is {"payload": ...}.
    # Falls back to a bot token + channel; if neither is set, alerts are logged
    # only (stub mode) — see slack_client.SlackAlerter.
    slack_webhook_url: str = ""
    slack_bot_token: str = ""
    slack_channel_id: str = ""

    project_key: str = MOD_PROJECT_KEY
    eligible_components: frozenset = field(default_factory=lambda: ELIGIBLE_COMPONENTS)

    # Exact-name release templates (design §8.2/§8.3). Overridable via env so a
    # test run can target a differently-named release (e.g. "... dummy") without
    # changing production semantics. Each template is formatted with X, Y, Z.
    redisearch_template: str = "RediSearch v{X}.{Y}.{Z}"
    enterprise_template: str = "RediSearchEnterprise v{X}.{Y}.{Z}"

    # Skip fork PRs (head repo != base repo) in BOTH the event and scheduled
    # paths, so an external contributor cannot cause fixVersions to be written
    # with the bot's credentials by referencing a MOD key. Set false to opt in.
    skip_fork_prs: bool = True

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            jira_base_url=_require("JIRA_BASE_URL").rstrip("/"),
            jira_email=_require("JIRA_USER_EMAIL"),
            jira_token=_require("JIRA_API_TOKEN"),
            github_token=(os.environ.get("GITHUB_TOKEN")
                          or os.environ.get("GH_TOKEN", "")).strip(),
            github_org=os.environ.get("GITHUB_ORG", REPO_REDISEARCH).strip() or REPO_REDISEARCH,
            slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL_FIX_VERSION", "").strip(),
            slack_bot_token=os.environ.get("SLACK_BOT_TOKEN", "").strip(),
            slack_channel_id=os.environ.get("SLACK_RELEASES_CHANNEL_ID", "").strip(),
            redisearch_template=(os.environ.get("JFV_REDISEARCH_RELEASE_TEMPLATE", "").strip()
                                 or "RediSearch v{X}.{Y}.{Z}"),
            enterprise_template=(os.environ.get("JFV_ENTERPRISE_RELEASE_TEMPLATE", "").strip()
                                 or "RediSearchEnterprise v{X}.{Y}.{Z}"),
            skip_fork_prs=_env_bool("JFV_SKIP_FORK_PRS", True),
        )
