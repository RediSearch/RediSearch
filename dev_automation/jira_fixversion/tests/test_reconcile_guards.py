# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Offline tests for the PR-processing guards (fork / closed-unmerged) and the
dev-panel PR-URL parsing — the security-relevant fixes from PR review."""

import unittest

from dev_automation.jira_fixversion.config import Config
from dev_automation.jira_fixversion.github_client import PRMeta
from dev_automation.jira_fixversion.jira_client import PRLink, Ticket, _parse_pr_url
from dev_automation.jira_fixversion.reconcile import Agent

VERSIONS = [
    {"id": "1", "name": "RediSearch v6.6.0", "released": False},
    {"id": "2", "name": "Open Source 6.6", "released": False},
]


def _cfg(**kw):
    base = dict(jira_base_url="https://jira.example", jira_email="e", jira_token="t",
                github_token="g")
    base.update(kw)
    return Config(**base)


def vh(x, y, z):
    return (f"#define REDISEARCH_VERSION_MAJOR {x}\n"
            f"#define REDISEARCH_VERSION_MINOR {y}\n"
            f"#define REDISEARCH_VERSION_PATCH {z}\n")


class FakeGitHub:
    def __init__(self, meta):
        self.meta = meta

    def get_pull_request(self, repo, number):
        return self.meta

    def read_version_h(self, repo, ref, path="src/version.h"):
        return vh(6, 6, 0)


class FakeJira:
    def __init__(self):
        self.added = []

    def add_fix_version(self, key, version_id):
        self.added.append((key, version_id))


class FakeSlack:
    def __init__(self):
        self.alerts = []

    def alert(self, alert):
        self.alerts.append(alert)


def make_agent(meta, *, skip_fork_prs=True):
    agent = Agent(_cfg(skip_fork_prs=skip_fork_prs))
    agent.github = FakeGitHub(meta)
    agent.jira = FakeJira()
    agent.slack = FakeSlack()
    agent.versions = VERSIONS
    return agent


def meta(state="MERGED", is_fork=False):
    return PRMeta(base_branch="6.6", head_branch="fix", head_sha="abc123",
                  state=state, url="https://github.com/RediSearch/RediSearch/pull/1",
                  is_fork=is_fork)


LINK = PRLink(repo="RediSearch", number=1)


def fresh_ticket():
    # A fresh ticket per test: _apply mutates fix_version_ids, so it must not be shared.
    return Ticket(key="MOD-1", issue_id="1", components={"RediSearch"})


class TestProcessPrGuards(unittest.TestCase):
    def test_merged_internal_applies(self):
        agent = make_agent(meta(state="MERGED"))
        agent._process_pr(fresh_ticket(), LINK)
        # Both RediSearch v6.6.0 and Open Source 6.6 found and added.
        self.assertEqual({v for _, v in agent.jira.added}, {"1", "2"})

    def test_closed_unmerged_skipped(self):
        agent = make_agent(meta(state="CLOSED"))
        agent._process_pr(fresh_ticket(), LINK)
        self.assertEqual(agent.jira.added, [])
        self.assertEqual(agent.slack.alerts, [])

    def test_fork_skipped_by_default(self):
        agent = make_agent(meta(state="MERGED", is_fork=True))
        agent._process_pr(fresh_ticket(), LINK)
        self.assertEqual(agent.jira.added, [])

    def test_fork_processed_when_allowed(self):
        agent = make_agent(meta(state="MERGED", is_fork=True), skip_fork_prs=False)
        agent._process_pr(fresh_ticket(), LINK)
        self.assertEqual({v for _, v in agent.jira.added}, {"1", "2"})


class TestParsePrUrl(unittest.TestCase):
    def test_standard_url(self):
        self.assertEqual(
            _parse_pr_url("https://github.com/RediSearch/RediSearch/pull/10116"),
            ("RediSearch", 10116),
        )

    def test_enterprise_owner(self):
        self.assertEqual(
            _parse_pr_url("https://github.com/RediSearch/RediSearchEnterprise/pull/42"),
            ("RediSearchEnterprise", 42),
        )

    def test_non_matching(self):
        self.assertEqual(_parse_pr_url(""), ("", 0))
        self.assertEqual(_parse_pr_url("https://example.com/foo"), ("", 0))


if __name__ == "__main__":
    unittest.main()
