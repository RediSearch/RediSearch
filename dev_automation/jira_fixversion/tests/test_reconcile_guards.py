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
    def __init__(self, meta, by_number=None):
        self.meta = meta            # default meta for any PR
        self.by_number = by_number or {}  # optional per-PR-number override

    def get_pull_request(self, repo, number, owner=""):
        return self.by_number.get(number, self.meta)

    def read_version_h(self, repo, ref, path="src/version.h", owner=""):
        return vh(6, 6, 0)


class FakeJira:
    def __init__(self, links=None):
        self.added = []
        self.removed = []
        self.links = links or []  # returned by get_linked_prs

    def add_fix_version(self, key, version_id):
        self.added.append((key, version_id))

    def remove_fix_version(self, key, version_id):
        self.removed.append((key, version_id))

    def get_linked_prs(self, issue_id):
        return self.links


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


def fresh_ticket(fix_version_ids=None):
    # A fresh ticket per test: _apply/_remove mutate fix_version_ids, so it must not be shared.
    return Ticket(key="MOD-1", issue_id="1", components={"RediSearch"},
                  fix_version_ids=set(fix_version_ids or ()))


class TestProcessPrGuards(unittest.TestCase):
    def test_merged_internal_applies(self):
        agent = make_agent(meta(state="MERGED"))
        agent._process_pr(fresh_ticket(), LINK)
        # Both RediSearch v6.6.0 and Open Source 6.6 found and added.
        self.assertEqual({v for _, v in agent.jira.added}, {"1", "2"})

    def test_closed_unmerged_removes_present_fixversions(self):
        # Closed without merge -> roll back the fix versions this PR maps to.
        agent = make_agent(meta(state="CLOSED"))
        agent._process_pr(fresh_ticket(fix_version_ids={"1", "2"}), LINK)
        self.assertEqual({v for _, v in agent.jira.removed}, {"1", "2"})
        self.assertEqual(agent.jira.added, [])
        self.assertEqual(agent.slack.alerts, [])

    def test_closed_unmerged_noop_when_absent(self):
        # Nothing on the ticket to roll back -> no writes, no alerts.
        agent = make_agent(meta(state="CLOSED"))
        agent._process_pr(fresh_ticket(), LINK)
        self.assertEqual(agent.jira.removed, [])
        self.assertEqual(agent.jira.added, [])
        self.assertEqual(agent.slack.alerts, [])

    def test_closed_unmerged_keeps_releases_other_pr_needs(self):
        # PR #1 closed-unmerged maps to {1,2}; sibling #2 is MERGED and maps to the
        # same releases -> nothing removed (another live PR still justifies them).
        agent = make_agent(meta(state="CLOSED"),
                           )  # default meta = CLOSED for #1
        agent.github.by_number = {2: meta(state="MERGED")}
        agent.jira.links = [PRLink(repo="RediSearch", number=1),
                            PRLink(repo="RediSearch", number=2)]
        agent._process_pr(fresh_ticket(fix_version_ids={"1", "2"}), LINK)
        self.assertEqual(agent.jira.removed, [])

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
            ("RediSearch", "RediSearch", 10116),
        )

    def test_fork_owner_preserved(self):
        # A fork URL keeps its owner so it isn't queried under our org.
        self.assertEqual(
            _parse_pr_url("https://github.com/alice/RediSearch/pull/7"),
            ("alice", "RediSearch", 7),
        )

    def test_non_matching(self):
        self.assertEqual(_parse_pr_url(""), ("", "", 0))
        self.assertEqual(_parse_pr_url("https://example.com/foo"), ("", "", 0))


if __name__ == "__main__":
    unittest.main()
