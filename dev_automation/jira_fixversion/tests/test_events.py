# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Offline unit tests for the event-payload parsing (design §5.1)."""

import unittest

from dev_automation.jira_fixversion.events import extract_issue_keys, parse_pr_event


def pr_payload(*, action="opened", number=10, head_ref="MOD-1-foo", base_ref="master",
               title="t", body="b", head_repo="RediSearch/RediSearch",
               base_repo="RediSearch/RediSearch", merged=False, state="open",
               repo_name="RediSearch"):
    return {
        "action": action,
        "repository": {"name": repo_name},
        "pull_request": {
            "number": number,
            "title": title,
            "body": body,
            "merged": merged,
            "state": state,
            "html_url": f"https://github.com/{base_repo}/pull/{number}",
            "head": {"ref": head_ref, "sha": "deadbeef", "repo": {"full_name": head_repo}},
            "base": {"ref": base_ref, "repo": {"full_name": base_repo}},
        },
    }


class TestExtractIssueKeys(unittest.TestCase):
    def test_from_branch_title_body(self):
        keys = extract_issue_keys("MOD-123-fix", "Fix MOD-456", "see mod-789 and MOD-123")
        self.assertEqual(keys, ["MOD-123", "MOD-456", "MOD-789"])  # unique, order, upper

    def test_none(self):
        self.assertEqual(extract_issue_keys("no keys here", "", None), [])

    def test_word_boundary(self):
        # Avoid matching substrings inside other tokens.
        self.assertEqual(extract_issue_keys("XMOD-1", "AMOD-2bar"), [])


class TestParsePrEvent(unittest.TestCase):
    def test_basic(self):
        ev = parse_pr_event(pr_payload(number=42, base_ref="6.6", head_ref="MOD-9-x"))
        self.assertEqual(ev.repo, "RediSearch")
        self.assertEqual(ev.number, 42)
        self.assertEqual(ev.base_ref, "6.6")
        self.assertFalse(ev.is_fork)
        self.assertEqual(ev.state, "OPEN")

    def test_merged_state(self):
        ev = parse_pr_event(pr_payload(merged=True, state="closed"))
        self.assertEqual(ev.state, "MERGED")

    def test_fork_detected(self):
        ev = parse_pr_event(pr_payload(head_repo="someuser/RediSearch",
                                       base_repo="RediSearch/RediSearch"))
        self.assertTrue(ev.is_fork)

    def test_not_a_pr_event(self):
        self.assertIsNone(parse_pr_event({"action": "created", "issue": {}}))


if __name__ == "__main__":
    unittest.main()
