#!/usr/bin/env python3
"""Unit tests for the auto-backport resolve logic.

Pure-logic coverage for the two behaviors that are easy to get subtly wrong:

- `resolve_create.resolve_targets` — the target-branch derivation, in
  particular that a `labeled` event backports to ALL matching labels on the PR
  (not just the one that fired), which is what makes multi-label backports
  reliable under GitHub's "keep only the latest pending run" concurrency.
- `resolve_fix` reviewer-feedback collectors — the write-level trust gate and
  the bot/command-comment exclusions.

No network: the `gh` helpers in `common` are monkeypatched. Run with:

    python3 -m unittest discover -s scripts/auto_backport/tests
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

# The resolve modules live one directory up and import a sibling `common`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import common  # noqa: E402
import resolve_create  # noqa: E402
import resolve_fix  # noqa: E402


def _labels(*names: str) -> dict:
    return {"labels": [{"name": n} for n in names]}


class ResolveTargetsTests(unittest.TestCase):
    def test_comment_args_override_labels(self):
        targets = resolve_create.resolve_targets(
            "issue_comment", "created", "", "/backport-agent 8.6 8.2",
            _labels("backport-8.4-agent"),
        )
        self.assertEqual(targets, ["8.6", "8.2"])

    def test_labeled_event_resolves_all_matching_labels(self):
        # The just-fired label is 8.6, but the PR also carries 8.4 and 8.2 —
        # all three must be backported, not just the fired one.
        targets = resolve_create.resolve_targets(
            "pull_request_target", "labeled", "backport-8.6-agent", "",
            _labels("backport-8.6-agent", "backport-8.4-agent",
                    "backport-8.2-agent", "unrelated"),
        )
        self.assertEqual(targets, ["8.6", "8.4", "8.2"])

    def test_labeled_event_includes_fired_label_missing_from_snapshot(self):
        # Guards the eventual-consistency race: the fired label isn't yet in the
        # `gh pr view` snapshot, but must still be resolved.
        targets = resolve_create.resolve_targets(
            "pull_request_target", "labeled", "backport-8.6-agent", "",
            _labels("backport-8.4-agent"),
        )
        self.assertEqual(targets, ["8.6", "8.4"])

    def test_closed_event_resolves_all_labels(self):
        targets = resolve_create.resolve_targets(
            "pull_request_target", "closed", "", "",
            _labels("backport-8.8-agent", "backport-8.6-agent"),
        )
        self.assertEqual(targets, ["8.8", "8.6"])

    def test_dedup_preserves_order(self):
        targets = resolve_create.resolve_targets(
            "pull_request_target", "labeled", "backport-8.6-agent", "",
            _labels("backport-8.6-agent", "backport-8.6-agent",
                    "backport-8.4-agent"),
        )
        self.assertEqual(targets, ["8.6", "8.4"])

    def test_non_matching_labels_yield_no_targets(self):
        targets = resolve_create.resolve_targets(
            "pull_request_target", "closed", "", "",
            _labels("enhancement", "backport 8.6"),  # legacy label, not -agent
        )
        self.assertEqual(targets, [])


class ReviewThreadTests(unittest.TestCase):
    def setUp(self):
        os.environ["GITHUB_REPOSITORY"] = "RediSearch/RediSearch"
        self._orig = common.gh_graphql

    def tearDown(self):
        common.gh_graphql = self._orig

    def _stub(self, nodes):
        def fake(query, **kwargs):
            return {"repository": {"pullRequest": {"reviewThreads": {"nodes": nodes}}}}
        common.gh_graphql = fake

    def test_keeps_unresolved_write_level_thread(self):
        self._stub([
            {
                "id": "T1", "isResolved": False, "isOutdated": False,
                "path": "src/foo.c", "line": 12,
                "comments": {"nodes": [
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "restore the NULL check"},
                ]},
            },
        ])
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["thread_id"], "T1")
        self.assertEqual(got[0]["path"], "src/foo.c")
        self.assertEqual(got[0]["comments"][0]["author"], "alice")

    def test_drops_resolved_thread(self):
        self._stub([
            {
                "id": "T1", "isResolved": True, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "a"}, "authorAssociation": "OWNER", "body": "x"},
                ]},
            },
        ])
        self.assertEqual(resolve_fix.fetch_unresolved_review_threads(1), [])

    def test_drops_thread_opened_by_non_write_level(self):
        self._stub([
            {
                "id": "T1", "isResolved": False, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "ext"}, "authorAssociation": "CONTRIBUTOR",
                     "body": "please rename"},
                ]},
            },
        ])
        self.assertEqual(resolve_fix.fetch_unresolved_review_threads(1), [])

    def test_none_data_yields_empty(self):
        common.gh_graphql = lambda *a, **k: None
        self.assertEqual(resolve_fix.fetch_unresolved_review_threads(1), [])


class GeneralCommentTests(unittest.TestCase):
    def setUp(self):
        os.environ["GITHUB_REPOSITORY"] = "RediSearch/RediSearch"
        self._orig = common.gh_paginated_array

    def tearDown(self):
        common.gh_paginated_array = self._orig

    def _stub(self, comments):
        common.gh_paginated_array = lambda *a, **k: comments

    def test_excludes_bot_and_command_comments(self):
        # gh_paginated_array already applied the author_association --jq filter,
        # so the stub returns only write-level rows; fetch_general_pr_comments
        # then drops the bot/command bodies.
        self._stub([
            {"author": "alice", "body": "needs the header include"},
            {"author": "redis-pr-app[bot]", "body": "🤖 Auto-backport summary\n..."},
            {"author": "bob", "body": "/backport-agent-context extra hint"},
            {"author": "carol", "body": "/backport-agent-fix"},
        ])
        got = resolve_fix.fetch_general_pr_comments(1)
        self.assertEqual(got, [{"author": "alice", "body": "needs the header include"}])

    def test_empty_when_none(self):
        self._stub([])
        self.assertEqual(resolve_fix.fetch_general_pr_comments(1), [])


if __name__ == "__main__":
    unittest.main()
