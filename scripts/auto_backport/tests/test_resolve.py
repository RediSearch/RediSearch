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

    def test_plain_comment_falls_back_to_labels(self):
        # Plain `/backport-agent` (no args) must still backport to every label
        # on the PR — not silently resolve nothing.
        targets = resolve_create.resolve_targets(
            "issue_comment", "created", "", "/backport-agent",
            _labels("backport-8.6-agent", "backport-8.4-agent"),
        )
        self.assertEqual(targets, ["8.6", "8.4"])

    def test_malformed_comment_targets_are_dropped(self):
        targets = resolve_create.resolve_targets(
            "issue_comment", "created", "", "/backport-agent 8.6 foo 8.4x 8.2",
            _labels(),
        )
        self.assertEqual(targets, ["8.6", "8.2"])

    def test_variant_and_multidigit_targets_are_valid(self):
        targets = resolve_create.resolve_targets(
            "pull_request_target", "closed", "", "",
            _labels("backport-8.6-rse-agent", "backport-8.10-agent",
                    "backport-experimental-agent"),  # dropped: not MAJOR.MINOR
        )
        self.assertEqual(targets, ["8.6-rse", "8.10"])


class ReviewThreadTests(unittest.TestCase):
    def setUp(self):
        os.environ["GITHUB_REPOSITORY"] = "RediSearch/RediSearch"
        self._orig = common.gh_graphql

    def tearDown(self):
        common.gh_graphql = self._orig

    def _stub(self, nodes, has_next=False, end_cursor=None):
        # Single-page stub unless has_next is set (then a second call returns an
        # empty terminal page, so the pagination loop exercises the cursor).
        pages = [{"nodes": nodes, "pageInfo": {"hasNextPage": has_next,
                                               "endCursor": end_cursor}}]
        if has_next:
            pages.append({"nodes": [], "pageInfo": {"hasNextPage": False,
                                                    "endCursor": None}})
        state = {"i": 0}

        def fake(query, **kwargs):
            page = pages[min(state["i"], len(pages) - 1)]
            state["i"] += 1
            return {"repository": {"pullRequest": {"reviewThreads": page}}}
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

    def test_skips_thread_bot_replied_last(self):
        # Bot already replied and is awaiting the reviewer — don't re-surface.
        self._stub([
            {
                "id": "T1", "isResolved": False, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "fix this"},
                    {"author": {"login": resolve_fix.BOT_LOGIN},
                     "authorAssociation": "NONE", "body": "🤖 Addressed in abc123."},
                ]},
            },
        ])
        self.assertEqual(resolve_fix.fetch_unresolved_review_threads(1), [])

    def test_paginates_across_pages(self):
        self._stub(
            [
                {
                    "id": "T1", "isResolved": False, "path": "a", "line": 1,
                    "comments": {"nodes": [
                        {"author": {"login": "alice"}, "authorAssociation": "OWNER",
                         "body": "page one"},
                    ]},
                },
            ],
            has_next=True, end_cursor="CURSOR",
        )
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertEqual([t["thread_id"] for t in got], ["T1"])

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
            {"id": 1, "author": "alice", "body": "needs the header include",
             "created_at": "2026-01-01T00:00:00Z"},
            {"id": 2, "author": "redis-pr-app[bot]",
             "body": "🤖 Auto-backport summary\n...", "created_at": "2026-01-01T00:00:00Z"},
            {"id": 3, "author": "bob", "body": "/backport-agent-context extra hint",
             "created_at": "2026-01-01T00:00:00Z"},
            {"id": 4, "author": "carol", "body": "/backport-agent-fix",
             "created_at": "2026-01-01T00:00:00Z"},
            {"id": 5, "author": "dave", "body": "🤖 Re: @alice — addressed",
             "created_at": "2026-01-01T00:00:00Z"},
        ])
        got = resolve_fix.fetch_general_pr_comments(1, None)
        self.assertEqual(
            got, [{"id": 1, "author": "alice", "body": "needs the header include"}])

    def test_since_cutoff_drops_older_comments(self):
        self._stub([
            {"id": 1, "author": "alice", "body": "old feedback",
             "created_at": "2026-01-01T00:00:00Z"},
            {"id": 2, "author": "bob", "body": "new feedback",
             "created_at": "2026-01-03T00:00:00Z"},
        ])
        got = resolve_fix.fetch_general_pr_comments(1, "2026-01-02T00:00:00Z")
        self.assertEqual(got, [{"id": 2, "author": "bob", "body": "new feedback"}])

    def test_empty_when_none(self):
        self._stub([])
        self.assertEqual(resolve_fix.fetch_general_pr_comments(1, None), [])


class ReviewBodyTests(unittest.TestCase):
    def setUp(self):
        os.environ["GITHUB_REPOSITORY"] = "RediSearch/RediSearch"
        self._orig = common.gh_paginated_array

    def tearDown(self):
        common.gh_paginated_array = self._orig

    def _stub(self, reviews):
        common.gh_paginated_array = lambda *a, **k: reviews

    def test_collects_write_level_nonempty_review_bodies(self):
        self._stub([
            {"id": 10, "author": "alice", "body": "Please restore the guard.",
             "submitted_at": "2026-01-03T00:00:00Z"},
            {"id": 11, "author": "bob", "body": "",  # approval with no text
             "submitted_at": "2026-01-03T00:00:00Z"},
            {"id": 12, "author": resolve_fix.BOT_LOGIN, "body": "🤖 something",
             "submitted_at": "2026-01-03T00:00:00Z"},
        ])
        got = resolve_fix.fetch_review_bodies(1, None)
        self.assertEqual(
            got, [{"id": 10, "author": "alice", "body": "Please restore the guard."}])

    def test_since_cutoff_applies(self):
        self._stub([
            {"id": 10, "author": "alice", "body": "old", "submitted_at": "2026-01-01T00:00:00Z"},
            {"id": 11, "author": "bob", "body": "new", "submitted_at": "2026-01-03T00:00:00Z"},
        ])
        got = resolve_fix.fetch_review_bodies(1, "2026-01-02T00:00:00Z")
        self.assertEqual(got, [{"id": 11, "author": "bob", "body": "new"}])


class LastFixTimestampTests(unittest.TestCase):
    def setUp(self):
        os.environ["GITHUB_REPOSITORY"] = "RediSearch/RediSearch"
        self._orig = common.gh_paginated_array

    def tearDown(self):
        common.gh_paginated_array = self._orig

    def test_returns_latest_fix_attempt_stamp(self):
        common.gh_paginated_array = lambda *a, **k: [
            "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z", "2026-01-03T00:00:00Z",
        ]
        self.assertEqual(
            resolve_fix.last_fix_attempt_timestamp(1), "2026-01-05T00:00:00Z")

    def test_none_when_no_prior_fix(self):
        common.gh_paginated_array = lambda *a, **k: []
        self.assertIsNone(resolve_fix.last_fix_attempt_timestamp(1))


if __name__ == "__main__":
    unittest.main()
