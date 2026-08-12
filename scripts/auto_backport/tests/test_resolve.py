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


class VersionFloorTests(unittest.TestCase):
    """`/backport-agent >= <version>` expansion over the release-branch registry.

    The registry is stubbed so these assertions stay stable as release lines come
    and go; RegistryFileTests covers the real file.
    """

    REGISTRY = ["2.10", "8.2", "8.4", "8.6", "8.6-rse", "8.8", "8.8-rse", "8.10"]

    def setUp(self):
        self._real_loader = resolve_create.load_release_branches
        resolve_create.load_release_branches = lambda: list(self.REGISTRY)
        self.addCleanup(setattr, resolve_create, "load_release_branches", self._real_loader)

    def _targets(self, comment: str, *labels: str) -> list[str]:
        return resolve_create.resolve_targets(
            "issue_comment", "created", "", comment, _labels(*labels),
        )

    def test_floor_expands_to_every_newer_line(self):
        self.assertEqual(self._targets("/backport-agent >= 2.10"), self.REGISTRY)

    def test_floor_without_space_is_equivalent(self):
        self.assertEqual(self._targets("/backport-agent >=2.10"), self.REGISTRY)

    def test_floor_excludes_older_lines_and_keeps_variants(self):
        # 8.4 and below drop out; `-rse` variants of included lines come along.
        self.assertEqual(
            self._targets("/backport-agent >= 8.6"),
            ["8.6", "8.6-rse", "8.8", "8.8-rse", "8.10"],
        )

    def test_floor_compares_numerically_not_lexically(self):
        # Lexically "8.10" < "8.9", which would wrongly exclude 8.10 here.
        self.assertEqual(self._targets("/backport-agent >= 8.9"), ["8.10"])

    def test_floor_matches_variant_of_the_floor_line(self):
        self.assertEqual(
            self._targets("/backport-agent >= 8.8-rse"), ["8.8", "8.8-rse", "8.10"],
        )

    def test_floor_unions_with_explicit_targets_and_dedups(self):
        self.assertEqual(
            self._targets("/backport-agent >= 8.8, 2.10, 8.8"),
            ["8.8", "8.8-rse", "8.10", "2.10"],
        )

    def test_floor_above_every_line_resolves_nothing_and_ignores_labels(self):
        # An explicit floor that matches nothing must NOT quietly fall back to
        # the PR's labels — main() then skips the run.
        self.assertEqual(self._targets("/backport-agent >= 99.0", "backport-8.6-agent"), [])

    def test_malformed_floor_is_dropped_without_label_fallback(self):
        for comment in ("/backport-agent >=", "/backport-agent >= foo",
                        "/backport-agent >= 8", "/backport-agent >=8.6x"):
            with self.subTest(comment=comment):
                self.assertEqual(self._targets(comment, "backport-8.4-agent"), [])

    def test_malformed_floor_does_not_drop_valid_siblings(self):
        self.assertEqual(self._targets("/backport-agent >= foo 8.4"), ["8.4"])

    def test_oversized_floor_is_dropped_without_crashing(self):
        # A floor with a giant component would trip int()'s digit limit in
        # version_key; it must be rejected as malformed, not abort the run, and
        # valid siblings must survive.
        huge = "9" * 5000
        self.assertEqual(self._targets(f"/backport-agent >= {huge}.1 8.4"), ["8.4"])

    def test_unavailable_registry_drops_the_floor_only(self):
        resolve_create.load_release_branches = lambda: []
        self.assertEqual(self._targets("/backport-agent >= 2.10 8.4"), ["8.4"])

    def test_registry_entries_are_validated(self):
        # A typo'd registry entry is dropped like any other malformed target.
        resolve_create.load_release_branches = lambda: ["8.6", "8.7x", "8.10"]
        self.assertEqual(self._targets("/backport-agent >= 8.6"), ["8.6", "8.10"])

    def test_floor_only_applies_to_comment_args(self):
        # A label can never carry a floor (`backport->=2.10-agent` is not a valid
        # label shape), so label-derived targets are untouched by expansion.
        targets = resolve_create.resolve_targets(
            "pull_request_target", "closed", "", "", _labels("backport-8.6-agent"),
        )
        self.assertEqual(targets, ["8.6"])


class RegistryFileTests(unittest.TestCase):
    """Guards the real `.github/release-branches.json` against a bad edit."""

    def test_file_parses_and_lists_well_formed_branches(self):
        branches = resolve_create.load_release_branches()
        self.assertTrue(branches, "release-branches.json produced no branches")
        for b in branches:
            with self.subTest(branch=b):
                self.assertRegex(b, resolve_create.TARGET_RE)

    def test_file_is_ordered_oldest_first(self):
        branches = resolve_create.load_release_branches()
        keys = [resolve_create.version_key(b) for b in branches]
        self.assertEqual(keys, sorted(keys), f"not oldest-first: {branches}")

    def test_file_has_no_duplicates(self):
        branches = resolve_create.load_release_branches()
        self.assertEqual(len(branches), len(set(branches)))


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
        self.assertFalse(got[0]["bot_replied_last"])

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

    def test_drops_thread_with_no_trusted_comment(self):
        # Only an untrusted comment in the thread → nothing actionable → dropped.
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

    def test_keeps_externally_opened_thread_with_trusted_reply(self):
        # Root is a non-write-level user, but a maintainer replied with actionable
        # feedback — keep the thread; expose only the trusted comment(s).
        self._stub([
            {
                "id": "T1", "isResolved": False, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "ext"}, "authorAssociation": "CONTRIBUTOR",
                     "body": "is this right?"},
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "no — restore the guard"},
                ]},
            },
        ])
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertEqual(len(got), 1)
        self.assertEqual([c["author"] for c in got[0]["comments"]], ["alice"])
        self.assertFalse(got[0]["bot_replied_last"])

    def test_flags_thread_bot_replied_last_as_resolution_only(self):
        # Bot already replied (resolve didn't stick) — surface it flagged, not
        # skipped, so the agent can retry the resolve without re-replying.
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
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertEqual(len(got), 1)
        self.assertTrue(got[0]["bot_replied_last"])
        # The bot's own reply is not exposed as reviewer feedback.
        self.assertEqual([c["author"] for c in got[0]["comments"]], ["alice"])

    def test_bot_replied_last_survives_trailing_untrusted_comment(self):
        # A non-bot comment after the bot's reply must NOT flip the flag off, as
        # long as no new *trusted* feedback arrived — else the next run re-does it.
        self._stub([
            {
                "id": "T1", "isResolved": False, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "fix this"},
                    {"author": {"login": resolve_fix.BOT_LOGIN},
                     "authorAssociation": "NONE", "body": "🤖 Addressed."},
                    {"author": {"login": "ext"}, "authorAssociation": "CONTRIBUTOR",
                     "body": "thanks!"},
                ]},
            },
        ])
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertTrue(got[0]["bot_replied_last"])

    def test_new_trusted_comment_after_bot_reply_reopens(self):
        # Fresh trusted feedback after the bot's reply → actionable again.
        self._stub([
            {
                "id": "T1", "isResolved": False, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "fix this"},
                    {"author": {"login": resolve_fix.BOT_LOGIN},
                     "authorAssociation": "NONE", "body": "🤖 Addressed."},
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "still broken, also handle NULL"},
                ]},
            },
        ])
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertFalse(got[0]["bot_replied_last"])

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

    def test_latest_comment_at_is_max_created(self):
        self._stub([
            {
                "id": "T1", "isResolved": False, "path": "a", "line": 1,
                "comments": {"nodes": [
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "first", "createdAt": "2026-01-01T00:00:00Z"},
                    {"author": {"login": "alice"}, "authorAssociation": "MEMBER",
                     "body": "second", "createdAt": "2026-01-04T00:00:00Z"},
                ]},
            },
        ])
        got = resolve_fix.fetch_unresolved_review_threads(1)
        self.assertEqual(got[0]["latest_comment_at"], "2026-01-04T00:00:00Z")

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

    def _c(self, id, author, body, updated_at="2026-01-01T00:00:00Z"):
        return {"id": id, "author": author, "body": body, "updated_at": updated_at}

    def test_excludes_bot_by_author_not_prose(self):
        # The bot (id=2) is dropped by AUTHOR; the `/backport-agent*` commands
        # (3,4) by prefix. dave (5) quotes the bot's `🤖 Re:` heading but is a
        # maintainer — it must be KEPT (content-based bot filtering would wrongly
        # drop it).
        self._stub([
            self._c(1, "alice", "needs the header include"),
            self._c(2, "redis-pr-app[bot]", "🤖 Auto-backport summary\n..."),
            self._c(3, "bob", "/backport-agent-context extra hint"),
            self._c(4, "carol", "/backport-agent-fix"),
            self._c(5, "dave", "🤖 Re: as you noted, this still drops the guard"),
        ])
        got = resolve_fix.fetch_general_pr_comments(1, {})
        self.assertEqual([c["id"] for c in got], [1, 5])
        self.assertTrue(all(c["kind"] == "comment" for c in got))

    def test_addressed_ids_are_dropped(self):
        # comment:1 was already replied to (and not edited since); comment:2 is
        # still open and must survive.
        self._stub([
            self._c(1, "alice", "already handled", updated_at="2026-01-01T00:00:00Z"),
            self._c(2, "bob", "still open"),
        ])
        got = resolve_fix.fetch_general_pr_comments(1, {"comment:1": "2026-01-02T00:00:00Z"})
        self.assertEqual([c["id"] for c in got], [2])

    def test_comment_edited_after_ack_is_resurfaced(self):
        # comment:1 was acked at T, then edited at T+1 → its revised feedback
        # must reappear rather than stay hidden behind the stale marker.
        self._stub([
            self._c(1, "alice", "revised: also fix the sibling", updated_at="2026-01-03T00:00:00Z"),
        ])
        got = resolve_fix.fetch_general_pr_comments(1, {"comment:1": "2026-01-02T00:00:00Z"})
        self.assertEqual([c["id"] for c in got], [1])

    def test_long_body_is_clipped(self):
        big = "x" * (resolve_fix.MAX_FEEDBACK_BODY_CHARS + 500)
        self._stub([self._c(1, "alice", big)])
        got = resolve_fix.fetch_general_pr_comments(1, {})
        self.assertLess(len(got[0]["body"]), len(big))
        self.assertTrue(got[0]["body"].endswith(resolve_fix._TRUNCATION_NOTE))

    def test_caps_number_of_items_keeping_newest(self):
        n = resolve_fix.MAX_FEEDBACK_ITEMS + 5
        self._stub([self._c(i, "alice", f"c{i}") for i in range(n)])  # oldest-first
        got = resolve_fix.fetch_general_pr_comments(1, {})
        self.assertEqual(len(got), resolve_fix.MAX_FEEDBACK_ITEMS)
        self.assertEqual(got[-1]["id"], n - 1)  # newest retained

    def test_empty_when_none(self):
        self._stub([])
        self.assertEqual(resolve_fix.fetch_general_pr_comments(1, {}), [])


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
            {"id": 10, "author": "alice", "body": "Please restore the guard."},
            {"id": 11, "author": "bob", "body": ""},  # approval with no text
            {"id": 12, "author": resolve_fix.BOT_LOGIN, "body": "🤖 something"},
        ])
        got = resolve_fix.fetch_review_bodies(1, {})
        self.assertEqual(got, [
            {"id": 10, "kind": "review", "author": "alice",
             "body": "Please restore the guard."}])

    def test_addressed_review_ids_are_dropped(self):
        self._stub([
            {"id": 10, "author": "alice", "body": "already handled"},
            {"id": 11, "author": "bob", "body": "still open"},
        ])
        got = resolve_fix.fetch_review_bodies(1, {"review:10": "2026-01-02T00:00:00Z"})
        self.assertEqual([r["id"] for r in got], [11])

    def test_body_superseded_by_later_approval_is_dropped(self):
        # alice requested changes, then approved — the withdrawn request-changes
        # body must not reach the agent.
        self._stub([
            {"id": 20, "author": "alice", "body": "please fix X",
             "state": "CHANGES_REQUESTED", "submitted_at": "2026-01-01T00:00:00Z"},
            {"id": 21, "author": "alice", "body": "",
             "state": "APPROVED", "submitted_at": "2026-01-02T00:00:00Z"},
        ])
        self.assertEqual(resolve_fix.fetch_review_bodies(1, {}), [])

    def test_request_changes_after_approval_is_kept(self):
        # A fresh review round after an approval is newer than the approval and
        # is genuine open feedback.
        self._stub([
            {"id": 20, "author": "alice", "body": "",
             "state": "APPROVED", "submitted_at": "2026-01-01T00:00:00Z"},
            {"id": 21, "author": "alice", "body": "actually, fix Y",
             "state": "CHANGES_REQUESTED", "submitted_at": "2026-01-03T00:00:00Z"},
        ])
        got = resolve_fix.fetch_review_bodies(1, {})
        self.assertEqual([r["id"] for r in got], [21])

    def test_query_excludes_dismissed_and_pending_reviews(self):
        # State-based exclusion lives in the jq (stubbed away above), so assert
        # the query drops withdrawn (DISMISSED) and not-yet-submitted (PENDING)
        # reviews rather than surfacing feedback the reviewer no longer wants.
        captured = {}

        def fake(*args, **kwargs):
            captured["jq"] = args[args.index("--jq") + 1]
            return []
        common.gh_paginated_array = fake
        resolve_fix.fetch_review_bodies(1, {})
        self.assertIn('.state != "DISMISSED"', captured["jq"])
        self.assertIn('.state != "PENDING"', captured["jq"])


class AddressedFeedbackTests(unittest.TestCase):
    def setUp(self):
        os.environ["GITHUB_REPOSITORY"] = "RediSearch/RediSearch"
        self._orig = common.gh_paginated_array

    def tearDown(self):
        common.gh_paginated_array = self._orig

    def test_maps_markers_to_latest_ack_timestamp(self):
        # The jq (stubbed) restricts to bot-authored comments; the parser maps
        # each `<kind>:<id>` marker to the newest reply that carried it.
        common.gh_paginated_array = lambda *a, **k: [
            {"created_at": "2026-01-01T00:00:00Z",
             "body": "🤖 Re: @alice.\n<!-- backport-agent-addressed: comment:123 -->"},
            {"created_at": "2026-01-05T00:00:00Z",  # a later re-ack of the same item
             "body": "🤖 Re: @alice again.\n<!-- backport-agent-addressed: comment:123 -->"},
            {"created_at": "2026-01-02T00:00:00Z",
             "body": "🤖 Re: @bob.\n<!-- backport-agent-addressed: review:456 -->"},
            {"created_at": "2026-01-03T00:00:00Z", "body": "🤖 no marker here"},
        ]
        self.assertEqual(resolve_fix.addressed_feedback(1), {
            "comment:123": "2026-01-05T00:00:00Z",
            "review:456": "2026-01-02T00:00:00Z",
        })

    def test_query_gates_on_bot_login(self):
        # A forged marker only counts if the comment is bot-authored, so the
        # query must filter on BOT_LOGIN.
        captured = {}

        def fake(*args, **kwargs):
            captured["jq"] = args[args.index("--jq") + 1]
            return []
        common.gh_paginated_array = fake
        self.assertEqual(resolve_fix.addressed_feedback(1), {})
        self.assertIn(f'.user.login == "{resolve_fix.BOT_LOGIN}"', captured["jq"])


if __name__ == "__main__":
    unittest.main()
