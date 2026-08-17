#!/usr/bin/env python3
"""Unit tests for the deterministic backport appliers.

These are the components that hold the write token, so the security-relevant
behavior under test is: the applier treats the (prompt-injectable) manifest as
data — rejecting branch/target/id values it wasn't given — and never
force-pushes. `git` and the `gh`/GraphQL helpers are stubbed; no network.

Run: python3 -m unittest discover -s scripts/auto_backport/tests
"""

from __future__ import annotations

import os
import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import common  # noqa: E402
import apply_create  # noqa: E402
import apply_fix  # noqa: E402


def _cp(returncode=0, stdout="", stderr=""):
    return types.SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


class ApplyCreateTargetTests(unittest.TestCase):
    def setUp(self):
        os.environ["RUNNER_TEMP"] = os.environ.get("TMPDIR", "/tmp")
        self.ctx = {"pr": 8774, "sha": "1a2b3c4d5e6f", "title": "[MOD-1] fix",
                    "url": "u", "body": "- [x] This PR requires release notes",
                    "targets": ["8.6", "8.2"]}
        self.gh_calls = []
        self.git_calls = []
        self._gh, self._git = common.gh, apply_create.git

        def fake_gh(*args, **kw):
            self.gh_calls.append(args)
            if args[:2] == ("pr", "list"):
                return ""                      # no existing PR
            if args[:2] == ("pr", "create"):
                return "https://github.com/RediSearch/RediSearch/pull/999\n"
            return ""
        common.gh = fake_gh

        def fake_git(work, *args, check=True):
            self.git_calls.append(args)
            if args[0] == "rev-parse":       # branch exists
                return _cp(0)
            if args[0] == "ls-remote":       # target exists
                return _cp(0)
            if args[0] == "push":
                return _cp(0)
            return _cp(0)
        apply_create.git = fake_git

    def tearDown(self):
        common.gh, apply_create.git = self._gh, self._git

    def _pushed(self):
        return any(a[0] == "push" for a in self.git_calls)

    def test_clean_target_pushes_and_opens_pr(self):
        row = apply_create.apply_target(
            self.ctx, "/w",
            {"target": "8.6", "branch": "backport-agent/pr-8774-to-8.6", "status": "clean"})
        self.assertEqual(row["status"], "clean")
        self.assertTrue(row["detail"].startswith("http"))
        self.assertTrue(self._pushed())
        self.assertTrue(any(a[:2] == ("pr", "create") for a in self.gh_calls))

    def test_branch_mismatch_is_rejected_without_push(self):
        row = apply_create.apply_target(
            self.ctx, "/w",
            {"target": "8.6", "branch": "backport-agent/pr-8774-to-EVIL", "status": "clean"})
        self.assertEqual(row["status"], "skipped")
        self.assertIn("!= expected", row["detail"])
        self.assertFalse(self._pushed())

    def test_unknown_target_is_rejected_without_push(self):
        row = apply_create.apply_target(
            self.ctx, "/w",
            {"target": "9.9", "branch": "backport-agent/pr-8774-to-9.9", "status": "clean"})
        self.assertEqual(row["status"], "skipped")
        self.assertFalse(self._pushed())

    def test_malformed_target_is_rejected(self):
        row = apply_create.apply_target(
            self.ctx, "/w",
            {"target": "8.6; rm -rf /", "branch": "x", "status": "clean"})
        self.assertEqual(row["status"], "skipped")
        self.assertFalse(self._pushed())

    def test_skipped_status_never_pushes(self):
        row = apply_create.apply_target(
            self.ctx, "/w", {"target": "8.2", "status": "skipped", "reason": "manual"})
        self.assertEqual(row["status"], "skipped")
        self.assertEqual(row["detail"], "manual")
        self.assertFalse(self._pushed())

    def test_existing_pr_is_not_reopened(self):
        common.gh = lambda *a, **k: ("OPEN https://github.com/x/y/pull/1"
                                     if a[:2] == ("pr", "list") else "")
        row = apply_create.apply_target(
            self.ctx, "/w",
            {"target": "8.6", "branch": "backport-agent/pr-8774-to-8.6", "status": "clean"})
        self.assertEqual(row["status"], "skipped")
        self.assertIn("already", row["detail"])
        self.assertFalse(self._pushed())


class ReleaseNotesTests(unittest.TestCase):
    def test_replicates_checked_requires(self):
        b = apply_create.release_notes_block("- [x] This PR requires release notes")
        self.assertIn("[x] This PR requires", b)
        self.assertIn("[ ] This PR does not require", b)

    def test_replicates_checked_does_not(self):
        b = apply_create.release_notes_block("- [x] This PR does not require release notes")
        self.assertIn("[x] This PR does not require", b)

    def test_defaults_to_requires_when_absent(self):
        b = apply_create.release_notes_block("no checkbox here")
        self.assertIn("[x] This PR requires", b)


class ApplyFixPushTests(unittest.TestCase):
    def setUp(self):
        self._git = apply_fix.git

    def tearDown(self):
        apply_fix.git = self._git

    def _stub_git(self, ahead="1", is_ancestor=0, push_rc=0):
        def fake_git(work, *args, check=True):
            if args[0] == "rev-parse" and "--verify" in args:
                return _cp(0)
            if args[0] == "rev-list":
                return _cp(0, stdout=ahead)
            if args[0] == "merge-base":
                return _cp(is_ancestor)      # 0 == ancestor == fast-forward
            if args[0] == "push":
                return _cp(push_rc)
            if args[0] == "rev-parse":
                return _cp(0, stdout="abc1234")
            return _cp(0)
        apply_fix.git = fake_git

    def test_fast_forward_push_ok(self):
        self._stub_git(ahead="1", is_ancestor=0)
        pushed, detail = apply_fix.push_fix("/w", "backport-agent/pr-1-to-8.6")
        self.assertTrue(pushed)

    def test_refuses_non_fast_forward(self):
        self._stub_git(ahead="1", is_ancestor=1)   # origin tip NOT an ancestor
        pushed, detail = apply_fix.push_fix("/w", "backport-agent/pr-1-to-8.6")
        self.assertFalse(pushed)
        self.assertIn("non-fast-forward", detail)

    def test_no_new_commit(self):
        self._stub_git(ahead="0")
        pushed, detail = apply_fix.push_fix("/w", "backport-agent/pr-1-to-8.6")
        self.assertFalse(pushed)
        self.assertEqual(detail, "no new commit to push")


class ApplyFixResolveGuardTests(unittest.TestCase):
    def setUp(self):
        self._g = common.gh_graphql

    def tearDown(self):
        common.gh_graphql = self._g

    def test_leaves_open_when_newer_non_bot_comment(self):
        def fake(query, **kw):
            if "resolveReviewThread" in query:
                raise AssertionError("must not resolve when a newer comment exists")
            return {"node": {"comments": {"nodes": [
                {"createdAt": "2026-02-01T00:00:00Z", "author": {"login": "alice"}}]}}}
        common.gh_graphql = fake
        status = apply_fix.resolve_thread_if_unchanged("T1", "2026-01-01T00:00:00Z")
        self.assertIn("left open", status)

    def test_resolves_when_unchanged(self):
        calls = []

        def fake(query, **kw):
            calls.append(query)
            if "resolveReviewThread" in query:
                return {"data": "ok"}
            return {"node": {"comments": {"nodes": [
                {"createdAt": "2026-01-01T00:00:00Z", "author": {"login": "alice"}}]}}}
        common.gh_graphql = fake
        status = apply_fix.resolve_thread_if_unchanged("T1", "2026-01-01T00:00:00Z")
        self.assertEqual(status, "resolved")
        self.assertTrue(any("resolveReviewThread" in q for q in calls))

    def test_bot_reply_does_not_block_resolve(self):
        # Only the bot commented after the snapshot → still safe to resolve.
        def fake(query, **kw):
            if "resolveReviewThread" in query:
                return {"data": "ok"}
            return {"node": {"comments": {"nodes": [
                {"createdAt": "2026-03-01T00:00:00Z",
                 "author": {"login": apply_fix.resolve_fix.BOT_LOGIN}}]}}}
        common.gh_graphql = fake
        self.assertEqual(
            apply_fix.resolve_thread_if_unchanged("T1", "2026-01-01T00:00:00Z"), "resolved")


if __name__ == "__main__":
    unittest.main()
