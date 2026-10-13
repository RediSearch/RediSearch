# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Exercise authorization and review side effects without live credentials."""

import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

from scripts.fast_track.approval import (
    API, LABEL, MARKER, NoRedirect, Rejected, approve, main, validate,
)


REPO = "RediSearch/RediSearch"
PATH = f"/repos/{REPO}/pulls/42"
SHA = "a" * 40
BOT = "fast-track[bot]"
PR = {
    "state": "open", "draft": False, "title": "[MOD-123] Fix query results",
    "user": {"login": "alice"}, "head": {"sha": SHA},
    "base": {"ref": "master", "repo": {"full_name": REPO}},
}


class FakeAPI:
    def __init__(self):
        self.pr = copy.deepcopy(PR)
        self.permission = "write"
        self.issue = {"key": "MOD-123", "fields": {"labels": [LABEL]}}
        self.reviews = []
        self.calls = []
        self.reads = 0
        self.change_at = None
        self.change = lambda pr: pr["head"].update(sha="b" * 40)
        self.error = None
        self.on_review_page = lambda page: None

    def request(self, path, method="GET", body=None):
        self.calls.append((path, method, body))
        if self.error:
            raise self.error
        if method == "POST":
            return {"id": 999}
        if method == "PUT":
            return {}
        if path == PATH:
            self.reads += 1
            if self.reads == self.change_at:
                self.change(self.pr)
            return copy.deepcopy(self.pr)
        if "/collaborators/" in path:
            return {"permission": self.permission}
        if path.startswith("/rest/api/3/issue/"):
            return copy.deepcopy(self.issue)
        if "/reviews?" in path:
            page = int(path.rsplit("=", 1)[1])
            self.on_review_page(page)
            return self.reviews[(page - 1) * 100:page * 100]
        raise AssertionError(path)


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.github = FakeAPI()
        self.jira = FakeAPI()

    def decision(self, requester="alice"):
        return validate(self.github, self.jira, REPO, 42, requester)

    def writes(self):
        return [(path, method, body) for path, method, body in self.github.calls
                if method != "GET"]

    def run_approval(self, decision=None):
        return approve(self.github, self.jira, decision or self.decision(), BOT,
                       "https://github.com/RediSearch/RediSearch/actions/runs/1")

    def test_eligible(self):
        decision = self.decision()
        self.assertTrue(decision.eligible)
        self.assertEqual((decision.ticket, decision.head_sha), ("MOD-123", SHA))
        self.assertEqual(self.writes(), [])

    def test_ineligible_prs(self):
        cases = [
            ("closed", lambda p: p.update(state="closed")),
            ("draft", lambda p: p.update(draft=True)),
            ("release", lambda p: p["base"].update(ref="8.8")),
            ("other_repo", lambda p: p["base"]["repo"].update(full_name="other/repo")),
            ("no_ticket", lambda p: p.update(title="Fix query")),
            ("wrong_prefix", lambda p: p.update(title="Fix MOD-123")),
            ("multiple", lambda p: p.update(title="[MOD-123] Fix MOD-456")),
            ("other_project", lambda p: p.update(title="[MOD-123] Fix RED-456")),
        ]
        for name, mutate in cases:
            with self.subTest(name=name):
                self.github.pr = copy.deepcopy(PR)
                mutate(self.github.pr)
                self.assertFalse(self.decision().eligible)
        self.assertEqual(self.jira.calls, [])
        self.assertEqual(self.writes(), [])

    def test_author_and_write_permission_required(self):
        self.assertFalse(self.decision("bob").eligible)
        for permission in ["read", "triage", "none"]:
            with self.subTest(permission=permission):
                self.github.permission = permission
                self.assertFalse(self.decision().eligible)
        self.assertEqual(self.jira.calls, [])

    def test_author_case_insensitive(self):
        self.assertTrue(self.decision("ALICE").eligible)
        self.run_approval(self.decision("ALICE"))
        self.assertEqual(len(self.writes()), 1)

    def test_exact_jira_label_and_key(self):
        for labels in [[], ["AI-fast-track"], ["ai-fast-track-extra"], LABEL, None]:
            with self.subTest(labels=labels):
                self.jira.issue["fields"]["labels"] = labels
                self.assertFalse(self.decision().eligible)
        self.jira.issue = {"key": "MOD-999", "fields": {"labels": [LABEL]}}
        self.assertFalse(self.decision().eligible)

    def test_github_label_cannot_grant_eligibility(self):
        self.github.pr["labels"] = [{"name": LABEL}]
        self.jira.issue["fields"]["labels"] = []
        self.assertFalse(self.decision().eligible)

    def test_api_failure_and_malformed_responses_fail_closed(self):
        for api in [self.github, self.jira]:
            api.error = Rejected("API unavailable")
            self.assertFalse(self.decision().eligible)
            api.error = None
        self.jira.issue = {}
        self.assertFalse(self.decision().eligible)
        self.github.pr = {}
        self.assertFalse(self.decision().eligible)
        self.assertEqual(self.writes(), [])

    def test_change_during_validation(self):
        self.github.change_at = 2
        self.assertFalse(self.decision().eligible)

    def test_approval_revalidates_jira_and_permissions(self):
        decision = self.decision()
        self.jira.issue["fields"]["labels"] = []
        with self.assertRaises(Rejected):
            self.run_approval(decision)
        self.jira.issue["fields"]["labels"] = [LABEL]
        self.github.permission = "read"
        with self.assertRaises(Rejected):
            self.run_approval(decision)
        self.assertEqual(self.writes(), [])

    def test_external_authority_revoked_during_pagination(self):
        for revoke in ("label", "permission"):
            with self.subTest(revoke=revoke):
                self.setUp()
                decision = self.decision()
                self.github.reviews = [
                    {"id": i, "user": {"login": "someone"}, "state": "COMMENTED"}
                    for i in range(100)
                ]
                pages = []

                def change_authority(page):
                    pages.append(page)
                    if page == 2:
                        if revoke == "label":
                            self.jira.issue["fields"]["labels"] = []
                        else:
                            self.github.permission = "read"

                self.github.on_review_page = change_authority
                with self.assertRaises(Rejected):
                    self.run_approval(decision)
                self.assertEqual(pages, [1, 2])
                self.assertEqual(self.writes(), [])

    def test_approval_records_exact_head_and_audit(self):
        self.run_approval()
        [(path, method, body)] = self.writes()
        self.assertEqual((path, method), (PATH + "/reviews", "POST"))
        self.assertEqual(body["commit_id"], SHA)
        self.assertEqual(body["event"], "APPROVE")
        for expected in [MARKER, "MOD-123", "@alice", "actions/runs/1"]:
            self.assertIn(expected, body["body"])

    def test_changes_before_submission_prevent_approval(self):
        for offset in [1, 2, 3]:
            with self.subTest(offset=offset):
                self.setUp()
                decision = self.decision()
                self.github.change_at = self.github.reads + offset
                with self.assertRaises(Rejected):
                    self.run_approval(decision)
                self.assertEqual(self.writes(), [])

    def test_metadata_changes_invalidate_decision(self):
        for mutate in [lambda p: p.update(title="[MOD-456] Fix query"),
                       lambda p: p.update(draft=True),
                       lambda p: p["base"].update(ref="8.8")]:
            with self.subTest(mutate=mutate):
                self.setUp()
                decision = self.decision()
                mutate(self.github.pr)
                with self.assertRaises(Rejected):
                    self.run_approval(decision)
                self.assertEqual(self.writes(), [])

    def test_change_during_submission_dismisses_review(self):
        decision = self.decision()
        self.github.change_at = self.github.reads + 4
        with self.assertRaises(Rejected):
            self.run_approval(decision)
        self.assertEqual([method for _, method, _ in self.writes()], ["POST", "PUT"])
        self.assertEqual(self.writes()[1][0], PATH + "/reviews/999/dismissals")

    def test_failed_post_submission_read_dismisses_review(self):
        decision = self.decision()
        self.github.change_at = self.github.reads + 4

        def fail(_):
            raise Rejected("GitHub unavailable")

        self.github.change = fail
        with self.assertRaisesRegex(Rejected, "GitHub unavailable"):
            self.run_approval(decision)
        self.assertEqual([method for _, method, _ in self.writes()], ["POST", "PUT"])

    def test_ineligible_decision_never_approves(self):
        self.jira.issue["fields"]["labels"] = []
        with self.assertRaises(Rejected):
            self.run_approval(self.decision())
        self.assertEqual(self.writes(), [])

    def test_approval_app_cannot_be_author(self):
        self.github.pr["user"]["login"] = BOT
        decision = self.decision(BOT)
        self.assertTrue(decision.eligible)
        with self.assertRaisesRegex(Rejected, "must not be the PR author"):
            self.run_approval(decision)
        self.assertEqual(self.writes(), [])

    def test_duplicate_and_dismissed_reviews(self):
        review = {"id": 1, "user": {"login": BOT}, "state": "APPROVED",
                  "commit_id": SHA, "body": f"{MARKER}\nTicket: MOD-123\n"}
        self.github.reviews = [review]
        self.assertIn("Already", self.run_approval())
        self.assertEqual(self.writes(), [])
        review["state"] = "DISMISSED"
        self.run_approval()
        self.assertEqual(len(self.writes()), 1)

    def test_review_pagination_and_latest_state(self):
        self.github.reviews = [
            {"id": i, "user": {"login": "someone"}, "state": "COMMENTED"}
            for i in range(100)
        ] + [{"id": 101, "user": {"login": BOT}, "state": "APPROVED",
              "commit_id": SHA, "body": f"{MARKER}\nTicket: MOD-123\n"}]
        self.assertIn("Already", self.run_approval())
        self.github.reviews.append({"id": 102, "user": {"login": BOT},
                                    "state": "DISMISSED"})
        self.run_approval()
        self.assertEqual(len(self.writes()), 1)

    def test_old_head_or_different_ticket_review_does_not_skip(self):
        for sha, ticket in [("b" * 40, "MOD-123"), (SHA, "MOD-456")]:
            with self.subTest(sha=sha, ticket=ticket):
                self.setUp()
                self.github.reviews = [{"id": 1, "user": {"login": BOT},
                    "state": "APPROVED", "commit_id": sha,
                    "body": f"{MARKER}\nTicket: {ticket}\n"}]
                self.run_approval()
                self.assertEqual(len(self.writes()), 1)

    def test_cli_denial_writes_decision_and_output(self):
        self.jira.issue["fields"]["labels"] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = {"GH_TOKEN": "test", "JIRA_EMAIL": "test", "JIRA_API_TOKEN": "test",
                   "GITHUB_REPOSITORY": REPO, "PR_NUMBER": "42", "REQUESTER": "alice",
                   "GITHUB_OUTPUT": str(root / "output"),
                   "GITHUB_STEP_SUMMARY": str(root / "summary")}
            with patch.dict(os.environ, env), patch("sys.argv", ["approval.py", "validate",
                       "--decision", str(root / "decision")]), patch(
                       "scripts.fast_track.approval.API", side_effect=[self.github, self.jira]):
                self.assertEqual(main(), 1)
            self.assertFalse(json.loads((root / "decision").read_text())["eligible"])
            self.assertEqual((root / "output").read_text(), "eligible=false\n")


class TransportTests(unittest.TestCase):
    def test_redirect_refused(self):
        with self.assertRaises(Rejected):
            NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.test")

    def test_http_error_does_not_expose_response_body(self):
        error = urllib.error.HTTPError("https://api.test", 401, "private detail", {},
                                       io.BytesIO(b"secret response"))
        with patch("urllib.request.OpenerDirector.open", side_effect=error):
            with self.assertRaisesRegex(Rejected, r"^API request failed \(HTTP 401\)$"):
                API("https://api.test", "secret").request("/issue")


if __name__ == "__main__":
    unittest.main()
