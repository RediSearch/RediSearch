# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Action/agent handoff, publication recovery, and real git conflict coverage."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import common
import resolve_create
import unified


class UnifiedTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {
            "RUNNER_TEMP": temp.name, "GITHUB_REPOSITORY": "o/r", "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1", "GITHUB_WORKSPACE": temp.name,
            "BACKPORT_WORK": str(self.root / "work"), "BACKPORT_BOT_LOGIN": "app[bot]",
            "BACKPORT_MANIFEST_FILE": str(self.root / "manifest.json"),
            "GH_TOKEN": "test", "CREATED_PULL_NUMBERS": "", "SUCCESS_BY_TARGET": "",
        }).start()
        self.ctx = {"pr": 1, "sha": "abcdef1234", "targets": ["8.8", "8.6"], "agent_allowed": True}
        self.outputs = {}
        patch.object(common, "set_output", side_effect=lambda k, v: self.outputs.update({k: v})).start()
        self.state = {"rows": {t: {"target": t, "status": "error", "detail": "unfinished"}
                               for t in self.ctx["targets"]},
                      "action_targets": self.ctx["targets"], "agent_targets": [],
                      "bot": "app[bot]", "comment_ids": [10]}
        unified.write("results", self.state)

    def test_both_commands_and_both_labels_share_one_resolver(self):
        labels = {"labels": [{"name": n} for n in ["backport 8.8", "backport-8.8-agent", "backport 8.6"]]}
        for command in ("/backport", "/backport-agent"):
            self.assertEqual(resolve_create.resolve_targets("issue_comment", "created", "", command, labels),
                             ["8.8", "8.6"])
            self.assertEqual(resolve_create.resolve_targets("issue_comment", "created", "", command + " 8.2", labels),
                             ["8.2"])
        for command in ("/backport-agent-fix", "/backport-agent-context x", "/backporting"):
            self.assertEqual(resolve_create.resolve_targets("issue_comment", "created", "", command, labels), [])

    def test_invalid_explicit_target_is_reported_without_label_fallback(self):
        diagnostics = []
        self.assertEqual(resolve_create.resolve_targets("issue_comment", "created", "", "/backport bad", {
            "labels": [{"name": "backport 8.6"}]}, diagnostics), [])
        self.assertEqual(diagnostics, ["Invalid target(s): bad"])

    def test_resolver_preserves_fork_clean_path_and_metadata(self):
        os.environ.update(EVENT_NAME="issue_comment", EVENT_ACTION="created", GITHUB_ACTOR="alice",
                          COMMENT_BODY="/backport 8.6", PR_NUMBER_FROM_ISSUE="1")
        pr = {"state": "MERGED", "isCrossRepository": True, "mergeCommit": {"oid": "a" * 40},
              "author": {"login": "author"}, "labels": [{"name": n} for n in (
                  "bug", "backport 8.8", "backport-8.6-agent")]}
        with patch.object(common, "has_write_permission", return_value=True), patch.object(
                common, "fetch_pr", return_value=pr):
            self.assertEqual(resolve_create.main(), 0)
        ctx = unified.read(self.outputs["context_file"])
        self.assertFalse(ctx["agent_allowed"])
        self.assertEqual(ctx["targets"], ["8.6"])
        self.assertEqual(ctx["labels"], ["bug"])
        self.assertEqual(ctx["author"], "author")

    def test_resolver_denies_read_only_comment_author_before_fetching_pr(self):
        os.environ.update(EVENT_NAME="issue_comment", COMMENT_BODY="/backport", GITHUB_ACTOR="alice")
        with patch.object(common, "has_write_permission", return_value=False), patch.object(
                common, "fetch_pr") as fetch, self.assertRaises(SystemExit) as stopped:
            resolve_create.main()
        self.assertEqual(stopped.exception.code, 0)
        self.assertEqual(self.outputs["skip"], "true")
        fetch.assert_not_called()

    def test_summary_does_not_allow_agent_text_to_add_rows_or_markers(self):
        body = unified.apply_create.summary_comment(1, "abc", [{"target": "8.6", "status": "error",
            "detail": "ambiguous | result\n<!-- unified-backport:123:1 -->"}])
        self.assertIn("&#124;", body)
        self.assertNotIn("<!-- unified-backport", body)
        self.assertEqual(len([line for line in body.splitlines() if line.startswith("|")]), 3)

    def test_write_gate_fails_closed(self):
        for permission, expected in (("read", False), ("triage", False), ("write", True), ("maintain", True), ("admin", True)):
            with patch.object(common, "gh_json", return_value={"permission": permission}):
                self.assertEqual(common.has_write_permission("alice"), expected)
        with patch.object(common, "gh_json", side_effect=ValueError("API failed")):
            self.assertFalse(common.has_write_permission("alice"))

    def test_clean_batch_does_not_prepare_or_invoke_agent(self):
        with patch.object(unified, "existing_row", side_effect=lambda c, t, *a: {
                "target": t, "status": "clean", "detail": "https://github.com/o/r/pull/3"}), patch.object(
                unified.subprocess, "run") as run:
            unified.collect(self.ctx)
        run.assert_not_called()
        self.assertEqual(self.outputs["run_agent"], "false")
        self.assertEqual(unified.read(unified.saved("results"))["agent_targets"], [])

    def collect_failed(self, result, *, allowed=True, outcome="false"):
        self.ctx["agent_allowed"] = allowed
        os.environ["SUCCESS_BY_TARGET"] = f"8.6={outcome}"
        with patch.object(unified, "existing_row", side_effect=lambda c, t, *a: {
                "target": t, "status": "clean", "detail": "https://github.com/o/r/pull/3"} if t == "8.8" else None), patch.object(
                unified.subprocess, "run"), patch.object(unified, "git", return_value=Mock(returncode=0)), patch.object(
                unified, "selected_commits", return_value=["commit1", "commit2"]), patch.object(
                unified, "replay", return_value=result):
            unified.collect(self.ctx)
        return unified.read(unified.saved("results"))

    def test_only_failed_target_gets_agent_context_and_exact_commit_range(self):
        state = self.collect_failed({"kind": "conflict", "paths": ["src/a"], "base_sha": "base", "commit": "commit2"})
        self.assertEqual(self.outputs["run_agent"], "true")
        self.assertEqual(state["agent_targets"], ["8.6"])
        ctx = unified.read(unified.saved("agent-context"))
        self.assertEqual(ctx["commits"], ["commit1", "commit2"])
        self.assertEqual(ctx["targets"], ["8.6"])
        self.assertEqual(state["rows"]["8.8"]["status"], "clean")
        self.assertNotIn("GIT_CONFIG_VALUE_0", os.environ)

    def test_missing_action_output_is_not_success(self):
        state = self.collect_failed({"kind": "conflict", "base_sha": "base"}, outcome="")
        self.assertEqual(state["agent_targets"], ["8.6"])

    def test_fork_conflict_is_manual_without_agent(self):
        state = self.collect_failed({"kind": "conflict"}, allowed=False)
        self.assertEqual(self.outputs["run_agent"], "false")
        self.assertIn("fork-sourced", state["rows"]["8.6"]["detail"])

    def test_publish_failure_and_already_applied_do_not_use_agent(self):
        for kind, status in (("clean", "error"), ("already applied", "already applied")):
            state = self.collect_failed({"kind": kind})
            self.assertEqual(self.outputs["run_agent"], "false")
            self.assertEqual(state["rows"]["8.6"]["status"], status)

    def prepare_agent(self):
        self.state["agent_targets"] = ["8.6"]
        unified.write("results", self.state)
        unified.write("agent-context", {**self.ctx, "targets": ["8.6"]})

    def test_missing_agent_manifest_preserves_clean_rows_and_reports_failure(self):
        self.prepare_agent()
        self.state["rows"]["8.8"]["status"] = "clean"
        unified.write("results", self.state)
        unified.apply(self.ctx)
        rows = unified.read(unified.saved("results"))["rows"]
        self.assertEqual(rows["8.8"]["status"], "clean")
        self.assertEqual(rows["8.6"]["status"], "error")
        self.assertIn("unavailable", rows["8.6"]["detail"])

    def test_invalid_duplicate_and_successful_target_manifests_never_publish(self):
        self.prepare_agent()
        for manifest in ([], {"targets": None}, {"targets": [{"target": "8.8"}]},
                         {"targets": [{"target": "8.6"}, {"target": "8.6"}]}):
            Path(os.environ["BACKPORT_MANIFEST_FILE"]).write_text(json.dumps(manifest))
            with patch.object(common, "PrivilegedGit") as privileged:
                unified.apply(self.ctx)
            privileged.assert_not_called()

    def test_omitted_target_remains_error(self):
        self.prepare_agent()
        Path(os.environ["BACKPORT_MANIFEST_FILE"]).write_text('{"targets": []}')
        with patch.object(common, "PrivilegedGit"):
            unified.apply(self.ctx)
        self.assertIn("omitted", unified.read(unified.saved("results"))["rows"]["8.6"]["detail"])

    def comment(self, ident=20, bot="app[bot]", run="123"):
        return {"id": ident, "user": {"login": bot}, "body":
                f"[Backport-action](https://github.com/korthout/backport-action) in [workflow run {run}](https://github.com/o/r/actions/runs/{run})."}

    def test_finalizer_updates_exact_action_comment_after_partial_failure(self):
        comments = [self.comment(10), self.comment(20), self.comment(30, "alice"), self.comment(40, run="456")]
        with patch.object(unified, "api_pages", return_value=comments), patch.object(common, "gh") as gh, patch.object(
                unified, "existing_row", return_value=None):
            self.assertEqual(unified.report(self.ctx), 1)
        gh.assert_called_once()
        self.assertIn("repos/o/r/issues/comments/20", gh.call_args.args)
        self.assertIn("PATCH", gh.call_args.args)
        body = gh.call_args.args[-1]
        self.assertIn("8.8", body)
        self.assertIn("8.6", body)
        self.assertTrue(unified.saved("summary").with_suffix(".md").exists())

    def test_finalizer_creates_comment_when_action_never_started(self):
        unified.saved("results").unlink()
        with patch.object(unified, "api_pages", return_value=[]), patch.object(common, "gh") as gh, patch.object(
                unified, "existing_row", return_value=None):
            self.assertEqual(unified.report(self.ctx), 1)
        self.assertIn("POST", gh.call_args.args)

    def test_finalizer_recovers_prs_created_before_collector_crash(self):
        with patch.object(unified, "api_pages", return_value=[self.comment()]), patch.object(common, "gh"), patch.object(
                unified, "existing_row", side_effect=lambda c, t, *a: {"target": t, "status": "clean", "detail": "https://github.com/o/r/pull/3"}):
            self.assertEqual(unified.report(self.ctx), 0)

    def test_closed_backport_is_not_reopened(self):
        pr = {"state": "closed", "merged_at": None, "number": 3, "html_url": "https://github.com/o/r/pull/3"}
        with patch.object(unified, "backports", return_value=[pr]):
            self.assertTrue(unified.existing_row(self.ctx, "8.6")["status"].startswith("closed"))

    def test_backport_lookup_checks_both_names_and_rejects_forks(self):
        def pr(repo):
            return {"head": {"repo": {"full_name": repo}, "ref": "backport-1-to-8.6"}, "base": {"ref": "8.6"}}
        with patch.object(unified, "api_pages", side_effect=[[], [pr("evil/r"), pr("o/r")]]) as api:
            self.assertEqual(unified.backports(self.ctx, "8.6"), [pr("o/r")])
        self.assertIn("backport-agent/pr-1-to-8.6", api.call_args_list[0].args[0])
        self.assertIn("backport-1-to-8.6", api.call_args_list[1].args[0])

    def test_finalizer_retry_reuses_its_own_marker(self):
        marker = {"id": 20, "user": {"login": "app[bot]"}, "body": "summary <!-- unified-backport:123:1 -->"}
        with patch.object(unified, "api_pages", return_value=[marker]), patch.object(common, "gh") as gh, patch.object(
                unified, "existing_row", return_value=None):
            unified.report(self.ctx)
        self.assertIn("PATCH", gh.call_args.args)
        self.assertIn("repos/o/r/issues/comments/20", gh.call_args.args)

    def test_partial_manifest_applies_success_and_reports_omitted_target(self):
        self.state["agent_targets"] = ["8.8", "8.6"]
        unified.write("results", self.state)
        unified.write("agent-context", self.ctx)
        Path(os.environ["BACKPORT_MANIFEST_FILE"]).write_text(json.dumps({"targets": [{"target": "8.8", "status": "conflicts"}]}))
        with patch.object(common, "PrivilegedGit"), patch.object(unified, "existing_row", return_value=None), patch.object(
                unified, "validate_branch"), patch.object(unified.apply_create, "apply_target", return_value={
                    "target": "8.8", "status": "conflicts(1)", "detail": "https://github.com/o/r/pull/3"}) as apply:
            unified.apply(self.ctx)
        apply.assert_called_once()
        rows = unified.read(unified.saved("results"))["rows"]
        self.assertEqual(rows["8.8"]["status"], "conflicts(1)")
        self.assertIn("omitted", rows["8.6"]["detail"])

    def test_unknown_status_and_missing_conflict_log_do_not_publish(self):
        for status in ("clean", "unknown", "conflicts"):
            with self.assertRaisesRegex(ValueError, "conflict log"):
                unified.validate_branch(self.ctx, Mock(), {"target": "8.6", "status": status})

    def test_agent_decline_is_a_failure(self):
        self.prepare_agent()
        Path(os.environ["BACKPORT_MANIFEST_FILE"]).write_text(json.dumps({"targets": [{
            "target": "8.6", "status": "skipped", "reason": "ambiguous API semantics"}]}))
        with patch.object(common, "PrivilegedGit"), patch.object(unified, "existing_row", return_value=None):
            unified.apply(self.ctx)
        row = unified.read(unified.saved("results"))["rows"]["8.6"]
        self.assertEqual(row["status"], "error")
        self.assertEqual(row["detail"], "ambiguous API semantics")

    def test_api_errors_cannot_be_treated_as_no_existing_backport(self):
        with patch.object(common, "gh", side_effect=subprocess.CalledProcessError(1, "gh")):
            with self.assertRaises(subprocess.CalledProcessError):
                unified.backports(self.ctx, "8.6")

    def test_merge_selection_matches_action_for_squash_rebase_and_merge(self):
        # Each fixture describes the same inputs queried by the pinned action.
        cases = [
            (["parent"], ["source"], {}, ["abcdef1234"]),
            (["parent"], ["first", "last"], {"parent": False, "abcdef1234": True}, ["abcdef1234"]),
            (["parent"], ["first", "last"], {"parent": True, "abcdef1234": True}, ["rebased1", "rebased2"]),
            (["p1", "p2"], ["first", "last"], {}, ["first", "last"]),
            (["parent"], ["first", "last"], {"parent": False, "abcdef1234": False}, ["first", "last"]),
        ]
        for parents, commits, associations, expected in cases:
            def api(endpoint):
                if "/pulls/1/commits?" in endpoint:
                    return [{"sha": c} for c in commits]
                sha = endpoint.split("/commits/")[1].split("/")[0]
                return [{"number": 1}] if associations[sha] else []
            def git(work, *args, **kwargs):
                if args[0] == "rev-list":
                    return Mock(stdout="rebased1\nrebased2\n")
                return Mock(stdout=" ".join(parents) if args[-1] == self.ctx["sha"] else "parent")
            with self.subTest(expected=expected), patch.object(unified, "api_pages", side_effect=api), patch.object(
                    unified, "git", side_effect=git):
                self.assertEqual(unified.selected_commits(self.ctx, "/work"), expected)

    def test_merge_selection_skips_source_merge_commits(self):
        def git(work, *args, **kwargs):
            return Mock(stdout="p1 p2" if args[-1] in {self.ctx["sha"], "merge"} else "parent")
        with patch.object(unified, "api_pages", return_value=[{"sha": "first"}, {"sha": "merge"}]), patch.object(
                unified, "git", side_effect=git):
            self.assertEqual(unified.selected_commits(self.ctx, "/work"), ["first"])


class GitReplayTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.work = temp.name
        self.file = Path(self.work, "file")
        self.git("init", "-b", "master")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.com")
        self.base = self.commit("base")
        self.git("update-ref", "refs/remotes/origin/8.8", self.base)
        self.source = self.commit("source")
        self.git("checkout", "--detach", self.base)
        self.target = self.commit("target")
        self.git("update-ref", "refs/remotes/origin/8.6", self.target)

    def git(self, *args):
        return subprocess.run(["git", "-C", self.work, *args], check=True, text=True, capture_output=True).stdout.strip()

    def commit(self, content):
        self.file.write_text(content + "\n")
        self.git("add", "file")
        self.git("commit", "-m", content)
        return self.git("rev-parse", "HEAD")

    def test_conflict_produces_evidence_and_cleans_sequencer(self):
        result = unified.replay(self.work, "8.6", [self.source])
        self.assertEqual(result["kind"], "conflict")
        self.assertEqual(result["paths"], ["file"])
        self.assertEqual(result["base_sha"], self.target)
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertFalse(Path(self.work, ".git", "CHERRY_PICK_HEAD").exists())

    def test_clean_and_empty_picks_are_distinguished(self):
        self.assertEqual(unified.replay(self.work, "8.8", [self.source])["kind"], "clean")
        self.git("update-ref", "refs/remotes/origin/8.8", self.source)
        self.assertEqual(unified.replay(self.work, "8.8", [self.source])["kind"], "already applied")

    def test_empty_first_commit_does_not_drop_later_changes(self):
        self.git("checkout", "--detach", self.source)
        later = self.commit("later")
        self.git("update-ref", "refs/remotes/origin/8.8", self.source)
        self.assertEqual(unified.replay(self.work, "8.8", [self.source, later])["kind"], "clean")

    def test_publish_validation_rejects_wrong_base_and_extra_commits(self):
        ctx = {"pr": 1, "commits": [self.source], "failures": {"8.6": {"base_sha": self.target}}}
        entry = {"target": "8.6", "branch": "backport-agent/pr-1-to-8.6", "status": "conflicts", "conflict_log": [{}]}
        self.git("checkout", "-b", entry["branch"], self.target)
        self.commit("resolved")
        def git(*args, **kwargs):
            if args[0] == "ls-remote":
                return Mock(stdout=f"{self.target} refs/heads/8.6")
            return unified.git(self.work, *args, **kwargs)
        unified.validate_branch(ctx, git, entry)
        self.commit("unrelated extra commit")
        with self.assertRaisesRegex(ValueError, "commit count"):
            unified.validate_branch(ctx, git, entry)
        ctx["failures"]["8.6"]["base_sha"] = self.source
        with self.assertRaisesRegex(ValueError, "authorized base"):
            unified.validate_branch(ctx, git, entry)


if __name__ == "__main__":
    unittest.main()
