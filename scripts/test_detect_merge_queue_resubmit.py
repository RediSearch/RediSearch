#!/usr/bin/env python3
"""Tests for the merge queue resubmission detection script."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from detect_merge_queue_resubmit import (
    analyze_failed_jobs,
    build_alert_payload,
    extract_pr_number,
    is_resubmission,
    parse_failure_from_logs,
    simplify_job_name,
)


# Helper to create payload with minimal boilerplate
def make_payload(analyzed_jobs, run_id=123, pr_number=1):
    return build_alert_payload(
        pr_number=pr_number,
        pr_title="Title",
        pr_author="author",
        pr_url=f"https://github.com/owner/repo/pull/{pr_number}",
        previous_run={"id": run_id},
        analyzed_jobs=analyzed_jobs,
        repo="owner/repo",
    )


class TestExtractPrNumber:
    """Test PR number extraction from various head_ref formats."""

    @pytest.mark.parametrize("ref,expected", [
        ("refs/pull/123/merge", 123),
        ("refs/pull/1/merge", 1),
        ("gh-readonly-queue/main/pr-456-abc123", 456),
        ("gh-readonly-queue/master/pr-789-def456", 789),
        ("gh-readonly-queue/main/pr-123-abc123def456789012345678901234567890abcd", 123),
        ("gh-readonly-queue/feature/fix-bug/pr-42-sha123", 42),
    ])
    def test_valid_formats(self, ref, expected):
        assert extract_pr_number(ref) == expected

    @pytest.mark.parametrize("ref", [
        "", None, "refs/heads/main", "feature/my-branch", "gh-readonly-queue/main/no-pr-here"
    ])
    def test_invalid_formats_return_none(self, ref):
        assert extract_pr_number(ref) is None


class TestSimplifyJobName:
    """Test job name simplification for display."""

    @pytest.mark.parametrize("name,expected", [
        ("coverage / Test ubuntu-latest, Redis unstable", "coverage"),
        ("sanitize / Address Sanitizer", "sanitize"),
        ("test-linux / linux-matrix-x86_64 (ubuntu:noble) / Run Tests", "ubuntu:noble x86_64"),
        ("test-linux / linux-matrix-aarch64 (amazonlinux:2) / Build", "amazonlinux:2 aarch64"),
        ("build", "build"),
        ("build / compile", "build"),
    ])
    def test_simplification(self, name, expected):
        assert simplify_job_name(name) == expected


class TestIsResubmission:
    """Test resubmission detection logic."""

    @pytest.mark.parametrize("commit_hour,run_hour,expected", [
        (10, 12, True),   # Commit older than run -> resubmission
        (12, 12, True),   # Same time -> resubmission
        (14, 12, False),  # Commit newer -> not resubmission
    ])
    @patch("detect_merge_queue_resubmit.get_commit_date")
    def test_commit_time_comparison(self, mock_get_commit_date, commit_hour, run_hour, expected):
        mock_get_commit_date.return_value = datetime(2024, 1, 1, commit_hour, 0, 0, tzinfo=timezone.utc)
        previous_run = {"created_at": f"2024-01-01T{run_hour:02d}:00:00Z"}
        assert is_resubmission("token", "owner/repo", 123, "sha123", previous_run) is expected

    @patch("detect_merge_queue_resubmit.get_commit_date")
    def test_returns_false_when_commit_date_unavailable(self, mock_get_commit_date):
        mock_get_commit_date.return_value = None
        assert is_resubmission("token", "owner/repo", 123, "sha", {"created_at": "2024-01-01T12:00:00Z"}) is False

    def test_returns_false_when_run_missing_created_at(self):
        assert is_resubmission("token", "owner/repo", 123, "sha", {}) is False


class TestBuildAlertPayload:
    """Test Slack alert payload generation."""

    def test_payload_structure(self):
        payload = make_payload([{"name": "test", "failure_type": None, "error_message": None}])
        assert payload["text"] == "🔄 Merge Queue Resubmission Detected"
        assert "blocks" in payload

    def test_payload_contains_pr_and_run_info(self):
        payload = make_payload([], run_id=67890, pr_number=99)
        payload_str = json.dumps(payload)
        assert "#99" in payload_str
        assert "actions/runs/67890" in payload_str

    def test_payload_lists_jobs(self):
        jobs = [
            {"name": "coverage", "failure_type": None, "error_message": None},
            {"name": "sanitize", "failure_type": None, "error_message": None},
        ]
        payload_str = json.dumps(make_payload(jobs))
        assert "coverage" in payload_str and "sanitize" in payload_str

    def test_payload_handles_empty_jobs(self):
        assert "Unknown failure" in json.dumps(make_payload([]))

    def test_includes_error_message_when_available(self):
        jobs = [{"name": "test", "failure_type": "test_failure", "error_message": "test.py::fail"}]
        payload_str = json.dumps(make_payload(jobs))
        assert "test" in payload_str and "test.py::fail" in payload_str

    def test_truncates_long_error_message(self):
        jobs = [{"name": "test", "failure_type": "test_failure", "error_message": "x" * 100}]
        payload_str = json.dumps(make_payload(jobs))
        assert "..." in payload_str and "x" * 100 not in payload_str


class TestFindPreviousFailedRun:
    """Test finding previous failed runs for a PR."""

    def _mock_runs(self, mock_get, runs):
        mock_response = MagicMock()
        mock_response.json.return_value = {"workflow_runs": runs}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

    @patch("detect_merge_queue_resubmit.requests.get")
    def test_finds_failed_run_for_pr(self, mock_get):
        from detect_merge_queue_resubmit import find_previous_failed_run
        self._mock_runs(mock_get, [
            {"id": 100, "head_branch": "gh-readonly-queue/main/pr-42-sha1", "status": "completed", "conclusion": "failure"},
            {"id": 99, "head_branch": "gh-readonly-queue/main/pr-99-sha2", "status": "completed", "conclusion": "failure"},
        ])
        result = find_previous_failed_run("token", "owner/repo", 42, current_run_id=200)
        assert result["id"] == 100

    @patch("detect_merge_queue_resubmit.requests.get")
    def test_skips_cancelled_and_current_runs(self, mock_get):
        from detect_merge_queue_resubmit import find_previous_failed_run
        self._mock_runs(mock_get, [
            {"id": 200, "head_branch": "gh-readonly-queue/main/pr-42-sha", "status": "completed", "conclusion": "failure"},  # current
            {"id": 100, "head_branch": "gh-readonly-queue/main/pr-42-sha1", "status": "completed", "conclusion": "cancelled"},
            {"id": 99, "head_branch": "gh-readonly-queue/main/pr-42-sha2", "status": "completed", "conclusion": "failure"},
        ])
        result = find_previous_failed_run("token", "owner/repo", 42, current_run_id=200)
        assert result["id"] == 99

    @patch("detect_merge_queue_resubmit.requests.get")
    def test_returns_none_when_no_failed_runs(self, mock_get):
        from detect_merge_queue_resubmit import find_previous_failed_run
        self._mock_runs(mock_get, [
            {"id": 100, "head_branch": "gh-readonly-queue/main/pr-42-sha1", "status": "completed", "conclusion": "success"},
        ])
        assert find_previous_failed_run("token", "owner/repo", 42, current_run_id=200) is None


class TestFetchJobsForRun:
    """Test fetching jobs for a workflow run."""

    @patch("detect_merge_queue_resubmit.requests.get")
    def test_fetches_jobs_successfully(self, mock_get):
        from detect_merge_queue_resubmit import fetch_jobs_for_run
        mock_response = MagicMock()
        mock_response.json.return_value = {"jobs": [{"id": 1, "name": "build"}, {"id": 2, "name": "test"}]}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        assert len(fetch_jobs_for_run("token", "owner/repo", 12345)) == 2

    @patch("detect_merge_queue_resubmit.requests.get")
    def test_returns_empty_on_error(self, mock_get):
        from detect_merge_queue_resubmit import fetch_jobs_for_run
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("API error")
        assert fetch_jobs_for_run("token", "owner/repo", 12345) == []


class TestParseFailureFromLogs:
    """Tests for parse_failure_from_logs function."""

    def test_detects_test_failure(self):
        log = "Failed Tests Summary:\n  test.py::fail\n[endgroup]\nexit code 1"
        result = parse_failure_from_logs(log)
        assert result["failure_type"] == "test_failure"
        assert "test.py::fail" in result["error_message"]

    def test_detects_sanitizer_leak(self):
        result = parse_failure_from_logs("Sanitizer: leaks detected:\n5 allocs\nexit code 1")
        assert result["failure_type"] == "sanitizer_leak"

    def test_detects_fatal_error(self):
        result = parse_failure_from_logs("fatal: unable to access repo\nexit code 1")
        assert result["failure_type"] == "fatal_error"

    @pytest.mark.parametrize("log", ["random output\nexit code 1", ""])
    def test_returns_none_for_unknown_or_empty(self, log):
        result = parse_failure_from_logs(log)
        assert result["failure_type"] is None and result["error_message"] is None

    def test_truncates_long_test_list(self):
        tests = "\n".join([f"  test_{i}.py::case" for i in range(10)])
        result = parse_failure_from_logs(f"Failed Tests Summary:\n{tests}\n[endgroup]\nexit code 1")
        assert "... and 5 more" in result["error_message"]


class TestAnalyzeFailedJobs:
    """Tests for analyze_failed_jobs function."""

    def test_filters_failed_jobs_and_skips_pr_validation(self):
        jobs = [
            {"id": 1, "name": "test-1", "conclusion": "success"},
            {"id": 2, "name": "pr-validation", "conclusion": "failure"},
            {"id": 3, "name": "test-2", "conclusion": "failure"},
        ]
        result = analyze_failed_jobs("", "owner/repo", jobs)
        assert len(result) == 1 and result[0]["name"] == "test-2"

    def test_simplifies_job_names(self):
        jobs = [{"id": 1, "name": "test-linux / linux-matrix-x86_64 (debian:bookworm) / Build", "conclusion": "failure"}]
        assert analyze_failed_jobs("", "owner/repo", jobs)[0]["name"] == "debian:bookworm x86_64"


# =============================================================================
# End-to-End Manual Tests (require GH_TOKEN)
# =============================================================================
# Run with: GH_TOKEN=ghp_xxx python -m pytest test_detect_merge_queue_resubmit.py -v -k e2e
# Or with specific PR: GH_TOKEN=ghp_xxx TEST_PR=7500 python -m pytest ... -k e2e

import os

GH_TOKEN = os.environ.get("GH_TOKEN", "")
TEST_REPO = os.environ.get("TEST_REPO", "RediSearch/RediSearch")
TEST_PR = os.environ.get("TEST_PR", "")  # Optional: specific PR to test


@pytest.mark.skipif(not GH_TOKEN, reason="GH_TOKEN not set - skipping e2e tests")
class TestE2EManual:
    """End-to-end tests against real GitHub API. Requires GH_TOKEN env var."""

    def test_e2e_extract_pr_and_fetch_details(self):
        """Test fetching real PR details from GitHub."""
        from detect_merge_queue_resubmit import get_pr_details

        # Use TEST_PR if set, otherwise use a known PR (adjust as needed)
        pr_number = int(TEST_PR) if TEST_PR else 1
        pr = get_pr_details(GH_TOKEN, TEST_REPO, pr_number)

        if pr:
            print(f"\n✅ Fetched PR #{pr_number}: {pr.get('title', 'N/A')}")
            print(f"   Author: {pr.get('user', {}).get('login', 'N/A')}")
            print(f"   State: {pr.get('state', 'N/A')}")
            assert "title" in pr
            assert "user" in pr
        else:
            pytest.skip(f"PR #{pr_number} not found or inaccessible")

    def test_e2e_find_recent_failed_runs(self):
        """Test finding recent failed merge queue runs."""
        import requests

        url = f"https://api.github.com/repos/{TEST_REPO}/actions/workflows/event-merge-to-queue.yml/runs"
        headers = {"Authorization": f"token {GH_TOKEN}"}
        params = {"status": "completed", "conclusion": "failure", "per_page": 5}

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        runs = response.json().get("workflow_runs", [])

        print(f"\n📋 Recent failed merge queue runs ({len(runs)} found):")
        for run in runs[:5]:
            pr_num = extract_pr_number(run.get("head_branch", ""))
            print(f"   Run {run['id']}: PR #{pr_num} - {run['created_at']}")

        # This is informational - doesn't fail if no runs found
        assert isinstance(runs, list)

    def test_e2e_full_detection_flow(self):
        """Test the full detection flow with a real or specified PR."""
        from detect_merge_queue_resubmit import (
            get_pr_details, find_previous_failed_run, fetch_jobs_for_run,
            analyze_failed_jobs, is_resubmission
        )

        # Find a PR to test with
        if TEST_PR:
            pr_number = int(TEST_PR)
            print(f"\n🔍 Testing with specified PR #{pr_number}")
        else:
            # Find a recent PR from failed runs
            import requests
            url = f"https://api.github.com/repos/{TEST_REPO}/actions/workflows/event-merge-to-queue.yml/runs"
            headers = {"Authorization": f"token {GH_TOKEN}"}
            params = {"status": "completed", "conclusion": "failure", "per_page": 1}
            response = requests.get(url, headers=headers, params=params, timeout=30)
            runs = response.json().get("workflow_runs", [])
            if not runs:
                pytest.skip("No failed runs found to test with")
            pr_number = extract_pr_number(runs[0].get("head_branch", ""))
            if not pr_number:
                pytest.skip("Could not extract PR number from recent run")
            print(f"\n🔍 Testing with PR #{pr_number} from recent failed run")

        # Step 1: Get PR details
        pr = get_pr_details(GH_TOKEN, TEST_REPO, pr_number)
        if not pr:
            pytest.skip(f"PR #{pr_number} not accessible")
        print(f"   ✅ PR Title: {pr.get('title', 'N/A')[:50]}...")

        # Step 2: Find previous failed run
        previous_run = find_previous_failed_run(GH_TOKEN, TEST_REPO, pr_number, current_run_id=0)
        if not previous_run:
            print("   ℹ️  No previous failed run found")
            return  # Test passes - just no data to analyze

        print(f"   ✅ Found previous failed run: {previous_run['id']}")
        print(f"      Created: {previous_run.get('created_at', 'N/A')}")

        # Step 3: Fetch and analyze jobs
        jobs = fetch_jobs_for_run(GH_TOKEN, TEST_REPO, previous_run["id"])
        print(f"   ✅ Fetched {len(jobs)} jobs")

        analyzed = analyze_failed_jobs(GH_TOKEN, TEST_REPO, jobs)
        print(f"   ✅ Analyzed {len(analyzed)} failed jobs:")
        for job in analyzed[:5]:  # Show first 5
            err = f": {job['failure_type']}" if job.get('failure_type') else ""
            print(f"      - {job['name']}{err}")

        # Step 4: Check if resubmission
        pr_head_sha = pr.get("head", {}).get("sha", "")
        is_resub = is_resubmission(GH_TOKEN, TEST_REPO, pr_number, pr_head_sha, previous_run)
        print(f"   {'⚠️  RESUBMISSION' if is_resub else '✅ Not a resubmission'}")

    def test_e2e_log_parsing(self):
        """Test fetching and parsing real job logs."""
        from detect_merge_queue_resubmit import (
            find_previous_failed_run, fetch_jobs_for_run, fetch_job_logs, parse_failure_from_logs
        )

        # Find a recent failed run
        import requests
        url = f"https://api.github.com/repos/{TEST_REPO}/actions/workflows/event-merge-to-queue.yml/runs"
        headers = {"Authorization": f"token {GH_TOKEN}"}
        params = {"status": "completed", "conclusion": "failure", "per_page": 1}
        response = requests.get(url, headers=headers, params=params, timeout=30)
        runs = response.json().get("workflow_runs", [])

        if not runs:
            pytest.skip("No failed runs found")

        run = runs[0]
        pr_number = extract_pr_number(run.get("head_branch", ""))
        print(f"\n🔍 Analyzing logs from run {run['id']} (PR #{pr_number})")

        # Fetch jobs
        jobs = fetch_jobs_for_run(GH_TOKEN, TEST_REPO, run["id"])
        failed_jobs = [j for j in jobs if j.get("conclusion") == "failure" and j.get("name") != "pr-validation"]

        if not failed_jobs:
            pytest.skip("No failed jobs in run")

        # Try to fetch and parse logs for first failed job
        job = failed_jobs[0]
        print(f"   Fetching logs for: {job['name'][:60]}...")

        logs = fetch_job_logs(GH_TOKEN, TEST_REPO, job["id"])
        if not logs:
            print("   ⚠️  Could not fetch logs (may be expired)")
            return

        print(f"   ✅ Fetched {len(logs)} bytes of logs")

        result = parse_failure_from_logs(logs)
        if result["failure_type"]:
            print(f"   ✅ Detected: {result['failure_type']}")
            if result["error_message"]:
                # Show first line
                first_line = result["error_message"].split("\n")[0][:80]
                print(f"   📝 Error: {first_line}")
        else:
            print("   ℹ️  No recognized failure pattern (conservative approach)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

