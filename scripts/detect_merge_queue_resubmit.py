#!/usr/bin/env python3
"""
Detect merge queue resubmissions without new commits.

This script detects when a PR is re-added to the merge queue after its workflow
failed, without any new commits being pushed. This helps identify cases where
someone is retrying a failed PR without addressing the underlying issues.

Key insight: We only check for `failure` conclusion (not `cancelled`), because:
- `failure` = workflow ran to completion, tests actually failed, PR was dequeued
- `cancelled` = likely automatic reprocessing when a PR ahead failed
"""

import json
import os
import re
import sys
from datetime import datetime

import requests

# Reuse patterns from collect_nightly_results.py
# These functions are adapted for the resubmission detection use case


def extract_pr_number(head_ref: str) -> int | None:
    """Extract PR number from merge queue head_ref.

    Formats:
    - refs/pull/NUMBER/merge
    - gh-readonly-queue/main/pr-NUMBER-SHA
    - gh-readonly-queue/master/pr-NUMBER-SHA

    Returns None if PR number cannot be extracted.
    """
    if not head_ref:
        return None

    # Format: refs/pull/NUMBER/merge
    match = re.search(r'refs/pull/(\d+)/merge', head_ref)
    if match:
        return int(match.group(1))

    # Format: gh-readonly-queue/BRANCH/pr-NUMBER-SHA
    match = re.search(r'pr-(\d+)-', head_ref)
    if match:
        return int(match.group(1))

    return None


def get_pr_details(token: str, repo: str, pr_number: int) -> dict | None:
    """Fetch PR details from GitHub API."""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Failed to fetch PR details: {e}")
        return None


def get_commit_date(token: str, repo: str, sha: str) -> datetime | None:
    """Get the commit date for a specific SHA."""
    url = f"https://api.github.com/repos/{repo}/commits/{sha}"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        date_str = data["commit"]["committer"]["date"]
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Failed to fetch commit date: {e}")
        return None


def find_previous_failed_run(
    token: str, repo: str, pr_number: int, current_run_id: int
) -> dict | None:
    """Find the most recent failed workflow run for this PR.

    Returns the run data if found, None otherwise.
    Only considers runs with conclusion == 'failure'.
    """
    url = f"https://api.github.com/repos/{repo}/actions/runs"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    params = {"event": "merge_group", "per_page": 100}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        runs = data.get("workflow_runs", [])

        # Filter runs for this PR that failed
        # PR number appears in head_branch as: gh-readonly-queue/BRANCH/pr-NUMBER-SHA
        pr_pattern = re.compile(rf'(^|/)pr-{pr_number}-')

        for run in runs:
            # Skip the current run
            if run["id"] == current_run_id:
                continue

            # Skip if not for this PR
            head_branch = run.get("head_branch", "")
            if not pr_pattern.search(head_branch):
                continue

            # Skip if not completed
            if run.get("status") != "completed":
                continue

            # We only care about failed runs
            if run.get("conclusion") == "failure":
                return run

        return None

    except requests.exceptions.RequestException as e:
        print(f"⚠️  Failed to fetch workflow runs: {e}")
        return None


def is_resubmission(
    token: str,
    repo: str,
    pr_number: int,
    pr_head_sha: str,
    previous_run: dict,
) -> bool:
    """Check if this is a resubmission without new commits.

    Returns True if:
    - Previous run failed
    - PR's current HEAD commit is older than or same as the previous run
    """
    # Get the timestamp of the previous run
    run_created_at = previous_run.get("created_at")
    if not run_created_at:
        return False

    run_date = datetime.fromisoformat(run_created_at.replace("Z", "+00:00"))

    # Get the commit date of the PR's current HEAD
    commit_date = get_commit_date(token, repo, pr_head_sha)
    if not commit_date:
        return False

    # If the commit is older than or same as the run, it's a resubmission
    return commit_date <= run_date


def fetch_jobs_for_run(token: str, repo: str, run_id: int) -> list:
    """Fetch jobs for a specific workflow run."""
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/jobs"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json().get("jobs", [])
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Failed to fetch jobs: {e}")
        return []


def fetch_job_logs(token: str, repo: str, job_id: int) -> str | None:
    """Fetch logs for a specific job.

    Returns the log content as a string, or None if unavailable.
    """
    url = f"https://api.github.com/repos/{repo}/actions/jobs/{job_id}/logs"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        return None


def parse_failure_from_logs(log_content: str) -> dict:
    """Parse failure reason from log content.

    Uses a conservative approach - only returns details if we can reliably
    identify them. Returns empty/unknown values rather than misleading output.

    Returns a dict with:
    - failure_type: 'test_failure', 'sanitizer_leak', 'fatal_error', or None
    - error_message: concise error message or None
    """
    if not log_content:
        return {"failure_type": None, "error_message": None}

    lines = log_content.split("\n")

    # Exit status patterns that indicate we should look for error details
    exit_patterns = [r"exit code [12]", r"code: 1"]

    for i, line in enumerate(lines):
        # Check if this line indicates an exit with error
        for exit_pattern in exit_patterns:
            if not re.search(exit_pattern, line, re.IGNORECASE):
                continue

            # Look backward up to 100 lines for known error patterns
            for j in range(i - 1, max(-1, i - 100), -1):
                prev_line = lines[j]

                # Pattern 1: Failed Tests Summary
                if "Failed Tests Summary:" in prev_line:
                    test_lines = []
                    for k in range(j + 1, min(len(lines), j + 50)):
                        test_line = lines[k]
                        if "[endgroup]" in test_line or "endgroup" in test_line:
                            break
                        # Remove timestamp prefix
                        clean = re.sub(
                            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*",
                            "",
                            test_line,
                        )
                        if clean.strip():
                            test_lines.append(clean.strip())

                    if test_lines:
                        # Limit to first 5 test failures for brevity
                        if len(test_lines) > 5:
                            test_lines = test_lines[:5] + [
                                f"... and {len(test_lines) - 5} more"
                            ]
                        return {
                            "failure_type": "test_failure",
                            "error_message": "\n".join(test_lines),
                        }

                # Pattern 2: Sanitizer leaks
                if "Sanitizer: leaks detected" in prev_line:
                    return {
                        "failure_type": "sanitizer_leak",
                        "error_message": "Memory leaks detected by sanitizer",
                    }

                # Pattern 3: Fatal error (be conservative - only clear fatal messages)
                if "fatal:" in prev_line.lower():
                    clean = re.sub(
                        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*", "", prev_line
                    )
                    # Only use if it's a meaningful message (not too short)
                    if len(clean.strip()) > 10:
                        return {
                            "failure_type": "fatal_error",
                            "error_message": clean.strip()[:200],
                        }

            # Found exit code but no known error pattern - don't guess
            break

    # Could not reliably identify the failure - return None rather than misleading info
    return {"failure_type": None, "error_message": None}


def analyze_failed_jobs(token: str, repo: str, jobs: list) -> list:
    """Analyze failed jobs to extract failure details.

    Returns a list of dicts with job name, failure type, and error message.
    Uses conservative approach - only includes details if reliably detected.
    """
    results = []

    for job in jobs:
        if job.get("conclusion") != "failure":
            continue

        # Skip pr-validation as it fails when any other job fails
        if job.get("name") == "pr-validation":
            continue

        job_id = job.get("id")
        job_name = job.get("name", "Unknown")
        simplified_name = simplify_job_name(job_name)

        result = {
            "name": simplified_name,
            "full_name": job_name,
            "failure_type": None,
            "error_message": None,
        }

        # Try to fetch and analyze logs
        if job_id and token:
            log_content = fetch_job_logs(token, repo, job_id)
            if log_content:
                analysis = parse_failure_from_logs(log_content)
                result["failure_type"] = analysis.get("failure_type")
                result["error_message"] = analysis.get("error_message")

        results.append(result)

    return results


def simplify_job_name(job_name: str) -> str:
    """Simplify job name for display.

    Examples:
    - "coverage / Test ubuntu-latest, Redis unstable" -> "coverage"
    - "test-linux / linux-matrix-x86_64 (ubuntu:noble) / ..." -> "ubuntu:noble x86_64"
    """
    # Special case: coverage or sanitize
    if job_name.startswith("coverage /") or job_name.startswith("sanitize /"):
        return job_name.split(" /")[0]

    # Extract architecture from "linux-matrix-aarch64" or "linux-matrix-x86_64"
    arch_match = re.search(r'linux-matrix-(aarch64|x86_64)', job_name)
    arch = arch_match.group(1) if arch_match else None

    # Extract container name from parentheses
    container_match = re.search(r'\(([^)]+)\)', job_name)
    container = container_match.group(1) if container_match else None

    if container and arch:
        return f"{container} {arch}"

    # Fallback to first part of job name
    return job_name.split(" /")[0] if " /" in job_name else job_name


def get_run_url(repo: str, run_id: int) -> str:
    """Generate GitHub Actions run URL."""
    return f"https://github.com/{repo}/actions/runs/{run_id}"


def build_alert_payload(
    pr_number: int,
    pr_title: str,
    pr_author: str,
    pr_url: str,
    previous_run: dict,
    analyzed_jobs: list,
    repo: str,
) -> dict:
    """Build Slack notification payload.

    Args:
        analyzed_jobs: List of analyzed job dicts with keys:
            - name: Simplified job name
            - failure_type: Type of failure or None
            - error_message: Error details or None
    """
    run_url = get_run_url(repo, previous_run["id"])

    # Format failed jobs with optional error details
    job_lines = []
    for job in analyzed_jobs:
        name = job.get("name", "Unknown")
        error_msg = job.get("error_message")

        if error_msg:
            # Include error message (truncated for Slack)
            # Take first line only for brevity
            first_line = error_msg.split("\n")[0]
            if len(first_line) > 80:
                first_line = first_line[:77] + "..."
            job_lines.append(f"• {name}: {first_line}")
        else:
            job_lines.append(f"• {name}")

    jobs_text = "\n".join(job_lines) if job_lines else "• Unknown failure"

    return {
        "text": "🔄 Merge Queue Resubmission Detected",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🔄 Merge Queue Resubmission Detected",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*PR:* <{pr_url}|#{pr_number}>"},
                    {"type": "mrkdwn", "text": f"*Title:* {pr_title}"},
                    {"type": "mrkdwn", "text": f"*Author:* @{pr_author}"},
                    {"type": "mrkdwn", "text": f"*Previous Failure:* <{run_url}|View Run>"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Job(s):*\n{jobs_text}",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "⚠️ Resubmitted without new commits",
                    }
                ],
            },
        ],
    }


def main():
    """Main entry point."""
    # Get environment variables
    token = os.environ.get("GH_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    head_ref = os.environ.get("MERGE_GROUP_HEAD_REF", "")
    current_run_id = int(os.environ.get("CURRENT_RUN_ID", "0"))
    github_output = os.environ.get("GITHUB_OUTPUT", "")

    if not repo or not head_ref:
        print("❌ Missing required environment variables")
        sys.exit(1)

    # Extract PR number
    pr_number = extract_pr_number(head_ref)
    if not pr_number:
        print(f"❌ Could not extract PR number from: {head_ref}")
        write_output(github_output, "resubmit", "false")
        sys.exit(0)

    print(f"🔍 Checking PR #{pr_number} for resubmission...")

    # Get PR details
    pr_details = get_pr_details(token, repo, pr_number)
    if not pr_details:
        print("❌ Could not fetch PR details")
        write_output(github_output, "resubmit", "false")
        sys.exit(0)

    pr_head_sha = pr_details["head"]["sha"]
    pr_title = pr_details["title"]
    pr_author = pr_details["user"]["login"]
    pr_url = pr_details["html_url"]

    print(f"   PR HEAD SHA: {pr_head_sha}")

    # Find previous failed run
    previous_run = find_previous_failed_run(token, repo, pr_number, current_run_id)
    if not previous_run:
        print("✅ No previous failed run found - not a resubmission")
        write_output(github_output, "resubmit", "false")
        sys.exit(0)

    print(f"   Found previous failed run: {previous_run['id']}")

    # Check if this is a resubmission
    if not is_resubmission(token, repo, pr_number, pr_head_sha, previous_run):
        print("✅ New commits detected since previous failure - not a resubmission")
        write_output(github_output, "resubmit", "false")
        sys.exit(0)

    print("⚠️  RESUBMISSION DETECTED - no new commits since previous failure")

    # Fetch and analyze failed jobs for the alert
    jobs = fetch_jobs_for_run(token, repo, previous_run["id"])
    analyzed_jobs = analyze_failed_jobs(token, repo, jobs)

    if analyzed_jobs:
        print(f"   Analyzed {len(analyzed_jobs)} failed job(s)")
        for job in analyzed_jobs:
            if job.get("error_message"):
                print(f"   - {job['name']}: {job['failure_type']}")
            else:
                print(f"   - {job['name']}: (no error details)")

    # Build and save alert payload
    payload = build_alert_payload(
        pr_number, pr_title, pr_author, pr_url, previous_run, analyzed_jobs, repo
    )

    payload_file = "/tmp/slack_payload.json"
    with open(payload_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"📝 Alert payload saved to {payload_file}")

    # Write outputs
    write_output(github_output, "resubmit", "true")
    write_output(github_output, "payload_file", payload_file)


def write_output(output_file: str, name: str, value: str):
    """Write to GitHub Actions output file."""
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"{name}={value}\n")
    print(f"   Output: {name}={value}")


if __name__ == "__main__":
    main()

