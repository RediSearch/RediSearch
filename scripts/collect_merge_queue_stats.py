#!/usr/bin/env python3
"""
Collect merge queue workflow statistics for a specific date range.
Saves raw JSON data for later analysis.
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta, timezone

def get_yesterday_date_range():
    """Get midnight-to-midnight date range for yesterday."""
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)

    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    return start, end, yesterday.strftime("%Y-%m-%d")

def fetch_jobs_for_run(token, jobs_url):
    """Fetch jobs for a specific workflow run."""
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(jobs_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("jobs", [])
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Warning: Failed to fetch jobs: {e}")
        return []

def fetch_workflow_runs(token, repo, workflow, start_time, end_time):
    """Fetch workflow runs within date range from GitHub API."""
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs"
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    all_runs = []
    page = 1
    per_page = 100
    rate_limit_info = None

    while True:
        params = {
            "per_page": per_page,
            "page": page,
            "created": f"{start_time.isoformat()}Z..{end_time.isoformat()}Z"
        }

        print(f"  Fetching page {page}...")

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {e}")
            return None, None

        # Extract rate limit from response headers
        if not rate_limit_info:
            try:
                remaining = response.headers["X-RateLimit-Remaining"]
                limit = response.headers["X-RateLimit-Limit"]
                rate_limit_info = (remaining, limit)
            except KeyError as e:
                print(f"⚠️  Warning: Missing rate limit header: {e}")
                rate_limit_info = (None, None)

        data = response.json()

        if "workflow_runs" not in data:
            print(f"❌ Unexpected API response: {data}")
            return None, None

        runs = data["workflow_runs"]
        if not runs:
            break

        # Fetch jobs for each run
        for run in runs:
            jobs_url = run["jobs_url"]
            jobs = fetch_jobs_for_run(token, jobs_url)
            run["jobs"] = jobs

        all_runs.extend(runs)

        if len(runs) < per_page:
            break

        page += 1

    return all_runs, rate_limit_info

def save_to_file(data, date_str):
    """Save data to JSON file."""
    filename = f"merge_queue_{date_str}.json"

    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        print(f"✅ Saved {len(data)} runs to {filename}")
        return filename
    except IOError as e:
        print(f"❌ Failed to write file: {e}")
        return None

def extract_version_branch(branch_name):
    """Extract version branch from merge queue branch name.

    Examples:
    - gh-readonly-queue/master/pr-7183-xxx -> master
    - gh-readonly-queue/8.2/pr-7235-xxx -> 8.2
    - master -> master
    """
    if not branch_name:
        raise ValueError("branch_name is empty or None")

    if branch_name.startswith("gh-readonly-queue/"):
        parts = branch_name.split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid merge queue branch format: {branch_name}")
        return parts[1]
    return branch_name

def simplify_job_name(job_name):
    """Simplify job name for display.

    Special cases (show only title):
    - "coverage / Test ubuntu-latest, Redis unstable" -> "coverage"
    - "sanitize / Test ubuntu-latest, Redis unstable" -> "sanitize"
    - "test-macos / build-macos (macos-15-intel) / ..." -> "macos-15-intel"
    - "test-macos / build-macos (macos-latest) / ..." -> "macos-latest"
    - "run-on-intel / Start self-hosted EC2 runner" -> "run-on-intel"

    Container jobs (show as "container arch"):
    - "test-linux / linux-matrix-aarch64 (gcc:11-bullseye) / ..." -> "gcc:11-bullseye aarch64"
    - "test-linux / linux-matrix-x86_64 (ubuntu:noble) / ..." -> "ubuntu:noble x86_64"
    """
    import re

    # Special case: coverage or sanitize (exact match at start)
    if job_name.startswith("coverage /") or job_name.startswith("sanitize /"):
        return job_name.split(" /")[0]

    # Special case: test-macos with macos version
    if job_name.startswith("test-macos / build-macos"):
        # Extract macos version from parentheses: "test-macos / build-macos (macos-latest) / ..."
        match = re.search(r'build-macos \(([^)]+)\)', job_name)
        if match:
            return match.group(1)

    # Special case: run-on-intel or other non-container jobs
    if job_name.startswith("run-on-intel"):
        return "run-on-intel"

    # For container jobs, extract container name and architecture
    # Format: "test-linux / linux-matrix-aarch64 (gcc:11-bullseye) / Test gcc:11-bullseye, Redis unstable"

    # Extract architecture from "linux-matrix-aarch64" or "linux-matrix-x86_64"
    arch_match = re.search(r'linux-matrix-(aarch64|x86_64)', job_name)
    arch = arch_match.group(1) if arch_match else None

    # Extract container name from parentheses
    container_match = re.search(r'\(([^)]+)\)', job_name)
    container = container_match.group(1) if container_match else None

    if container and arch:
        return f"{container} {arch}"

    # Fallback to original name if parsing fails
    return job_name

def print_summary(runs, rate_limit_info):
    """Print success/failure summary by branch and rate limit."""
    # Overall stats
    success_count = sum(1 for r in runs if r["conclusion"] == "success")
    failed_count = sum(1 for r in runs if r["conclusion"] == "failure")
    cancelled_count = sum(1 for r in runs if r["conclusion"] == "cancelled")

    # Group by branch
    branches = {}
    for run in runs:
        try:
            full_branch = run["head_branch"]
            conclusion = run["conclusion"]
        except KeyError as e:
            print(f"❌ Missing required field in run data: {e}")
            sys.exit(1)

        branch = extract_version_branch(full_branch)

        if branch not in branches:
            branches[branch] = {"success": 0, "failed": 0, "cancelled": 0}

        if conclusion == "success":
            branches[branch]["success"] += 1
        elif conclusion == "failure":
            branches[branch]["failed"] += 1
        elif conclusion == "cancelled":
            branches[branch]["cancelled"] += 1

    print()
    print("📈 Summary:")
    print(f"   ✅ Successful: {success_count}")
    print(f"   ❌ Failed: {failed_count}")
    if cancelled_count > 0:
        print(f"   ⏸️  Cancelled: {cancelled_count}")

    print()
    print("📊 By Branch:")
    for branch in sorted(branches.keys()):
        stats = branches[branch]
        total = stats["success"] + stats["failed"] + stats["cancelled"]
        success_pct = (stats["success"] / total * 100) if total > 0 else 0
        print(f"   {branch}:")
        print(f"      ✅ {stats['success']}/{total} ({success_pct:.1f}%)")
        if stats["failed"] > 0:
            print(f"      ❌ {stats['failed']} failed")
        if stats["cancelled"] > 0:
            print(f"      ⏸️  {stats['cancelled']} cancelled")

        # Show failed jobs for this branch
        failed_jobs = {}
        for run in runs:
            full_branch = run["head_branch"]
            run_branch = extract_version_branch(full_branch)

            if run_branch == branch and run["conclusion"] == "failure":
                jobs = run.get("jobs", [])
                for job in jobs:
                    if job["conclusion"] == "failure":
                        job_name = job["name"]
                        simplified_name = simplify_job_name(job_name)
                        failed_jobs[simplified_name] = failed_jobs.get(simplified_name, 0) + 1

        if failed_jobs:
            print(f"      Failed Jobs:")
            for job_name in sorted(failed_jobs.keys()):
                count = failed_jobs[job_name]
                print(f"         - {job_name} ({count})")

    if rate_limit_info:
        remaining, limit = rate_limit_info
        print()
        print(f"   📊 API Rate Limit: {remaining}/{limit}")

def main():
    """Main entry point."""
    # Configuration
    repo = "RediSearch/RediSearch"
    workflow = "event-merge-to-queue.yml"
    token = os.getenv("GH_TOKEN")

    print("📊 Collecting merge queue statistics...")
    print(f"   Repository: {repo}")
    print(f"   Workflow: {workflow}")

    # Get date range
    start_time, end_time, date_str = get_yesterday_date_range()
    print(f"   Date range: {start_time.isoformat()}Z to {end_time.isoformat()}Z")
    print()

    # Check if file already exists
    filename = f"merge_queue_{date_str}.json"
    rate_limit_info = None

    if os.path.exists(filename):
        print(f"✅ File {filename} already exists, skipping API call")
        with open(filename, "r") as f:
            runs = json.load(f)
        print(f"✅ Loaded {len(runs)} runs from file")
    else:
        # Fetch data
        print("🔄 Fetching workflow runs...")
        runs, rate_limit_info = fetch_workflow_runs(token, repo, workflow, start_time, end_time)

        if runs is None:
            print("❌ Failed to fetch data")
            sys.exit(1)

        if not runs:
            print("⚠️  No runs found for this date range")
            sys.exit(0)

        print(f"✅ Found {len(runs)} runs")
        print()

        # Save to file
        print("💾 Saving data...")
        save_to_file(runs, date_str)

    # Print summary
    print_summary(runs, rate_limit_info)
    print()
    print("✅ Done!")

if __name__ == "__main__":
    main()
