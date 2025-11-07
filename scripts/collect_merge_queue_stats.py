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
            remaining = response.headers.get("X-RateLimit-Remaining", "unknown")
            limit = response.headers.get("X-RateLimit-Limit", "unknown")
            rate_limit_info = (remaining, limit)

        data = response.json()

        if "workflow_runs" not in data:
            print(f"❌ Unexpected API response: {data}")
            return None, None

        runs = data["workflow_runs"]
        if not runs:
            break

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

def print_summary(runs, rate_limit_info):
    """Print success/failure summary and rate limit."""
    success_count = sum(1 for r in runs if r.get("conclusion") == "success")
    failed_count = sum(1 for r in runs if r.get("conclusion") == "failure")

    print()
    print("📈 Summary:")
    print(f"   ✅ Successful: {success_count}")
    print(f"   ❌ Failed: {failed_count}")

    if rate_limit_info:
        remaining, limit = rate_limit_info
        print(f"   📊 API Rate Limit: {remaining}/{limit}")

def main():
    """Main entry point."""
    # Configuration
    repo = "RediSearch/RediSearch"
    workflow = "event-merge-to-queue.yml"
    token = os.getenv("GITHUB_TOKEN")

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
