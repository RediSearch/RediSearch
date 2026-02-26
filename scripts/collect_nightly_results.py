#!/usr/bin/env python3
"""
Collect merge queue workflow statistics for a specific date range.
Saves raw JSON data for later analysis.
"""

import os
import sys
import json
import requests
import zipfile
import io
import re
from datetime import datetime, timedelta, timezone

def get_yesterday_date_range():
    """Get midnight-to-midnight date range for yesterday in UTC."""
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)

    # Return timezone-naive UTC datetimes for consistent ISO formatting
    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=None)

    return start, end, yesterday.strftime("%Y-%m-%d")

def fetch_jobs_for_run(token, jobs_url):
    """Fetch jobs for a specific workflow run."""
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    all_jobs = []
    # Add per_page=100 to get maximum results per page
    separator = "&" if "?" in jobs_url else "?"
    url = f"{jobs_url}{separator}per_page=100"

    try:
        while url:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            all_jobs.extend(data.get("jobs", []))

            # Check for next page in Link header
            link_header = response.headers.get("Link", "")
            url = None
            if link_header:
                # Parse Link header for next page
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        url = link[link.find("<") + 1:link.find(">")]
                        break

        return all_jobs
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Warning: Failed to fetch jobs: {e}")
        return all_jobs  # Return what we got so far

def fetch_workflow_runs(token, repo, workflow, start_time, end_time, dir_name=None):
    """Fetch workflow runs within date range from GitHub API."""
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs"
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    all_runs = []
    failed_runs = []
    all_data_lines = []  # Collect all run info lines across all pages
    page = 1
    per_page = 100
    rate_limit_info = None

    while True:
        # Format timestamps correctly: remove tzinfo if present, then append Z
        start_str = start_time.isoformat() if start_time.tzinfo is None else start_time.replace(tzinfo=None).isoformat()
        end_str = end_time.isoformat() if end_time.tzinfo is None else end_time.replace(tzinfo=None).isoformat()
        
        params = {
            "per_page": per_page,
            "page": page,
            "created": f"{start_str}Z..{end_str}Z"
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

        # Collect run information into a list of strings
        for r in runs:
            line = f"{r['id']} - {r['head_branch']} - {r['conclusion']}"
            print(f"    {line}")
            all_data_lines.append(line)
      
        # For successful runs, set jobs to empty list
        for run in runs:
            if "jobs" not in run:
                run["jobs"] = []
        
        failed_runs.extend([r for r in runs if r["conclusion"] == "failure"])
        all_runs.extend(runs)

        if len(runs) < per_page:
            break

        page += 1
    
    # Save all runs to file
    if all_data_lines:
        runs_list_file = "runs_list.txt"
        data = "\n".join(all_data_lines)
        save_to_file(data, runs_list_file, dir_name)
        print(f"✅ Saved {len(all_data_lines)} runs to {runs_list_file}")
    
    # Fetch jobs only for failed runs (we don't need job details for successful runs)
    print(f"  Fetching jobs for {len(failed_runs)} failed runs (out of {len(all_runs)} total)...")
    for run in failed_runs:
            jobs_url = run["jobs_url"]
            jobs = fetch_jobs_for_run(token, jobs_url)
            print(f"  Fetched  {len(jobs)} jobs for run {run['id']}")
            run["jobs"] = jobs


    return all_runs, rate_limit_info

def save_to_file(data, filename, dir_name=None):
    """Save data to file. If data is a list/dict, save as JSON. If string, save as text."""
    if dir_name:
        filename = os.path.join(dir_name, filename)

    try:
        with open(filename, "a") as f:
            if isinstance(data, (list, dict)):
                json.dump(data, f, indent=2)
            else:
                f.write(str(data))
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
    - "test-macos-15 / build-macos-15 (macos-15) / ..." -> "macos-15"
    - "test-macos-26 / build-macos-26 (macos-26) / ..." -> "macos-26"
    - "run-on-intel / Start self-hosted EC2 runner" -> "run-on-intel"

    Container jobs (show as "container arch"):
    - "test-linux / linux-matrix-aarch64 (gcc:11-bullseye) / ..." -> "gcc:11-bullseye aarch64"
    - "test-linux / linux-matrix-x86_64 (ubuntu:noble) / ..." -> "ubuntu:noble x86_64"
    """
    import re

    # Special case: coverage or sanitize (exact match at start)
    if job_name.startswith("coverage /") or job_name.startswith("sanitize /"):
        return job_name.split(" /")[0]

    # Special case: test-macos-* with macOS runner version.
    if re.match(r'^test-macos(?:-\d+)? / build-macos(?:-\d+)?', job_name):
        # Extract macOS runner from parentheses:
        # "test-macos-15 / build-macos-15 (macos-15) / ..."
        match = re.search(r'build-macos(?:-\d+)? \(([^)]+)\)', job_name)
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

def fetch_job_logs(token, repo, job_id):
    """Fetch logs for a specific job."""
    url = f"https://api.github.com/repos/{repo}/actions/jobs/{job_id}/logs"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        response.raise_for_status()
        return response.text  # Returns log content as text
    except requests.exceptions.RequestException as e:
        return None  # Job logs not available, will fall back to run logs

def fetch_run_logs(token, repo, run_id):
    """Fetch logs for an entire workflow run (returns zip file bytes)."""
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/logs"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        response.raise_for_status()
        return response.content  # Returns zip file bytes
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Warning: Failed to fetch run logs: {e}")
        return None

def fetch_check_runs_for_commit(token, repo, commit_sha):
    """Fetch check runs for a specific commit."""
    url = f"https://api.github.com/repos/{repo}/commits/{commit_sha}/check-runs"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("check_runs", [])
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Warning: Failed to fetch check runs: {e}")
        return []

def fetch_annotations_for_check_run(token, repo, check_run_id):
    """Fetch annotations for a specific check run."""
    url = f"https://api.github.com/repos/{repo}/check-runs/{check_run_id}/annotations"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Warning: Failed to fetch annotations: {e}")
        return []

def extract_job_log_from_zip(zip_content, job_name):
    """Extract the log content for a specific job from a run's zip file.

    Finds the directory that contains key words from the job name,
    then reads and concatenates all log files in that directory.

    Returns the concatenated content of all log files for this job.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # Extract key words from job name (words longer than 3 chars)
            # For "test-matrix / rockylinux:8 (x86_64) / Test Rocky Linux 8 x86_64, Redis 8.4.0"
            # We want: ["test-matrix", "rockylinux", "x86_64", "Test", "Rocky", "Linux", "Redis"]
            words = []
            for part in job_name.replace('/', ' ').replace(':', ' ').replace(',', ' ').split():
                # Remove parentheses and keep words longer than 3 chars
                word = part.strip('()')
                if len(word) > 3:
                    words.append(word.lower())

            # Find directories that contain most of these words
            directories = {}
            for fname in zf.namelist():
                if '/' in fname:
                    directory = fname.split('/')[0]
                    if directory not in directories:
                        dir_lower = directory.lower()
                        # Count how many key words appear in this directory name
                        match_count = sum(1 for word in words if word in dir_lower)
                        directories[directory] = match_count

            # Find the directory with the most matches
            if not directories:
                return None

            best_dir = max(directories.items(), key=lambda x: x[1])[0]

            # Read all log files from this directory
            job_logs = []
            for fname in zf.namelist():
                if not fname.startswith(best_dir + '/'):
                    continue
                if not fname.endswith('.txt'):
                    continue
                if fname.endswith('/system.txt'):
                    continue

                try:
                    content = zf.read(fname).decode('utf-8', errors='ignore')
                    job_logs.append(content)
                except Exception:
                    continue

            # Concatenate all logs for this job
            return '\n'.join(job_logs) if job_logs else None
    except Exception as e:
        print(f"⚠️  Error extracting from zip: {e}")
        return None

def parse_failure_from_logs(log_content, job_name):
    """Parse failure reason from log content.

    Look for exit code 1 or 2, then look backward up to 1000 lines
    to find "Failed Tests Summary:" or "fatal:".

    Returns a dict with:
    - failure_type: 'test_failure', 'fatal_error', or 'unknown'
    - error_message: concise error message
    - error_lines: list of error lines
    """
    lines = log_content.split('\n')

    # Exit status patterns
    exit_status_patterns = [
        r'exit code 2',
        r'exit code 1',
        r'code: 1',
    ]

    # Find exit status line
    for i in range(len(lines)):
        line = lines[i]

        # Check if this line matches an exit status pattern
        for exit_pattern in exit_status_patterns:
            if re.search(exit_pattern, line, re.IGNORECASE):
                # Look backward up to 100 lines for "Failed Tests Summary:" or "fatal: or leak"
                for j in range(i - 1, max(-1, i - 100), -1) :
                    prev_line = lines[j]

                    # Check for "Failed Tests Summary:"
                    if 'Failed Tests Summary:' in prev_line:
                        # Collect all lines until "[endgroup]"
                        test_lines = []
                        for k in range(j + 1, len(lines)):
                            test_line = lines[k]

                            # Stop at [endgroup]
                            if '[endgroup]' in test_line or 'endgroup' in test_line:
                                break

                            # Add the line as-is (only removing timestamps)
                            clean_line = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*', '', test_line)
                            if clean_line.strip():
                                test_lines.append(clean_line)

                        # Return analysis dict
                        if test_lines:
                            error_message = '\n'.join(test_lines)
                            return {
                                'failure_type': 'test_failure',
                                'error_message': error_message,
                                'error_lines': test_lines
                            }
                        break

                    if 'Sanitizer: leaks detected:' in prev_line:
                        clean_prev_line = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*', '', prev_line)
                        clean_current_line = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*', '', lines[j+1])

                        error_message = clean_prev_line.strip() + " " + clean_current_line.strip()
                        return {
                            'failure_type': 'sanitizer_leak',
                            'error_message': error_message,
                            'error_lines': [error_message]
                        }
                    
                    # Check for "fatal:" or "error:"
                    if 'fatal:' in prev_line.lower() or 'error:' in prev_line.lower():                        
                        # Return the fatal/error line as-is (only removing timestamp)
                        clean_line = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*', '', prev_line)
                        error_message = clean_line.strip()
                        if 'fatal:' in clean_line.lower():
                            failure = 'fatal_error'
                        else:
                            failure = 'generic_error'

                        return {
                            'failure_type': failure,
                            'error_message': error_message,
                            'error_lines': [error_message]
                        }
                    
                    
                break

    print("     No specific error found  ")
    return {
        'failure_type': 'unknown',
        'error_message': None,
        'error_lines': []
    }


def download_and_analyze_failed_jobs(token, repo, runs, date_str, dir_name=None, workflow_name="merge_queue"):
    """Download logs for failed jobs and analyze failure reasons."""
    failed_runs = [r for r in runs if r["conclusion"] == "failure"]

    if not failed_runs:
        print("No failed runs to analyze")
        return

    print(f"\n📥 Downloading and analyzing logs for failed jobs...")

    failure_analysis = []

    for run in failed_runs:
        run_id = run["id"]
        branch = extract_version_branch(run["head_branch"])
        commit_sha = run.get("head_sha")

        # Fetch check runs for this commit to get annotations
        check_runs = fetch_check_runs_for_commit(token, repo, commit_sha) if commit_sha else []

        # Build a map of check run name -> annotations
        check_run_annotations = {}
        for check_run in check_runs:
            if check_run.get("conclusion") == "failure" and check_run.get("output", {}).get("annotations_count", 0) > 0:
                annotations = fetch_annotations_for_check_run(token, repo, check_run["id"])
                if annotations:
                    check_run_annotations[check_run["name"]] = annotations

        # Get failed jobs (exclude pr-validation as it fails when any other job fails)
        failed_jobs = [j for j in run.get("jobs", [])
                      if j["conclusion"] == "failure" and j["name"] != "pr-validation"]

        if not failed_jobs:
            continue

        print(f"\n  Run {run_id} (branch: {branch}) - {len(failed_jobs)} failed job(s)")

        # Try to fetch job logs directly first
        # If that fails (404), fall back to downloading run logs once for all jobs
        run_logs_zip = None

        for job in failed_jobs:
            job_id = job["id"]
            job_name = job["name"]
            simplified_name = simplify_job_name(job_name)

            print(f"    Analyzing job: {simplified_name}")

            # Check if we have annotations for this job
            annotations = check_run_annotations.get(job_name, [])
            error_message = None
            failure_type = "unknown"
            error_lines = []


            # Try to get job logs directly
            print(f"      Fetching job logs... {job_id}")
            log_content = fetch_job_logs(token, repo, job_id)

            # If job logs not available, fall back to run logs
            if not log_content:
                # Download run logs once and reuse for all jobs in this run
                if run_logs_zip is None:
                    print(f"      Job logs not available, downloading run logs...")
                    run_logs_zip = fetch_run_logs(token, repo, run_id)
                    if not run_logs_zip:
                        print(f"      ⚠️  Could not download run logs")
                        continue

                # Extract this job's logs from the zip
                log_content = extract_job_log_from_zip(run_logs_zip, job_name)
                if not log_content:
                    print(f"      ⚠️  Could not find job logs in zip")
                    continue

            # Save log content to a unique file
            # if log_content and dir_name:
            #     # Create a safe filename from job name and run ID
            #     safe_job_name = simplified_name.replace('/', '_').replace(' ', '_')
            #     log_file = os.path.join(dir_name, f"log_{run_id}_{safe_job_name}.txt")
            #     with open(log_file, "w") as f:
            #         f.write(log_content)
            #     print(f"      Saved log to {log_file}")

            # Parse failure reason from the full log
            analysis = parse_failure_from_logs(log_content, job_name)
            failure_type = analysis['failure_type']
            error_message = analysis.get('error_message')
            error_lines = analysis['error_lines']

            if annotations and failure_type == "unknown":
                # Use annotations if available - they often have cleaner error messages
                print(f"      Found {len(annotations)} annotation(s)")
                # Get the first failure annotation
                for annotation in annotations:
                    if annotation.get("annotation_level") == "failure":
                        msg = annotation.get("message", "").strip()
                        # Skip generic error messages - we'll fall back to log parsing for these
                        if msg and not msg.startswith("Process completed with exit code"):
                            # Extract first line of error message for cleaner display
                            first_line = msg.split('\n')[0]
                            error_message = first_line if len(first_line) < 200 else first_line[:200] + "..."
                            error_lines = [error_message]
                            failure_type = "annotation_error"
                            break


            failure_analysis.append({
                'run_id': run_id,
                'branch': branch,
                'job_name': simplified_name,
                'full_job_name': job_name,
                'failure_type': failure_type,
                'error_message': error_message,
                'error_lines': error_lines
            })

            print(f"      Failure type: {failure_type}")
            if error_message:
                print(f"      Error: {error_message}")

    # Save analysis to file
    if failure_analysis:
        analysis_file = f"{workflow_name}_{date_str}_failures.json"
        if dir_name:
            analysis_file = os.path.join(dir_name, analysis_file)
        with open(analysis_file, "w") as f:
            json.dump(failure_analysis, f, indent=2)
        print(f"\n✅ Saved failure analysis to {analysis_file}")

        # Also create a human-readable report
        report_file = f"{workflow_name}_{date_str}_failure_report.txt"
        if dir_name:
            report_file = os.path.join(dir_name, report_file)
        with open(report_file, "w") as f:
            f.write("=" * 80 + "\n")
            f.write(f"{workflow_name.upper().replace('_', ' ')} FAILURE ANALYSIS REPORT  {date_str}\n")
            f.write("=" * 80 + "\n\n")

            # Generate summary by branch and run
            branch_runs = {}
            for item in failure_analysis:
                branch = item['branch']
                run_id = item['run_id']

                if branch not in branch_runs:
                    branch_runs[branch] = {}

                if run_id not in branch_runs[branch]:
                    branch_runs[branch][run_id] = []

                branch_runs[branch][run_id].append(item)

            # Write summary
            f.write("SUMMARY BY BRANCH\n")
            f.write("=" * 80 + "\n\n")

            for branch in sorted(branch_runs.keys()):
                f.write(f"Branch: {branch}\n")
                f.write("-" * 80 + "\n")

                # Calculate failure type summary for this branch
                branch_failures = []
                for run_id in branch_runs[branch]:
                    branch_failures.extend(branch_runs[branch][run_id])

                failure_type_counts = {}
                for item in branch_failures:
                    failure_type = item['failure_type']
                    failure_type_counts[failure_type] = failure_type_counts.get(failure_type, 0) + 1

                # Write failure type summary for this branch
                f.write("\nFailure Type Summary:\n")
                total_failures = len(branch_failures)
                for failure_type in sorted(failure_type_counts.keys()):
                    count = failure_type_counts[failure_type]
                    percentage = (count / total_failures * 100) if total_failures > 0 else 0
                    f.write(f"  {failure_type}: {count} ({percentage:.1f}%)\n")
                f.write(f"  Total: {total_failures} failure(s)\n\n")

                for run_id in sorted(branch_runs[branch].keys()):
                    failures = branch_runs[branch][run_id]
                    f.write(f"\n Run {run_id} (branch: {branch}) - {len(failures)} failed job(s)\n")

                    for failure in failures:
                        full_job_name = failure['full_job_name']
                        failure_type = failure['failure_type']
                        error_message = failure.get('error_message')

                        f.write(f" Job: {full_job_name}\n")
                        f.write(f" Failure type: {failure_type}\n")

                        if error_message:
                            # Clean up ANSI codes
                            clean_msg = re.sub(r'\x1b\[[0-9;]*m', '', error_message)
                            clean_msg = re.sub(r'\[[0-9;]*m', '', clean_msg)

                            # For multi-line errors, show first line
                            first_line = clean_msg.split('\n')[0]
                            if len(first_line) > 100:
                                first_line = first_line[:97] + "..."

                            f.write(f" Error: {first_line}\n")

                            # If there are more lines, indicate it
                            # if '\n' in clean_msg:
                            #     num_lines = len(clean_msg.split('\n'))
                            #     f.write(f"        (+ {num_lines - 1} more lines, see detailed logs below)\n")

                        f.write("\n")

                f.write("\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("DETAILED FAILURE LOGS\n")
            f.write("=" * 80 + "\n\n")

            for item in failure_analysis:
                f.write(f"Run ID: {item['run_id']}\n")
                f.write(f"Branch: {item['branch']}\n")
                f.write(f"Job: {item['job_name']}\n")
                f.write(f"Full Job Name: {item['full_job_name']}\n")
              #  f.write(f"Failure Type: {item['failure_type']}\n")
                f.write(f"\nError Details:\n")
                f.write("-" * 80 + "\n")

                # Clean up error lines for better readability
                for line in item['error_lines']:
                    # Remove ANSI escape codes (both actual and literal)
                    clean_line = re.sub(r'\x1b\[[0-9;]*m', '', line)
                    clean_line = re.sub(r'\[[0-9;]*m', '', clean_line)
                    # Remove excessive whitespace but preserve indentation
                    clean_line = clean_line.rstrip()
                    if clean_line:
                        f.write(clean_line + "\n")

                f.write("-" * 80 + "\n\n")

        print(f"✅ Saved failure report to {report_file}")

    return failure_analysis

def print_summary(runs, rate_limit_info, output_file=None, workflow_name=None, date_str=None):
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

    # Build summary text
    lines = []
    lines.append("")
    lines.append("📈 Summary:")
    lines.append(f"   ✅ Successful: {success_count}")
    lines.append(f"   ❌ Failed: {failed_count}")
    if cancelled_count > 0:
        lines.append(f"   ⏸️  Cancelled: {cancelled_count}")

    lines.append("")
    lines.append("📊 By Branch:")
    for branch in sorted(branches.keys()):
        stats = branches[branch]
        total = stats["success"] + stats["failed"] + stats["cancelled"]
        success_pct = (stats["success"] / total * 100) if total > 0 else 0
        lines.append(f"   {branch}:")
        lines.append(f"      ✅ {stats['success']}/{total} ({success_pct:.1f}%)")
        if stats["failed"] > 0:
            lines.append(f"      ❌ {stats['failed']} failed")
        if stats["cancelled"] > 0:
            lines.append(f"      ⏸️  {stats['cancelled']} cancelled")

        # Show failed jobs for this branch (exclude pr-validation)
        failed_jobs = {}
        for run in runs:
            full_branch = run["head_branch"]
            run_branch = extract_version_branch(full_branch)

            if run_branch == branch and run["conclusion"] == "failure":
                jobs = run.get("jobs", [])
                for job in jobs:
                    if job["conclusion"] == "failure" and job["name"] != "pr-validation":
                        job_name = job["name"]
                        simplified_name = simplify_job_name(job_name)
                        failed_jobs[simplified_name] = failed_jobs.get(simplified_name, 0) + 1

        if failed_jobs:
            lines.append(f"      Failed Jobs:")
            for job_name in sorted(failed_jobs.keys()):
                count = failed_jobs[job_name]
                lines.append(f"         - {job_name} ({count})")

    if rate_limit_info:
        remaining, limit = rate_limit_info
        lines.append("")
        lines.append(f"   📊 API Rate Limit: {remaining}/{limit}")

    # Print to console
    for line in lines:
        print(line)

    # Save to file if requested
    if output_file:
        with open(output_file, "w") as f:
            # Add header if workflow_name and date_str are provided
            if workflow_name and date_str:
                f.write("=" * 80 + "\n")
                f.write(f"{workflow_name.upper().replace('_', ' ').replace('-', ' ')} ANALYSIS REPORT  {date_str}\n")
                f.write("=" * 80 + "\n")
            f.write("\n".join(lines) + "\n")

def main():
    """Main entry point."""
    import argparse

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Collect GitHub workflow statistics and analyze failures',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Collect stats for yesterday (default)
  python collect.py

  # Collect stats for a specific date
  python collect.py --date 2025-12-07

  # Use a different workflow
  python collect.py --workflow event-nightly.yml

  # Use a different repository
  python collect.py --repo owner/repository

  # Combine options
  python collect.py --date 2025-12-07 --workflow event-nightly.yml --repo owner/repo
        '''
    )

    parser.add_argument(
        '--date',
        type=str,
        help='Date to collect stats for (format: YYYY-MM-DD). Default: yesterday'
    )

    parser.add_argument(
        '--workflow',
        type=str,
        default='event-merge-to-queue.yml',
        help='Workflow file name (default: event-merge-to-queue.yml)'
    )

    parser.add_argument(
        '--repo',
        type=str,
        default='RediSearch/RediSearch',
        help='Repository in format owner/repo (default: RediSearch/RediSearch)'
    )

    args = parser.parse_args()

    # Configuration
    repo = args.repo
    workflow = args.workflow
    token = os.getenv("GH_TOKEN")

    print("📊 Collecting workflow statistics...")
    print(f"   Repository: {repo}")
    print(f"   Workflow: {workflow}")

    # Get date range
    if args.date:
        try:
            # Parse date in format YYYY-MM-DD
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            start_time = datetime.combine(target_date, datetime.min.time())
            end_time = datetime.combine(target_date, datetime.max.time())
            date_str = target_date.strftime("%Y-%m-%d")
            print(f"   Using specified date: {date_str}")
        except ValueError:
            print(f"   ⚠️  Invalid date format: {args.date}")
            print(f"   Expected format: YYYY-MM-DD (e.g., 2025-12-07)")
            print(f"   Falling back to yesterday")
            start_time, end_time, date_str = get_yesterday_date_range()
    else:
        start_time, end_time, date_str = get_yesterday_date_range()

    print(f"   Date range: {start_time.isoformat()}Z to {end_time.isoformat()}Z")
    print()

    # Extract workflow name from filename (e.g., "event-nightly.yml" -> "nightly")
    workflow_name = workflow.replace("event-", "").replace(".yml", "").replace(".yaml", "")

    # Create directory for this date
    dir_name = f"{workflow_name}_{date_str}"
    os.makedirs(dir_name, exist_ok=True)
    print(f"📁 Using directory: {dir_name}/")
    print()

    # Check if file already exists
    filename = os.path.join(dir_name, f"{workflow_name}_{date_str}.json")
    summary_filename = os.path.join(dir_name, f"{workflow_name}_{date_str}_summary.txt")
    rate_limit_info = None

    if os.path.exists(filename):
        print(f"✅ File {filename} already exists, skipping API call")
        with open(filename, "r") as f:
            runs = json.load(f)
        print(f"✅ Loaded {len(runs)} runs from file")
    else:
        # Fetch data
        print(f"🔄 Fetching workflow {workflow} runs...")
        runs, rate_limit_info = fetch_workflow_runs(token, repo, workflow, start_time, end_time, dir_name)

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
        save_to_file(runs, filename)
        print(f"✅ Saved {len(runs)} runs to {filename}")

    # Print and save summary
    print_summary(runs, rate_limit_info, output_file=summary_filename, workflow_name=workflow_name, date_str=date_str)
    print()
    print(f"✅ Summary saved to {summary_filename}")

    # Analyze failed jobs
    if token:
        download_and_analyze_failed_jobs(token, repo, runs, date_str, dir_name, workflow_name)
    else:
        print("\n⚠️  Skipping failure analysis (GH_TOKEN not set)")

    # No temporary files to clean up anymore (we fetch job logs directly)

    print("\n✅ Done!")

if __name__ == "__main__":
    main()
