#!/bin/bash

# This script uses the GitHub API to fetch the latest micro-benchmark artifact from master,
# it performs a recursive search and is invoked by CI.

# Check for required commands
command -v curl >/dev/null 2>&1 || { echo "Error: curl is required but not installed"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "Error: jq is required but not installed"; exit 1; }

# Check if required environment variables are set
if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GITHUB_TOKEN and GITHUB_REPOSITORY must be set"
  exit 1
fi

# Set default GITHUB_REPOSITORY if not provided
GITHUB_REPOSITORY=${GITHUB_REPOSITORY:-RediSearch}

# Get workflow ID for flow-micro-benchmarks.yml
WORKFLOW_RESPONSE=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/$GITHUB_REPOSITORY/actions/workflows/flow-micro-benchmarks.yml")
WORKFLOW_ID=$(echo "$WORKFLOW_RESPONSE" | jq -r '.id')

# Fetch the 50 most recent commits on master
COMMITS=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/$GITHUB_REPOSITORY/commits?sha=master&per_page=50" \
  | jq -r '.[].sha')

ARTIFACT_URL=""
for COMMIT_SHA in $COMMITS; do
  RUN_RESPONSE=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/$GITHUB_REPOSITORY/actions/runs?head_sha=$COMMIT_SHA&status=success&workflow_id=$WORKFLOW_ID")
  ARTIFACT_URL=$(echo "$RUN_RESPONSE" | jq -r '.workflow_runs[0].artifacts_url' \
    | xargs -I {} curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
      -H "Accept: application/vnd.github+json" {} \
    | jq -r '.artifacts[] | select(.name == "rust-benchmark-results-master") | .archive_download_url')
  if [ -n "$ARTIFACT_URL" ]; then
    echo "Found artifact for commit $COMMIT_SHA: $ARTIFACT_URL"
    break
  fi
done

echo "artifact-url=$ARTIFACT_URL"
if [ -z "$ARTIFACT_URL" ]; then
  echo "No artifact found for rust-benchmark-results-master in the last 50 commits"
  exit 1
fi

exit 0