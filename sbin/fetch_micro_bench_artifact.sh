#!/bin/bash

# This script uses the GitHub CLI (gh) to fetch the latest micro-benchmark artifact from master,
# and unzips it. It searches recursively and is invoked by CI.

# Config - pass DEST_DIR as an argument or set default
ARTIFACT_NAME="rust-benchmark-results-master"
BRANCH="master"
WORKFLOW_FILE="event-push-to-integ.yml"
LIMIT=25
DEST_DIR="${1:-./bin/redisearch_rs/criterion}"  # default to ./artifact_contents or use first script arg
OWENR=""

# Check for required commands
command -v gh >/dev/null 2>&1 || { echo "❌ Error: gh is required but not installed"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "❌ Error: jq is required but not installed"; exit 1; }

echo "🔍 Searching for artifact '$ARTIFACT_NAME' from workflow '$WORKFLOW_FILE' on branch '$BRANCH' (last $LIMIT runs)..."
echo "📂 Will unzip artifact into: $DEST_DIR"

REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
WORKFLOW_ID=$(gh api "/repos/$REPO/actions/workflows/$WORKFLOW_FILE" --jq '.id')

if [ -z "$WORKFLOW_ID" ]; then
  echo "❌ Could not find workflow ID for '$WORKFLOW_FILE'"
  exit 1
fi

found=0

# 1) Run the API call once and save its compacted output:
gh api "/repos/$REPO/actions/workflows/$WORKFLOW_ID/runs?branch=$BRANCH&per_page=$LIMIT" \
  --jq '.workflow_runs[]' \
| jq -c '.' > runs.json

# 2) Iterate over the runs to find the latest benchmark artifact on master
while IFS= read -r run; do
    run_id=$(echo "$run" | jq -r '.id')
    commit_sha=$(echo "$run" | jq -r '.head_sha')
    status=$(echo "$run" | jq -r '.status')
    conclusion=$(echo "$run" | jq -r '.conclusion')
    timestamp=$(echo "$run" | jq -r '.created_at')

    echo "➡️  Checking run $run_id [commit: ${commit_sha:0:7}, status: $status, conclusion: $conclusion, created: $timestamp]"

    artifacts_json=$(gh api "/repos/$REPO/actions/runs/$run_id/artifacts")
    if echo "$artifacts_json" | jq -e --arg name "$ARTIFACT_NAME" '.artifacts[] | select(.name == $name)' > /dev/null; then
        echo "✅ Found artifact '$ARTIFACT_NAME' in run $run_id — downloading..."

        # Create destination directory
        mkdir -p "$DEST_DIR"

        # Download unzipped artifact into DEST_DIR
        gh run download "$run_id" --name "$ARTIFACT_NAME" --dir "$DEST_DIR" > /dev/null

        found=1
        break
    fi
done < runs.json

if [ "$found" -eq 1 ]; then
  echo "🎉 Artifact downloaded and extracted successfully to '$DEST_DIR'."
  exit 0
else
  echo "❌ Artifact '$ARTIFACT_NAME' not found in last $LIMIT runs of '$WORKFLOW_FILE' on branch '$BRANCH'."
  exit 1
fi
