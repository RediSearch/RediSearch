#!/bin/bash
set -e

echo "Adding all files..."
git add -A

echo "Creating tree..."
TREE_HASH=$(git write-tree)
echo "Tree hash: $TREE_HASH"

echo "Creating commit..."
COMMIT_MSG="CI: Flatten CI matrix - unified matrix generation and build system

- Add task-generate-test-matrix.yml for unified matrix generation  
- Update flow-build-artifacts.yml to use unified build system
- Update flow-macos.yml to use generated matrix
- Remove task-get-linux-configurations.yml (replaced by unified system)
- Support architecture filtering for macOS matrix
- Pass architecture through flow-macos.yml and update runners

Rebased on top of latest master"

PARENT_COMMIT="f5a29907d695bdf021ac966b00a5ce2bbfb24c05"
COMMIT_HASH=$(echo "$COMMIT_MSG" | git commit-tree $TREE_HASH -p $PARENT_COMMIT)
echo "New commit hash: $COMMIT_HASH"

echo "Updating branch reference..."
echo "$COMMIT_HASH" > .git/refs/heads/master_jk_flatten_ci_matrix

echo "Success! Branch updated to: $COMMIT_HASH"
