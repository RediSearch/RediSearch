#!/bin/bash

# Final attempt to create the commit
echo "=== Completing the rebase ==="

# Stage all changes
git add -A

# Check if there are any changes to commit
if git diff --cached --quiet; then
    echo "No changes to commit - the rebase may already be complete"
    echo "Current commit: $(git rev-parse HEAD)"
    echo "Current branch: $(git branch --show-current)"
    exit 0
fi

# Create the commit
git commit -m "CI: Flatten CI matrix - unified matrix generation and build system

- Add task-generate-test-matrix.yml for unified matrix generation  
- Update flow-build-artifacts.yml to use unified build system
- Update flow-macos.yml to use generated matrix
- Remove task-get-linux-configurations.yml (replaced by unified system)
- Support architecture filtering for macOS matrix
- Pass architecture through flow-macos.yml and update runners

Rebased on top of latest master"

echo "=== Rebase completed successfully! ==="
echo "New commit: $(git rev-parse HEAD)"
echo "Branch: $(git branch --show-current)"
