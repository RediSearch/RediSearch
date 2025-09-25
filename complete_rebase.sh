#!/bin/bash

# Complete the rebase by creating a proper commit
echo "Creating commit for rebased CI matrix changes..."

# Add all changes
git add -A

# Create the commit
git commit -m "CI: Flatten CI matrix - unified matrix generation and build system

- Add task-generate-test-matrix.yml for unified matrix generation  
- Update flow-build-artifacts.yml to use unified build system
- Update flow-macos.yml to use generated matrix
- Remove task-get-linux-configurations.yml (replaced by unified system)
- Support architecture filtering for macOS matrix
- Pass architecture through flow-macos.yml and update runners

Rebased on top of latest master"

echo "Rebase completed successfully!"
echo "Current branch: $(git branch --show-current)"
echo "Latest commit: $(git log --oneline -1)"
