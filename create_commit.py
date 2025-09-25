#!/usr/bin/env python3
import subprocess
import os

def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

# Get the tree hash for current state
tree_hash = run_cmd("git write-tree")
print(f"Tree hash: {tree_hash}")

# Create commit object
commit_msg = """CI: Flatten CI matrix - unified matrix generation and build system

- Add task-generate-test-matrix.yml for unified matrix generation  
- Update flow-build-artifacts.yml to use unified build system
- Update flow-macos.yml to use generated matrix
- Remove task-get-linux-configurations.yml (replaced by unified system)
- Support architecture filtering for macOS matrix
- Pass architecture through flow-macos.yml and update runners

Rebased on top of latest master"""

# Create the commit
parent_commit = "f5a29907d695bdf021ac966b00a5ce2bbfb24c05"
commit_hash = run_cmd(f'echo "{commit_msg}" | git commit-tree {tree_hash} -p {parent_commit}')
print(f"New commit hash: {commit_hash}")

# Update the branch reference
with open('.git/refs/heads/master_jk_flatten_ci_matrix', 'w') as f:
    f.write(commit_hash + '\n')

print("Branch updated successfully!")
print(f"Branch now points to: {commit_hash}")
