#!/bin/bash

# Script to cherry-pick a specific commit to new branches on top of all old version branches
# Usage: ./cherry_pick_to_versions.sh <commit_hash> [branch_prefix]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 <commit_hash> [branch_prefix]"
    echo ""
    echo "Arguments:"
    echo "  commit_hash   The commit hash to cherry-pick"
    echo "  branch_prefix Optional prefix for the new branch names (default: 'cherry-pick')"
    echo ""
    echo "Example:"
    echo "  $0 abc123def456"
    echo "  $0 abc123def456 fix-issue"
    echo ""
    echo "This will create branches:"
    echo "  cherry-pick-8.4 (or fix-issue-8.4)"
    echo "  cherry-pick-8.2 (or fix-issue-8.2)"
    echo "  cherry-pick-8.0 (or fix-issue-8.0)"
    echo "  cherry-pick-2.8 (or fix-issue-2.8)"
    echo "  cherry-pick-2.6 (or fix-issue-2.6)"
    echo "  cherry-pick-2.4 (or fix-issue-2.4)"
}

# Function to validate commit hash
validate_commit() {
    local commit_hash="$1"
    
    if ! git cat-file -e "$commit_hash^{commit}" 2>/dev/null; then
        print_error "Invalid commit hash: $commit_hash"
        print_error "Please provide a valid commit hash that exists in this repository."
        exit 1
    fi
}

# Function to get commit message for naming
get_commit_message() {
    local commit_hash="$1"
    git log --format="%s" -n 1 "$commit_hash" 2>/dev/null || echo "unknown-commit"
}

# Function to sanitize branch name
sanitize_branch_name() {
    local name="$1"
    # Replace spaces and special characters with hyphens, convert to lowercase
    echo "$name" | sed 's/[^a-zA-Z0-9._-]/-/g' | sed 's/--*/-/g' | sed 's/^-\|-$//g' | tr '[:upper:]' '[:lower:]'
}

# Function to create branch name
create_branch_name() {
    local prefix="$1"
    local version="$2"
    local commit_msg="$3"
    
    # Sanitize the commit message for use in branch name
    local sanitized_msg=$(sanitize_branch_name "$commit_msg")
    
    # Limit the commit message part to avoid very long branch names
    if [ ${#sanitized_msg} -gt 30 ]; then
        sanitized_msg="${sanitized_msg:0:30}"
    fi
    
    echo "${prefix}-${sanitized_msg}-${version}"
}

# Function to cherry-pick commit to a version branch
cherry_pick_to_version() {
    local commit_hash="$1"
    local version_branch="$2"
    local new_branch="$3"
    local commit_msg="$4"
    
    print_info "Processing version branch: $version_branch"
    
    # Check if the version branch exists locally
    if ! git show-ref --verify --quiet "refs/heads/$version_branch"; then
        # Try to fetch and checkout from remote
        if git show-ref --verify --quiet "refs/remotes/origin/$version_branch"; then
            print_info "Creating local branch $version_branch from origin/$version_branch"
            git checkout -b "$version_branch" "origin/$version_branch"
            print_info "✓ Local branch $version_branch created from origin/$version_branch"
        else
            print_warning "Version branch $version_branch not found locally or remotely, skipping..."
            return 1
        fi
    else
        print_info "Using existing local branch $version_branch"
        git checkout "$version_branch"
        # Make sure we're on the right branch by resetting to the remote
        print_info "Resetting $version_branch to match origin/$version_branch"
        git reset --hard "origin/$version_branch"
    fi
    
    # Pull latest changes
    print_info "Pulling latest changes for $version_branch"
    git pull origin "$version_branch" || print_warning "Failed to pull latest changes for $version_branch"
    
    # Verify we're on the correct base branch
    local current_branch=$(git branch --show-current)
    if [ "$current_branch" != "$version_branch" ]; then
        print_error "Expected to be on $version_branch but currently on $current_branch"
        return 1
    fi
    
    # Verify the current commit matches the remote
    local remote_commit=$(git rev-parse "origin/$version_branch")
    local local_commit=$(git rev-parse HEAD)
    if [ "$remote_commit" != "$local_commit" ]; then
        print_warning "Local $version_branch ($local_commit) doesn't match remote origin/$version_branch ($remote_commit)"
        print_info "Resetting to match remote..."
        git reset --hard "origin/$version_branch"
    fi
    
    # Create new branch
    print_info "Creating new branch: $new_branch from $version_branch"
    if git show-ref --verify --quiet "refs/heads/$new_branch"; then
        print_warning "Branch $new_branch already exists, deleting and recreating..."
        git branch -D "$new_branch"
    fi
    
    git checkout -b "$new_branch"
    
    # Cherry-pick the commit
    print_info "Cherry-picking commit $commit_hash to $new_branch"
    
    # Check if this is a merge commit
    local parent_count=$(git rev-list --parents -n 1 "$commit_hash" | wc -w)
    parent_count=$((parent_count - 1))  # Subtract 1 for the commit itself
    
    local cherry_pick_cmd="git cherry-pick -x"
    if [ $parent_count -gt 1 ]; then
        print_warning "Detected merge commit with $parent_count parents"
        print_info "Using -m 1 to mainline (first parent)"
        cherry_pick_cmd="git cherry-pick -x -m 1"
    fi
    
    if $cherry_pick_cmd "$commit_hash"; then
        print_success "Successfully cherry-picked $commit_hash to $new_branch"
        
        # Push the new branch to remote
        print_info "Pushing $new_branch to remote..."
        if git push -u origin "$new_branch"; then
            print_success "Successfully pushed $new_branch to remote"
        else
            print_warning "Failed to push $new_branch to remote"
        fi
    else
        print_error "Failed to cherry-pick $commit_hash to $new_branch"
        print_info "You may need to resolve conflicts manually in branch $new_branch"
        return 1
    fi
    
    return 0
}

# Main function
main() {
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        print_error "Not in a git repository. Please run this script from within a git repository."
        exit 1
    fi
    
    # Check for help option
    if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ "$1" = "help" ]; then
        show_usage
        exit 0
    fi
    
    # Check arguments
    if [ $# -lt 1 ] || [ $# -gt 2 ]; then
        print_error "Invalid number of arguments"
        show_usage
        exit 1
    fi
    
    local commit_hash="$1"
    local branch_prefix="${2:-cherry-pick}"
    
    # Validate commit hash
    validate_commit "$commit_hash"
    
    # Get commit message for naming
    local commit_msg=$(get_commit_message "$commit_hash")
    print_info "Commit message: $commit_msg"
    
    # Define version branches (in order from newest to oldest)
    local version_branches=("8.4" "8.2" "8.0" "2.8" "2.6" "2.4")
    
    print_info "Starting cherry-pick process for commit: $commit_hash"
    print_info "Branch prefix: $branch_prefix"
    print_info "Version branches to process: ${version_branches[*]}"
    echo ""
    
    # Store current branch to return to it later
    local current_branch=$(git branch --show-current)
    print_info "Current branch: $current_branch"
    
    local success_count=0
    local total_count=${#version_branches[@]}
    
    # Process each version branch
    for version in "${version_branches[@]}"; do
        local new_branch=$(create_branch_name "$branch_prefix" "$version" "$commit_msg")
        
        echo "----------------------------------------"
        print_info "Processing version: $version -> $new_branch"
        
        if cherry_pick_to_version "$commit_hash" "$version" "$new_branch" "$commit_msg"; then
            ((success_count++))
        fi
        
        echo ""
    done
    
    # Return to original branch
    print_info "Returning to original branch: $current_branch"
    git checkout "$current_branch"
    
    # Summary
    echo "========================================"
    print_info "Cherry-pick process completed!"
    print_info "Successfully processed: $success_count/$total_count version branches"
    
    if [ $success_count -eq $total_count ]; then
        print_success "All version branches processed successfully!"
    else
        print_warning "Some version branches failed. Check the output above for details."
    fi
    
    echo ""
    print_info "Created branches:"
    for version in "${version_branches[@]}"; do
        local new_branch=$(create_branch_name "$branch_prefix" "$version" "$commit_msg")
        if git show-ref --verify --quiet "refs/heads/$new_branch"; then
            print_success "  ✓ $new_branch"
        else
            print_error "  ✗ $new_branch (failed)"
        fi
    done
}

# Run main function with all arguments
main "$@"

