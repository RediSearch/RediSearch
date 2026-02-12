# PR Design Context

This directory is for AI-generated design files used during PR review.

## For PR Owners

1. During development, add design docs here (e.g., `query-parser-refactor.md`)
2. **Before merging:** Delete these files or move valuable docs to `docs/design/`

## For Reviewers

Use these files to navigate the changes intentionally rather than going through files sequentially. Ask your AI to create a review tree in a new .md file in this directory:

1. **High-level design** - Intent, approach, trade-offs
2. **Classes/methods/APIs added or changed** - What was introduced and why
3. **Review plan** - Suggested order to go over the files, important changes to focus on

## Rules

- CI will fail in the merge queue if any files other than `README.md` exist here
- Design docs worth keeping permanently should be moved to `docs/design/`
