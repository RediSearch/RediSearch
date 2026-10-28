## Describe the changes in the pull request

1. Current: The Rust GEO filter implementation has no C boundary for legacy argument parsing and ownership.
2. Change: Expose and test that boundary under temporary symbols while production still calls C.
3. Outcome: Internal-only preparation for the final switch; no command or persistence changes.

#### Which additional issues this PR fixes

N/A

#### Main objects this PR modified

1. GEO filter C boundary — preserve argument cursor and error behavior.
2. Filter ownership — retain Redis allocations and Rust numeric-filter cleanup.
3. Boundary tests — exercise construction, parsing failures, and destruction.

#### Mark if applicable

- [ ] This PR introduces API changes
- [ ] This PR introduces serialization changes

#### Release Notes

- [ ] This PR requires release notes
- [x] This PR does not require release notes
