## Describe the changes in the pull request

1. Current: Production still runs the C GEO filter implementation despite the tested Rust replacement.
2. Change: Route the existing C entry points to Rust and remove the production C implementation.
3. Outcome: Internal-only completion of the Geo Index implementation port, preserving query results, errors, allocation ownership, and persisted formats.

#### Which additional issues this PR fixes

N/A

#### Main objects this PR modified

1. GEO filter entry points — production switches to Rust.
2. Geo Index C implementation — removed; shared C layouts remain for callers.
3. Query and iterator integration — verified against the unchanged behavior tests.

#### Mark if applicable

- [ ] This PR introduces API changes
- [ ] This PR introduces serialization changes

#### Release Notes

- [ ] This PR requires release notes
- [x] This PR does not require release notes
