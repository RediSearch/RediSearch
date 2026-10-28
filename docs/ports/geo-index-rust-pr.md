## Describe the changes in the pull request

1. Current: GEO filter validation and distance units still live in C, alongside the existing Rust geohash implementation.
2. Change: Establish the C behavior baseline and add a tested Rust implementation without switching production callers.
3. Outcome: Internal-only preparation for removing the remaining Geo Index C implementation. Search behavior and stored data remain unchanged.

#### Which additional issues this PR fixes

N/A

#### Main objects this PR modified

1. GEO filter parameters — preserve validation and unit semantics.
2. Characterization tests — pin the existing C and command behavior.
3. Geo benchmarks — retain the original C comparison after the final switch.

#### Mark if applicable

- [ ] This PR introduces API changes
- [ ] This PR introduces serialization changes

#### Release Notes

- [ ] This PR requires release notes
- [x] This PR does not require release notes
