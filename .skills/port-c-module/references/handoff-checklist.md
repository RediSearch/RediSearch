# Pre-handoff self-audit

Guidance that is only prose gets skipped. Before ending an attempt as ready
for review, run each check and record its result and evidence path in the
validation evidence. A check that cannot be run is an evidence gap.

| Check | How to verify |
| --- | --- |
| Patch completeness | Export the patch from the candidate worktree including untracked files; the file list equals the reviewed file list. |
| No unrelated edits | Every changed file maps to a requirement or a recorded finding. |
| Generated files | Regenerate headers and generated sources with the repository tool; the diff is empty. No hand-edited generated file. |
| Types and FFI | No hardcoded C typedef widths; each preprocessor variant that changes a type was built or tested. |
| Exports | Each exported symbol has a listed caller at the starting SHA or a stated reason. |
| C cleanup | Replaced C sources, headers, build entries and stubs are gone, or the plan records why each remains. |
| Executed tests | For each selected test target, the executed-test count is nonzero and matches the expectation. |
| Deleted tests | Every deleted or weakened test or assertion is listed with its replacement or the capability it lost and an owner. |
| Literal statistics | Tests and docs asserting literal counts, sizes or statistics were searched and each changed value is a recorded, user-visible change or a justified internal one. |
| Repository tooling | New tests and benchmarks use the workspace's standard property-test and benchmark libraries, or the plan records why not. |
| Caller benchmarks | If a hot-path inline C function became a call, the repository's existing caller microbenchmarks and a product workload ran on both builds with recorded profiles and binary hashes. |
| C-compatibility record | Each kept C dependency, quirk or signature has an entry with its cost and alternative. |
| Docs and errors | Public items have worked examples where a format is involved; public errors implement `Display` and `std::error::Error`. |
| Evidence durability | Differential corpora, logs and benchmark records are stored outside scratch directories or committed as tests. |
