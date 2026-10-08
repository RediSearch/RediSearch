# Dependency analysis for migration scope and validation

Use graph analysis when available, alongside ownership and behavioral evidence.
Do not require it for a small port whose boundary is already clear. A file graph
is not a safety proof, a test-selection engine, or a numerical risk score.

## Generate a usable snapshot

The [shared graph tool ZIP](https://redislabs.atlassian.net/wiki/pages/viewpageattachments.action?pageId=6791364628&preview=%2F6791364628%2F6836420647%2Fmigration-graph-double-click.zip)
contains source, pinned requirements, tests, licenses, and an offline viewer.
Inspect its README before running it. It needs Python 3.11+, Clang, matching
generated headers, and a compilation database from the checkout being analyzed.
Do not copy another checkout's database with stale absolute paths.

```sh
python migration_graph.py build --root <checkout> \
  --compile-commands <matching-compile_commands.json> --output <snapshot-directory>
python migration_graph.py summary --graph <snapshot-directory>/graph.json
python migration_graph.py deps <source-file> --reverse --graph <snapshot-directory>/graph.json
python migration_graph.py deps <source-file> --transitive --graph <snapshot-directory>/graph.json
python migration_graph.py cycles --language c --graph <snapshot-directory>/graph.json
```

Read parse/indexing and unresolved-symbol diagnostics before interpreting counts.
Include header dependencies when relevant; default implementation queries omit
headers and includes. Use `--headers --kind includes` to inspect include edges.
Language filters restrict traversal: C-only queries do not traverse through Rust.
External libraries, runtime callbacks, and Rust dispatch/macros need manual review.

## Compare boundaries

For each proposed scope, record scoped implementation files/modules, direct external
dependencies, direct consumers, transitive consumers, strongly connected groups,
and boundary crossings. Define whether counts include headers and how repeated
relationships are deduplicated. Unknown analysis is not a zero-dependency result.

- Consider migrating tightly coupled ownership/cycle groups together to reduce
  temporary adapters; do not pull in an entire large cycle without reviewing why
  its edges exist. Shared infrastructure can connect otherwise separable work.
- Compare smaller changes against one connected migration using boundary/adapter
  work, validation cost, reviewability, performance risk, and likely rework.
- Use the dependency map to order prerequisites and identify independent tasks.
  Multiple consumers can make a small utility high-impact.
- Map consumers to behavior requirements and integration/flow tests manually.
  The graph excludes tests and cannot justify skipping required suites by itself.

## Check the result

Keep snapshots for the baseline and candidate, with source SHA/patch identity,
build configuration, compilation-database hash, and tool version/archive hash.
Compare intended boundary changes: remaining C callers, unexpected dependencies,
new cycles, and temporary FFI expected to disappear. Account for file renames and
C-to-Rust replacements before comparing counts. Investigate unexpected changes;
smaller edge counts alone do not prove improvement or compatibility.

For historical POCs, generate at the actual starting SHA. Current graph numbers
may help shortlist cases but cannot characterize the old C graph. Keep graph size
separate from behavioral risks such as persistence, malformed inputs, and concurrency.
