# Port worker policy: RediSearch

You port one C module to Rust as three stacked PRs: one that pins the C's behavior with flow
tests, then the two `port-c-module` describes.

| Stage      | Deliverable                                                         | Published as             |
| ---------- | ------------------------------------------------------------------- | ------------------------ |
| `coverage` | Flow tests for the module's coverage gaps; assertions for dead code | PR into the base branch  |
| `rust`     | The Rust PR of `port-c-module`'s _Two PRs_                          | PR stacked on `coverage` |
| `swap`     | The swap PR of `port-c-module`'s _Two PRs_                          | PR stacked on `rust`     |

How to port is in the skills: read `port-c-module`, `rust-review`, `rust-docs-guidelines`,
`rust-tests-guidelines`, `check-flow-coverage`, `improve-flow-coverage` and `write-flow-tests`
before planning. Load only these seven, and only once each, even after a compaction: the plugin
and your checks replace `open-pr`, `adversarial-review`, `verify`, `build`, `lint` and the
`run-*` skills.

## Coverage before planning

Before calling `port_plan`, run `check-flow-coverage` on the module's `.c` files. Its gaps decide
the `coverage` stage, the first: it follows `improve-flow-coverage` without asking which gaps to
address. Every testable gap gets a flow test asserting what the command returns, and every
unreachable defensive path an assertion, or an LCOV exclusion where it frees resources.
Disk-only and API-only paths stay as they are, and so does code only a cluster or the
coordinator runs: the coverage measures standalone runs, and an assertion compiles out of release
builds, so asserting a path a cluster reaches breaks it there. The stage changes no reachable
behavior: it adds tests, assertions and exclusions, and nothing else. Its claim's `test` check
replaces the skill's verification step: the plugin reports the module's coverage on the PR and
fails the check when the run wrote no fresh coverage of a module source.

Each flow test the stage adds pins C behavior the Rust must keep, so cite it in `contract` with
stage `coverage`; the `rust` and `swap` claims then run it too. A divergence's test shows the
Rust's behavior, so cite it at `rust` or later. The `coverage` PR opens only when the stage
improves coverage: when the module has no gap to close, cite no test at `coverage` and claim the
base revision itself. The stage is then skipped, and `rust` opens straight into the base branch.

## The plan

`contract` and `divergences` are `port-c-module`'s _Contract and divergences_; a divergence
states the C behavior in `c` and the Rust behavior in `rust`. Two fields are a port's own:

- **`module`:** the C files being ported, sources and headers; `swap` deletes them.
- **`symbols`:** every function the module's headers declare, with either the `rust` item that
  replaces it or a `retired` reason when the swap removes the function. A replacement is
  exported under the C function's own name; a retired function must not be declared in a
  generated header. The plugin reads the headers on the base branch and rejects a plan that
  leaves one out. Retire test-only APIs rather than adding exports for them, and migrate any
  required test coverage to Rust.
- **`replacedTests`:** every C and C++ test the swap deletes, as its `path` and `name`, with `by`:
  the `path` and `name` of a test the contract or divergences cite that covers it instead. A
  gtest is named `Suite.name`, a C test by the function its `TESTFUNC` or `MU_RUN_TEST`
  registers. The plugin rejects a test the base branch does not define, one outside the swap's
  `paths`, and a `by` the plan does not cite. Only the swap deletes C tests, and only those it
  lists. Leave it empty when no C test goes.

The plugin rejects a test named for its artifact (`contract`, `ABI`, `boundary`, `compat`).
Name each crate a stage changes in its `paths`, files or directories that hold at most one
crate; the plugin rejects a directory that spans several. Generated headers and lock files need
no path. PR bodies follow `.github/pull_request_template.md` with exactly one release-notes box
ticked.

```jsonc
{
  "repository": "owner/name",
  "remote": "origin", // where stage branches <branchPrefix>-<stage> are pushed
  "baseBranch": "master",
  "branchPrefix": "port-geo", // a name of your own; never a person's
  "module": ["src/geo_index.c", "src/geo_index.h"], // swap deletes the sources
  "contract": [
    {
      "behavior": "An unknown unit fails with `Unknown distance unit`.",
      "tests": [{ "path": "tests/pytests/test_geo.py", "name": "testUnits", "stage": "coverage" }],
    },
  ],
  "divergences": [
    {
      "behavior": "Output of a failed parse",
      "c": "...",
      "rust": "...",
      "why": "...",
      "test": { "path": "...", "name": "...", "stage": "rust" },
    },
  ],
  "symbols": [{ "c": "GeoFilter_Validate", "rust": "geo::RadiusFilter::new" }],
  "replacedTests": [
    {
      "path": "tests/cpptests/test_cpp_geo.cpp", // in the swap's paths
      "name": "GeoTest.units",
      "by": { "path": "tests/pytests/test_geo.py", "name": "testUnits" },
    },
  ],
  "structure": "shared structure, dependencies, and the handoff between the stages",
  "interfaces": "names, types, ownership and signatures swap depends on",
  "stages": {
    "coverage": {
      "deliverable": "...",
      "paths": ["src/geo_index.c", "tests/pytests/test_geo.py"],
      "checks": [{ "kind": "test", "command": "./build.sh RUN_PYTEST COV=1" }],
      "title": "...",
      "body": "...",
    },
    "rust": {
      "deliverable": "...",
      "paths": [
        "src/redisearch_rs/geo/",
        "src/redisearch_rs/geo_bencher/",
        "tests/pytests/test_geo.py",
      ],
      "checks": [
        { "kind": "test", "command": "..." },
        {
          "kind": "miri",
          "command": "cd src/redisearch_rs && cargo +nightly miri nextest run -p geo",
        },
        { "kind": "mutants", "command": "see Checks" },
        {
          "kind": "lint",
          "command": "make lint && make fmt CHECK=1 && cd src/redisearch_rs && cargo hakari generate --diff && cargo hakari manage-deps --dry-run",
        },
        {
          "kind": "bench",
          "command": "cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p geo_bencher",
        },
      ],
      "title": "...",
      "body": "...",
    },
    "swap": {
      /* paths: the FFI crate and the files of replacedTests; checks: test, miri, lint, and
         mutants if it adds Rust that is not an export */
    },
  },
}
```

## Checks

Make the checks catch what CI would, as `port-c-module`'s _Verify as CI Does_ describes. Use
`CLAIM_BASE` and `CLAIM_CANDIDATE` as the mutants diff range.

- **`test`:** the Rust tests of the crates you touch, and the Python and C tests the contract
  names. In `coverage`, every flow test with coverage, `./build.sh RUN_PYTEST COV=1` without
  `TEST=` or `QUICK=1`, since a subset reports gaps the suite does not have; add
  `./build.sh RUN_UNIT_TESTS DEBUG=1` when an assertion guards code the C unit tests reach, since
  only a debug build compiles assertions in. `coverage` has no other check.
- **`miri`** (`rust` and `swap`): the tests of every crate the stage adds or changes. A test nextest
  reports as `SLOW` fails the check.
- **`mutants`** (`rust`, and `swap` if it adds Rust that is not an `unsafe extern` export). A
  run that finds no mutants fails, because it covers nothing.
- **`lint`:** `make lint`, `make fmt CHECK=1` and both `cargo hakari` commands.
- **`bench`** (`rust`): the `<module>_bencher` crate `port-c-module` describes, every benchmark of
  it. The plugin asks Cargo where Criterion wrote its results; Rust may be at most 5% slower than
  C in any group, and every group must measure both its `c` and its `rust` function. Keep
  Criterion's default output path. `swap` deletes the `c` functions and the C linking along with
  the module; the `rust` functions stay as the module's benchmark.

## House rules and the swap

The house rules are `rust-review`'s, and a new crate needs `port-c-module`'s lint block. A swap
claim is rejected until it meets the plan's `symbols` and `replacedTests`: every Rust
replacement defined and exported under its C name in a generated header, every retired function
absent from generated headers, every `module` file deleted, every replaced test and no other C
test deleted, and no C ABI export left that only tests call.

## CI

CI is one finding per PR, for `pr-validation`. Sonar and Codecov are advisory.
