# Port worker policy: RediSearch

You port one C module to Rust as the two stacked PRs `port-c-module` describes.

| Stage  | Deliverable                                | Published as            |
| ------ | ------------------------------------------ | ----------------------- |
| `rust` | The Rust PR of `port-c-module`'s _Two PRs_ | PR into the base branch |
| `swap` | The swap PR of `port-c-module`'s _Two PRs_ | PR stacked on `rust`    |

How to port is in the skills: read `port-c-module`, `rust-review`, `rust-docs-guidelines` and
`rust-tests-guidelines` before planning. Load only these four, and only once each, even after a
compaction: the plugin and your checks replace `open-pr`, `adversarial-review`, `verify`,
`build`, `lint` and the `run-*` skills.

## The plan

`contract` and `divergences` are `port-c-module`'s _Contract and divergences_; a divergence
states the C behavior in `c` and the Rust behavior in `rust`. Two fields are a port's own:

- **`module`:** the C files being ported, sources and headers; `swap` deletes them.
- **`symbols`:** every function the module's headers declare, with the Rust item that replaces
  it, which `swap` exports under the C function's own name. The plugin reads the headers on the
  base branch and rejects a plan that leaves one out.

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
      "tests": [{ "path": "tests/pytests/test_geo.py", "name": "testUnits", "stage": "rust" }],
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
  "structure": "shared structure, dependencies, and the handoff between the stages",
  "interfaces": "names, types, ownership and signatures swap depends on",
  "stages": {
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
      /* checks: test, miri, lint, and mutants if it adds Rust that is not an export */
    },
  },
}
```

## Checks

Make the checks catch what CI would, as `port-c-module`'s _Verify as CI Does_ describes. Use
`CLAIM_BASE` and `CLAIM_CANDIDATE` as the mutants diff range.

- **`test`:** the Rust tests of the crates you touch, and the Python and C tests the contract
  names.
- **`miri`** (every stage): the tests of every crate the stage adds or changes. A test nextest
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
claim is rejected until it meets the plan's `symbols`: every Rust replacement defined, every C
function exported under its own name in a generated header, every `module` file deleted, and no
C ABI export left that only tests call.

## CI

CI is one finding per PR, for `pr-validation`. Sonar and Codecov are advisory.
