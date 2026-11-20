---
name: build
description: Build one approved issue end to end. Invoke with OUTERLAYER_WORK=<n> claude "/build".
disable-model-invocation: true
---
<!-- yours to edit. Copied once from the default template by outerlayer init 0.42.0; never overwritten. -->

Read ~/.agents/skills/outerlayer-build/SKILL.md and follow it from start to finish. Apply the changes below for this repository.

## Settings

- **Gate command:** for a port issue, the checks its stage lists under "Port checks" below, in
  order, stopping at the first that fails; a check passes only if every command in it does. For
  any other issue, the checks CLAUDE.md names for the code the change touches.
- **Agent budget:** 30.
- **Branch prefix:** `port-` for a port issue, `feat/` otherwise.

## Changes for this repository

A **port issue** is one whose body has a `Port stage:` line, naming `rust` or `swap`, as
`.agents/skills/spec/SKILL.md` files it. The changes below apply only to port issues; every other
issue follows the core alone.

### Intake

- Read `.skills/port-c-module/SKILL.md`, `.skills/rust-review/SKILL.md`,
  `.skills/rust-docs-guidelines/SKILL.md` and `.skills/rust-tests-guidelines/SKILL.md`, once each,
  even after a compaction. They say how to port. Do not load `open-pr`, `adversarial-review`,
  `verify`, `build`, `lint` or the `run-*` skills: this build, its gate and the factory's review
  replace them.
- The issue holds the plan: `Module`, `Symbols`, `Contract`, `Divergences`, `Paths` and
  `Port checks`. It exists only there; never write it to a file or commit it.
- For a `swap` issue, stop with `outerlayer work needs-attention` unless the `rust` issue it
  names under `Depends on` has a merged pull request: the swap builds on the merged Rust.

### Criteria

- Every `Contract` item and every `Divergences` item is a criterion, with `proof: test` naming
  its test. Keep the issue's ids.

### Implement

- Change only the files and directories under the issue's `Paths`, generated headers and
  `Cargo.lock` aside. When the work shows a path is missing or a check command does not work,
  say so in the pull request description under "Plan changes" and change the issue with
  `outerlayer spec edit`. Never use that to weaken or drop a check, cite fewer tests, or widen
  scope to dodge the size gate.
- Write less code. `port_checks.py diff` reports the stage's size class: up to 250 added lines
  needs nothing; M (up to 500) means delete what the port does not need before going on; L (up to
  1000) means stop adding and cut; XL (over 1000) fails the gate. Deleting never costs a class.
- Once the factory's code review has reviewed the branch, a fix may not grow the stage's net
  code (lines of code added minus deleted, tests and generated files aside): it pays for itself
  by deleting as much as it adds. Tests may grow.
- Never weaken a test, mark one `#[ignore]`, or remove a benchmark case.
- A `swap` is done only when every `Symbols` entry's Rust item is defined and exported under the
  C function's own name in a generated header (`make generate-rust-headers`), every `Module` file
  is deleted, and no C ABI export is left that only tests call.

### Pull request

- The title and body follow `.github/pull_request_template.md`, with exactly one release-notes
  box ticked. The body lists the issue's `Divergences` as a table (behavior, C, Rust, why, test),
  where a person accepts or rejects each, and a "Plan changes" section if the build changed the
  issue.
- Never merge, enable auto-merge or use a merge queue.

### Evidence

- Skip the manual test and screenshots: RediSearch has no user interface. Attach each failing or
  passing gate log the final gate wrote as an artifact.
- Retrospective: before the final report, record each item as a finding with the
  `outerlayer-findings` skill. It is about the next port, not a recap: what went well needs no
  entry. Record mistakes (what went wrong, its cost in rounds, time or lines, what would have
  prevented it), cuts (what the delivered diff should not contain, with an estimate of the lines)
  and improvements (to these skills, the checks, the review rules or the judges), each citing
  files, lines, logs or findings.

## Port checks

Each command runs from the repository root. `$BASE` is the merge base of the branch with
`origin/master`. Name each crate the stage adds or changes with `-p`.

| Check     | Stages                                                  | Command                                                                                                                                                                           |
| --------- | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `diff`    | both                                                    | `.outerlayer/port_checks.py diff --base "$BASE" --paths <the issue's Paths>`                                                                                                     |
| `test`    | both                                                    | `cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crates>`, then the Python and C tests the contract names (`./build.sh RUN_PYTEST TEST=<file>:<test>`)        |
| `miri`    | both                                                    | `.outerlayer/port_checks.py miri -- cargo +nightly miri nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crates>`                                                    |
| `mutants` | `rust`; `swap` if it adds Rust that is not an `unsafe extern` export | `.outerlayer/port_checks.py mutants -- cargo mutants --manifest-path src/redisearch_rs/Cargo.toml -p <crates> --in-diff <(git diff "$BASE")`                          |
| `lint`    | both                                                    | `make lint && make fmt CHECK=1 && (cd src/redisearch_rs && cargo hakari generate --diff && cargo hakari manage-deps --dry-run)`                                                  |
| `bench`   | `rust`                                                  | `.outerlayer/port_checks.py bench -- cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p <module>_bencher`                                                               |

`bench` runs every benchmark of the `<module>_bencher` crate `port-c-module` describes; each group
measures both a `c` and a `rust` function, and Rust may be at most `SLOWDOWN_PERCENT`
(`.outerlayer/port_checks.py`) slower than C in any of them.
The `swap` deletes the `c` functions and the C linking along with the module.
