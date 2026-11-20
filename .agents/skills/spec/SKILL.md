---
name: spec
description: >
  Turns an idea into an approved issue: searches for duplicates, drafts
  acceptance criteria with their proof forms, decides the docs
  verdict, splits work that is too big for one story, and files it after you
  approve it. Then offers to start the build. Use when you want to write up a
  feature or change before building it. Invoke with
  /spec <what you want built>.
---
<!-- yours to edit. Copied once from the default template by outerlayer init 0.42.0; never overwritten. -->

Read ~/.agents/skills/outerlayer-spec/SKILL.md and follow it from start to finish. If that file is missing, run `outerlayer init --local` first, or `npx -y @outerlayer/cli init --local` when `outerlayer` is not on PATH. Apply the settings and the changes below for this repository.

## Settings

- **Repository:** `mainmatter/RediSearch`.

## Changes for this repository

A **port** is an idea to port a C module to Rust. The changes below apply only to ports; every
other idea follows the core alone.

### Scope

- Read `.skills/port-c-module/SKILL.md` and follow its _Analyze the C Code_ and _Define A Porting
  Plan_ sections: read the module, its callers and its tests, and measure the module's flow-test
  coverage. Read bulky material, such as callers in other crates and long files, through an agent
  that returns only what you need.

### Draft

Draft the plan in the issue body under these headings. The build reads them by name.

- `Port stage:` on its own line, `rust` or `swap`.
- `Depends on:` (the `swap` issue only): the `rust` issue.
- `Module`: the C files being ported, sources and headers; the `swap` deletes them.
- `Symbols`: one line per function the module's headers declare, as `` `C_Function` → `crate::Item` ``,
  the Rust item the `swap` exports under the C function's own name. Read the headers on `master`;
  leave none out and invent none.
- `Contract`: `port-c-module`'s contract. Each item is a criterion with `proof: test`, naming the
  test that pins it as `path::name` and the stage that adds it. Name each test for the behavior it
  asserts, never for its artifact (`contract`, `ABI`, `boundary`, `compat`).
- `Divergences`: `port-c-module`'s divergences, each as what C does, what Rust does, why, and the
  test that shows the Rust behavior. Each is a criterion with `proof: test`. An empty list means
  the Rust is indistinguishable from the C.
- `Paths`: the files and directories the stage may change, each holding at most one crate.
  Generated headers and lock files need no path.
- `Port checks`: the commands of `.agents/skills/build/SKILL.md`'s Port checks table, filled in
  for this module's crates and contract tests.
- `Structure` and `Interfaces` (the `rust` issue): the structure the stages share, and the names,
  types, ownership and signatures the `swap` depends on.

### Size

- A port is always two issues, filed together: `rust` (the Rust PR of `port-c-module`'s _Two PRs_)
  and `swap` (the swap PR), which depends on it. Split further only when a stage cannot stay
  under 1000 added lines.

### File

- File the `rust` issue first, then the `swap` issue naming it under `Depends on:`.

### Build

- Offer to build only the `rust` issue. The `swap` is built after the `rust` pull request merges.
