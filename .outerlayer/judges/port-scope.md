---
name: Port scope and size
description: A port stage changes only its issue's Paths, stays under 1000 added lines and leaves no blank-line-only file.
when:
  paths: ["src/**", "tests/**"]
required: true
---
Read `issue.md`. If it has no `Port stage:` line, this is not a port: pass with the reason "not a port issue" and stop.

Otherwise judge `diff.patch` against the issue's `Paths`. Files under `src/redisearch_rs/headers/` and any `Cargo.lock` are generated and exempt from every check below. Markdown and `.gitignore` files do not count toward size.

Fail, with one item per problem and a ref to each file, when:

1. A changed file is outside every entry of `Paths` (an entry ending in `/` covers everything below it). Name the file and the paths.
2. An entry of `Paths` is a directory that holds more than one `Cargo.toml` in `tree/`: the stage must name each crate it changes.
3. A file's change consists only of added or removed blank lines.
4. The diff adds more than 1000 lines in total (count `+` lines, not `+++` headers).

When the diff adds more than 250 lines, add a passing item that states the size class: M up to 500, L up to 1000.

If `previous.json` holds an earlier result that passed, and the code review has reviewed the branch since, compare net lines of code (added minus deleted, outside `tests/`, `benches/` and `#[cfg(test)]` modules) with that result's: an increase fails, because a fix after review must delete as much code as it adds. Record the net count in a passing item so the next run can compare.
