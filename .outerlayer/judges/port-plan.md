---
name: Port plan fidelity
description: A port's pull request lists every behavior change as a divergence, pins its contract with behavior-named tests, and ticks one release-notes box.
when:
  paths: ["src/**", "tests/**"]
required: true
---
Read `issue.md`. If it has no `Port stage:` line, pass with the reason "not a port issue" and stop.

Fail, with one item per problem and refs, when:

1. A test the issue's `Contract` or `Divergences` cites for this stage, or an earlier one, is not defined in `tree/` at the path it names.
2. A test this diff adds or a criterion cites is named for its artifact: a word of its name, split on `_` and case changes, is `contract`, `boundary`, `abi` or `compat`.
3. The diff changes behavior the issue's `Contract` pins, or you can name an input on which the Rust in `tree/` behaves differently from the C the diff deletes or replaces, and no `Divergences` entry lists that change. Cite the input and both behaviors.
4. `pull-request.md` does not list every `Divergences` entry of the issue, or a "Plan changes" section is missing although the issue's `Paths` or `Port checks` differ from what the diff needed.
5. `pull-request.md` lacks either release-notes box of `.github/pull_request_template.md`, or does not have exactly one of them ticked.
6. The diff marks a test `#[ignore]` or `skip`, weakens an assertion, or removes a benchmark case.
