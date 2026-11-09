# rust-port

An Amp plugin that gates a staged C-to-Rust port. The worker reasons; the plugin runs the checks,
records evidence, decides admission, opens the stage PRs and tracks what is left to do. The worker
follows [`core/policy.md`](core/policy.md), [`amp/policy.md`](amp/policy.md) and
[`redisearch/policy.md`](redisearch/policy.md).

```
amp  --drives-->  core  --configured by, calls hooks in-->  redisearch  --composes-->  c-to-rust
```

| Directory     | What it owns                                                                                                                                                                                                                                                                                                                                                    |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `core/`       | The workflow, independent of agent host, language and project. `ports.ts` declares everything it needs from outside: `Host`, `Forge`, `Vcs` and `Profile`. `engine.ts` runs the job: operations, claims, assessment, publication and handoff. `git.ts` and `github.ts` are the `Vcs` and `Forge` it ships with; the engine reaches them only through the ports. |
| `c-to-rust/`  | What any port of C to Rust in a Cargo workspace shares: Rust source scanning, Cargo judges, the plan's `module` and `symbols`, the swap gate, and the reviewers.                                                                                                                                                                                                |
| `redisearch/` | The `Profile`: RediSearch's stages, checks, layout, house rules, CI gate and PR template.                                                                                                                                                                                                                                                                       |
| `amp/`        | `Host` on Amp threads, and the plugin: tools, the grant command, the turn hooks and the status item.                                                                                                                                                                                                                                                            |

Porting another repository means writing another profile; another language, another library
like `c-to-rust/`; another agent host, another adapter like `amp/`; another forge, another
implementation of `Forge` beside `core/github.ts`. `core/` imports none of the other directories.

## Install and build

```sh
npm --prefix .amp/plugins/rust-port ci
npm --prefix .amp/plugins/rust-port run build
```

Amp loads `dist/plugin.js`, one bundle of every directory, through `index.js`; rebuild after a
source change, then run **plugins: reload**. `npm run check` (format, type-check, tests) runs
before every commit.

GitHub calls use `GH_TOKEN`/`GITHUB_TOKEN`, else the token git's credential helper has for
`github.com`, which in an orb is Amp's. It needs to push, open PRs, reply and resolve threads.

## How a job runs

The thread that calls `port_plan` owns the job; hooks ignore every other thread. Each
`port_claim` of a committed, checked-out revision:

1. **Gates**, at no cost: the scope, blank-line-only files, the XL size class, the growth floor
   (a reviewed stage's net code may not grow), the house rules, the cited tests, fixes the
   candidate lacks, and the profile's own gates, such as everything the plan's `symbols` promise
   for the swap.
2. **Checks**, one at a time, stopping at the first failure; then every fixed finding's
   reproduction.
3. **Reviewers**, if all passed: fresh threads on a stage's first review, then the same threads
   asked about the change since. A finding blocks only if its failing test fails, its deletion
   keeps the tests passing, or it cites a rule.
4. **Admission**, if the evidence passed and no blocking finding holds the stage: the stage PR is
   opened or updated, stacked on the one before it. Admitting the stage a worker started on
   hands the job to a fresh thread for the next stage, briefed with the plan and the outstanding
   findings, because every request re-reads its thread's history. Former workers own nothing.

Evidence is keyed by stage, candidate, base and plan hash, so passing evidence is reused and an
amendment invalidates every admission. Everything left to do is a finding; the worker closes
reviewer findings and comments, and failed checks and CI close themselves.

## State and debugging

`.amp/rust-port/state.json` holds the job, evidence and findings; `runs/` the diffs and check
logs; `debug.log` every step, also sent to Amp's log. A state from an older version is refused:
move it aside.

```sh
tail -f .amp/rust-port/debug.log
```

## Budget and continuation

A job runs 30 assessments; a rejected claim, a reviewer failure and reused evidence cost none.
After each turn the worker continues only while it has budget, has not reported a blocker with
`port_blocked`, and the last continued turn moved the job. A finished job waiting for CI sleeps
until its GitHub subscription wakes it. Otherwise it waits for a human, who can run **rust-port:
Grant assessments** from the command palette; agents cannot.

To measure a run, use `amp threads usage <id> --details` and `amp threads export <id>` for each
worker thread (the owner and `job.former`) and each reviewer thread; reviewer thread IDs and
check times are in the evidence.
