# rust-port

An Amp plugin that gates a staged C-to-Rust port. The worker reasons; the plugin runs the checks,
records evidence, decides admission, opens the stage PRs and tracks what is left to do. The worker
follows [`policy.md`](policy.md).

| File            | What it owns                                                                           |
| --------------- | -------------------------------------------------------------------------------------- |
| `plugin.ts`     | The loop: tools, hooks, claims, assessment, publication, state on disk.                |
| `state.ts`      | The job, evidence and findings, and the pure functions over them.                      |
| `plan.ts`       | The plan: its shape, validation, header inspection and PR bodies.                      |
| `gates.ts`      | What rejects a claim before anything runs: scope, size, growth, rules, swap admission. |
| `checks.ts`     | Running the stage's checks and judging their logs (Miri, mutants, benchmarks).         |
| `review.ts`     | The reviewer threads: their instructions, brief, follow-ups and parsed proposals.      |
| `confirm.ts`    | Turning proposals into findings by running their failing test or deletion.             |
| `github.ts`     | Publishing stacked PRs, collecting comments and CI, replying.                          |
| `refs.ts`       | Checking cited files, lines and tests.                                                 |
| `rules.ts`      | The house-rule ratchet and the Rust source patterns rules count.                       |
| `redisearch.ts` | Everything specific to RediSearch: layout, required checks, runners, CI, house rules.  |

Porting another repository means replacing `redisearch.ts`; the other files hold only what any
C-to-Rust port in a Cargo workspace shares.

## Install and build

```sh
npm --prefix .amp/plugins/rust-port ci
npm --prefix .amp/plugins/rust-port run build
```

Amp loads `dist/plugin.js` through `index.js`; rebuild after a source change, then run **plugins:
reload**. `npm run check` (format, type-check, tests) runs before every commit.

GitHub calls use `GH_TOKEN`/`GITHUB_TOKEN`, else the token git's credential helper has for
`github.com`, which in an orb is Amp's. It needs to push, open PRs, reply and resolve threads.

## How a job runs

The thread that calls `port_plan` owns the job; hooks ignore every other thread. Each
`port_claim` of a committed, checked-out revision:

1. **Gates**, at no cost: the scope, blank-line-only files, the XL size class, the growth floor
   (a reviewed stage's net code may not grow), the house rules, the cited tests, fixes the
   candidate lacks, and for the swap everything the plan's `symbols` promise.
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
