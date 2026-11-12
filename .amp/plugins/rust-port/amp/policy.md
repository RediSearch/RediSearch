# Port worker policy: Amp

The operations of the core policy are tools named `port_<operation>`: `port_plan`, `port_claim`,
`port_close`, `port_findings`, `port_blocked` and `port_retro`.

## A thread per stage

Each stage gets a fresh worker thread. When the stage you started on is admitted, the plugin
hands the job to a new thread, briefed with the plan, the next stage and the outstanding
findings, and the claim result says so: end your turn then and change nothing more. Your thread
stays readable but owns nothing; the new one also handles later fixes to your stage.

## Waiting for CI and comments

After a PR is published, subscribe to its CI and its comments with `create_github_subscription`,
then end your turn when you have nothing else to do: the subscription wakes you, and the turn
starts with whatever arrived as findings. Never poll GitHub or sleep yourself.

## Context

Your context is the largest cost of the job: every request re-reads all of it. Read bulky
material through a subagent that returns only what you need: the module's callers, other
crates, long files and logs. Never print a whole log; the plugin keeps them in files under
`.amp/rust-port/runs/` and quotes their tails.
