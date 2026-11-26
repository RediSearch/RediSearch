# Port worker policy: Amp

The operations of the core policy are tools named `port_<operation>`: `port_plan`, `port_claim`,
`port_close`, `port_findings`, `port_blocked` and `port_retro`.

## A thread per stage

Each stage gets a fresh worker thread. When the stage you started on is admitted, report the admission without making further changes to the checkout. After your turn ends, the plugin hands the job to a new thread, briefed with the plan, the next stage and the outstanding findings. Your thread stays readable but owns nothing after the handoff; the new one also handles later fixes to your stage.

## Waiting for CI and comments

After a PR is published, try subscribing to its CI and comments with `create_github_subscription`. If the tool is unavailable or the Amp GitHub App cannot access the repository, verify access with local `gh pr view` and continue using `gh` for GitHub reads and the plugin for publication and findings. When `gh` works, report that GitHub access works through local `gh` and only automatic CI/comment notifications are unavailable. Report these together in status updates and handoffs; do not present the subscription failure as a blocker or ask for App access to continue.

Subscriptions wake the thread when CI or comments arrive. Without them, the plugin still collects feedback at the next turn; when only waiting remains, tell the user to resume the thread later rather than claiming automatic monitoring is active. Never poll GitHub or sleep yourself.

## Context

Your context is the largest cost of the job: every request re-reads all of it. Read bulky
material through a subagent that returns only what you need: the module's callers, other
crates, long files and logs. Never print a whole log; the plugin keeps them in files under
`.amp/rust-port/runs/` and quotes their tails.
