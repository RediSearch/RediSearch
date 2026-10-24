# Coordinator search reduction through result processors

## Why

Distributed FT.SEARCH embeds QueryRequest but implements its own rows, selection
heaps, post-processing, serialization, and timeout callbacks. That duplicates
lifecycle work and makes the future worker-to-main-thread drain handoff harder.

The agreed direction is a simple refactor with an explicit accounting of observable
differences. Converting reduction to RPs is useful only if it enables common
execution, reply, timeout, and free handling.

## What changes

Consume the collected shard reply array through a source RP, select/order rows
through bounded sorter RPs, and paginate through the standard pager. Integrate
with QueryRequest's stored results and common callback machinery in small steps.

No new command, option, persistence format, or intentional breaking behavior is
proposed. Preserve shard command construction, fanout, successful reply semantics,
and the current timeout ownership protocol during the initial migration.

This work targets four implementation PRs: mechanical extraction, reply-array
source and ordinary ordering, KNN paths, and common lifecycle/callbacks. Planning
artifacts accompany the first PR.

The public Drain API and nonblocking strict-timeout handoff belong to the user's
separate Drain API stack. This refactor does not add a fifth PR for that work.
Either stack may merge first; the design records the integration boundary.

## Review artifacts

- [Design and source map](design.md)
- [Behavior delta ledger](behavior-deltas.md): authoritative record of differences
- [Implementation tasks](tasks.md)
- [Compatibility requirements](specs/coordinator-search/spec.md)

Tracking: [MOD-20150](https://redislabs.atlassian.net/browse/MOD-20150).

Status: implementation prepared as four local layers following directional
agreement in conversation. Build, unit, and focused three-shard regression runs
are recorded in the behavior ledger; full verification and PR references remain
pending. No new timeout ownership protocol is introduced here.
