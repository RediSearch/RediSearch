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

No new command, option, or persistence format is proposed. The approved follow-up
corrects global KNN membership with numeric final sorting and distance DESC, and
aligns sort values with standard typed comparison. Preserve fanout and the current
timeout ownership protocol; see the behavior ledger for observable differences.

This work targets four implementation PRs: mechanical extraction, reply-array
source and ordinary ordering, KNN paths, and shared stored-result ownership. Planning
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

Status: four ready-for-review PRs in native stack 11989, following directional
agreement in conversation:

1. [Extraction](https://github.com/RediSearch/RediSearch/pull/11985)
2. [Reply source and ordinary ordering](https://github.com/RediSearch/RediSearch/pull/11986)
3. [KNN reduction](https://github.com/RediSearch/RediSearch/pull/11987)
4. [Source reply ownership and search fanout](https://github.com/RediSearch/RediSearch/pull/11988)

Local validation and its scope are recorded in the behavior ledger. Remote CI
and automated review are separate gates. PR 4 also moves shard-reply ownership
into the source and synchronizes reply admission at timeout. BG/main reduction
arbitration remains unchanged; abort/drain handoff stays in the Drain stack.
