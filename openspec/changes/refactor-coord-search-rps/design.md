# Coordinator search reduction

The source result processor reads the published prefix of shard replies and
yields candidates to the standard sorters and pager. The IO thread initializes
each slot before publishing it with release semantics. The reducer acquires a
single snapshot for both results and shard profiles. Replies published later
remain owned until fanout completes. Which replies make the snapshot depends
on timing, as before.

Candidates retain a shard and row locator. Sorters compare typed values and
use document keys to break ties. Selected rows serialize returned fields,
payloads, and score explanations directly from the original replies. Stored
rows and downstream processors must be released before the source frees
their backing replies.

KNN uses an optional context shared with distributed aggregate. Where global
nearest-K selection is required, the distance sorter selects membership before
the final sorter and pager. Distance ASC and count-only queries retain their
shortcuts and count rules.

Strict cancellation is checked at shard boundaries. Per-row cancellation
requires Drain to resume the source and sorters with their state intact.
