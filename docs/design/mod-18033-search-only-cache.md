# MOD-18033: Search-owned background metrics

## Scope and behavior

RediSearch and RediSearchEnterprise change together. Redis and Speedb need no
changes. The existing BigModule V1 disk-usage callback returns one atomic total;
it does not walk the C index dictionary or read a native property. Ordinary
reads return the last successful value, including after expiry or an error.
New empty indexes contribute zero. Existing storage is sampled synchronously
when opened, before its contribution becomes visible.

Search INFO still walks visible indexes to aggregate numeric snapshots and live
RAM counters. It does not collect Speedb properties. FT.INFO and other explicit
synchronous diagnostic consumers retain their existing behavior. Speedb's own
internal statistics collection is outside this change.

Operational accounting uses the existing `live-sst-files-size` meaning for the
document-table, fulltext, missing, tag, numeric and vector column families. The
current RSE branch stores the document table in `default`; use its configured
constant rather than an old literal name. This is live SST size, not physical
filesystem usage, retained obsolete files, or memtable bytes.

## Private interface

Five added callbacks connect the repositories: `getCollector`, `collect`,
`activateTarget`,
`getCachedTotalDiskUsage` and `readCachedIndexMetrics`. The last returns one
numeric record containing memory, operational disk usage and block estimates,
and stages the component snapshot for the existing INFO output callback.

Index retirement and INFO-map cleanup run
through the existing main-thread close callback, which receives the owning disk
context. No separate retirement or blocking freshness API crosses FFI.

## Executor and scheduling

RediSearch owns one dedicated single-worker pool using its existing `deps/thpool`
implementation. It does not share the query or GC queue. A 50 ms module timer
submits work. There is at most one outstanding collection job. A running
job may replace itself with one continuation.

RSE supplies one private collection callback/context. Each operational batch
checks its approximately 5 ms budget between native reads. A single property
read can exceed that budget. Indexes rotate fairly, one CF at a time. Each
index's next ordinary pass is due one second after the previous pass started.
An overrun therefore makes the next pass immediately eligible. Flush/compaction
completion sets an index's dirty flag, making it eligible before its
normal due time. Events coalesce and never enqueue a job per write.

Each worker invocation runs an operational batch and a diagnostic batch, so
a large operational pass cannot starve diagnostics. Their slower refresh cadence
remains. Both
lanes retain progress across jobs. Diagnostic SST fields are sampled in that
lane too, but INFO overlays them with operational SST values. Removing duplicate
background SST reads is a separate simplification, not required for cheap reads.

There is no hard freshness bound under sustained overload or a long native
read. Last-good values remain available; collection failure retries with a short
backoff. Operators can inspect operational oldest-sample age, pending dirty
indexes, unready indexes and collection errors. Health is sampled by the worker;
its getters do not scan SSTs. There is no synchronous expiry fallback.

## Ownership and lifecycle

The worker never touches `specDict_g` or C IndexSpecs. Its Rust registry holds
weak entry/DB references and CF names plus native identities. A DB and CF are
pinned only during a native read. A CF-layout revision rejects results collected
for an old target. Each index publishes its contribution and metric categories
atomically; a wider internal ledger prevents overflow from breaking later
subtraction. The externally visible total saturates at `u64::MAX`.

A separate lifetime native listener marks flush and compaction completion with
atomics only. Its RAII token is owned alongside the DB using `self_cell`, so it
is removed before the DB closes. Existing GC-scoped listeners retain their
existing reclaimed-byte semantics.

An index becomes accounted when added to the visible C registry. Removal/drop
subtracts its contribution immediately, then drains its current native read
before BigModule unregisters the DB. Late completion cannot re-add a removed
index. Dynamic CF creation replaces the target and requests refresh.
Cold DB open/reopen seeds existing SSTs before activation.

The collector only reads native properties, so foreground persistence windows
do not pause it. Shutdown stops the timer and drains/joins the pool before
closing DiskContext. Generic `pthread_atfork` hooks prevent new work and drain
one current batch before fork. The parent resumes; the child submits/joins no
worker and reads atomic scalar mirrors rather than inherited Rust locks.

## Cache structure

Two caches share one worker. Operational usage needs immediate create/drop accounting
and one atomic total for quota/eviction. Diagnostic INFO needs stable aggregates of
many properties. Keeping these contracts separate avoids coupling their refresh cycles.

```mermaid
flowchart TD
    Context[DiskContext] --> Collector[RSE Collector]
    Worker[RediSearch worker] -->|calls| Collector
    Collector --> Usage[UsageCache]
    Collector --> Diagnostics[AsyncSnapshots]
    Index[Rust IndexSpec] --> UE[usage_cache::Entry]
    Index --> DE[async_snapshot::Entry]
    Usage -. weak registry .-> UE
    Diagnostics -. weak queue .-> DE
    UE --> State[IndexState and CF Samples]
    UE --> Counters[Published atomic counters]
    DE --> Pending[Pending Collection]
    DE --> Working[Working Collection]
    DE --> Published[Published immutable Snapshot]
    Working -->|publishes| Published
    Listener[Native UsageListener] -. marks .-> Signal[DirtySignal]
    UE --> Signal
```

The index owns its entries; the registries never keep an index alive. The worker
borrows native DB/CF handles only for a property read and never accesses C IndexSpecs.

| Type | Role and reason for the boundary |
|---|---|
| `Collector` | Shared worker context, separate from the mutable main-thread `DiskContext`. Services both caches each invocation. |
| `Target` | Weak DB reference and CF names/identities, shared by both cache implementations. Detects replaced CFs without retaining native handles. |
| `UsageCache` / `Registry` | Global atomic total for readers; membership and the exact accounting sum change together under the registry lock. |
| `usage_cache::Entry` / `IndexState` | Per-index published counters plus locked lifecycle/refresh state. A separate native-read lock lets drop debit accounting before draining the read. |
| `Sample` | Last-good byte count and sample time for one CF; errors preserve both. |
| `DirtySignal` / `UsageListener` | Minimal event notification state and its native adapter. A callback can request refresh without retaining the index. |
| `AsyncSnapshots` | Weak scheduling queue and diagnostic error accounting; the C executor supplies the single worker. |
| `async_snapshot::Entry` | One index's pending replacement, active collection, and published result; rejects obsolete results and drains on retirement. |
| `Collection` | Target, cursor, due time, revision, and working snapshot. Resumes a pass after yielding between native properties. |
| `Snapshot` / `CfSample` | Stable aggregate plus per-CF history. Per-property success times prevent a successful read from making a failed property appear fresh. |
| `Health` | Small result type summarizing the same snapshots retained for INFO output. |

The diagnostic entry has three distinct roles:

- **Pending:** the latest layout replacement; submitting it must not wait for native I/O.
- **Working:** mutable collection progress; its lock spans one native property read.
- **Published:** an immutable result retained by INFO readers while collection continues.

A layout revision prevents old work from publishing after replacement. Working and
published snapshots share storage until the next property read needs a private copy.
Per-CF history preserves matching last-good values when the layout changes; the
precomputed aggregate keeps INFO from walking that history.

These ownership and synchronization boundaries are the important part, not the
number of named types. Small wrappers could be inlined, but that alone would not
remove the state or locking requirements.

RediSearch owns timer/pool scheduling, the pre-fork barrier, and early shutdown.
RSE owns native collection, cache ownership, and accounting. Stop rejects new jobs
and drains before index destruction. Child-process checks do not replace the
pre-fork drain; there is no general pause/resume API or second collector mutex.

## Coordinated PRs and qualification

1. **RediSearch:** the pool, fork/shutdown gates, V1 callback, cached INFO
   reads, visibility hooks, private FFI contract and C/C++ tests.
2. **RediSearchEnterprise:** the operational ledger, listener ownership, seeding,
   fair incremental refresh, diagnostic snapshots and scalar mirrors,
   Rust tests and the matching RediSearch dependency.

Validation must cover successful/error/zero samples, overflow followed by drop,
native flush events, persisted reopen, layout replacement, retention of last-good
values on errors, diagnostic fairness, stop/drain, fork and shutdown.
Representative performance qualification remains a separate matched comparison
on an idle machine: normal writes/reads with and without INFO, main-thread
property attribution, operational cache age and collector native contention.
Historical prototype timings do not establish this implementation's results.
