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

An index owns one usage entry. Its atomic total and six category counters are
published values; the native listener shares only its dirty signal. A mutex
protects the CF target, aligned last-good samples, lifecycle flags and refresh
cursor. The revision rejects a result collected before a layout replacement.
A separate native-read mutex lets retirement debit accounting first and then
drain the read before the DB is unregistered.

The module-wide cache keeps a queue of weak entries and an exact accounting sum,
with one atomic total for operational readers. It never owns a C IndexSpec.
Health publication is separate from the operational getter and runs at most
once per second. No freshness tickets, waiting callers, scope generations or
cross-repository availability notifications are needed.

## Coordinated PRs and qualification

1. **RediSearch:** the pool, pause/fork/shutdown gates, V1 callback, cached INFO
   reads, visibility hooks, private FFI contract and C/C++ tests.
2. **RediSearchEnterprise:** the operational ledger, listener ownership, seeding,
   fair incremental refresh, diagnostic snapshots and scalar mirrors,
   Rust tests and the matching RediSearch dependency.

Validation must cover successful/error/zero samples, overflow followed by drop,
native flush events, persisted reopen, layout replacement, retention of last-good
values on errors, diagnostic fairness, pause/drain, fork and shutdown.
Representative performance qualification remains a separate matched comparison
on an idle machine: normal writes/reads with and without INFO, main-thread
property attribution, operational cache age and collector native contention.
Historical prototype timings do not establish this implementation's results.
