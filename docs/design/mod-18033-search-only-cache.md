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

Seven added callbacks connect the repositories: `getCollector`, `collect`,
`setAvailable`, `activateTarget`, `waitFreshUsage`,
`getCachedTotalDiskUsage` and `readCachedIndexMetrics`. The last returns one
numeric record containing memory, operational disk usage and block estimates,
and stages the component snapshot for the existing INFO output callback.

Index retirement and INFO-map cleanup run
through the existing main-thread close callback, which receives the owning disk
context. No separate retirement API or freshness-ticket ownership crosses FFI.

## Executor and scheduling

RediSearch owns one dedicated single-worker pool using its existing `deps/thpool`
implementation. It does not share the query or GC queue. A 50 ms module timer
submits work; an internal freshness request can submit directly without needing
the main event loop. There is at most one outstanding collection job. A running
job may replace itself with one continuation.

RSE supplies one private collection callback/context. Each operational batch
checks its approximately 5 ms budget between native reads. A single property
read can exceed that budget. Indexes rotate fairly, one CF at a time. Each
index's next ordinary pass is due one second after the previous pass started.
An overrun therefore makes the next pass immediately eligible. Flush/compaction
completion advances an index's dirty generation, making it eligible before its
normal due time. Events coalesce and never enqueue a job per write.

Operational batches run first. Diagnostic snapshots run when no operational
continuation is needed; their existing slower refresh cadence remains. Both
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

Disk consistency windows pause and drain the collector, and resume it only when
all pause reasons are released. Shutdown disables freshness waits, stops the
timer, drains/joins the pool and waits for active internal API callers before
closing DiskContext. Generic `pthread_atfork` hooks prevent new work and drain
one current batch before fork. The parent resumes; the child submits/joins no
worker and reads atomic scalar mirrors rather than inherited Rust locks.

## Internal freshness API

No new client command or blocked-client behavior is introduced. The private
Search disk API offers one blocking call for one index incarnation or the visible
total. Rust captures and owns the ticket, invokes the supplied worker-wake
callback after capture, waits with a deadline and drops the ticket on every
return path. C leases collector lifetime across that call. Neither side performs
a native collection on the waiting thread.

`max_age=0` requires a sample whose native read started after the request. An
already-fresh request may return immediately. Every captured CF must have a
successful sample meeting the cutoff and captured dirty generation; a recent
aggregate publication alone is insufficient. Later events do not extend the
captured requirement indefinitely. CF-layout or visible-scope changes return
`ScopeChanged`; pause/shutdown/child return `Unavailable`; expiry returns
`TimedOut`. Errors preserve last-good values and cannot satisfy freshness.

Callers must not hold a spec/lifecycle lock or be the collection worker. This is
for controlled internal/test/background paths. Normal Redis commands never call
it. A main-thread internal call is technically supported because submission
requires no timer, but it deliberately stalls that thread until completion or
deadline and must not be added to ordinary request paths. The caller keeps an
index alive while creating its ticket; outstanding waits are leased across
module shutdown.

## Coordinated PRs and qualification

1. **RediSearch:** the pool, pause/fork/shutdown gates, V1 callback, cached INFO
   reads, visibility hooks, private FFI contract and C/C++ tests.
2. **RediSearchEnterprise:** the operational ledger, listener ownership, seeding,
   fair incremental refresh, diagnostic snapshots and scalar mirrors, internal
   freshness tickets, Rust tests and the matching RediSearch dependency.

Validation must cover successful/error/zero samples, overflow followed by drop,
native flush events, persisted reopen, internal wait deadlines,
all-CF freshness, worker wake without a timer, pause/drain, fork and shutdown.
Representative performance qualification remains a separate matched comparison
on an idle machine: normal writes/reads with and without INFO, main-thread
property attribution, operational cache age and collector native contention.
Historical prototype timings do not establish this implementation's results.
