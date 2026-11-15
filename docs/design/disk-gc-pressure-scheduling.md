# Disk GC pressure scheduling

The opt-in scheduler checks pressure on the existing GC worker. Every admitted
cycle still performs the backend's full cleanup, including TEXT, TAG,
INDEXMISSING, NUMERIC/GEO and slot postings. Eligibility is not a proof that
deleted IDs may be retired. Compaction, collector application, marker retirement,
forced runs and consistency-window gates retain their existing contracts.

| Configuration | Default | Meaning |
| --- | ---: | --- |
| `search-disk-gc-check-interval` | 0 | Seconds between pressure checks, before the generic scheduler's jitter. Zero selects the existing count-gated periodic policy. |
| `search-disk-gc-min-interval` | 30 | Minimum seconds after a completed attempt before another automatic compaction. |
| `search-disk-gc-l0-file-threshold` | 4 | Run when any GC-owned CF has this many L0 files. Zero disables this signal. |
| `search-fork-gc-clean-threshold` | Existing disk default | Pending deleted IDs needed for an early cleanup. In legacy mode, retains its existing writes + updates + deletes meaning. |
| `search-fork-gc-run-interval` | Existing disk default | With pressure checks enabled, maximum age of observed pending work before cleanup becomes eligible. |

New options are available through `CONFIG GET` / `CONFIG SET`. The deprecated
`FT.CONFIG` command does not acquire new aliases. Runtime changes apply when the
existing timer next fires; they do not cancel an already-armed timer.

## Admission

On the GC worker, after the existing teardown and pause checks:

1. Observe pending notifications and outstanding deleted IDs, including restored
   IDs and deferred deletes. Record the first observation with a monotonic clock.
2. Wait at least the configured cooldown **and the previous attempt's duration**
   after that attempt ends. Expensive cycles therefore cannot immediately repeat.
3. Run when deleted-ID count reaches the threshold, any GC-owned CF reaches the
   L0 threshold, or pending work reaches the age limit. An idle index does not run
   merely because time passes. L0 pressure is checked even without notifications,
   which covers an index reopened with existing SSTs.
4. Reset notification counters using the existing capture/subtract protocol.
   Concurrent writes remain pending. A retry restores captured counters, keeps
   the age of pending work and sets an explicit retry flag, even for an L0-only
   cycle with no notifications. That flag survives disabling pressure checks.
   A successful run clears it and starts a new observation window.

Forced requests bypass admission and cooldown but retain teardown/pause gates.
The worker reads CF metadata only after releasing schema-map locks. No new
main-thread engine calls, timers, threads, persisted state or per-document state
are added. Memory overhead is one fixed-size policy state per index.

The age limit is an eligibility deadline, not a reclamation SLA: queueing,
pauses, retry, cooldown and execution can delay completion. Timer jitter adds up
to twice the check interval to each observation opportunity. Writes made during
a cycle are first observed by the following check.

## Initial choices and validation

The starting experiment uses a 5-second check interval, a 30-second cooldown and
a four-file threshold. These are **candidate values, not benchmark conclusions**.
L0 counts must be read per CF: summing one file across many fields would compact
an index with no individual CF pressure. The threshold includes missing and slot
postings and excludes independently compacted doc-table/vector storage.

The periodic write fallback remains necessary for postings consolidation and
numeric maintenance even when there are no deletes and few SSTs. A small number
of writes or deletes can no longer remain pending indefinitely. This may perform
more full compactions on some insert-only workloads; fewer cycles are not claimed.

Reclaimed bytes alone are not a useful backoff signal: consolidation and numeric
splits can be valuable without reclaiming bytes. The cooldown uses elapsed work
instead. L0 bytes, deletion ratios and selective-CF cleanup are deferred until
measurements justify their complexity.

Rollout remains off by default until comparison with the legacy policy covers
query correctness during compaction, query p50/p99, write throughput, cycle count
and duration, deletion retirement delay, process I/O and L0 backlog. Tests and
benchmarks must exercise both field groups and INDEXMISSING without forcing GC
before the measurement interval. The existing live-snapshot cleanup limitation
must be resolved before enabling more frequent cleanup in production.
