# Per-index GC settings (`FT.ALTER <index> GC SET`)

## Why

RAM-index GC is tuned by node-wide settings for the active GC policy, which is chosen at
load (`search-gc-policy`). THREAD, the default since
[#12199](https://github.com/RediSearch/RediSearch/pull/12199), reads
`search-thread-gc-run-interval`, `search-thread-gc-clean-threshold` and
`search-gc-clean-ratio-ppm`. FORK reads `search-fork-gc-run-interval` and
`search-fork-gc-clean-threshold`. Every index on the node gets the same values, but
indexes differ widely in size and update rate. A small index that is rewritten all day
needs frequent cleanup. A large, mostly static index gains nothing from frequent cycles.
Under FORK each cycle that cleans also forks and holds Redis's single fork slot, which
can delay BGSAVE and replica sync
([MOD-18645](https://redislabs.atlassian.net/browse/MOD-18645)). Under THREAD a cycle
forks nothing, but it still costs CPU and time on the GC pool.

Today users work around this by rebuilding a hot index and switching an alias to the
new copy just to drop its garbage
([MOD-19545](https://redislabs.atlassian.net/browse/MOD-19545)). The same request asks
for a threshold relative to index size. THREAD already has one, the clean ratio, and T5
lets an index override it under either policy. Tracked as epic
[MOD-19815](https://redislabs.atlassian.net/browse/MOD-19815).

## What Changes

A new `FT.ALTER` action sets or clears optional overrides on one index: a run interval
and a clean threshold, plus a clean ratio if Product signs off on it (decision 13,
[MOD-20691](https://redislabs.atlassian.net/browse/MOD-20691)):

```
FT.ALTER <index> GC SET [RUN_INTERVAL <seconds|DEFAULT>] [CLEAN_THRESHOLD <count|DEFAULT>]
                        [CLEAN_RATIO_PPM <ppm|DEFAULT>]
```

- At least one setting, each at most once, in any order. Setting one keeps the others.
- `SET` is required, so the shortest command, `FT.ALTER idx GC SET RUN_INTERVAL 10`, has
  six words, the same minimum as `FT.ALTER ... SCHEMA ADD`. `FT.ALTER` keeps its
  declared arity of `-6`.
- The overrides apply under both policies. An index without an override uses the active
  policy's global value and follows later `CONFIG SET` changes: the `search-thread-gc-*`
  settings under THREAD, the `search-fork-gc-*` ones under FORK. `DEFAULT` removes the
  override. Global values are never copied into an index.
- Under THREAD, a per-index `CLEAN_THRESHOLD` replaces only the absolute threshold. The
  clean ratio still applies on top, so on a large index an override alone can't lower
  the bar below the ratio.
- Ranges: `RUN_INTERVAL` 1 to `GC_MAX_INTERVAL_SEC` seconds (about 73,000 years), the
  same limit as `search-thread-gc-run-interval`; `CLEAN_THRESHOLD` 0 to 2^63−1.
  Threshold 0 means what it means globally today: under FORK a cycle forks every
  interval, even when nothing was deleted; under THREAD the ratio still decides.
- Pending Product sign-off (T5): a per-index clean ratio, `CLEAN_RATIO_PPM`, in parts per
  million of the live documents, 0 to 1,000,000. A scheduled cycle runs only when the
  index's deletes reach both the clean threshold and that share of its live documents,
  the rule THREAD uses today. An unset ratio inherits the active policy's:
  `search-gc-clean-ratio-ppm` under THREAD, and a new FORK global,
  `search-fork-gc-clean-ratio-ppm`, under FORK. The FORK global defaults to 0, which
  turns the ratio off, so FORK behaves as today unless it is set.
- The command is all-or-nothing, changes no schema, and triggers no reindex.
- A new run interval restarts the waiting timer from the command, only when the
  effective interval differs from the one that timer was armed with. A new threshold
  applies to runs queued after the change; a run already queued or running keeps the
  values it started with.
- Overrides are saved in the RDB, replicated, and written to the AOF. On a cluster,
  `FT.ALTER` on the coordinator is sent to every shard. If a shard rejects it, the
  other shards keep the change, the command returns an error, and running it again
  brings the shards back in line.
- Given an alias, the command changes the index the alias points to at that moment.
- `FT.INFO` gains a `gc_settings` section with the override and effective value of
  each setting. Later GC settings are added to it as new keys.
- `FT.ALTER ... GC` is rejected on TEMPORARY indexes, which have no GC, and on Flex
  nodes, as every `FT.ALTER` is there today, until task 6. It is accepted and stored on
  nodes started with `search-no-gc`, where it has no effect.
- The index encoding version changes to 32. Overrides are saved as a list of tagged
  entries, so later GC settings need no further version bump. Use the command only once
  every node is upgraded: an older binary can't load a version-32 RDB, and an older
  shard drops the overrides of an index it receives by slot migration (see `design.md`,
  Edge cases).
- The error for an unknown `FT.ALTER` action names both `SCHEMA` and `GC`.
- The scheduler rework (T1) also changes indexes without overrides: a forced run's fork
  result no longer moves the schedule onto or off the retry interval; a delay longer
  than one Redis module timer can wait (about 24.86 days) is armed in steps instead of
  making the timer loop spin or stop; and a `CONFIG SET` made between the GC thread's
  read of the interval and the timer re-arm takes effect at that arm instead of being
  lost.

### Later phase

Designed here (`design.md`, Phase 2) and shipped after tasks 0–5:

- Disk (Flex) indexes, task 6 ([MOD-21052](https://redislabs.atlassian.net/browse/MOD-21052)).
  `FT.ALTER <index> GC SET` works on Flex with the same syntax, ranges, inheritance and
  FT.INFO; `SCHEMA ADD` stays rejected there. Disk GC compacts in-process instead of
  forking, and its threshold counts writes as well as deletes and updates, as the global
  setting does on disk today.
- GC options in `FT.CREATE`, task 7
  ([MOD-21053](https://redislabs.atlassian.net/browse/MOD-21053)): the same `GC SET`
  clause before `SCHEMA`, for RAM indexes, and for Flex indexes once task 6 has shipped.
  Until task 7, running `FT.ALTER` right after `FT.CREATE` covers the rebuild and
  alias-swap pattern, because the new index has no deletes yet.

### Non-goals

- A per-index GC policy (`POLICY`). The policy is fixed at load for the whole node, and
  a doc table keeps the deleted-ID set the THREAD GC
  ([#12035](https://github.com/RediSearch/RediSearch/pull/12035)) needs only when the
  node runs THREAD. It can be added later as one more setting.
- Freeing a dropped index's GC state before its timer fires. Existing behavior,
  unchanged.
- Stopping or bounding a cycle that is already running
  ([MOD-19577](https://redislabs.atlassian.net/browse/MOD-19577)).
- Backports to lines older than 8.8, and backporting the THREAD policy or master's GC
  rewrite. The 8.12, 8.10 and 8.8 backports are FORK only, and save at version 32 on RAM
  nodes only (see `design.md`, Backports).
