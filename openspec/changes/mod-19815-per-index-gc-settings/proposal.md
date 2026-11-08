# Per-index fork GC settings (`FT.ALTER <index> GC SET`)

## Why

Fork GC is tuned by two node-wide settings, `search-fork-gc-run-interval` and
`search-fork-gc-clean-threshold`. Every index on the node gets the same values, but
indexes differ widely in size and update rate. A small index that is rewritten all day
needs frequent cleanup. A large, mostly static index gains nothing from frequent forks,
and each fork holds Redis's single fork slot, which can delay BGSAVE and replica sync
([MOD-18645](https://redislabs.atlassian.net/browse/MOD-18645)).

Today users work around this by rebuilding a hot index and switching an alias to the
new copy just to drop its garbage
([MOD-19545](https://redislabs.atlassian.net/browse/MOD-19545)). The same request asks
for a threshold relative to index size, so that one setting fits both small and large
indexes. Tracked as epic
[MOD-19815](https://redislabs.atlassian.net/browse/MOD-19815).

## What Changes

A new `FT.ALTER` action sets or clears optional overrides on one index: a run interval
and a clean threshold, plus a clean threshold percentage if Product signs off on it
(decision 13, [MOD-20691](https://redislabs.atlassian.net/browse/MOD-20691)):

```
FT.ALTER <index> GC SET [RUN_INTERVAL <seconds|DEFAULT>] [CLEAN_THRESHOLD <count|DEFAULT>]
                        [CLEAN_THRESHOLD_PERCENT <percent|DEFAULT>]
```

- At least one setting, each at most once, in any order. Setting one keeps the others.
- `SET` is required, so the shortest command, `FT.ALTER idx GC SET RUN_INTERVAL 10`, has
  six words, the same minimum as `FT.ALTER ... SCHEMA ADD`. `FT.ALTER` keeps its
  declared arity, so the Redis Software proxy, which checks it, needs no change.
- An index without an override uses the global value and follows later `CONFIG SET`
  changes. `DEFAULT` removes the override. Global values are never copied into an index.
- Ranges match the global settings: `RUN_INTERVAL` 1 to 2^63−1 seconds,
  `CLEAN_THRESHOLD` 0 to 2^63−1. Threshold 0 means what it means globally today: a cycle
  forks every interval, even when nothing was deleted.
- Pending Product sign-off (T5): a new global setting,
  `search-fork-gc-clean-threshold-percent`, takes a whole percentage from 0 to 100 and
  defaults to 0, which turns it off. A cycle forks when the
  index's deletes and updates since the last cycle reach either `CLEAN_THRESHOLD` or that
  percentage of the index's documents, whichever comes first. With the default, nothing
  changes.
- The settings apply to whichever GC policy collects the index's RAM garbage, so a later
  thread-based policy ([MOD-20523](https://redislabs.atlassian.net/browse/MOD-20523))
  honors them as well.
- The command is all-or-nothing, changes no schema, and triggers no reindex.
- A new run interval restarts the waiting timer from the command, only when the
  effective interval differs from the one that timer was armed with. A new threshold
  applies to runs queued after the change; a run already queued or running keeps the
  values it started with.
- Overrides are saved in the RDB, replicated, and written to the AOF. On a cluster,
  `FT.ALTER` on the coordinator applies to every shard.
- Given an alias, the command changes the index the alias points to at that moment.
- `FT.INFO` gains a `gc_settings` section with the configured and effective value of
  each setting. Later GC settings are added to it as new keys.
- `FT.ALTER ... GC` is rejected on TEMPORARY indexes, which have no GC. It is accepted
  and stored on nodes started with `search-no-gc`, where it has no effect.
- The index encoding version changes. Overrides are saved as a list of tagged entries,
  so later GC settings need no further version bump.
- The error for an unknown `FT.ALTER` action names both `SCHEMA` and `GC`. No other
  existing behavior changes.

### Non-goals

- Disk (Flex) indexes. `FT.ALTER` is not available on Flex today: shards and the
  coordinator reject it with "FT.ALTER is not supported in Redis Flex". Disk GC also has
  its own scheduling and threshold code, which needs its own changes. Without `FT.ALTER`
  on Flex, `FT.CREATE` options alone would reach only new indexes, which means a
  rebuild. Follow-up under the disk MS4 epic.
- GC options in `FT.CREATE`. Leaving them out keeps the API change small. Adding them
  later only needs parsing into the same stored fields, since the overrides are part of
  the index's saved record. For the rebuild and alias-swap pattern, running `FT.ALTER`
  right after `FT.CREATE` is enough, because the new index has no deletes yet.
- A per-index GC policy (`POLICY`). Postponed until the thread-based GC in MOD-20523
  decides whether a policy can change per index at runtime. It can be added later as one
  more setting.
- Freeing a dropped index's GC state before its timer fires. Existing behavior,
  unchanged.
- Stopping or bounding a cycle that is already running
  ([MOD-19577](https://redislabs.atlassian.net/browse/MOD-19577)).
- Backport below 8.12.
