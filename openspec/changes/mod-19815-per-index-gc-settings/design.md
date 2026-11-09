# Design: per-index fork GC settings

The full design, with the product decision table, is on Confluence:
[Per-index fork GC settings (FT.ALTER GC) — Design & Breakdown](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6926172171).
This file is the reviewable copy of the engineering part. Task numbers T1–T5 match the
Jira tasks under [MOD-19815](https://redislabs.atlassian.net/browse/MOD-19815). T5, the
percentage threshold ([MOD-20691](https://redislabs.atlassian.net/browse/MOD-20691)), is
pending Product sign-off (decision 13).

## Current behavior

- `IndexSpec_StartGC` creates a `GCContext` per index, unless GC is off (`search-no-gc`)
  or the index is TEMPORARY. Disk indexes get disk GC; RAM indexes get fork GC. Both use
  the same scheduler in `gc.c` and read the same two global settings.
- `GCContext` owns one timer. When it fires, the main thread queues a run on the GC
  thread pool and sets `GC_SCHED_RUN_PENDING`; with `AvoidReplicaTraffic` it re-arms
  instead. When the run ends, the GC thread copies `getInterval` into `pendingInterval`
  and posts a one-shot that re-arms on the main thread. `pendingInterval` is a copy so
  that a queued `GC_FORCEINVOKE` can't change it before the one-shot runs.
  `GC_SCHED_PAUSED` (disk consistency window) and `enabled` also block arming.
- `getNextPeriod` arms for `interval + rand() % interval` seconds, so the real period
  runs from the interval to almost twice the interval.
- `FGC_RunCycle` returns early, without forking, while the counted deletes and updates
  are below `forkGcCleanThreshold`. The check runs on the GC thread before the GIL is
  taken, and reads the global setting without synchronization. Forced runs skip it.
- For fork GC, `getInterval` returns `ForkGC.retryInterval`, which serves as both the
  run interval and the fork-retry interval. `FGC_Create` sets it to the run interval;
  after that only a cycle writes it, forced or scheduled: a successful fork sets the run
  interval, a failed fork sets `forkGcRetryInterval`, and a cycle skipped by the
  threshold leaves it unchanged.
- Dropping an index does not free its `GCContext` when a timer is armed. The next timer
  fire queues a run, which finds the index gone, and `taskCallback` frees the context.
  `GCContext_BeginDrop` decides whether the drop must queue that run itself
  (`GCContext_FinishDrop`), when no timer or run would discover it. This stays as it is
  (see Out of scope).

Existing defects that T1 fixes, because per-index settings make them worse:

1. A new global interval reaches an index only after its next fork. An index that stays
   below the threshold never picks it up, and after a failed fork it keeps polling at
   the retry interval until a fork succeeds.
2. `interval.tv_sec * 1000` overflows for values the config accepts (up to
   `LLONG_MAX`).
3. `taskCallback` copies the interval into `pendingInterval` on the GC thread before it
   posts the re-arm, so a change between that copy and the one-shot misses that arm.

## Data model (T2)

- `IndexSpec` gets two optional values, `gcRunIntervalSec` and `gcCleanThreshold`, each
  with an explicit unset state. T5 adds a third, `gcCleanThresholdPercent`. 0 is a valid
  threshold and percentage, so it can't mean unset.
- One helper per setting returns the override if set, otherwise the global value. Every
  read of the run interval used for scheduling, and of the clean threshold, goes through
  it. The other fork-GC settings stay global and are read as today: the retry interval,
  `forkGCCleanNumericEmptyNodes`, `forkGcSleepBeforeExit`, and the run interval that
  `reap_child_blocking` takes as its timeout (see Out of scope).
- The helpers reach the index through `ForkGC`'s weak reference. When the promote fails
  because the index was unlinked, the interval helper returns the global value: it never
  declines to arm. A drop that overtakes a finished run while its re-arm one-shot is
  still pending relies on the timer that one-shot arms to find the drop and free the
  context (`GCContext_BeginDrop`); not arming would leak the context and its logically
  deleted count.
- The settings are not specific to fork GC. They describe when an index's RAM garbage
  is collected, whatever the policy. The thread-based GC in
  [MOD-20523](https://redislabs.atlassian.net/browse/MOD-20523) shares the fork path's
  interval and threshold reads
  ([#12035](https://github.com/RediSearch/RediSearch/pull/12035)). Whichever of the two
  lands second covers these integration points:
  - Every read of the run interval and clean threshold on the thread path goes through
    the same helpers, including `runThreadCycle`, `hasEnoughWorkCb`, the threshold check
    that [#12036](https://github.com/RediSearch/RediSearch/pull/12036) moves, and the
    cycle started as soon as the threshold is reached
    ([MOD-20666](https://redislabs.atlassian.net/browse/MOD-20666)).
  - Thread-GC runs are queued by the same scheduler, so the thread policy sets the
    queue-time copy callback too. Any other GC-thread read of a setting needs its own
    synchronization.
  - After `FT.ALTER` lowers a threshold under the thread policy, T3 calls
    `GCContext_RunIfWaitedFor` ([#12072](https://github.com/RediSearch/RediSearch/pull/12072)),
    or the run waits for the next delete or tick. `GCContext_OnIntervalChanged` keeps
    `GC_SCHED_WAIT_FOR_WORK` and updates any deadline the thread stack stores when a
    timer is armed.
- Disk GC keeps reading the globals it reads today and ignores the new percentage
  setting. `FT.ALTER` is disabled on disk nodes (`DiskDisabledCmd`, on shards and the
  coordinator), so its indexes never have overrides. Disk GC has its own scheduling and
  threshold code in `disk_gc.c`: it keeps its own `intervalSec`, refreshed only after a
  run (defect 1 above), and its threshold counts writes as well as deletes and updates.
  Per-index settings for disk are task 6 (see Phase 2).
- Overrides are stored whether or not the node has a `GCContext`.
- No atomics, and no index lock for the overrides. They are written only on the main
  thread, under the GIL. The interval is read only on the main thread, when T1 computes
  the next arm. The threshold reaches the GC thread only through the queue-time copy
  (see Threshold check).

## Persistence (T2)

- `INDEX_CURRENT_VERSION` goes from 29 to 30. The override block is appended at the end
  of the spec record in `IndexSpec_RdbSave`, after the conditional disk block, and is
  written for every index. The load side reads it at the same position, after its own
  disk-block condition.
- The block is a count followed by one `(setting id, value)` pair of unsigned 64-bit
  numbers per override that is set. Unset settings are not written, so an index without
  overrides costs one number. Setting ids are fixed and never reused.
- A loader skips ids it doesn't know and logs a notice once per load. That lets a later
  setting, such as a per-index `POLICY`, ship without another version bump, and lets a
  binary that predates it load the RDB with that setting ignored. Ignoring is safe
  because every GC setting only tunes when garbage is collected, never what a query
  returns. The cost: if that binary saves the RDB, the unknown setting is dropped. A
  known id with an out-of-range value fails the load as corrupt.
- Encodings below 30 load with every override unset.
- Specs are module aux data, and Redis does not reject an aux encoding version above the
  module's own. Old binaries have no upper-bound check, so a version-30 RDB on an old
  binary is expected to fail to load rather than misread. T2 confirms what an older
  binary actually does.
- Slot migration needs nothing new. Overrides reach every shard through the
  coordinator's fan-out, and `_FT._RESTOREIFNX` keeps the target's own copy of an index
  that already exists. Only an index the target doesn't have yet is created from the
  serialized spec, overrides included. A version-30 spec sent to an older target is a
  mixed-version cluster, which is not supported for this command (see Edge cases).

## Scheduler (T1)

- Replace the dual use of `ForkGC.retryInterval` with an explicit "last fork failed"
  state. Only scheduled cycles change it; a forced run's fork result leaves it as it
  was. A successful fork clears it, and so does a scheduled cycle that skips the fork
  because the count is below the threshold: there is nothing left to retry, so the next
  arm uses the run interval. Without that, raising the threshold after a failed fork
  would keep the index on the retry interval until enough changes accumulate. The
  design leaves room for a third state, "yielded, continue soon", needed by
  [MOD-19577](https://redislabs.atlassian.net/browse/MOD-19577).
- Compute the delay on the main thread when arming, from the effective interval and the
  scheduling state, instead of copying it on the GC thread. This fixes defects 1 and 3
  and removes `pendingInterval`.
- `getNextPeriod` keeps today's jitter range, from the interval to almost twice the
  interval, drawn on each arm.
- Delays saturate instead of overflowing, and are capped at `INT_MAX` ms (about 24.86
  days). Redis serves every module timer from one event, `moduleTimerHandler`, which
  returns the time to the earliest module timer as an `int`; a longer delay truncates
  and makes the timer loop spin or stop
  ([MOD-21060](https://redislabs.atlassian.net/browse/MOD-21060)). The cap also stays
  clear of the microsecond overflow in `RedisModule_CreateTimer`. An interval of 24.86
  days or more therefore runs about every 24.86 days, with no jitter. This fixes
  defect 2.
- `GCContext` records the interval each run-interval timer was armed with, before
  jitter. It keeps no arm time and no jitter.
- A main-thread entry point, `GCContext_OnIntervalChanged`, acts only on a waiting
  run-interval timer, and only when the effective interval differs from the one that
  timer was armed with: `GCContext_DisarmTimer`, then `GCContext_ArmTimer`, so the timer
  restarts from the command with the new interval and new jitter. It never queues a
  run. It does nothing when there is no waiting run-interval timer: a run queued or
  running, GC stopped, scheduling paused, or the waiting timer is a fork retry. The next
  run-interval arm then uses the latest effective interval.
- The comparison with the armed interval is required, not an optimization. It keeps
  repeated commands from postponing GC. It also covers a `CONFIG SET` that changed an
  inheriting index's effective interval after its timer was armed: with a timer armed
  for 1000 s, `CONFIG SET search-fork-gc-run-interval 10` followed by
  `FT.ALTER idx GC SET RUN_INTERVAL 10` restarts the timer, although the effective value
  is 10 both before and after the command.
- Global `CONFIG SET` keeps today's behavior: it changes no waiting timer, and inheriting
  indexes use the new value at their next arm.
- Disk GC shares this code, so it gets the same arming, jitter and cap with the global
  interval. Its cycle logic is unchanged, including where it refreshes its own
  `intervalSec`, so defect 1 stays there until task 6 (Phase 2). The existing disk GC
  tests must keep passing.

## Threshold check (T2)

- A change applies to runs queued after it. A run is queued when its timer fires. A run
  that is already queued or running, forked or not, keeps the values it started with.
- Inside `GCContext_QueueRun`, after it claims `GC_SCHED_RUN_PENDING` and before it
  hands the run to the thread pool, a new GC callback copies the effective threshold
  into a plain `ForkGC` field. Disk GC leaves the callback unset. The thread-pool
  hand-off, which takes a mutex, makes the copy visible to the GC thread, and
  `FGC_RunCycle`'s check reads only that copy. This also removes today's unsynchronized
  read of the global threshold on the GC thread.
- The copy must come after the claim. `QueueRun` returns early when a run already holds
  the context, for example `GC_CONTINUE_SCHEDULE` (`GCContext_StartNow`) while a run
  started before `GC_STOP_SCHEDULE` is still going, and that run's GC thread may be
  reading the field.
- `QueueRun` is also how a drop is discovered: a timer that fires after the drop, and
  `GCContext_FinishDrop` after the unlink, both reach it. The callback promotes the
  index's weak reference. If the index is gone, it leaves the copy as it is, and the run
  finds the drop as today.
- Cost: raising the threshold while a run is queued can let that run fork once more, and
  lowering it applies one run later. The window runs from queueing to the fork, normally
  milliseconds.
- Forced runs skip the check, as today.
- The comparison is unchanged (skip while the count is below the threshold), so 0 keeps
  its meaning. After T5 the copy holds the combined threshold below.

## Percentage threshold (T5)

- New global module config `search-fork-gc-clean-threshold-percent`, a whole number from
  0 to 100, default 0, changeable at runtime. It has no `FT.CONFIG` alias. Module numeric
  configs are integers, and whole percents are enough: the percentage only wins when it
  is below `CLEAN_THRESHOLD`, which is the small-index case.
- With `p` the effective percentage and `N` the effective `CLEAN_THRESHOLD`, a scheduled
  cycle forks once the index's counted deletes and updates reach the combined
  threshold: `N` when `p = 0`, as today, and otherwise
  `min(N, max(1, ceil(p × base / 100)))`. The `p = 0` case is checked first; the
  formula alone would give 1.
- Both counts are the index's own. The numerator is the existing counter of deletes and
  updates since the last cycle. Each update gives the key a new document ID and retires
  the old one, so the counter approximates garbage document IDs. It does not count
  garbage index entries.
- `base` is the largest live-document count seen since the counter was last reset.
  Live documents now would overstate deletes: 50 deletes in a 100-document index leave
  50 live, which would read as 100%. The largest count keeps deletes at 50% and updates
  at their share, and an index that grew uses its new size even if it shrank again:
  100 documents, then 100 added and 100 deleted, reads as 50% of 200.
- The largest count is sampled, not tracked on every document add. `ForkGC` keeps a
  peak value. Each queue-time copy raises it to the live count at that moment, then
  uses it as `base`. The run that resets the counter sets the peak to the live count at
  the reset plus the slot-cleanup deletes still deferred at that moment
  (`gcDeferredDeletes`, read without taking it). It reads both from the index it already
  references, under the spec read lock taken inside the GIL it already holds there. A
  queue-time sample taken before deletes the fork collects therefore never carries into
  the new window.
- The deferred deletes are added back because the reset is not one cutoff for every
  delete. A main-thread delete holds the GIL, so it lands wholly before or after the
  reset. Slot cleanup deletes without the GIL, and a delete after the run took the
  deferred count is counted in the next window although the live count already
  excludes it. Without the add-back, 100 live documents with 40 deleted by slot cleanup
  before the reset would give a base of 60 and a count of 40, so a 60% threshold would
  fork at 40% of the population. With it, the base is 100. The pair is consistent only
  if slot cleanup reports its deletes inside the write lock it deletes under, so T5
  moves `IndexSpec_DeferGCDeletes` inside that lock in `SlotCleanup_ApplyDeletes`.
- Forced runs reset the counter at the same point, so they set the peak the
  same way and need no queue-time sample. A fork that fails resets neither. The peak
  starts at the live count when the GC context is created. Every timer fire queues a
  run, including cycles the threshold then skips, so the peak is sampled once per
  interval.
- Limitation: growth and deletes between two samples aren't seen. 100 documents, then
  100 added and 100 deleted within one interval, still reads as 100%, so the
  percentage can fork early. It can't fork late, and a fork resets the window.
- The floor of 1 keeps an empty index from forking every interval: without it,
  `ceil(p × 0 / 100)` is 0 and the check always passes. `CLEAN_THRESHOLD 0` still forks
  every interval, because the minimum is then 0.
- The math saturates instead of overflowing: `p × base` is computed in 128 bits, or
  `base` is divided first when it is large.
- The combined threshold is computed on the main thread as part of the queue-time copy.
  The live-document count is `sp->stats.scoring.numDocuments` at that moment, read under
  the spec read lock, as FT.INFO reads it. The GIL alone is not enough: slot cleanup
  deletes documents on a background thread under the spec write lock, without the GIL.

## Command (T3)

- `AlterIndexInternalCommand` branches on `GC` before the `SCHEMA`-specific code,
  including the `_FT._ALTERIFNX` field probe. The word after `GC` must be `SET`; any
  other word gets an error that names `SET`.
- `FT.ALTER` keeps its declared arity of `-6`. `SET` makes the shortest command,
  `FT.ALTER idx GC SET RUN_INTERVAL 10`, six words long, so a shorter form gets the arity
  error from Redis before the handler runs. The handler's own `argc < 5` check stays.
  The Redis Software proxy checks the same arity and hard-codes `-6` for `FT.ALTER`.
  Proxies already deployed can't change, and the module runs on them, so the minimum
  can't go below six words.
- The index name resolves aliases, as every `FT.ALTER` does today. `FT.ALTER <alias> GC`
  changes the index the alias points to at that moment.
- `SKIPINITIALSCAN` followed by `GC` is an error. `SKIPINITIALSCAN` only applies to
  `SCHEMA ADD`, where it skips indexing existing documents into the new field. `GC` adds
  no field and starts no scan, so the combination is rejected rather than silently
  ignored.
- Parse and validate every setting before applying any. Reject duplicates, unknown
  words, out-of-range values and a missing value. `CLEAN_THRESHOLD_PERCENT` (T5) takes a
  whole number from 0 to 100; T3 leaves room for it in the parser and T5 adds it.
- TEMPORARY index: reject. Any other index, with or without a `GCContext`: store, and
  call `GCContext_OnIntervalChanged` when the command sets or clears `RUN_INTERVAL`. It
  restarts the timer only when the effective interval differs from the armed one (see
  Scheduler). The next queued run picks up the threshold.
- Replicate with the existing `_FT._ALTERIFNX` call. Applying the same command twice
  leaves the same state.
- The error for an unknown action becomes one that names both `SCHEMA` and `GC`. No
  pytest matches on the old text.
- `commands.json`: `FT.ALTER` gets a `oneof` between a `SCHEMA` branch
  (`[SKIPINITIALSCAN] SCHEMA ADD ...`) and a `GC SET` branch, so the published syntax
  doesn't allow `SKIPINITIALSCAN GC`. The generator takes the shortest branch, which
  with optional settings is below six words, so `FT.ALTER` is pinned at `-6` in
  `COMMAND_ARITY_OVERRIDES` in `srcutil/gen_command_info.py`. The summary, today "Adds a new field to the
  index", covers both actions, and the complexity says `O(N)` applies to `SCHEMA ADD`
  and `GC` is `O(1)`. `src/command_info/command_info.c` is regenerated with
  `srcutil/gen_command_info.py`, and `COMMAND DOCS` is checked.
- An `FT.DEBUG` subcommand reports the time left on an index's GC timer and whether it
  is a run-interval or retry timer, so flow tests can check timing without sleeping
  for the jitter. T5 adds the combined threshold and its `base` to the same reply.
- Command flags stay `write deny-oom`, as agreed in engineering, and the docs say so.
  Command flags apply to all of `FT.ALTER`, so dropping it for `GC` would also let
  `SCHEMA ADD` run under memory pressure. In a cluster, shards that are out of memory
  reject the command while the others apply it, with no rollback, as with any
  `FT.ALTER` today. Re-running fixes it.
- The command stays `DiskDisabledCmd`, so Flex nodes reject it as they reject every
  `FT.ALTER` today, until task 6 (Phase 2).

## FT.INFO (T4)

- Required, not optional: an override doesn't follow later `CONFIG SET` changes, so
  users need to see which value each index uses. Only the field names are still open.
- New top-level `gc_settings` map with `run_interval` and `clean_threshold`, each a map
  of `configured` and `effective`. T5 adds `clean_threshold_percent` in the same shape.
  Values are integers. `configured` is null when inherited: RESP3 null, RESP2 nil bulk
  string. A later setting, such as `policy`, is one more key, so clients that read the
  map by key are unaffected.
- Present for every index except TEMPORARY indexes, whether or not the node runs GC.
  FT.INFO of a TEMPORARY index is unchanged.
- It can't go in `index_options` (a flat list of flags) or `gc_stats` (missing without a
  `GCContext`, and summed by the coordinator).
- The coordinator drops top-level keys it doesn't know, so T4 adds `gc_settings` to its
  merge. It copies the first shard's section, and tolerates shard replies without one
  (older shards during an upgrade). `configured` is the same on every shard
  once a fan-out has succeeded everywhere. `effective` can differ if the global settings
  differ between shards, since `CONFIG SET` is per node. A shard that rejected the
  command (partial upgrade, out of memory) shows its own values only in its own
  FT.INFO, not through the coordinator. The docs say so.

## Edge cases

- **`search-no-gc` node:** stores the override and reports it in FT.INFO; it has no
  effect there. Rejecting would make nodes diverge: a replica without GC would fail the
  replicated command, and the override would be lost when a GC node later full-syncs
  from it.
- **Drop and recreate:** overrides belong to the index and go with it. The rebuild and
  alias-swap pattern must re-run `FT.ALTER ... GC` on the new index, by name or through
  the alias after the swap.
- **Mixed versions:** upgrade replicas, fail over, then upgrade old masters. Use the
  command only after every node is upgraded. Old nodes reject `GC` with an error and
  don't crash: a build without this change expects an optional `SKIPINITIALSCAN` and
  then `SCHEMA`, so `GC` gets "ALTER must be followed by SCHEMA". An old replica fails
  the replicated command and follows `propagation-error-behavior`: under the default,
  `ignore`, it logs the error and lacks the override; under `panic` the replica stops. In
  a partly upgraded cluster the coordinator returns an error, and upgraded shards keep the
  change. Until the upgrade completes, only GC timing differs between shards; queries are
  unaffected. Re-running after the upgrade makes all shards agree. Known gap: the
  coordinator's FT.INFO shows the first shard's values, so a mismatch shows only in each
  shard's own FT.INFO.
- **Very large values:** capped; they mean "practically never".
- **Percentage on a tiny index:** at 10% a 5-document index forks after 1 change. That
  is the intent, and the floor of 1 keeps an empty index quiet.

## Out of scope

Existing behavior that per-index settings do not depend on, left as it is:

- A dropped index keeps its `GCContext` until its timer fires.
- `reap_child_blocking` truncates its timeout to `int`. Its call keeps passing the global
  run interval, not the per-index helper: it is a timeout for reaping a child, not a
  scheduling read, and changing it would change kill-failure handling, which this
  design leaves alone.
- No upper-bound check on the index encoding version at load. It would only protect a
  future downgrade.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Ship behind `ENABLE_UNSTABLE_FEATURES` | The gate forbids persisted or replicated state (`docs/CONTRIBUTING-unstable-features.md`, Requirement 3). |
| Keep overrides in memory only | Lost on restart, failover and full sync. |
| A module config listing overrides by index name | Keyed by name, so it breaks the rebuild and alias-swap pattern. Depends on the control plane to apply it. |
| Reject the command where GC is off | Nodes diverge, as above. |
| Abort a running cycle when the threshold is raised | Throws away work and adds exits to the riskiest GC code. Belongs to MOD-19577. |
| Re-check the threshold just before the fork | Costs a second check under the GIL, its own exit path and a sync-point test, for a window of milliseconds. |
| Protect the values with the index lock | `FT.ALTER` would wait behind queries, and GC would wait for the lock while holding the GIL, stalling Redis. |
| Compare the effective interval before and after the command | Misses a `CONFIG SET` that changed an inheriting index's effective interval after its timer was armed. The timer keeps the old interval, or a repeat of the old value restarts it. |
| Keep the original arm time and jitter, run at once if the new deadline passed | Needs stored state per index, deadline math, and a run-now path that repeats `timerCallback`. The restart is simpler, and repeats still can't postpone GC. Cost: a lowered long interval runs one to two new intervals after the command instead of at once. |
| Leave the waiting timer alone | A lowered long interval first waits out the old one. |
| Also restart timers on `CONFIG SET` | Changes existing behavior and walks every index. Not needed. |
| `GC` with no verb (`FT.ALTER idx GC RUN_INTERVAL 10`) | Five words, below `FT.ALTER`'s arity of `-6`. The Redis Software proxy enforces that arity and hard-codes it, and proxies already deployed can't change. |
| `SET` and `RESET` verbs instead of `DEFAULT` | `GC RESET RUN_INTERVAL` is five words, so it hits the same limit. `DEFAULT` also lets one command set one setting and clear another. |
| A fixed RDB layout per setting | Every new GC setting would need another version bump. |
| GC options in `FT.CREATE` in phase 1 | Not needed to keep the first API change small. Task 7 adds them as parsing into the same stored fields (Phase 2). Until then, `FT.ALTER` right after `FT.CREATE` covers rebuild and alias swap, since the new index has no deletes yet. |
| A per-index `POLICY` now (`FORK`, `NONE`) | Commits to a policy that changes per index at runtime before MOD-20523 decides that; the thread GC creates its deleted-ID set only when the whole node runs THREAD. `NONE` also stops tiered vector GC, which runs inside the cycle. Postponed; it can be added later as one more setting with no version bump. |
| Percentage only when both thresholds are met (AND) | Only helps large indexes fork less, which the per-index `CLEAN_THRESHOLD` already does. OR lets one global setting fit small and large indexes. |
| Percentage of live documents only | Overstates deletes: 50 deletes in 100 documents would read as 100%. |
| `base` as the larger of the live count now and at the last reset | Loses growth that is deleted again: 100 documents, then 100 added and 100 deleted, reads as 100% instead of 50%. |
| Track the exact peak on every document add | Adds work to every document add. Sampling at each reset and once per interval only misses changes within one interval. |
| Percentage of `num_records` | Counts inverted-index entries, live and dead, not garbage documents. |
| Fractional percentages | Module numeric configs are integers, and the percentage only matters below `CLEAN_THRESHOLD`, where whole percents are fine. |

## Open questions (recommendation first)

The spec delta describes the recommended answer to each.

1. **FT.INFO field names.** The `gc_settings` section is agreed and required (T4); only
   its field names are open. Recommended: the names above, with no per-node GC flag.
   Whether GC runs on a node is already visible through `CONFIG GET search-no-gc`.
2. **Schema fingerprint.** Leave the overrides out of `schemaFingerprint`. They are
   operational tuning, not schema. Fingerprints are compared only between nodes on the
   same encoding version, so mixed versions are not the concern. Cost:
   `FT._LIST WITHCLUSTERSTATE` won't flag a shard that missed the change in a partial
   fan-out. The comment above `IndexSpec_RdbSave` asks for schema-defining members to
   be mirrored in the fingerprint; a line next to the new block says it is not one.
3. **Enterprise.** The proxy needs no change: it already sends `FT.ALTER` to every
   shard, and the six-word minimum passes its arity check (see Command). To confirm
   with RS/DMC: Active-Active needs nothing beyond what `FT.ALTER ... SCHEMA ADD` gets.

## Testing strategy

- C++ unit tests: saturation and the cap, timer restart when the effective interval
  differs from the armed one and no restart when it doesn't, the restart as a no-op in
  every no-timer state and on a retry timer, a forced run leaving the scheduling state
  alone, a scheduled cycle skipped by the threshold after a failed fork arming with the
  run interval, the queue-time threshold copy (including a dropped index with an armed
  timer, `GCContext_FinishDrop`, and `QueueRun` returning early while a run holds the
  context), the interval helper returning the global value for an unlinked index, RDB
  round trip at version 30 and load of version 29, a block with an
  unknown setting id loading with that setting skipped, and a known id with an
  out-of-range value (`RUN_INTERVAL` 0, a value above 2^63−1) failing the load.
- C++ unit tests for T5: the combined threshold for updates, deletes, growth, growth
  then deletes across samples, deletes before the first fork, slot-cleanup deletes
  between taking the deferred count and the reset, an empty index, `p = 0`
  with `N` above 1, `CLEAN_THRESHOLD 0`, and values near the overflow limit.
- Flow tests for T5: the two-index example in the spec delta, inheritance of the global
  percentage, `DEFAULT`, range errors, FT.INFO in RESP2 and RESP3.
- Flow tests for T1: an idle index below the threshold picks up a new global interval
  within one old period; a change in the capture-to-drain window
  (`SYNC_POINT_GC_BEFORE_REARM_POST`) takes effect on the next arm; fork-failure retry
  still uses `search-fork-gc-retry-interval`; the existing `test_gc.py` scheduling tests
  pass unchanged.
- Flow tests, standalone and cluster: two indexes making progress with different
  settings, syntax, arity and validation errors, all-or-nothing, aliases, ACL, unknown
  index, inheritance and `DEFAULT`, `CONFIG SET` leaving timers alone, threshold 0, timer
  restart timing and the `DEFAULT` cases through the `FT.DEBUG` hook, repeated commands
  leaving the timer alone, `CONFIG SET` followed by `FT.ALTER` with the new value
  restarting the timer, updates while GC is queued, running, retrying or stopped,
  TEMPORARY rejection,
  `search-no-gc` nodes as master and as replica, FT.INFO in RESP2 and RESP3.
- Queued run keeps its threshold (T2): with `ENABLE_ASSERT`, park a run at
  `SYNC_POINT_GC_TASK_START`, change the threshold, and check that the parked run uses
  the old value and the next queued run uses the new one.
- Recovery flow tests: `DEBUG RELOAD`, full sync, failover, AOF and `BGREWRITEAOF`, slot
  migration of an index the target doesn't have yet.
- Disk GC: the existing tests pass unchanged after T1.

## Phase 2: Flex and FT.CREATE (tasks 6 and 7)

Designed with the rest so nothing is missed, and shipped after tasks 0–5. A per-index
`POLICY` (after MOD-20523) and the self-service guardrail on the Confluence page stay
unscheduled.

### Flex: `FT.ALTER ... GC` on disk indexes (task 6)

- Users see the same syntax, ranges, inheritance, `DEFAULT` and FT.INFO as on RAM
  indexes. `SCHEMA ADD` stays rejected on Flex.
- Disk GC compacts in-process on the shard's single GC thread instead of forking, so
  the fork-slot warning doesn't apply; an index that compacts often delays GC for the
  other indexes on the shard. Its threshold counts writes, deletes and updates, as the
  global setting does on disk today, and threshold 0 compacts every interval even when
  nothing changed.
- Command: remove `DiskDisabledCmd` from `FT.ALTER`, `_FT._ALTERIFNX` and the
  coordinator's `FT.ALTER`. On Flex, the handler rejects every action except `GC`, and
  the coordinator rejects before fan-out, with "FT.ALTER SCHEMA ADD is not supported in
  Redis Flex".
- Interval: `disk_gc.c` drops its cached `intervalSec`, and `getInterval` returns the
  effective-value helper, read on the main thread when the timer is armed (T1). This
  fixes defect 1 for disk.
- Threshold: disk GC sets the queue-time copy callback, and `periodicCb` checks only
  that copy, which removes its unsynchronized read of the global on the GC thread.
- `GCContext_OnIntervalChanged` already does nothing while `GC_SCHED_PAUSED`; resuming
  arms with the effective interval.
- Persistence: none. The override block is written for every index after the disk
  block, so Flex indexes already carry it. Tests cover loading with and without
  `REDISMODULE_CTX_FLAGS_SST_RDB`.
- Percentage (if T5 ships): the same formula, with disk's change count (writes
  included) as the numerator and a `DiskGC` peak of the document count, read under the
  spec read lock. Open for the disk team; if a share of changes isn't a useful
  compaction trigger, disk ignores the percentage.
- Tests: the existing disk GC tests, unit tests for the disk callbacks, and Flex flow
  tests for the command and the `SCHEMA ADD` rejection on shards and the coordinator,
  inheritance, timer restart, threshold counting, FT.INFO, RDB round trip and
  replication.

### GC options in `FT.CREATE` (task 7)

- `FT.CREATE <index> ... [GC SET <settings>] SCHEMA ...`, parsed by the same code as
  `FT.ALTER ... GC SET` into the same stored fields. `DEFAULT` is rejected at create
  time, since there is nothing to clear.
- RDB, replication and FT.INFO are unchanged. It works on RAM and Flex indexes.
- `commands.json` gains the clause; regenerate `src/command_info/command_info.c`.
