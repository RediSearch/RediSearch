# Design: per-index GC settings

The full design, with the product decision table, is on Confluence:
[Per-index fork GC settings (FT.ALTER GC) — Design & Breakdown](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6926172171).
This file is the reviewable copy of the engineering part. Task numbers T1–T5 match the
Jira tasks under [MOD-19815](https://redislabs.atlassian.net/browse/MOD-19815). T5, the
per-index clean ratio ([MOD-20691](https://redislabs.atlassian.net/browse/MOD-20691)), is
pending Product sign-off (decision 13).

## Current behavior

- `IndexSpec_StartGC` creates a `GCContext` per index, unless GC is off (`search-no-gc`)
  or the index is TEMPORARY. Disk indexes get disk GC (`disk_gc.c`). RAM indexes get
  `IndexGC` (`index_gc.c`), which runs one of two policies, fixed at load by
  `search-gc-policy`. THREAD, the default since
  [#12199](https://github.com/RediSearch/RediSearch/pull/12199)
  ([MOD-20668](https://redislabs.atlassian.net/browse/MOD-20668)), cleans on the GC
  pool without forking (`ThreadCycle_Run`, added in
  [#12035](https://github.com/RediSearch/RediSearch/pull/12035),
  [MOD-20665](https://redislabs.atlassian.net/browse/MOD-20665)). FORK cleans in a forked
  child (`ForkCycle_Run`). Both run through `IndexGC_RunCycle`, and all GC shares the
  scheduler in `gc.c`.
- Each policy reads its own globals:

  | Policy | Run interval | Clean threshold | Clean ratio | Retry interval |
  | --- | --- | --- | --- | --- |
  | THREAD | `search-thread-gc-run-interval` (7 s) | `search-thread-gc-clean-threshold` (100) | `search-gc-clean-ratio-ppm` (1000) | none |
  | FORK | `search-fork-gc-run-interval` (30 s) | `search-fork-gc-clean-threshold` (100) | none | `search-fork-gc-retry-interval` (5 s) |

  `search-gc-clean-ratio-ppm` is a module config with no `FT.CONFIG` alias. Disk GC reads
  the `search-fork-gc-*` interval and threshold, with disk defaults.
- `GCContext` owns one timer. When it fires, the main thread queues a run on the GC pool
  and sets `GC_SCHED_RUN_PENDING`; with `AvoidReplicaTraffic` it re-arms instead. When
  the run ends, the GC thread copies `getIntervalMs` into `pendingIntervalMs` and posts
  a one-shot that re-arms on the main thread. Intervals are carried in milliseconds
  ([#12306](https://github.com/RediSearch/RediSearch/pull/12306),
  [MOD-21201](https://redislabs.atlassian.net/browse/MOD-21201)), and
  `GC_IntervalSecToMs` caps them at `GC_MAX_INTERVAL_SEC`. `GC_SCHED_PAUSED` (disk
  consistency window) and `enabled` also block arming.
- `GCContext_NextPeriodMs` arms FORK and disk timers for `interval + rand() % interval`,
  so the real period runs from the interval to almost twice the interval. In-memory
  THREAD timers arm for the interval exactly: a THREAD cycle forks nothing, and the pool
  already bounds how many run at once.
- `IndexGC_RunCycle` returns early while the counted deletes and updates are below the
  threshold (`belowCleanThreshold`). Under FORK the threshold is
  `search-fork-gc-clean-threshold`. Under THREAD a scheduled cycle runs only when the
  count reaches both `search-thread-gc-clean-threshold` and `search-gc-clean-ratio-ppm`
  of the live documents (`sp->stats.scoring.numDocuments`, read under the spec read
  lock). The absolute threshold is checked first, so an idle index never takes the
  lock. The check runs on the GC thread and reads the globals without synchronization.
  Forced runs skip it.
- `IndexGC.retryIntervalMs` serves as both the run interval and the fork-retry interval.
  `IndexGC_Create` sets it to the run interval. A skipped cycle, a successful fork and a
  THREAD cycle set it to the run interval; a failed fork sets
  `search-fork-gc-retry-interval`, forced or scheduled.
- Dropping an index does not free its `GCContext` when a timer is armed. The next timer
  fire queues a run, which finds the index gone, and `taskCallback` frees the context.
  `GCContext_BeginDrop` decides whether the drop must queue that run itself
  (`GCContext_FinishDrop`), when no timer or run would discover it. This stays as it is
  (see Out of scope).

Existing defects that T1 fixes, because per-index settings make them worse:

1. A forced run's result moves the schedule: a forced fork that fails puts the index on
   the retry interval, and one that succeeds takes it off.
2. One Redis module timer can wait at most `INT_MAX` ms (about 24.86 days), but the
   interval settings accept far more. A longer delay makes Redis's timer loop spin or
   stop ([MOD-21060](https://redislabs.atlassian.net/browse/MOD-21060)).
3. The run's tail copies the interval into `pendingIntervalMs` on the GC thread before
   it posts the re-arm, so a change between that copy and the one-shot misses that arm.

## Data model (T2)

- `IndexSpec.gcSettings` (`IndexGCSettings`, `index_gc_settings.h`) holds two optional
  overrides, `runIntervalSec` and `cleanThreshold`. Each is an `IndexGCOverride` with an
  explicit `isSet` flag, since 0 is a valid threshold; a zeroed override is unset. T5
  adds a third, `cleanRatioPpm`.
- One helper per setting returns the override if set, otherwise the active policy's
  global value as it is when read:

  | Setting | THREAD global | FORK global |
  | --- | --- | --- |
  | `RUN_INTERVAL` | `search-thread-gc-run-interval` | `search-fork-gc-run-interval` |
  | `CLEAN_THRESHOLD` | `search-thread-gc-clean-threshold` | `search-fork-gc-clean-threshold` |
  | `CLEAN_RATIO_PPM` (T5) | `search-gc-clean-ratio-ppm` | `search-fork-gc-clean-ratio-ppm` (new) |

- Under THREAD, the clean ratio applies on top of the clean threshold, whether that is
  the index's or the global one. A per-index `CLEAN_THRESHOLD` replaces only the
  absolute threshold, so on a large index it can't lower the bar below the ratio:
  with the default 1000 ppm, a 1,000,000-document index with `CLEAN_THRESHOLD 10` still
  waits for 1,000 deletes. Until T5 the ratio can't be set per index; T5 adds that.
- Every read of the run interval used for scheduling, and of the clean threshold, goes
  through the helpers under both policies: the timer plan (`IndexGC_RunIntervalSec`,
  read when arming) and the queue-time threshold copy. `ThreadCycle_Run` and
  `ForkCycle_Run` read neither. The other settings stay global and are read as today:
  the retry interval, `forkGCCleanNumericEmptyNodes`, `forkGcSleepBeforeExit`, the run
  interval that `reap_child_blocking` takes as its timeout (see Out of scope), and,
  until T5, the clean ratio.
- The helpers reach the index through `IndexGC`'s weak reference. When the promote fails
  because the index was unlinked, `IndexGC_RunIntervalSec` returns the global value: it
  never declines to arm. A drop that overtakes a finished run while its re-arm one-shot
  is still pending relies on the timer that one-shot arms to find the drop and free the
  context (`GCContext_BeginDrop`); not arming would leak the context and its logically
  deleted count.
- Disk GC keeps reading the globals it reads today and ignores the clean ratio.
  `FT.ALTER` is disabled on disk nodes (`DiskDisabledCmd`, on shards and the
  coordinator), so its indexes never have overrides. Disk GC has its own scheduling and
  threshold code in `disk_gc.c`: it keeps its own `intervalSec`, refreshed only after a
  run that passed the threshold, and its threshold counts writes as well as deletes and
  updates. Per-index settings for disk are task 6 (see Phase 2).
- Overrides are stored whether or not the node has a `GCContext`, and are not part of
  `schemaFingerprint`.
- No atomics, and no index lock for the overrides. They are written only on the main
  thread, under the GIL. The interval is read only on the main thread, when T1 computes
  the next arm. The threshold reaches the GC thread only through the queue-time copy
  (see Threshold check).

## Persistence (T2)

- `INDEX_CURRENT_VERSION` goes from 31 to 32 (`INDEX_GC_SETTINGS_VERSION`); master
  took 30 (`INDEX_SST_TAG_VALUES_VERSION`) and 31 (`INDEX_SPARSE_VECTOR_VERSION`). The
  override block is appended at the end of the spec record in `IndexSpec_RdbSave`,
  after the conditional disk block, and is written for every index. The load side
  reads it at the same position, after its own disk-block condition.
- The block is a count followed by one `(setting id, value)` pair per override that is
  set. The id is an unsigned number; the value is a string buffer. Numeric settings
  hold an 8-byte little-endian unsigned number, written byte by byte so the encoding
  doesn't depend on the host. Unset settings are not written, so an index without
  overrides costs one number. Setting ids are fixed and never reused: 1 is
  `RUN_INTERVAL`, 2 is `CLEAN_THRESHOLD`, and T5 adds 3 for `CLEAN_RATIO_PPM`.
- A loader skips an id it doesn't know, whatever the size of its value, and counts it.
  `IndexGCSettings_ReportSkipped` logs one notice when a whole RDB load or a single
  serialized spec load ends, however many indexes it holds. That lets a later setting
  ship without another version bump, and lets a binary that predates it load the RDB
  with that setting ignored. Ignoring is safe because every GC setting only tunes when
  garbage is collected, never what a query returns. The cost: if that binary saves the
  RDB, the unknown setting is dropped.
- The load fails as corrupt when the block is truncated, or a known id has a value that
  is not 8 bytes, has a value out of range (`RUN_INTERVAL` 0 or above
  `GC_MAX_INTERVAL_SEC`, `CLEAN_THRESHOLD` above 2^63−1), or appears twice.
- Encodings below 32 load with every override unset.
- Specs are module aux data, and Redis does not reject an aux encoding version above the
  module's own. An older binary fails to load a version-32 RDB; T2 confirmed this on the
  bench VM, and it goes into the release notes.
- Slot migration needs nothing new. Overrides reach every shard through the
  coordinator's fan-out, and `_FT._RESTOREIFNX` keeps the target's own copy of an index
  that already exists. Only an index the target doesn't have yet is created from the
  serialized spec, overrides included. An older target creates that index without its
  overrides, silently (confirmed on the bench VM). That is a mixed-version cluster,
  which is not supported for this command (see Edge cases).

## Scheduler (T1)

Implemented in [#12234](https://github.com/RediSearch/RediSearch/pull/12234)
([MOD-20260](https://redislabs.atlassian.net/browse/MOD-20260)), on top of
[#12236](https://github.com/RediSearch/RediSearch/pull/12236) (MOD-21060).

- Replace the dual use of `IndexGC.retryIntervalMs` with an explicit scheduling state,
  `IndexGCSchedState` (`OK` or `FORK_FAILED`). Only scheduled cycles change it; a forced
  run's result leaves it as it was. A failed scheduled fork sets `FORK_FAILED`. A
  successful fork, a scheduled THREAD cycle, and a scheduled cycle that the threshold
  skips clear it: there is nothing left to retry, so the next arm uses the run interval.
  Without the skip case, raising the threshold after a failed fork would keep the index
  on the retry interval until enough changes accumulate. The design leaves room for a
  third state, "yielded, continue soon", needed by
  [MOD-19577](https://redislabs.atlassian.net/browse/MOD-19577). This fixes defect 1.
- Compute the delay on the main thread when arming, instead of copying it on the GC
  thread. A `nextTimer` callback returns a plan, the timer kind (run interval or retry)
  and its interval, from the scheduling state and the effective interval. This fixes
  defect 3 and removes `pendingIntervalMs`.
- `GCContext_NextPeriodMs` keeps today's jitter rules, drawn on each arm: FORK and disk
  from the interval to almost twice the interval, in-memory THREAD none.
- Delays saturate instead of overflowing, at `LLONG_MAX` ms. Redis serves every module
  timer from one event, `moduleTimerHandler`, which returns the time to the earliest
  module timer as an `int`, so one timer waits at most `INT_MAX` ms. A longer delay is
  armed in steps of at most `RS_MAX_TIMER_DELAY_MS` toward its deadline. Each step's
  callback arms the next step instead of queuing a run, so long intervals keep their
  full value and jitter. A step re-arms afresh from now when the effective interval
  changed since the arm, so a lowered interval takes effect within one step. This fixes
  defect 2.
- `GCContext` records the plan each timer was armed with (`armedPlan`), with the
  interval before jitter. For a stepped timer it also keeps the deadline
  (`stepDeadlineMs`, monotonic), and nothing else about when the timer was armed.
- A main-thread entry point, `GCContext_OnIntervalChanged`, acts only on a waiting
  run-interval timer, including one waiting between steps, and only when the effective
  interval differs from the one that timer was armed with: `GCContext_DisarmTimer`, then
  `GCContext_ArmTimer`, so the timer restarts from the command with the new interval
  and, where the policy draws one, new jitter. It never queues a run. It does nothing
  when there is no waiting run-interval timer: a run queued or running, GC stopped,
  scheduling paused, or the waiting timer is a fork retry. The next run-interval arm
  then uses the latest effective interval. `GCContext_OnIntervalChanged(NULL)` is a
  no-op, so T3 can call it for an index without a `GCContext`.
- The comparison with the armed interval is required, not an optimization. It keeps
  repeated commands from postponing GC. It also covers a `CONFIG SET` that changed an
  inheriting index's effective interval after its timer was armed: with a timer armed
  for 1000 s, `CONFIG SET search-thread-gc-run-interval 10` followed by
  `FT.ALTER idx GC SET RUN_INTERVAL 10` restarts the timer, although the effective value
  is 10 both before and after the command.
- Global `CONFIG SET` keeps today's behavior: it changes no waiting timer, and inheriting
  indexes use the new value at their next arm. A timer waiting between steps picks up
  the new value at its next step.
- Disk GC shares this code, so it gets the same arming, jitter and stepping with the
  global interval. Its cycle logic is unchanged, including where it refreshes its own
  `intervalSec`, until task 6 (Phase 2). The existing disk GC tests must keep passing.

## Threshold check (T2)

- A change applies to runs queued after it. A run is queued when its timer fires. A run
  that is already queued or running, cleaning or not, keeps the values it started with.
- Inside `GCContext_QueueRun`, after it claims `GC_SCHED_RUN_PENDING` and before it
  hands the run to the pool, a new callback, `GCCallbacks.onQueueRun`, copies the
  effective threshold into a plain `IndexGC.cleanThreshold` field. Disk GC leaves the
  callback unset. The pool hand-off, which takes a mutex, makes the copy visible to the
  GC thread, and `IndexGC_RunCycle`'s check reads only that copy. This also removes
  today's unsynchronized read of the global threshold on the GC thread. Until T5 the
  clean ratio is still read from its global at the check.
- The copy must come after the claim. `QueueRun` returns early when a run already holds
  the context, for example `GC_CONTINUE_SCHEDULE` (`GCContext_StartNow`) while a run
  started before `GC_STOP_SCHEDULE` is still going, and that run's GC thread may be
  reading the field.
- `QueueRun` is also how a drop is discovered: a timer that fires after the drop, and
  `GCContext_FinishDrop` after the unlink, both reach it. The callback promotes the
  index's weak reference. If the index is gone, it leaves the copy as it is, and the run
  finds the drop as today.
- Cost: raising the threshold while a run is queued can let that run clean once more,
  and lowering it applies one run later. The window runs from queueing to the check,
  normally milliseconds.
- Forced runs skip the check, as today.
- The comparison is unchanged (skip while the count is below the threshold), so 0 keeps
  its meaning: under FORK every scheduled cycle forks; under THREAD the ratio still
  applies.

## Clean ratio (T5)

Replaces the earlier percentage design (`CLEAN_THRESHOLD_PERCENT`, combined with
`CLEAN_THRESHOLD` by OR, over a sampled peak of live documents). Master's THREAD policy
already has a size-relative threshold, `search-gc-clean-ratio-ppm`, so a per-index
override of it, under both policies, is simpler and keeps the two policies consistent.

- New global module config `search-fork-gc-clean-ratio-ppm`, 0 to 1,000,000 parts per
  million of the live documents, default 0, changeable at runtime, read only under
  FORK. Like `search-gc-clean-ratio-ppm`, it has no `FT.CONFIG` alias. The default turns
  the ratio off, so FORK behaves as today.
- New per-index override `CLEAN_RATIO_PPM`, 0 to 1,000,000, under both policies. Unset,
  it inherits the active policy's ratio (see Data model).
- With `N` the effective clean threshold and `r` the effective ratio, a scheduled cycle
  runs only when the index's counted deletes and updates reach `N` and, when `r` is not
  0, also `r × live / 1,000,000`. This is master's THREAD rule, now under FORK as well.
  `live` is `sp->stats.scoring.numDocuments`, read at the check under the spec read
  lock, as today. The absolute threshold is checked first, so an idle index never takes
  the lock. The product is split so it can't overflow, as today.
- A ratio of 0 leaves `N` alone. An empty index has a ratio bar of 0, so `N` alone
  decides.
- The queue-time copy holds the effective ratio next to the threshold, and the check
  reads only the copy, so a ratio change applies to runs queued after it, like a
  threshold change.
- New setting id 3 in the RDB block, with no version bump, since older loaders skip it.
  A stored ratio above 1,000,000 fails the load.
- `clean_ratio_ppm` in FT.INFO `gc_settings`. Disk GC ignores the ratio, so a disk
  index's FT.INFO leaves it out.

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
- Parse and validate every setting before applying any, with
  `IndexGCSettings_IsValid`. Reject duplicates, unknown words, out-of-range values and a
  missing value. `RUN_INTERVAL` above `GC_MAX_INTERVAL_SEC` is out of range, so no
  accepted value is silently capped. `CLEAN_RATIO_PPM` (T5) takes 0 to 1,000,000; T3
  leaves room for it in the parser and T5 adds it.
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
  `COMMAND_ARITY_OVERRIDES` in `srcutil/gen_command_info.py`. The summary, today "Adds a
  new field to the index", covers both actions, and the complexity says `O(N)` applies
  to `SCHEMA ADD` and `GC` is `O(1)`. `src/command_info/command_info.c` is regenerated
  with `srcutil/gen_command_info.py`, and `COMMAND DOCS` is checked.
- An `FT.DEBUG` subcommand reports the time left on an index's GC timer and whether it
  is a run-interval or retry timer, so flow tests can check timing without sleeping
  for the jitter.
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
  of `configured` and `effective`. T5 adds `clean_ratio_ppm` in the same shape. Values
  are integers. `configured` is null when inherited: RESP3 null, RESP2 nil bulk string.
  `effective` is the override, or the active policy's global. A later setting, such as
  `policy`, is one more key, so clients that read the map by key are unaffected.
- Present for every index except TEMPORARY indexes, whether or not the node runs GC.
  FT.INFO of a TEMPORARY index is unchanged. On a disk index `effective` is the global
  value disk GC reads, and `clean_ratio_ppm` (T5) is left out, because disk GC ignores
  the ratio; task 6 adds it if disk adopts the ratio.
- It can't go in `index_options` (a flat list of flags) or `gc_stats` (missing without a
  `GCContext`, and summed by the coordinator).
- The coordinator drops top-level keys it doesn't know, so T4 adds `gc_settings` to its
  merge. It copies the first shard's section, and tolerates shard replies without one
  (older shards during an upgrade). `configured` is the same on every shard once a
  fan-out has succeeded everywhere. `effective` can differ if the global settings
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
- **Mixed versions:** use the command only once every node is upgraded. Old nodes
  reject `GC` with an error and don't crash: a build without this change expects an
  optional `SKIPINITIALSCAN` and then `SCHEMA`, so `GC` gets "ALTER must be followed by
  SCHEMA". An old replica fails the replicated command and follows
  `propagation-error-behavior`: under the default, `ignore`, it logs the error and lacks
  the override; under `panic` the replica stops. In a partly upgraded cluster the
  coordinator returns an error, and upgraded shards keep the change. An older shard
  that receives an index by slot migration drops its overrides silently, and an older
  binary can't load a version-32 RDB (see Persistence). Until the upgrade completes,
  only GC timing differs between shards; queries are unaffected. Re-running after the
  upgrade makes all shards agree. Known gap: the coordinator's FT.INFO shows the first
  shard's values, so a mismatch shows only in each shard's own FT.INFO. Upgrade order
  and downgrade guidance belong in the Enterprise notes, not here.
- **Very large intervals:** the longest accepted, `GC_MAX_INTERVAL_SEC`, means
  "practically never". A timer past `INT_MAX` ms is armed in steps.
- **Threshold 0 under THREAD:** the ratio still applies, so a large index still waits
  for its share. `CLEAN_RATIO_PPM 0` (T5) makes threshold 0 clean every interval, as
  under FORK.

## Out of scope

Existing behavior that per-index settings do not depend on, left as it is:

- A dropped index keeps its `GCContext` until its timer fires. With a stepped timer,
  the next step finds the drop and queues the run that frees the context.
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
| Setting values as fixed 64-bit numbers in the RDB block | A loader could skip an unknown id only if its value had that size. A string buffer lets a later setting hold any value. |
| Accept `RUN_INTERVAL` up to 2^63−1, as `search-fork-gc-run-interval` does | Values above `GC_MAX_INTERVAL_SEC` would be capped silently when armed. Rejecting them matches `search-thread-gc-run-interval`. |
| GC options in `FT.CREATE` in phase 1 | Not needed to keep the first API change small. Task 7 adds them as parsing into the same stored fields (Phase 2). Until then, `FT.ALTER` right after `FT.CREATE` covers rebuild and alias swap, since the new index has no deletes yet. |
| A per-index `POLICY` now (`FORK`, `NONE`) | The policy is fixed at load for the whole node, and a doc table keeps the THREAD GC's deleted-ID set only when the node runs THREAD. `NONE` also stops tiered vector GC, which runs inside the cycle. Postponed; it can be added later as one more setting with no version bump. |
| Under THREAD, a per-index `CLEAN_THRESHOLD` also turns the ratio off | One setting would silently change two rules, and the override would mean different things under the two policies. T5's `CLEAN_RATIO_PPM` sets the ratio on its own. |
| A percentage OR-combined with `CLEAN_THRESHOLD`, over a sampled peak of live documents (the earlier T5) | Master's THREAD ratio already makes the threshold relative to size, with an AND rule over live documents. A second rule would make the policies disagree, and the peak added state that forced runs and slot-cleanup deletes had to keep consistent. |

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

- C++ unit tests for T1: saturation, a stepped timer reaching its deadline, a step
  re-arming afresh after the interval changed, a step finding a dropped index, a
  restart between steps, restart when the effective interval differs from the armed
  one and no restart when it doesn't, the restart as a no-op in every no-timer state,
  on a retry timer and for a NULL context, a forced run leaving the scheduling state
  alone, a scheduled THREAD cycle and a cycle skipped by the threshold after a failed
  fork arming with the run interval.
- C++ unit tests for T2: the threshold check with a per-index threshold under both
  policies, including 0, an unset value following a changed global of the active
  policy, the ratio still applying on top of a per-index threshold under THREAD; the
  queue-time copy (including a dropped index with an armed timer,
  `GCContext_FinishDrop`, and `QueueRun` returning early while a run holds the
  context); the interval helper returning the global value for an unlinked index; RDB
  round trip at version 32 and loads of versions 29, 30 and 31; a block with an unknown
  setting id of any value size loading with that setting skipped and one notice; and
  each load failure: a truncated block, a known id with a value of the wrong size, out
  of range (`RUN_INTERVAL` 0 or above `GC_MAX_INTERVAL_SEC`), or given twice.
- C++ unit tests for T5: the ratio rule under both policies, inheritance of each
  policy's ratio, `CLEAN_RATIO_PPM 0` leaving the threshold alone, an empty index,
  `CLEAN_THRESHOLD 0` with a ratio, values near the overflow limit, the queue-time copy
  of the ratio, RDB round trip with the ratio set and cleared, and a stored ratio above
  1,000,000 failing the load.
- Flow tests for T5: the scenarios in the spec delta under both policies, inheritance
  and `DEFAULT`, range errors, FT.INFO in RESP2 and RESP3.
- Flow tests for T1: an idle index below the threshold picks up a new global interval
  within one old period; a change in the capture-to-drain window
  (`SYNC_POINT_GC_BEFORE_REARM_POST`) takes effect on the next arm; a forced fork that
  fails leaves the run-interval timer; fork-failure retry still uses
  `search-fork-gc-retry-interval`; the existing `test_gc.py` and `test_gc_thread.py`
  scheduling tests pass unchanged.
- Flow tests, standalone and cluster, under both policies: two indexes making progress
  with different settings, syntax, arity and validation errors, all-or-nothing,
  aliases, ACL, unknown index, inheritance and `DEFAULT`, `CONFIG SET` leaving timers
  alone, threshold 0, timer restart timing and the `DEFAULT` cases through the
  `FT.DEBUG` hook, repeated commands leaving the timer alone, `CONFIG SET` followed by
  `FT.ALTER` with the new value restarting the timer, updates while GC is queued,
  running, retrying or stopped, TEMPORARY rejection, `search-no-gc` nodes as master and
  as replica, FT.INFO in RESP2 and RESP3.
- Queued run keeps its threshold (T2): with `ENABLE_ASSERT`, park a run at
  `SYNC_POINT_GC_TASK_START`, change the threshold, and check that the parked run uses
  the old value and the next queued run uses the new one.
- Recovery flow tests: `DEBUG RELOAD`, full sync, failover, AOF and `BGREWRITEAOF`, slot
  migration of an index the target doesn't have yet.
- Disk GC: the existing tests pass unchanged after T1.

## Phase 2: Flex and FT.CREATE (tasks 6 and 7)

Designed with the rest so nothing is missed, and shipped after tasks 0–5. A per-index
`POLICY` and the self-service guardrail on the Confluence page stay unscheduled.

### Flex: `FT.ALTER ... GC` on disk indexes (task 6)

- Users see the same syntax, ranges, inheritance, `DEFAULT` and FT.INFO as on RAM
  indexes. `SCHEMA ADD` stays rejected on Flex.
- Disk GC compacts in-process on the shard's single GC thread instead of forking, so
  the fork-slot warning doesn't apply; an index that compacts often delays GC for the
  other indexes on the shard. Its threshold counts writes, deletes and updates, as the
  global setting does on disk today, and threshold 0 compacts every interval even when
  nothing changed.
- Command: remove `DiskDisabledCmd` from `FT.ALTER` and `_FT._ALTERIFNX`, both on the
  shards and on the coordinator. On Flex, the handler rejects every action except `GC`,
  and the coordinator rejects before fan-out in both commands, with "FT.ALTER SCHEMA
  ADD is not supported in Redis Flex".
- Interval: `disk_gc.c` drops its cached `intervalSec`, and its timer plan uses the
  effective-value helper, read on the main thread when the timer is armed (T1). Disk
  then picks up a changed interval at its next arm, not only after a run that passed
  the threshold.
- Threshold: disk GC sets the queue-time copy callback, and `periodicCb` checks only
  that copy, which removes its unsynchronized read of the global on the GC thread.
- `GCContext_OnIntervalChanged` already does nothing while `GC_SCHED_PAUSED`; resuming
  arms with the effective interval.
- Persistence: none. The override block is written for every index after the disk
  block, so Flex indexes already carry it. Tests cover loading with and without
  `REDISMODULE_CTX_FLAGS_SST_RDB`.
- Clean ratio (if T5 ships): the same rule, with disk's change count (writes included)
  and its document count, read under the spec read lock. Open for the disk team; if a
  share of changes isn't a useful compaction trigger, disk ignores the ratio.
- Tests: the existing disk GC tests, unit tests for the disk callbacks, and Flex flow
  tests for the command and the `SCHEMA ADD` rejection on shards and the coordinator,
  inheritance, timer restart, threshold counting, FT.INFO, RDB round trip and
  replication.

### GC options in `FT.CREATE` (task 7)

- `FT.CREATE <index> ... [GC SET <settings>] SCHEMA ...`, parsed by the same code as
  `FT.ALTER ... GC SET` into the same stored fields. `DEFAULT` is rejected at create
  time, since there is nothing to clear.
- RDB, replication and FT.INFO are unchanged. It works on RAM indexes, and on Flex once
  task 6 routes disk GC through the helpers. Until then Flex rejects the clause, so it
  never stores settings that disk GC ignores.
- `commands.json` gains the clause; regenerate `src/command_info/command_info.c`.
