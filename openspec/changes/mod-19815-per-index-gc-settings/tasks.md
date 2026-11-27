# Tasks: per-index GC settings

Each top-level task is one PR and matches a Jira task under
[MOD-19815](https://redislabs.atlassian.net/browse/MOD-19815). Order: 0 and 1 can start
in parallel. 2 needs 0, for the fingerprint decision. Whichever of 1 and 2 merges second
connects the arm-time interval read to the effective-value helper (2.2). 3 needs 1 and
2; 4 needs 3. 5 needs 3, and Product sign-off (0.5) before it merges; whichever of 4
and 5 merges second adds `clean_ratio_ppm` to FT.INFO (5.4). 5 can be dropped or shipped
in a later release without changing 1–4, because the RDB block (2.4) already takes new
settings without a version bump. Phase 2: 6 needs 1–4 (and 5 for the ratio on disk); 7
needs 2 and 3, and its Flex path needs 6. 9 needs 1–5 merged on master.

## 0. Spec review ([MOD-20259](https://redislabs.atlassian.net/browse/MOD-20259))

- [ ] 0.1 Product sign-off on the user-facing behavior (Confluence §3)
- [ ] 0.2 Dev sign-off on this design and the open questions in `design.md`
- [x] 0.4 Sync MOD-19815, T0–T5 and the Confluence page with the review decisions
- [ ] 0.5 Product sign-off on T5 (decision 13)
- [x] 0.6 Note on MOD-20523 that per-index `POLICY` is postponed, with the RDB and
      FT.INFO room this spec leaves for it

## 1. Scheduler ([MOD-20260](https://redislabs.atlassian.net/browse/MOD-20260), [#12234](https://github.com/RediSearch/RediSearch/pull/12234) on [#12236](https://github.com/RediSearch/RediSearch/pull/12236))

- [ ] 1.1 Replace the dual use of `IndexGC.retryIntervalMs` with `IndexGCSchedState`,
      changed only by scheduled cycles: a failed fork sets it; a successful fork, a
      THREAD cycle and a cycle skipped by the threshold clear it
- [ ] 1.2 Compute the next delay on the main thread when arming, from a `nextTimer`
      plan; remove `pendingIntervalMs`
- [ ] 1.3 Keep today's jitter rules, drawn on each arm (none for in-memory THREAD);
      saturate delays; arm a delay past `INT_MAX` ms in steps toward its deadline,
      re-arming afresh at a step when the effective interval changed and queuing the run
      at a step that finds the index dropped; record in `GCContext` the plan each timer
      was armed with
- [ ] 1.4 Add `GCContext_OnIntervalChanged`: when a run-interval timer is waiting,
      including between steps, and the effective interval differs from its armed
      interval, disarm it and arm it again from now; never queue a run; no-op otherwise
      and for a NULL context
- [ ] 1.5 C++ unit tests: saturation, a stepped timer reaching its deadline, a step
      re-arming after the interval changed, a step finding a dropped index, a restart
      between steps, restart when the effective interval differs from the armed one and
      no restart when it doesn't, the restart as a no-op in each no-timer state, on a
      retry timer and for NULL, forced runs leaving the scheduling state alone, a
      threshold-skipped cycle after a failed fork and a THREAD cycle arming with the run
      interval; run under UBSan if practical
- [ ] 1.6 Flow tests: an idle index below the threshold picks up a new global interval
      within one old period; a change in the capture-to-drain window
      (`SYNC_POINT_GC_BEFORE_REARM_POST`) takes effect on the next arm; a forced fork
      that fails leaves the run-interval timer; fork-failure retry still uses
      `search-fork-gc-retry-interval`
- [ ] 1.7 The existing `test_gc.py`, `test_gc_thread.py` scheduling tests and the disk GC
      tests pass unchanged

## 2. Storage, queue-time threshold copy, RDB ([MOD-20261](https://redislabs.atlassian.net/browse/MOD-20261), [#12345](https://github.com/RediSearch/RediSearch/pull/12345))

- [ ] 2.1 Add `IndexSpec.gcSettings` (`IndexGCSettings`), the effective-value helpers
      that fall back to the active policy's globals, and the `IndexGC.cleanThreshold`
      field that holds the queue-time copy; `IndexGC_RunIntervalSec` returns the global
      value when the index is gone
- [ ] 2.2 Route every read of the scheduling interval and the clean threshold, under
      both policies, through the helpers; under THREAD keep the global clean ratio on
      top of the effective threshold; disk GC keeps its cached `intervalSec` and its own
      reads, and the other settings, including the retry interval and the
      `reap_child_blocking` timeout, stay global
- [ ] 2.3 New `GCCallbacks.onQueueRun` hook, called in `GCContext_QueueRun` after it
      claims `GC_SCHED_RUN_PENDING` and before `add_work`, that copies the effective
      threshold into `IndexGC` and leaves the copy as it is when the index is gone;
      `IndexGC_RunCycle` checks only the copy
- [ ] 2.4 Bump `INDEX_CURRENT_VERSION` to 32; save the override block as a count plus
      `(setting id, string-buffer value)` pairs; on load skip unknown ids of any value
      size with one notice per load, and fail on a truncated block or a known id whose
      value has the wrong size, is out of range or is given twice
- [x] 2.5 Confirm what an old binary does with a version-32 RDB: it fails to load it;
      an older shard that receives an index by slot migration drops its overrides
      (both seen on the bench VM)
- [ ] 2.6 C++ unit tests: the threshold check with a per-index threshold under both
      policies, including 0, and an unset value following a changed global of the
      active policy; the ratio still applying on top of a threshold override under
      THREAD; the queue-time copy, including a dropped index with an armed timer,
      `GCContext_FinishDrop`, and `QueueRun` returning early while a run holds the
      context; the interval helper returning the global value for an unlinked index;
      RDB round trip with no override, one, and both, plus threshold 0; overrides on a
      spec without a GC context; loads of versions 29, 30 and 31; a block with an
      unknown setting id; each load failure in 2.4; the unset state on every
      construction path (create, RDB load, legacy load, deserialize)
- [ ] 2.7 Flow test (`ENABLE_ASSERT`): park a run at `SYNC_POINT_GC_TASK_START`, change
      the threshold, and check that the parked run uses the old value and the next
      queued run uses the new one

## 3. `FT.ALTER ... GC` ([MOD-20263](https://redislabs.atlassian.net/browse/MOD-20263))

- [ ] 3.1 Parse and validate `GC SET` in `AlterIndexInternalCommand`, all-or-nothing,
      with `IndexGCSettings_IsValid`; reject `RUN_INTERVAL` above `GC_MAX_INTERVAL_SEC`
- [ ] 3.2 Reject TEMPORARY indexes and `SKIPINITIALSCAN GC`; store on indexes without a
      `GCContext`
- [ ] 3.3 Call `GCContext_OnIntervalChanged` when the command sets or clears
      `RUN_INTERVAL`; it restarts only when the effective interval differs from the
      armed one
- [ ] 3.4 Replicate through `_FT._ALTERIFNX`; new unknown-action error text
- [ ] 3.5 `commands.json`: `oneof` between `[SKIPINITIALSCAN] SCHEMA ADD` and `GC SET`,
      a summary covering both actions, and per-action complexity; pin `FT.ALTER` at
      arity `-6` in `COMMAND_ARITY_OVERRIDES`; regenerate
      `src/command_info/command_info.c` with `srcutil/gen_command_info.py` and check
      `COMMAND DOCS`
- [ ] 3.6 `FT.DEBUG` hook that reports the time left and the type of an index's GC timer
- [ ] 3.7 Flow tests under both policies: two indexes making progress with different
      settings, syntax, the six-word form, a missing `SET`, arity and validation,
      aliases, ACL, unknown index, inheritance from each policy's globals and `DEFAULT`,
      `CONFIG SET` leaving timers alone, `CONFIG SET` followed by `FT.ALTER` with the
      new value restarting the timer, threshold 0, timer restart timing, repeated
      commands and the `DEFAULT` cases leaving or restarting the timer, updates while GC
      is queued, running, retrying or stopped, drop and recreate, a drop with a
      `RUN_INTERVAL` override overtaking a pending re-arm still tearing down (as
      `testGCTeardownWhenDropOvertakesPendingRearm_MOD_17309` does without one),
      TEMPORARY, `search-no-gc` master and replica, cluster fan-out
- [ ] 3.8 Recovery flow tests: `DEBUG RELOAD`, full sync, failover, AOF,
      `BGREWRITEAOF`, slot migration of an index the target doesn't have

## 4. FT.INFO and release notes ([MOD-20264](https://redislabs.atlassian.net/browse/MOD-20264))

- [ ] 4.1 `gc_settings` in FT.INFO, RESP2 and RESP3, with `effective` from the active
      policy
- [ ] 4.2 Add `gc_settings` to the coordinator's FT.INFO merge (first shard), tolerating
      shard replies without it
- [ ] 4.3 Flow tests for both protocols and both policies, standalone and through the
      coordinator
- [ ] 4.4 Release notes: the command, the encoding version change, that an older binary
      can't load the RDB and an older shard drops migrated overrides, so the command is
      for fully upgraded clusters only
- [ ] 4.5 Update the delta spec to match what shipped

## 5. Per-index clean ratio ([MOD-20691](https://redislabs.atlassian.net/browse/MOD-20691), [#12620](https://github.com/RediSearch/RediSearch/pull/12620), merge pending 0.5)

- [ ] 5.1 Global module config `search-fork-gc-clean-ratio-ppm`, 0 to 1,000,000,
      default 0, no `FT.CONFIG` alias, read only under FORK
- [ ] 5.2 Per-index `CLEAN_RATIO_PPM` override: parser, storage, RDB setting id 3,
      effective-value helper inheriting the active policy's ratio; add it to the
      `GC SET` branch in `commands.json`, regenerate `src/command_info/command_info.c`
      and check `COMMAND DOCS`
- [ ] 5.3 The queue-time copy holds the effective ratio next to the threshold; the check
      applies the ratio rule from the copy under both policies, so FORK gains it and
      THREAD stops reading the global at the check
- [ ] 5.4 `clean_ratio_ppm` in FT.INFO `gc_settings`, left out on a disk index
- [ ] 5.5 C++ unit tests: the ratio rule under both policies, inheritance of each
      policy's ratio, `CLEAN_RATIO_PPM 0`, an empty index, `CLEAN_THRESHOLD 0` with a
      ratio, values near the overflow limit, a ratio change applying to the next queued
      run; RDB round trip with the ratio set and cleared with `DEFAULT`, and a stored
      ratio above 1,000,000 failing the load
- [ ] 5.6 Flow tests: the T5 scenarios in the spec delta under both policies, global
      inheritance, `DEFAULT`, range errors, FT.INFO in RESP2 and RESP3, and no
      `clean_ratio_ppm` on a disk index

## 6. Flex: `FT.ALTER ... GC` on disk indexes, phase 2 ([MOD-21052](https://redislabs.atlassian.net/browse/MOD-21052))

- [ ] 6.1 Remove `DiskDisabledCmd` from `FT.ALTER` and `_FT._ALTERIFNX`, on the shards
      and on the coordinator; on Flex reject every action except `GC` in the handler
      and, for both commands, on the coordinator before fan-out
- [ ] 6.2 `disk_gc.c`: drop the cached `intervalSec`; its timer plan uses the
      effective-value helper
- [ ] 6.3 Set the queue-time copy callback for disk GC; `periodicCb` checks only the copy
- [ ] 6.4 If 5 has shipped and the disk team agrees: the ratio with disk's change count
      and document count, and `clean_ratio_ppm` in a disk index's FT.INFO
- [ ] 6.5 Unit tests for the disk callbacks; the existing disk GC tests pass
- [ ] 6.6 Flex flow tests: the command and the `SCHEMA ADD` rejection on shards and the
      coordinator, inheritance and `DEFAULT`, timer restart, threshold counting writes,
      FT.INFO, RDB round trip with and without `REDISMODULE_CTX_FLAGS_SST_RDB`,
      replication through `_FT._ALTERIFNX` on shards and the coordinator
- [ ] 6.7 Update the delta spec: replace the Flex rejection of `FT.ALTER ... GC` with
      the Flex behavior

## 7. GC options in `FT.CREATE`, phase 2 ([MOD-21053](https://redislabs.atlassian.net/browse/MOD-21053))

- [ ] 7.1 Parse `GC SET <settings>` before `SCHEMA` with the `FT.ALTER` parser, into
      the same stored fields; reject `DEFAULT`; on Flex, reject the clause until 6 has
      shipped
- [ ] 7.2 `commands.json`: add the clause to `FT.CREATE`; regenerate
      `src/command_info/command_info.c` and check `COMMAND DOCS`
- [ ] 7.3 Flow tests: each setting and both, validation and `DEFAULT` rejection, FT.INFO,
      replication, RDB reload, cluster fan-out, Flex (rejected before 6, accepted after)

## 8. Docs (DOC ticket, created after sign-off)

- [ ] 8.1 Syntax, inheritance from each policy's globals, aliases, when changes apply,
      jitter under each policy, FT.INFO and per-node `effective` values, upgrades, the
      fork-slot warning for threshold 0 with short intervals under FORK, `deny-oom` and
      partial apply in a cluster, the coordinator's FT.INFO showing the first shard, how
      the clean ratio combines with `CLEAN_THRESHOLD` (both must be met)

## 9. Backport to 8.12, 8.10 and 8.8, FORK only (`design.md`, Backports)

One PR per line, in this order, each a port onto the line's own FORK GC.

- [ ] 9.1 8.10 and 8.8: backport the main-thread timer fix (#10728, #10747, MOD-17309)
      as its own bug-fix PR first
- [ ] 9.2 Port T1's arm-time interval read, timer restart and forced-run fix, and the
      MOD-21060 timer cap, onto `src/gc.c` and `src/fork_gc/fork_gc.c`
- [ ] 9.3 Port T2–T5 without the THREAD parts: overrides and RDB block, `FT.ALTER ...
      GC SET` with all three settings, `search-fork-gc-clean-ratio-ppm`, FT.INFO
      `gc_settings`, `FT.DEBUG GC_TIMER_INFO`
- [ ] 9.4 Register `ft_index0` at 32 on RAM nodes and at the line's current version on
      Flex nodes; load files up to 32 in both modes; reserve the versions between as
      numbers only
- [ ] 9.5 8.10 and 8.8: write and read the version 27 and 28 vector values for each
      tiered HNSW field at 32, refusing any other value on load
- [ ] 9.6 Refuse an SST file at 30 or above, and field type bits the line doesn't know
- [ ] 9.7 Flex: keep overrides loaded from a RAM file in memory, with one notice that
      the next save drops them
- [ ] 9.8 Tests: the line's GC flow and unit tests for the ported parts; its RAM RDB at
      32 with overrides and tiered HNSW, flat and SVS vector fields loads on master with
      both; master's RAM RDB at 32 without sparse or quantized fields loads on the line;
      its Flex node saves at its current version; a RAM RDB at 32 with overrides loads
      on its Flex node
