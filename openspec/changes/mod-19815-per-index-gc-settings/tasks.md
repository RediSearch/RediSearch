# Tasks: per-index fork GC settings

Each top-level task is one PR and matches a Jira task under
[MOD-19815](https://redislabs.atlassian.net/browse/MOD-19815). Order: 0 and 1 can start
in parallel. 2 needs 0, for the fingerprint decision. Whichever of 1 and 2 merges second
connects the arm-time interval read to the effective-value helper (2.2). 3 needs 1 and
2; 4 needs 3. 5 needs 3 and 4, and Product sign-off (0.5). It can be dropped or shipped
in a later release without changing 1–4, because the RDB block (2.4) already takes new
settings without a version bump.

## 0. Spec review ([MOD-20259](https://redislabs.atlassian.net/browse/MOD-20259))

- [ ] 0.1 Product sign-off on the user-facing behavior (Confluence §3)
- [ ] 0.2 Dev sign-off on this design and the open questions in `design.md`
- [ ] 0.3 Confirm Enterprise proxy and Active-Active behavior with RS/DMC
- [x] 0.4 Sync MOD-19815, T0–T5 and the Confluence page with the review decisions
- [ ] 0.5 Product sign-off on T5 (decision 13)
- [ ] 0.6 Note on MOD-20523 that per-index `POLICY` is postponed, with the RDB and
      FT.INFO room this spec leaves for it

## 1. Scheduler ([MOD-20260](https://redislabs.atlassian.net/browse/MOD-20260))

- [ ] 1.1 Replace the dual use of `ForkGC.retryInterval` with an explicit "last fork
      failed" state, changed only by scheduled cycles
- [ ] 1.2 Compute the next delay on the main thread when arming; remove
      `pendingInterval`
- [ ] 1.3 Keep today's jitter range, drawn on each arm; cap delays and saturate
      conversions; record in `GCContext` the interval each run-interval timer was armed
      with
- [ ] 1.4 Add `GCContext_OnIntervalChanged`: when a run-interval timer is waiting and the
      effective interval differs from its armed interval, disarm it and arm it again
      from now; never queue a run; no-op otherwise. If MOD-20523 has landed, keep
      `GC_SCHED_WAIT_FOR_WORK` and update any deadline it stores at arm
- [ ] 1.5 C++ unit tests: saturation and the cap, restart when the effective interval
      differs from the armed one and no restart when it doesn't, the restart as a no-op
      in each no-timer state and on a retry timer, forced runs leaving the scheduling
      state alone; run under UBSan if practical
- [ ] 1.6 Flow tests: an idle index below the threshold picks up a new global interval
      within one old period; a change in the capture-to-drain window
      (`SYNC_POINT_GC_BEFORE_REARM_POST`) takes effect on the next arm; fork-failure
      retry still uses `search-fork-gc-retry-interval`
- [ ] 1.7 The existing `test_gc.py` scheduling tests and the disk GC tests pass
      unchanged

## 2. Storage, queue-time threshold copy, RDB ([MOD-20261](https://redislabs.atlassian.net/browse/MOD-20261))

- [ ] 2.1 Add the two optional values to `IndexSpec`, the effective-value helpers, and
      the `ForkGC` field that holds the queue-time threshold copy
- [ ] 2.2 Route every read of the scheduling interval and the clean threshold through the
      helpers; the other fork-GC settings, including the `reap_child_blocking` timeout,
      stay global
- [ ] 2.3 New GC callback, called in `GCContext_QueueRun` after it claims
      `GC_SCHED_RUN_PENDING` and before `add_work`, that copies the effective threshold
      into `ForkGC` and leaves the copy as it is when the index is gone; `FGC_RunCycle`
      checks only the copy
- [ ] 2.4 Bump `INDEX_CURRENT_VERSION` to 30; save and load the override block as a
      count plus `(setting id, value)` pairs, skipping unknown ids
- [ ] 2.5 Confirm what an old binary does with a version-30 RDB
- [ ] 2.6 C++ unit tests: `FGC_RunCycle` with a per-index threshold, including 0, and an
      unset value following a changed global; the queue-time copy, including a dropped
      index with an armed timer, `GCContext_FinishDrop`, and `QueueRun` returning early
      while a run holds the context; RDB round trip with no override, one, and both,
      plus threshold 0; overrides on a spec without a GC context; version-29 load; a
      block with an unknown setting id; the unset state on every construction path
      (create, RDB load, legacy load, deserialize)
- [ ] 2.7 Flow test (`ENABLE_ASSERT`): park a run at `SYNC_POINT_GC_TASK_START`, change
      the threshold, and check that the parked run uses the old value and the next
      queued run uses the new one

## 3. `FT.ALTER ... GC` ([MOD-20263](https://redislabs.atlassian.net/browse/MOD-20263))

- [ ] 3.1 Parse and validate `GC` in `AlterIndexInternalCommand`, all-or-nothing
- [ ] 3.2 Reject TEMPORARY indexes and `SKIPINITIALSCAN GC`; store on indexes without a
      `GCContext`
- [ ] 3.3 Call `GCContext_OnIntervalChanged` when the command sets or clears
      `RUN_INTERVAL`; it restarts only when the effective interval differs from the
      armed one. If MOD-20523 has landed, call `GCContext_RunIfWaitedFor` after a
      threshold change
- [ ] 3.4 Replicate through `_FT._ALTERIFNX`; new unknown-action error text
- [ ] 3.5 `commands.json`: `oneof` between `[SKIPINITIALSCAN] SCHEMA ADD` and `GC`;
      regenerate `src/command_info/command_info.c` with `srcutil/gen_command_info.py`
      and check `COMMAND DOCS`
- [ ] 3.6 `FT.DEBUG` hook that reports the time left and the type of an index's GC timer
- [ ] 3.7 Flow tests: two indexes making progress with different settings, syntax, arity
      and validation, aliases, ACL, unknown index, inheritance and `DEFAULT`,
      `CONFIG SET` leaving timers alone, `CONFIG SET` followed by `FT.ALTER` with the new
      value restarting the timer, threshold 0, timer restart timing, repeated commands
      and the `DEFAULT` cases leaving or restarting the timer, updates while GC is
      queued, running, retrying or stopped, drop and recreate, TEMPORARY, `search-no-gc`
      master and replica, cluster fan-out
- [ ] 3.8 Recovery flow tests: `DEBUG RELOAD`, full sync, failover, AOF,
      `BGREWRITEAOF`, slot migration of an index the target doesn't have

## 4. FT.INFO and release notes ([MOD-20264](https://redislabs.atlassian.net/browse/MOD-20264))

- [ ] 4.1 `gc_settings` in FT.INFO, RESP2 and RESP3
- [ ] 4.2 Add `gc_settings` to the coordinator's FT.INFO merge (first shard), tolerating
      shard replies without it
- [ ] 4.3 Flow tests for both protocols, standalone and through the coordinator
- [ ] 4.4 Release notes: the command, the encoding version change, upgrade order
- [ ] 4.5 Update the delta spec to match what shipped

## 5. Percentage threshold ([MOD-20691](https://redislabs.atlassian.net/browse/MOD-20691), pending 0.5)

- [ ] 5.1 Global module config `search-fork-gc-clean-threshold-percent`, 0 to 100,
      default 0
- [ ] 5.2 Per-index `CLEAN_THRESHOLD_PERCENT` override: parser, storage, RDB setting id,
      effective-value helper
- [ ] 5.3 Combined threshold `min(N, max(1, ceil(p × base / 100)))`, computed on the main
      thread in the queue-time copy, with `base` the larger of the live documents now and
      at the last counter reset; read the live count under the spec read lock; store the
      queue-time sample of the run that resets the counter in `ForkGC`, starting at the
      live count when the GC context is created
- [ ] 5.4 `clean_threshold_percent` in FT.INFO `gc_settings`; combined threshold and
      `base` in the `FT.DEBUG` hook
- [ ] 5.5 C++ unit tests: updates, deletes, growth, deletes before the first fork,
      empty index, `p = 0`, `CLEAN_THRESHOLD 0`, overflow limits
- [ ] 5.6 Flow tests: the two-index example, global inheritance, `DEFAULT`, range errors,
      FT.INFO in RESP2 and RESP3

## 6. Docs (DOC ticket, created after sign-off)

- [ ] 6.1 Syntax, inheritance, aliases, when changes apply, jitter, FT.INFO and per-node
      `effective` values, upgrades, the fork-slot warning for threshold 0 with short
      intervals, `deny-oom` and partial apply in a cluster, the coordinator's FT.INFO
      showing the first shard, how the percentage combines with `CLEAN_THRESHOLD`
