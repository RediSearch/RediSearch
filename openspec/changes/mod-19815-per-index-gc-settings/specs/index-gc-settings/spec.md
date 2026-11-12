# index-gc-settings (delta)

Parts marked (T5) depend on Product sign-off on the percentage threshold (decision 13,
MOD-20691). Until T5 ships, `CLEAN_THRESHOLD_PERCENT`,
`search-fork-gc-clean-threshold-percent` and `clean_threshold_percent` do not exist, and
the clean threshold is the effective `CLEAN_THRESHOLD` alone.

## ADDED Requirements

### Requirement: FT.ALTER sets per-index GC overrides
`FT.ALTER <index> GC SET [RUN_INTERVAL <seconds|DEFAULT>]
[CLEAN_THRESHOLD <count|DEFAULT>] [CLEAN_THRESHOLD_PERCENT <percent|DEFAULT>]` SHALL set or clear the index's GC run
interval, clean threshold and (T5) clean threshold percentage. `SET` SHALL be required.
It SHALL take at least one setting, each at most once, in any order. It SHALL change no
schema and trigger no reindex. `FT.ALTER` SHALL keep its declared arity of at least six
words. This requirement covers RAM indexes. On a Flex (disk) node, `FT.ALTER ... GC`
SHALL return an error and change nothing, as every `FT.ALTER` does there today; task 6
replaces this with the Flex behavior.

#### Scenario: Set both settings
- **WHEN** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 100 CLEAN_THRESHOLD 500`
- **THEN** the reply SHALL be `OK`
- **AND** `idx` SHALL use a run interval of 100 seconds and a clean threshold of 500

#### Scenario: Setting one keeps the other
- **WHEN** `idx` has overrides `RUN_INTERVAL 100` and `CLEAN_THRESHOLD 500`
- **AND** the user executes `FT.ALTER idx GC SET CLEAN_THRESHOLD 50`
- **THEN** `idx` SHALL keep the run interval override of 100

#### Scenario: Invalid command changes nothing
- **WHEN** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 100 CLEAN_THRESHOLD -1`
- **THEN** the command SHALL return an error
- **AND** neither setting of `idx` SHALL change

#### Scenario: Shortest form
- **WHEN** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 10`, directly or through
  the Redis Software proxy
- **THEN** the reply SHALL be `OK`

#### Scenario: Rejected forms
- **WHEN** the user executes `FT.ALTER idx GC SET` with no setting, omits `SET`, repeats
  a setting, gives an unknown setting, omits a value, or gives `RUN_INTERVAL 0`
- **THEN** the command SHALL return an error (the arity error when the command has fewer
  than six words, an error naming `SET` when another word follows `GC`)

#### Scenario: SKIPINITIALSCAN is not accepted with GC
- **WHEN** the user executes `FT.ALTER idx SKIPINITIALSCAN GC SET RUN_INTERVAL 100`
- **THEN** the command SHALL return an error

#### Scenario: Unknown action
- **WHEN** the user executes `FT.ALTER idx FOO a b c`
- **THEN** the error SHALL name both `SCHEMA` and `GC`

#### Scenario: Alias argument
- **WHEN** `alias` points to `idx`
- **AND** the user executes `FT.ALTER alias GC SET RUN_INTERVAL 100`
- **THEN** `idx` SHALL get the override
- **AND** an index that `alias` points to later SHALL NOT get it

#### Scenario: Flex node
- **WHEN** the node runs Redis Flex
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 100`, on a shard or through
  the coordinator
- **THEN** the command SHALL return an error
- **AND** `idx` SHALL have no override

### Requirement: Indexes inherit global GC settings unless overridden
An index without an override for a setting SHALL use the current value of the matching
global setting (`search-fork-gc-run-interval`, `search-fork-gc-clean-threshold`, or
(T5) `search-fork-gc-clean-threshold-percent`).
`DEFAULT` SHALL remove the override. Global values SHALL NOT be copied into an index.

#### Scenario: Inherited value follows CONFIG SET
- **WHEN** `idx` has no clean threshold override
- **AND** the user executes `CONFIG SET search-fork-gc-clean-threshold 10`
- **THEN** `idx` SHALL use a clean threshold of 10

#### Scenario: DEFAULT clears an override
- **WHEN** `idx` has `RUN_INTERVAL 100`
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL DEFAULT`
- **THEN** `idx` SHALL use the global run interval and follow later changes to it

### Requirement: Value ranges match the global settings
`RUN_INTERVAL` SHALL accept 1 to 2^63−1 seconds, `CLEAN_THRESHOLD` SHALL accept 0 to
2^63−1, and (T5) `CLEAN_THRESHOLD_PERCENT` SHALL accept whole numbers from 0 to 100. A
scheduled GC cycle SHALL be skipped while the number of documents deleted or updated
since the last cycle is below the combined clean threshold defined below, so with a
clean threshold of 0 every scheduled cycle forks.

#### Scenario: Very large interval
- **WHEN** the run-interval timer of `idx` is waiting
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 9223372036854775807`
- **THEN** the reply SHALL be `OK`
- **AND** the GC timer of `idx` SHALL be armed for the longest supported delay, with no
  arithmetic overflow

#### Scenario: Percentage out of range (T5)
- **WHEN** the user executes `FT.ALTER idx GC SET CLEAN_THRESHOLD_PERCENT 101`, or gives a
  negative or fractional percentage
- **THEN** the command SHALL return an error

### Requirement: The clean threshold can be a percentage of the index (T5)
With `N` the effective clean threshold and `p` the effective clean threshold
percentage, a scheduled cycle SHALL fork once the index's deletes and updates since the
last cycle reach the combined threshold. When `p` is 0 the combined threshold SHALL be
`N`. Otherwise it SHALL be `min(N, max(1, ceil(p × base / 100)))`, where `base` is the
largest live-document count of the index sampled since the count was last reset. The
live count SHALL be sampled when the count is reset, by a forced or a scheduled cycle,
and at least each time a scheduled run is queued. Both counts SHALL be the index's own.

#### Scenario: Each index reaches a different threshold
- **WHEN** the global clean threshold is 90 and the global percentage is 60
- **AND** `idx1` has 100 documents and 70 of them were updated since its last cycle
- **AND** `idx2` has 200 documents and 100 of them were updated since its last cycle
- **THEN** the next scheduled cycle of `idx1` SHALL fork, because 70 reaches 60% of 100
- **AND** the next scheduled cycle of `idx2` SHALL fork, because 100 reaches 90

#### Scenario: Deletes count against the size before the deletes
- **WHEN** `idx` had 100 live documents at its last cycle and has `CLEAN_THRESHOLD_PERCENT 60`
  with a clean threshold of 1000
- **AND** 50 documents were deleted since, leaving 50
- **THEN** the next scheduled cycle SHALL NOT fork

#### Scenario: Growth that is deleted again
- **WHEN** `idx` had 100 live documents at its last cycle and has `CLEAN_THRESHOLD_PERCENT 60`
  with a clean threshold of 1000
- **AND** 100 documents were added, and a scheduled run was queued and skipped by the
  threshold
- **AND** 100 documents were deleted after that, leaving 100
- **THEN** the next scheduled cycle SHALL NOT fork, because 100 is 50% of 200

#### Scenario: Deletes collected by the fork that resets the count
- **WHEN** `idx` has 1000 live documents when a scheduled run is queued, and has
  `CLEAN_THRESHOLD_PERCENT 60` with a clean threshold of 1000
- **AND** 900 documents are deleted before that run forks, leaving 100
- **AND** after the fork, 60 more documents are deleted
- **THEN** the next scheduled cycle SHALL fork, because 60 reaches 60% of 100

#### Scenario: Forced cycle resets the window
- **WHEN** `idx` has `CLEAN_THRESHOLD_PERCENT 60` with a clean threshold of 1000
- **AND** a forced cycle forks while `idx` has 100 live documents
- **AND** after it, 60 documents are deleted
- **THEN** the next scheduled cycle SHALL fork, because 60 reaches 60% of 100

#### Scenario: Forced cycle after a scheduled run is queued
- **WHEN** `idx` has `CLEAN_THRESHOLD_PERCENT 60` with a clean threshold of 1000
- **AND** a scheduled run is queued while `idx` has 1000 live documents
- **AND** before that run starts, a forced cycle forks while `idx` has 100 live
  documents, and 60 documents are then updated
- **THEN** the queued run SHALL fork, because 60 reaches 60% of 100

#### Scenario: Zero percent with a larger clean threshold
- **WHEN** `idx` has `CLEAN_THRESHOLD_PERCENT 0` and a clean threshold of 100
- **AND** 1 document was deleted since its last cycle
- **THEN** the next scheduled cycle SHALL NOT fork

#### Scenario: Empty index
- **WHEN** `idx` has no documents, nothing was deleted or updated since its last cycle,
  and it has `CLEAN_THRESHOLD_PERCENT 10`
- **THEN** the next scheduled cycle SHALL NOT fork

#### Scenario: Percentage off by default
- **WHEN** neither `idx` nor the global setting sets a percentage
- **THEN** `idx` SHALL fork exactly as it would with its clean threshold alone

### Requirement: Interval changes restart a waiting timer
When `FT.ALTER ... GC` sets or clears the run interval of an index while its
run-interval timer is waiting, and the new effective interval differs from the interval
that timer was armed with, the timer SHALL restart from the command, with the new
interval and new jitter. When the new effective interval equals the armed one (the same
value again, or `DEFAULT` when the global value equals it), the timer SHALL NOT restart.
The command SHALL NOT start a GC run. When no run-interval timer is waiting (a cycle
queued or running, GC stopped or paused, or the waiting timer is a fork retry), the next
run-interval arm SHALL use the latest effective interval.

A change to the effective clean threshold or percentage SHALL apply to scheduled runs
queued after it. A run is queued when its timer fires. A run that is already queued or
running, forked or not, SHALL keep the values it started with. So raising the threshold
while a run is queued can let that run fork once more, and lowering it applies one run
later.
A global `CONFIG SET` SHALL NOT change any waiting timer.

#### Scenario: Lowering a long interval
- **WHEN** the GC timer of `idx` was armed 50 seconds ago with a run interval of 1000
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 10`
- **THEN** the timer of `idx` SHALL restart, with more than 9 seconds and less than 20
  seconds left

#### Scenario: Global change does not change a waiting timer
- **WHEN** `idx` has no run interval override and its timer is waiting
- **AND** the user executes `CONFIG SET search-fork-gc-run-interval 1`
- **THEN** the timer of `idx` SHALL NOT restart
- **AND** the next run-interval arm of `idx` SHALL use an interval of 1

#### Scenario: FT.ALTER after a global change restarts the timer
- **WHEN** `idx` has no run interval override and its run-interval timer was armed with
  a global interval of 1000
- **AND** the user executes `CONFIG SET search-fork-gc-run-interval 10`
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 10`
- **THEN** the timer of `idx` SHALL restart, with less than 20 seconds left

#### Scenario: Repeating the command does not postpone GC
- **WHEN** `idx` has `RUN_INTERVAL 100` and its run-interval timer, armed with 100, is
  waiting
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 100` again
- **THEN** the timer of `idx` SHALL NOT restart

#### Scenario: DEFAULT with an override equal to the global value
- **WHEN** `idx` has a run interval override equal to the global run interval and its
  run-interval timer, armed with that value, is waiting
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL DEFAULT`
- **THEN** the timer of `idx` SHALL NOT restart

#### Scenario: DEFAULT with a different override
- **WHEN** `idx` has a run interval override that differs from the global run interval
  and its run-interval timer, armed with the override, is waiting
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL DEFAULT`
- **THEN** the timer of `idx` SHALL restart from the command with the global run
  interval

#### Scenario: A queued run keeps its threshold
- **WHEN** a scheduled GC run for `idx` is queued but has not started
- **AND** the user changes the clean threshold of `idx`
- **THEN** that run SHALL use the threshold in effect when it was queued
- **AND** the next run queued for `idx` SHALL use the new threshold

### Requirement: Only a failed fork keeps an index on the retry interval
After a failed scheduled fork, the next timer SHALL use the fork retry interval. A
scheduled cycle that skips the fork because the count is below the threshold SHALL arm
the next timer with the run interval, whether or not the previous fork failed.

#### Scenario: Raising the threshold after a failed fork
- **WHEN** a scheduled fork of `idx` failed and its retry timer is waiting
- **AND** the clean threshold of `idx` is raised above its count
- **THEN** the retry cycle SHALL NOT fork
- **AND** the next timer of `idx` SHALL use the run interval, not the retry interval

### Requirement: Overrides are durable and consistent across nodes
Overrides SHALL be saved in the RDB, replicated, and written to the AOF. An index created
on a shard by slot migration SHALL arrive with its overrides. Every node SHALL store an
override, including nodes started with
`search-no-gc`, where it SHALL have no effect. Dropping an index SHALL drop its
overrides. On a cluster, the coordinator SHALL send `FT.ALTER ... GC` to every shard
and SHALL return `OK` only when every shard applies it. A shard that rejects it, for
example under `deny-oom` or on a version without the command, SHALL leave the other
shards' changes in place, and the coordinator SHALL return an error. Running the same
command again SHALL bring every shard to the same overrides.

#### Scenario: A shard rejects the command
- **WHEN** one shard of a cluster rejects `FT.ALTER idx GC SET RUN_INTERVAL 100` and the
  others apply it
- **THEN** the coordinator SHALL return an error
- **AND** the shards that applied it SHALL keep `RUN_INTERVAL 100`
- **AND** once the rejecting shard accepts writes again, running the same command SHALL
  give every shard `RUN_INTERVAL 100`

#### Scenario: Survives restart
- **WHEN** `idx` has `CLEAN_THRESHOLD 0`
- **AND** the node restarts from its RDB
- **THEN** `idx` SHALL still have `CLEAN_THRESHOLD 0`

#### Scenario: Node without GC
- **WHEN** the node was started with `search-no-gc`
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 100`
- **THEN** the reply SHALL be `OK`
- **AND** FT.INFO SHALL report the override

#### Scenario: Recreated index starts without overrides
- **WHEN** `idx` with overrides is dropped and created again
- **THEN** the new `idx` SHALL have no overrides

#### Scenario: Older RDB
- **WHEN** a node loads an RDB written with index encoding version 29 or lower
- **THEN** every index SHALL load with no overrides

#### Scenario: Setting from a newer version
- **WHEN** a node loads an RDB whose override block holds a setting it does not know
- **THEN** the load SHALL succeed
- **AND** the index SHALL keep every override the node knows, ignoring the unknown one

#### Scenario: Known setting with an invalid value
- **WHEN** a node loads an RDB whose override block holds a known setting with a value
  outside its range, such as a run interval of 0
- **THEN** the load SHALL fail as corrupt

### Requirement: TEMPORARY indexes reject GC overrides
`FT.ALTER ... GC` on a TEMPORARY index SHALL return an error and change nothing.

#### Scenario: TEMPORARY index
- **WHEN** `tmp` was created with `TEMPORARY`
- **AND** the user executes `FT.ALTER tmp GC SET RUN_INTERVAL 100`
- **THEN** the command SHALL return an error

#### Scenario: FT.INFO of a TEMPORARY index
- **WHEN** `tmp` was created with `TEMPORARY`
- **THEN** FT.INFO of `tmp` SHALL have no `gc_settings` section

### Requirement: FT.INFO reports GC settings
FT.INFO SHALL include a `gc_settings` map with `run_interval`, `clean_threshold` and
(T5) `clean_threshold_percent`. Each
SHALL be a map with `configured` (the override, or null when inherited) and `effective`
(the value in use), both integers. A null `configured` SHALL be a RESP3 null and a RESP2
nil bulk string. The section SHALL be present for every index except TEMPORARY indexes,
in RESP2 and RESP3, standalone and through the coordinator, which SHALL report the first
shard's section. On a disk index, `gc_settings` SHALL NOT include
`clean_threshold_percent`, because disk GC ignores the percentage.

#### Scenario: Inherited setting
- **WHEN** `idx` has no run interval override and the global run interval is 30
- **THEN** FT.INFO SHALL report `run_interval` with `configured` null and `effective` 30

#### Scenario: Overridden setting
- **WHEN** `idx` has `CLEAN_THRESHOLD 0`
- **THEN** FT.INFO SHALL report `clean_threshold` with `configured` 0 and `effective` 0

#### Scenario: Disk index (T5)
- **WHEN** the node runs Redis Flex and the global clean threshold percentage is 10
- **THEN** FT.INFO of a disk index SHALL have no `clean_threshold_percent` in
  `gc_settings`
