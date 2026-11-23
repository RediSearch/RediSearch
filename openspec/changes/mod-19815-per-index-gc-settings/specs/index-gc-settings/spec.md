# index-gc-settings (delta)

Parts marked (T5) depend on Product sign-off on the per-index clean ratio (decision 13,
MOD-20691). Until T5 ships, `CLEAN_RATIO_PPM`, `search-fork-gc-clean-ratio-ppm` and
`clean_ratio_ppm` do not exist; under THREAD the clean ratio is
`search-gc-clean-ratio-ppm`, and under FORK there is none.

The GC policy is fixed at load (`search-gc-policy`); THREAD is the default. Each
setting below falls back to the active policy's global:

| Setting | THREAD global | FORK global |
| --- | --- | --- |
| `RUN_INTERVAL` | `search-thread-gc-run-interval` | `search-fork-gc-run-interval` |
| `CLEAN_THRESHOLD` | `search-thread-gc-clean-threshold` | `search-fork-gc-clean-threshold` |
| `CLEAN_RATIO_PPM` (T5) | `search-gc-clean-ratio-ppm` | `search-fork-gc-clean-ratio-ppm` |

## ADDED Requirements

### Requirement: FT.ALTER sets per-index GC overrides
`FT.ALTER <index> GC SET [RUN_INTERVAL <seconds|DEFAULT>]
[CLEAN_THRESHOLD <count|DEFAULT>] [CLEAN_RATIO_PPM <ppm|DEFAULT>]` SHALL set or clear the
index's GC run interval, clean threshold and (T5) clean ratio, under either GC policy.
`SET` SHALL be required. It SHALL take at least one setting, each at most once, in any
order. It SHALL change no schema and trigger no reindex. `FT.ALTER` SHALL keep its
declared arity of at least six words. This requirement covers RAM indexes. On a Flex
(disk) node, `FT.ALTER ... GC` SHALL return an error and change nothing, as every
`FT.ALTER` does there today; task 6 replaces this with the Flex behavior.

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

### Requirement: Indexes inherit the active policy's global GC settings unless overridden
An index without an override for a setting SHALL use the current value of the active
policy's matching global setting, from the table above. An override SHALL apply under
either policy. `DEFAULT` SHALL remove the override. Global values SHALL NOT be copied
into an index.

#### Scenario: Inherited value follows CONFIG SET
- **WHEN** the node runs THREAD and `idx` has no clean threshold override
- **AND** the user executes `CONFIG SET search-thread-gc-clean-threshold 10`
- **THEN** `idx` SHALL use a clean threshold of 10

#### Scenario: The FORK policy's globals
- **WHEN** the node runs FORK and `idx` has no run interval override
- **THEN** `idx` SHALL use `search-fork-gc-run-interval`
- **AND** a change to `search-thread-gc-run-interval` SHALL NOT change it

#### Scenario: DEFAULT clears an override
- **WHEN** `idx` has `RUN_INTERVAL 100`
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL DEFAULT`
- **THEN** `idx` SHALL use the active policy's global run interval and follow later
  changes to it

### Requirement: Under THREAD the clean ratio applies on top of the clean threshold
Under THREAD, a scheduled cycle SHALL run only when the index's deletes and updates not
yet collected reach both its effective clean threshold and its effective clean ratio of
its live documents. A per-index `CLEAN_THRESHOLD` SHALL replace only the absolute
threshold; the ratio SHALL still apply. Until T5, the ratio SHALL be
`search-gc-clean-ratio-ppm` for every index.

#### Scenario: A threshold override does not lower the bar below the ratio
- **WHEN** the node runs THREAD with `search-gc-clean-ratio-ppm` 1000
- **AND** `idx` has 1,000,000 live documents and `CLEAN_THRESHOLD 10`
- **AND** 500 documents were deleted since its last cycle
- **THEN** the next scheduled cycle SHALL NOT clean, because 500 is below 1000 ppm of
  1,000,000
- **AND** once 1000 documents are deleted, the next scheduled cycle SHALL clean

### Requirement: Value ranges
`RUN_INTERVAL` SHALL accept 1 to `GC_MAX_INTERVAL_SEC` (2305843009213) seconds, the same
range as `search-thread-gc-run-interval`. `CLEAN_THRESHOLD` SHALL accept 0 to 2^63−1, and
(T5) `CLEAN_RATIO_PPM` SHALL accept 0 to 1,000,000. A scheduled GC cycle SHALL be
skipped while the number of documents deleted or updated since the last cycle is below
the clean threshold, or below the clean ratio where one applies. So with a clean
threshold of 0 and no ratio, every scheduled cycle cleans.

#### Scenario: Longest interval
- **WHEN** the run-interval timer of `idx` is waiting
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 2305843009213`
- **THEN** the reply SHALL be `OK`
- **AND** the GC timer of `idx` SHALL be armed for that interval, in steps where one
  Redis module timer can't wait that long, with no arithmetic overflow

#### Scenario: Interval above the limit
- **WHEN** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 2305843009214`
- **THEN** the command SHALL return an error

#### Scenario: Ratio out of range (T5)
- **WHEN** the user executes `FT.ALTER idx GC SET CLEAN_RATIO_PPM 1000001`, or gives a
  negative or fractional ratio
- **THEN** the command SHALL return an error

### Requirement: The clean ratio can be set per index (T5)
With `N` the effective clean threshold and `r` the effective clean ratio, in parts per
million, a scheduled cycle SHALL clean only when the index's deletes and updates not yet
collected reach `N` and, when `r` is not 0, also `r × live / 1,000,000`, where `live` is
the index's live-document count at the check. This rule SHALL apply under both
policies. An unset ratio SHALL inherit the active policy's global from the table above.
`search-fork-gc-clean-ratio-ppm` SHALL default to 0.

#### Scenario: A ratio override lowers the bar on a large index
- **WHEN** the node runs THREAD with a clean threshold of 100 and
  `search-gc-clean-ratio-ppm` 1000
- **AND** `idx` has 1,000,000 live documents and `CLEAN_RATIO_PPM 200`
- **AND** 300 documents were deleted since its last cycle
- **THEN** the next scheduled cycle SHALL clean, because 300 reaches both 100 and 200 ppm
  of 1,000,000

#### Scenario: Ratio 0 leaves the clean threshold alone
- **WHEN** `idx` has `CLEAN_RATIO_PPM 0` and a clean threshold of 100
- **AND** `idx` has 1,000,000 live documents and 100 were deleted since its last cycle
- **THEN** the next scheduled cycle SHALL clean

#### Scenario: Ratio under FORK
- **WHEN** the node runs FORK with a clean threshold of 100
- **AND** `idx` has 20,000 live documents and `CLEAN_RATIO_PPM 10000`
- **AND** 150 documents were deleted since its last cycle
- **THEN** the next scheduled cycle SHALL NOT fork, because 150 is below 10000 ppm of
  20,000
- **AND** once 200 documents are deleted, the next scheduled cycle SHALL fork

#### Scenario: FORK unchanged by default
- **WHEN** the node runs FORK and neither `idx` nor `search-fork-gc-clean-ratio-ppm`
  sets a ratio
- **THEN** `idx` SHALL fork exactly as it would with its clean threshold alone

#### Scenario: Inherited ratio follows the active policy
- **WHEN** `idx` has no ratio override
- **THEN** under THREAD it SHALL use `search-gc-clean-ratio-ppm`, and under FORK
  `search-fork-gc-clean-ratio-ppm`

#### Scenario: Empty index
- **WHEN** `idx` has no live documents and a clean threshold of 100
- **AND** 100 documents were deleted since its last cycle
- **THEN** the next scheduled cycle SHALL clean, whatever its ratio

### Requirement: Interval changes restart a waiting timer
When `FT.ALTER ... GC` sets or clears the run interval of an index while its
run-interval timer is waiting, including one waiting between steps, and the new
effective interval differs from the interval that timer was armed with, the timer SHALL
restart from the command with the new interval, and new jitter where the policy draws
one. When the new effective interval equals the armed one (the same value again, or
`DEFAULT` when the global value equals it), the timer SHALL NOT restart. The command
SHALL NOT start a GC run. When no run-interval timer is waiting (a cycle queued or
running, GC stopped or paused, or the waiting timer is a fork retry), the next
run-interval arm SHALL use the latest effective interval.

A change to the effective clean threshold or (T5) ratio SHALL apply to scheduled runs
queued after it. A run is queued when its timer fires. A run that is already queued or
running, cleaning or not, SHALL keep the values it started with. So raising the
threshold while a run is queued can let that run clean once more, and lowering it
applies one run later.

A global `CONFIG SET` SHALL NOT restart any waiting timer. A timer waiting between
steps SHALL use the new value from its next step.

#### Scenario: Lowering a long interval
- **WHEN** the GC timer of `idx` was armed 50 seconds ago with a run interval of 1000
- **AND** the user executes `FT.ALTER idx GC SET RUN_INTERVAL 10`
- **THEN** the timer of `idx` SHALL restart, with more than 9 seconds and less than 20
  seconds left

#### Scenario: Global change does not change a waiting timer
- **WHEN** the node runs THREAD, `idx` has no run interval override, and its timer is
  waiting
- **AND** the user executes `CONFIG SET search-thread-gc-run-interval 1`
- **THEN** the timer of `idx` SHALL NOT restart
- **AND** the next run-interval arm of `idx` SHALL use an interval of 1

#### Scenario: Global change reaches a timer waiting between steps
- **WHEN** `idx` has no run interval override and its timer, armed with an interval
  longer than one Redis module timer can wait, is waiting between steps
- **AND** the user lowers the active policy's global run interval
- **THEN** at its next step the timer of `idx` SHALL be armed afresh with the new
  interval

#### Scenario: FT.ALTER after a global change restarts the timer
- **WHEN** the node runs THREAD, `idx` has no run interval override, and its
  run-interval timer was armed with a global interval of 1000
- **AND** the user executes `CONFIG SET search-thread-gc-run-interval 10`
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

### Requirement: Only a failed scheduled fork keeps an index on the retry interval
After a failed scheduled fork, the next timer SHALL use the fork retry interval. A
forced run SHALL NOT change which interval the next timer uses. A scheduled cycle that
skips because the count is below the threshold, and a scheduled THREAD cycle, SHALL arm
the next timer with the run interval, whether or not the previous fork failed.

#### Scenario: Raising the threshold after a failed fork
- **WHEN** a scheduled fork of `idx` failed and its retry timer is waiting
- **AND** the clean threshold of `idx` is raised above its count
- **THEN** the retry cycle SHALL NOT fork
- **AND** the next timer of `idx` SHALL use the run interval, not the retry interval

#### Scenario: A forced fork fails
- **WHEN** the run-interval timer of `idx` is waiting
- **AND** a forced run of `idx` fails to fork
- **THEN** the next timer of `idx` SHALL use the run interval

### Requirement: Overrides are durable and consistent across nodes
Overrides SHALL be saved in the RDB, replicated, and written to the AOF. An index created
on a shard by slot migration SHALL arrive with its overrides. Every node SHALL store an
override, including nodes started with `search-no-gc`, where it SHALL have no effect.
Dropping an index SHALL drop its overrides. On a cluster, the coordinator SHALL send
`FT.ALTER ... GC` to every shard and SHALL return `OK` only when every shard applies it.
A shard that rejects it, for example under `deny-oom` or on a version without the
command, SHALL leave the other shards' changes in place, and the coordinator SHALL
return an error. Running the same command again SHALL bring every shard to the same
overrides. These guarantees hold only once every node is upgraded: an older shard drops
the overrides of an index it receives by slot migration, and an older binary can't load
the RDB.

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
- **WHEN** a node loads an RDB written with index encoding version 31 or lower
- **THEN** every index SHALL load with no overrides

#### Scenario: Setting from a newer version
- **WHEN** a node loads an RDB whose override block holds a setting it does not know,
  with a value of any size
- **THEN** the load SHALL succeed
- **AND** the index SHALL keep every override the node knows, ignoring the unknown one
- **AND** the node SHALL log one notice for the whole load

#### Scenario: Known setting with an invalid value
- **WHEN** a node loads an RDB whose override block holds a known setting with a value
  of the wrong size, or outside its range, such as a run interval of 0
- **THEN** the load SHALL fail as corrupt

#### Scenario: Setting given twice
- **WHEN** a node loads an RDB whose override block holds the same known setting twice
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
(T5) `clean_ratio_ppm`. Each SHALL be a map with `configured` (the override, or null
when inherited) and `effective` (the value in use: the override, or the active policy's
global), both integers. A null `configured` SHALL be a RESP3 null and a RESP2 nil bulk
string. The section SHALL be present for every index except TEMPORARY indexes, in RESP2
and RESP3, standalone and through the coordinator, which SHALL report the first shard's
section. On a disk index, `gc_settings` SHALL NOT include `clean_ratio_ppm`, because
disk GC ignores the ratio.

#### Scenario: Inherited setting
- **WHEN** the node runs THREAD, `idx` has no run interval override, and
  `search-thread-gc-run-interval` is 7
- **THEN** FT.INFO SHALL report `run_interval` with `configured` null and `effective` 7

#### Scenario: Overridden setting
- **WHEN** `idx` has `CLEAN_THRESHOLD 0`
- **THEN** FT.INFO SHALL report `clean_threshold` with `configured` 0 and `effective` 0

#### Scenario: Disk index (T5)
- **WHEN** the node runs Redis Flex
- **THEN** FT.INFO of a disk index SHALL have no `clean_ratio_ppm` in `gc_settings`
