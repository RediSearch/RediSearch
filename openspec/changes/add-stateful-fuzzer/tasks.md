# Tasks: stateful, coverage-guided fuzzer

Each top-level task is roughly one reviewable PR. The **Done when** line is the
acceptance check for that PR.

## 0. Setup

- [x] 0.1 `fuzz/setup.sh`: submodules, AddressSanitizer `redis-server` at the CI-pinned ref, AddressSanitizer module, `bin/fuzz/env.sh`
- [x] 0.2 Setup section in `fuzz/README.md`, including prerequisites, runtime environment, and known pitfalls
- [ ] 0.3 Run `fuzz/setup.sh` on Linux with clang and record any differences

## 1. Target and replay

- [x] 1.1 Create the `fuzz/` crate with `Program`, `Bytes`, and the versioned JSON format
- [x] 1.2 Implement `lower` for `Create`, `Query`, and `Command`
- [x] 1.3 Implement `target`: spawn in a process group with a unix socket, log capture, connections with deadlines
- [x] 1.4 Implement the interpreter lifecycle and `Exit`, including the release list and the control connection
- [x] 1.5 Implement the safety judge and signatures
- [x] 1.6 Add the `run <file>` command and the finding bundle
- [x] 1.7 Unit tests: JSON round-trip, `lower` against expected argv, signature extraction from logs
- [x] 1.8 Harness self-test programs: a segfault, a hang, and a stall the release resolves
- **Done when:** a crashing program yields the same signature ten times out of ten, and server startup time under AddressSanitizer is recorded in the PR. If startup exceeds one second, open a follow-up for forking test cases from a loaded server.
- **Status:** the panic and segfault self-tests reproduce ten times out of ten. With the module loaded, the AddressSanitizer server is ready in about 1.6 s on a cold start and well under a second when warm. The bugs the earlier fuzzer found are fixed on master, so sanitizer, assertion, and leak signatures are so far tested only against logs in the documented formats; task 5.0 adds a real one.

## 2. Scheduling and multiple connections

- [x] 2.1 Implement `ReplyMode::Defer` with send and receive sequence numbers
- [x] 2.2 Drive the `FT.DEBUG` scheduling hooks, `DEBUG RELOAD`, and `CONFIG SET` as `Command` ops
- [x] 2.3 Implement `Barrier` with its polling predicates and timeout
- [x] 2.4 Tests: a released stall versus `Hung` on a program that never resumes workers; barrier timeout is not a finding
- **Done when:** a seed with workers enabled holds a query pending behind paused workers while another connection drops the index, and completes with exit `Clean`.
- **Status:** done; `tests/programs/drop-under-pending-query.json`. The pending query is answered with `SEARCH_INDEX_DROPPED_BG` after the drop. `TTL_PAUSE` and `TTL_EXPIRE` apply only to temporary indexes, so their hooks arrive with temporary-index support in 7.2.

## 3. Coverage

- [x] 3.1 Add `FUZZ_COV` to `build.sh` and CMake: imply `SAN=address`, force clang, reject test-building options, separate output and Rust target directories
- [x] 3.2 Add the coverage shim with the header, capacity check, and edge-presence writes
- [x] 3.3 Create, zero, and read the segment in `target`; read only after the process group is reaped
- [ ] 3.4 Add an optional guard-to-address dump for reporting unreached code (deferred until a report needs it)
- [ ] 3.5 Verify the build and symbol binding on Linux with clang
- **Done when:** two different seeds produce different edge sets, a forced-GC seed adds edges not seen without it, and ten runs of one seed report the fraction of unstable edges on both macOS and Linux.
- **Status (macOS arm64):** the module has 718,275 guards. A tag-only program reaches 3,881 edges and the smoke seed 5,720. The same writes and deletes reach 5,086 edges without `GC_FORCEINVOKE` and 6,562 with it. Over ten smoke runs, 5,721 edges appear in any run and 5,718 in every run, so 3 are unstable. A run including server startup takes about 0.4 s. Linux is not yet verified.

## 4. Generator and edits

- [x] 4.1 Implement `Scope` and the value pools
- [x] 4.2 Implement `generate`
- [x] 4.3 Implement the edit set, `mutate`, and `smaller`
- [x] 4.4 Property tests: every edit yields a program that round-trips; every `smaller` candidate strictly decreases a size measure
- **Done when:** at least 80% of replies are not errors across 200 generated programs run against a live server.
- **Status:** about 97.5% of replies are not errors (93 errors across 200 programs; the live test checks 50). 190 programs run clean; the other 10 hit one module bug, a heap-buffer-overflow in `DebugIndexes_ScanProc` (`indexes_scan.c`): `_FT.DEBUG BG_SCAN_CONTROLLER SET_PAUSE_BEFORE_SCAN` turns on debug mode while a scan created with the plain scanner is queued, and the scan then reads it as the larger debug scanner. It is reachable only through `_FT.DEBUG`. Stalls no longer end a program: the harness releases its controls, records a `RELEASE` step, and carries on, and it waits only a short settle time before releasing.

## 5. Reduce

- [x] 5.0 Planted module bug: a test-only patch kept beside its seed, built into a separate module, giving a real sanitizer report and a real assertion
- [x] 5.1 Implement `reduce` with signature preservation and the reproduction rate
- [x] 5.2 Write the reduced program with the original into the finding bundle
- **Done when:** the planted-bug seed padded with 200 generated ops reduces to at most five ops with the same signature.
- **Status:** `reduce` is done and checked on a real finding: a 33-op generated program hitting the racy `DebugIndexes_ScanProc` overflow reduced to 5 ops in 4 minutes, reproducing four times out of ten afterwards (three of three before). Each planted seed, renamed apart and spread through 200 generated ops (with `FT.DROPINDEX` removed from the padding, which otherwise hit the planted assertion by itself), reduced to at most five ops with the same signature, reproducing ten times out of ten: the overflow from 205 ops to 5 in 3.7 minutes, the assertion from 204 to 4 in 6.5 minutes.

## 6. Campaign

- [x] 6.1 Implement `Input` and `HasLen` for `Program`, the executor, and the two observers
- [x] 6.2 Wire feedback, objective, scheduler, the capped mutational stage, and known signatures (a finding's directory marks it known)
- [x] 6.3 Add the `fuzz` command with a corpus directory and a findings directory
- [x] 6.4 If not running within five working days, replace with the hand-written loop behind the same interface (not needed: the LibAFL wiring ran on the first day)
- **Status:** three 30-minute single-worker campaigns from `seeds/` against the planted module each found the planted overflow (after 2, 5, and 30 s), the planted assertion (35 to 79 s), and the real `DebugIndexes_ScanProc` overflow (268 to 505 s). Each reached about 11,000 edges, against 5,720 for the smoke seed, at 1.3 programs per second. The planted bugs are close to the smoke seed, which already has a sortable numeric field, a TEXT field, and a TAG field; the real bug is the stronger signal. With `--jobs 12` on a 12-core machine, a campaign runs 12.4 programs per second.
- **Done when:** from the committed seeds, three runs out of three find the planted bug within thirty minutes on one core, and edge count exceeds the seeds' own.

## 7. Depth operations

- [x] 7.1 `Populate` with the golden expansion test
- [x] 7.2 `Alter`, aggregations with cursors bound to variables, cursor reads and deletes
- [x] 7.3 Vector fields, JSON documents, geo fields
- [x] 7.4 Seeds for each
- **Done when:** each new operation produces non-error replies in a seed and adds edges not reached by the earlier seeds.
- **Status:** `Populate` is done; `seeds/populate.json` writes 500 documents, queries, deletes, and forces a GC, reaching 8,056 edges against 5,720 for the smoke seed. A live test checks that every seed runs without error replies and reaches edges no other seed does. `seeds/aggregate.json` covers `Aggregate` with grouping, reducers, sorting, `APPLY`, `FILTER`, and cursors read and deleted through variables, and `Alter`. `seeds/vectors.json` covers FLAT and HNSW vector fields with KNN searches and aggregates, filtered and unfiltered, before and after a forced GC; it reaches 11,688 edges. `seeds/json.json` covers a JSON index with text, numeric, tag, and vector fields, partial document updates, KNN, and aggregation, reaching about 10,400 edges. RedisJSON is built by `setup.sh` the way the Python tests build it and loaded into every server; on macOS without a sanitizer, because RedisJSON's sanitizer build supports only Linux.

## 8. Logic rules

- [x] 8.1 Reply-confirmed scope in the judge, the skip conditions, and the allow-list of commands that cannot change results
- [x] 8.2 Twin rule and the twin-index generator bias
- [x] 8.3 Equivalence rule
- [x] 8.4 Partition rule and the related-query edit
- [x] 8.5 For each rule: a seed that passes and a doctored trace that fails
- **Done when:** 1,000 generated runs produce no untriaged logic finding.
- **Status:** 1,000 generated programs, rerun after each fix, end with no logic finding. The first run gave 26 partition findings, all false: dialect 1 parses `-(a) b` as `-(a b)`, and drops clauses on unknown fields and terms the tokenizer splits. Rendering negations as `(-(…))` and skipping vanishing terms and unknown fields resolved all of them. `seeds/logic.json` checks a partition over a term and over a range, equivalence across a forced GC and across `DEBUG RELOAD`, and a twin created after the documents, against a live server. Unit tests check each rule against consistent and doctored traces, and that an intervening write, a reload without a barrier, or the default timeout policy suppresses it. The same runs found a leak in the KNN score path and a deadlock in `FT.DEBUG WORKERS PAUSE`, recorded for later filing.

## 9. Scale-out

- [x] 9.1 Run several campaigns at once: `fuzz --jobs N` runs N worker threads with separate corpora and one findings directory. Sharing programs between workers was tried and removed (11.12).
- [x] 9.2 Seeds recorded from Python tests with `MONITOR` and read with `import`: synonyms, spellcheck, suggestions, summarize, phonetics, field expiry, profile, hybrid
- [x] 9.3 `export` prints a program as `redis-cli` lines; `import` reads them back
- **Done when:** four workers find the planted bugs sooner than one in the same wall time.
- **Status:** planted module, the four seeds that trigger no planted bug, 240 s per trial. Seconds until both planted bugs were found: one worker 61, 196, 34; four workers 20, 13 (a third four-worker trial ran alongside leftover servers and is not counted). Each worker ran about as many programs as a lone one. The four-worker trials also found a fuzzer false positive: a deferred write whose reply was read after a search had not counted as disturbing it. Fixed.

## 10. Docs and spec delta

- [x] 10.1 A pointer from `CONTRIBUTING.md` to `fuzz/README.md`, which covers building with `FUZZ_COV`, running a campaign, and replaying a finding
- [x] 10.2 Update the delta spec under `specs/stateful-fuzzer/spec.md` to match what shipped
- [x] 10.3 No shipped behaviour changes, so the PR ticks "does not require release notes"

## 11. Review: a model-driven core

An external review asked for a core that assumes no Redis, RESP, or single
process, with the RediSearch layer reduced to what is RediSearch-specific.

- [x] 11.1 Fix the bugs that survive the redesign: a worker failure stops the campaign at once; SIGINT and SIGTERM stop cleanly; an empty RESP line is a protocol violation; poll exit every millisecond and size the edge map once
- [x] 11.2 Add `Draw`, `Model`, `System`, `Session`, `Program`, `Run`, and draw-based `mutate` and `shrink`, tested against an in-memory store
- [x] 11.3 Hand `check` the state each completed step saw
- [x] 11.4 Add `Service`, the sanitizer probe, and `procfuzz-redis` with the RESP2 codec and Redis crash reports
- [x] 11.5 Add `kvd` with three planted bugs as `procfuzz`'s own target
- [x] 11.6 Ship the commands in `procfuzz` and the clap subcommand behind the `cli` feature
- [x] 11.7 Port RediSearch to `Model`, with one table of command effects
- [x] 11.8 Delete the first core
- [x] 11.9 Put the engine behind one function from program to features, with a plain loop beside LibAFL, and add a stability check before counting a new feature
- [x] 11.10 Record a reproduction rate for every finding the campaign writes
- [x] 11.11 Rerun the engine comparison on RediSearch, at least 8 trials per setup, before choosing an engine or a per-feature score
- **Measured (RediSearch, planted module, the four seeds that do not hit a planted bug, 8 trials of 500 runs per setup, reruns counted):**

  | Setup | Kept | Planted overflow | Planted assertion | `DebugIndexes_ScanProc` | `TrieMap_Find` crash |
  |---|---|---|---|---|---|
  | `libafl` | 168 | 8/8, median run 30 | 8/8, median 157 | 4/8 | 2/8 |
  | `libafl+confirm` | 121 | 8/8, median 16 | 8/8, median 37 | 5/8 | 1/8 |
  | `basic+confirm` | 126 | 8/8, median 34 | 8/8, median 133 | 3/8 | 0/8 |

  Confirming new features keeps 28% fewer programs and, although its reruns count against the budget, reaches both planted bugs in about half the runs or fewer. LibAFL with confirmation stays the default engine. LibAFL's length-and-time minimizing scheduler is kept; no other per-feature score was tried.
- [x] 11.12 Share kept programs between workers without rerunning them (in-process, as LLMP's receiving side does: the workers are threads of one process). Tried, then removed for lack of a measured benefit (11.13)
- **Measured (`kvd` with edge coverage, 8 trials of 5,000 runs per setup):** confirming new features cuts the programs kept from 124 to 70 with LibAFL and from 119 to 69 with the plain loop. Bugs found per setup, out of 8 trials (crash, hang, wrong answer): `libafl` 1, 6, 1; `libafl+confirm` 0, 6, 2; `basic` 0, 5, 1; `basic+confirm` 1, 5, 1. That is too few to separate the setups, so the default stays LibAFL with confirmation until the comparison runs on RediSearch (11.11).
- **Status:** `kvd`'s tests reproduce each planted bug from a fixed program and shrink padded programs to exactly the planted 5, 7, and 7 ops. Against the AddressSanitizer build, every live test passes except the seed-coverage check, which needs the module rebuilt with the renamed `PROCFUZZ_COVERAGE_SHM`; the short campaign was not run for the same reason.
- [x] 11.13 Simplify after the review: one engine, always confirming; no sharing between workers; one send input; no reset; the comparison harness, the plain loop, and `kvd-fuzz` removed once the comparison was recorded; the logic tests folded into one table; the logic rules skip dialect 4, whose totals are capped
- [x] 11.14 Simplify after the second review: module arguments become `CONFIG SET` commands the state follows, so the logic rules see runtime configuration changes; the search loop replaces LibAFL, keeping confirmation and the shortest program per feature, with workers sharing what they keep; `kvd` and the toy model removed in favour of live tests against the real server; writes bounded by the deadline, a timed-out admin connection reopened, a batched input completed only by its last reply, `Populate`'s seed recorded, shrinking allowed to drop optional parts, one log probe for findings, and the setup recorded in each manifest
- **Not yet measured:** the search loop against the 11.11 numbers; rerun that comparison on RediSearch before relying on it.
