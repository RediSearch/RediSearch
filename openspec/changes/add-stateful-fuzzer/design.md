# Design: stateful, coverage-guided fuzzer

## Overview

A Cargo workspace at `fuzz/`, outside the `src/redisearch_rs/` workspace so
that workspace-wide test, miri, and coverage runs do not build it. It has four
crates:

- **`procfuzz`** knows nothing about RediSearch, Redis, or RESP, and is meant
  to be reusable. It owns the model of a stateful program, running a program
  against a system, mutating and shrinking programs, edge coverage and its C
  shim, sanitizer reports, finding directories, the LibAFL campaign, and the
  commands `run`, `reduce`, `fuzz`, and `generate`, as plain functions and,
  behind the `cli` feature, as a clap subcommand.
- **`procfuzz-redis`** holds what any Redis module target shares: the RESP2
  codec, launching `redis-server` with modules, and reading Redis assertion,
  panic, and crash reports.
- **`redisearch-fuzz`** is RediSearch as a model: its ops, what they set up,
  which make sense when, what each command does, and the logic rules.
- **`kvd`** is a small RESP server with planted bugs and its model:
  `procfuzz`'s own target, tested without a sanitizer build.

A target writes one trait, and the core asks one of a system:

```rust
trait Model {
    type Op; type State: Default; type Input; type Reply;
    fn next(&self, state: &State, draw: &mut Draw) -> Op;            // an op that makes sense
    fn apply(&self, state: &mut State, op: &Op, reply: Option<&Reply>); // what it sets up
    fn lower(&self, state: &State, op: &Op) -> Input;                 // the one input it sends
    fn check(&self, ops: &[Op], steps: &[Step<Reply>], notes: &mut Notes) {}
}
trait System { fn start(&self) -> io::Result<impl Session>; }       // any closure is one
trait Session {
    fn step(&mut self, at: usize, input: &Input, run: &mut Run<Reply>) -> io::Result<ControlFlow<()>>;
    fn stop(self, run: &mut Run<Reply>) -> io::Result<()>;
}
```

`next` is the only generator: the core stores the draws that made each op,
mutates a program by changing a draw and making the op again, and shrinks it
by deleting ops and lowering draws. A target writes no mutator or shrinker.
`apply` serves prediction while generating (`reply` is `None`), the live
state a run lowers its ops against, and the state `check` judges replies by.

| `procfuzz` module | Responsibility |
|---|---|
| `draw` | `Draw`: every random decision, replayed from a record |
| `model` | `Model`, `Program`, `Made`, generating a program |
| `system` | `System`, `Session`, `Run`, `Step`, `Notes`; `run` and `in_completion_order` |
| `edit` | `mutate` and `shrink` through stored draws |
| `service` | `Service`: a `System` for a process on a Unix socket, built from a `Codec` and hooks |
| `process`, `coverage`, `sanitizer` | The process under test, its coverage segment, AddressSanitizer reports |
| `campaign`, `findings`, `commands`, `cli` | The LibAFL loop, finding directories, the commands, the subcommand |
| `interrupt`, `error` | Stopping cleanly on SIGINT and SIGTERM; the error type |

| `redisearch-fuzz` module | Responsibility |
|---|---|
| `op` | `Op` and `Action`: types, lowering, the fixed vocabulary |
| `state` | What the ops set up: indexes and schemas, cursors, workers, the last comparable search |
| `generate` | `next`: ops well-typed against the state |
| `effect` | One table of what each command does to results, to the state, and how to undo its pauses |
| `logic` | Equivalence, twin, and partition rules over search results |
| `server` | `redis-server` with the module and RedisJSON as a `Service` |

A finding is a kind and a key, written `<kind>:<key>`. Only
`procfuzz::campaign` depends on LibAFL.

## Program

```rust
// procfuzz
struct Program<O> { ops: Vec<Made<O>> }
struct Made<O>    { op: O, from: Vec<u64> }   // the draws that made `op`
// redisearch-fuzz
struct Op { conn: u8, defer: bool, action: Action }
enum Action {
    Boot { args },                            // module arguments
    Create { index, on, prefix, fields },
    Alter { index, on, field },
    Query { index, query: Query, options },
    Aggregate { index, query, pipeline, cursor: Option<CursorSlot> },
    CursorRead { slot, count }, CursorDel { slot },
    Populate { on, prefix, fields, n, seed },
    Settle,
    Command { argv },                         // at least one word
}
```

The rules the design depends on:

1. **One op, one input, at most one reply.** Steps line up with ops one to
   one, so there is no harness step and no sentinel. `Populate` lowers to one
   batch input whose requests run one after another.
2. **Ops stay explicit.** Deleting an op never changes what another sends. An
   op that names a missing index is still sent and draws an ordinary error.
3. **Names come from the state.** `next` picks an index, a field, or a cursor
   slot from the state rather than computing one, so an op made again after an
   earlier op changed still names something that exists.
4. **0 is the simplest draw.** Alternatives are ordered simplest first, and an
   optional addition is taken only when `Draw::one_in` is true, so lowering a
   draw simplifies the op.
5. **Everything a run depends on is in the program.** Module arguments are
   `Boot` ops, which count only before the first other op.

Further points:

- **Typed where the fuzzer reasons about an op.** `Create` declares a schema
  the generator builds on and `Alter` extends it; `Query` is a tree the logic
  rules match structurally; `Aggregate` keeps a cursor in a slot. Writes,
  deletes, drops, `FT.DEBUG` scheduling hooks, `DEBUG RELOAD`, and `CONFIG
  SET` are `Command`s.
- **Cursors.** A cursor's id is chosen by the server. `apply` stores the id
  from the reply (`[results, id]`) in the cursor's slot, and `lower` reads it
  when a later op reads or deletes the cursor. An op whose reply names a
  cursor is always awaited, even when deferred, so the id is known before the
  next op is lowered. A slot without an id reads as cursor 0 and draws an
  ordinary error.
- **`on`** (`hash` or `json`) selects the data type. A JSON schema declares
  `$.name AS name`; a JSON write is `JSON.SET key $` of an object with text and
  geo as strings, tags as arrays of strings, numbers as numbers, and vectors
  as arrays of floats. About a third of generated indexes are JSON, on
  prefixes of their own.
- **`Bytes`** serialises as a JSON string when it is valid UTF-8 and as
  `{"hex": "…"}` otherwise. A vector is a FLOAT32 blob, so NaN, infinities,
  and negative zero survive. A `Knn` node is valid only as the whole query;
  lowering adds the `PARAMS` its vectors need and `DIALECT 2` unless the
  options name a dialect.
- **`Populate`** carries its own key prefix and field list. It expands to `n`
  writes of keys `<prefix>0` to `<prefix>n-1`, with values drawn by a `Draw`
  seeded with `seed`, which uses SplitMix64 and raw 64-bit draws only, so a
  dependency update cannot change it. A golden test freezes the expansion.
- **`conn`** names one of three connections.

`State` is what the ops set up: the indexes and their schemas, whether
workers are configured and paused, the cursors, the module arguments, and the
last search the logic rules can compare. A successful raw `FT.CREATE` or
`FT.ALTER` marks its index's schema unknown, so the index has no twin, and a
raw `FT.DROPINDEX` removes it.

## Execution

`run(model, system, program) -> io::Result<Run>`. An error means the harness
broke, for example the server did not start, and is never a finding. Each op
is lowered against the state its completed predecessors left, folded in the
order their replies arrived.

`Service` is the `Session` for a process that answers requests on a socket.
It is assembled from parts, not implemented by the target: a `Codec`, a
launch command, the request that checks readiness and the one that shuts the
process down, a `heal` hook that undoes every pause a program can set, an
`idle` hook, probes that read findings from the log, and `Timing`.

```rust
enum Input<Q> {
    Boot(Vec<String>),                          // launch arguments
    Send { conn: u8, wait: bool, request: Q },
    Batch { conn: u8, requests: Vec<Q> },
    Settle,                                     // read every reply, then wait until idle
    Reset { conn: u8 },
}
```

### Lifecycle

1. **Launch** lazily, on the first input that is not `Boot`: create a private
   temporary directory and a zeroed coverage segment, spawn the process in its
   own process group, listening on a Unix socket in that directory, with
   stdout and stderr in a log file, and wait until it answers the readiness
   request. A program of only `Boot` ops starts nothing.
2. **Perform** inputs in order. Before sending on a connection, read the
   replies outstanding on it. `wait` reads the reply at once. A reply that
   does not arrive within `patience` gets `deadline` more after `heal`; one
   that still does not arrive is a hang and ends the run. A connection that
   closes or sends an invalid reply is dropped with its outstanding replies;
   the run ends only if the process is exiting.
3. **Heal.** For RediSearch: resume workers, resume the background scan,
   clear sync points, and continue the GC schedule of every index, all from
   the effects table. It stops at the first command the server does not
   answer, since its main thread is then blocked.
4. **Stop.** Heal, read what is outstanding, send `SHUTDOWN NOSAVE`, and wait
   for the process to exit. A process that does not exit is hung.
5. **Abort** a hung process with `SIGABRT`, so it writes a backtrace.
6. **Reap.** Let the process finish exiting, so a sanitizer report is
   complete, kill what is left of its group, then read the coverage segment
   and the log.

### Steps

```rust
struct Step<R> { sent: u32, done: Option<u32>, reply: Option<R> }
```

`sent` and `done` are ticks of one clock, so a check can tell which steps ran
while another's reply was outstanding. `done` is `None` when the input never
completed: its reply was lost, or the run ended first. A run that ended early
has fewer steps than ops.

### Settle

`Settle` reads every outstanding reply, then polls the `idle` hook until it
reports idle or `patience` passes. For RediSearch, idle means `FT.INFO`
reports `indexing` as 0 for every index in `FT._LIST` and `_FT.DEBUG WORKERS
STATS` reports no pending and no in-progress jobs, after which it calls
`GC_WAIT_FOR_JOBS`. It polls worker statistics because `WORKERS DRAIN` can
deadlock the main thread. A settle completes only when the server became
idle; one that timed out stays incomplete, which makes logic rules across it
inconclusive and is never a finding.

### Server environment

- `redis-server` is an AddressSanitizer build; the module is built with
  `FUZZ_COV=1`, which implies `SAN=address` and therefore a Debug build with
  assertions.
- Server flags: `--enable-debug-command yes`, `--save ""`, `--appendonly no`.
- `ASAN_OPTIONS` with `halt_on_error=1`, `detect_odr_violation=0` and
  `detect_leaks=1`, with `RS_GLOBAL_DTORS=1` and the suppressions file the
  Python tests use.

## Findings a run shows

**Safety findings** come from `Service::stop`: the first log line that
reports a failure (an AddressSanitizer summary, a Redis assertion, or a Redis
panic; reports from forked children land in the same log), each leak, a hang,
and a crash, which is a signal or a non-zero exit when nothing else explains
it, and a reply that violates the protocol. A hang or crash is named by the
frame the crash report blames, the module's first, else the
`redis-server` frame after the signal handler, or else by the signal.

**Logic findings** are rules over `FT.SEARCH` replies (`logic`). Two replies
are compared only inside one *comparison window*: it opens at a settle that
completed and closes at the next step that can change results, and no such
step may be sent while either search is in flight. A step's effect comes from
the effects table:

- **Reads**: searches, aggregates, cursors, and inspection commands. **Schedules**:
  the `FT.DEBUG` GC, worker, scan-control, and sync-point hooks. Neither
  closes a window, so a forced GC between two searches is checked.
- **Rebuilds**: `DEBUG RELOAD`. It must not change results, but leaves the
  index rebuilding, so the next comparable search needs a settle after it.
- **Writes**: everything else, including every unknown command, and any step
  that never completed.

The rules:

- **Equivalence.** The same query on the same index returns the same total,
  and the same keys when both replies list all of them.
- **Twin.** The same query on two indexes whose server-confirmed schemas match
  (data type, prefix, and fields, ignoring order and `SORTABLE`) returns the
  same total and keys.
- **Partition.** For `q`, `q p`, and `q -p` on the same index, the totals of
  the last two add up to the first. A lone `p` and `-p` partition `*`.

Rules apply only when the program boots the module with `ON_TIMEOUT FAIL`,
since the default policy returns partial results on a timeout. A search is
skipped when its reply is an error, when it contains a KNN node
(approximate), and, for partition, when a term can become an empty node
(empty, a stopword, or split by the tokenizer) or a clause names a field the
confirmed schema lacks, since dialect 1 drops such a clause. The key is the
rule and the query's shape without its values, for example
`logic:partition:and(term,tag)`.

Query rendering writes each negation as `(-(…))`: dialect 1 parses `-(a) b`
as `-(a b)`, which the first 1,000-program run showed as false partition
findings. The generator boots with `ON_TIMEOUT FAIL` in more than half of its
programs, makes a third of new indexes a twin of an existing one (sometimes
with different `SORTABLE` flags), and half the time follows a comparable
search with a related op: the same query again, on the same index or a twin,
with a part added or subtracted, a settle, or a forced GC.

**Features** are anything worth reaching: coverage edges from `Service`, and
from the RediSearch check nothing yet. `kvd`'s check also records kinds of
reply and shapes of state.

## Coverage

`FUZZ_COV=1` implies `SAN=address` and adds:

- C and C++: `-fsanitize-coverage=trace-pc-guard`. This needs clang.
- Rust: `-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3
  -Cllvm-args=-sanitizer-coverage-trace-pc-guard`.
- `procfuzz/shim/coverage.c`, about fifty lines, excluded from
  instrumentation and linked into `redisearch.so`. At guard initialisation it
  numbers the guards and maps the POSIX shared-memory segment named by
  `PROCFUZZ_COVERAGE_SHM`. On each edge it stores `1` at the guard's index.

C and Rust share one id space. Writes go directly to shared memory, so edges
from the fork-GC child and from a crashing server are kept. The segment has a
header holding the capacity, 2^20 edges, and the guard count the shim records
at load; a run reads only up to the guard count. The shim never aborts: edges
past the capacity are dropped. The build option is rejected together with
any test-building option, because test binaries would not link the shim.

The `FUZZ_COV` build has its own build directory (`*-debug-asan-fuzz`) and its
own Rust target directory (`bin/redisearch_rs_fuzz`). The CMake rule that runs
Cargo depends on Rust sources, not on `RUSTFLAGS`, so a shared target directory
would link whichever instrumentation built last into every flavor.

Edge presence is used instead of hit counts. Unsynchronised counters written by
worker threads and a forked child make scheduling variation look like new
behaviour, and each false novelty costs a server start.

## Generation, mutation, shrinking

`next` draws each op against the state the ops before it predict: writes and
searches name existing indexes and use fields of the right type, a `Create`
takes an unused name, cursor ops name a slot that holds a cursor, and while
workers are paused searches are deferred and a resume is likely. Words and
numbers come from a fixed vocabulary.

`mutate` makes one edit, chosen by weight:

| Edit | Weight |
|---|---|
| Insert one to three fresh ops anywhere, made in the state the ops before them predict | 4 |
| Redraw an op: replace one of its draws, draw the rest fresh, and make it again | 3 |
| Delete an op | 1 |
| Append up to four ops of a donor program | 1 |
| Append a fresh op | 1 |

`shrink` deletes runs of ops, halving the run length down to single ops, then
lowers each draw of each op to 0 or to half, keeping a candidate only if it
still fails and its op took exactly the lowered draws, and repeats until
neither helps. Hand-written ops have no draws and shrink by deletion only.
Each candidate strictly decreases the op count or a draw, so shrinking
terminates. `reduce` shrinks a program while it still yields a finding, then
runs the result ten times and records the reproduction rate. A racy finding
can shrink into a program that hits it less often; the rate in the manifest
is the signal.

## Campaign

LibAFL 0.16.1 supplies the corpus, novelty, scheduling, and statistics.

- A wrapper around a program implements `Input` with JSON files and `HasLen`
  as its op count.
- The executor runs a program and hashes each feature into a map observer of
  2^20 entries. The command layer writes each finding that has no directory
  under `findings/` yet: creating the directory claims it, so a known bug is
  marked by its directory, and of several workers only one reports a finding.
- Feedback is max-map novelty over that one map. There is no LibAFL
  objective; findings never enter a corpus.
- The scheduler is the length-and-time minimizer over a queue.
- One mutational stage, at most eight mutations per scheduled program, calls
  `mutate` with a donor drawn from the corpus and a `Draw` seeded from
  LibAFL's state RNG.
- The corpus is kept on disk as program JSON, and is reloaded with the seeds
  when a campaign starts again. Without seeds the campaign starts from the
  empty program.
- A run may fail three times in a row before the worker gives up; a worker
  that gives up stops the others after their current run. The first SIGINT or
  SIGTERM does the same, so every server, coverage segment, and directory is
  cleaned up.

`fuzz --jobs N` (by default one per core) starts N worker threads, each an
independent campaign with its own corpus under `corpus/<worker>` and a shared
findings directory. Workers do not exchange corpus entries yet. LibAFL's LLMP
can share them without rerunning an imported program when the clients share a
configuration; that is to be tried.

## Planted bugs

`fuzz/redisearch/planted/planted.patch` adds two deterministic bugs that each need a few
ops of state: a range query on a sortable numeric field of an index with a TEXT
or TAG field and at least three documents reads past a buffer, and
`FT.DROPINDEX … DD` of an index with a TAG field and at least two documents
fails an assertion. `setup.sh` applies the patch for one incremental build in
the fuzz build directory, copies the module to `bin/fuzz/planted/`, reverts the
patch, and rebuilds; a second build directory would cost a full build. Seeds
beside the patch trigger each bug. They are the targets for the reduce and
campaign checks, and the only real sanitizer and assertion reports the judge is
tested against while master has no known bug.

`kvd` has three planted bugs of its own: a crash at 7 ops, a wrong answer at
7 ops behind a launch flag, and a hang at 5 ops. Its tests run them without a
sanitizer build.

## Findings

```
findings/<finding>/
  program.json     the failing program, reduced if reduce wrote the bundle
  original.json    written by reduce: the program before reduction
  trace.json       the steps and the run's findings
  server.log
  manifest.json    the finding, reproduction rate, binaries, sanitizer options
```

The native `run` command is the authoritative reproducer. `export` is limited
to programs with one connection, no deferred replies, and no cursors.

## Edge cases

- **Server fails to start.** `run` returns an error, never a finding; the
  campaign stops after three failures in a row.
- **Program leaves workers paused.** Resolved by healing; the program carries
  on.
- **Hook errors** (for example pausing workers when none are configured). An
  ordinary error reply.
- **Crash in the fork-GC child.** The parent does not inspect the child's exit
  status, so detection relies on the log.
- **Default query timeout.** 500 ms with partial results returned; logic rules
  therefore require the fail policy and skip on timeout.
- **Known easy bugs.** Suppressed by directory, so they do not hide others, but
  each occurrence still costs a run.

## Alternatives considered

- **Reuse one server with a reset between programs.** Rejected: module state
  survives a flush, which is what made earlier findings non-reproducible.
- **Mutate a byte buffer decoded into commands.** Rejected: a mutation that
  changes one choice reinterprets everything after it, and the corpus is opaque.
- **A typed-op trait with a hand-written shrinker per type, variables bound
  from replies, and harness steps in the trace.** The first design. Replaced:
  the shrinker and literal editing were hand-written per type, the variable
  syntax was in-band, harness steps needed sentinels, and four string tables
  described commands, one of which missed `FT.ALTER`.
- **Hit counts with a stability stage.** Rejected for edge presence; a
  stability check on new features is to be measured.
- **Inline 8-bit counters.** Rejected: they live in the module's private data
  and are lost in a forked child and on a crash.
- **AFL++ compiler wrappers, cargo-fuzz, honggfuzz, bolero.** Rejected: they
  assume an in-process target, or do not link a runtime into a shared object.
- **LibAFL's calibration stage with a power schedule.** Measured on `kvd` as
  the worst of the setups tried; not adopted.
- **Forking each test case from a loaded server.** Deferred until server
  startup time is measured, because fork drops threads.

## Verification status

- **Tested by experiment (macOS arm64):** the coverage flags and shim, with a C
  file and a Rust static library in one shared object loaded by an
  uninstrumented host, with and without AddressSanitizer, including an edge hit
  only in a forked child.
- **Tested in CI-able tests (`kvd`):** each planted bug from a fixed program; a
  held-back reply released by healing; a cursor read by the id the server
  chose; padded programs shrinking to exactly the planted ones.
- **Read in this repository:** the `FT.DEBUG` hooks named above; the
  `WORKERS DRAIN` deadlock warning; the meaning of `indexing` and `cleaning` in
  `FT.INFO`; the default timeout and policy; the immutable settings; how
  `build.sh` selects the nightly toolchain and build target under `SAN`.
- **Read in library source:** the LibAFL 0.16.1 traits used above. The
  `redis` crate (1.8.2) was rejected: after a read timeout its synchronous
  connection marks the late reply to be skipped, so a reply that arrives after
  healing would be discarded.
- **Measured (macOS arm64, AddressSanitizer builds):** `redis-server` alone is
  ready in about 0.5 s; with the module loaded, about 1.6 s. A clean shutdown
  reports no sanitizer errors with leak detection on.
- **Measured with the first design:** each planted seed spread through 200
  generated ops reduced to at most five ops in under seven minutes; a 33-op
  program hitting the `DebugIndexes_ScanProc` overflow reduced to 5 ops in 4
  minutes. Not yet measured with draw-based shrinking.
- **Not verified:** the coverage build on Linux; the reproduction rate and
  stability of real programs; whether reply and state features, a stability
  check, or another engine find more on RediSearch.
