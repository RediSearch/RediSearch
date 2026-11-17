# Design: stateful, coverage-guided fuzzer

## Overview

One standalone crate, `fuzz/`, outside the `src/redisearch_rs/` workspace so that
workspace-wide test, miri, and coverage runs do not build it. Two plain data
types carry everything between components, and one component performs IO.

```
 generate ─┐                 run (only IO)            judge, logic (pure)
 mutate   ─┼─▶ Program ─▶ interpreter ─▶ Trace ─▶ findings
 smaller  ─┘      ▲                                    │
                  └───────── reduce, campaign ◀────────┘
```

| Module | Responsibility |
|---|---|
| `program` | The types, the JSON format, and the one visitor over an op's values |
| `lower` | An op as the commands it sends: query, schema, and write syntax, `Populate` |
| `scope` | What a sequence of ops set up: indexes and schemas, worker state, cursors |
| `generate` | Ops and programs well-typed against a scope, and the value pools |
| `edit` | One edit set: `mutate` and `smaller`, with the size measure |
| `target`, `resp`, `coverage` | Server process, connections, the RESP reader, coverage memory |
| `run` | The interpreter: `Program` → `Trace` |
| `judge`, `logic` | `(Program, Trace)` → findings: safety from the log and exit, logic from replies |
| `reduce`, `bundle`, `campaign` | Consumers of `judge::execute` |

`judge::execute(program, env)` runs and judges a program; `run`, `reduce`,
and `fuzz` all go through it. `scope` serves both the generator, which folds
every op of a prefix, and the logic rules, which fold only the ops the server
accepted. Only `campaign` depends on LibAFL.

## Program

```rust
struct Program { version: u32, module_args: Vec<Bytes>, ops: Vec<Op> }
struct Op      { conn: Conn, reply: ReplyMode, bind: Option<Var>, kind: OpKind }
enum ReplyMode { Await, Defer }
enum OpKind {
    Create { index, on, prefix, fields },
    Alter { index, on, field },
    Query { index, query: QueryAst, options },
    Populate { on, prefix, fields, n, seed },
    Barrier,
    Command { argv: Vec<Bytes> },
}
```

- **Typed only where the fuzzer reasons about an op.** `Create` declares a
  schema the generator builds on, and `Alter` extends it; `Query` is a tree
  the logic rules match structurally and shrinking simplifies; `Populate`
  expands into many writes; `Barrier` is executed by the interpreter itself.
  Everything else, including writes, deletes, drops, aggregations, cursor
  reads, `FT.DEBUG` scheduling hooks, `DEBUG RELOAD`, and `CONFIG SET`, is a
  `Command`; the generator builds aggregations from the schema as tokens.
  The interpreter treats a typed op and a `Command` the same way once lowered.
- **`on`** (`hash` or `json`) on `Create`, `Alter`, and `Populate` selects the
  data type. A JSON schema declares `$.name AS name`; a JSON write is
  `JSON.SET key $` of an object with text and geo as strings, tags as arrays
  of strings, numbers as numbers, and vectors as arrays of floats. The
  generator makes about a third of its indexes JSON, on prefixes of their own,
  so writes of one type rarely meet keys of the other.
- **`module_args`** holds only what must exist before the server starts: some
  `search-*` settings are immutable after load. Runtime configuration is a
  `CONFIG SET` command.
- **Explicit names.** Indexes, keys, and cursors are named in the program.
  Deleting an op never changes what another op sends. An op that refers to a
  missing index is still sent and draws an ordinary error.
- **`Bytes`** serialises as a JSON string when it is valid UTF-8 and as
  `{"hex": "…"}` otherwise, so invalid UTF-8 is representable. A vector is a
  FLOAT32 blob in `Bytes`, so NaN, infinities, and negative zero survive and
  the type is hashable. A `Knn` query node is valid only as the whole query;
  lowering adds the `PARAMS` its vectors need and `DIALECT 2` unless the
  options name a dialect.
- **`Populate`** carries its own key prefix and field list, so it does not
  depend on an earlier `Create`. It expands to `n` writes of keys
  `<prefix>0` to `<prefix>n-1`, with values drawn from the fixed dictionary in
  `lower` by SplitMix64 seeded with `seed`, using raw integer draws only, so a
  dependency update cannot change it. A golden test freezes the expansion. The
  generator draws `n` up to 513, log-uniformly.
- **`Var`** holds a server-chosen value, a cursor id. An op with `bind` stores
  the id in its reply (`[results, id]`) in that variable when the reply is
  read, and a command token `$<n>` is replaced by variable `n` when sent. An
  unbound variable reads as 0, because its source op was deleted, failed, or
  is still deferred on another connection, and draws an ordinary error.
  Before sending an op, the interpreter reads the pending replies on its
  connection, so a binding made earlier on the same connection is seen.
- **`conn`** is taken modulo a small fixed number of connections.

`lower(&OpKind) -> Vec<Argv>` turns an op into the commands it sends; `Barrier`
lowers to none.

`Scope` is a pure fold over a prefix of a program: the indexes that exist and
their schemas (a `Create` of an existing name changes nothing, `FT.DROPINDEX`
removes one), whether workers are configured, and whether they are paused. The
generator uses it to produce well-typed ops. The judge uses a second fold over only those ops whose recorded reply
succeeded.

## Execution

`run(&Program, &Env) -> Result<Trace, String>`. An error means the server could
not start, which is never a finding. The interpreter owns the server and all
execution state: connections, pending replies, and a harness connection for its
own commands.

### Lifecycle

1. **Start.** Create a private temporary directory and a zeroed coverage segment.
   Spawn `redis-server` in its own process group, listening on a unix socket in
   that directory, with stdout and stderr redirected to a log file.
2. **Execute** ops in order. For `Await`, send and read the reply. For `Defer`,
   send and leave the reply pending. Pending replies on a connection are read
   when that connection is next used, at a `Barrier`, and at the end. A reply
   that does not arrive within a short settle time gets a full deadline after
   the harness releases its controls (below); the release is recorded as a
   `RELEASE` step and the program carries on. A connection that closes or sends an invalid reply is
   recorded and reopened on next use; the program stops only if the server is
   exiting.
3. **Release.** The harness connection sends a fixed list: resume workers,
   resume the background scan, clear sync points, and continue the GC schedule
   of every index. It stops at the first command the server does not answer,
   since its main thread is then blocked.
4. **Stop.** At the end: release, read the remaining replies, send
   `SHUTDOWN NOSAVE`, and wait for the server to exit.
5. **Abort** a hung server with `SIGABRT` to obtain a backtrace.
6. **Reap.** Let the server finish exiting, so a sanitizer report is complete,
   kill what is left of the process group, then read the coverage segment and
   the log.

```rust
enum Exit { Clean, Died { at_op }, Hung { at_op } }
```

`Hung` means a reply did not arrive even after the release. A run
that reaches the end but whose server exits non-zero, for example with a leak
report, is `Died` at the end.

### Trace

```rust
struct Trace { steps: Vec<Step>, exit: Exit, status: Status, log: Bytes, coverage: Coverage }
struct Step  { op: usize, conn: Conn, argv: Argv, sent: u32, received: Option<u32>, reply: Option<Reply> }
enum Reply   { Value(Value), Timeout, Closed(String), Invalid(String) }
```

`sent` and `received` are positions on one harness-side clock, so the judge can
tell which ops ran inside a deferred op's window. `reply` is `None` when the
reply was never read because the server died first. Each write a `Populate`
expands to is an ordinary step.

### Barrier

`Barrier` reads all pending replies, then on the harness connection polls until
`FT.INFO` reports `indexing` as 0 for every index in `FT._LIST` and
`FT.DEBUG WORKERS STATS` reports no pending and no in-progress jobs, then calls
`GC_WAIT_FOR_JOBS`. It polls worker statistics because `WORKERS DRAIN` can
deadlock the main thread. It is recorded as a step whose reply is `OK`, or
`Timeout` when the server did not become quiet in time; a timed-out barrier
makes logic rules across it inconclusive and is never a finding.

### Server environment

- `redis-server` is an AddressSanitizer build; the module is built with
  `FUZZ_COV=1`, which implies `SAN=address` and therefore a Debug build with
  assertions.
- Server flags: `--enable-debug-command yes`, `--save ""`, `--appendonly no`.
- `ASAN_OPTIONS` with `halt_on_error=1`, `detect_odr_violation=0` and
  `detect_leaks=1`, with `RS_GLOBAL_DTORS=1` and the suppressions file the
  Python tests use.

## Judge

`judge(&Program, &Trace) -> Verdict { findings, features }` is pure.

**Safety findings**: the server died; a sanitizer report appears anywhere in the
log (reports from forked children land there); an assertion failed; the exit is
`Hung`; a leak was reported at shutdown; a reply violated the protocol. The
signature is the sanitizer summary line, the assertion location, or the leading
module frames of the abort backtrace.

**Logic findings** are rules over `FT.SEARCH` replies already in the trace
(`logic`). Two replies are compared only inside one *settled period*: it
starts at a barrier that completed and ends at the next op that can change
results, and neither query may have had such an op sent while it was in
flight. Ops are classed by their effect on results:

- **Neutral**: queries, aggregates, cursors, inspection commands (`FT.INFO`,
  `FT.EXPLAIN`, and the like), and the `FT.DEBUG` GC, worker, and scan-control
  hooks. They do not end a period, so a forced GC between two queries is
  checked.
- **Transparent**: `DEBUG RELOAD`. It must not change results, but leaves the
  index rebuilding, so the next comparable query needs a barrier after it;
  the period continues across it.
- **Changing**: everything else, including every unknown command.

The rules:

- **Equivalence.** The same query on the same index returns the same total,
  and the same keys when both replies list all of them.
- **Twin.** The same query on two indexes whose server-confirmed schemas match
  (data type, prefix, and fields, ignoring order and `SORTABLE`) returns the
  same total and keys.
- **Partition.** For `q`, `q p`, and `q -p` on the same index, the totals of
  the last two add up to the first. A lone `p` and `-p` partition `*`.

Rules apply only when the program loads the module with `ON_TIMEOUT FAIL`,
since the default policy returns partial results on a timeout. A query is
skipped when its reply is an error, when it contains a KNN node (approximate),
and, for partition, when a term can become an empty node (empty, a stopword,
or split by the tokenizer) or a clause names a field the confirmed schema
lacks, since dialect 1 drops such a clause. The signature is the rule and the
query's shape without its values, for example `logic:partition:and(term,tag)`.

Query rendering writes each negation as `(-(…))`: dialect 1 parses `-(a) b`
as `-(a b)`, which the first 1,000-program run showed as false partition
findings. The generator loads `ON_TIMEOUT FAIL` in three programs of four,
makes a third of new indexes a twin of an existing one (sometimes with
different `SORTABLE` flags), and emits a related-query group: a barrier, `q`,
`q p`, `q -p`, sometimes a forced GC, `q` again, and `q` on a twin if one
exists. The same group is a mutation edit.

**Features** beyond coverage edges are not implemented: edges alone have kept
campaigns growing so far. Reply classes can join the map if a campaign
plateaus.

## Coverage

`FUZZ_COV=1` implies `SAN=address` and adds:

- C and C++: `-fsanitize-coverage=trace-pc-guard`. This needs clang.
- Rust: `-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3
  -Cllvm-args=-sanitizer-coverage-trace-pc-guard`.
- A shim of about fifty lines of C, excluded from instrumentation and linked
  into `redisearch.so`. At guard initialisation it numbers the guards and maps
  the POSIX shared-memory segment named by `REDISEARCH_COV_SHM`. On each edge it
  stores `1` at the guard's index.

C and Rust share one id space. Writes go directly to shared memory, so edges
from the fork-GC child and from a crashing server are kept. The segment has a
header holding a magic number, the capacity, and the guard count the shim
records at load. The shim never aborts: edges past the capacity are dropped,
and the fuzzer fails the run at startup when the guard count exceeds the
capacity. The feature tail starts at the capacity. The build option is rejected
together with any test-building option, because test binaries would not link
the shim.

The `FUZZ_COV` build has its own build directory (`*-debug-asan-fuzz`) and its
own Rust target directory (`bin/redisearch_rs_fuzz`). The CMake rule that runs
Cargo depends on Rust sources, not on `RUSTFLAGS`, so a shared target directory
would link whichever instrumentation built last into every flavor.

Edge presence is used instead of hit counts. Unsynchronised counters written by
worker threads and a forked child make scheduling variation look like new
behaviour, and each false novelty costs a server start.

## Generation, mutation, reduction

`generate` draws each op against the `Scope` of the ops before it: writes and
queries name existing indexes and use fields of the right type, a `Create`
takes an unused name, and while workers are paused queries are deferred and a
resume is likely. Values come from pools: a fixed dictionary plus every word
and number already in the programs being mutated, so values that reached new
code are reused.

One edit set serves both mutation and reduction:

| Edit | Smaller? |
|---|---|
| Delete a range of ops | yes |
| Replace an op with its lowered `Command` ops (`Lower`) | no |
| Simplify a leaf (shorter bytes, 0 or 1, fewer fields, smaller `n`) | yes |
| Insert a generated op, typed against `Scope` | no |
| Splice a range from another program | no |
| Regenerate one field from the pools | no |
| Insert a related-query group | no |
| Edit `Command` argv tokens: delete, duplicate, swap, replace from a pool | no |

Changing an op's connection or reply mode counts as regenerating a field, and
editing a token's bytes (inserting query syntax, deleting a byte, splicing in a
pool word) as a token edit. `Lower` does not apply to ops that define or use a
`Var`. `mutate` applies one to three edits, because each run costs a server
start. `smaller` enumerates the smaller edits, largest first: dropping the
module arguments, deleting ranges of ops halving in length, removing a schema
field, hoisting a query node's child, deleting a token, and shortening a word
or number. Each strictly decreases a size measure, so reduction terminates.

`reduce` keeps a smaller candidate when `judge(run(candidate))` still contains
the original finding, resumes at the same position in the new candidate list,
and stops after a full pass keeps nothing. It then runs the result ten times
and records the reproduction rate. Each candidate runs once. A racy finding can
therefore shrink into a program that hits it less often; requiring two
reproductions per candidate was tried and did not help, because luck still
accumulates over dozens of kept candidates. The rate in the manifest is the
signal. Lowering a `Populate` exposes its
writes to range deletion.

## Campaign

LibAFL 0.16.1 supplies the corpus, novelty, scheduling, and statistics.

- `Program` implements `Input` with JSON `to_file` and `from_file`, and
  `HasLen` as the size measure `smaller` decreases.
- One executor calls `run` then `judge`, sets one byte per edge in a map
  observer of 2^20 entries (more than the module has guards), and stores the
  trace and findings in a second observer. Judge features join the map with
  the logic rules (task 8).
- Feedback is max-map novelty over that one map.
- The objective is a finding whose signature has no directory under
  `findings/` yet: creating the directory claims it, so a known bug is marked
  by its directory, and of several workers only one reports a finding. The
  objective writes the bundle itself, so LibAFL's solutions corpus stays in
  memory.
- The scheduler is the length-and-time minimizer over a queue.
- One mutational stage, at most eight mutations per scheduled program, wraps a
  mutator that calls `edit::mutate` with a donor drawn from the corpus, seeded
  from LibAFL's state RNG.
- The corpus is kept on disk as program JSON without metadata files, and is
  reloaded with the seeds when a campaign starts again.
- Calibration, input and corpus minimisation, the fork server, the restarting
  manager, and the launcher are not used.

If this wiring is not running within the budget set in the tasks, it is
replaced by a hand-written loop of about two hundred lines (a corpus directory,
a seen-edges table, a weighted pick) behind the same `campaign` interface.

`fuzz --jobs N` (by default one per core) starts N worker processes, each an
independent campaign with its own corpus under `corpus/<worker>` and a shared
findings directory; creating a finding's directory claims it. Workers do not
exchange corpus entries while running: LibAFL imports a program by running
it, and while unstable edges make about one run in seven a new corpus entry,
importing from eleven siblings would cost more runs than fuzzing.

## Planted bugs

`fuzz/planted/planted.patch` adds two deterministic bugs that each need a few
ops of state: a range query on a sortable numeric field of an index with a TEXT
or TAG field and at least three documents reads past a buffer, and
`FT.DROPINDEX … DD` of an index with a TAG field and at least two documents
fails an assertion. `setup.sh` applies the patch for one incremental build in
the fuzz build directory, copies the module to `bin/fuzz/planted/`, reverts the
patch, and rebuilds; a second build directory would cost a full build. Seeds
beside the patch trigger each bug. They are the targets for the reduce and
campaign checks, and the only real sanitizer and assertion reports the judge is
tested against while master has no known bug.

## Findings

```
findings/<signature-hash>/
  program.json     the failing program, reduced if reduce wrote the bundle
  original.json    written by reduce: the program before reduction
  trace.json
  server.log
  manifest.json    module git SHA, Redis version, sanitizer options, reproduction rate
```

The native `run` command is the authoritative reproducer. `export` is limited
to programs with one connection, no deferred replies, and no variables.

## Edge cases

- **Server fails to start.** `run` returns an error, never a finding; the
  campaign stops after repeated failures.
- **Program leaves workers paused.** Resolved by the release list, recorded as
  a `RELEASE` step; the program carries on.
- **Hook errors** (for example pausing workers when none are configured). An
  ordinary error reply.
- **Crash in the fork-GC child.** The parent does not inspect the child's exit
  status, so detection relies on the log scan.
- **Default query timeout.** 500 ms with partial results returned; logic rules
  therefore require the fail policy and skip on timeout.
- **Known easy bugs.** Suppressed by signature, so they do not hide others, but
  each occurrence still costs a run.

## Alternatives considered

- **Reuse one server with a reset between programs.** Rejected: module state
  survives a flush, which is what made earlier findings non-reproducible.
- **Mutate a byte buffer decoded into commands.** Rejected: a mutation that
  changes one choice reinterprets everything after it, and the corpus is opaque.
- **Compound oracle ops and a versioned bulk expansion.** Rejected in favour of
  judge rules over ordinary replies and a frozen expansion with a `Lower` edit.
- **Hit counts with a stability stage.** Rejected for edge presence; revisit
  only if coverage growth stalls.
- **Inline 8-bit counters.** Rejected: they live in the module's private data
  and are lost in a forked child and on a crash.
- **AFL++ compiler wrappers, cargo-fuzz, honggfuzz, bolero.** Rejected: they
  assume an in-process target, or do not link a runtime into a shared object.
- **A derive-based mutation library.** Rejected: it cannot see which names are
  in scope, and the scope-aware generator already regenerates fields.
- **A hand-written campaign loop from the start.** Viable; kept as the fallback.
- **Forking each test case from a loaded server.** Deferred until server
  startup time is measured, because fork drops threads.

## Verification status

- **Tested by experiment (macOS arm64):** the coverage flags and shim, with a C
  file and a Rust static library in one shared object loaded by an
  uninstrumented host, with and without AddressSanitizer, including an edge hit
  only in a forked child.
- **Read in this repository:** the `FT.DEBUG` hooks named above; the
  `WORKERS DRAIN` deadlock warning; the meaning of `indexing` and `cleaning` in
  `FT.INFO`; the default timeout and policy; the immutable settings; how
  `build.sh` selects the nightly toolchain and build target under `SAN`.
- **Read in library source, not compiled:** the LibAFL 0.16.1 traits used above.
- **Rejected after reading its source:** the `redis` crate (1.8.2). After a read
  timeout its synchronous connection marks the late reply to be skipped, so a
  reply that arrives once the harness releases its controls would be discarded.
  The target reads RESP itself, keeping its buffer across timeouts; protocol
  violations then surface directly.
- **Measured (macOS arm64, AddressSanitizer builds):** `redis-server` alone is
  ready in about 0.5 s; with the module loaded, about 1.6 s. A clean shutdown
  reports no sanitizer errors with leak detection on.
- **Measured (reduce, planted bugs):** each planted seed spread through 200
  generated ops reduced to at most five ops in under seven minutes, reproducing
  ten times out of ten.
- **Measured (reduce, real bug):** a 33-op generated program hitting the
  `DebugIndexes_ScanProc` overflow, which reproduced three times out of three,
  reduced to 5 ops in 4 minutes; the result reproduces four times out of ten.
- **Measured (generator):** of 200 generated programs, 190 run clean and 10
  hit the same sanitizer finding (task 4 status); about 2.5% of replies are
  errors.
- **Not verified:** the coverage build on Linux, including symbol binding
  against an AddressSanitizer `redis-server`; the reproduction rate and
  stability of real programs.
