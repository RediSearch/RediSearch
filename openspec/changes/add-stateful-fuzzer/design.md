# Design: stateful, coverage-guided fuzzer

## Overview

One standalone crate, `fuzz/`, outside the `src/redisearch_rs/` workspace so that
workspace-wide test, miri, and coverage runs do not build it. Two plain data
types carry everything between components, and one component performs IO.

```
 gen (pure)             run (only IO)               judge (pure)
 generate ─┐
 mutate   ─┼─▶ Program ─▶ interpreter ─▶ Trace ─▶ findings + features
 smaller  ─┘      ▲                                    │
                  └──────────── campaign ◀─────────────┘
```

| Module | Responsibility | Depends on |
|---|---|---|
| `program` | Program and trace types, JSON format, `lower`, `Scope` | serde |
| `gen` | Generate, mutate, and shrink programs | `program` |
| `target` | Server process, connections, log, coverage memory | a small RESP reader of its own |
| `run` | The interpreter: `Program` → `Trace` | `program`, `target` |
| `judge` | `(Program, Trace)` → findings and features | `program` |
| `reduce` | Shrink a failing program | `gen`, `run`, `judge` |
| `campaign` | Corpus, novelty, scheduling | all of the above, LibAFL |

`fuzz`, `run <file>`, `reduce <file>`, and `export <file>` all go through the
same `judge(run(program))`. Only `campaign` depends on LibAFL.

## Program

```rust
struct Program { version: u32, setup: Setup, pools: Pools, ops: Vec<Op> }
struct Setup   { module_args: Vec<(String, String)>, modules: Vec<Module> }
struct Pools   { terms: Vec<Bytes>, tags: Vec<Bytes>, nums: Vec<F64Bits>, vecs: Vec<Vec<F32Bits>> }
struct Op      { conn: u8, reply: ReplyMode, kind: OpKind }
enum ReplyMode { Await, Defer }
enum OpKind {
    Create { name, schema, opts }, Alter { index, field }, Drop { index, delete_docs },
    Write { key, fields }, Delete { key },
    Populate { index, fields, n, seed },
    Query { index, query: QueryAst, opts },
    Aggregate { index, query: QueryAst, steps, cursor: Option<Var> },
    CursorRead { cursor: Var },
    Admin { hook: Hook, args }, Barrier, Raw { argv: Vec<Bytes> },
}
```

- **`Setup` holds only what must exist before the server starts**: module load
  arguments (some `search-*` settings are immutable after load) and extra
  modules such as RedisJSON. Runtime configuration is an `Admin` op.
- **Explicit names.** Indexes, keys, and cursors are named in the program.
  Deleting an op never changes what another op sends. An op that refers to a
  missing index is still sent and draws an ordinary error.
- **`Bytes`** serialises as a JSON string when it is valid UTF-8 and as
  `{"hex": "…"}` otherwise, so invalid UTF-8 is representable. Floats are stored
  as bit patterns, so NaN, infinities, and negative zero survive and the type is
  hashable.
- **`Populate`** carries its own field list, so it does not depend on an earlier
  `Create`. It expands to `n` writes drawn from `pools` with a pinned PRNG, using
  raw integer draws only. A golden test freezes the expansion.
- **`QueryAst`** is a tree, not a string. The judge recognises related queries
  by structure.
- **`Admin`** is table-driven. Each `Hook` has an argv template and an effect,
  `PreservesResults` or `ChangesResults`. The table covers `FT.DEBUG WORKERS`
  pause, resume, and drain; GC stop, continue, force, and wait;
  `BG_SCAN_CONTROLLER`; TTL pause and expire; `DEBUG RELOAD`; and one
  `CONFIG SET` hook per allow-listed setting. `SYNC_POINT` is a table row armed
  with its auto-release timeout; few sync points are reachable without the
  coordinator, so no further design rests on it.
- **`Raw`** is always `ChangesResults`.
- **`Var`** binds a server-chosen value (a cursor id). If its source op was
  deleted, failed, or is still deferred, the op using it is recorded as skipped.
- **`conn`** is taken modulo a small fixed number of connections.

`lower(&Op, &Pools, &Bindings) -> Vec<Argv>` is a pure helper for the ops that
are plain commands. It is not the boundary for `Barrier`, which is implemented
by the interpreter.

`Scope` is a pure fold over a prefix of a program: declared indexes and schemas,
configuration, and twin indexes. The generator uses it to produce well-typed
ops. The judge uses a second fold over only those ops whose recorded reply
succeeded, starting from `Setup`.

## Execution

The interpreter owns all execution state: connections, pending replies and the
ops they belong to, bindings, deadlines, and the server process.

### Lifecycle

1. **Start.** Create a private temporary directory and a zeroed coverage segment.
   Spawn `redis-server` in its own process group, listening on a unix socket in
   that directory, with stdout and stderr redirected to a log file. Wait for
   readiness or record `StartupFailed`.
2. **Execute** ops in order. For `Await`, send and read the reply. For `Defer`,
   send and leave the reply pending. Pending replies on a connection are read
   when that connection is next used, at a `Barrier`, and at the end. Every
   read has a deadline.
3. **Release.** On any deadline expiry, and always before the final collection,
   a harness-owned control connection sends a fixed release list: resume
   workers, resume the background scan, unpause TTL, continue the GC schedule,
   clear sync points.
4. **Stop.** Collect remaining replies, send `SHUTDOWN NOSAVE`, and wait.
5. **Abort** if the server is still unresponsive after the release list: send
   `SIGABRT` to obtain a backtrace, then kill the process group.
6. **Reap** the whole process group, then read the coverage segment and the log.

```rust
enum Exit { Clean, Died { status, at_step }, Hung { at_step }, Stalled { at_step }, StartupFailed }
```

`Stalled` means replies arrived only after the release list: the program held
its own controls. It is not a finding. `Hung` means they did not. The trace
records whether a terminating signal came from the harness.

### Trace

```rust
struct Trace { steps: Vec<Step>, exit: Exit, log: Bytes, edges: Vec<u8> }
struct Step  { op: usize, argv: Argv, sent: u32, received: Option<u32>, reply: Reply, micros: u64 }
```

`sent` and `received` are sequence numbers on one harness-side clock, so the
judge can tell which ops ran inside a deferred op's window. `Populate` records a
summary (count, error count, first error). A reply is a value, a server error,
a protocol error, a timeout, or skipped.

### Barrier

`Barrier` reads all pending replies, then on the control connection polls until
`FT.INFO` reports `indexing` as 0 for every index in `FT._LIST` and
`FT.DEBUG WORKERS STATS` reports no pending and no in-progress jobs, then calls
`GC_WAIT_FOR_JOBS`. It polls worker statistics because `WORKERS DRAIN` can
deadlock the main thread. If the barrier times out it is recorded as
inconclusive; that is never a finding.

### Server environment

- `redis-server` is an AddressSanitizer build; the module is built with
  `SAN=address`, `ENABLE_ASSERT=1`, and `FUZZ_COV=1`.
- Server flags: `--enable-debug-command yes`, `--save ""`, `--appendonly no`.
- `ASAN_OPTIONS` with `halt_on_error=1` and `detect_odr_violation=0`.
- Leak checking on Linux only, with `RS_GLOBAL_DTORS=1` and the suppressions
  file the Python tests use.

## Judge

`judge(&Program, &Trace) -> Verdict { findings, features }` is pure.

**Safety findings**: the server died; a sanitizer report appears anywhere in the
log (reports from forked children land there); an assertion failed; the exit is
`Hung`; a leak was reported at shutdown; a reply violated the protocol. The
signature is the sanitizer summary line, the assertion location, or the leading
module frames of the abort backtrace.

**Logic findings** are rules over replies already in the trace:

- **Equivalence.** The same query on the same index, with only `PreservesResults`
  ops between the two, returns the same total, and the same id set when the
  total is under a cap.
- **Twin.** Two indexes with the same schema and prefix return the same key set
  for the same query after a conclusive barrier. Order and scores are not
  compared.
- **Partition.** For queries `q`, `q p`, and `q -p` on the same index with no
  `ChangesResults` op between them, the totals add up.

A rule instance is skipped when the reply-confirmed scope does not show the
fail-on-timeout policy, when any query in it timed out or errored, when a
deferred query's window overlaps a `ChangesResults` op, when a barrier between
them was inconclusive, or when the query is not exact (approximate KNN, prefix
expansion that can hit the expansion cap, a truncated result). The generator
has one edit that inserts such a group of related queries.

**Features** are the coverage edges plus reply classes (op kind and outcome
class) hashed into a reserved tail of the same map.

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

One edit set serves both mutation and reduction:

| Edit | Smaller? |
|---|---|
| Delete a range of ops | yes |
| Replace an op with its lowered `Raw` ops (`Lower`) | no |
| Simplify a leaf (shorter bytes, 0 or 1, fewer fields, smaller `n`) | yes |
| Insert a generated op, typed against `Scope` | no |
| Splice a range from another program | no |
| Regenerate one field from the pools | no |
| Insert a related-query group | no |
| Edit `Raw` argv tokens: delete, duplicate, swap, replace from a pool | no |

`Lower` does not apply to ops that define or use a `Var`. `mutate` samples one
edit. `smaller` enumerates the smaller edits, largest first.

`reduce` keeps a smaller candidate when `judge(run(candidate))` still contains
the original signature, and repeats until none is kept. It then runs the result
ten times and records the reproduction rate. Lowering a `Populate` exposes its
writes to range deletion.

## Campaign

LibAFL 0.16.1 supplies the corpus, novelty, scheduling, and statistics.

- `Program` implements `Input` with JSON `to_file` and `from_file`, and `HasLen`.
- One executor calls `run` then `judge`, copies `edges` and the features into a
  map observer, and stores the verdict in a second observer.
- Feedback is max-map novelty over that one map.
- The objective is a finding whose signature is neither in a known-signatures
  file nor already present as a finding directory.
- The scheduler is the length-and-time minimizer over a queue.
- One mutational stage with a small iteration cap wraps a mutator that calls
  `gen::mutate`, seeded from LibAFL's state RNG.
- Calibration, input and corpus minimisation, the fork server, the restarting
  manager, and the launcher are not used.

If this wiring is not running within the budget set in the tasks, it is
replaced by a hand-written loop of about two hundred lines (a corpus directory,
a seen-edges table, a weighted pick) behind the same `campaign` interface.

Several cores are used by running several processes that share a corpus
directory and a findings directory.

## Findings

```
findings/<signature-hash>/
  program.json     original failing program
  reduced.json     written by reduce
  trace.json
  server.log
  manifest.json    module git SHA, Redis version, sanitizer options, reproduction rate
```

The native `run` command is the authoritative reproducer. `export` is limited
to programs with one connection, no deferred replies, and no variables.

## Edge cases

- **Server fails to start.** Recorded as `StartupFailed`, never as a finding;
  the campaign stops after repeated failures.
- **Program leaves workers paused.** Resolved by the release list; `Stalled`.
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
- **Not verified:** the coverage build on Linux, including symbol binding
  against an AddressSanitizer `redis-server`; the reproduction rate and
  stability of real programs.
