# Add a stateful, coverage-guided fuzzer for RediSearch

## Why

Many RediSearch bugs are only reachable through long command sequences against a
live server: an index is created, populated, altered, queried while workers are
paused, garbage-collected in a forked child, reloaded from RDB. Unit tests and
the Python flow tests exercise the sequences someone thought to write.
`tests/pytests/test_fuzz.py` randomises documents and queries but not the
sequence, the configuration, or the scheduling.

An earlier attempt (PR #10560) showed that this class of testing pays off: it
found several distinct memory-safety and assertion failures, one of them
reachable only with worker threads enabled. It also showed what makes such a
tool untrustworthy:

- The server was reused between test cases, so some findings depended on state
  the saved repro did not contain.
- The model of the server was never corrected by the server's replies, so a
  failed `FT.CREATE` wasted the rest of the sequence.
- Feedback was a small set of reply-shape features, with no code coverage.
- Crashes in the fork-GC child were invisible.
- The repro shipped to developers was replayed by different code from the one
  that validated it.

This change replaces that attempt with a design built around trustworthy
reproduction first, and depth of state second.

## What Changes

Add a standalone developer tool under `fuzz/` and one build option. No shipped
behaviour, command, reply, configuration default, or persistence format changes.

- **A fuzzer binary** with four subcommands: `fuzz` (run a campaign), `run`
  (replay one program), `reduce` (shrink a failing program), and `export`
  (render a simple program as a script). All four share one execution path.
- **A test-case format.** A program is a readable, versioned JSON file: load-time
  module settings plus a list of typed operations with explicit object names.
  Programs are the corpus, the seeds, and the reproducers.
- **Hermetic execution.** Every program runs against a freshly started
  `redis-server`, which is never reused.
- **Scheduling as input.** Worker pause and resume, forced GC, background-scan
  control, TTL control, reload, and runtime configuration are ordinary
  operations in the program, using existing `FT.DEBUG` hooks. A query can be
  left pending on one connection while another connection changes the index.
- **Oracles.** Crashes, sanitizer reports (including in forked children), failed
  assertions, hangs, and leaks; plus logic checks that need no model of search
  semantics: results unchanged across operations that must not change them,
  agreement between an index and an identically defined twin, and partition
  counts.
- **Edge coverage from the real module.** A new build option, `FUZZ_COV=1`,
  instruments the C and Rust code and links a small shim into `redisearch.so`
  that reports edges through shared memory. It is off by default and is not
  used by any shipped build.
- **Findings as bundles.** Each finding is a directory holding the program, the
  trace, the server log, and a manifest naming the builds and sanitizer options.

### Non-goals

- Coordinator and replication topologies.
- Deterministic control of thread interleavings; flaky findings carry a measured
  reproduction rate instead.
- Fuzzing the RESP wire format, which belongs to Redis.
- Running in pull-request CI. Campaigns are run on demand or on a schedule.
