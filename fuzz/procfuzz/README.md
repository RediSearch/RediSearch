# procfuzz

Coverage-guided fuzzing of stateful programs against a server process.

A program is a sequence of ops sent over several connections to one server.
Each op awaits its reply or leaves it for later, and can bind a value from
its reply (such as a cursor id) to a variable that later commands use. Ops
are raw commands, barriers that wait for background work, or the target's
own typed ops, which lower to commands.

A target supplies three things:

- its typed ops, implementing `program::TypedOp`: the commands each sends,
  its literals, and how it gets simpler;
- `server::Server`: how to start the server, its wire protocol, how to tell
  it is idle, how to lift pauses a program set, and optionally how to read
  its own crash reports;
- a generator, as a closure that proposes ops to follow a prefix.

`procfuzz` supplies the rest, each piece usable on its own:

- `run::execute` starts a fresh server with edge coverage, runs a program,
  and finds sanitizer reports, crashes, hangs, and protocol violations.
- `edit` mutates programs, lists strictly smaller ones, and reduces a failing
  one.
- `campaign::fuzz` searches for programs that reach new edges (LibAFL).
- `findings::Findings` keeps one directory per finding, shared by workers.
- `session` composes these: replay a program, reduce one to a finding, fuzz
  in parallel threads.

Coverage comes from SanitizerCoverage's `trace-pc-guard`: build the server
with `-fsanitize-coverage=trace-pc-guard` and link `shim/coverage.c` into it
uninstrumented. The shim writes edges to the shared memory named in
`PROCFUZZ_COVERAGE_SHM`, so edges from forked children and from a crashing
process are kept. Progress is logged with the `log` crate.
