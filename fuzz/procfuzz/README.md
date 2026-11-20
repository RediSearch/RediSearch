# procfuzz

Coverage-guided fuzzing of stateful systems: servers, and anything else a
program of ops can drive.

A target implements one trait, `model::Model`:

- `next` makes an op that makes sense in the current state, from the random
  decisions in a `Draw`;
- `apply` folds an op, and its reply if there is one, into that state;
- `lower` turns an op into the one input it sends, with names such as a
  server-chosen cursor id resolved against the live state;
- `check` judges a finished run and may record features worth reaching;
- `reproduction` may turn a run that showed a finding into a standalone
  script.

The engine stores the draws that made each op, so it mutates a program by
changing a draw and shrinks one by deleting ops and lowering draws. A target
writes no mutator and no shrinker.

A system is anything that starts sessions (`system::System`; any
`Fn() -> io::Result<impl Session>` is one). `service::Service` is the system
for a process answering requests on a Unix socket, assembled from a `Codec`,
a launch command, and hooks that heal pauses, tell when the process is idle,
and read findings from its log. It records edge coverage, sanitizer reports,
crashes, and hangs.

The rest is usable on its own:

- `system::run` runs a program and judges it;
- `edit::mutate` and `edit::shrink`;
- `campaign::fuzz` searches for programs that reach new features;
- `findings::Findings` keeps one directory per finding, shared by workers;
- `commands` reduces a program to a finding and writes it, fuzzes in
  parallel threads, and generates programs; `commands::Command` is those commands as a
  clap subcommand, for a target's own parser beside its own flags;
- `interrupt::on_signals` makes SIGINT and SIGTERM stop cleanly.

Coverage comes from SanitizerCoverage's `trace-pc-guard`: build the target
with `-fsanitize-coverage=trace-pc-guard` and link `shim/coverage.c` into it
uninstrumented. The shim writes edges to the shared memory named in
`PROCFUZZ_COVERAGE_SHM`, so edges from forked children and from a crashing
process are kept. Progress is logged with the `log` crate.

`procfuzz-redis` adds Redis: the RESP2 codec and `redis-server` with modules
as a `Service`.
