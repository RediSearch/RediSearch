# kvd

A small multi-threaded RESP server with three planted bugs (a crash, a wrong
answer behind the `--compact` launch flag, and a hang), its `procfuzz` model,
and `kvd-fuzz`, a fuzzer for it. `cargo test -p kvd` reproduces each bug from
a fixed program and shrinks padded programs to exactly the planted ones; it
needs no sanitizer build.

To fuzz it with edge coverage, build the server with the coverage shim:

```bash
clang -c -O2 ../procfuzz/shim/coverage.c -o $TMPDIR/coverage.o
RUSTFLAGS="-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3 \
  -Cllvm-args=-sanitizer-coverage-trace-pc-guard -Clink-arg=$TMPDIR/coverage.o" \
  cargo build --bin kvd --target-dir target-cov
cargo run --bin kvd-fuzz -- --kvd target-cov/debug/kvd fuzz --duration 600
```
