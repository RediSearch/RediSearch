# Rust Developer Documentation

## General Guidelines

- Pointers are shaped by which side of the boundary the crate is on:
  - `extern "C"` signatures in `c_entrypoint/*_ffi` take raw `*const T` / `*mut T`.
    Convert on entry (`NonNull::new`, `as_ref`) and state the null contract in the
    `# Safety` doc, so the generated C prototype keeps its `const` qualifiers and
    matches the Rust signature one-to-one.
  - Pure Rust crates use references, `NonNull<T>`, or `Option<NonNull<T>>` in their
    public APIs, never raw pointers — except for `as_ptr` / `into_raw` / `from_raw`
    style conversions, which follow the `std` convention; `extern "C"` callbacks,
    which are FFI signatures wherever they live; and the fields of `#[repr(C)]` types
    that mirror a C layout or are exported by cheadergen, which keep raw pointers so
    the generated header keeps the pointee qualifiers.
  - `c_wrappers/*` are exempt: they mirror C structs, so raw pointer accessors and
    setters are the interface.
- Safety Comments: Number invariants in the safety doc comment and refer to these invariants in your safety in-line comments throughout that function.
- debug_assert invariants in FFI functions
- RediSearch deals with potentially invalid UTF-8 strings so **never assume** `str` /`String` are fine for user input. Prefer `[u8]`, `Vec<u8>`, `CStr`, or `CString`.
- [`unsafe-tools`](https://github.com/JonasKruckenberg/unsafe-tools)’ `mimic` and `canary` for sized-opaque types that can be passed to C (and e.g. stack allocated)
- Know and use `Pin` when heap allocated Rust objects and pointers are at play (chances are high you can't move that object!)

## Dependencies

Dependencies should be added to the `Cargo.toml` file in the root of the workspace.
They can then be used by workspace members via:

```toml
[dependencies]
thiserror.workspace = true
```

Dependency versions should be updated:
- ASAP in case of security advisories.
- Whenever we need newer features or bug fixes released in a newer version.
- Once in a while, via [`cargo upgrade`](https://crates.io/crates/cargo-edit), if neither of the two things above have happened.

## Tests

Rust Unit tests use the regular Rust test harness and test runner. All regular Rust testing practices apply with a few specifics:

- Use [`proptest`](https://docs.rs/proptest/latest/proptest/) whenever possible, this lets us test inputs in-depth instead of superficially.
- Prefer integration tests in tests/ over in-crate unit tests, as integration tests are restricted to a crate's public interface, which is what you generally want to test. In-crate unit tests can access internal implementation details, which is only occasionally useful.
- All tests *should* pass under [miri](https://github.com/rust-lang/miri). We’re writing nuanced, tricky code and miri is invaluable in making it safe. If miri flags UB in your test and you think it's false positive, think again, then raise the issue with the team before skipping the test under miri.
*All skipped tests must have a reason for skipping attached.*

## Logging

C code uses [`RedisModule_Log`](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#redismodule_log) to log messages
while the Rust side uses the standard [`tracing`](https://docs.rs/tracing/latest/tracing/) crate, see below.

The [`tracing_redismodule`](tracing_redismodule) crate provides a bridge between the two logging systems.
It implements a tracing subscriber emitting traces and logs to the RedisModule logging system.
This subscriber is automatically registered when the RediSearch module is loaded by the Redis server.

### Logging from Rust

The recommended way for Rust code to emit logs is through [`tracing`](https://docs.rs/tracing/latest/tracing/) since it allows us to produce structured logging output, but also generate performance data through spans.
 To record events to the redis log you can use the macros provided by [`tracing`]:
 ```rust
 tracing::trace!("This is the most verbose");
 tracing::debug!("This is the second most verbose");
 tracing::info!("This is the third most verbose");
 tracing::warn!("This is the fourth most verbose");
 tracing::error!("This is the fifth most verbose");
 ```

By default, only `error`, `warn`, and `info` events are emitted but you can set the `RUST_LOG` environment variable to customize the behaviour of the system:

```bash
# enables all verbositity levels from all sources
RUST_LOG=trace 
# enables all verbositity levels from all sources EXCEPT the result_processor crate which is fully disabled.
RUST_LOG=trace,result_processor=off 
```

These directives can be chained and support quite deep configuration. For details, see the [`tracing_subscriber`](https://docs.rs/tracing-subscriber/0.3.20/tracing_subscriber/filter/struct.EnvFilter.html#directives) documentation.

> Note, the verbosity levels defined by `tracing` are NOT the same as the ones used by redis. The mapping is like so:
> - `trace` => `LOGLEVEL_DEBUG`
> - `debug` => `LOGLEVEL_VERBOSE`
> - `info` => `LOGLEVEL_VERBOSE`
> - `warn` and `error` => `LOGLEVEL_WARNING`

By default the log output will be colored to help with reading. The system already attempts to be smart about turning it off (e.g. in CI) but if you manually need to disable/enable output coloring set the `RUST_LOG_STYLE` environment variable. It supports the following values:
- `RUST_LOG_STYLE=never` never print color codes
- `RUST_LOG_STYLE=always` always print color codes
- `RUST_LOG_STYLE=auto` automatically detect when to enable or disable color codes (default)

### Logging in Tests

[`tracing_redismodule`](tracing_redismodule) is not meant to be used in Rust tests.
Instead, the [`redis_mock`](redis_mock) crate re-implements the `RedisModule_Log` so logs from C
are emitted using `tracing`.

Tests can then use the [`test-log`](https://docs.rs/test-log/latest/test_log/) crate to easily initialize `tracing`
and receive logs from both C and Rust.

```rust
#[test_log::test]
fn some_test() {
}
```

The log level can be configured by setting the `RUST_LOG` environment variable when running the tests:

```
$ RUST_LOG=debug cargo test some_test -- --nocapture
```

By default, `test-log` sets this level to `info`, but this can be overridden in the test itself if its output is too verbose.

```rust
#[test_log::test]
#[test_log(default_log_filter = "error")]
fn some_test() {
}
```

## Invoking foreign C symbols in tests and benchmarks

Some Rust modules rely on functionality that's provided by C modules.
That's not an issue when it comes to _compilation_, but it becomes a challenge in Rust tests and benchmarks: those symbols will be invoked, therefore they must be defined.

### Our solution

The CMake build creates `libredisearch_c_bundle.a`, a unified static library that bundles all C/C++ dependencies (including VectorSimilarity, SVS, spdlog, etc.). Rust crates that need to link against C code use the `build_utils::bind_foreign_c_symbols()` function in their `build.rs` to link this library.

They must also depend on `redisearch_rs` and invoke `redis_mock::mock_or_stub_missing_redis_c_symbols!()` to ensure that C symbols defined in Rust are available.

### Adding integration tests to an existing crate

Rust integration tests live in the `tests` subfolder, next to the `Cargo.toml` of the crate they refer to. To avoid linking C code in normal builds, use a feature flag:

1. **Add the `unittest` feature** to your `Cargo.toml`:
   ```toml
   [features]
   unittest = []

   [build-dependencies]
   build_utils.workspace = true

   [dev-dependencies]
   # Depend on your own crate as a dev dependency, to enable the 
   # "unittest" feature flag.
   my_crate = { path = ".", features = ["unittest"] }
   redisearch_rs = { workspace = true, features = ["mock_allocator"] }
   redis_mock.workspace = true
   ```

2. **Add a `build.rs`** to your crate (if it doesn't have one):
   ```rust
   fn main() {
       #[cfg(feature = "unittest")]
       build_utils::bind_foreign_c_symbols();
   }
   ```

3. **Ensure all C symbols are available to your tests**. In
   the root of your test suite (e.g. `tests/integration/main.rs`) add:
   ```rust
   // Link both Rust-provided and C-provided symbols
   extern crate redisearch_rs;
   // Mock or stub the ones that aren't provided by the line above
   redis_mock::mock_or_stub_missing_redis_c_symbols!();
   ```

### Adding a benchmark crate

Benchmark crates (e.g., named `*_bencher`) are pure testing code, so they don't need an additional feature flag:

1. **Configure `Cargo.toml`**:
   ```toml
   [build-dependencies]
   build_utils = { workspace = true }

   [dependencies]
   redisearch_rs = { workspace = true, features = ["mock_allocator"] }
   redis_mock.workspace = true
   ```

2. **Add a `build.rs`**:
   ```rust
   fn main() {
       build_utils::bind_foreign_c_symbols();
   }
   ```

3. **Ensure all C symbols are available to your benchmark code**:
   ```rust
   // In the root of your benching crate:
   // - Link both Rust-provided and C-provided symbols
   extern crate redisearch_rs;
   // - Mock or stub the ones that aren't provided by the line above
   redis_mock::mock_or_stub_missing_redis_c_symbols!();
   ```

## Running benchmarks

Benchmarks use [Criterion](https://bheisler.github.io/criterion.rs/book/). Run them from `src/redisearch_rs/`:

```bash
# All benchmarks of a crate
cargo bench -p rqe_iterators_bencher
# Only the benchmarks whose id matches a regex
cargo bench -p rqe_iterators_bencher -- 'Iterator - Metric'
# List the benchmark ids
cargo bench -p rqe_iterators_bencher -- --list
# Compare against an earlier run: save a baseline, change the code, then compare
cargo bench -p rqe_iterators_bencher -- --save-baseline before 'Iterator - Metric'
cargo bench -p rqe_iterators_bencher -- --baseline before 'Iterator - Metric'
```

Benchmark ids are `<group>/<function>`, e.g. `Iterator - Metric - Read Dense/Rust`.

### Profiling a benchmark with hotpath

`rqe_iterators_bencher` can report a benchmark's per-call allocations, and its
instruction counts, with [hotpath's Criterion integration](https://hotpath.rs/criterion).
Profiling is opt-in: without the features and environment variables below the
benchmarks run as plain Criterion benchmarks.

A profiling run executes a single benchmark routine exactly
`HOTPATH_CRITERION_ITERATIONS` times, in Criterion's test mode, and prints a
report. The filter must match exactly one benchmark, so run each benchmark in
its own process:

```bash
HOTPATH_CRITERION_ITERATIONS=100 cargo bench -p rqe_iterators_bencher --features hotpath \
  -- --test --exact 'Iterator - Metric - Read Dense/Rust'
```

The report has two rows:

- `criterion::routine`: each call of the benchmark routine, without setup.
- `criterion::bench`: the whole loop, including setup and dropping the inputs.

There is no warm-up, and hotpath's instrumentation adds a few nanoseconds per call:
use the regular Criterion run for timing, and the profiling run for allocations.
Allocations made by the C code are not tracked.

Pick the iteration count per benchmark: nanosecond-scale routines need many
iterations, routines taking milliseconds only a few.

To write the report as JSON instead, set `HOTPATH_OUTPUT_FORMAT=json` and
`HOTPATH_OUTPUT_PATH=<file>`.

#### Counting instructions with Callgrind

The `hotpath-callgrind` feature adds the number of instructions executed per
routine call, which is deterministic, so it is a reliable measure for comparing
two versions of the code. Build with the `callgrind` profile, which keeps the
symbols Callgrind needs by disabling link-time optimization, then run the
benchmark binary, printed on the `Executable` line, under Callgrind:

```bash
cargo bench -p rqe_iterators_bencher --features hotpath-callgrind --profile callgrind --no-run
HOTPATH_CRITERION_ITERATIONS=20 valgrind --tool=callgrind \
  ../../bin/redisearch_rs/callgrind/deps/iterators-<hash> \
  --bench --test --exact 'Iterator - Metric - Read Dense/Rust'
```

Under Callgrind the report has no timing, and running is tens of times slower,
so keep the iteration count low.

To get timing, allocations and instruction counts in a single JSON report, run
the same binary natively first, then merge its report into the Callgrind run's.
Merging also needs the `hotpath/hotpath-cloud` feature:

```bash
cargo bench -p rqe_iterators_bencher --features hotpath-callgrind,hotpath/hotpath-cloud \
  --profile callgrind --no-run
BENCH=../../bin/redisearch_rs/callgrind/deps/iterators-<hash>
ID='Iterator - Metric - Read Dense/Rust'
export HOTPATH_CRITERION_ITERATIONS=20 HOTPATH_OUTPUT_FORMAT=json
HOTPATH_OUTPUT_PATH=native.json "$BENCH" --bench --test --exact "$ID"
HOTPATH_OUTPUT_PATH=merged.json HOTPATH_MERGE_REPORT=native.json \
  valgrind --tool=callgrind "$BENCH" --bench --test --exact "$ID"
```

The timing in the merged report comes from the `callgrind` profile, without
link-time optimization, so it is slower than in a regular benchmark run.
