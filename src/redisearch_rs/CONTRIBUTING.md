# Rust Developer Documentation

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

## Invoking foreign C symbols in tests and benchmarks

Some Rust modules rely on functionality that's provided by C modules.
That's not an issue when it comes to _compilation_, but it becomes a challenge in Rust tests and benchmarks: those symbols will be invoked, therefore they must be defined.

### Our solution

The CMake build creates `libredisearch_all.a`, a unified static library that bundles all C/C++ dependencies (including VectorSimilarity, SVS, spdlog, etc.). Rust crates that need to link against C code use the `build_utils::bind_foreign_c_symbols()` function in their `build.rs` to link this library.

They must also depend on `redisearch_rs` and invoke `redis_mock::mock_or_stub_c_symbols!()` to ensure that C symbols defined in Rust are available.

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
   redis_mock::mock_or_stub_c_symbols!();
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
   redis_mock::mock_or_stub_c_symbols!();
   ```
