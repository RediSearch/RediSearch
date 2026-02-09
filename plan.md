# Buck2 Build System Setup for RediSearch

## Context

RediSearch currently uses CMake (C/C++) + Cargo (Rust) orchestrated by `build.sh`. The goal is to create a Buck2 build that can:
1. Build all C/C++ code (deps + core)
2. Build all Rust code (37 workspace members)
3. Link everything into the final `redisearch.so` shared library
4. Run C++ tests (Google Test), C tests, and Rust tests

The repo already has a basic Buck2 scaffold (`.buckroot`, `.buckconfig`, `toolchains/BUCK` with demo toolchains, empty root `BUCK`).

## Approach: Incremental, Practical

Given the complexity (~65 C files, ~15 C deps, 37 Rust crates, 18 FFI crates), we take a layered approach:
- **C deps**: `cxx_library` for simple deps, `prebuilt_cxx_library` for complex ones (VectorSimilarity, libuv, hiredis)
- **Core C code**: `cxx_library` with explicit source lists
- **Rust crates**: `rust_library` targets written manually for each workspace member
- **Third-party Rust deps**: `reindeer` to generate BUCK files from Cargo.toml
- **Final .so**: Custom rule or `cxx_library(preferred_linkage = "shared")` linking C + Rust static libs

---

## Step 1: Toolchain Configuration

**File: `toolchains/BUCK`**

Replace `system_demo_toolchains()` with explicit toolchain configuration:

```python
load("@prelude//toolchains:cxx.bzl", "system_cxx_toolchain")
load("@prelude//toolchains:rust.bzl", "system_rust_toolchain")
load("@prelude//toolchains:python.bzl", "system_python_bootstrap_toolchain")
load("@prelude//toolchains:genrule.bzl", "system_genrule_toolchain")

system_cxx_toolchain(
    name = "cxx",
    visibility = ["PUBLIC"],
)

system_rust_toolchain(
    name = "rust",
    default_edition = "2024",
    visibility = ["PUBLIC"],
)

system_python_bootstrap_toolchain(
    name = "python_bootstrap",
    visibility = ["PUBLIC"],
)

system_genrule_toolchain(
    name = "genrule",
    visibility = ["PUBLIC"],
)
```

---

## Step 3: C/C++ Dependencies (`deps/`)

**File: `deps/BUCK`** (single BUCK file for all simple deps)

### Simple OBJECT-equivalent deps → `cxx_library`

Each simple dep becomes a `cxx_library`:

| Target | Sources | Notes |
|--------|---------|-------|
| `//deps:rmutil` | 8 .c files | Redis module utilities |
| `//deps:friso` | 9 .c files | Chinese tokenizer, needs `-Wno-tautological-compare` |
| `//deps:snowball` | glob `src_c/*.c`, `libstemmer/*.c`, `runtime/*.c` | Stemming; include `include/` |
| `//deps:phonetics` | `double_metaphone.c` | Single file |
| `//deps:fast_float` | `fast_float_strtod.cpp` | C++, defines: `FASTFLOAT_SKIP_WHITE_SPACE`, `FASTFLOAT_ALLOWS_LEADING_PLUS` |
| `//deps:cndict` | `cndict/cndict_data.c` | Chinese dictionary data |
| `//deps:libnu` | glob `libnu/*.c` | Unicode library |
| `//deps:miniz` | `miniz/miniz.c` | Compression |
| `//deps:thpool` | `thpool/thpool.c`, `thpool/barrier.c` | Thread pool |
| `//deps:geohash` | `geohash/geohash.c`, `geohash/geohash_helper.c` | Geospatial |
| `//deps:redismodules_sdk` | (headers only) | `exported_headers` for `redismodule.h` |

### Complex deps → `prebuilt_cxx_library` (Phase 1)

These are built externally via CMake and referenced as prebuilt:

| Target | Library | Notes |
|--------|---------|-------|
| `//deps:vectorsimilarity` | `libVectorSimilarity.a` | C++20, complex build with spaces subdir, optional SVS |
| `//deps:libuv` | `libuv_a.a` | Async I/O, complex platform-specific build |
| `//deps:boost` | (headers only) | Header-only, needs fetch/path |

For Phase 1, we'll use `prebuilt_cxx_library` pointing to CMake-built artifacts. These can be migrated to native Buck2 builds later.

### System deps

```python
# OpenSSL - system dependency
prebuilt_cxx_library(
    name = "openssl",
    exported_linker_flags = ["-lssl", "-lcrypto"],
    visibility = ["PUBLIC"],
)
```

---

## Step 4: Core C Library (`src/`)

**File: `src/BUCK`**

### Sub-module libraries

Each `src/` subdirectory that currently builds as a separate CMake target becomes a `cxx_library`:

| Target | Sources | Notes |
|--------|---------|-------|
| `//src/buffer:buffer` | `buffer.c` | |
| `//src/iterators:iterators` | glob `*.c` | |
| `//src/wildcard:wildcard` | `wildcard.c` | |
| `//src/index_result:index_result` | `index_result.c` | + query_term subdir |
| `//src/value:value` | `value.c` | |
| `//src/util:mempool` | `mempool/mempool.c` | |
| `//src/util:arr` | `arr/arr.c` | |
| `//src/util:dict` | `dict/dict.c` | |
| `//src/util:hash` | `hash/*.cpp` | C++20, needs Boost headers |
| `//src/coord:coord` | glob `*.c`, `*.cpp`, `rmr/*.c` | Cluster coordination |
| `//src/ttl_table:ttl_table` | glob `*.c` | |
| `//src/geometry:geometry` | glob `*.cpp` | C++20, needs Boost |

These can either be individual `BUCK` files per directory or consolidated in `src/BUCK`.

### Main `rscore` library

```python
cxx_library(
    name = "rscore",
    srcs = glob([
        "*.c",
        "pipeline/*.c",
        "aggregate/*.c",
        "aggregate/expr/*.c",
        "aggregate/functions/*.c",
        "aggregate/reducers/*.c",
        "command_info/*.c",
        "hybrid/*.c",
        "hybrid/parse/*.c",
        "ext/*.c",
        "hll/*.c",
        "query_parser/v1/*.c",
        "query_parser/v2/*.c",
        "util/*.c",
        "trie/*.c",
        "info/*.c",
        "info/info_redis/*.c",
        "info/info_redis/threads/*.c",
        "info/info_redis/types/*.c",
        "module-init/*.c",
        "obfuscation/*.c",
        "profile/*.c",
    ]),
    # ... headers, deps, preprocessor_flags
)
```

### Combined `redisearch_c` target

```python
cxx_library(
    name = "redisearch_c",
    exported_deps = [
        ":rscore",
        "//src/buffer:buffer",
        "//src/iterators:iterators",
        # ... all sub-modules
        "//deps:rmutil",
        "//deps:friso",
        "//deps:snowball",
        # ... all deps
    ],
    visibility = ["PUBLIC"],
)
```

### Key preprocessor flags (from CMakeLists.txt)

```python
preprocessor_flags = [
    "-DREDISEARCH_MODULE_NAME=\"search\"",
    "-DREDISMODULE_SDK_RLEC",
    "-D_GNU_SOURCE",
    "-DREDIS_MODULE_TARGET",
]
```

### Key compiler flags

```python
compiler_flags = [
    "-fPIC", "-g", "-pthread",
    "-fno-strict-aliasing",
    "-Wno-unused-function",
    "-Wno-unused-variable",
    "-Wno-sign-compare",
    "-fcommon",
    "-funsigned-char",
    "-Wformat",
]
```

---

## Step 5: Third-Party Rust Dependencies (reindeer)

**Directory: `third-party/rust/`**

### Setup reindeer

1. Create `third-party/rust/Cargo.toml` mirroring the workspace dependencies from `src/redisearch_rs/Cargo.toml` `[workspace.dependencies]` section (only the external crates.io deps, ~45 crates)
2. Create `third-party/rust/reindeer.toml` configuration
3. Run `reindeer --third-party-dir=third-party/rust vendor` to download sources
4. Run `reindeer --third-party-dir=third-party/rust buckify` to generate BUCK files
5. Create fixups for crates with `build.rs` (e.g., `bindgen`, `cc`, `syn`, `ahash`, etc.)

### reindeer.toml

```toml
[vendor]
# Vendor sources locally
vendor = true

[buck]
file_name = "BUCK"

# Map to the cell reference
[buck.buckfile_imports]
```

### Key fixups needed

Crates with `build.rs` that need fixups:
- `ahash` (build.rs for feature detection)
- `bindgen` (build.rs)
- `libc` (build.rs for cfg detection)
- `cc` (build.rs)
- `redis-module` (git dep with build.rs — may need special handling)

The `redis-module` git dependency needs to be included in the third-party Cargo.toml with its git rev.

---

## Step 6: Rust Workspace Members

**Files: One `BUCK` per crate directory under `src/redisearch_rs/`**

Each of the 37 workspace members gets a `rust_library` (or `rust_test`) target. Example for a typical pure Rust crate:

```python
# src/redisearch_rs/trie_rs/BUCK
rust_library(
    name = "trie_rs",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/redisearch_rs/thin_vec:thin_vec",
        "//src/redisearch_rs/wildcard:wildcard",
        "//third-party/rust:memchr",
        # ... other deps from Cargo.toml
    ],
    visibility = ["PUBLIC"],
)
```

### FFI crates (18 in `c_entrypoint/`)

Each FFI crate wraps a Rust library for C consumption. These have `build.rs` scripts that run `cbindgen` to generate C headers. For Buck2:

- Option A: Use `genrule` to run cbindgen separately, making headers available as a `filegroup`
- Option B: Pre-commit the generated headers (already done — they exist in `src/redisearch_rs/headers/`) and treat them as source

**Recommended: Option B** — the headers are already committed in `src/redisearch_rs/headers/`. We can reference them directly and skip cbindgen in the Buck2 build. The CMake/Cargo build will keep them up to date.

### Main FFI entrypoint crate

```python
# src/redisearch_rs/c_entrypoint/redisearch_rs/BUCK
rust_library(
    name = "redisearch_rs",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    preferred_linkage = "static",
    deps = [
        "//src/redisearch_rs/c_entrypoint/fnv_ffi:fnv_ffi",
        "//src/redisearch_rs/c_entrypoint/inverted_index_ffi:inverted_index_ffi",
        "//src/redisearch_rs/c_entrypoint/iterators_ffi:iterators_ffi",
        # ... all 17 FFI crate deps
        "//third-party/rust:redis-module",
    ],
    visibility = ["PUBLIC"],
)
```

### Crate count summary

| Category | Count | Target type |
|----------|-------|------------|
| Pure Rust libs | ~14 | `rust_library` |
| FFI crates | 18 | `rust_library` |
| Build tools | 2 | `rust_binary` (tools/) |
| Benchmarkers | 4 | `rust_binary` |
| Test utils | 3 | `rust_library` (test-only) |
| workspace_hack | 1 | `rust_library` |

---

## Step 7: Final Shared Library

**File: Root `BUCK`**

Link `redisearch_c` (static) + `redisearch_rs` (static) + system libs into `redisearch.so`:

```python
cxx_library(
    name = "redisearch",
    preferred_linkage = "shared",
    exported_deps = [
        "//src:redisearch_c",
        "//src/redisearch_rs/c_entrypoint/redisearch_rs:redisearch_rs",
    ],
    exported_linker_flags = select({
        "config//os:macos": ["-undefined", "dynamic_lookup"],
        "config//os:linux": ["-Bsymbolic", "-Bsymbolic-functions"],
    }),
    soname = "redisearch.so",
    visibility = ["PUBLIC"],
)
```

Note: Linking a `rust_library` (staticlib) into a `cxx_library` (shared) may require a custom rule or `prebuilt_cxx_library` wrapper around the Rust static lib output. We'll validate this during implementation and adjust.

---

## Step 8: Tests

### C++ Tests (Google Test)

**File: `tests/cpptests/BUCK`**

```python
# Google Test dependency
cxx_library(
    name = "gtest",
    # ... or use prebuilt/http_archive
)

cxx_test(
    name = "rstest",
    srcs = glob(["test_cpp_*.cpp"]) + ["common.cpp", "index_utils.cpp", "iterator_util.cpp", "stacktrace.cpp"],
    deps = [
        "//deps/googletest:gtest",
        "//:redisearch",
        "//tests/cpptests/redismock:redismock",
    ],
)
```

### C Tests

**File: `tests/ctests/BUCK`**

Each `test_*.c` file becomes a `cxx_test`:

```python
[cxx_test(
    name = f.removesuffix(".c"),
    srcs = [f],
    deps = ["//:redisearch"],
) for f in glob(["test_*.c"])]
```

### Rust Tests

Each `rust_library` that has tests gets a companion `rust_test`:

```python
rust_test(
    name = "trie_rs_test",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [":trie_rs", /* test deps */],
)
```

---

## Step 9: File Layout Summary

```
.buckconfig                          # Already exists, update if needed
.buckroot                            # Already exists
BUCK                                 # Root: final redisearch target + aliases
toolchains/BUCK                      # Toolchain config (update)
third-party/rust/                    # NEW: reindeer-managed Rust deps
    Cargo.toml                       # External Rust deps
    reindeer.toml                    # Reindeer config
    fixups/                          # Build script fixups
    BUCK                             # Auto-generated by reindeer
deps/BUCK                            # C/C++ dependencies
deps/VectorSimilarity/BUCK           # Complex dep (or prebuilt)
deps/googletest/BUCK                 # Test framework
src/BUCK                             # Core C library + sub-modules
src/command_info/BUCK                # Code generation
src/geometry/BUCK                    # C++ geometry (Boost)
src/coord/BUCK                       # Cluster coordination
src/redisearch_rs/BUCK               # Rust workspace root (if needed)
src/redisearch_rs/trie_rs/BUCK       # Example: one per Rust crate
src/redisearch_rs/buffer/BUCK
src/redisearch_rs/.../BUCK           # ... (one per crate, ~37 files)
src/redisearch_rs/c_entrypoint/redisearch_rs/BUCK  # Main FFI entrypoint
tests/cpptests/BUCK                  # C++ tests
tests/ctests/BUCK                    # C tests
```

Total new/modified files: ~50-55 BUCK files + third-party setup.

---

## Implementation Order

1. **`toolchains/BUCK`** — proper toolchain setup
2. **`deps/BUCK`** — simple C deps as `cxx_library`
3. **`src/BUCK`** — core C code, verify it compiles
4. **`third-party/rust/`** — reindeer setup for external Rust deps
5. **`src/redisearch_rs/*/BUCK`** — Rust workspace members (start with leaf crates, work up)
6. **Root `BUCK`** — final `redisearch.so` linking
7. **`tests/*/BUCK`** — test targets
8. **Complex deps** — migrate VectorSimilarity, libuv, hiredis from prebuilt to native

## Verification

1. `buck2 build //deps/...` — verify all C deps compile
2. `buck2 build //src:redisearch_c` — verify core C library
3. `buck2 build //src/redisearch_rs/...` — verify all Rust crates
4. `buck2 build //:redisearch` — verify final shared library links
5. `buck2 test //tests/...` — run C/C++ tests
6. `buck2 test //src/redisearch_rs/...` — run Rust tests

## Open Questions / Risks

1. **Rust ↔ C linking**: Buck2's `rust_library` with `preferred_linkage = "static"` should produce a `.a` that can be consumed by `cxx_library`, but the exact mechanics of linking a Rust staticlib into a C shared library may need a custom rule or `prebuilt_cxx_library` wrapper.
2. **VectorSimilarity**: This is a complex C++20 library with platform-specific optimizations (SVS/Intel). Starting as `prebuilt_cxx_library` is safest.
3. **Reindeer fixups**: Some Rust deps with `build.rs` scripts (especially `redis-module` git dep) may need non-trivial fixups.
4. **cbindgen headers**: Using pre-committed headers avoids running cbindgen in Buck2, but means Buck2 builds may diverge if headers aren't kept in sync. This is acceptable since CMake/Cargo remains the primary build.
5. **Boost**: Header-only library needs to be fetched. Can use `http_archive` to download or point to a local path.
6. **Platform select()**: Some flags differ between macOS and Linux. Buck2's `select()` with platform constraints handles this.
