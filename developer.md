# Developer Getting Started Guide

## Cloning the Project

Clone RediSearch with all its git submodule dependencies:

```sh
git clone --recursive https://github.com/RediSearch/RediSearch.git
cd RediSearch
```

If you already cloned without `--recursive`, initialize the submodules:

```sh
git submodule update --init --recursive
```

## Installing Dependencies

Building and testing RediSearch requires the following dependencies:

- `rust` (latest stable version)
- `cmake >= 3.25.1`
- `boost == 1.88.0` (optional — CMake will fetch it automatically, but with a build time penalty)
- `build-essential` (on Debian/Ubuntu) or equivalent build tools on other systems
- `python3` and `python3-pip` (for running tests)
- `openssl-devel` / `libssl-dev` (for secure connections)

### Using Installation Scripts

Install all required build tools using the provided scripts:

```sh
cd .install
./install_script.sh
cd ..
```

This uses your system's native package manager (apt, yum, homebrew, etc.).

#### nextest

Extra dependencies not yet installed through the install script is `nextest`.

If you have `cargo-binstall` available, install it with:

```sh
cargo binstall cargo-nextest --secure
```

Or:

```sh
cargo install cargo-nextest --locked
```

### Alternative: Dev Container

A dev container based on `ubuntu:latest` is available with all dependencies pre-installed. Open the repository in VS Code with the Dev Containers extension, and it will set up the environment automatically.

### Installing Redis

RediSearch requires `redis-server` in your PATH. We recommend building Redis from source since RediSearch `master` often requires unreleased features.

Follow the steps in the [Redis Readme on building Redis from source](https://github.com/redis/redis#build-redis-from-source) to get it installed.

## Building the Project

RediSearch has two main CLIs at the moment the (old, legacy) `MAKEFILE` and the new preferred `build.sh` file. (The old `MAKEFILE` invokes `./build.sh` for all its actions)

Do a regular build (with release optimizations):

```sh
./build.sh
```

Build in debug mode:

```sh
./build.sh DEBUG
```

Force a fresh rebuild (useful after switching branches):

```sh
./build.sh FORCE
```

Build including test binaries:

```sh
./build.sh TESTS
```

Build flags can also be combined, e.g:

```sh
./build.sh TESTS FORCE
```

The compiled module is located at:
```
bin/<target>-<release|debug>/search-community/redisearch.so
```

## Running Tests

### Unit Tests (C/C++)

Build and run C/C++ unit tests:

```sh
./build.sh RUN_UNIT_TESTS
```

### Rust Tests

Build and run Rust tests:

```sh
./build.sh RUN_RUST_TESTS
```

For Rust coverage tests, install the nightly toolchain first:

```sh
rustup toolchain install nightly \
    --allow-downgrade \
    --component llvm-tools-preview \
    --component miri

# Tool required to compute test coverage for Rust code
cargo install cargo-llvm-cov --locked

# Make sure `miri` is fully operational before running tests with it.
# See https://github.com/rust-lang/miri/blob/master/README.md#running-miri-on-ci
# for more details.
cargo +nightly miri setup
```

### Python Tests

#### Setting Up the Python Environment

Install `uv` (a fast Python package manager):

```sh
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or via pip
pip install uv
```

Create and activate a virtual environment:

```sh
uv venv --seed
source .venv/bin/activate
```

Install test dependencies:

```sh
uv sync --locked --all-packages
```

#### Running Python Tests

With the virtual environment activated:

```sh
# Run all Python tests
./build.sh RUN_PYTEST

# Run a specific test file
./build.sh RUN_PYTEST TEST=<test_file_name>

# Run a specific test function
./build.sh RUN_PYTEST TEST=<test_file_name>:<test_function_name>
```

#### Skipping RedisJSON Tests

Some tests require RedisJSON. To skip them:

```sh
./build.sh RUN_PYTEST REJSON=0
```

Or specify a path to an existing RedisJSON module:

```sh
./build.sh RUN_PYTEST REJSON_PATH=/path/to/redisjson.so
```

### Running All Tests

To build and run all tests (unit, Rust, and integration):

```sh
./build.sh RUN_TESTS
```

### Sanitizers

Currently address sanitizer is supported (Linux only). To run the tests with address sanitizer you can use the following command:

```sh
./build.sh RUN_TESTS SAN=address
```

## Debugging Tests

### C/C++ Unit Tests

Unit tests are compiled into standalone binaries that can be loaded into `lldb` or `gdb` as-is. 

C unit test artifacts can be found in this folder: `bin/<your target>-<release or debug>/search-community/tests/ctests/`.

C++ Unit tests use the [Google Test Framework](https://github.com/google/googletest) and are compiled into this binary `bin/<your target>-<release or debug>/search-community/tests/cpptests/rstest`.

Run a specific C++ test:

```sh
bin/<your target>-<release or debug>/search-community/tests/cpptests/rstest --gtest_filter <test name>
```

### Rust Unit Tests

Rust Unit tests can be found in the appropriate target folder (which for RediSearch is here `bin/redisearch_rs/`).

### Debugging Integration Tests

By default the Python test runner will spin up redis-server instances under the hood. Pass `EXT=1` to instruct the runner to connect to an existing external instance. You may optionally use `EXT_HOST=<ip addr>` and `EXT_PORT=<port>` to connect to a non-local or non-standard-port instance.

To start the external redis-server instance:

1. Create a `redis.conf` config file in your project root:
   
   ```
   loadmodule bin/<your target>-<release or debug>/search-community/redisearch.so
   enable-debug-command yes
   ```

2. Start redis using this configuration under lldb/gdb:
   
   ```sh
   lldb redis-server redis.conf
   # or
   gdb redis-server redis.conf
   ```

3. Set up your breakpoints/watchpoints and run the binary.

4. Run the integration tests:
   
   ```sh
   ./build.sh RUN_PYTEST EXT=1 TEST=<name of the test>
   ```

## Benchmarking RediSearch

### Dependencies

#### Full-Text Search Benchmark (FTSB)

Install Full-Text Search Benchmark (FTSB) as per the instructions on https://github.com/RediSearch/ftsb

Make sure you have the `ftsb_redisearch` binary available in your `$PATH`.

#### memtier_benchmark

Install `memtier_benchmark` as per the instructions on https://github.com/redis/memtier_benchmark

Make sure you have the `memtier_benchmark` binary available in your `$PATH`.

#### Python packages

Install necessary python packages:

```sh
uv pip install -r ./tests/benchmarks/requirements.txt
```

### Run benchmarks

To run a specific benchmark, use the following command:

```sh
uv redisbench-admin run-local \
    --module_path $(find $(pwd)/bin -name "redisearch.so" | head -1) \
    --required-module search \
    --allowed-setups oss-standalone \
    --allowed-envs oss-standalone \
    --test tests/benchmarks/<benchmark>.yml
```

Replace `<benchmark>` in the `--test` argument with the desired benchmark file. Look in `tests/benchmarks` for all available benchmarks.

#### Profiling benchmarks with Samply

Install `samply` as per the instructions on https://github.com/mstange/samply

Make sure you have the `samply` binary available in your `$PATH`.

In one terminal panel run:

```sh
samply record redis-server --loadmodule $(find $(pwd)/bin -name "redisearch.so" | head -1)
```

In the other terminal panel run:

```sh
uv redisbench-admin run-local \
    --skip-redis-spin True \
    --required-module search \
    --allowed-setups oss-standalone \
    --allowed-envs oss-standalone \
    --test tests/benchmarks/<benchmark>.yml
```

## Supported Platforms

The following operating systems are supported and tested in CI on both `x86_64` and `aarch64` (with the exception of macOS 14, which is ARM64-only):

* Ubuntu 20.04 (Focal)
* Ubuntu 22.04 (Jammy)
* Ubuntu 24.04 (Noble)
* Ubuntu 26.04 (Resolute)
* Debian 12 (Bookworm)
* Debian 13 (Trixie)
* Rocky Linux 8
* Rocky Linux 9
* Rocky Linux 10
* Amazon Linux 2023
* Azure Linux 3
* Alpine Linux 3.23
* macOS 14 (ARM64)
* macOS 15
* macOS 26

### Platform-specific notes

`./install_script.sh` covers compiler and build-tool installation on every supported platform. The notes below only flag things the script cannot do for you:

- **macOS**: Homebrew must already be installed. The script will fail fast if `brew` is not on `$PATH`. Install it from https://brew.sh first.
- **Rocky Linux 8 / 9**: The script installs GCC via `gcc-toolset-13` / `gcc-toolset-14` and registers the toolset under `/etc/profile.d/`, which only takes effect in **new** shells. To use the toolset in your current shell, run `source /opt/rh/gcc-toolset-13/enable` (Rocky 8) or `source /opt/rh/gcc-toolset-14/enable` (Rocky 9).

## Updating Dependencies

### Snowball Stemmer

The snowball stemmer lives in `deps/snowball` as a git submodule. During the
build, CMake compiles the snowball compiler, runs it on the `.sbl` algorithm
files, and generates a C registry header (`modules.h`) that wires up every
stemmer.

The registry generation is handled by `cmake/generate_snowball_modules_h.cmake`,
which parses `deps/snowball/libstemmer/modules.txt` and emits the include
directives, encoding enum, module lookup table, and algorithm name list. It
replaces the upstream `libstemmer/mkmodules.pl` Perl script and filters to
UTF-8 encodings only.

When pulling in a new snowball revision:

1. Update the submodule: `git -C deps/snowball checkout <new-rev> && git add deps/snowball`
2. Check whether `libstemmer/modules.txt` has changed (new languages, renamed
   algorithms, new encodings). If the only changes are new algorithms with
   `UTF_8` encoding, the CMake script picks them up automatically.
3. If upstream added a new encoding beyond `UTF_8` that we need to support, or
   changed the format of `modules.txt`, update
   `cmake/generate_snowball_modules_h.cmake` to match.
4. Build with `./build.sh FORCE` and verify the generated
   `bin/<arch>/search-community/src/snowball/libstemmer/modules.h` looks correct.
