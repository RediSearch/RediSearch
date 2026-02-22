# Developer Getting Started Guide

## Cloning the Project

Clone RediSearch with all its git submodule dependencies:

```bash
git clone --recursive https://github.com/RediSearch/RediSearch.git
cd RediSearch
```

If you already cloned without `--recursive`, initialize the submodules:

```bash
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

```bash
cd .install
./install_script.sh
cd ..
```

This uses your system's native package manager (apt, yum, homebrew, etc.).

#### nextest

Extra dependencies not yet installed through the install script is `nextest`.

If you have `cargo-binstall` available, install it with:

```bash
cargo binstall cargo-nextest --secure
```

Or:

```bash
cargo install cargo-nextest --locked
```

### Alternative: Dev Container

A dev container based on `ubuntu:latest` is available with all dependencies pre-installed. Open the repository in VS Code with the Dev Containers extension, and it will set up the environment automatically.

### Alternative: Nix

If you prefer Nix, skip the install scripts and start a development environment:

```bash
nix develop github:chesedo/redisearch-nix-env
```

This also builds Redis from source and includes Python test dependencies.

To run the coverage tests, you’ll need nightly Rust. You can get that here

```bash
nix develop github:chesedo/redisearch-nix-env#nightly
```

### Installing Redis

RediSearch requires `redis-server` in your PATH. We recommend building Redis from source since RediSearch `master` often requires unreleased features:

```bash
git clone https://github.com/redis/redis.git
cd redis
make
sudo make install
cd ..
```

## Building the Project

RediSearch has two main CLIs at the moment the (old, legacy) `MAKEFILE` and the new preferred `build.sh` file. You will sometimes see the redis team share commands using old CLI for particular things that the new CLI might not be capable of yet. We will however only use the new CLI in this guide.

```bash
# Build with release optimizations
./build.sh

# Build in debug mode
./build.sh DEBUG

# Force a fresh rebuild (useful after switching branches)
./build.sh FORCE

# Build including test binaries
./build.sh TESTS
```

The compiled module is located at:
```
bin/<target>-<release|debug>/search-community/redisearch.so
```

## Running Tests

### Unit Tests (C/C++)

Build and run C/C++ unit tests:

```bash
./build.sh RUN_UNIT_TESTS
```

### Rust Tests

Build and run Rust tests:

```bash
./build.sh RUN_RUST_TESTS
```

For Rust coverage tests, install the nightly toolchain first:

```bash
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

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or via pip
pip install uv
```

Create and activate a virtual environment:

```bash
uv venv --seed
source .venv/bin/activate
```

Install test dependencies:

```bash
uv sync --locked --all-packages
```

#### Running Python Tests

With the virtual environment activated:

```bash
# Run all Python tests
./build.sh RUN_PYTEST

# Run a specific test file
./build.sh RUN_PYTEST TEST=<test_file_name>

# Run a specific test function
./build.sh RUN_PYTEST TEST=<test_file_name>:<test_function_name>
```

#### Skipping RedisJSON Tests

Some tests require RedisJSON. To skip them:

```bash
./build.sh RUN_PYTEST REJSON=0
```

Or specify a path to an existing RedisJSON module:

```bash
./build.sh RUN_PYTEST REJSON_PATH=/path/to/redisjson.so
```

### Running All Tests

To build and run all tests (unit, Rust, and integration):

```bash
./build.sh RUN_TESTS
```

### Sanitizers

Currently address sanitizer is supported (Linux only). To run the tests with address sanitizer you can use the following command:

```bash
./build.sh RUN_TESTS SAN=address
```

## Debugging Tests

### C/C++ Unit Tests

Unit tests are compiled into standalone binaries that can be loaded into `lldb` or `gdb` as-is. 

C unit test artifacts can be found in this folder: `bin/<your target>-<release or debug>/search-community/tests/ctests/`.

C++ Unit tests use the [Google Test Framework](https://github.com/google/googletest) and are compiled into this binary `bin/<your target>-<release or debug>/search-community/tests/cpptests/rstest`.

Run a specific C++ test:

```bash
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
   
   ```bash
   lldb redis-server redis.conf
   # or
   gdb redis-server redis.conf
   ```

3. Set up your breakpoints/watchpoints and run the binary.

4. Run the integration tests:
   
   ```bash
   ./build.sh RUN_PYTEST EXT=1 TEST=<name of the test>
   ```

## Benchmarking RediSearch

```bash
PATH=$PATH:~/GitHub/RediSearch/bench_tools redisbench-admin run-local \
    --module_path ~/GitHub/RediSearch/bin/macos-aarch64-debug/search-community/redisearch.so \
    --required-module search \
    --allowed-setups oss-standalone \
    --allowed-envs oss-standalone \
    --test search-ftsb-10K-enwiki_abstract-hashes-term-wildcard.yml
```

Use `--skip-redis-spin True` to skip spinning up a Redis instance.

## Supported Platforms

The following operating systems are supported and tested in CI:

* Ubuntu 18.04
* Ubuntu 20.04
* Ubuntu 22.04
* Ubuntu 24.04
* Debian linux 11
* Debian linux 12
* Rocky linux 8
* Rocky linux 9
* Amazon linux 2
* Amazon linux 2023
* Mariner 2.0
* Azure linux 3
* MacOS
* Alpine linux 3

### Platform-specific compiler requirements

- Ubuntu 18.04: GCC 10 (not default, installed via PPA)
- Ubuntu 20.04: GCC 10 (not default, installed via PPA)
- Ubuntu 22.04: GCC 12 (not default, PPA not required)
- Ubuntu 24.04: Default GCC is sufficient
- Debian 11: Default GCC is sufficient
- Debian 12: Default GCC is sufficient
- Rocky Linux 8: GCC 13 (not default, installed via gcc-toolset-13-gcc and gcc-toolset-13-gcc-c++)
- Rocky Linux 9: GCC 14 (not default, installed via gcc-toolset-14-gcc and gcc-toolset-14-gcc-c++)
- Amazon Linux 2: GCC 11 (not default, installed via Amazon's SCL)
- Amazon Linux 2023: Default GCC is sufficient
- Mariner 2.0: Default GCC is sufficient
- Azure Linux 3: Default GCC is sufficient
- MacOS: Install clang-18 via brew
- Alpine Linux 3: Default GCC is sufficient
