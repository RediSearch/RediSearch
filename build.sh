#!/usr/bin/env bash
set -e

#-----------------------------------------------------------------------------
# RediSearch Build Script
#
# This script handles building the RediSearch module and running tests.
# It supports various build configurations and test types.
#-----------------------------------------------------------------------------

# Get the absolute path to script directory
ROOT="$(cd "$(dirname "$0")" && pwd)"
BINROOT="$ROOT/bin"

#-----------------------------------------------------------------------------
# Default configuration values
#-----------------------------------------------------------------------------
COORD="oss"      # Coordinator type: oss or rlec
DEBUG=0          # Debug build flag
PROFILE=0        # Profile build flag
FORCE=0          # Force clean build flag
VERBOSE=0        # Verbose output flag
QUICK=${QUICK:-0} # Quick test mode (subset of tests)
COV=${COV:-0}    # Coverage mode (for building and testing)

# Test configuration (0=disabled, 1=enabled)
BUILD_TESTS=0    # Build test binaries
RUN_UNIT_TESTS=0 # Run C/C++ unit tests
RUN_RUST_TESTS=0 # Run Rust tests
RUN_PYTEST=0     # Run Python tests
RUN_ALL_TESTS=0  # Run all test types
RUN_MICRO_BENCHMARKS=0 # Run micro-benchmarks

# Rust configuration
RUST_PROFILE=""  # Which profile should be used to build/test Rust code
                 # If unspecified, the correct profile will be determined based
                 # the operations to be performed

#-----------------------------------------------------------------------------
# Function: parse_arguments
# Parse command-line arguments and set configuration variables
#-----------------------------------------------------------------------------
parse_arguments() {
  for arg in "$@"; do
    case $arg in
      COORD=*)
        COORD="${arg#*=}"
        ;;
      DEBUG|debug)
        DEBUG=1
        ;;
      TESTS|tests)
        BUILD_TESTS=1
        ;;
      RUN_TESTS|run_tests)
        RUN_ALL_TESTS=1
        ;;
      RUN_UNIT_TESTS|run_unit_tests)
        RUN_UNIT_TESTS=1
        ;;
      RUN_RUST_TESTS|run_rust_tests)
        RUN_RUST_TESTS=1
        ;;
      RUN_MICRO_BENCHMARKS|run_micro_benchmarks|RUN_MICROBENCHMARKS|run_microbenchmarks)
        RUN_MICRO_BENCHMARKS=1
        ;;
      COV=*)
        COV="${arg#*=}"
        ;;
      RUN_PYTEST|run_pytest)
        RUN_PYTEST=1
        ;;
      TEST=*)
        TEST_FILTER="${arg#*=}"
        ;;
      RUST_PROFILE=*)
        RUST_PROFILE="${arg#*=}"
        ;;
      SAN=*)
        SAN="${arg#*=}"
        ;;
      FORCE|force)
        FORCE=1
        ;;
      VERBOSE|verbose)
        VERBOSE=1
        ;;
      QUICK=*)
        QUICK="${arg#*=}"
        ;;
      SA=*)
        SA="${arg#*=}"
        ;;
      REDIS_STANDALONE=*)
        REDIS_STANDALONE="${arg#*=}"
        ;;
      *)
        # Pass all other arguments directly to CMake
        CMAKE_ARGS="$CMAKE_ARGS -D${arg}"
        ;;
    esac
  done
}

#-----------------------------------------------------------------------------
# Function: setup_test_configuration
# Configure test settings based on input arguments
#-----------------------------------------------------------------------------
setup_test_configuration() {
  # If any tests will be run, ensure BUILD_TESTS is enabled
  if [[ "$RUN_ALL_TESTS" == "1" || "$RUN_UNIT_TESTS" == "1" || "$RUN_RUST_TESTS" == "1" || "$RUN_PYTEST" == "1" || "$RUN_MICRO_BENCHMARKS" == "1" ]]; then
    if [[ "$BUILD_TESTS" != "1" ]]; then
      echo "Test execution requested, enabling test build automatically"
      BUILD_TESTS="1"
    fi
  fi

  # If RUN_ALL_TESTS is enabled, enable all test types
  if [[ "$RUN_ALL_TESTS" == "1" ]]; then
    RUN_UNIT_TESTS=1
    RUN_RUST_TESTS=1
    RUN_PYTEST=1
  fi
}

#-----------------------------------------------------------------------------
# Function: setup_build_environment
# Configure the build environment variables
#-----------------------------------------------------------------------------
setup_build_environment() {
  # Determine build flavor
  if [ "$SAN" == "address" ]; then
    FLAVOR="debug-asan"
  elif [[ "$DEBUG" == "1" ]]; then
    FLAVOR="debug"
  elif [[ "$COV" == "1" ]]; then
    FLAVOR="debug-cov"
  elif [[ "$PROFILE" == "1" ]]; then
    FLAVOR="release-profile"
  else
    FLAVOR="release"
  fi

  # If unset, determine the Rust build profile
  if [[ "$RUST_PROFILE" == "" ]]; then
    if [[ "$BUILD_TESTS" == "1" ]]; then
      if [[ "$DEBUG" == "1" || "$COV" == "1" ]]; then
        RUST_PROFILE="dev"
      else
        RUST_PROFILE="optimised_test"
      fi
    else
      RUST_PROFILE="release"
    fi
  fi

  # Get OS and architecture
  OS_NAME=$(uname)
  # Convert OS name to lowercase and convert Darwin to macos
  if [[ "$OS_NAME" == "Darwin" ]]; then
    OS_NAME="macos"
  else
    OS_NAME=$(echo "$OS_NAME" | tr '[:upper:]' '[:lower:]')
  fi

  # Get architecture and convert arm64 to arm64v8
  ARCH=$(uname -m)
  if [[ "$ARCH" == "arm64" ]]; then
    ARCH="aarch64"
  elif [[ "$ARCH" == "x86_64" ]]; then
    ARCH="x64"
  fi

  # Create full variant string for the build directory
  FULL_VARIANT="${OS_NAME}-${ARCH}-${FLAVOR}"

  # Set BINDIR based on configuration and FULL_VARIANT
  if [[ "$COORD" == "oss" ]]; then
    OUTDIR="search-community"
  elif [[ "$COORD" == "rlec" ]]; then
    OUTDIR="search-enterprise"
  else
    echo "COORD should be either oss or rlec"
    exit 1
  fi

  # Set the final BINDIR using the full variant path
  BINDIR="${BINROOT}/${FULL_VARIANT}/${OUTDIR}"
}

start_group() {
  if [[ -n $GITHUB_ACTIONS ]]; then
    echo "::group::$1"
  fi
}

end_group() {
  if [[ -n $GITHUB_ACTIONS ]]; then
    echo "::endgroup::"
  fi
}

#-----------------------------------------------------------------------------
# Function: prepare_coverage_capture
# Run lcov preparations before testing for coverage
#-----------------------------------------------------------------------------
prepare_coverage_capture() {
  start_group "Code Coverage Preparation"
  lcov --zerocounters      --directory $BINROOT --base-directory $ROOT
  lcov --capture --initial --directory $BINROOT --base-directory $ROOT -o $BINROOT/base.info
  end_group
}

#-----------------------------------------------------------------------------
# Function: capture_coverage
# Capture coverage collected since `prepare_coverage_capture` was invoked
#-----------------------------------------------------------------------------
capture_coverage() {
  NAME=${1:-cov} # Get output name. Defaults to `cov.info`

  start_group "Code Coverage Capture ($NAME)"

  # Capture coverage collected while running tests previously
  lcov --capture --directory $BINROOT --base-directory $ROOT -o $BINROOT/test.info

  # Accumulate results with the baseline captured before the test
  lcov --add-tracefile $BINROOT/base.info --add-tracefile $BINROOT/test.info -o $BINROOT/full.info

  # Extract only the coverage of the project source files
  lcov --output-file $BINROOT/source.info --extract $BINROOT/full.info \
    "$ROOT/src/*" \
    "$ROOT/deps/thpool/*" \

  # Remove coverage for directories we don't want (ignore if no file matches)
  lcov -o $BINROOT/$NAME.info --ignore-errors unused --remove $BINROOT/source.info \
    "*/tests/*" \

  end_group

  # Clean up temporary files
  rm $BINROOT/base.info $BINROOT/test.info $BINROOT/full.info $BINROOT/source.info
}

#-----------------------------------------------------------------------------
# Function: prepare_cmake_arguments
# Prepare arguments to pass to CMake
#-----------------------------------------------------------------------------
prepare_cmake_arguments() {
  # Initialize with base arguments
  CMAKE_BASIC_ARGS="-DCOORD_TYPE=$COORD"

  if [[ "$BUILD_TESTS" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_SEARCH_UNIT_TESTS=ON"
  fi

  if [[ -n "$SAN" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DSAN=$SAN"
    DEBUG="1"
  fi

  if [[ "$COV" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCOV=1"
    DEBUG=1
  fi

  if [[ "$PROFILE" != 0 ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DPROFILE=$PROFILE"
    # We shouldn't run profile with debug - so we fail the build
    if [[ "$DEBUG" == "1" ]]; then
      echo "Error: Cannot run profile with debug/sanitizer/coverage"
      exit 1
    fi
  fi

  # Set build type
  if [[ "$DEBUG" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=Debug"
  else
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=RelWithDebInfo"
  fi

  # Ensure output file is always .so even on macOS
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_SHARED_LIBRARY_SUFFIX=.so"

  # Add caching flags to prevent using old configurations
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -UCMAKE_TOOLCHAIN_FILE"

  if [[ "$OS_NAME" == "macos" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++"
  fi
}

#-----------------------------------------------------------------------------
# Function: run_cmake
# Run CMake to configure the build
#-----------------------------------------------------------------------------
run_cmake() {
  # Create build directory and ensure any parent directories exist
  mkdir -p "$BINDIR"
  cd "$BINDIR"

  # Clean up any cached CMake configuration if force is enabled
  if [[ "$FORCE" == "1" ]]; then
    echo "Cleaning CMake cache..."
    rm -f CMakeCache.txt
    rm -rf CMakeFiles
  fi

  echo "Configuring CMake..."
  echo "Build directory: $BINDIR"

  # Run CMake with all the flags
  if [[ "$FORCE" == "1" || ! -f "$BINDIR/Makefile" ]]; then
    CMAKE_CMD="cmake $ROOT $CMAKE_BASIC_ARGS $CMAKE_ARGS"
    echo "$CMAKE_CMD"

    # If verbose, dump all CMake variables before and after configuration
    if [[ "$VERBOSE" == "1" ]]; then
      echo "Running CMake with verbose output..."
      $CMAKE_CMD --trace-expand
    else
      $CMAKE_CMD
    fi
  fi
}

#-----------------------------------------------------------------------------
# Function: build_redisearch_rs
# Build the redisearch_rs target explicitly
#-----------------------------------------------------------------------------
build_redisearch_rs() {
  echo "Building redisearch_rs..."
  REDISEARCH_RS_DIR="$ROOT/src/redisearch_rs"
  REDISEARCH_RS_TARGET_DIR="$ROOT/bin/redisearch_rs"
  REDISEARCH_RS_BINDIR="$BINDIR/redisearch_rs"

  # Determine Rust artifact directory based on the chosen profile
  if [[ "$RUST_PROFILE" == "dev" ]]; then
    RUST_ARTIFACT_SUBDIR="debug"
  else
    RUST_ARTIFACT_SUBDIR="$RUST_PROFILE"
  fi
  # Set up RUSTFLAGS for dynamic C runtime if needed
  if [[ "$RUST_DYN_CRT" == "1" ]]; then
    # Disable statically linking the C runtime.
    # Default behaviour or ignored on most platforms,
    # but necessary on Alpine Linux.
    # See: https://doc.rust-lang.org/reference/linkage.html#r-link.crt
    export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-C target-feature=-crt-static"
  fi
  # Build using cargo
  mkdir -p "$REDISEARCH_RS_TARGET_DIR"
  pushd .
  cd "$REDISEARCH_RS_DIR"
  cargo build --profile="$RUST_PROFILE"

  # Copy artifacts to the target directory
  mkdir -p "$REDISEARCH_RS_BINDIR"
  cp "$REDISEARCH_RS_TARGET_DIR/$RUST_ARTIFACT_SUBDIR"/*.a "$REDISEARCH_RS_BINDIR"
  popd
}

#-----------------------------------------------------------------------------
# Function: build_project
# Build the RediSearch project using Make
#-----------------------------------------------------------------------------
build_project() {
  # Build redisearch_rs explicitly
  build_redisearch_rs

  # Determine number of parallel jobs for make
  if command -v nproc &> /dev/null; then
    NPROC=$(nproc)
  elif command -v sysctl &> /dev/null && [[ "$OS_NAME" == "macos" ]]; then
    NPROC=$(sysctl -n hw.physicalcpu)
  else
    NPROC=4  # Default if we can't determine
  fi
  echo "Building RediSearch with $NPROC parallel jobs..."
  make -j "$NPROC"

  # Build test dependencies if needed
  build_test_dependencies

  # Report build success
  echo "Build complete. Artifacts in $BINDIR"
}

#-----------------------------------------------------------------------------
# Function: build_test_dependencies
# Build additional dependencies needed for tests
#-----------------------------------------------------------------------------
build_test_dependencies() {
  if [[ "$BUILD_TESTS" == "1" ]]; then
    # Ensure ext-example binary gets compiled
    if [[ -d "$ROOT/tests/ctests/ext-example" ]]; then
      echo "Building ext-example for unit tests..."

      # Check if we're already in the build directory
      if [[ "$PWD" != "$BINDIR" ]]; then
        cd "$BINDIR"
      fi

      # The example_extension target is created by CMake in the build directory
      # First check if the target exists in this build
      if grep -q "example_extension" Makefile 2>/dev/null || (make -q example_extension 2>/dev/null); then
        make example_extension
      else
        # If the target doesn't exist, we need to ensure the test was properly configured
        echo "Warning: 'example_extension' target not found in Makefile"
        echo "Checking for extension binary..."

        # Check if extension was already built by a previous run
        EXTENSION_PATH="$BINDIR/example_extension/libexample_extension.so"
        if [[ -f "$EXTENSION_PATH" ]]; then
          echo "Extension binary already exists at: $EXTENSION_PATH"
        else
          echo "Extension binary not found. Some tests may fail."
          echo "Try running 'make example_extension' manually in $BINDIR"
        fi
      fi

      # Export extension path for tests
      EXTENSION_PATH="$BINDIR/example_extension/libexample_extension.so"
      if [[ -f "$EXTENSION_PATH" ]]; then
        echo "Example extension located at: $EXTENSION_PATH"
        export EXT_TEST_PATH="$EXTENSION_PATH"
      else
        echo "Warning: Could not find example extension at $EXTENSION_PATH"
        echo "Some tests may fail if they depend on this extension"
      fi
    fi
  fi
}

#-----------------------------------------------------------------------------
# Function: run_unit_tests
# Run C/C++ unit tests
#-----------------------------------------------------------------------------
run_unit_tests() {
  if [[ "$RUN_UNIT_TESTS" != "1" ]]; then
    return 0
  fi

  echo "Running unit tests..."

  # Set test environment variables if needed
  if [[ "$OS_NAME" == "macos" ]]; then
    echo "Running unit tests on macOS"
    # On macOS, we may need to set DYLD_LIBRARY_PATH or similar
    # Uncomment if needed:
    # export DYLD_LIBRARY_PATH="$BINDIR:$DYLD_LIBRARY_PATH"
  fi

  # Set up environment variables for the unit-tests script
  export BINDIR

  # Set up test filter if provided
  if [[ -n "$TEST_FILTER" ]]; then
    echo "Running tests matching: $TEST_FILTER"
    export TEST="$TEST_FILTER"
  fi

  if [[ $COV == 1 ]]; then
    prepare_coverage_capture
  fi

  # Set verbose mode if requested
  if [[ "$VERBOSE" == "1" ]]; then
    export VERBOSE=1
  fi

  # Set sanitizer mode if requested
  if [[ "$SAN" == "address" ]]; then
    export SAN="address"
  fi

  # Call the unit-tests script from the sbin directory
  echo "Calling $ROOT/sbin/unit-tests"
  "$ROOT/sbin/unit-tests"

  # Check test results
  UNIT_TEST_RESULT=$?
  if [[ $UNIT_TEST_RESULT -eq 0 ]]; then
    echo "All unit tests passed!"
    if [[ $COV == 1 ]]; then
      capture_coverage unit
    fi
  else
    echo "Some unit tests failed. Check the test logs above for details."
    HAS_FAILURES=1
  fi
}

#-----------------------------------------------------------------------------
# Function: run_rust_tests
# Run Rust tests
#-----------------------------------------------------------------------------
run_rust_tests() {
  if [[ "$RUN_RUST_TESTS" != "1" ]]; then
    return 0
  fi

  echo "Running Rust tests..."

  # Set Rust test environment
  RUST_DIR="$ROOT/src/redisearch_rs"

  # Set up RUSTFLAGS for warnings
  if [[ "$RUST_DENY_WARNS" == "1" ]]; then
    export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-D warnings"
  fi

  # Pin a specific working version of nightly to prevent breaking the CI because
  # regressions in a nightly build.
  # Make sure to synchronize updates across all modules: Redis and RedisJSON.
  NIGHTLY_VERSION="nightly-2025-07-30"

  # Add Rust test extensions
  if [[ $COV == 1 ]]; then
    # We use the `nightly` compiler in order to include doc tests in the coverage computation.
    # See https://github.com/taiki-e/cargo-llvm-cov/issues/2 for more details.
    RUST_EXTENSIONS="+$NIGHTLY_VERSION llvm-cov"
    RUST_TEST_OPTIONS="
      --doctests
      --codecov
      --workspace
      --exclude=inverted_index_bencher
      --exclude=trie_bencher
      --exclude=varint_bencher
      --ignore-filename-regex="varint_bencher/*,trie_bencher/*,inverted_index_bencher/*"
      --output-path=$BINROOT/rust_cov.info
    "
  elif [[ -n "$SAN" || "$RUN_MIRI" == "1" ]]; then # using `elif` as we shouldn't run with both
    RUST_EXTENSIONS="+$NIGHTLY_VERSION miri"
  fi

  # Run cargo test with the appropriate filter
  cd "$RUST_DIR"
  cargo $RUST_EXTENSIONS test --profile=$RUST_PROFILE $RUST_TEST_OPTIONS $TEST_FILTER -- --nocapture

  # Check test results
  RUST_TEST_RESULT=$?
  if [[ $RUST_TEST_RESULT -eq 0 ]]; then
    echo "All Rust tests passed!"
  else
    echo "Some Rust tests failed. Check the test logs above for details."
    HAS_FAILURES=1
  fi
}

#-----------------------------------------------------------------------------
# Function: run_python_tests
# Run Python behavioral tests
#-----------------------------------------------------------------------------
run_python_tests() {
  if [[ "$RUN_PYTEST" != "1" ]]; then
    return 0
  fi

  echo "Running Python behavioral tests..."

  # Locate the built module
  MODULE_PATH="$BINDIR/redisearch.so"
  if [[ ! -f "$MODULE_PATH" && -f "$BINDIR/module-enterprise.so" ]]; then
    MODULE_PATH="$BINDIR/module-enterprise.so"
  fi

  if [[ ! -f "$MODULE_PATH" ]]; then
    echo "Error: Cannot find RediSearch module binary in $BINDIR"
    exit 1
  fi

  # Set up environment variables required by runtests.sh
  export MODULE="$(realpath "$MODULE_PATH")"
  export BINROOT
  export FULL_VARIANT
  export BINDIR
  export REJSON="${REJSON:-1}"
  export REJSON_BRANCH="${REJSON_BRANCH:-master}"
  export REJSON_PATH
  export REJSON_ARGS
  export TEST
  export FORCE
  export PARALLEL="${PARALLEL:-1}"
  export LOG_LEVEL="${LOG_LEVEL:-debug}"
  export TEST_TIMEOUT
  export REDIS_STANDALONE="${REDIS_STANDALONE:-1}"
  export SA="${SA:-$REDIS_STANDALONE}"
  export COV

  # Set up test filter if provided
  if [[ -n "$TEST_FILTER" ]]; then
    export TEST="$TEST_FILTER"
    echo "Running Python tests matching: $TEST_FILTER"
  fi

  # Enable quick mode if requested (run only a subset of tests)
  if [[ "$QUICK" == "1" ]]; then
    echo "Running in QUICK mode - using a subset of tests"
    export QUICK=1
  fi

  # Enable verbose mode if requested
  if [[ "$VERBOSE" == "1" ]]; then
    export VERBOSE=1
    export RLTEST_VERBOSE=1
  fi

  if [[ $COV == 1 ]]; then
    prepare_coverage_capture
  fi

  # Use the runtests.sh script for Python tests
  TESTS_SCRIPT="$ROOT/tests/pytests/runtests.sh"
  echo "Running Python tests with module at: $MODULE"

  # Run the tests from the ROOT directory with the requested params
  cd "$ROOT"
  $TESTS_SCRIPT

  # Check test results
  PYTHON_TEST_RESULT=$?
  if [[ $PYTHON_TEST_RESULT -eq 0 ]]; then
    echo "All Python tests passed!"
    if [[ $COV == 1 ]]; then
      if [[ "$REDIS_STANDALONE" == "1" ]]; then
        DEPLOYMENT_TYPE="standalone"
      else
        DEPLOYMENT_TYPE="coordinator"
      fi
      capture_coverage flow_$DEPLOYMENT_TYPE
    fi
  else
    echo "Some Python tests failed. Check the test logs above for details."
    HAS_FAILURES=1
  fi
}

#-----------------------------------------------------------------------------
# Function: run_tests
# Run all requested tests and check results
#-----------------------------------------------------------------------------
run_tests() {
  HAS_FAILURES=0

  # Run each test type as requested
  run_unit_tests
  run_rust_tests
  run_python_tests

  # Exit with failure if any test suite failed
  if [[ "$HAS_FAILURES" == "1" ]]; then
    echo "One or more test suites had failures"
    exit 1
  fi
}

#-----------------------------------------------------------------------------
# Function: run_micro_benchmarks
# Run micro-benchmarks
#-----------------------------------------------------------------------------
run_micro_benchmarks() {
  if [[ "$RUN_MICRO_BENCHMARKS" != "1" ]]; then
    return 0
  fi

  echo "Running micro-benchmarks..."
  # Check if micro-benchmarks directory exists
  MICRO_BENCH_DIR="$BINDIR/micro-benchmarks"

  # Run each benchmark executable
  echo "Running benchmarks from $MICRO_BENCH_DIR"
  cd "$MICRO_BENCH_DIR"

  for benchmark in benchmark_*; do
    if [[ -x "$benchmark" ]]; then
      benchmark_name=${benchmark#benchmark_}

      echo "Running $benchmark..."
      ./"$benchmark" --benchmark_out_format=json --benchmark_out="${benchmark_name}_results.json" || HAS_FAILURES=1
    fi
  done

  if [[ "$HAS_FAILURES" == "1" ]]; then
    echo "Some micro-benchmarks failed. Check the logs above for details."
  else
    echo "All micro-benchmarks completed successfully."
    echo "Results saved to $MICRO_BENCH_DIR/*_results.json"
  fi
}

#-----------------------------------------------------------------------------
# Main execution flow
#-----------------------------------------------------------------------------

# Parse command line arguments
parse_arguments "$@"

# Set up test configuration based on input parameters
setup_test_configuration

# Set up the build environment
setup_build_environment

# Prepare CMake arguments
prepare_cmake_arguments

# Run CMake to configure the build
run_cmake

# Build the project
build_project

# Run tests if requested
run_tests

# Run micro-benchmarks if requested
run_micro_benchmarks

exit 0
