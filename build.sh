#!/bin/bash
set -e

# RediSearch Build Script - Simplified version
# All build logic is now in CMakeLists.txt

# Root directory
ROOT="$(cd "$(dirname "$0")" && pwd)"
BINROOT="$ROOT/bin"

# Default values
COORD="oss"  # oss or rlec
DEBUG=0
FORCE=0
VERBOSE=0

# Test types (0=disabled, 1=enabled)
BUILD_TESTS=0
RUN_UNIT_TESTS=0
RUN_RUST_TESTS=0
RUN_PYTEST=0
RUN_ALL_TESTS=0

# Parse arguments
for arg in "$@"; do
  case $arg in
    COORD=*)
      COORD="${arg#*=}"
      ;;
    DEBUG=*)
      DEBUG="${arg#*=}"
      ;;
    STATIC=*)
      STATIC="${arg#*=}"
      ;;
    LITE=*)
      LITE="${arg#*=}"
      ;;
    TESTS=*)
      BUILD_TESTS="${arg#*=}"
      ;;
    RUN_TESTS=*)
      RUN_ALL_TESTS="${arg#*=}"
      ;;
    RUN_UNIT_TESTS=*)
      RUN_UNIT_TESTS="${arg#*=}"
      ;;
    RUN_RUST_TESTS=*)
      RUN_RUST_TESTS="${arg#*=}"
      ;;
    RUN_PYTEST=*)
      RUN_PYTEST="${arg#*=}"
      ;;
    TEST=*)
      TEST_FILTER="${arg#*=}"
      ;;
    SAN=*)
      SAN="${arg#*=}"
      ;;
    FORCE=*)
      FORCE="${arg#*=}"
      ;;
    VERBOSE=*)
      VERBOSE="${arg#*=}"
      ;;
    *)
      # Pass all other arguments directly to CMake
      CMAKE_ARGS="$CMAKE_ARGS -D${arg}"
      ;;
  esac
done

# If any tests will be run, ensure BUILD_TESTS is enabled
if [[ "$RUN_ALL_TESTS" == "1" || "$RUN_UNIT_TESTS" == "1" || "$RUN_RUST_TESTS" == "1" || "$RUN_PYTEST" == "1" ]]; then
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

# Determine build flavor
if [[ "$DEBUG" == "1" ]]; then
  FLAVOR="debug"
else
  FLAVOR="release"
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
  ARCH="arm64v8"
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

if [[ -n "$LITE" && "$LITE" == "1" ]]; then
  OUTDIR="search-lite"
fi

if [[ -n "$STATIC" && "$STATIC" == "1" ]]; then
  OUTDIR="search-static"
fi

# Set the final BINDIR using the full variant path
BINDIR="${BINROOT}/${FULL_VARIANT}/${OUTDIR}"

# Prepare CMake arguments
CMAKE_BASIC_ARGS="-DCOORD_TYPE=$COORD"

if [[ -n "$STATIC" && "$STATIC" == "1" ]]; then
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_STATIC=ON"
fi

if [[ -n "$LITE" && "$LITE" == "1" ]]; then
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_LITE=ON"
fi

if [[ "$BUILD_TESTS" == "1" ]]; then
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_SEARCH_UNIT_TESTS=ON"
fi

if [[ -n "$SAN" ]]; then
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DSAN=$SAN"
fi

if [[ "$DEBUG" == "1" ]]; then
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=Debug"
else
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=Release"
fi

# Ensure output file is always .so even on macOS
CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_SHARED_LIBRARY_SUFFIX=.so"

# Add caching flags to prevent using old configurations
CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -UCMAKE_TOOLCHAIN_FILE -DREUSE_READIES=OFF"

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

# If needed, build the example extension library
if [[ "$BUILD_TESTS" == "1" ]]; then
  # Ensure ext-example binary gets compiled
  if [[ -d "$ROOT/tests/ctests/ext-example" ]]; then
    echo "Building ext-example for unit tests..."
    make -j "$NPROC" example_extension
    
    # Make sure the extension binary exists and export its path
    EXTENSION_PATH="$BINDIR/example_extension/libexample_extension.so"
    if [[ -f "$EXTENSION_PATH" ]]; then
      echo "Example extension built at: $EXTENSION_PATH"
      export EXT_TEST_PATH="$EXTENSION_PATH"
    else
      echo "Warning: Could not find example extension at $EXTENSION_PATH"
      echo "Some tests may fail if they depend on this extension"
    fi
  fi
fi

echo "Build complete. Artifacts in $BINDIR"

# Run tests if requested
if [[ "$RUN_UNIT_TESTS" == "1" ]]; then
  echo "Running unit tests..."
  
  # Set test environment variables if needed
  if [[ "$OS_NAME" == "macos" ]]; then
    echo "Running unit tests on macOS"
    # On macOS, we may need to set DYLD_LIBRARY_PATH or similar
    # Uncomment if needed:
    # export DYLD_LIBRARY_PATH="$BINDIR:$DYLD_LIBRARY_PATH"
  fi
  
  if [[ -n "$TEST_FILTER" ]]; then
    echo "Running tests matching: $TEST_FILTER"
    TEST_FILTER_ARG="--gtest_filter=$TEST_FILTER"
  fi
  
  # Change to the build directory and run ctest with output on failure
  cd "$BINDIR"
  if [[ "$VERBOSE" == "1" ]]; then
    ctest --output-on-failure -V $TEST_FILTER_ARG # Verbose test output
  else
    ctest --output-on-failure $TEST_FILTER_ARG # Regular test output with failures shown
  fi
  
  # Check test results
  UNIT_TEST_RESULT=$?
  if [[ $UNIT_TEST_RESULT -eq 0 ]]; then
    echo "All unit tests passed!"
  else
    echo "Some unit tests failed. Check the test logs above for details."
    HAS_FAILURES=1
  fi
fi

# Run Rust tests if requested
if [[ "$RUN_RUST_TESTS" == "1" ]]; then
  echo "Running Rust tests..."
  
  # Set Rust test environment
  RUST_DIR="$ROOT/src/redisearch_rs"
  
  # Use the appropriate Rust build mode
  if [[ "$DEBUG" == "1" ]]; then
    RUST_MODE=""
  else
    RUST_MODE="--release"
  fi
  
  # Run cargo test with the appropriate filter
  cd "$RUST_DIR"
  if [[ -n "$TEST_FILTER" ]]; then
    echo "Running Rust tests matching: $TEST_FILTER"
    cargo test $RUST_MODE $TEST_FILTER -- --nocapture
  else
    cargo test $RUST_MODE -- --nocapture
  fi
  
  # Check test results
  RUST_TEST_RESULT=$?
  if [[ $RUST_TEST_RESULT -eq 0 ]]; then
    echo "All Rust tests passed!"
  else
    echo "Some Rust tests failed. Check the test logs above for details."
    HAS_FAILURES=1
  fi
fi

# Run Python tests if requested
if [[ "$RUN_PYTEST" == "1" ]]; then
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
  export BINROOT="$BINROOT"
  export BINDIR="$BINDIR"
  
  # Default to standalone mode
  export REDIS_STANDALONE=1
  
  # Set up test filter if provided
  if [[ -n "$TEST_FILTER" ]]; then
    export TEST="$TEST_FILTER"
    echo "Running Python tests matching: $TEST_FILTER"
  fi
  
  # Enable verbose mode if requested
  if [[ "$VERBOSE" == "1" ]]; then
    export VERBOSE=1
    export RLTEST_VERBOSE=1
  fi
  
  # Use the runtests.sh script for Python tests
  TESTS_SCRIPT="$ROOT/tests/pytests/runtests.sh"
  echo "Running Python tests with module at: $MODULE"
  
  # Run the tests from the ROOT directory
  cd "$ROOT"
  $TESTS_SCRIPT
  
  # Check test results
  PYTHON_TEST_RESULT=$?
  if [[ $PYTHON_TEST_RESULT -eq 0 ]]; then
    echo "All Python tests passed!"
  else
    echo "Some Python tests failed. Check the test logs above for details."
    HAS_FAILURES=1
  fi
fi

# Exit with failure if any test suite failed
if [[ "$HAS_FAILURES" == "1" ]]; then
  echo "One or more test suites had failures"
  exit 1
fi