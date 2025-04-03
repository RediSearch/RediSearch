#!/bin/bash
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
FORCE=0         # Force clean build flag
VERBOSE=0        # Verbose output flag
QUICK=0          # Quick test mode (subset of tests)

# Test configuration (0=disabled, 1=enabled)
BUILD_TESTS=0    # Build test binaries
RUN_UNIT_TESTS=0 # Run C/C++ unit tests
RUN_RUST_TESTS=0 # Run Rust tests
RUN_PYTEST=0     # Run Python tests
RUN_ALL_TESTS=0  # Run all test types

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

  if [[ -n "$LITE" && "$LITE" == "1" ]]; then
    OUTDIR="search-lite"
  fi

  if [[ -n "$STATIC" && "$STATIC" == "1" ]]; then
    OUTDIR="search-static"
  fi

  # Set the final BINDIR using the full variant path
  BINDIR="${BINROOT}/${FULL_VARIANT}/${OUTDIR}"
}

#-----------------------------------------------------------------------------
# Function: prepare_cmake_arguments
# Prepare arguments to pass to CMake
#-----------------------------------------------------------------------------
prepare_cmake_arguments() {
  # Initialize with base arguments
  CMAKE_BASIC_ARGS="-DCOORD_TYPE=$COORD"

  # Add configuration-specific arguments
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
    DEBUG="1"
  fi

  # Set build type
  if [[ "$DEBUG" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=Debug"
  else
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=Release"
  fi

  # Ensure output file is always .so even on macOS
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_SHARED_LIBRARY_SUFFIX=.so"

  # Add caching flags to prevent using old configurations
  CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -UCMAKE_TOOLCHAIN_FILE"
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

  # Determine Rust build mode
  if [[ "$DEBUG" == "1" ]]; then
    RUST_BUILD_MODE=""
    RUST_ARTIFACT_SUBDIR="debug"
  else
    RUST_BUILD_MODE="--release"
    RUST_ARTIFACT_SUBDIR="release"
  fi

  # Build using cargo
  mkdir -p "$REDISEARCH_RS_TARGET_DIR"
  cd "$REDISEARCH_RS_DIR"
  cargo build $RUST_BUILD_MODE

  # Copy artifacts to the target directory
  mkdir -p "$REDISEARCH_RS_BINDIR"
  cp "$REDISEARCH_RS_TARGET_DIR/$RUST_ARTIFACT_SUBDIR"/*.a "$REDISEARCH_RS_BINDIR"
}

#-----------------------------------------------------------------------------
# Function: build_project
# Build the RediSearch project using Make
#-----------------------------------------------------------------------------
build_project() {
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

  # Build redisearch_rs explicitly
  build_redisearch_rs

  # Build test dependencies if needed
  build_test_dependencies
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
  export BINROOT="$BINROOT"
  export FULL_VARIANT="$FULL_VARIANT"
  export BINDIR="$BINDIR"
  export REJSON="${REJSON:-1}"
  export REJSON_BRANCH="${REJSON_BRANCH:-master}"
  export REJSON_PATH="${REJSON_PATH:-}"
  export REJSON_ARGS="${REJSON_ARGS:-}"
  export TEST="${TEST:-}"
  export FORCE="${FORCE:-}"
  export PARALLEL="${PARALLEL:-1}"
  export LOG_LEVEL="${LOG_LEVEL:-debug}"
  export TEST_TIMEOUT="${TEST_TIMEOUT:-}"
  export REDIS_STANDALONE="${REDIS_STANDALONE:-}"
  export SA="${SA:-1}"
  
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
  
  # Report build success
  echo "Build complete. Artifacts in $BINDIR"
  
  # Exit with failure if any test suite failed
  if [[ "$HAS_FAILURES" == "1" ]]; then
    echo "One or more test suites had failures"
    exit 1
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

exit 0