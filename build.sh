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
COORD=${COORD:-0}        # Coordinator type: 0 (disabled), 1/oss, or rlec
if [[ "$COORD" == "1" ]]; then
  COORD="oss"
fi
DEBUG=0          # Debug build flag
PROFILE=0        # Profile build flag
FORCE=0          # Force clean build flag
VERBOSE=0        # Verbose output flag
QUICK=0          # Quick test mode (subset of tests)
# Use environment variable if set, otherwise default to 0
ENABLE_ASSERT=${ENABLE_ASSERT:-0}

# Enable multi-threading support (0=disabled, 1=enabled)
# Use environment variable if set, otherwise default to 0
# Support both REDISEARCH_MT_BUILD and MT as aliases
MT_BUILD=${REDISEARCH_MT_BUILD:-${MT:-0}}

# Test configuration (0=disabled, 1=enabled)
BUILD_TESTS=0    # Build test binaries
RUN_UNIT_TESTS=0 # Run C/C++ unit tests
RUN_PYTEST=0     # Run Python tests
RUN_ALL_TESTS=${RUN_ALL_TESTS:-0}  # Run all test types

#-----------------------------------------------------------------------------
# Function: parse_arguments
# Parse command-line arguments and set configuration variables
#-----------------------------------------------------------------------------
parse_arguments() {
  for arg in "$@"; do
    case $arg in
      COORD=*)
        COORD_VALUE="${arg#*=}"
        # Handle COORD=1 as COORD=oss
        echo "coord is set to $COORD"
        if [[ "$COORD_VALUE" == "1" ]]; then
          COORD="oss"
        else
          COORD="$COORD_VALUE"
        fi
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
      RUN_PYTEST|run_pytest|RUNPYTEST|runpytest)
        RUN_PYTEST=1
        ;;
      TEST=*)
        TEST_FILTER="${arg#*=}"
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
      ENABLE_ASSERT|enable_assert)
        ENABLE_ASSERT=1
        ;;
      MT_BUILD|mt_build|MT|mt)
        MT_BUILD=1
        ;;
      QUICK|quick)
        QUICK=1
        ;;
      SA|sa)
        SA=1
        ;;
      REDIS_STANDALONE|redis_standalone)
        REDIS_STANDALONE=1
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
  if [[ "$RUN_ALL_TESTS" == "1" || "$RUN_UNIT_TESTS" == "1" || "$RUN_PYTEST" == "1" ]]; then
    if [[ "$BUILD_TESTS" != "1" ]]; then
      echo "Test execution requested, enabling test build automatically"
      BUILD_TESTS="1"
    fi
  fi

  # If RUN_ALL_TESTS is enabled, enable all test types
  if [[ "$RUN_ALL_TESTS" == "1" ]]; then
    RUN_UNIT_TESTS=1
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
  elif [[ "$PROFILE" == "1" ]]; then
    FLAVOR="release-profile"
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
  if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
    ARCH="arm64v8" # so that unit tests can find the right binary
  elif [[ "$ARCH" == "x86_64" ]]; then
    ARCH="x64"
  fi

  # Create full variant string for the build directory
  FULL_VARIANT="${OS_NAME}-${ARCH}-${FLAVOR}"

  # Set OUTDIR based on configuration
  if [[ "$COORD" == "oss" || "$COORD" == "1" ]]; then
    OUTDIR="coord-oss"
  elif [[ "$COORD" == "rlec" ]]; then
    OUTDIR="coord-rlec"
  elif [[ "$COORD" == "0" ]]; then
    OUTDIR="search"
  else
    echo "COORD should be either 0, 1, oss, or rlec"
    exit 1
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
  CMAKE_BASIC_ARGS=""

  # Only pass COORD_TYPE and BUILD_COORDINATOR when building with COORD=oss or COORD=rlec
  if [[ "$COORD" == "oss" || "$COORD" == "rlec" ]]; then
    echo "Building with coordinator support (COORD=$COORD)"
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCOORD_TYPE=$COORD -DBUILD_COORDINATOR=ON"
  else
    echo "Building without coordinator support"
    # Explicitly set BUILD_COORDINATOR to OFF for vanilla builds
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_COORDINATOR=OFF"
  fi

  if [[ "$BUILD_TESTS" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_SEARCH_UNIT_TESTS=ON"

    # Always build the example extension for unit tests
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DBUILD_EXAMPLE_EXTENSION=ON"
  fi

  if [[ -n "$SAN" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DSAN=$SAN"
    DEBUG="1"
  fi

  if [[ "$PROFILE" != 0 ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DPROFILE=$PROFILE"
    # We shouldn't run profile with debug - so we fail the build
    if [[ "$DEBUG" == "1" ]]; then
      echo "Error: Cannot run profile with debug/sanitizer"
      exit 1
    fi
  fi

  # Set build type
  if [[ "$DEBUG" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=Debug"
    # Debug builds automatically enable assertions in CMakeLists.txt
  else
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DCMAKE_BUILD_TYPE=RelWithDebInfo"
  fi

  # Handle ENABLE_ASSERT (already initialized from environment variable if set)
  if [[ "$ENABLE_ASSERT" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DENABLE_ASSERT=ON"
    echo "Building with assertions enabled"
  else
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DENABLE_ASSERT=OFF"
    echo "Building with assertions disabled"
  fi

  # Handle MT_BUILD (multi-threading support)
  if [[ "$MT_BUILD" == "1" ]]; then
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DMT_BUILD=ON"
    echo "Building with multi-threading support enabled"
  else
    CMAKE_BASIC_ARGS="$CMAKE_BASIC_ARGS -DMT_BUILD=OFF"
    echo "Building with multi-threading support disabled"
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

  # Determine which CMake entry point to use based on COORD
  if [[ "$COORD" == "oss" || "$COORD" == "rlec" ]]; then
    echo "Using coordinator build entry point (coord/CMakeLists.txt)"
    CMAKE_SOURCE_DIR="$ROOT/coord"
  else
    echo "Using standard build entry point (CMakeLists.txt)"
    CMAKE_SOURCE_DIR="$ROOT"
  fi

  # Run CMake with all the flags
  if [[ "$FORCE" == "1" || ! -f "$BINDIR/Makefile" ]]; then
    CMAKE_CMD="cmake $CMAKE_SOURCE_DIR $CMAKE_BASIC_ARGS $CMAKE_ARGS"
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

  # Set up environment variables for the unit-tests script
  export BINROOT="$BINROOT/$FULL_VARIANT"

  # Set up test filter if provided
  if [[ -n "$TEST_FILTER" ]]; then
    echo "Running tests matching: $TEST_FILTER"
    export TEST="$TEST_FILTER"
  fi

  # Set verbose mode if requested
  if [[ "$VERBOSE" == "1" ]]; then
    export VERBOSE=1
  fi

  # Call the unit-tests script from the sbin directory
  echo "Calling $ROOT/sbin/unit-tests"
  "$ROOT/sbin/unit-tests"

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
# Function: run_python_tests
# Run Python behavioral tests
#-----------------------------------------------------------------------------
run_python_tests() {
  if [[ "$RUN_PYTEST" != "1" ]]; then
    return 0
  fi

  echo "Running Python behavioral tests..."

  # Locate the built module based on COORD value
  echo "COORD value: '$COORD'"
  if [[ "$COORD" == "0" || -z "$COORD" ]]; then
    # Standalone build
    MODULE_PATH="$BINDIR/redisearch.so"
    echo "Looking for standalone module at: $MODULE_PATH"
  elif [[ "$COORD" == "oss" ]]; then
    # OSS coordinator build
    MODULE_PATH="$BINDIR/module-oss.so"
    echo "Looking for OSS coordinator module at: $MODULE_PATH"
  elif [[ "$COORD" == "rlec" ]]; then
    # RLEC coordinator build
    MODULE_PATH="$BINDIR/module-enterprise.so"
    echo "Looking for RLEC coordinator module at: $MODULE_PATH"
  else
    echo "Error: Unknown COORD value: '$COORD'"
    exit 1
  fi

  if [[ ! -f "$MODULE_PATH" ]]; then
    echo "Error: Cannot find RediSearch module binary at $MODULE_PATH"
    echo "BINDIR: $BINDIR"
    echo "COORD: $COORD"
    echo "Files in $BINDIR:"
    ls -la "$BINDIR"
    exit 1
  fi

  # Set up environment variables required by runtests.sh
  export COORD="$COORD"
  export MODULE="$(realpath "$MODULE_PATH")"
  export BINROOT="$BINROOT"
  export FULL_VARIANT="$FULL_VARIANT"
  export BINDIR="$BINDIR"

  # Pass MT_BUILD to Python tests (using both environment variables)
  export REDISEARCH_MT_BUILD="$MT_BUILD"
  export MT="$MT_BUILD"
  echo "Setting REDISEARCH_MT_BUILD=$MT_BUILD and MT=$MT_BUILD for Python tests"

  export REJSON="${REJSON:-1}"
  export REJSON_BRANCH="${REJSON_BRANCH:-master}"
  export REJSON_PATH="${REJSON_PATH:-}"
  export REJSON_ARGS="${REJSON_ARGS:-}"
  export TEST="${TEST:-}"
  export FORCE="${FORCE:-}"
  export PARALLEL="${PARALLEL:-1}"
  export LOG_LEVEL="${LOG_LEVEL:-debug}"
  export TEST_TIMEOUT="${TEST_TIMEOUT:-}"

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
