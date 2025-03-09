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
FORCE=1
VERBOSE=0

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
      TESTS="${arg#*=}"
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

if [[ -n "$TESTS" && "$TESTS" == "1" ]]; then
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

echo "Build complete. Artifacts in $BINDIR"