cmake_minimum_required(VERSION 3.15)

# Find Cargo
find_program(CARGO_EXECUTABLE cargo REQUIRED)

if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR "Cargo not found. Please install Rust and Cargo.")
endif()

# Use RUST_TOOLCHAIN_MODIFIER if provided (e.g., +nightly, +stable)
if(DEFINED RUST_TOOLCHAIN_MODIFIER)
  set(TOOLCHAIN_MODIFIER "${RUST_TOOLCHAIN_MODIFIER}")
else()
  set(TOOLCHAIN_MODIFIER "")
endif()

# Use RUST_PROFILE if provided, otherwise default to release
if(NOT DEFINED RUST_PROFILE)
  set(RUST_PROFILE "release")
endif()

# Set RUSTFLAGS from environment variable
# This avoids CMake argument parsing issues with complex flag values
set(RUST_FLAGS "$ENV{RUSTFLAGS}")

# Configure ASAN flags for Rust if sanitizer is enabled
if(SAN STREQUAL "address")
  message(STATUS "Configuring Rust build with AddressSanitizer")
  # RUST_FLAGS for ASAN is set via environment variable RUSTFLAGS in build.sh

  # -Zbuild-std is a cargo flag (not rustc), so set it separately
  set(CARGO_BUILD_FLAGS "-Zbuild-std")

  # Note: ASAN in Rust requires nightly compiler
  # The build.sh script should handle setting the correct toolchain
else()
  set(CARGO_BUILD_FLAGS "")
endif()

# Map Rust profile names to their corresponding artifact directory names
if(RUST_PROFILE STREQUAL "dev")
  set(RUST_ARTIFACT_DIR "debug")
else()
  # For release, optimised_test, and other custom profiles, use the profile name as-is
  set(RUST_ARTIFACT_DIR "${RUST_PROFILE}")
endif()

# If a build target is explicitly specified, adjust the artifact directory accordingly
if(DEFINED CARGO_BUILD_TARGET)
  set(RUST_ARTIFACT_DIR "${CARGO_BUILD_TARGET}/${RUST_ARTIFACT_DIR}")
endif()

# Determine the output library path based on profile and context
if(DEFINED RUST_BINROOT)
  # We're being built as part of the main RediSearch build
  set(RUST_TARGET_DIR "${RUST_BINROOT}/redisearch_rs")
  set(RUST_LIB_PATH "${RUST_BINROOT}/redisearch_rs/${RUST_ARTIFACT_DIR}/libredisearch_rs.a")
else()
  # We're being built standalone or as a subdirectory
  set(RUST_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/target")
  set(RUST_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/target/${RUST_ARTIFACT_DIR}/libredisearch_rs.a")
endif()

# Check if we're being used as a subdirectory to avoid target conflicts
if(NOT TARGET redisearch_rs_build)
  # Create the custom target for building Rust code
  add_custom_target(redisearch_rs_build
        COMMAND ${CMAKE_COMMAND} -E env
            "RUSTFLAGS=${RUST_FLAGS}"
            "CARGO_TARGET_DIR=${RUST_TARGET_DIR}"
            "CARGO_INCREMENTAL=0"
            ${CARGO_EXECUTABLE} ${TOOLCHAIN_MODIFIER} ${CARGO_BUILD_FLAGS}
            build
            --package redisearch_rs
            --profile=${RUST_PROFILE}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Building Rust workspace with profile ${RUST_PROFILE}"
    )
endif()

# Generate C headers from Rust FFI crates using cheadergen.
# cheadergen is a standalone CLI that relies on cargo/nightly under the hood,
# so it must run outside of build.rs, after the Rust code has been compiled.
#
# Header generation is gated by REDISEARCH_GENERATE_HEADERS (defined in the
# top-level CMakeLists.txt — defaults to ON when RediSearch is the top-level
# project, OFF when consumed as a subdirectory). When disabled, we still
# create an empty `cheadergen_generate` target so existing
# `add_dependencies(... cheadergen_generate)` calls remain valid.
if(NOT TARGET cheadergen_generate)
  if(REDISEARCH_GENERATE_HEADERS)
    # Validate at configure time that cheadergen is on PATH. The actual
    # invocation (CLI args + env scrub) lives in `regen_headers.sh` so the
    # recipe is a single source of truth shared with the top-level
    # Makefile's `generate-rust-headers` target.
    find_program(CHEADERGEN_EXECUTABLE cheadergen REQUIRED)
    add_custom_target(cheadergen_generate
          COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/regen_headers.sh
          COMMENT "Generating C headers from Rust FFI crates via cheadergen"
      )
    add_dependencies(cheadergen_generate redisearch_rs_build)
  else()
    # No-op stub so dependents can still depend on `cheadergen_generate`.
    add_custom_target(cheadergen_generate
          COMMENT "Skipping header generation (REDISEARCH_GENERATE_HEADERS=OFF); using checked-in headers"
      )
  endif()
endif()

# Create a custom command to ensure the library exists
add_custom_command(
    OUTPUT ${RUST_LIB_PATH}
    DEPENDS redisearch_rs_build
    COMMENT "Ensuring Rust library exists at ${RUST_LIB_PATH}"
)

# Check if we're being used as a subdirectory to avoid target conflicts
if(NOT TARGET redisearch_rs)
  # Create an imported library target
  add_library(redisearch_rs STATIC IMPORTED GLOBAL)

  # Set the location of the imported library
  set_target_properties(redisearch_rs PROPERTIES
        IMPORTED_LOCATION "${RUST_LIB_PATH}"
    )

  # Make the imported library depend on the build target and the library file
  add_dependencies(redisearch_rs redisearch_rs_build)

  # Create a target that depends on the library file
  add_custom_target(redisearch_rs_lib_file DEPENDS ${RUST_LIB_PATH})
  add_dependencies(redisearch_rs redisearch_rs_lib_file)
endif()

message(STATUS "Rust configuration:")
message(STATUS "  Cargo: ${CARGO_EXECUTABLE}")
message(STATUS "  Toolchain: ${TOOLCHAIN_MODIFIER}")
message(STATUS "  Profile: ${RUST_PROFILE}")
message(STATUS "  Cargo Build Flags: ${CARGO_BUILD_FLAGS}")
message(STATUS "  RUSTFLAGS: ${RUST_FLAGS}")
message(STATUS "  Target Dir: ${RUST_TARGET_DIR}")
message(STATUS "  Library Path: ${RUST_LIB_PATH}")
