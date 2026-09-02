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

set(REDISEARCH_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
set(RUST_BUILD_STAMP "${RUST_TARGET_DIR}/${RUST_ARTIFACT_DIR}/.redisearch_rs_cmake_build")
set(CHEADERGEN_STAMP "${CMAKE_CURRENT_BINARY_DIR}/.cheadergen_generate")
set(RUST_BUILD_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/redisearch_rs_build_config.txt")
set(RUST_BUILD_CONFIG_TMP "${RUST_BUILD_CONFIG}.tmp")

file(WRITE "${RUST_BUILD_CONFIG_TMP}"
  "CARGO_EXECUTABLE=${CARGO_EXECUTABLE}\n"
  "CARGO_BUILD_FLAGS=${CARGO_BUILD_FLAGS}\n"
  "CARGO_BUILD_TARGET=${CARGO_BUILD_TARGET}\n"
  "RUST_ARTIFACT_DIR=${RUST_ARTIFACT_DIR}\n"
  "RUST_FLAGS=${RUST_FLAGS}\n"
  "RUST_LIB_PATH=${RUST_LIB_PATH}\n"
  "RUST_PROFILE=${RUST_PROFILE}\n"
  "RUST_TARGET_DIR=${RUST_TARGET_DIR}\n"
  "TOOLCHAIN_MODIFIER=${TOOLCHAIN_MODIFIER}\n"
)
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy_if_different "${RUST_BUILD_CONFIG_TMP}" "${RUST_BUILD_CONFIG}"
)
file(REMOVE "${RUST_BUILD_CONFIG_TMP}")

set(RUST_BUILD_INPUTS
  "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml"
  "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.lock"
  "${REDISEARCH_ROOT}/.rust-nightly"
  "${REDISEARCH_ROOT}/rust-toolchain.toml"
)

file(GLOB RUST_TOP_LEVEL_MANIFESTS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/*.toml"
)
list(APPEND RUST_BUILD_INPUTS ${RUST_TOP_LEVEL_MANIFESTS})

file(GLOB RUST_WORKSPACE_ENTRIES CONFIGURE_DEPENDS LIST_DIRECTORIES true
  "${CMAKE_CURRENT_SOURCE_DIR}/*"
)
foreach(RUST_WORKSPACE_ENTRY IN LISTS RUST_WORKSPACE_ENTRIES)
  if(NOT IS_DIRECTORY "${RUST_WORKSPACE_ENTRY}")
    continue()
  endif()

  get_filename_component(RUST_WORKSPACE_ENTRY_NAME "${RUST_WORKSPACE_ENTRY}" NAME)
  if(RUST_WORKSPACE_ENTRY_NAME STREQUAL "target"
      OR RUST_WORKSPACE_ENTRY_NAME STREQUAL "tools"
      OR RUST_WORKSPACE_ENTRY_NAME MATCHES "_bencher$")
    continue()
  endif()

  file(GLOB_RECURSE RUST_CRATE_INPUTS CONFIGURE_DEPENDS
    "${RUST_WORKSPACE_ENTRY}/*.rs"
    "${RUST_WORKSPACE_ENTRY}/*.toml"
  )
  list(APPEND RUST_BUILD_INPUTS ${RUST_CRATE_INPUTS})
endforeach()

list(FILTER RUST_BUILD_INPUTS EXCLUDE REGEX "/tests?/")
list(FILTER RUST_BUILD_INPUTS EXCLUDE REGEX "/benches?/")

set(RUST_FFI_INPUT_ROOTS
  "${REDISEARCH_ROOT}/deps"
  "${REDISEARCH_ROOT}/src/redisearch_rs/headers"
)
set(RUST_FFI_INPUTS)
file(GLOB RUST_FFI_TOP_LEVEL_INPUTS CONFIGURE_DEPENDS
  "${REDISEARCH_ROOT}/src/*.c"
  "${REDISEARCH_ROOT}/src/*.h"
)
list(APPEND RUST_FFI_INPUTS ${RUST_FFI_TOP_LEVEL_INPUTS})
file(GLOB REDISEARCH_SRC_ENTRIES CONFIGURE_DEPENDS LIST_DIRECTORIES true
  "${REDISEARCH_ROOT}/src/*"
)
foreach(REDISEARCH_SRC_ENTRY IN LISTS REDISEARCH_SRC_ENTRIES)
  if(NOT IS_DIRECTORY "${REDISEARCH_SRC_ENTRY}")
    continue()
  endif()

  get_filename_component(REDISEARCH_SRC_ENTRY_NAME "${REDISEARCH_SRC_ENTRY}" NAME)
  if(REDISEARCH_SRC_ENTRY_NAME STREQUAL "redisearch_rs")
    continue()
  endif()

  file(GLOB_RECURSE RUST_FFI_SRC_INPUTS CONFIGURE_DEPENDS
    "${REDISEARCH_SRC_ENTRY}/*.c"
    "${REDISEARCH_SRC_ENTRY}/*.h"
  )
  list(APPEND RUST_FFI_INPUTS ${RUST_FFI_SRC_INPUTS})
endforeach()
foreach(RUST_FFI_INPUT_ROOT IN LISTS RUST_FFI_INPUT_ROOTS)
  if(EXISTS "${RUST_FFI_INPUT_ROOT}")
    file(GLOB_RECURSE RUST_FFI_ROOT_INPUTS CONFIGURE_DEPENDS
      "${RUST_FFI_INPUT_ROOT}/*.c"
      "${RUST_FFI_INPUT_ROOT}/*.h"
    )
    list(APPEND RUST_FFI_INPUTS ${RUST_FFI_ROOT_INPUTS})
  endif()
endforeach()
list(FILTER RUST_FFI_INPUTS EXCLUDE REGEX "/\\.git/")
list(FILTER RUST_FFI_INPUTS EXCLUDE REGEX "/target/")
list(FILTER RUST_FFI_INPUTS EXCLUDE REGEX "/bin/")
list(FILTER RUST_FFI_INPUTS EXCLUDE REGEX "/build/")
list(REMOVE_DUPLICATES RUST_FFI_INPUTS)

set(RUST_BUILD_DEPENDS
  "${RUST_BUILD_CONFIG}"
  ${RUST_BUILD_INPUTS}
  ${RUST_FFI_INPUTS}
)

# Check if we're being used as a subdirectory to avoid target conflicts
if(NOT TARGET redisearch_rs_build)
  add_custom_command(
    OUTPUT "${RUST_LIB_PATH}" "${RUST_BUILD_STAMP}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${RUST_TARGET_DIR}/${RUST_ARTIFACT_DIR}"
    COMMAND ${CMAKE_COMMAND} -E env
      "RUSTFLAGS=${RUST_FLAGS}"
      "CARGO_TARGET_DIR=${RUST_TARGET_DIR}"
      ${CARGO_EXECUTABLE} ${TOOLCHAIN_MODIFIER} ${CARGO_BUILD_FLAGS}
      build
      --package redisearch_rs
      --profile=${RUST_PROFILE}
    COMMAND ${CMAKE_COMMAND} -E touch_nocreate "${RUST_LIB_PATH}"
    COMMAND ${CMAKE_COMMAND} -E touch "${RUST_BUILD_STAMP}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${RUST_BUILD_DEPENDS}
    COMMENT "Building Rust workspace with profile ${RUST_PROFILE}"
    VERBATIM
  )
  add_custom_target(redisearch_rs_build
    DEPENDS "${RUST_LIB_PATH}" "${RUST_BUILD_STAMP}"
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
    add_custom_command(
      OUTPUT "${CHEADERGEN_STAMP}"
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/regen_headers.sh
      COMMAND ${CMAKE_COMMAND} -E touch "${CHEADERGEN_STAMP}"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      DEPENDS
        redisearch_rs_build
        ${RUST_BUILD_INPUTS}
        "${CMAKE_CURRENT_SOURCE_DIR}/regen_headers.sh"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../.cheadergen-version"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../.rust-nightly"
      COMMENT "Generating C headers from Rust FFI crates via cheadergen"
      VERBATIM
    )
    add_custom_target(cheadergen_generate
      DEPENDS "${CHEADERGEN_STAMP}"
    )
  else()
    # No-op stub so dependents can still depend on `cheadergen_generate`.
    add_custom_target(cheadergen_generate
      COMMENT "Skipping header generation (REDISEARCH_GENERATE_HEADERS=OFF); using checked-in headers"
    )
  endif()
endif()

# Check if we're being used as a subdirectory to avoid target conflicts
if(NOT TARGET redisearch_rs)
  # Create an imported library target
  add_library(redisearch_rs STATIC IMPORTED GLOBAL)

  # Set the location of the imported library
  set_target_properties(redisearch_rs PROPERTIES
        IMPORTED_LOCATION "${RUST_LIB_PATH}"
    )

  # Make the imported library depend on the build target
  add_dependencies(redisearch_rs redisearch_rs_build)
endif()

message(STATUS "Rust configuration:")
message(STATUS "  Cargo: ${CARGO_EXECUTABLE}")
message(STATUS "  Toolchain: ${TOOLCHAIN_MODIFIER}")
message(STATUS "  Profile: ${RUST_PROFILE}")
message(STATUS "  Cargo Build Flags: ${CARGO_BUILD_FLAGS}")
message(STATUS "  RUSTFLAGS: ${RUST_FLAGS}")
message(STATUS "  Target Dir: ${RUST_TARGET_DIR}")
message(STATUS "  Library Path: ${RUST_LIB_PATH}")
message(STATUS "  Build stamp: ${RUST_BUILD_STAMP}")
