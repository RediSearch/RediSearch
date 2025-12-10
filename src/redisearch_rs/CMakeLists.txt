cmake_minimum_required(VERSION 3.13)

# Find Cargo
find_program(CARGO_EXECUTABLE cargo REQUIRED)

if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found. Please install Rust and Cargo.")
endif()

# Use RUST_PROFILE if provided, otherwise default to release
if(NOT DEFINED RUST_PROFILE)
    set(RUST_PROFILE "release")
endif()

# Set RUSTFLAGS from environment variable
# This avoids CMake argument parsing issues with complex flag values
set(RUST_FLAGS "$ENV{RUSTFLAGS}")

# Map Rust profile names to their corresponding artifact directory names
if(RUST_PROFILE STREQUAL "dev")
    set(RUST_ARTIFACT_DIR "debug")
else()
    # For release, optimised_test, and other custom profiles, use the profile name as-is
    set(RUST_ARTIFACT_DIR "${RUST_PROFILE}")
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
            ${CARGO_EXECUTABLE} build
            --package redisearch_rs
            --profile=${RUST_PROFILE}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Building Rust workspace with profile ${RUST_PROFILE}"
    )
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
message(STATUS "  Profile: ${RUST_PROFILE}")
message(STATUS "  RUSTFLAGS: ${RUST_FLAGS}")
message(STATUS "  Target Dir: ${RUST_TARGET_DIR}")
message(STATUS "  Library Path: ${RUST_LIB_PATH}")
