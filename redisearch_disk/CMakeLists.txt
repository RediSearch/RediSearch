cmake_minimum_required(VERSION 3.13)

find_program(CARGO_EXECUTABLE cargo REQUIRED)

if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found. Please install Rust and Cargo.")
endif()

if(NOT DEFINED RUST_PROFILE)
    set(RUST_PROFILE "release")
endif()

# Use sccache for Rust if available (detected in root CMakeLists.txt)
# Note: ccache doesn't work with Rust, only sccache does
if(SCCACHE_PROGRAM AND NOT DEFINED ENV{RUSTC_WRAPPER})
    set(RUSTC_WRAPPER_ENV "RUSTC_WRAPPER=${SCCACHE_PROGRAM}")
elseif(DEFINED ENV{RUSTC_WRAPPER})
    set(RUSTC_WRAPPER_ENV "RUSTC_WRAPPER=$ENV{RUSTC_WRAPPER}")
else()
    set(RUSTC_WRAPPER_ENV "")
endif()

# Map Rust profile names to their corresponding artifact directory names
if(RUST_PROFILE STREQUAL "dev")
    set(RUST_ARTIFACT_DIR "debug")
else()
    set(RUST_ARTIFACT_DIR "${RUST_PROFILE}")
endif()

set(RUST_TARGET_DIR ${CMAKE_CURRENT_SOURCE_DIR}/target)
set(RUST_LIB_PATH ${RUST_TARGET_DIR}/${RUST_ARTIFACT_DIR}/libredisearch_disk.a)

add_custom_target(redisearch-disk_build
    COMMAND ${CMAKE_COMMAND} -E env ${RUSTC_WRAPPER_ENV} ${CARGO_EXECUTABLE} build --profile=${RUST_PROFILE} --features redis_allocator
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust crate with profile ${RUST_PROFILE}"
)

# Ensure SpeedB is built before Cargo runs (Rust links against libspeedb.so)
add_dependencies(redisearch-disk_build speedb-shared)

add_library(redisearch-disk STATIC IMPORTED GLOBAL)
set_target_properties(redisearch-disk PROPERTIES
    IMPORTED_LOCATION "${RUST_LIB_PATH}"
)

add_dependencies(redisearch-disk redisearch-disk_build)

message(STATUS "Rust configuration:")
message(STATUS "  Cargo: ${CARGO_EXECUTABLE}")
message(STATUS "  Profile: ${RUST_PROFILE}")
message(STATUS "  RUSTFLAGS: ${RUST_FLAGS}")
message(STATUS "  Target Dir: ${RUST_TARGET_DIR}")
message(STATUS "  Library Path: ${RUST_LIB_PATH}")
