cmake_minimum_required(VERSION 3.13)

# Find Rust and Cargo
find_program(CARGO_EXECUTABLE cargo REQUIRED)

if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found. Please install Rust and Cargo.")
endif()

# Determine Rust profile based on build configuration
set(RUST_PROFILE "release")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RUST_PROFILE "dev")
elseif(DEFINED BUILD_SEARCH_UNIT_TESTS AND BUILD_SEARCH_UNIT_TESTS)
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(RUST_PROFILE "dev")
    else()
        set(RUST_PROFILE "optimised_test")
    endif()
endif()

# Set up RUSTFLAGS
set(RUST_FLAGS "-D warnings")
if(DEFINED RUST_DYN_CRT AND RUST_DYN_CRT)
    set(RUST_FLAGS "${RUST_FLAGS} -C target-feature=-crt-static")
endif()

# Exclude benchmarking crates that link C code
set(EXCLUDE_ARGS "--exclude" "inverted_index_bencher" "--exclude" "rqe_iterators_bencher")

# Determine the output library path based on profile and context
if(DEFINED BINDIR)
    # We're being built as part of the main RediSearch build
    set(RUST_TARGET_DIR "${BINDIR}/redisearch_rs")
    set(RUST_LIB_PATH "${BINDIR}/redisearch_rs/${RUST_PROFILE}/libredisearch_rs.a")
else()
    # We're being built standalone or as a subdirectory
    set(RUST_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/target")
    if(RUST_PROFILE STREQUAL "dev")
        set(RUST_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/target/debug/libredisearch_rs.a")
    else()
        set(RUST_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/target/release/libredisearch_rs.a")
    endif()
endif()

# Check if we're being used as a subdirectory to avoid target conflicts
if(NOT TARGET redisearch_rs_build)
    # Create the custom target for building Rust code
    add_custom_target(redisearch_rs_build
        COMMAND ${CMAKE_COMMAND} -E env
            "RUSTFLAGS=${RUST_FLAGS}"
            "CARGO_TARGET_DIR=${RUST_TARGET_DIR}"
            ${CARGO_EXECUTABLE} build
            --workspace
            ${EXCLUDE_ARGS}
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
