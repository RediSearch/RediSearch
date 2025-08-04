cmake_minimum_required(VERSION 3.13)
project(redisearch_rs)

# Get the current directory and binary directory
get_filename_component(REDISEARCH_RS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR} ABSOLUTE)
get_filename_component(REDISEARCH_RS_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR} ABSOLUTE)

# Determine the target directory based on COORD_TYPE
if(NOT DEFINED COORD_TYPE)
    set(COORD_TYPE "oss")
endif()

if(COORD_TYPE STREQUAL "oss")
    set(TARGET_SUBDIR "search-community")
elseif(COORD_TYPE STREQUAL "rlec")
    set(TARGET_SUBDIR "search-enterprise")
else()
    message(FATAL_ERROR "Invalid COORD_TYPE (='${COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

# Set the target directory for the Rust library
set(RUST_LIB_TARGET_DIR "${CMAKE_BINARY_DIR}/${TARGET_SUBDIR}/redisearch_rs")
set(RUST_LIB_TARGET_PATH "${RUST_LIB_TARGET_DIR}/libredisearch_rs.a")

# Handle Rust profile configuration
# Support for RUST_PROFILE environment variable (like the bash script)
if(DEFINED ENV{RUST_PROFILE})
    set(RUST_PROFILE $ENV{RUST_PROFILE})
else()
    # Determine profile based on CMAKE_BUILD_TYPE
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(RUST_PROFILE "dev")
    else()
        set(RUST_PROFILE "release")
    endif()
endif()

# Map Rust profile to cargo flags and target directory
if(RUST_PROFILE STREQUAL "dev")
    set(CARGO_BUILD_FLAGS "")
    set(RUST_ARTIFACT_SUBDIR "debug")
elseif(RUST_PROFILE STREQUAL "optimised_test")
    set(CARGO_BUILD_FLAGS "--profile=optimised_test")
    set(RUST_ARTIFACT_SUBDIR "optimised_test")
else()
    # Default to release
    set(CARGO_BUILD_FLAGS "--profile=${RUST_PROFILE}")
    set(RUST_ARTIFACT_SUBDIR "${RUST_PROFILE}")
endif()

set(CARGO_TARGET_DIR "${REDISEARCH_RS_SOURCE_DIR}/../../bin/redisearch_rs/${RUST_ARTIFACT_SUBDIR}")
set(RUST_LIB_SOURCE_PATH "${CARGO_TARGET_DIR}/libredisearch_rs.a")

# Handle RUSTFLAGS environment variable
set(RUSTFLAGS_ENV "")
if(DEFINED ENV{RUSTFLAGS})
    set(RUSTFLAGS_ENV $ENV{RUSTFLAGS})
else()
    # Default to deny warnings (like the bash script)
    set(RUSTFLAGS_ENV "-D warnings")
endif()

# Handle dynamic CRT configuration
if(DEFINED ENV{RUST_DYN_CRT} AND ENV{RUST_DYN_CRT} STREQUAL "1")
    # Disable statically linking the C runtime for Alpine Linux compatibility
    set(RUSTFLAGS_ENV "${RUSTFLAGS_ENV} -C target-feature=-crt-static")
endif()

# Find Cargo
find_program(CARGO_EXECUTABLE cargo REQUIRED)
if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found. Please install Rust and Cargo.")
endif()

# Handle RUSTFLAGS environment variable
set(RUSTFLAGS_ENV "")
if(DEFINED ENV{RUSTFLAGS})
    set(RUSTFLAGS_ENV $ENV{RUSTFLAGS})
else()
    # Default to deny warnings (like the bash script)
    set(RUSTFLAGS_ENV "-D warnings")
endif()

# Handle dynamic CRT configuration
if(DEFINED ENV{RUST_DYN_CRT} AND ENV{RUST_DYN_CRT} STREQUAL "1")
    # Disable statically linking the C runtime for Alpine Linux compatibility
    set(RUSTFLAGS_ENV "${RUSTFLAGS_ENV} -C target-feature=-crt-static")
endif()

# Create a custom target for building the Rust library
add_custom_target(redisearch_rs_build
    COMMAND ${CMAKE_COMMAND} -E env RUSTFLAGS=${RUSTFLAGS_ENV} ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS}
    WORKING_DIRECTORY ${REDISEARCH_RS_SOURCE_DIR}
    COMMENT "Building Rust components with cargo build ${CARGO_BUILD_FLAGS} (RUSTFLAGS=${RUSTFLAGS_ENV})"
    VERBATIM
)

# Create the target directory
add_custom_command(
    OUTPUT ${RUST_LIB_TARGET_DIR}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${RUST_LIB_TARGET_DIR}
    COMMENT "Creating target directory for Rust library"
)

# Copy all Rust libraries to the expected location (like the bash script)
add_custom_command(
    OUTPUT ${RUST_LIB_TARGET_PATH}
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CARGO_TARGET_DIR} ${RUST_LIB_TARGET_DIR}
    DEPENDS redisearch_rs_build ${RUST_LIB_TARGET_DIR}
    COMMENT "Copying all Rust artifacts from ${CARGO_TARGET_DIR} to ${RUST_LIB_TARGET_DIR}"
    VERBATIM
)

# Create a custom target that depends on the copied library
add_custom_target(redisearch_rs ALL
    DEPENDS ${RUST_LIB_TARGET_PATH}
    COMMENT "Rust library target"
)

# Add dependency to ensure the build happens before copy
add_dependencies(redisearch_rs redisearch_rs_build)

# Create an imported library target for linking
add_library(redisearch_rs_lib STATIC IMPORTED GLOBAL)
set_target_properties(redisearch_rs_lib PROPERTIES
    IMPORTED_LOCATION ${RUST_LIB_TARGET_PATH}
)

# Make sure the imported library depends on our custom target
add_dependencies(redisearch_rs_lib redisearch_rs)

# Export variables for parent CMakeLists.txt
set(REDISEARCH_RS_LIBRARY ${RUST_LIB_TARGET_PATH} PARENT_SCOPE)
set(REDISEARCH_RS_INCLUDE_DIR ${REDISEARCH_RS_SOURCE_DIR}/headers PARENT_SCOPE)

# Print status information
message(STATUS "RediSearch Rust components configuration:")
message(STATUS "  Source directory: ${REDISEARCH_RS_SOURCE_DIR}")
message(STATUS "  Rust profile: ${RUST_PROFILE}")
message(STATUS "  Artifact subdirectory: ${RUST_ARTIFACT_SUBDIR}")
message(STATUS "  Target directory: ${RUST_LIB_TARGET_DIR}")
message(STATUS "  Library path: ${RUST_LIB_TARGET_PATH}")
message(STATUS "  RUSTFLAGS: ${RUSTFLAGS_ENV}")
message(STATUS "  Cargo executable: ${CARGO_EXECUTABLE}")
