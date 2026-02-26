cmake_minimum_required(VERSION 3.13)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
# The `binroot` path includes a platform+profile segment
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)
# Rust takes care of the different platform+profiles on its own,
# therefore we don't need to use separate target directories for different
# platform+profiles combinations.
get_filename_component(rust_binroot ${binroot}/.. ABSOLUTE)

# Platform detection
if(APPLE)
    set(OS "macos")
elseif(UNIX)
    set(OS "linux")
endif()
message(STATUS "OS detected: ${OS}")

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
# Export the underlying compiler invocations in a compile_commands.json, located in the bin directory.
# It'll be picked up by clangd to provide LSP support in editors like Zed and VSCode
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Define compiler setup function
function(setup_cc_options)
    message("# CMAKE_C_COMPILER_ID: " ${CMAKE_C_COMPILER_ID})

    # Common compiler flags
    set(CMAKE_C_FLAGS "-fPIC -g -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -fcommon -funsigned-char" PARENT_SCOPE)
    set(CMAKE_CXX_FLAGS "-fPIC -g -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare" PARENT_SCOPE)
    set(CMAKE_CXX_STANDARD 20)
    # Release/Debug/Profile specific flags
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS} -O0 -fno-omit-frame-pointer -ggdb" PARENT_SCOPE)
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -fno-omit-frame-pointer -ggdb" PARENT_SCOPE)
    elseif(PROFILE)
        set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS} -O2 -fno-omit-frame-pointer" PARENT_SCOPE)
        set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS} -O2 -fno-omit-frame-pointer" PARENT_SCOPE)
    else()
        # Default to Release flags
        set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS} -O3" PARENT_SCOPE)
        set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS} -O3" PARENT_SCOPE)
    endif()
endfunction()

# Define shared object setup function
function(setup_shared_object_target target)
    if(APPLE)
        set_target_properties(${target} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
        # Force .so extension on macOS instead of .dylib
        set_target_properties(${target} PROPERTIES SUFFIX ".so")
    else()
        # We are building a shared library and want to verify that any reference to a symbol within the library will resolve to
        # the library's own definition, rather than to a definition in another shared library or the main executable.
        set_target_properties(${target} PROPERTIES LINK_FLAGS "-pthread -shared -Bsymbolic -Bsymbolic-functions")
    endif()
    set_target_properties(${target} PROPERTIES PREFIX "")
endfunction()

# Define debug symbols extraction function
function(extract_debug_symbols target)
    if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo" AND NOT APPLE)
        add_custom_command(TARGET ${target} POST_BUILD
        COMMAND cp $<TARGET_FILE:${target}> $<TARGET_FILE:${target}>.debug
        COMMAND objcopy --add-gnu-debuglink=$<TARGET_FILE:${target}>.debug $<TARGET_FILE:${target}>
        COMMAND strip -g $<TARGET_FILE:${target}>
        COMMENT "Extracting debug symbols from ${target}"
    )
    endif()
endfunction()

#----------------------------------------------------------------------------------------------
# Command line options with default values
option(USE_REDIS_ALLOCATOR "Use redis allocator" ON)
option(BUILD_SEARCH_UNIT_TESTS "Build unit tests" OFF)
option(VERBOSE_UTESTS "Enable verbose unit tests" OFF)
option(ENABLE_ASSERT "Enable assertions" OFF)
option(MAX_WORKER_THREADS "Override them maximum parallel worker threads allowed in thread-pool" "")
option(BUILD_TESTING "Enable testing for cpu-features dep" OFF)


#----------------------------------------------------------------------------------------------
project(redisearch)

# Configure output paths based on build configuration
set(MODULE_NAME "search" CACHE STRING "Module name" FORCE)
if(NOT DEFINED COORD_TYPE)
    set(COORD_TYPE "oss")
endif()

set(RUST_BINROOT "${rust_binroot}")
if(COORD_TYPE STREQUAL "oss")
    set(BINDIR "${binroot}/search-community")
elseif(COORD_TYPE STREQUAL "rlec")
    set(BINDIR "${binroot}/search-enterprise")
    add_compile_definitions(PRIVATE RS_CLUSTER_ENTERPRISE)
else()
    message(FATAL_ERROR "Invalid COORD_TYPE (='${COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

#----------------------------------------------------------------------------------------------

# Configure compiler options
setup_cc_options()

# Sanitizer settings
message(STATUS "SAN: ${SAN}")
if(SAN)
    if(SAN STREQUAL "address")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize-recover=all")
        set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
        message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
        message(STATUS "CMAKE_LINKER_FLAGS: ${CMAKE_LINKER_FLAGS}")
        message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")
    endif()
endif()

# Coverage settings
message(STATUS "COV: ${COV}")
if (COV)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
    add_compile_definitions(COVERAGE=1)
endif()

# Get Git version info - to be printed in log upon loading the module
execute_process(
  COMMAND git describe --abbrev=7 --always
  WORKING_DIRECTORY ${root}
  OUTPUT_VARIABLE GIT_VERSPEC
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${root}
  OUTPUT_VARIABLE GIT_SHA
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

# ugly hack for cpu_features::list_cpu_features coming from VecSim
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${CMAKE_LD_FLAGS}")

# Treat all format-string warnings as errors
set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS} -Wformat -Werror=discarded-qualifiers")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wformat")


add_compile_definitions(
    "REDISEARCH_MODULE_NAME=\"${MODULE_NAME}\""
    "GIT_VERSPEC=\"${GIT_VERSPEC}\""
    "GIT_SHA=\"${GIT_SHA}\""
    REDISMODULE_SDK_RLEC
    _GNU_SOURCE)

if(BUILD_INTEL_SVS_OPT)
    add_compile_definitions("BUILD_INTEL_SVS_OPT=1")
endif()

if(MAX_WORKER_THREADS)
    set_source_files_properties(src/config.c PROPERTIES COMPILE_DEFINITIONS MAX_WORKER_THREADS=${MAX_WORKER_THREADS})
endif()

if(USE_REDIS_ALLOCATOR)
    add_compile_definitions(REDIS_MODULE_TARGET)
endif()

if(VERBOSE_UTESTS)
    add_compile_definitions(VERBOSE_UTESTS=1)
endif()

# Platform-specific settings
if(APPLE)
    # Find OpenSSL on macOS
    find_package(OpenSSL REQUIRED)
    include_directories(${OPENSSL_INCLUDE_DIR})

    if(DEFINED LIBSSL_DIR)
        include_directories(${LIBSSL_DIR}/include)
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${LIBSSL_DIR}/lib")
    endif()

    set(SSL_LIBS ${OPENSSL_LIBRARIES})
    set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -dynamiclib")
else()
    set(SSL_LIBS crypto crypt ssl)
endif()

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_LD_FLAGS}")

# On debug artifacts, enable assertions
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR ENABLE_ASSERT)
    add_compile_definitions(ENABLE_ASSERT=1)
endif()

#----------------------------------------------------------------------------------------------
# Include external dependencies (hiredis and boost - needed before src/)
include(${root}/build/hiredis/hiredis.cmake)

if (NOT DEFINED BOOST_DIR)
    set(BOOST_DIR "${root}/.install/boost")
endif()

include(${root}/build/boost/boost.cmake)
if (NOT IS_DIRECTORY ${BOOST_DIR})
    message(FATAL_ERROR "BOOST_DIR is not defined or does not point to a valid directory ${BOOST_DIR}")
endif()

message(STATUS "BOOST_DIR: ${BOOST_DIR}")
set(BOOST_ROOT ${BOOST_DIR})
set(Boost_NO_WARN_NEW_VERSIONS ON)

#----------------------------------------------------------------------------------------------
# Include directories (needed by both src/ and tests/)
include_directories(
    ${root}/src
    ${root}/src/buffer
    ${root}/src/dict
    ${root}/src/coord
    ${root}/src/redisearch_rs/headers
    ${root}/deps/libuv/include
    ${root}/deps
    ${root}/deps/RedisModulesSDK
    ${root}/deps/VectorSimilarity/src
    ${root}/deps/rmalloc
    ${root}/src/ttl_table
    ${BOOST_DIR}
    ${root})

#----------------------------------------------------------------------------------------------
# Build the C code and its dependencies
add_subdirectory(src)

# Build the Rust code
add_subdirectory(src/redisearch_rs)

#----------------------------------------------------------------------------------------------
# Build the final shared library by merging C and Rust static libraries

# Default behavior: SHARED when main project, STATIC when subdirectory
if(NOT DEFINED REDISEARCH_BUILD_SHARED)
    if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
        set(REDISEARCH_BUILD_SHARED ON)
    else()
        set(REDISEARCH_BUILD_SHARED OFF)
    endif()
endif()

option(REDISEARCH_BUILD_SHARED "Build RediSearch as shared library" OFF)

# Create library with determined type
if(REDISEARCH_BUILD_SHARED)
    add_library(redisearch SHARED ${REDISEARCH_C_FINAL_OBJECTS})
    message(STATUS "Building RediSearch as SHARED library")
else()
    add_library(redisearch STATIC ${REDISEARCH_C_FINAL_OBJECTS})
    message(STATUS "Building RediSearch as STATIC library")
endif()

if(COORD_TYPE STREQUAL "rlec")
    set_target_properties(redisearch PROPERTIES OUTPUT_NAME "module-enterprise")
endif()

set_target_properties(redisearch
    PROPERTIES
    LINKER_LANGUAGE CXX
    C_STANDARD 17
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON)
setup_shared_object_target(redisearch "")

# Link the final library - redisearch_c brings in all C dependencies transitively
target_link_libraries(redisearch
    redisearch_c
    redisearch_rs
    ${SSL_LIBS}
    ${CMAKE_LD_LIBS})

extract_debug_symbols(redisearch)
add_dependencies(redisearch redisearch_rs)

#----------------------------------------------------------------------------------------------
# Unit tests configuration
if(BUILD_SEARCH_UNIT_TESTS)
    enable_testing()

    add_subdirectory(tests/cpptests/redismock)

    set(BUILD_GTEST ON CACHE BOOL "enable gtest" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "disable gmock" FORCE)

    add_subdirectory(deps/googletest)
    add_subdirectory(tests/cpptests)
    add_subdirectory(tests/cpptests/micro-benchmarks)
    add_subdirectory(tests/ctests)
    add_subdirectory(tests/ctests/ext-example example_extension)
    add_subdirectory(tests/ctests/coord_tests)
endif()
