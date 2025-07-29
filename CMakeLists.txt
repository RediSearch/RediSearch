cmake_minimum_required(VERSION 3.13)

# Set project name first
project(redisearch-disk)

#----------------------------------------------------------------------------------------------
# BUILD STRATEGY:
# - RediSearch: Built statically and linked into final .so
# - CRoaring: Built statically and linked into final .so
# - Disk code: Built statically and linked into final .so
# - Speedb: Built as shared library and linked dynamically
# - Final output: Single redisearch.so Redis module with RediSearch entry point
#----------------------------------------------------------------------------------------------

# Get the absolute paths for RediSearch integration
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/deps/RediSearch ABSOLUTE)
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR} ABSOLUTE)

# Platform detection (copied from RediSearch)
if(APPLE)
    set(OS "macos")
elseif(UNIX)
    set(OS "linux")
endif()
message(STATUS "OS detected: ${OS}")

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
# Export the underlying compiler invocations in a compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Define compiler setup function (copied from RediSearch)
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

# Define shared object setup function (copied from RediSearch)
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

# Define debug symbols extraction function (copied from RediSearch)
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
# Command line options - simplified
option(USE_REDIS_ALLOCATOR "Use redis allocator" ON)
option(USE_DISK_STORAGE "Enable disk-based storage implementation" ON)

#----------------------------------------------------------------------------------------------
# Project was already declared above

# Simplified configuration - always use community version
set(MODULE_NAME "search" CACHE STRING "Module name" FORCE)
set(COORD_TYPE "rlec")
set(BINDIR "${binroot}/search-enterprise")

#----------------------------------------------------------------------------------------------

# Configure compiler options
setup_cc_options()

# Add disk storage compilation flag if enabled
if(USE_DISK_STORAGE)
    add_compile_definitions(USE_DISK_STORAGE=1)
    message(STATUS "Disk storage implementation enabled - disk components will be built but not integrated yet")
endif()

# Sanitizer and coverage settings removed for simplicity

# Get Git version info - to be printed in log upon loading the module
execute_process(
  COMMAND git describe --abbrev=7 --always
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_VERSPEC
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_SHA
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

# ugly hack for cpu_features::list_cpu_features coming from VecSim
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${CMAKE_LD_FLAGS}")

# Treat all format‑string warnings as errors
set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS} -Wformat")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wformat")

add_compile_definitions(
    "REDISEARCH_MODULE_NAME=\"${MODULE_NAME}\""
    "GIT_VERSPEC=\"${GIT_VERSPEC}\""
    "GIT_SHA=\"${GIT_SHA}\""
    REDISMODULE_SDK_RLEC
    _GNU_SOURCE)

if(USE_REDIS_ALLOCATOR)
    add_compile_definitions(REDIS_MODULE_TARGET)
endif()

# Platform-specific settings are handled by RediSearch

# Enable assertions in debug builds
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(ENABLE_ASSERT=1)
endif()

# External dependencies are now handled by RediSearch's CMakeLists.txt
# We only need to include our disk directory for disk includes
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Add disk implementation if enabled
if(USE_DISK_STORAGE)
    add_subdirectory(disk)
endif()

# Now we'll include RediSearch as a subdirectory to build it as a static library
# We modified RediSearch's CMakeLists.txt to build static instead of shared

# Set required variables for RediSearch build
set(LIBUV_BUILD_TESTS OFF CACHE BOOL "Build libuv tests" FORCE)
set(LIBUV_BUILD_BENCH OFF CACHE BOOL "Build libuv benchmarks" FORCE)
set(LIBUV_BUILD_SHARED OFF CACHE BOOL "Build shared libuv library" FORCE)
set(VECSIM_BUILD_TESTS OFF CACHE BOOL "Build vecsim tests" FORCE)

# Include RediSearch build - this will create a static 'redisearch' library
add_subdirectory(deps/RediSearch)

# RediSearch is now built as a static library by the subdirectory
# All components are handled by RediSearch's own CMakeLists.txt

#----------------------------------------------------------------------------------------------
# Build the final shared library - this will be the Redis module with RediSearch entry point
# We create it as a shared library that links the RediSearch static library
add_library(redisearch_module SHARED redisearch_module.c)

# Always use "redisearch" as the output name
set_target_properties(redisearch_module PROPERTIES OUTPUT_NAME "redisearch")

set_target_properties(redisearch_module
    PROPERTIES
    LINKER_LANGUAGE CXX
    C_STANDARD 17
    C_STANDARD_REQUIRED ON)
setup_shared_object_target(redisearch_module "")

# Link with the RediSearch static library and its dependencies
# The RediSearch static library contains all the components and the RedisModule_OnLoad entry point
# RediSearch handles its own dependencies (hiredis, SSL, etc.)
target_link_libraries(redisearch_module
    redisearch                         # RediSearch static library (built by subdirectory)
    dl                                 # Dynamic loading
    pthread                            # Threading
    m                                  # Math library
)

# Add disk storage libraries if enabled
if(USE_DISK_STORAGE)
    # Link disk components statically
    target_link_libraries(redisearch_module
        redisearch-disk  # Our static disk library
        ${CMAKE_BINARY_DIR}/croaring-build/src/libroaring.a  # CRoaring static
    )

    # Link with Speedb dynamically
    target_link_libraries(redisearch_module ${CMAKE_BINARY_DIR}/speedb-build/libspeedb.so)

    # Ensure disk components are built before the main target
    add_dependencies(redisearch_module redisearch-disk speedb croaring)

    message(STATUS "Disk storage enabled: CRoaring and disk code linked statically, Speedb linked dynamically")
endif()

extract_debug_symbols(redisearch_module)
add_dependencies(redisearch_module redisearch)

# Unit tests configuration removed for simplicity
