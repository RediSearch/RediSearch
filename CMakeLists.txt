cmake_minimum_required(VERSION 3.13)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)

# Platform detection
if(APPLE)
  set(OS "macos")
elseif(UNIX)
  set(OS "linux")
elseif(WIN32)
  set(OS "windows")
endif()

# CMake policies
if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW) # project() command manages VERSION variables
endif()
if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW) # Include file check macros honor CMAKE_REQUIRED_LIBRARIES
endif()
if(POLICY CMP0077)
  cmake_policy(SET CMP0077 NEW) # option() honors normal variables
endif()

# Initialize build flags
set(CMAKE_LD_FLAGS "-pthread -shared -Bsymbolic -Bsymbolic-functions")

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")

# Define compiler setup function
function(setup_cc_options)
  # Common compiler flags
  set(CMAKE_C_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -Werror=incompatible-pointer-types -Werror=implicit-function-declaration -fcommon" PARENT_SCOPE)
  set(CMAKE_CXX_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -std=c++20" PARENT_SCOPE)

  # Release/Debug/Profile specific flags
  set(CMAKE_C_FLAGS_RELEASE "-O3" PARENT_SCOPE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O3" PARENT_SCOPE)
  set(CMAKE_C_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer -ggdb" PARENT_SCOPE)
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer -ggdb" PARENT_SCOPE)
  set(CMAKE_C_FLAGS_PROFILE "-O2 -fno-omit-frame-pointer" PARENT_SCOPE)
  set(CMAKE_CXX_FLAGS_PROFILE "-O2 -fno-omit-frame-pointer" PARENT_SCOPE)
  # Initialize basic flags needed for builds
  set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS}" PARENT_SCOPE)
  set(CMAKE_LD_LIBS "${CMAKE_LD_LIBS}" PARENT_SCOPE)
endfunction()

# Define shared object setup function
function(setup_shared_object_target target)
  if(APPLE)
    set_target_properties(${target} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
    # Force .so extension on macOS instead of .dylib
    set_target_properties(${target} PROPERTIES SUFFIX ".so")
  endif()
  set_target_properties(${target} PROPERTIES PREFIX "")
endfunction()

# Define debug symbols extraction function
function(extract_debug_symbols target)
  if(CMAKE_BUILD_TYPE STREQUAL "Release" AND NOT APPLE AND NOT WIN32)
    add_custom_command(TARGET ${target} POST_BUILD
      COMMAND objcopy --only-keep-debug $<TARGET_FILE:${target}> $<TARGET_FILE:${target}>.debug
      COMMAND objcopy --strip-debug $<TARGET_FILE:${target}>
      COMMAND objcopy --add-gnu-debuglink=$<TARGET_FILE:${target}>.debug $<TARGET_FILE:${target}>
      COMMENT "Extracting debug symbols from ${target}"
    )
  endif()
endfunction()

#----------------------------------------------------------------------------------------------
# Command line options with default values
option(BUILD_STATIC "Build a static library" OFF)
option(USE_REDIS_ALLOCATOR "Use redis allocator" ON)
option(BUILD_SEARCH_UNIT_TESTS "Build unit tests" OFF)
option(BUILD_LITE "Build lite version" OFF)
option(VERBOSE_UTESTS "Enable verbose unit tests" OFF)
option(ENABLE_ASSERT "Enable assertions" OFF)
# For cpu_features::list_cpu_features coming from VecSim
option(BUILD_TESTING "Enable test rule" OFF)

# Set default values for other build parameters if not specified
if(NOT DEFINED COORD_TYPE)
  set(COORD_TYPE "oss")
endif()

# Default values for other parameters
if(NOT DEFINED MAX_WORKER_THREADS)
  set(MAX_WORKER_THREADS "")
endif()

if(NOT DEFINED BOOST_DIR)
  set(BOOST_DIR "${root}/.install/boost")
endif()

#----------------------------------------------------------------------------------------------
# Configure output paths based on build configuration
if(COORD_TYPE STREQUAL "oss")
  set(BINDIR "${binroot}/search-community")
  set(MODULE_NAME "search" CACHE STRING "Module name" FORCE)
elseif(COORD_TYPE STREQUAL "rlec")
  set(BINDIR "${binroot}/search-enterprise")
  set(MODULE_NAME "search" CACHE STRING "Module name" FORCE)
else()
  message(FATAL_ERROR "Invalid COORD_TYPE (='${COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

if(BUILD_LITE)
  set(BINDIR "${binroot}/search-lite")
  set(MODULE_NAME "searchlight" CACHE STRING "Module name" FORCE)
endif()

if(BUILD_STATIC)
  set(BINDIR "${binroot}/search-static")
endif()

#----------------------------------------------------------------------------------------------
project(redisearch)

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
        message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
        message(STATUS "CMAKE_LINKER_FLAGS: ${CMAKE_LINKER_FLAGS}")
        message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")
    endif()
endif()

# Get Git version info
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

# Set default signedness of char to unsigned
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -funsigned-char")

add_compile_definitions(
    "REDISEARCH_MODULE_NAME=\"${MODULE_NAME}\""
    "GIT_VERSPEC=\"${GIT_VERSPEC}\""
    "GIT_SHA=\"${GIT_SHA}\""
    REDISMODULE_SDK_RLEC
    _GNU_SOURCE)

if(BUILD_LITE)
    message(WARNING "Building artifact under the name searchlight for backward compatibility, but coordinator will be activated in clustered environment")
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

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_LD_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_LD_FLAGS}")

# On debug artifacts, enable assertions
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR ENABLE_ASSERT)
    add_compile_definitions(ENABLE_ASSERT=1)
endif()

#----------------------------------------------------------------------------------------------
# Include external dependencies

# Keep the include for hiredis
include(${root}/build/hiredis/hiredis.cmake)
include(${root}/build/boost/boost.cmake)
if (NOT IS_DIRECTORY ${BOOST_DIR})
    message(FATAL_ERROR "BOOST_DIR is not defined or does not point to a valid directory ${BOOST_DIR}")
endif()

set(BOOST_ROOT ${BOOST_DIR})
set(Boost_NO_WARN_NEW_VERSIONS ON)

include_directories(
    ${root}/src
    ${root}/src/coord
    ${root}/src/redisearch_rs
    ${root}/deps/libuv/include
    ${root}/deps
    ${root}/deps/RedisModulesSDK
    ${root}/deps/VectorSimilarity/src
    ${BOOST_DIR}
    ${root})

add_subdirectory(deps/rmutil)
add_subdirectory(deps/friso)
add_subdirectory(deps/snowball)
add_subdirectory(deps/phonetics)

# Configure libuv options
set(LIBUV_BUILD_TESTS OFF CACHE BOOL "Build libuv tests" FORCE)
set(LIBUV_BUILD_BENCH OFF CACHE BOOL "Build libuv benchmarks" FORCE)
set(LIBUV_BUILD_SHARED OFF CACHE BOOL "Build shared libuv library" FORCE) # Force static library
add_subdirectory(deps/libuv)

option(VECSIM_STATIC "Build as static library" ON)
option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)

add_subdirectory(deps/VectorSimilarity)
add_subdirectory(src/geometry)
add_subdirectory(src/util/hash)
add_subdirectory(src/coord)

#----------------------------------------------------------------------------------------------
# Source files for the core library
file(GLOB SOURCES
    "src/*.c"
    "src/aggregate/*.c"
    "src/aggregate/expr/*.c"
    "src/aggregate/functions/*.c"
    "src/aggregate/reducers/*.c"
    "src/ext/*.c"
    "src/hll/*.c"
    "src/query_parser/v1/*.c"
    "src/query_parser/v2/*.c"
    "src/util/*.c"
    "src/trie/*.c"
    "src/info/*.c"
    "src/module-init/*.c"
    "src/obfuscation/*.c"

    "deps/cndict/cndict_data.c"
    "deps/libnu/*.c"
    "deps/miniz/*.c"
    "deps/base64/*.c"
    "deps/thpool/*.c"
    "deps/triemap/*.c"
    "deps/geohash/*.c")

add_library(rscore OBJECT ${SOURCES})

set(FINAL_OBJECTS
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>
    $<TARGET_OBJECTS:metaphone>
    $<TARGET_OBJECTS:redisearch-coord>
)

#----------------------------------------------------------------------------------------------
# Build configuration
set(REDISEARCH_LIBS "")

if(BUILD_STATIC)
    add_library(redisearch-static STATIC ${FINAL_OBJECTS})
    add_library(redisearch SHARED ${FINAL_OBJECTS})

    set_target_properties(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    setup_shared_object_target(redisearch "")
    target_link_libraries(redisearch redisearch-geometry redisearch-hash VectorSimilarity ${REDISEARCH_LIBS} ${CMAKE_LD_LIBS})

    # Link Rust library if available
    if(EXISTS "${binroot}/redisearch_rs/libtrie_rs.a")
        target_link_libraries(redisearch ${binroot}/redisearch_rs/libtrie_rs.a)
    endif()

    set(TEST_MODULE "redisearch-static")
    set(TEST_MODULE_SO $<TARGET_FILE:redisearch>)

else() # OSS RediSearch
    add_library(redisearch SHARED ${FINAL_OBJECTS})

    set_target_properties(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    setup_shared_object_target(redisearch "")
    target_link_libraries(redisearch
            redisearch-geometry
            redisearch-hash
            VectorSimilarity
            redisearch-coord
            uv_a  # Changed from uv to uv_a (the static library target name)
            ${HIREDIS_LIBS}
            ${SSL_LIBS}
            ${REDISEARCH_LIBS}
            ${CMAKE_LD_LIBS})

    # Link Rust library if available
    if(EXISTS "${binroot}/redisearch_rs/libtrie_rs.a")
        target_link_libraries(redisearch ${binroot}/redisearch_rs/libtrie_rs.a)
    endif()

    extract_debug_symbols(redisearch)
    add_dependencies(redisearch VectorSimilarity)
    add_dependencies(redisearch uv_a)  # Changed from uv to uv_a (static library)
endif()

# Configure output name based on COORD_TYPE
if(COORD_TYPE STREQUAL "oss")
    add_compile_definitions(PRIVATE)
elseif(COORD_TYPE STREQUAL "rlec")
    add_compile_definitions(PRIVATE RS_CLUSTER_ENTERPRISE)
    set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "module-enterprise")
endif()

#----------------------------------------------------------------------------------------------
# Unit tests configuration
if(BUILD_SEARCH_UNIT_TESTS)
    enable_testing()

    add_subdirectory(tests/cpptests/redismock)

    set(BUILD_GTEST ON CACHE BOOL "enable gtest" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "disable gmock" FORCE)

    add_subdirectory(deps/googletest)
    add_subdirectory(tests/cpptests)
    add_subdirectory(tests/ctests)
    add_subdirectory(tests/ctests/ext-example example_extension)
    add_subdirectory(tests/ctests/coord_tests)
endif()
