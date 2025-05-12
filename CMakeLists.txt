cmake_minimum_required(VERSION 3.13)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)

# Platform detection
if(APPLE)
  set(OS "macos")
elseif(UNIX)
  set(OS "linux")
endif()
message(STATUS "OS detected: ${OS}")

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")

# Define compiler setup function
function(setup_cc_options)
    message("# CMAKE_C_COMPILER_ID: " ${CMAKE_C_COMPILER_ID})

    # Common compiler flags
    set(CMAKE_C_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -Werror=incompatible-pointer-types -Werror=implicit-function-declaration -fcommon -funsigned-char" PARENT_SCOPE)
    set(CMAKE_CXX_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare" PARENT_SCOPE)
    set(CMAKE_CXX_STANDARD 20)
    # Release/Debug/Profile specific flags
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS} -O0 -g -fno-omit-frame-pointer -ggdb" PARENT_SCOPE)
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -ggdb" PARENT_SCOPE)
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

if (COV)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
endif()

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
set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS} -Wformat -Werror=format-security")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wformat -Werror=format-security")


add_compile_definitions(
    "REDISEARCH_MODULE_NAME=\"${MODULE_NAME}\""
    "GIT_VERSPEC=\"${GIT_VERSPEC}\""
    "GIT_SHA=\"${GIT_SHA}\""
    REDISMODULE_SDK_RLEC
    _GNU_SOURCE)

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
# Include external dependencies
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

include_directories(
    ${root}/src
    ${root}/src/coord
    ${root}/src/redisearch_rs
    ${root}/deps/libuv/include
    ${root}/deps
    ${root}/deps/triemap
    ${root}/deps/RedisModulesSDK
    ${root}/deps/VectorSimilarity/src
    ${BOOST_DIR}
    ${root})

add_subdirectory(deps/rmutil)
add_subdirectory(deps/friso)
add_subdirectory(deps/snowball)
add_subdirectory(deps/phonetics)
add_subdirectory(deps/fast_float)
add_subdirectory(deps/triemap)

# Configure libuv options
set(LIBUV_BUILD_TESTS OFF CACHE BOOL "Build libuv tests" FORCE)
set(LIBUV_BUILD_BENCH OFF CACHE BOOL "Build libuv benchmarks" FORCE)
set(LIBUV_BUILD_SHARED OFF CACHE BOOL "Build shared libuv library" FORCE) # Force static library
add_subdirectory(deps/libuv)

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
    "src/iterators/*.c"
    "src/info/*.c"
    "src/info/info_redis/*.c"
    "src/info/info_redis/threads/*.c"
    "src/info/info_redis/types/*.c"
    "src/module-init/*.c"
    "src/obfuscation/*.c"

    "deps/cndict/cndict_data.c"
    "deps/libnu/*.c"
    "deps/miniz/*.c"
    "deps/base64/*.c"
    "deps/fast_float/*.c"
    "deps/thpool/*.c"
    "deps/geohash/*.c")

add_library(rscore OBJECT ${SOURCES})

set(FINAL_OBJECTS
    $<TARGET_OBJECTS:trie>
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>
    $<TARGET_OBJECTS:metaphone>
    $<TARGET_OBJECTS:fast_float_strtod>
    $<TARGET_OBJECTS:redisearch-coord>
)

#----------------------------------------------------------------------------------------------
# Build the shared library
add_library(redisearch SHARED ${FINAL_OBJECTS})

if(COORD_TYPE STREQUAL "rlec")
    set_target_properties(redisearch PROPERTIES OUTPUT_NAME "module-enterprise")
endif()

set_target_properties(redisearch
    PROPERTIES
    LINKER_LANGUAGE CXX
    C_STANDARD 17
    C_STANDARD_REQUIRED ON)
setup_shared_object_target(redisearch "")
target_link_libraries(redisearch
        redisearch-geometry
        redisearch-hash
        VectorSimilarity
        redisearch-coord
        trie
        uv_a
        ${BINDIR}/redisearch_rs/libredisearch_rs.a
        ${HIREDIS_LIBS}
        ${SSL_LIBS}
        ${CMAKE_LD_LIBS})

extract_debug_symbols(redisearch)
add_dependencies(redisearch VectorSimilarity)
add_dependencies(redisearch uv_a)


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
