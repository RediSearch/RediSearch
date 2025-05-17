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
    # -static-libgcc++ is required for versions that are running against redis < 8 (as of redis 8 redis is linked dynamically against libstdc++)
    set(CMAKE_CXX_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -static-libstdc++" PARENT_SCOPE)
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
      set_target_properties(${target} PROPERTIES LINK_FLAGS "-pthread -shared -Wl,-Bsymbolic,-Bsymbolic-functions")
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
option(MT_BUILD "Enable multi-threading support" OFF)
option(MAX_WORKER_THREADS "Override them maximum parallel worker threads allowed in thread-pool" "")
option(BUILD_TESTING "Enable testing for cpu-features dep" OFF)
option(BUILD_EXAMPLE_EXTENSION "Build example extension for tests" ON)


#----------------------------------------------------------------------------------------------
project(redisearch)

if (NOT MODULE_NAME)
    set(MODULE_NAME "search")
endif()

#----------------------------------------------------------------------------------------------

# Configure compiler options
setup_cc_options()

# ugly hack for cpu_features::list_cpu_features coming from VecSim
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${CMAKE_LD_FLAGS}")

message(STATUS "SAN: ${SAN}")
if(SAN)
    if(SAN STREQUAL "address")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize-recover=all")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize-recover=all")
        set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
        message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
        message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
        message(STATUS "CMAKE_LINKER_FLAGS: ${CMAKE_LINKER_FLAGS}")
        message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")
    endif()
endif()

if (COV)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
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

add_compile_definitions(
    "REDISEARCH_MODULE_NAME=\"${MODULE_NAME}\""
    "GIT_VERSPEC=\"${GIT_VERSPEC}\""
    "GIT_SHA=\"${GIT_SHA}\""
    REDISMODULE_SDK_RLEC
    _GNU_SOURCE)

if (USE_REDIS_ALLOCATOR)
    add_compile_definitions(REDIS_MODULE_TARGET)
endif()

# Set RS_COORDINATOR flag when building with coordinator support
if (BUILD_COORDINATOR)
    add_compile_definitions(RS_COORDINATOR)
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

# Enable multi-threading support if requested
if(MT_BUILD)
    add_compile_definitions(MT_BUILD=1)
    message(STATUS "Building with multi-threading support")
endif()

#----------------------------------------------------------------------------------------------
# Include external dependencies
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
    ${root}/deps
    ${root}/deps/RedisModulesSDK
    ${root}/deps/VectorSimilarity/src
    ${BOOST_DIR}
    ${root})

add_subdirectory(deps/rmutil)
add_subdirectory(deps/friso)
add_subdirectory(deps/snowball)
add_subdirectory(deps/phonetics)

option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)

add_subdirectory(deps/VectorSimilarity)
add_subdirectory(src/geometry)
add_subdirectory(src/util/hash)

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
    $<TARGET_OBJECTS:metaphone>)

#----------------------------------------------------------------------------------------------

if (BUILD_COORDINATOR)
    if (BUILD_COORD_OSS)
        add_library(redisearch-oss STATIC ${SOURCES} ${FINAL_OBJECTS} src/module-init/module-init.c coord/src/rmr/reply.c)
        target_link_libraries(redisearch-oss redisearch-hash)
        target_compile_definitions(redisearch-oss PRIVATE
            RS_NO_ONLOAD
            RS_NO_RMAPI
            RS_CLUSTER_OSS)

    elseif (BUILD_COORD_RLEC)
        add_library(redisearch-enterprise STATIC ${SOURCES} ${FINAL_OBJECTS} src/module-init/module-init.c coord/src/rmr/reply.c)
        target_link_libraries(redisearch-enterprise redisearch-hash)
        target_compile_definitions(redisearch-enterprise PRIVATE
            RS_NO_ONLOAD
            RS_NO_RMAPI
            RS_CLUSTER_ENTERPRISE)
    endif()

elseif (BUILD_STATIC)
    add_library(redisearch-static STATIC ${FINAL_OBJECTS} src/module-init/module-init.c)

    target_compile_definitions(redisearch-static PRIVATE
        RS_NO_ONLOAD
        RS_NO_RMAPI)

    add_library(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)

    set_target_properties(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    setup_shared_object_target(redisearch "")
    target_link_libraries(redisearch redisearch-geometry redisearch-hash VectorSimilarity ${REDISEARCH_LIBS} ${CMAKE_LD_LIBS})

    set(TEST_MODULE "redisearch-static")
    set(TEST_MODULE_SO $<TARGET_FILE:redisearch>)

else() # OSS RediSearch
    add_library(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)

    set_target_properties(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    setup_shared_object_target(redisearch "")
    target_link_libraries(redisearch redisearch-geometry redisearch-hash VectorSimilarity ${REDISEARCH_LIBS} ${CMAKE_LD_LIBS})

    extract_debug_symbols(redisearch)
    add_dependencies(redisearch VectorSimilarity)
endif()

#----------------------------------------------------------------------------------------------

# Only override BUILD_SEARCH_UNIT_TESTS for coordinator builds if not explicitly set
if (BUILD_COORDINATOR AND NOT DEFINED CMAKE_BUILD_SEARCH_UNIT_TESTS)
    set(BUILD_SEARCH_UNIT_TESTS OFF)
endif()

# Always build unit tests if explicitly requested
if (BUILD_SEARCH_UNIT_TESTS)
    set(BUILD_GTEST ON CACHE BOOL "enable gtest" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "disable gmock" FORCE)

    add_subdirectory(deps/googletest)
    add_subdirectory(tests/cpptests)
    add_subdirectory(tests/ctests)
    add_subdirectory(tests/c_utils)

    # Build the example extension if requested
    if (BUILD_EXAMPLE_EXTENSION)
        add_subdirectory(tests/ctests/ext-example example_extension)
    endif()
    if (ENABLE_REDIS_MOCK)
        add_subdirectory(tests/cpptests/redismock)
    endif()
endif()
