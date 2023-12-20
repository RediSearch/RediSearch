cmake_minimum_required(VERSION 3.13)
set(CMAKE_CXX_STANDARD 20)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
include(${root}/deps/readies/cmake/main)

get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)

str_from_env(CONAN_BINDIR)
set(CMAKE_MODULE_PATH ${CONAN_BINDIR})

#----------------------------------------------------------------------------------------------

option(BUILD_STATIC "Build a static library" OFF)
# BUILD_COORDINATOR
# BUILD_COORD_OSS
# BUILD_COORD_RLEC

option(USE_REDIS_ALLOCATOR "Use redis allocator" ON)

option(BUILD_SEARCH_UNIT_TESTS "Build unit tests" OFF)

#----------------------------------------------------------------------------------------------

project(redisearch)

if (NOT MODULE_NAME)
    set(MODULE_NAME "search")
endif()

#----------------------------------------------------------------------------------------------

setup_cc_options()

# lists_from_env(S2GEOMETRY)

# set(REDISEARCH_LIBS ${S2GEOMETRY})

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

#----------------------------------------------------------------------------------------------

include_directories(
    ${root}/src
    ${root}/deps
    ${root}/deps/RedisModulesSDK
    ${root}/deps/VectorSimilarity/src
#   ${root}/deps/s2geometry/src
    ${BOOST_DIR}
    ${root})

add_subdirectory(deps/rmutil)
add_subdirectory(deps/rmutil/cxx)
add_subdirectory(deps/friso)
add_subdirectory(deps/snowball)
add_subdirectory(deps/phonetics)

option(VECSIM_STATIC "Build as static library" ON)
option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)

add_subdirectory(deps/VectorSimilarity)
add_subdirectory(src/geometry)

#----------------------------------------------------------------------------------------------

file(GLOB SOURCES
    "src/*.c"
    "src/rmalloc.cpp"
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

    "deps/cndict/cndict_data.c"
    "deps/libnu/*.c"
    "deps/miniz/*.c"
    "deps/base64/*.c"
    "deps/thpool/*.c"
    "deps/triemap/*.c"
    "deps/wildcard/*.c"
    "deps/geohash/*.c")

add_library(rscore OBJECT ${SOURCES})

set(FINAL_OBJECTS
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:rmutil-cxx>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>
    $<TARGET_OBJECTS:metaphone>)

#----------------------------------------------------------------------------------------------

if (BUILD_COORDINATOR)
    if (BUILD_COORD_OSS)
        add_library(redisearch-oss STATIC ${SOURCES} ${FINAL_OBJECTS} src/module-init/module-init.c)

        target_compile_definitions(redisearch-oss PRIVATE
            RS_NO_ONLOAD
            RS_NO_RMAPI
            RS_CLUSTER_OSS)

    elseif (BUILD_COORD_RLEC)
        add_library(redisearch-enterprise STATIC ${SOURCES} ${FINAL_OBJECTS} src/module-init/module-init.c)

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
    target_link_libraries(redisearch redisearch-geometry VectorSimilarity ${REDISEARCH_LIBS} ${CMAKE_LD_LIBS})

    set(TEST_MODULE "redisearch-static")
    set(TEST_MODULE_SO $<TARGET_FILE:redisearch>)

else() # OSS RediSearch
    add_library(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)

    set_target_properties(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    setup_shared_object_target(redisearch "")
    target_link_libraries(redisearch redisearch-geometry VectorSimilarity ${REDISEARCH_LIBS} ${CMAKE_LD_LIBS})

    extract_debug_symbols(redisearch)
    
    add_dependencies(redisearch VectorSimilarity)
endif()

#----------------------------------------------------------------------------------------------

if (BUILD_COORDINATOR)
    set(BUILD_SEARCH_UNIT_TESTS OFF)
endif()

if (BUILD_SEARCH_UNIT_TESTS AND NOT BUILD_COORDINATOR AND NOT BUILD_STATIC)
    set(BUILD_GTEST ON CACHE BOOL "enable gtest" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "disable gmock" FORCE)

    add_subdirectory(deps/googletest)
    add_subdirectory(tests/cpptests)
    add_subdirectory(tests/ctests)
    add_subdirectory(tests/c_utils)
    add_subdirectory(tests/ctests/ext-example example_extension)
    if (ENABLE_REDIS_MOCK)
        add_subdirectory(tests/cpptests/redismock)
    endif()
endif()
