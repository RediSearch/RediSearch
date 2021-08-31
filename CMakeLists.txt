CMAKE_MINIMUM_REQUIRED(VERSION 3.0.0)
PROJECT(redisearch)

# Allow to use 'LOCATION' setting
#IF(POLICY CMP0026)
#    CMAKE_POLICY(SET CMP0026 OLD)
#ENDIF()
#IF(POLICY CMP0045)
#    cmake_policy(SET CMP0045 OLD)
#ENDIF()

OPTION(RS_RUN_TESTS "Build & Run RediSearch's tests" OFF)
OPTION(RS_BUILD_STATIC "Builds a static library" OFF)
OPTION(USE_ASAN "Use AddressSanitizer (clang)" OFF)
OPTION(USE_MSAN "Use MemorySanitizer (clang)" OFF)
OPTION(USE_TSAN "Use ThreadSanitizer (clang)" OFF)
OPTION(USE_REDIS_ALLOCATOR "Use redis allocator" ON)
OPTION(USE_COVERAGE "Build with coverage" OFF)

IF (RS_MODULE_NAME) 
    ADD_DEFINITIONS("-DREDISEARCH_MODULE_NAME=\"${RS_MODULE_NAME}\"")
ENDIF()

INCLUDE(cmake/redisearch_cflags.cmake)
INCLUDE(cmake/redisearch_debug.cmake)

ADD_DEFINITIONS(-UNDEBUG)

set(RS_VECSIM_SO_FLAGS "-lstdc++")

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${RS_C_FLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${RS_CXX_FLAGS}")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${RS_EXE_FLAGS}")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${RS_SO_FLAGS} ${RS_VECSIM_SO_FLAGS}")

IF (RS_BUILD_COORDINATOR)
    SET(RS_RUN_TESTS OFF)
ENDIF()

IF(RS_RUN_TESTS)
    ENABLE_TESTING()
ENDIF()

# Also remove /D NDEBUG to avoid MSVC warnings about conflicting defines.
FOREACH (flags_var_to_scrub
         CMAKE_CXX_FLAGS_RELEASE
         CMAKE_CXX_FLAGS_RELWITHDEBINFO
         CMAKE_CXX_FLAGS_MINSIZEREL
         CMAKE_C_FLAGS_RELEASE
         CMAKE_C_FLAGS_RELWITHDEBINFO
         CMAKE_C_FLAGS_MINSIZEREL)
    STRING(REGEX REPLACE "(^| )[/-]D *NDEBUG($| )" " "
        "${flags_var_to_scrub}" "${${flags_var_to_scrub}}")
ENDFOREACH()

IF (NOT GIT_DESCRIBE_VERSION)
    EXECUTE_PROCESS(COMMAND git describe
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        OUTPUT_VARIABLE GIT_DESCRIBE_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
ENDIF()

IF (NOT GIT_SHA)
    EXECUTE_PROCESS(COMMAND git rev-parse HEAD
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        OUTPUT_VARIABLE GIT_SHA
        OUTPUT_STRIP_TRAILING_WHITESPACE)
ENDIF()

IF (NOT GIT_DESCRIBE_VERSION AND NOT RS_FORCE_NO_GITVERSION)
    MESSAGE(FATAL_ERROR "`git describe` failed. Either define `GIT_DESCRIBE_VERSION` or `RS_FORCE_NO_GITVERSION`")
ENDIF()

MESSAGE("${GIT_DESCRIBE_VERSION}")
MESSAGE("${GIT_SHA}")

IF (GIT_DESCRIBE_VERSION)
    ADD_DEFINITIONS(-DRS_GIT_VERSION="${GIT_DESCRIBE_VERSION}")
ENDIF (GIT_DESCRIBE_VERSION)

IF (GIT_SHA)
    ADD_DEFINITIONS(-DRS_GIT_SHA="${GIT_SHA}")
ENDIF (GIT_SHA)


IF (USE_COVERAGE)
    SET(_lcovInit "${PROJECT_BINARY_DIR}/lcov-init.sh")
    SET(_lcovCapture "${PROJECT_BINARY_DIR}/lcov-capture.sh")
    CONFIGURE_FILE("cmake/lcov-init.sh.in" "${_lcovInit}" @ONLY)
    CONFIGURE_FILE("cmake/lcov-capture.sh.in" "${_lcovCapture}" @ONLY)
    EXECUTE_PROCESS(COMMAND chmod a+x ${_lcovInit} ${_lcovCapture})
ENDIF()

# Set globals here!
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/deps)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/deps/VectorSimilarity/src)

ADD_DEFINITIONS(-DREDISMODULE_EXPERIMENTAL_API)
ADD_DEFINITIONS(-D_GNU_SOURCE)

IF(USE_REDIS_ALLOCATOR)
    ADD_DEFINITIONS(-DREDIS_MODULE_TARGET)
ENDIF ()

ADD_SUBDIRECTORY(src/rmutil)
ADD_SUBDIRECTORY(src/dep/friso)
ADD_SUBDIRECTORY(src/dep/snowball)
ADD_SUBDIRECTORY(src/dep/phonetics)

include (ExternalProject)
ExternalProject_Add (VectorSimilarity
	SOURCE_DIR      ${CMAKE_CURRENT_SOURCE_DIR}/deps/VectorSimilarity/src
	PREFIX          ${CMAKE_CURRENT_BINARY_DIR}/VectorSimilarity
	CMAKE_ARGS      -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/VectorSimilarity
	CMAKE_ARGS      -DVECSIM_STATIC:BOOL=ON
)

ExternalProject_Get_Property (VectorSimilarity install_dir)
set(VectorSimilarity_bindir ${install_dir})
message("VectorSimilarity bindir: ${VectorSimilarity_bindir}")

include_directories (${VectorSimilarity_bindir}/include)

add_library (vecsim STATIC IMPORTED)
set_target_properties (vecsim PROPERTIES IMPORTED_LOCATION ${VectorSimilarity_bindir}/src/VectorSimilarity-build/libVectorSimilarity.a)

add_library (openblas STATIC IMPORTED)
set_target_properties (openblas PROPERTIES IMPORTED_LOCATION ${VectorSimilarity_bindir}/src/VectorSimilarity-build/OpenBLAS/lib/libopenblas.a)


FILE(GLOB RS_SRC
    "src/*.c"
    "src/aggregate/*.c"
    "src/aggregate/expr/*.c"
    "src/aggregate/functions/*.c"
    "src/aggregate/reducers/*.c"
    "src/ext/*.c"
    "src/query_parser/*.c"
    "src/util/*.c"
    "src/trie/*.c"
    "src/dep/cndict/cndict_data.c"
    "src/dep/hll/*.c"
    "src/dep/libnu/*.c"
    "src/dep/miniz/*.c"
    "src/dep/base64/*.c"
    "src/dep/thpool/*.c"
    "src/dep/triemap/*.c"
    "src/dep/geo/*.c"
    ${RS_DEBUG_SRC})

ADD_LIBRARY(rscore OBJECT ${RS_SRC})

SET(FINAL_OBJECTS
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>
    $<TARGET_OBJECTS:metaphone>)

IF (RS_BUILD_COORDINATOR)
    ADD_LIBRARY(redisearch-oss STATIC ${RS_SRC} ${FINAL_OBJECTS} src/module-init/module-init.c)
    target_compile_definitions(redisearch-oss PRIVATE RS_NO_ONLOAD RS_NO_RMAPI RS_CLUSTER_OSS)
    SET_TARGET_PROPERTIES(redisearch-oss PROPERTIES OUTPUT_NAME "redisearch-oss")

    ADD_LIBRARY(redisearch-enterprise STATIC ${RS_SRC} ${FINAL_OBJECTS} src/module-init/module-init.c)
    target_compile_definitions(redisearch-enterprise PRIVATE RS_NO_ONLOAD RS_NO_RMAPI RS_CLUSTER_ENTERPRISE)
    SET_TARGET_PROPERTIES(redisearch-enterprise PROPERTIES OUTPUT_NAME "redisearch-enterprise")

ELSEIF (RS_BUILD_STATIC)
    ADD_LIBRARY(redisearchS STATIC ${FINAL_OBJECTS} src/module-init/module-init.c)
    target_compile_definitions(redisearchS PRIVATE RS_NO_ONLOAD RS_NO_RMAPI)
    SET_TARGET_PROPERTIES(redisearchS PROPERTIES OUTPUT_NAME "redisearch")

    ADD_LIBRARY(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)
    TARGET_LINK_LIBRARIES(redisearch dl m)
    SET_TARGET_PROPERTIES(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    SET_TARGET_PROPERTIES(redisearch PROPERTIES PREFIX "")
    SET_TARGET_PROPERTIES(redisearch PROPERTIES SUFFIX ".so")
    ADD_LDFLAGS(redisearch "-Wl,--enable-new-dtags -Wl,-rpath,\$ORIGIN")

    SET(RS_TEST_MODULE redisearchS)
    SET(RS_TEST_MODULE_SO $<TARGET_FILE:redisearch>)

ELSE ()
    ADD_LIBRARY(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)
    TARGET_LINK_LIBRARIES(redisearch dl m vecsim openblas)
    SET_TARGET_PROPERTIES(redisearch PROPERTIES LINKER_LANGUAGE CXX)
    SET_TARGET_PROPERTIES(redisearch PROPERTIES PREFIX "")
    SET_TARGET_PROPERTIES(redisearch PROPERTIES SUFFIX ".so")
    ADD_LDFLAGS(redisearch "-Wl,--enable-new-dtags -Wl,-rpath,\$ORIGIN")

    IF (NOT APPLE)
        ADD_CUSTOM_COMMAND(TARGET redisearch POST_BUILD
            COMMAND objcopy --only-keep-debug $<TARGET_FILE:redisearch> $<TARGET_FILE:redisearch>.debug
            COMMAND objcopy --add-gnu-debuglink $<TARGET_FILE:redisearch>.debug $<TARGET_FILE:redisearch>
            COMMAND ${CMAKE_STRIP} -g $<TARGET_FILE:redisearch>) 
    ENDIF()
ENDIF()

IF (APPLE) 
    SET_TARGET_PROPERTIES(redisearch PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
ENDIF()

add_dependencies(redisearch VectorSimilarity)

IF (RS_RUN_TESTS AND NOT RS_BUILD_COORDINATOR)
    SET(BUILD_SHARED_LIBS ON CACHE BOOL "enable shared libs" FORCE)
    SET(BUILD_GTEST ON CACHE BOOL "enable gtest" FORCE)
    SET(BUILD_GMOCK OFF CACHE BOOL "disable gmock" FORCE)
    ADD_SUBDIRECTORY(deps/googletest)
    ADD_SUBDIRECTORY(tests/cpptests)
    ADD_SUBDIRECTORY(tests/ctests)
    ADD_SUBDIRECTORY(tests/pytests)
    ADD_SUBDIRECTORY(tests/c_utils)
ELSEIF (RS_ENABLE_MOCK)
    ADD_SUBDIRECTORY(tests/cpptests/redismock)
ENDIF()

# SET(RMTEST_MODULE_PATH $<TARGET_FILE:redisearch>)
# SET(RMTEST_EXT_PATH $<TARGET_FILE:example_extension>)

# GET_TARGET_PROPERTY(RMTEST_MODULE_PATH redisearch LOCATION)
# GET_TARGET_PROPERTY(RMTEST_EXT_PATH example_extension LOCATION)

# CONFIGURE_FILE(cmake/rmtest.config.in
#     ${PROJECT_BINARY_DIR}/rmtest.config.default)

