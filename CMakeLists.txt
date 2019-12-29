CMAKE_MINIMUM_REQUIRED(VERSION 2.8.9)
PROJECT(redisearch)

# Allow to use 'LOCATION' setting
IF(POLICY CMP0026)
    CMAKE_POLICY(SET CMP0026 OLD)
ENDIF()


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

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${RS_C_FLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${RS_CXX_FLAGS}")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${RS_EXE_FLAGS}")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${RS_SO_FLAGS}")

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
ADD_DEFINITIONS(-DREDISMODULE_EXPERIMENTAL_API)
ADD_DEFINITIONS(-D_GNU_SOURCE)

IF(USE_REDIS_ALLOCATOR)
    ADD_DEFINITIONS(-DREDIS_MODULE_TARGET)
ENDIF ()

ADD_SUBDIRECTORY(src/rmutil)
ADD_SUBDIRECTORY(src/dep/friso)
ADD_SUBDIRECTORY(src/dep/snowball)
ADD_SUBDIRECTORY(src/dep/phonetics)

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
    "src/dep/thpool/*.c"
    "src/dep/triemap/*.c"
    ${RS_DEBUG_SRC})


ADD_LIBRARY(rscore OBJECT ${RS_SRC})

SET(FINAL_OBJECTS
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>
    $<TARGET_OBJECTS:metaphone>)

ADD_LIBRARY(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)
TARGET_LINK_LIBRARIES(redisearch dl m)
SET_TARGET_PROPERTIES(redisearch PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(redisearch PROPERTIES SUFFIX ".so")

IF (RS_BUILD_STATIC)
    ADD_LIBRARY(redisearchS STATIC ${FINAL_OBJECTS} src/module-init/module-init.c)
    SET_TARGET_PROPERTIES(redisearchS PROPERTIES COMPILE_DEFINITIONS RS_NO_ONLOAD)
    SET_TARGET_PROPERTIES(redisearchS PROPERTIES OUTPUT_NAME "redisearch")
ENDIF()

IF (APPLE) 
    SET_TARGET_PROPERTIES(redisearch
        PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
ENDIF()

IF (RS_RUN_TESTS)
    SET(BUILD_SHARED_LIBS ON CACHE BOOL "enable shared libs" FORCE)
    SET(BUILD_GTEST ON CACHE BOOL "enable gtest" FORCE)
    SET(BUILD_GMOCK OFF CACHE BOOL "disable gmock" FORCE)
    ADD_SUBDIRECTORY(src/dep/gtest)
    ADD_SUBDIRECTORY(src/cpptests)
    ADD_SUBDIRECTORY(src/tests)
    ADD_SUBDIRECTORY(src/pytest)
ELSEIF (RS_ENABLE_MOCK)
    ADD_SUBDIRECTORY(src/cpptests/redismock)
ENDIF()

# SET(RMTEST_MODULE_PATH $<TARGET_FILE:redisearch>)
# SET(RMTEST_EXT_PATH $<TARGET_FILE:example_extension>)

GET_TARGET_PROPERTY(RMTEST_MODULE_PATH redisearch LOCATION)
GET_TARGET_PROPERTY(RMTEST_EXT_PATH example_extension LOCATION)

CONFIGURE_FILE(cmake/rmtest.config.in
    ${PROJECT_BINARY_DIR}/rmtest.config.default)

ADD_SUBDIRECTORY(src/c_utils)
