CMAKE_MINIMUM_REQUIRED(VERSION 2.8.9)
PROJECT(redisearch)

OPTION(RS_RUN_TESTS "Build & Run RediSearch's tests" ON)

ADD_DEFINITIONS(-UNDEBUG)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wno-unused-function -Wno-unused-variable -Wno-unused-result")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Werror=implicit-function-declaration -pthread")
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


EXECUTE_PROCESS(COMMAND git describe
    WORKING_DIRECTORY "${CURRENT_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_DESCRIBE_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE)

MESSAGE("${GIT_DESCRIBE_VERSION}")

IF (GIT_DESCRIBE_VERSION)
    ADD_DEFINITIONS(-DRS_GIT_VERSION="${GIT_DESCRIBE_VERSION}")
ENDIF (GIT_DESCRIBE_VERSION)


# Set globals here!
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)
ADD_DEFINITIONS(-DREDISMODULE_EXPERIMENTAL_API)
ADD_DEFINITIONS(-D_GNU_SOURCE)
ADD_DEFINITIONS(-DREDIS_MODULE_TARGET)

ADD_SUBDIRECTORY(src/rmutil)
ADD_SUBDIRECTORY(src/dep/friso)
ADD_SUBDIRECTORY(src/dep/snowball)

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
    "src/dep/triemap/*.c")


ADD_LIBRARY(rscore OBJECT ${RS_SRC})

SET(FINAL_OBJECTS
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>)

ADD_LIBRARY(redisearchS STATIC ${FINAL_OBJECTS})
ADD_LIBRARY(redisearch SHARED ${FINAL_OBJECTS} src/module-init/module-init.c)

SET_TARGET_PROPERTIES(redisearch PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(redisearch PROPERTIES SUFFIX ".so")
TARGET_LINK_LIBRARIES(redisearchS -lm -lc -ldl)

IF (RS_RUN_TESTS)
    ENABLE_TESTING()
    ADD_SUBDIRECTORY(src/tests)
    ADD_SUBDIRECTORY(src/pytest)
ENDIF()
