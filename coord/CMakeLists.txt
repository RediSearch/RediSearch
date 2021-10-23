# cmake_minimum_required(VERSION 3.10.0)

get_filename_component(RS_DIR ${CMAKE_SOURCE_DIR}/.. ABSOLUTE)
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)
message("# coord root: " ${RS_DIR})
message("# coord binroot: " ${binroot})

include(${RS_DIR}/build/cmake/common.cmake)

set(RS_RUN_TESTS OFF CACHE BOOL "" FORCE) # Don't run RS' own tests
set(RS_ENABLE_MOCK ON CACHE BOOL "" FORCE) # But build mock so we can link our own tests
set(RS_BUILD_STATIC ON CACHE BOOL "" FORCE)

#----------------------------------------------------------------------------------------------

project(RSCoordinator)

include(${RS_DIR}/build/cmake/redisearch_cflags.cmake)

option(RS_ENTERPRISE_BUILD "build for enterprise cluster" OFF)

set(RS_BUILD_COORDINATOR 1)

#----------------------------------------------------------------------------------------------

add_definitions(-DREDISMODULE_EXPERIMENTAL_API)
add_definitions(-D_GNU_SOURCE)
add_definitions(-DREDIS_MODULE_TARGET)
add_definitions(-UNDEBUG)
add_definitions(-DRS_COORDINATOR)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${RS_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${RS_CXX_FLAGS}")
set(CMAKE_LINKER "${CMAKE_C_COMPILER}")

# Specific to coordinator
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden")

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Bsymbolic -Bsymbolic-functions")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Bsymbolic -Bsymbolic-functions")

#----------------------------------------------------------------------------------------------

include(${RS_DIR}/build/libuv/libuv.cmake)
include(${RS_DIR}/build/hiredis/hiredis.cmake)

#----------------------------------------------------------------------------------------------

include_directories(${RS_DIR}/coord/src)
include_directories(${LIBUV_INCLUDE_DIR})
include_directories(${RS_DIR}/deps)
include_directories(${RS_DIR}/src)
include_directories(${RS_DIR})

add_subdirectory(${RS_DIR} search)
add_subdirectory(src/rmr)
add_subdirectory(src)

set(FINAL_OBJECTS
    ${COORDINATOR_SRC}
    $<TARGET_OBJECTS:coordinator-core>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:rmr>)

macro(MODULE_TARGET SUFFIX)
    set(_moduleName module-${SUFFIX})
    add_library(${_moduleName} SHARED ${FINAL_OBJECTS} src/build-info/info-${SUFFIX}.c)
    target_link_libraries(${_moduleName} redisearch-${SUFFIX} ${LIBUV_LIBRARIES} hiredis hiredis_ssl crypt ssl)

    if (${SUFFIX} STREQUAL "oss")
        set(_moduleDefs "RS_CLUSTER_OSS")
    elseif (${SUFFIX} STREQUAL "enterprise")
        SET(_moduleDefs "RS_CLUSTER_ENTERPRISE")
    endif()
    target_compile_definitions(${_moduleName} PRIVATE ${_moduleDefs})
    
    set_target_properties(${_moduleName} PROPERTIES PREFIX "")
    set_target_properties(${_moduleName} PROPERTIES SUFFIX ".so")

    if (NOT APPLE)
        add_custom_command(TARGET ${_moduleName} POST_BUILD
            COMMAND objcopy --only-keep-debug $<TARGET_FILE:${_moduleName}> $<TARGET_FILE:${_moduleName}>.debug
            COMMAND objcopy --add-gnu-debuglink $<TARGET_FILE:${_moduleName}>.debug $<TARGET_FILE:${_moduleName}>
            COMMAND ${CMAKE_STRIP} -g $<TARGET_FILE:${_moduleName}>)
    endif()
endmacro()

MODULE_TARGET(oss)
MODULE_TARGET(enterprise)

#----------------------------------------------------------------------------------------------

enable_testing() # enable coordinator tests

add_library(testdeps STATIC ${FINAL_OBJECTS})
target_link_libraries(testdeps ${LIBUV_LIBS} redisearch-oss redismock ${HIREDIS_LIBS} crypt ssl dl)

add_subdirectory(src/rmr/test)
add_subdirectory(tests/unit)
add_subdirectory(${RS_DIR}/tests/ctests/ext-example ext-example)

# set_target_properties(redismock PROPERTIES COMPILE_FLAGS "-fvisibility=default")
