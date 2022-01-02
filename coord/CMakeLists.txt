
get_filename_component(RS_DIR ${CMAKE_CURRENT_LIST_DIR}/.. ABSOLUTE)
if(NOT DEFINED root)
	set(root ${RS_DIR})
endif()
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)
message("# coord root: " ${RS_DIR})
message("# coord binroot: " ${binroot})

include(${RS_DIR}/build/cmake/common.cmake)

# RS_COORD_TYPE=oss|rlec

if (NOT RS_COORD_TYPE)
	message(FATAL_ERROR "Missing RS_COORD_TYPE. Should be 'oss' or 'rlec'.")
elseif (RS_COORD_TYPE STREQUAL "oss")
	set(RS_BUILD_COORD_OSS 1)
elseif (RS_COORD_TYPE STREQUAL "rlec")
	set(RS_BUILD_COORD_RLEC 1)
else()
	message(FATAL_ERROR "Invalid RS_COORD_TYPE (='${RS_COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

set(RS_RUN_TESTS OFF CACHE BOOL "" FORCE) # Don't run RS' own tests
set(RS_ENABLE_MOCK ON CACHE BOOL "" FORCE) # But build mock so we can link our own tests
set(RS_BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(RS_BUILD_COORDINATOR 1)

#----------------------------------------------------------------------------------------------

project(RSCoordinator)

include(${RS_DIR}/build/cmake/redisearch_cflags.cmake)
include(${RS_DIR}/build/cmake/redisearch_debug.cmake)

#----------------------------------------------------------------------------------------------

add_definitions(-DREDISMODULE_EXPERIMENTAL_API)
add_definitions(-D_GNU_SOURCE)
add_definitions(-DREDIS_MODULE_TARGET)
add_definitions(-UNDEBUG)
add_definitions(-DRS_COORDINATOR)
add_definitions(-DRMUTIL_NO_SDS)

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
include_directories(${RS_DIR}/deps/VectorSimilarity/src)
if (APPLE)
    include_directories(/usr/local/opt/openssl/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L/usr/local/opt/openssl/lib")
endif()

add_subdirectory(${RS_DIR} search)
add_subdirectory(src/rmr)
add_subdirectory(src)
option(VECSIM_STATIC "Build as static library" ON)
option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)

set(FINAL_OBJECTS
    $<TARGET_OBJECTS:coordinator-core>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:rmr>)

if (APPLE)
	include_directories(/usr/local/opt/openssl/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L/usr/local/opt/openssl/lib")
	set(SSL_LIBS crypto ssl)
else()
	set(SSL_LIBS crypt ssl)
endif()

macro(MODULE_TARGET TYPE)
    if (${TYPE} STREQUAL "oss")
        set(_moduleDefs "RS_CLUSTER_OSS")
		set(SUFFIX "oss")
    elseif (${TYPE} STREQUAL "rlec")
        set(_moduleDefs "RS_CLUSTER_ENTERPRISE")
		set(SUFFIX "enterprise")
	else()
		message(FATAL_ERROR "Invalid TYPE (='${TYPE}'). Should be either 'oss' or 'rlec'")
    endif()

    set(_moduleName module-${SUFFIX})
    add_library(${_moduleName} SHARED ${FINAL_OBJECTS} src/build-info/info-${SUFFIX}.c)
    target_link_libraries(${_moduleName} redisearch-${SUFFIX} VectorSimilarity ${LIBUV_LIBS} ${HIREDIS_LIBS} ${SSL_LIBS})

    target_compile_definitions(${_moduleName} PRIVATE ${_moduleDefs})
    
    set_target_properties(${_moduleName} PROPERTIES PREFIX "")
    set_target_properties(${_moduleName} PROPERTIES SUFFIX ".so")

    if (NOT CMAKE_BUILD_TYPE STREQUAL DEBUG AND NOT APPLE)
        add_custom_command(TARGET ${_moduleName} POST_BUILD
            COMMAND objcopy --only-keep-debug $<TARGET_FILE:${_moduleName}> $<TARGET_FILE:${_moduleName}>.debug
            COMMAND objcopy --add-gnu-debuglink $<TARGET_FILE:${_moduleName}>.debug $<TARGET_FILE:${_moduleName}>
            COMMAND ${CMAKE_STRIP} -g $<TARGET_FILE:${_moduleName}>)
    endif()
endmacro()

MODULE_TARGET(${RS_COORD_TYPE})

#----------------------------------------------------------------------------------------------

if (RS_BUILD_COORD_OSS)
	enable_testing()

	add_library(testdeps STATIC ${FINAL_OBJECTS})
	target_link_libraries(testdeps ${LIBUV_LIBS} redisearch-oss redismock VectorSimilarity ${HIREDIS_LIBS} ${SSL_LIBS} dl)

	add_subdirectory(src/rmr/test)
	add_subdirectory(tests/unit)
	add_subdirectory(${RS_DIR}/tests/ctests/ext-example example_extension)
	add_subdirectory(${RS_DIR}/tests/cpptests/redismock redismock)
endif()
