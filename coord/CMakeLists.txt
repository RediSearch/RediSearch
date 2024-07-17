
cmake_minimum_required(VERSION 3.13)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/.. ABSOLUTE)
include(${root}/deps/readies/cmake/main)

get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)

#@@ lists_from_env(LIBUV HIREDIS)

#----------------------------------------------------------------------------------------------

# COORD_TYPE=oss|rlec

if (NOT COORD_TYPE)
	message(FATAL_ERROR "Missing COORD_TYPE. Should be 'oss' or 'rlec'.")
elseif (COORD_TYPE STREQUAL "oss")
	set(BUILD_COORD_OSS 1)
elseif (COORD_TYPE STREQUAL "rlec")
	set(BUILD_COORD_RLEC 1)
else()
	message(FATAL_ERROR "Invalid COORD_TYPE (='${COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

set(BUILD_TESTS OFF CACHE BOOL "" FORCE) # don't run RediSearch tests
set(ENABLE_REDIS_MOCK ON CACHE BOOL "" FORCE) # build mock to link coordinator tests
set(BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(BUILD_COORDINATOR 1)

#----------------------------------------------------------------------------------------------

project(RSCoordinator)

#----------------------------------------------------------------------------------------------

add_compile_definitions(
	REDISMODULE_SDK_RLEC
	_GNU_SOURCE
	REDIS_MODULE_TARGET
	RS_COORDINATOR
	RMUTIL_NO_SDS)

setup_cc_options()

# Specific to coordinator
add_compile_options("-fvisibility=hidden")

#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden")

#----------------------------------------------------------------------------------------------

include_directories(
	${root}/coord/src
	${root}/deps/libuv/include
	${root}/deps
	${root}/deps/RedisModulesSDK
	${root}/src
	${root}
	${root}/deps/VectorSimilarity/src)

if (APPLE)
    include_directories(/usr/local/opt/openssl/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L/usr/local/opt/openssl/lib")
endif()

add_subdirectory(${root} search)
add_subdirectory(src/rmr)
add_subdirectory(src)

option(VECSIM_STATIC "Build as static library" ON)
option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)

include(${root}/build/libuv/libuv.cmake)
include(${root}/build/hiredis/hiredis.cmake)

set(FINAL_OBJECTS
    $<TARGET_OBJECTS:coordinator-core>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:rmr>)

if (APPLE)
	include_directories(${LIBSSL_DIR}/include)
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${LIBSSL_DIR}/lib")
	set(SSL_LIBS crypto ssl)
else()
	set(SSL_LIBS crypto crypt ssl)
endif()

#----------------------------------------------------------------------------------------------

macro(MODULE_TARGET TYPE)
    if (${TYPE} STREQUAL "oss")
        set(_moduleDefs "RS_CLUSTER_OSS")
		set(MODULE_NAME_SUFFIX "oss")
		set(_moduleName "redisearch")
    elseif (${TYPE} STREQUAL "rlec")
        set(_moduleDefs "RS_CLUSTER_ENTERPRISE")
		set(MODULE_NAME_SUFFIX "enterprise")
		set(_moduleName module-${MODULE_NAME_SUFFIX}) # TODO: WHY???
	else()
		message(FATAL_ERROR "Invalid TYPE (='${TYPE}'). Should be either 'oss' or 'rlec'")
    endif()

    add_library(${_moduleName} SHARED
		${FINAL_OBJECTS}
		src/build-info/info-${MODULE_NAME_SUFFIX}.c)

    target_compile_definitions(${_moduleName} PRIVATE ${_moduleDefs})

    target_link_libraries(${_moduleName}
		redisearch-${MODULE_NAME_SUFFIX}
		redisearch-geometry
		VectorSimilarity
		${LIBUV_LIBS}
		${HIREDIS_LIBS}
		${SSL_LIBS})

	setup_shared_object_target(${_moduleName} ${_moduleName})

	extract_debug_symbols(${_moduleName})
endmacro()

MODULE_TARGET(${COORD_TYPE})

#----------------------------------------------------------------------------------------------

if (BUILD_COORD_OSS)
	enable_testing()

	add_library(testdeps STATIC ${FINAL_OBJECTS})
	target_link_libraries(testdeps
		${LIBUV_LIBS}
		redisearch-oss
		redisearch-geometry
		redismock
		VectorSimilarity
		${HIREDIS_LIBS}
		${SSL_LIBS}
		dl)

	if (BUILD_SEARCH_UNIT_TESTS)
		add_subdirectory(src/rmr/test)
		add_subdirectory(tests/unit)
		add_subdirectory(${root}/tests/ctests/ext-example example_extension)
		add_subdirectory(${root}/tests/cpptests/redismock redismock)
	endif()
endif()
