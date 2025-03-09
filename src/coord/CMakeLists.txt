cmake_minimum_required(VERSION 3.13)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../.. ABSOLUTE)
# Removed readies dependency
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/../.. ABSOLUTE)

#----------------------------------------------------------------------------------------------

# COORD_TYPE=oss|rlec
if (NOT COORD_TYPE)
	set(BUILD_COORD_OSS 1)
elseif (COORD_TYPE STREQUAL "oss")
	set(BUILD_COORD_OSS 1)
elseif (COORD_TYPE STREQUAL "rlec")
	set(BUILD_COORD_RLEC 1)
else()
	message(FATAL_ERROR "Invalid COORD_TYPE (='${COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

#----------------------------------------------------------------------------------------------

project(RSCoordinator)

add_compile_definitions(
	REDISMODULE_SDK_RLEC
	_GNU_SOURCE
	REDIS_MODULE_TARGET)

# No need to call setup_cc_options() here as it's already called in the main CMakeLists.txt
# The compiler flags set there are propagated to this subdirectory

#----------------------------------------------------------------------------------------------

include_directories(
	${root}/src/coord
	${root}/deps/libuv/include
	${root}/deps
	${root}/deps/RedisModulesSDK
	${root}/src
	${root}
	${root}/deps/VectorSimilarity/src)

add_subdirectory(rmr)

file(GLOB COORDINATOR_SRC *.c *.cpp)
add_library(coordinator-core OBJECT ${COORDINATOR_SRC})


set(FINAL_OBJECTS
    $<TARGET_OBJECTS:coordinator-core>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:rmr>)

add_library(redisearch-coord STATIC ${FINAL_OBJECTS})
