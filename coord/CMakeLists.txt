
cmake_minimum_required(VERSION 3.13)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/.. ABSOLUTE)
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR}/.. ABSOLUTE)

#----------------------------------------------------------------------------------------------

# COORD_TYPE=oss|rlec

# Only build coordinator components if COORD_TYPE is set
if (NOT DEFINED COORD_TYPE)
	message(FATAL_ERROR "Missing COORD_TYPE. Should be 'oss' or 'rlec'.")
elseif (COORD_TYPE STREQUAL "oss")
	set(BUILD_COORD_OSS 1)
elseif (COORD_TYPE STREQUAL "rlec")
	set(BUILD_COORD_RLEC 1)
else()
	message(FATAL_ERROR "Invalid COORD_TYPE (='${COORD_TYPE}'). Should be either 'oss' or 'rlec'")
endif()

# Only override BUILD_SEARCH_UNIT_TESTS if not explicitly set
if(NOT DEFINED BUILD_SEARCH_UNIT_TESTS)
    set(BUILD_SEARCH_UNIT_TESTS OFF CACHE BOOL "Build unit tests" FORCE)
endif()

set(ENABLE_REDIS_MOCK ON CACHE BOOL "" FORCE) # build mock to link coordinator tests
set(BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(BUILD_COORDINATOR 1)

#----------------------------------------------------------------------------------------------

project(RSCoordinator)

#----------------------------------------------------------------------------------------------

add_compile_definitions(
	REDISMODULE_EXPERIMENTAL_API
	_GNU_SOURCE
	REDIS_MODULE_TARGET
	RS_COORDINATOR
	RMUTIL_NO_SDS)

# Define compiler setup function
function(setup_cc_options)
    message("# CMAKE_C_COMPILER_ID: " ${CMAKE_C_COMPILER_ID})

    # Set C and C++ flags separately
    set(CMAKE_C_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -Werror=incompatible-pointer-types -Werror=implicit-function-declaration -fcommon -funsigned-char -fvisibility=hidden" PARENT_SCOPE)
    set(CMAKE_CXX_FLAGS "-fPIC -pthread -fno-strict-aliasing -Wno-unused-function -Wno-unused-variable -Wno-sign-compare -fvisibility=hidden" PARENT_SCOPE)

    # Set C++ standard
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
      set_target_properties(${target} PROPERTIES LINK_FLAGS "-pthread -shared -Wl,-Bsymbolic,-Bsymbolic-functions -static-libstdc++")
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

# Call the setup function
setup_cc_options()

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

#----------------------------------------------------------------------------------------------

include_directories(
	${root}/coord/src
	${root}/deps/libuv/include
	${root}/deps
	${root}/src
	${root}
	${root}/deps/VectorSimilarity/src)

add_subdirectory(${root} search)
add_subdirectory(src/rmr)
add_subdirectory(src)

option(VECSIM_STATIC "Build as static library" ON)
option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)

# Configure libuv options
set(LIBUV_BUILD_TESTS OFF CACHE BOOL "Build libuv tests" FORCE)
set(LIBUV_BUILD_BENCH OFF CACHE BOOL "Build libuv benchmarks" FORCE)
set(LIBUV_BUILD_SHARED OFF CACHE BOOL "Build shared libuv library" FORCE) # Force static library
set(LIBUV_LIBS uv_a)

# Add libuv and hiredis as subdirectories
add_subdirectory(${root}/deps/libuv ${CMAKE_CURRENT_BINARY_DIR}/deps/libuv)
include(${root}/build/hiredis/hiredis.cmake)
message("HIREDIS_LIBS: ${HIREDIS_LIBS}")

set(FINAL_OBJECTS
    $<TARGET_OBJECTS:coordinator-core>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:rmutil-cxx>
    $<TARGET_OBJECTS:rmr>)
#----------------------------------------------------------------------------------------------

macro(MODULE_TARGET TYPE)
    if (${TYPE} STREQUAL "oss")
        set(_moduleDefs "RS_CLUSTER_OSS")
		set(MODULE_NAME_SUFFIX "oss")
    elseif (${TYPE} STREQUAL "rlec")
        set(_moduleDefs "RS_CLUSTER_ENTERPRISE")
		set(MODULE_NAME_SUFFIX "enterprise")
	else()
		message(FATAL_ERROR "Invalid TYPE (='${TYPE}'). Should be either 'oss' or 'rlec'")
    endif()

    set(_moduleName module-${MODULE_NAME_SUFFIX})

    add_library(${_moduleName} SHARED
		${FINAL_OBJECTS}
		src/build-info/info-${MODULE_NAME_SUFFIX}.c)

    target_compile_definitions(${_moduleName} PRIVATE ${_moduleDefs})

    target_link_libraries(${_moduleName}
		redisearch-${MODULE_NAME_SUFFIX}
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
