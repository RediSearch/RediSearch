# src/CMakeLists.txt
# Builds the C code and all its dependencies as a static library

#----------------------------------------------------------------------------------------------
# Define paths for the script and output files (command info generation)
set(GEN_SCRIPT "${root}/srcutil/gen_command_info.py")
set(COMMAND_JSON "${root}/commands.json")
set(COMMAND_INFO_FILE_NAME "command_info")
set(COMMAND_INFO_FOLDER_NAME "command_info")
set(COMMAND_OUTPUT_FOLDER "${CMAKE_CURRENT_SOURCE_DIR}/${COMMAND_INFO_FOLDER_NAME}")
set(COMMAND_OUTPUT_FILE "${COMMAND_OUTPUT_FOLDER}/${COMMAND_INFO_FILE_NAME}")
set(COMMAND_OUTPUT_H "${COMMAND_OUTPUT_FILE}.h")
set(COMMAND_OUTPUT_C "${COMMAND_OUTPUT_FILE}.c")

# Add custom command to run the Python script
add_custom_command(
    OUTPUT ${COMMAND_OUTPUT_H} ${COMMAND_OUTPUT_C}
    COMMAND python3 ${GEN_SCRIPT} -j ${COMMAND_JSON} -f ${COMMAND_OUTPUT_FILE} -i ${COMMAND_INFO_FOLDER_NAME}
    DEPENDS ${GEN_SCRIPT} ${COMMAND_JSON}
    COMMENT "Generating ${COMMAND_INFO_FILE_NAME}.h, ${COMMAND_INFO_FILE_NAME}.c"
    VERBATIM
)

# Create a custom target that relies on the generated files
add_custom_target(generate_command_info ALL DEPENDS ${COMMAND_OUTPUT_H} ${COMMAND_OUTPUT_C})

#----------------------------------------------------------------------------------------------
# Add subdirectories for dependencies (from deps/)
add_subdirectory(${root}/deps/rmutil ${CMAKE_CURRENT_BINARY_DIR}/rmutil)
add_subdirectory(${root}/deps/friso ${CMAKE_CURRENT_BINARY_DIR}/friso)
add_subdirectory(${root}/deps/snowball ${CMAKE_CURRENT_BINARY_DIR}/snowball)
add_subdirectory(${root}/deps/phonetics ${CMAKE_CURRENT_BINARY_DIR}/phonetics)
add_subdirectory(${root}/deps/fast_float ${CMAKE_CURRENT_BINARY_DIR}/fast_float)

# Configure libuv options
set(LIBUV_BUILD_TESTS OFF CACHE BOOL "Build libuv tests" FORCE)
set(LIBUV_BUILD_BENCH OFF CACHE BOOL "Build libuv benchmarks" FORCE)
set(LIBUV_BUILD_SHARED OFF CACHE BOOL "Build shared libuv library" FORCE)
add_subdirectory(${root}/deps/libuv ${CMAKE_CURRENT_BINARY_DIR}/libuv)

option(VECSIM_BUILD_TESTS "Build vecsim tests" OFF)
add_subdirectory(${root}/deps/VectorSimilarity ${CMAKE_CURRENT_BINARY_DIR}/VectorSimilarity)

#----------------------------------------------------------------------------------------------
# Add subdirectories for src/ components
add_subdirectory(geometry)
add_subdirectory(buffer)
add_subdirectory(iterators)
add_subdirectory(wildcard)
add_subdirectory(index_result)
add_subdirectory(value)
add_subdirectory(util/mempool)
add_subdirectory(util/arr)
add_subdirectory(util/dict)
add_subdirectory(util/hash)
add_subdirectory(coord)
add_subdirectory(ttl_table)

#----------------------------------------------------------------------------------------------
# Source files for the core library
file(GLOB SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/pipeline/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/aggregate/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/aggregate/expr/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/aggregate/functions/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/aggregate/reducers/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/command_info/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/hybrid/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/hybrid/parse/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/ext/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/hll/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/query_parser/v1/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/query_parser/v2/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/util/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/trie/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/info/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/info/info_redis/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/info/info_redis/threads/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/info/info_redis/types/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/module-init/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/obfuscation/*.c"

    "${root}/deps/cndict/cndict_data.c"
    "${root}/deps/libnu/*.c"
    "${root}/deps/miniz/*.c"
    "${root}/deps/fast_float/*.c"
    "${root}/deps/thpool/*.c"
    "${root}/deps/geohash/*.c")

#----------------------------------------------------------------------------------------------
# Create the rscore object library
add_library(rscore OBJECT ${SOURCES})
add_dependencies(rscore generate_command_info)

# Collect all object files
set(FINAL_OBJECTS
    $<TARGET_OBJECTS:arr>
    $<TARGET_OBJECTS:buffer>
    $<TARGET_OBJECTS:dict>
    $<TARGET_OBJECTS:iterators>
    $<TARGET_OBJECTS:wildcard>
    $<TARGET_OBJECTS:index_result>
    $<TARGET_OBJECTS:mempool>
    $<TARGET_OBJECTS:value>
    $<TARGET_OBJECTS:rscore>
    $<TARGET_OBJECTS:rmutil>
    $<TARGET_OBJECTS:friso>
    $<TARGET_OBJECTS:snowball>
    $<TARGET_OBJECTS:metaphone>
    $<TARGET_OBJECTS:fast_float_strtod>
    $<TARGET_OBJECTS:redisearch-coord>
    $<TARGET_OBJECTS:ttl_table>
)

#----------------------------------------------------------------------------------------------
# Common link libraries for both static and shared targets
set(REDISEARCH_C_LINK_LIBS
    redisearch-geometry
    redisearch-hash
    VectorSimilarity
    redisearch-coord
    arr
    buffer
    dict
    iterators
    index_result
    value
    mempool
    wildcard
    uv_a
    ttl_table
    ${HIREDIS_LIBS}
    ${SSL_LIBS}
    ${CMAKE_LD_LIBS})

# Common properties for both static and shared targets
set(REDISEARCH_C_PROPERTIES
    LINKER_LANGUAGE CXX
    C_STANDARD 17
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON)

# Common dependencies for both targets
set(REDISEARCH_C_DEPENDENCIES VectorSimilarity generate_command_info uv_a)

#----------------------------------------------------------------------------------------------
# Build the static library containing all C code
add_library(redisearch_c STATIC ${FINAL_OBJECTS})
set_target_properties(redisearch_c PROPERTIES ${REDISEARCH_C_PROPERTIES})
target_link_libraries(redisearch_c ${REDISEARCH_C_LINK_LIBS})
add_dependencies(redisearch_c ${REDISEARCH_C_DEPENDENCIES})

# Export FINAL_OBJECTS to parent scope for potential use
set(REDISEARCH_C_FINAL_OBJECTS ${FINAL_OBJECTS} PARENT_SCOPE)

#----------------------------------------------------------------------------------------------
# Build a shared library for Rust benchmarks/tests to link against.
# This bundles all C dependencies into a single dylib, allowing undefined
# symbols (like RedisModule_*) to be resolved at runtime.
add_library(redisearch_c_shared SHARED ${FINAL_OBJECTS})
set_target_properties(redisearch_c_shared PROPERTIES
    ${REDISEARCH_C_PROPERTIES}
    OUTPUT_NAME "redisearch_c")
target_link_libraries(redisearch_c_shared ${REDISEARCH_C_LINK_LIBS})
add_dependencies(redisearch_c_shared ${REDISEARCH_C_DEPENDENCIES})

# Platform-specific linker flags to allow undefined symbols
if(APPLE)
    set_target_properties(redisearch_c_shared PROPERTIES
        LINK_FLAGS "-undefined dynamic_lookup"
        SUFFIX ".dylib")
else()
    set_target_properties(redisearch_c_shared PROPERTIES
        LINK_FLAGS "-Wl,--allow-shlib-undefined")
endif()
