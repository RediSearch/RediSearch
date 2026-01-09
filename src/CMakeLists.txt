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

# Export FINAL_OBJECTS to parent scope for the top-level redisearch target
set(REDISEARCH_C_FINAL_OBJECTS ${FINAL_OBJECTS} PARENT_SCOPE)

#----------------------------------------------------------------------------------------------
# Build a static library from the C object files
add_library(redisearch_c STATIC ${FINAL_OBJECTS})
set_target_properties(redisearch_c PROPERTIES LINKER_LANGUAGE CXX)

# Declare dependencies for transitive linking
target_link_libraries(redisearch_c
    redisearch-geometry
    redisearch-hash
    VectorSimilarity
    redisearch-coord
    uv_a
    ttl_table
    ${HIREDIS_LIBS})

add_dependencies(redisearch_c VectorSimilarity)
add_dependencies(redisearch_c generate_command_info)

#----------------------------------------------------------------------------------------------
# Build a combined static library for Rust tests/benchmarks (only when tests are enabled)
if(BUILD_SEARCH_UNIT_TESTS)
    # Recursively collect all static library dependencies from a target
    function(collect_static_libs target out_libs out_targets)
        set(libs ${${out_libs}})
        set(targets ${${out_targets}})

        # Skip if already visited
        if(${target} IN_LIST targets)
            return()
        endif()

        # Check if it's a valid target
        if(NOT TARGET ${target})
            return()
        endif()

        # Get target type
        get_target_property(target_type ${target} TYPE)

        # Add static libraries to our list (skip INTERFACE libraries - they have no .a file)
        if(target_type STREQUAL "STATIC_LIBRARY")
            list(APPEND libs $<TARGET_FILE:${target}>)
            list(APPEND targets ${target})
        endif()

        # Mark as visited (even for INTERFACE libs to avoid infinite loops)
        if(target_type STREQUAL "INTERFACE_LIBRARY")
            list(APPEND targets ${target})
        endif()

        # Recurse into dependencies (both LINK_LIBRARIES and INTERFACE_LINK_LIBRARIES)
        foreach(prop LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
            get_target_property(dep_libs ${target} ${prop})
            if(dep_libs)
                foreach(dep IN LISTS dep_libs)
                    # Skip generator expressions and non-targets
                    if(NOT dep MATCHES "^\\$<" AND TARGET ${dep})
                        collect_static_libs(${dep} libs targets)
                    endif()
                endforeach()
            endif()
        endforeach()

        set(${out_libs} ${libs} PARENT_SCOPE)
        set(${out_targets} ${targets} PARENT_SCOPE)
    endfunction()

    set(REDISEARCH_ALL_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/libredisearch_all.a")

    # Automatically collect all static library dependencies from redisearch_c
    set(LIBS_TO_MERGE "")
    set(VISITED_TARGETS "")
    collect_static_libs(redisearch_c LIBS_TO_MERGE VISITED_TARGETS)

    # Remove duplicates while preserving order
    list(REMOVE_DUPLICATES LIBS_TO_MERGE)
    list(REMOVE_DUPLICATES VISITED_TARGETS)

    message(STATUS "Libraries to merge into libredisearch_all.a: ${VISITED_TARGETS}")

    # Create the combined library using platform-specific tools
    if(APPLE)
        # macOS: use libtool to merge static libraries
        add_custom_command(
            OUTPUT ${REDISEARCH_ALL_OUTPUT}
            COMMAND libtool -static -o ${REDISEARCH_ALL_OUTPUT} ${LIBS_TO_MERGE}
            DEPENDS ${VISITED_TARGETS}
            COMMENT "Creating unified libredisearch_all.a with libtool"
            VERBATIM
            COMMAND_EXPAND_LISTS
        )
    else()
        # Linux: extract all .o files and recombine with ar
        # We generate a CMake script to avoid shell escaping issues with VERBATIM
        set(MERGE_TEMP_DIR "${CMAKE_CURRENT_BINARY_DIR}/merge_temp")
        set(MERGE_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/merge_libs.cmake")

        # Generate a CMake script that extracts all .o files
        # Each library is extracted into its own subdirectory to avoid filename collisions
        # (e.g., multiple libraries may have util.o or common.o)
        file(WRITE ${MERGE_SCRIPT} "
file(REMOVE_RECURSE \"${MERGE_TEMP_DIR}\")
file(MAKE_DIRECTORY \"${MERGE_TEMP_DIR}\")
set(LIBS \"${LIBS_TO_MERGE}\")
set(LIB_INDEX 0)
foreach(lib \${LIBS})
    # Create a unique subdirectory for each library to avoid .o filename collisions
    set(LIB_DIR \"${MERGE_TEMP_DIR}/lib_\${LIB_INDEX}\")
    file(MAKE_DIRECTORY \${LIB_DIR})
    execute_process(
        COMMAND ar -x \${lib}
        WORKING_DIRECTORY \${LIB_DIR}
    )
    math(EXPR LIB_INDEX \"\${LIB_INDEX} + 1\")
endforeach()
# Collect all .o files from all subdirectories
file(GLOB_RECURSE OBJ_FILES \"${MERGE_TEMP_DIR}/*.o\")
execute_process(
    COMMAND ar -rcs \"${REDISEARCH_ALL_OUTPUT}\" \${OBJ_FILES}
)
file(REMOVE_RECURSE \"${MERGE_TEMP_DIR}\")
")
        add_custom_command(
            OUTPUT ${REDISEARCH_ALL_OUTPUT}
            COMMAND ${CMAKE_COMMAND} -P ${MERGE_SCRIPT}
            DEPENDS ${VISITED_TARGETS}
            COMMENT "Creating unified libredisearch_all.a with ar"
            VERBATIM
        )
    endif()

    add_custom_target(redisearch_all ALL DEPENDS ${REDISEARCH_ALL_OUTPUT})
endif()
