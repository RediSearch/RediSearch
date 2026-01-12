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
    # Uses global variables _COLLECT_LIBS and _COLLECT_TARGETS for accumulation
    function(collect_static_libs_impl target)
        # Skip if already visited
        if(${target} IN_LIST _COLLECT_TARGETS)
            return()
        endif()

        # Check if it's a valid target
        if(NOT TARGET ${target})
            return()
        endif()

        # Get target type and check if it's imported
        get_target_property(target_type ${target} TYPE)
        get_target_property(is_imported ${target} IMPORTED)

        # Handle static libraries (both regular and IMPORTED)
        if(target_type STREQUAL "STATIC_LIBRARY")
            if(is_imported)
                # IMPORTED static library - get the actual file location
                get_target_property(location ${target} IMPORTED_LOCATION)
                if(NOT location)
                    # Try config-specific locations
                    get_target_property(location ${target} IMPORTED_LOCATION_RELEASE)
                endif()
                if(NOT location)
                    get_target_property(location ${target} IMPORTED_LOCATION_NOCONFIG)
                endif()
                if(location AND location MATCHES "\\.(a|lib)$")
                    message(STATUS "  Found IMPORTED static library: ${target} -> ${location}")
                    list(APPEND _COLLECT_LIBS ${location})
                    list(APPEND _COLLECT_TARGETS ${target})
                    set(_COLLECT_LIBS ${_COLLECT_LIBS} PARENT_SCOPE)
                    set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
                endif()
            else()
                # Regular static library - use generator expression
                message(STATUS "  Found static library: ${target}")
                list(APPEND _COLLECT_LIBS $<TARGET_FILE:${target}>)
                list(APPEND _COLLECT_TARGETS ${target})
                set(_COLLECT_LIBS ${_COLLECT_LIBS} PARENT_SCOPE)
                set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
            endif()
        elseif(target_type STREQUAL "UNKNOWN_LIBRARY")
            # IMPORTED targets with unknown type - check if they're static libraries
            get_target_property(location ${target} IMPORTED_LOCATION)
            if(NOT location)
                get_target_property(location ${target} IMPORTED_LOCATION_RELEASE)
            endif()
            if(location AND location MATCHES "\\.(a|lib)$")
                message(STATUS "  Found UNKNOWN_LIBRARY static: ${target} -> ${location}")
                list(APPEND _COLLECT_LIBS ${location})
                list(APPEND _COLLECT_TARGETS ${target})
                set(_COLLECT_LIBS ${_COLLECT_LIBS} PARENT_SCOPE)
                set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
            endif()
        elseif(target_type STREQUAL "INTERFACE_LIBRARY")
            # Mark as visited to avoid infinite loops (no .a file)
            message(STATUS "  Found INTERFACE library: ${target}")
            list(APPEND _COLLECT_TARGETS ${target})
            set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
        else()
            # Mark other types as visited
            list(APPEND _COLLECT_TARGETS ${target})
            set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
        endif()

        # Recurse into dependencies (both LINK_LIBRARIES and INTERFACE_LINK_LIBRARIES)
        foreach(prop LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
            get_target_property(dep_libs ${target} ${prop})
            if(dep_libs)
                message(STATUS "    ${target} ${prop}: ${dep_libs}")
                foreach(dep IN LISTS dep_libs)
                    # Skip generator expressions and non-targets
                    if(NOT dep MATCHES "^\\$<" AND TARGET ${dep})
                        collect_static_libs_impl(${dep})
                        # Propagate changes back up
                        set(_COLLECT_LIBS ${_COLLECT_LIBS} PARENT_SCOPE)
                        set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
                    elseif(NOT dep MATCHES "^\\$<")
                        message(STATUS "      Skipping non-target: ${dep}")
                    endif()
                endforeach()
            endif()
        endforeach()
    endfunction()

    # Wrapper function that initializes and returns results
    macro(collect_static_libs target out_libs out_targets)
        set(_COLLECT_LIBS "")
        set(_COLLECT_TARGETS "")
        collect_static_libs_impl(${target})
        set(${out_libs} ${_COLLECT_LIBS})
        set(${out_targets} ${_COLLECT_TARGETS})
    endmacro()

    set(REDISEARCH_ALL_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/libredisearch_all.a")

    # Automatically collect all static library dependencies from redisearch_c
    message(STATUS "Collecting static libraries for libredisearch_all.a...")
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
        # Linux: use ar with MRI script to merge static libraries
        # Generate MRI script with library paths (one ADDLIB per line)
        set(MRI_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/merge_libs.mri")
        string(REPLACE ";" "\nADDLIB " ADDLIB_COMMANDS "${LIBS_TO_MERGE}")
        file(GENERATE OUTPUT ${MRI_SCRIPT} CONTENT
"CREATE ${REDISEARCH_ALL_OUTPUT}
ADDLIB ${ADDLIB_COMMANDS}
SAVE
END
")
        add_custom_command(
            OUTPUT ${REDISEARCH_ALL_OUTPUT}
            COMMAND ${CMAKE_COMMAND} -E rm -f ${REDISEARCH_ALL_OUTPUT}
            COMMAND ${CMAKE_COMMAND} -E echo "Merging libraries from MRI script:"
            COMMAND ${CMAKE_COMMAND} -E cat ${MRI_SCRIPT}
            COMMAND ar -M < ${MRI_SCRIPT}
            DEPENDS ${VISITED_TARGETS} ${MRI_SCRIPT}
            COMMENT "Creating unified libredisearch_all.a with ar"
            VERBATIM
        )
    endif()

    add_custom_target(redisearch_all ALL DEPENDS ${REDISEARCH_ALL_OUTPUT})
endif()
