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
    # Recursively collect all static library dependencies from a target.
    # Accumulates results in _COLLECT_LIBS (library paths) and _COLLECT_TARGETS (target names).
    function(_collect_static_libs_recurse target)
        # Skip if already visited or not a valid target
        if(target IN_LIST _COLLECT_TARGETS OR NOT TARGET ${target})
            return()
        endif()

        # Mark as visited immediately to avoid cycles
        list(APPEND _COLLECT_TARGETS ${target})
        set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)

        # Get target properties
        get_target_property(target_type ${target} TYPE)
        get_target_property(is_imported ${target} IMPORTED)

        # Try to get library file path for static libraries
        set(lib_path "")
        if(target_type STREQUAL "STATIC_LIBRARY")
            if(is_imported)
                # Try various IMPORTED_LOCATION properties
                foreach(loc_prop IMPORTED_LOCATION IMPORTED_LOCATION_RELEASE IMPORTED_LOCATION_NOCONFIG)
                    get_target_property(lib_path ${target} ${loc_prop})
                    if(lib_path)
                        break()
                    endif()
                endforeach()
            else()
                set(lib_path $<TARGET_FILE:${target}>)
            endif()
        elseif(target_type STREQUAL "UNKNOWN_LIBRARY")
            # IMPORTED target with unknown type - check if it's a static library
            foreach(loc_prop IMPORTED_LOCATION IMPORTED_LOCATION_RELEASE)
                get_target_property(lib_path ${target} ${loc_prop})
                if(lib_path)
                    break()
                endif()
            endforeach()
        endif()

        # Add to collection if we found a static library
        if(lib_path AND (lib_path MATCHES "\\.(a|lib)$" OR NOT is_imported))
            list(APPEND _COLLECT_LIBS ${lib_path})
            set(_COLLECT_LIBS ${_COLLECT_LIBS} PARENT_SCOPE)
        endif()

        # Recurse into link dependencies
        foreach(prop LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
            get_target_property(deps ${target} ${prop})
            if(NOT deps)
                continue()
            endif()
            foreach(dep IN LISTS deps)
                # Skip generator expressions
                if(dep MATCHES "^\\$<")
                    continue()
                endif()
                # Recurse if it's a valid target
                if(TARGET ${dep})
                    # Resolve ALIAS targets to their real name
                    get_target_property(aliased ${dep} ALIASED_TARGET)
                    if(aliased)
                        set(dep ${aliased})
                    endif()
                    _collect_static_libs_recurse(${dep})
                    set(_COLLECT_LIBS ${_COLLECT_LIBS} PARENT_SCOPE)
                    set(_COLLECT_TARGETS ${_COLLECT_TARGETS} PARENT_SCOPE)
                endif()
            endforeach()
        endforeach()
    endfunction()

    # Collect all static library dependencies from a target into output variables.
    macro(collect_static_libs target out_libs out_targets)
        set(_COLLECT_LIBS "")
        set(_COLLECT_TARGETS "")
        _collect_static_libs_recurse(${target})
        set(${out_libs} ${_COLLECT_LIBS})
        set(${out_targets} ${_COLLECT_TARGETS})
    endmacro()

    set(REDISEARCH_ALL_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/libredisearch_all.a")

    # Collect all static library dependencies from redisearch_c
    set(LIBS_TO_MERGE "")
    set(VISITED_TARGETS "")
    collect_static_libs(redisearch_c LIBS_TO_MERGE VISITED_TARGETS)

    # On Linux, SVS is fetched via FetchContent and its IMPORTED targets aren't
    # visible from this scope. Add SVS static libraries from the known path.
    set(_svs_lib_dir "${CMAKE_CURRENT_BINARY_DIR}/VectorSimilarity/_deps/svs-src/lib")
    if(EXISTS "${_svs_lib_dir}")
        file(GLOB _svs_static_libs "${_svs_lib_dir}/*.a")
        list(APPEND LIBS_TO_MERGE ${_svs_static_libs})
        message(STATUS "SVS static libraries found: ${_svs_static_libs}")
    else()
        message(STATUS "SVS lib directory not found: ${_svs_lib_dir}")
    endif()

    list(REMOVE_DUPLICATES LIBS_TO_MERGE)
    list(REMOVE_DUPLICATES VISITED_TARGETS)
    message(STATUS "Static libraries for libredisearch_all.a: ${VISITED_TARGETS}")

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
            COMMAND ar -M < ${MRI_SCRIPT}
            DEPENDS ${VISITED_TARGETS} ${MRI_SCRIPT}
            COMMENT "Creating unified libredisearch_all.a with ar"
            VERBATIM
        )
    endif()

    add_custom_target(redisearch_all ALL DEPENDS ${REDISEARCH_ALL_OUTPUT})
endif()
