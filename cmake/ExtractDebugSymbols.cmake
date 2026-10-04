# extract_debug_symbols(<target> [FILE <path>])
#
# Move a shared object's DWARF into a `.debug` sidecar, leaving `.symtab` and a
# `.gnu_debuglink` behind. Continuous profilers read `.symtab` and fetch
# debuginfo out of band; a shipped `.so` embedding full DWARF can reach hundreds
# of megabytes, too large for that fetch, so symbols end up present on disk yet
# absent from every profile.
#
# FILE names the object explicitly, for artifacts built by a custom target where
# `$<TARGET_FILE:>` does not resolve. `objcopy` and `strip` write through a
# versioned-soname symlink, so `lib.so -> lib.so.1.2` survives.
#
# Safe to run repeatedly: the work happens in ExtractDebugSymbolsRun.cmake, which
# skips an object whose DWARF has already been moved out.

# The function survives into a parent scope after add_subdirectory(); a directory-scoped
# variable would not, leaving the generated command with no script to run.
set_property(GLOBAL PROPERTY _EDS_RUN_SCRIPT
    "${CMAKE_CURRENT_LIST_DIR}/ExtractDebugSymbolsRun.cmake")

function(extract_debug_symbols target)
    cmake_parse_arguments(ARG "" "FILE" "" ${ARGN})

    # Debug keeps DWARF inline for local debugging; only optimized builds ship.
    # CMake selects the per-config flags case-insensitively, so match that way too:
    # `release` gets -O3 -DNDEBUG just like `Release` and must be stripped as well.
    string(TOUPPER "${CMAKE_BUILD_TYPE}" build_type)
    if(APPLE OR NOT build_type MATCHES "^(RELEASE|RELWITHDEBINFO)$")
        return()
    endif()

    if(ARG_FILE)
        set(object "${ARG_FILE}")
    else()
        get_target_property(type ${target} TYPE)
        if(type STREQUAL "UTILITY")
            message(FATAL_ERROR "extract_debug_symbols(${target}): custom targets need FILE <path>")
        endif()
        # A static library is linked into someone else's shared object, which is
        # where extraction belongs.
        if(type STREQUAL "STATIC_LIBRARY")
            return()
        endif()
        set(object "$<TARGET_FILE:${target}>")
    endif()

    get_property(run_script GLOBAL PROPERTY _EDS_RUN_SCRIPT)

    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
            "-DOBJECT=${object}"
            "-DEDS_READELF=${CMAKE_READELF}"
            "-DEDS_OBJCOPY=${CMAKE_OBJCOPY}"
            "-DEDS_STRIP=${CMAKE_STRIP}"
            -P "${run_script}"
        COMMENT "Extracting debug symbols from ${target}"
    )
endfunction()
