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

set(_EDS_RUN_SCRIPT "${CMAKE_CURRENT_LIST_DIR}/ExtractDebugSymbolsRun.cmake")

function(extract_debug_symbols target)
    cmake_parse_arguments(ARG "" "FILE" "" ${ARGN})

    # Debug keeps DWARF inline for local debugging; only optimized builds ship.
    if(APPLE OR NOT CMAKE_BUILD_TYPE MATCHES "^(Release|RelWithDebInfo)$")
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

    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${CMAKE_COMMAND} "-DOBJECT=${object}" -P "${_EDS_RUN_SCRIPT}"
        COMMENT "Extracting debug symbols from ${target}"
    )
endfunction()
