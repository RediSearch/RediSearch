# Invoked as `cmake -DOBJECT=<path> -P` by extract_debug_symbols().
#
# A POST_BUILD command cannot be conditional, and custom targets are always
# considered out of date, so this runs again on objects it already processed.
# Repeating the rewrite would fail on the existing `.gnu_debuglink` and would
# copy the already-stripped object over the sidecar, destroying the DWARF.
# Checking first makes a repeat run a no-op.

# extract_debug_symbols() forwards the binutils CMake resolved for the active
# toolchain. Falling back to PATH would pick host tools when cross-compiling, or
# GNU tools under an LLVM-only toolchain.
foreach(tool readelf objcopy strip)
    string(TOUPPER ${tool} _var)
    set(_tool "${EDS_${_var}}")
    if(NOT _tool)
        find_program(_found_${_var} ${tool})
        set(_tool "${_found_${_var}}")
    endif()
    if(NOT _tool)
        message(FATAL_ERROR "extract_debug_symbols: ${tool} not found")
    endif()
    set(_EDS_${_var} "${_tool}")
endforeach()

function(_eds_run)
    execute_process(COMMAND ${ARGV} RESULT_VARIABLE _result)
    if(NOT _result EQUAL 0)
        message(FATAL_ERROR "extract_debug_symbols: ${ARGV} failed (${_result})")
    endif()
endfunction()

execute_process(
    COMMAND ${_EDS_READELF} -S "${OBJECT}"
    OUTPUT_VARIABLE _sections
    RESULT_VARIABLE _result
)
if(NOT _result EQUAL 0)
    message(FATAL_ERROR "extract_debug_symbols: cannot read ${OBJECT}")
endif()

# `.zdebug_info` is the same section under --compress-debug-sections=zlib-gnu.
if(NOT _sections MATCHES "\\.z?debug_info")
    message(STATUS "extract_debug_symbols: ${OBJECT} has no DWARF, already extracted")
    return()
endif()

_eds_run(${CMAKE_COMMAND} -E copy "${OBJECT}" "${OBJECT}.debug")
_eds_run(${_EDS_OBJCOPY} "--add-gnu-debuglink=${OBJECT}.debug" "${OBJECT}")
_eds_run(${_EDS_STRIP} -g "${OBJECT}")
