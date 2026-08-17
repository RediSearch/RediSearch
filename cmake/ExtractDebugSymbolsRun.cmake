# Invoked as `cmake -DOBJECT=<path> -P` by extract_debug_symbols().
#
# A POST_BUILD command cannot be conditional, and custom targets are always
# considered out of date, so this runs again on objects it already processed.
# Repeating the rewrite would fail on the existing `.gnu_debuglink` and would
# copy the already-stripped object over the sidecar, destroying the DWARF.
# Checking first makes a repeat run a no-op.

foreach(tool readelf objcopy strip)
    string(TOUPPER ${tool} _var)
    find_program(_EDS_${_var} ${tool})
    if(NOT _EDS_${_var})
        message(FATAL_ERROR "extract_debug_symbols: ${tool} not found")
    endif()
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

if(NOT _sections MATCHES "\\.debug_info")
    message(STATUS "extract_debug_symbols: ${OBJECT} has no DWARF, already extracted")
    return()
endif()

_eds_run(${CMAKE_COMMAND} -E copy "${OBJECT}" "${OBJECT}.debug")
_eds_run(${_EDS_OBJCOPY} "--add-gnu-debuglink=${OBJECT}.debug" "${OBJECT}")
_eds_run(${_EDS_STRIP} -g "${OBJECT}")
