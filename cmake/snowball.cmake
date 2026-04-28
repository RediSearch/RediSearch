# cmake/snowball.cmake
#
# Builds the snowball stemmer library from the snowball git submodule.
#
# Stages:
#   1. Build the snowball compiler (host tool)
#   2. Generate C stemmers from .sbl algorithm files
#   3. Generate module registry headers via mkmodules.pl (requires Perl)
#   4. Generate libstemmer.c from the template
#   5. Patch runtime and libstemmer sources to use rmalloc
#   6. Compile into the `snowball` OBJECT library

set(SNOWBALL_SRC "${root}/deps/snowball")
set(SNOWBALL_BUILD "${CMAKE_CURRENT_BINARY_DIR}/snowball")
set(SNOWBALL_PATCH_SCRIPT "${root}/cmake/patch_snowball_alloc.cmake")
set(SNOWBALL_MKMODULES_SCRIPT "${root}/cmake/generate_snowball_modules_h.cmake")

# =============================================================================
# Stage 1: Build the snowball compiler
# =============================================================================

add_executable(snowball_compiler
    ${SNOWBALL_SRC}/compiler/analyser.c
    ${SNOWBALL_SRC}/compiler/driver.c
    ${SNOWBALL_SRC}/compiler/generator.c
    ${SNOWBALL_SRC}/compiler/generator_ada.c
    ${SNOWBALL_SRC}/compiler/generator_csharp.c
    ${SNOWBALL_SRC}/compiler/generator_go.c
    ${SNOWBALL_SRC}/compiler/generator_java.c
    ${SNOWBALL_SRC}/compiler/generator_js.c
    ${SNOWBALL_SRC}/compiler/generator_pascal.c
    ${SNOWBALL_SRC}/compiler/generator_python.c
    ${SNOWBALL_SRC}/compiler/generator_rust.c
    ${SNOWBALL_SRC}/compiler/space.c
    ${SNOWBALL_SRC}/compiler/tokeniser.c
)
set_target_properties(snowball_compiler PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    RUNTIME_OUTPUT_DIRECTORY "${SNOWBALL_BUILD}/bin"
)

# The upstream snowball compiler leaves small allocations unreleased (e.g. driver read_options).
# When SAN=address, the host tool is linked with ASan; disable LeakSanitizer for stemmer emission only.
if(SAN STREQUAL "address")
    set(SNOWBALL_COMPILER_CMD
        ${CMAKE_COMMAND} -E env "ASAN_OPTIONS=detect_leaks=0" $<TARGET_FILE:snowball_compiler>)
else()
    set(SNOWBALL_COMPILER_CMD $<TARGET_FILE:snowball_compiler>)
endif()

# =============================================================================
# Stage 2: Parse modules.txt and generate C stemmers
# =============================================================================

set(SNOWBALL_STEMMER_SOURCES "")
set(SNOWBALL_STEMMER_HEADERS "")

# Parses a modules.txt file and generates C stemmers from the .sbl algorithms.
# Uses a macro so that list(APPEND ...) modifies the caller's scope directly.
macro(snowball_generate_stemmers _modules_txt _algorithms_dir _label)
    file(STRINGS "${_modules_txt}" _lines)
    foreach(_line IN LISTS _lines)
        string(STRIP "${_line}" _line)
        if(_line STREQUAL "" OR _line MATCHES "^#")
            continue()
        endif()

        # Parse: algorithm  encodings  aliases [parent_algorithm]
        string(REGEX MATCH "^([^ \t]+)[ \t]+([^ \t]+)" _ "${_line}")
        set(_alg "${CMAKE_MATCH_1}")

        set(_stem_base "stem_UTF_8_${_alg}")
        set(_stem_c "${SNOWBALL_BUILD}/src_c/${_stem_base}.c")
        set(_stem_h "${SNOWBALL_BUILD}/src_c/${_stem_base}.h")

        add_custom_command(
            OUTPUT "${_stem_c}" "${_stem_h}"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${SNOWBALL_BUILD}/src_c"
            COMMAND ${SNOWBALL_COMPILER_CMD}
                "${_algorithms_dir}/${_alg}.sbl"
                -o "${SNOWBALL_BUILD}/src_c/${_stem_base}"
                -eprefix "${_alg}_UTF_8_"
                -r runtime
                -u
            DEPENDS snowball_compiler
                    "${_algorithms_dir}/${_alg}.sbl"
            COMMENT "Generating ${_label}: ${_stem_base}"
            VERBATIM
        )

        list(APPEND SNOWBALL_STEMMER_SOURCES "${_stem_c}")
        list(APPEND SNOWBALL_STEMMER_HEADERS "${_stem_h}")
    endforeach()
endmacro()

# --- 2a: Upstream snowball stemmers (from deps/snowball) ---
snowball_generate_stemmers(
    "${SNOWBALL_SRC}/libstemmer/modules.txt"
    "${SNOWBALL_SRC}/algorithms"
    "snowball stemmer"
)

# --- 2b: Custom stemmers (from deps/stemmers) ---
set(CUSTOM_STEMMERS_SRC "${root}/deps/stemmers")
set(CUSTOM_STEMMERS_MODULES_TXT "${CUSTOM_STEMMERS_SRC}/modules.txt")
if(EXISTS "${CUSTOM_STEMMERS_MODULES_TXT}")
    snowball_generate_stemmers(
        "${CUSTOM_STEMMERS_MODULES_TXT}"
        "${CUSTOM_STEMMERS_SRC}/algorithms"
        "custom stemmer"
    )
endif()

# =============================================================================
# Stage 3: Generate module registry header (modules.h)
# =============================================================================

set(SNOWBALL_MODULES_H "${SNOWBALL_BUILD}/libstemmer/modules.h")

set(_MODULES_H_DEPENDS
    "${SNOWBALL_MKMODULES_SCRIPT}"
    "${SNOWBALL_SRC}/libstemmer/modules.txt"
)
set(_EXTRA_MODULES_TXT_ARG "")
if(EXISTS "${CUSTOM_STEMMERS_MODULES_TXT}")
    set(_EXTRA_MODULES_TXT_ARG "-DEXTRA_MODULES_TXT=${CUSTOM_STEMMERS_MODULES_TXT}")
    list(APPEND _MODULES_H_DEPENDS "${CUSTOM_STEMMERS_MODULES_TXT}")
endif()

add_custom_command(
    OUTPUT "${SNOWBALL_MODULES_H}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${SNOWBALL_BUILD}/libstemmer"
    COMMAND ${CMAKE_COMMAND}
        "-DMODULES_TXT=${SNOWBALL_SRC}/libstemmer/modules.txt"
        ${_EXTRA_MODULES_TXT_ARG}
        "-DOUTPUT=${SNOWBALL_MODULES_H}"
        "-DC_SRC_DIR=src_c"
        -P "${SNOWBALL_MKMODULES_SCRIPT}"
    DEPENDS ${_MODULES_H_DEPENDS}
    COMMENT "Generating snowball module registry (modules.h)"
    VERBATIM
)

# =============================================================================
# Stage 4: Generate libstemmer.c from the template (libstemmer_c.in)
# =============================================================================

# The template contains @MODULES_H@ which configure_file replaces.
set(MODULES_H "modules.h")
configure_file(
    "${SNOWBALL_SRC}/libstemmer/libstemmer_c.in"
    "${SNOWBALL_BUILD}/libstemmer/libstemmer_unpatched.c"
    @ONLY
)

# =============================================================================
# Stage 5: Patch allocator calls in runtime and libstemmer sources
# =============================================================================

set(PATCHED_API_C "${SNOWBALL_BUILD}/runtime/api.c")
add_custom_command(
    OUTPUT "${PATCHED_API_C}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${SNOWBALL_BUILD}/runtime"
    COMMAND ${CMAKE_COMMAND}
        "-DINPUT=${SNOWBALL_SRC}/runtime/api.c"
        "-DOUTPUT=${PATCHED_API_C}"
        -P "${SNOWBALL_PATCH_SCRIPT}"
    DEPENDS "${SNOWBALL_SRC}/runtime/api.c" "${SNOWBALL_PATCH_SCRIPT}"
    COMMENT "Patching snowball runtime/api.c with rmalloc"
    VERBATIM
)

set(PATCHED_UTILITIES_C "${SNOWBALL_BUILD}/runtime/utilities.c")
add_custom_command(
    OUTPUT "${PATCHED_UTILITIES_C}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${SNOWBALL_BUILD}/runtime"
    COMMAND ${CMAKE_COMMAND}
        "-DINPUT=${SNOWBALL_SRC}/runtime/utilities.c"
        "-DOUTPUT=${PATCHED_UTILITIES_C}"
        -P "${SNOWBALL_PATCH_SCRIPT}"
    DEPENDS "${SNOWBALL_SRC}/runtime/utilities.c" "${SNOWBALL_PATCH_SCRIPT}"
    COMMENT "Patching snowball runtime/utilities.c with rmalloc"
    VERBATIM
)

set(PATCHED_LIBSTEMMER_C "${SNOWBALL_BUILD}/libstemmer/libstemmer.c")
add_custom_command(
    OUTPUT "${PATCHED_LIBSTEMMER_C}"
    COMMAND ${CMAKE_COMMAND}
        "-DINPUT=${SNOWBALL_BUILD}/libstemmer/libstemmer_unpatched.c"
        "-DOUTPUT=${PATCHED_LIBSTEMMER_C}"
        -P "${SNOWBALL_PATCH_SCRIPT}"
    DEPENDS "${SNOWBALL_BUILD}/libstemmer/libstemmer_unpatched.c"
            "${SNOWBALL_PATCH_SCRIPT}"
    COMMENT "Patching snowball libstemmer.c with rmalloc"
    VERBATIM
)

# =============================================================================
# Stage 6: Compile the snowball OBJECT library
# =============================================================================

add_library(snowball OBJECT
    "${PATCHED_API_C}"
    "${PATCHED_UTILITIES_C}"
    "${PATCHED_LIBSTEMMER_C}"
    ${SNOWBALL_STEMMER_SOURCES}
    # Generated headers listed so CMake knows to run their custom commands
    # before compiling any source in this target.
    "${SNOWBALL_MODULES_H}"
    ${SNOWBALL_STEMMER_HEADERS}
)
set_source_files_properties(
    "${SNOWBALL_MODULES_H}" ${SNOWBALL_STEMMER_HEADERS}
    PROPERTIES HEADER_FILE_ONLY TRUE
)

target_include_directories(snowball PRIVATE
    "${SNOWBALL_SRC}"              # resolves "runtime/header.h" from generated stemmers
    "${SNOWBALL_SRC}/include"      # resolves "libstemmer.h"
    "${SNOWBALL_SRC}/runtime"      # resolves "header.h" and "api.h"
    "${SNOWBALL_BUILD}/libstemmer" # resolves "modules.h"
    "${SNOWBALL_BUILD}/src_c"      # resolves generated stem_*.h
)
