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

find_program(PERL_EXECUTABLE perl REQUIRED)

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

# =============================================================================
# Stage 2: Parse modules.txt and generate C stemmers
# =============================================================================

set(SNOWBALL_STEMMER_SOURCES "")
set(SNOWBALL_STEMMER_HEADERS "")

file(STRINGS "${SNOWBALL_SRC}/libstemmer/modules.txt" _MODULES_LINES)
foreach(_line IN LISTS _MODULES_LINES)
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
        COMMAND $<TARGET_FILE:snowball_compiler>
            "${SNOWBALL_SRC}/algorithms/${_alg}.sbl"
            -o "${SNOWBALL_BUILD}/src_c/${_stem_base}"
            -eprefix "${_alg}_UTF_8_"
            -r runtime
            -u
        DEPENDS snowball_compiler
                "${SNOWBALL_SRC}/algorithms/${_alg}.sbl"
        COMMENT "Generating snowball stemmer: ${_stem_base}"
        VERBATIM
    )

    list(APPEND SNOWBALL_STEMMER_SOURCES "${_stem_c}")
    list(APPEND SNOWBALL_STEMMER_HEADERS "${_stem_h}")
endforeach()

# =============================================================================
# Stage 3: Generate module registry headers (modules.h) via mkmodules.pl
# =============================================================================

set(SNOWBALL_MODULES_H "${SNOWBALL_BUILD}/libstemmer/modules.h")
set(SNOWBALL_MKINC "${SNOWBALL_BUILD}/mkinc.mak")

add_custom_command(
    OUTPUT "${SNOWBALL_MODULES_H}" "${SNOWBALL_MKINC}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${SNOWBALL_BUILD}/libstemmer"
    COMMAND ${PERL_EXECUTABLE}
        "${SNOWBALL_SRC}/libstemmer/mkmodules.pl"
        "${SNOWBALL_MODULES_H}"
        src_c
        "${SNOWBALL_SRC}/libstemmer/modules.txt"
        "${SNOWBALL_MKINC}"
        utf8
    DEPENDS "${SNOWBALL_SRC}/libstemmer/mkmodules.pl"
            "${SNOWBALL_SRC}/libstemmer/modules.txt"
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
