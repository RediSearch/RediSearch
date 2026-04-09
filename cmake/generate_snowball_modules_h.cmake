# cmake/generate_snowball_modules_h.cmake
#
# Generates the snowball modules.h registry header from modules.txt.
# Replaces the upstream mkmodules.pl Perl script, filtering to UTF-8 only.
#
# Usage:
#   cmake -DMODULES_TXT=<path> -DOUTPUT=<path> -DC_SRC_DIR=<dir>
#         -P cmake/generate_snowball_modules_h.cmake

if(NOT DEFINED MODULES_TXT OR NOT DEFINED OUTPUT OR NOT DEFINED C_SRC_DIR)
    message(FATAL_ERROR "MODULES_TXT, OUTPUT, and C_SRC_DIR must be defined")
endif()

# ── Parse modules.txt ────────────────────────────────────────────────────────

file(STRINGS "${MODULES_TXT}" _lines)

set(_algorithms "")
set(_aliases "")

foreach(_line IN LISTS _lines)
    string(STRIP "${_line}" _line)
    if(_line STREQUAL "" OR _line MATCHES "^#")
        continue()
    endif()

    string(REGEX MATCH "^([^ \t]+)[ \t]+([^ \t]+)[ \t]+([^ \t]+)" _ "${_line}")
    set(_alg "${CMAKE_MATCH_1}")
    set(_encstr "${CMAKE_MATCH_2}")
    set(_aliasstr "${CMAKE_MATCH_3}")

    # Only include algorithms that support UTF-8.
    string(REPLACE "," ";" _encs "${_encstr}")
    set(_has_utf8 FALSE)
    foreach(_enc IN LISTS _encs)
        string(TOLOWER "${_enc}" _enc_lower)
        string(REPLACE "_" "" _enc_norm "${_enc_lower}")
        if(_enc_norm STREQUAL "utf8")
            set(_has_utf8 TRUE)
        endif()
    endforeach()
    if(NOT _has_utf8)
        continue()
    endif()

    list(APPEND _algorithms "${_alg}")

    string(REPLACE "," ";" _alias_list "${_aliasstr}")
    foreach(_alias IN LISTS _alias_list)
        list(APPEND _aliases "${_alias}")
        set("_ALIAS_ALG_${_alias}" "${_alg}")
    endforeach()
endforeach()

list(REMOVE_DUPLICATES _algorithms)
list(SORT _algorithms)
list(SORT _aliases)

# ── Build comment header with line-wrapping ──────────────────────────────────

set(_prefix " * Modules included by this file are: ")
string(LENGTH "${_prefix}" _linelen)
set(_comment_line "${_prefix}")
set(_need_sep FALSE)

foreach(_alg IN LISTS _algorithms)
    string(LENGTH "${_alg}" _alg_len)
    if(_need_sep)
        math(EXPR _test_len "${_linelen} + 2 + ${_alg_len}")
        if(_test_len GREATER 77)
            string(APPEND _comment_line ",\n * ")
            set(_linelen 3)
        else()
            string(APPEND _comment_line ", ")
            math(EXPR _linelen "${_linelen} + 2")
        endif()
    endif()
    string(APPEND _comment_line "${_alg}")
    math(EXPR _linelen "${_linelen} + ${_alg_len}")
    set(_need_sep TRUE)
endforeach()

# ── Generate modules.h ───────────────────────────────────────────────────────

set(_o "")
string(APPEND _o "/* ${OUTPUT}: List of stemming modules.\n")
string(APPEND _o " *\n")
string(APPEND _o " * This file is generated from modules.txt.\n")
string(APPEND _o " * Do not edit manually.\n")
string(APPEND _o " *\n")
string(APPEND _o "${_comment_line}\n */\n\n")

foreach(_alg IN LISTS _algorithms)
    string(APPEND _o "#include \"../${C_SRC_DIR}/stem_UTF_8_${_alg}.h\"\n")
endforeach()

string(APPEND _o "\ntypedef enum {\n")
string(APPEND _o "  ENC_UNKNOWN=0,\n")
string(APPEND _o "  ENC_UTF_8\n")
string(APPEND _o "} stemmer_encoding_t;\n")

string(APPEND _o "\nstruct stemmer_encoding {\n")
string(APPEND _o "  const char * name;\n")
string(APPEND _o "  stemmer_encoding_t enc;\n")
string(APPEND _o "};\n")
string(APPEND _o "static const struct stemmer_encoding encodings[] = {\n")
string(APPEND _o "  {\"UTF_8\", ENC_UTF_8},\n")
string(APPEND _o "  {0,ENC_UNKNOWN}\n")
string(APPEND _o "};\n")

string(APPEND _o "\nstruct stemmer_modules {\n")
string(APPEND _o "  const char * name;\n")
string(APPEND _o "  stemmer_encoding_t enc;\n")
string(APPEND _o "  struct SN_env * (*create)(void);\n")
string(APPEND _o "  void (*close)(struct SN_env *);\n")
string(APPEND _o "  int (*stem)(struct SN_env *);\n")
string(APPEND _o "};\n")
string(APPEND _o "static const struct stemmer_modules modules[] = {\n")

foreach(_alias IN LISTS _aliases)
    set(_alg "${_ALIAS_ALG_${_alias}}")
    set(_p "${_alg}_UTF_8")
    string(APPEND _o
        "  {\"${_alias}\", ENC_UTF_8, ${_p}_create_env, ${_p}_close_env, ${_p}_stem},\n")
endforeach()

string(APPEND _o "  {0,ENC_UNKNOWN,0,0,0}\n")
string(APPEND _o "};\n")

string(APPEND _o "static const char * algorithm_names[] = {\n")
foreach(_alg IN LISTS _algorithms)
    string(APPEND _o "  \"${_alg}\",\n")
endforeach()
string(APPEND _o "  0\n")
string(APPEND _o "};\n")

file(WRITE "${OUTPUT}" "${_o}")
