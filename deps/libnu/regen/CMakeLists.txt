cmake_minimum_required(VERSION 3.13)
project(libnu_regen NONE)

# Regenerates deps/libnu/gen/_*.c from the UCD inputs in ./unicode.org/
# using the Python tools in ./tools/ (originally from upstream nunicode 1.11,
# Py3-ported in place). Run as:
#
#     cmake -B build -S .
#     cmake --build build --target gen
#
# Targets:
#   _tofold       -> ../gen/_tofold.c
#   _tolower      -> ../gen/_tolower.c
#   _toupper      -> ../gen/_toupper.c
#   _tounaccent   -> ../gen/_tounaccent.c
#   _ducet        -> ../gen/_ducet.c + ../gen/_ducet_switch.c
#   gen           -> all of the above
#
# After regeneration, _ducet_switch.c is rewritten in place so that
# `#include "udb.h"` becomes `#include "../udb.h"` (the path needed
# when included from deps/libnu/ducet.c).

set(DATA ${CMAKE_CURRENT_LIST_DIR}/unicode.org)
set(TOOLS ${CMAKE_CURRENT_LIST_DIR}/tools)
set(GEN ${CMAKE_CURRENT_LIST_DIR}/../gen)

set(DUCET ${DATA}/allkeys.txt)
set(SORTABLE ${DATA}/sortable.tmp)
set(FILTERED ${DATA}/filtered.tmp)
set(FILTERED_CONTRACTIONS ${DATA}/filtered_contractions.tmp)
set(UNIDATA ${DATA}/UnicodeData.txt)
set(SPECIAL ${DATA}/SpecialCasing.txt)
set(FOLDING ${DATA}/CaseFolding.txt)
set(DECOMPS ${DATA}/decomps.txt)

add_custom_target(_ducet
	COMMAND cat ${UNIDATA} | ${TOOLS}/unidata-tosortable >${SORTABLE}
	COMMAND cat ${DUCET} | ${TOOLS}/filter-sortable ${SORTABLE} >${FILTERED} 2>${FILTERED_CONTRACTIONS}
	COMMAND ${TOOLS}/filtered-tomph ${FILTERED} ${FILTERED_CONTRACTIONS}
		| ${TOOLS}/mph-indexed "NU_DUCET" 0 >${GEN}/_ducet.c
	COMMAND ${TOOLS}/contractions-toc ${FILTERED} ${FILTERED_CONTRACTIONS} "_nu_ducet" 0 >${GEN}/_ducet_switch.c
	COMMAND ${TOOLS}/fixup-ducet-switch-include.sh ${GEN}/_ducet_switch.c
	COMMAND rm -f ${SORTABLE} ${FILTERED} ${FILTERED_CONTRACTIONS}
)

add_custom_target(_toupper
	COMMAND echo -n && (cat ${UNIDATA} | ${TOOLS}/unidata-toupper
		&& cat ${SPECIAL} | ${TOOLS}/special-toupper)
	| ${TOOLS}/mph-combined "NU_TOUPPER" 0 >${GEN}/_toupper.c
)

add_custom_target(_tolower
	COMMAND echo -n && (cat ${UNIDATA} | ${TOOLS}/unidata-tolower
		&& cat ${SPECIAL} | ${TOOLS}/special-tolower)
	| ${TOOLS}/mph-combined "NU_TOLOWER" 0 >${GEN}/_tolower.c
)

add_custom_target(_tofold
	COMMAND echo -n && cat ${FOLDING} | ${TOOLS}/casefolding-tofolding
	| ${TOOLS}/mph-combined "NU_TOFOLD" 0 >${GEN}/_tofold.c
)

add_custom_target(_tounaccent
	COMMAND echo -n && cat ${DECOMPS} | ${TOOLS}/decomps-tounaccent
	| ${TOOLS}/mph-combined "NU_TOUNACCENT" 0 >${GEN}/_tounaccent.c
)

add_custom_target(gen DEPENDS _toupper _tolower _tofold _ducet _tounaccent)
