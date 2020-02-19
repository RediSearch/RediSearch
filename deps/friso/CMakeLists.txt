IF ("${CMAKE_C_COMPILER_ID}" MATCHES "Clang")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-tautological-compare")
ENDIF()

ADD_LIBRARY(friso OBJECT
    friso.c
    friso_array.c
    friso_hash.c
    friso_lexicon.c
    friso_link.c
    friso_string.c
    friso_ctype.c
    friso_UTF8.c
    friso_GBK.c)