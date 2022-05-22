FILE(GLOB SNOWBALL_SRC
    "src_c/*.c"
    "libstemmer/libstemmer.c"
    "runtime/*.c")
INCLUDE_DIRECTORIES(include)
ADD_LIBRARY(snowball OBJECT ${SNOWBALL_SRC})
