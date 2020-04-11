ADD_LIBRARY(rax OBJECT
    rax/rax.c
    rax/crc16.c
    rax/rc4rand.c)
SET_TARGET_PROPERTIES(rax PROPERTIES COMPILE_FLAGS "-fvisibility=hidden")

ADD_LIBRARY(dict OBJECT
    dict/dict.c)
SET_TARGET_PROPERTIES(dict PROPERTIES COMPILE_FLAGS "-fvisibility=hidden")
