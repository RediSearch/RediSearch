#pragma once

#ifdef BUILD_TRACE
#define NAMED_TRACED_BP(label) \
    do { \
    printf("%s\r\n", label); \
    register char*  arg2 asm("rsi") = (char*)label; \
    asm("int3"); \
    } while(0);
#else 
#define NAMED_TRACED_BP(label) ;
#endif
