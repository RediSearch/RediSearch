#pragma once

#ifdef BUILD_TRACE
#define NAMED_TRACED_BP(label) \
    register char*  arg2 asm("rsi") = (char*)label; \
    asm("int3");
#else 
#define NAMED_TRACED_BP(label) ;
#endif
