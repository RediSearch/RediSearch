
#pragma once

#ifdef BUILD_TRACE
// This macro is used to trace the execution of the program.
// It sets the breakpoint label in the rsi register and then
// executes the int3 instruction which is SIGTRAP.
#define NAMED_TRACED_BP(label) \
    do { \
    printf("%s\r\n", label); \
    register char*  arg2 asm("rsi") = (char*)label; \
    asm("int3"); \
    } while(0)
#else 
#define NAMED_TRACED_BP(label) 
#endif
