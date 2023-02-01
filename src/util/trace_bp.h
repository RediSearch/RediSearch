#pragma once

void NAMED_TRACED_BP(const char *label) {
    register char*  arg2 asm("rsi") = (char*)label;
    asm("int3");
}
