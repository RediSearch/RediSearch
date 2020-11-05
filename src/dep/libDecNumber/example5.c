// example5.c -- decimal64 conversions
#include "dpd/decimal64.h"
// decimal64 and decNumber library
#include <stdio.h>
// for (s)printf
int main(int argc, char *argv[]) {
  decimal64 a;
  // working decimal64 number
  decNumber d;
  // working number
  decContext set;
  // working context
  char string[DECIMAL64_String];
  // number->string buffer
  char hexes[25];
  // decimal64->hex buffer
  int i;
  // counter
  decContextDefault(&set, DEC_INIT_DECIMAL64); // initialize
  decimal64FromString(&a, argv[1], &set);
  // lay out the decimal64 as eight hexadecimal pairs
  for (i=0; i<8; i++) {
  sprintf(&hexes[i*3], "%02x ", a.bytes[i]);
  }
  decimal64ToNumber(&a, &d);
  decNumberToString(&d, string);
  printf("%s => %s=> %s\n", argv[1], hexes, string);
  return 0;
} // main