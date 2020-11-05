#include <stdio.h> 
#include <stdlib.h>

int main() {
  _Decimal32 a = strtod32("3.14");
  _Decimal32 b = 3;
  _Decimal32 c = a + b;
  printf("string = %Df\n", a);      
  printf("%x\n", *((unsigned int *) &b));      
  printf("%x\n", *((unsigned int *) &c));      
                       
  return 0;
}