#include "stdio.h"
#include "base64.h"

int main() {
  size_t outLen, outLen64;
  unsigned char ch[5];
  ch[4] = '\0';

  for (int i = 0; i < (1 << 8); i++) {
    ch[0] = ch[1] = ch[2] = ch[3] = i;
    unsigned char *vector64 = base64_encode(ch, 4, &outLen64);
    unsigned char *vector = base64_decode(vector64, outLen64, &outLen);    
    printf("original %s len 4 vector64 %s len %ld vector %s len %ld\n", ch, vector64, outLen64, vector, outLen);
    base64_free(vector64);
    base64_free(vector);
  }

  return 0;
}