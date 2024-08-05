/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../src/dep/crc16.h"

const char *alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

void bf_recursive(char *str, int len, int pos, int numSlots, char **keys, int *count) {
  size_t absize = strlen(alphabet);
  if (pos < len) {
    for (int i = 0; i < absize; i++) {
      str[pos] = alphabet[i];
      str[pos + 1] = '\0';

      uint16_t hash = crc16(str, pos + 1) % numSlots;
      if (keys[hash] == NULL) {
        keys[hash] = rm_strndup(str, pos + 1);
        ++*count;
        // printf("Found new hash: %s, count now %d\n", keys[hash], *count);
      } else if (strlen(keys[hash]) > pos + 1) {
        // printf("Replaces hash: %s => %s, count now %d\n", keys[hash], str, *count);
        rm_free(keys[hash]);
        keys[hash] = rm_strndup(str, pos + 1);
      }

      bf_recursive(str, len, pos + 1, numSlots, keys, count);
    }
  }
}
void bruteforce_crc16(int numSlots) {
  char *keys[numSlots];
  memset(keys, 0, numSlots * sizeof(char *));
  char str[4];
  int count = 0;
  bf_recursive(str, 4, 0, numSlots, keys, &count);

  printf("const char *table[] = {\n");
  for (int i = 0; i < numSlots; i++) {
    printf("\"%s\", ", keys[i]);
    if (i % 20 == 19) printf("\n");
  }
  printf("};\n");
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  bruteforce_crc16(16384);
}