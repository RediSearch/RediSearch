/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef HASH_H
#define HASH_H
#include <stdint.h>
#include "obfuscation/hidden.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  // SHA-1 produces a 160-bit hash, i.e., 5 32-bit words
  uint32_t hash[5];
} Sha1;

#define SHA1_TEXT_MAX_LENGTH 40

void Sha1_Compute(Sha1* output, const char *value, size_t len);
char* Sha1_Format(const Sha1* sha1);
void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer);

inline char* Sha1_InlineFormat(const char *value, size_t len) {
  Sha1 sha1;
  Sha1_Compute(&sha1, value, len);
  return Sha1_Format(&sha1);
}

#ifdef __cplusplus
}
#endif

#endif //HASH_H