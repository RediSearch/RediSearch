/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
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

// Computes the sha1 hash for the given buffer
void Sha1_Compute(const char *value, size_t len, Sha1* output);
// Prints to buffer the hash, the buffer's length is assumed to be at least SHA1_TEXT_MAX_LENGTH + 1
void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer);

#ifdef __cplusplus
}
#endif
