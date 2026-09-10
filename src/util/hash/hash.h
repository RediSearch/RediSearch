/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  // SHA-1 produces a 160-bit hash
  unsigned char hash[20];
} Sha1;

#define SHA1_TEXT_MAX_LENGTH 40

// Computes the sha1 hash for the given buffer
void Sha1_Compute(const char *value, size_t len, Sha1* output);
// Prints to buffer the hash, the buffer's length is assumed to be at least SHA1_TEXT_MAX_LENGTH + 1
void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer);

// The leading 8 bytes of the hash as a 64-bit value, most significant byte first. Built byte
// by byte rather than memcpy'd, so it reads the same on either endianness and can therefore
// be compared across hosts.
static inline uint64_t Sha1_LeadingU64(const Sha1 *sha1) {
  uint64_t out = 0;
  for (size_t i = 0; i < sizeof(out); i++) {
    out = (out << 8) | (uint64_t)sha1->hash[i];
  }
  return out;
}

#ifdef __cplusplus
}
#endif
