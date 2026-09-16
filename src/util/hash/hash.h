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
  // SHA-1 produces a 160-bit hash, i.e., 5 32-bit words
  uint32_t hash[5];
} Sha1;

#define SHA1_TEXT_MAX_LENGTH 40

// Computes the sha1 hash for the given buffer
void Sha1_Compute(const char *value, size_t len, Sha1* output);
// The context is stack-owned by Sha1_ComputeValue and valid only during visit.
typedef struct Sha1Context Sha1Context;
uint64_t Sha1_ComputeValue(void (*visit)(Sha1Context *, const void *), const void *value);
void Sha1_UpdateU64(Sha1Context *ctx, uint64_t value);
void Sha1_UpdateDouble(Sha1Context *ctx, double value);
// Length-prefixed bytes; NULL is allowed only for an empty buffer.
void Sha1_UpdateBuffer(Sha1Context *ctx, const char *value, size_t len);
// Optional C string: distinguish NULL from empty and consume through its terminator.
void Sha1_UpdateCString(Sha1Context *ctx, const char *value);

// Prints to buffer the hash, the buffer's length is assumed to be at least SHA1_TEXT_MAX_LENGTH + 1
void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer);

// The leading two SHA-1 words as a host-independent 64-bit value.
static inline uint64_t Sha1_LeadingU64(const Sha1 *sha1) {
  return ((uint64_t)sha1->hash[0] << 32) | sha1->hash[1];
}

#ifdef __cplusplus
}
#endif
