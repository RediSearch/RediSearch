/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef UTIL_CHARBUF_H__
#define UTIL_CHARBUF_H__

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// A pointer and length; the bytes need not be NUL-terminated.
typedef struct CharBuf {
  char *buf;
  size_t len;
} CharBuf;

// dictType callbacks for CharBuf keys. KeyDup copies the bytes; KeyDestructor frees such a copy.
uint64_t CharBuf_HashFunction(const void *key);
int CharBuf_KeyCompare(void *privdata, const void *key1, const void *key2);
void *CharBuf_KeyDup(void *privdata, const void *key);
void CharBuf_KeyDestructor(void *privdata, void *key);

#ifdef __cplusplus
}
#endif

#endif  // UTIL_CHARBUF_H__
