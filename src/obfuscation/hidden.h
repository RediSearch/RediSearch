/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#ifndef HIDDEN_H
#define HIDDEN_H
#include <stdint.h>
#include "reply.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct HiddenString HiddenString;

// Hides the string, obfuscation is done elsewhere
// Should discourage directly accessing the string and printing out user data
// This is a security measure to prevent leaking user data
// The additional takeOwnership determines whether to duplicate the buffer or directly point at the given buffer
// HiddenString_Free must be called for the object to release it
HiddenString *NewHiddenString(const char *name, size_t length, bool takeOwnership);
// Frees a hidden string, if takeOwnership is true, the buffer is freed as well
void HiddenString_Free(const HiddenString *value, bool tookOwnership);

// Comparison functions
// CompareC overloads receive a const char* right argument for the comparison for backward compatibility with existing code
// Eventually the hope is to remove them altogether.
int HiddenString_Compare(const HiddenString *left, const HiddenString *right);
int HiddenString_CompareC(const HiddenString *left, const char *right, size_t right_length);
int HiddenString_CaseInsensitiveCompare(const HiddenString *left, const HiddenString *right);
int HiddenString_CaseInsensitiveCompareC(const HiddenString *left, const char *right, size_t right_length);

// ownership managment
HiddenString *HiddenString_Duplicate(const HiddenString *value);
void HiddenString_TakeOwnership(HiddenString *hidden);
void HiddenString_Clone(const HiddenString *src, HiddenString **dst);

// Allowed actions
// Save a hidden string to an RDB file, e.g an index name
void HiddenString_SaveToRdb(const HiddenString* value, RedisModuleIO* rdb);
// Remove a key from the keyspace using the hidden string, e.g an index name that
// Used in legacy code, should be avoided in new code
void HiddenString_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, const HiddenString* value);
// Creates a redis module string from a hidden string
RedisModuleString *HiddenString_CreateRedisModuleString(const HiddenString* value, RedisModuleCtx* ctx);

// Direct access to user data, should be used only when necessary
// Avoid outputing user data to:
// 1. Logs
// 2. Metrics
// 3. Command responses
const char *HiddenString_GetUnsafe(const HiddenString* value, size_t* length);

#ifdef __cplusplus
}
#endif

#endif //HIDDEN_H
