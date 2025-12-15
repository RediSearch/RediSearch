/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include "redismodule.h"
#include "rmutil/sds.h"

#ifdef __cplusplus
extern "C" {
#endif


// Unicode Strings support
// opaque struct for hidden unicode strings
typedef struct HiddenUnicodeString HiddenUnicodeString;

// Creates a new hidden unicode string from a sds string
// name must have been created using sdsnew, takes ownership by default
HiddenUnicodeString *NewHiddenUnicodeString(const char *name);
// Freeds a hidden unicode string
void HiddenUnicodeString_Free(const HiddenUnicodeString *value);
// Compares two hidden unicode strings
int HiddenUnicodeString_Compare(const HiddenUnicodeString *left, const HiddenUnicodeString *right);
// Compares a hidden unicode string with an sds string
// returns 0 if equal, -1 if left < right, 1 if left > right
int HiddenUnicodeString_CompareC(const HiddenUnicodeString *left, sds right);
// Returns the length of the hidden unicode string and a pointer to the data
sds HiddenUnicodeString_GetUnsafe(const HiddenUnicodeString *value, size_t *length);
// Creates a redis module string from a hidden string
RedisModuleString *HiddenUnicodeString_CreateRedisModuleString(const HiddenUnicodeString* value, RedisModuleCtx* ctx);
// Saves a hidden unicode string to an RDB file
void HiddenUnicodeString_SaveToRdb(const HiddenUnicodeString* value, RedisModuleIO* rdb);

#ifdef __cplusplus
}
#endif
