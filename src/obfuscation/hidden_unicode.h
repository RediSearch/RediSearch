/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include "redismodule.h"
#include "hiredis/sds.h"

// Unicode Strings support

typedef struct HiddenUnicodeString HiddenUnicodeString;

// name must have been created using sdsnew, takes ownership by default
HiddenUnicodeString *NewHiddenUnicodeString(const char *name);
void HiddenUnicodeString_Free(const HiddenUnicodeString *value);
int HiddenUnicodeString_Compare(const HiddenUnicodeString *left, const HiddenUnicodeString *right);
int HiddenUnicodeString_CompareC(const HiddenUnicodeString *left, sds right);
sds HiddenUnicodeString_GetUnsafe(const HiddenUnicodeString *value, size_t *length);
// Creates a redis module string from a hidden string
RedisModuleString *HiddenUnicodeString_CreateRedisModuleString(HiddenUnicodeString* value, RedisModuleCtx* ctx);
void HiddenUnicodeString_SaveToRdb(HiddenUnicodeString* value, RedisModuleIO* rdb);