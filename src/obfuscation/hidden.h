/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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
HiddenString *NewHiddenString(const char *name, uint64_t length, bool takeOwnership);
void HiddenString_Free(HiddenString *value, bool tookOwnership);

// comparison
int HiddenString_Compare(const HiddenString *left, const HiddenString *right);
int HiddenString_CompareC(const HiddenString *left, const char *right, size_t right_length);
int HiddenString_CaseInsensitiveCompare(HiddenString *left, HiddenString *right);
int HiddenString_CaseInsensitiveCompareC(HiddenString *left, const char *right, size_t right_length);

// ownership managment
HiddenString *HiddenString_Duplicate(const HiddenString *value);
void HiddenString_TakeOwnership(HiddenString *hidden);
void HiddenString_Clone(HiddenString *src, HiddenString **dst);

// allowed actions
void HiddenString_SaveToRdb(HiddenString* value, RedisModuleIO* rdb);
void HiddenString_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenString* value);
RedisModuleString *HiddenString_CreateString(HiddenString* value, RedisModuleCtx* ctx);

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
