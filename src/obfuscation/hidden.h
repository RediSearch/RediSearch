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

typedef struct HiddenName HiddenName;

// Hides the string, obfuscation is done elsewhere
// Should discourage directly accessing the string and printing out user data
HiddenName *NewHiddenName(const char *name, uint64_t length, bool takeOwnership);
void HiddenName_Free(HiddenName *value, bool tookOwnership);

// comparison
int HiddenName_Compare(const HiddenName *left, const HiddenName *right);
int HiddenName_CompareC(const HiddenName *left, const char *right, size_t right_length);
int HiddenName_CaseInsensitiveCompare(HiddenName *left, HiddenName *right);
int HiddenName_CaseInsensitiveCompareC(HiddenName *left, const char *right, size_t right_length);

// ownership managment
HiddenName *HiddenName_Duplicate(const HiddenName *value);
void HiddenName_TakeOwnership(HiddenName *hidden);
void HiddenName_Clone(HiddenName *src, HiddenName **dst);

// allowed actions
void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);
void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value);
RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx);

// Direct access to user data, should be used only when necessary
// Avoid outputting user data to:
// 1. Logs
// 2. Metrics
// 3. Command responses
const char *HiddenName_GetUnsafe(const HiddenName* value, size_t* length);

#ifdef __cplusplus
}
#endif

#endif //HIDDEN_H
