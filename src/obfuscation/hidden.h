//
// Created by jonathan on 9/18/24.
//

#ifndef HIDDEN_H
#define HIDDEN_H
#include <stdint.h>
#include "reply.h"

struct HiddenStringStruct;
struct HiddenSizeStruct;
struct HiddenNameStruct;
typedef struct HiddenStringStruct HiddenString;
typedef struct HiddenSizeStruct HiddenSize;
typedef struct HiddenNameStruct HiddenName;

// Hides the string and obfuscates it
HiddenString *HideAndObfuscateString(const char *str, uint64_t length, bool takeOwnership);
// Hides the size and obfuscates it
HiddenSize *HideAndObfuscateNumber(uint64_t num);
// Hides the string, obfuscation is done elsewhere
HiddenName *NewHiddenName(const char *name, uint64_t length, bool takeOwnership);

void HiddenString_Free(HiddenString *value, bool tookOwnership);
void HiddenSize_Free(HiddenSize *value);
void HiddenName_Free(HiddenName *value, bool tookOwnership);

HiddenString *HiddenString_Clone(const HiddenString* value);
const char *HiddenString_Get(const HiddenString *value, bool obfuscate);
int HiddenString_CompareC(HiddenString *left, const char *right, size_t right_length);
int HiddenString_Compare(HiddenString *left, HiddenString *right);
int HiddenString_CaseInsensitiveCompareC(HiddenString *left, const char *right, size_t right_length);
int HiddenString_CaseInsensitiveCompare(HiddenString *left, HiddenString *right);
void HiddenString_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);

int HiddenName_Compare(const HiddenName *left, const HiddenName *right);
int HiddenName_CompareC(const HiddenName *left, const char *right, size_t right_length);
int HiddenName_CaseInsensitiveCompareC(HiddenName *left, const char *right, size_t right_length);
int HiddenName_CaseInsensitiveCompare(HiddenName *left, HiddenName *right);
HiddenName *HiddenName_Duplicate(const HiddenName *value);
void HiddenName_TakeOwnership(HiddenName *hidden);
void HiddenName_Clone(HiddenName *src, HiddenName **dst);

void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);
void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value);
// Temporary for the sake of comparmentilization
const char *HiddenName_GetUnsafe(const HiddenName* value, size_t* length);

RedisModuleString *HiddenString_CreateString(HiddenString* value, RedisModuleCtx* ctx);
RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx);


#endif //HIDDEN_H
