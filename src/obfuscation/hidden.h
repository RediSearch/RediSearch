//
// Created by jonathan on 9/18/24.
//

#ifndef HIDDEN_H
#define HIDDEN_H
#include <stdint.h>
#include "reply.h"

struct Hidden;
typedef struct Hidden HiddenString;
typedef struct Hidden HiddenSize;
typedef struct Hidden HiddenName;

// Hides the string and obfustaces it
HiddenString *HideAndObfuscateString(const char *str, uint64_t length);
// Hides the size and obfustaces it
HiddenSize *HideAndObfuscateNumber(uint64_t num);
// Hides the string, obfuscation is done elsewhere
HiddenName *NewHiddenName(const char *name, uint64_t length);

void HiddenString_Free(HiddenString *value);
void HiddenSize_Free(HiddenSize *value);
void HiddenName_Free(HiddenName *value);

HiddenString *HiddenString_Clone(const HiddenString* value);
const char *HiddenString_Get(const HiddenString *value, bool obfuscate);
int HiddenString_CompareC(HiddenString *left, const char *right, size_t right_length);
int HiddenString_Compare(HiddenString *left, HiddenString *right);
int HiddenString_CaseSensitiveCompareC(HiddenString *left, const char *right, size_t right_length);
int HiddenString_CaseSensitiveCompare(HiddenString *left, HiddenString *right);
void HiddenString_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);

int HiddenName_Compare(const HiddenName *left, const HiddenName *right);
int HiddenName_CompareC(const HiddenName *left, const char *right, size_t right_length);
int HiddenName_CaseSensitiveCompareC(HiddenName *left, const char *right, size_t right_length);
int HiddenName_CaseSensitiveCompare(HiddenName *left, HiddenName *right);
void HiddenName_Clone(HiddenName *src, HiddenName **dst);
void HiddenName_SendInReplyAsString(HiddenName* value, RedisModule_Reply* reply);
void HiddenName_SendInReplyAsKeyValue(HiddenName* value, const char *key, RedisModule_Reply* reply);

void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);
void HiddenName_SendInReplyAsString(HiddenName* value, RedisModule_Reply* reply);
void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value);
// Temporary for the sake of comparmentilization
const char* HiddenName_GetUnsafe(const HiddenName* value, size_t* length);

RedisModuleString *HiddenString_CreateString(HiddenString* value, RedisModuleCtx* ctx);
RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx);


#endif //HIDDEN_H
