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
HiddenString *HideAndObfuscateString(char *str, uint64_t length);
// Hides the size and obfustaces it
HiddenSize *HideAndObfuscateNumber(uint64_t num);
// Hides the string, obfuscation is done elsewhere
HiddenName *NewHiddenName(const char *name, uint64_t length);

void HiddenString_Free(HiddenString *value);
void HiddenSize_Free(HiddenSize *value);
void HiddenName_Free(HiddenName *value);

const char* HiddenString_Get(HiddenString *value, bool obfuscate);
bool HiddenString_Equal(HiddenString *left, HiddenString *right);

int HiddenName_Compare(HiddenName *left, HiddenName *right);
void HiddenName_Clone(HiddenName *src, HiddenName **dst);
void HiddenName_SendInReplyAsString(HiddenName* value, RedisModule_Reply* reply);
void HiddenName_SendInReplyAsKeyValue(HiddenName* value, const char *key, RedisModule_Reply* reply);

void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);
void HiddenName_SendInReplyAsString(HiddenName* value, RedisModule_Reply* reply);
void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value);
// Temporary for the sake of comparmentilization
const char* HiddenName_GetUnsafe(HiddenName* value, size_t* length);

RedisModuleString *HiddenString_CreateString(HiddenString* value, RedisModuleCtx* ctx);
RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx);


#endif //HIDDEN_H
