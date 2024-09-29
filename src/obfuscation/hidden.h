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

int HiddenName_Compare(HiddenName *left, HiddenName *right);
void HiddenName_Clone(HiddenName *src, HiddenName **dst);
void HiddenName_SendInReply(HiddenName* value, RedisModule_Reply* reply);

void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb);
void HiddenName_SendInReplyAsString(HiddenName* value, RedisModule_Reply* reply);
void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value);

#endif //HIDDEN_H
