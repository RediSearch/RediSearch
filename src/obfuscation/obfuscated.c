#include "obfuscated.h"
#include "rmalloc.h"

typedef struct {
  const char* user;
  uint64_t length;
  char* obfuscated;
} UserAndObfuscatedString;

typedef struct {
  uint64_t user;
  uint64_t obfuscated;
} UserAndObfuscatedUInt64;

ObfuscatedString* ObfuscateString(const char* str, uint64_t length, bool takeOwnership) {
  UserAndObfuscatedString* value = rm_malloc(sizeof(*value));
  value->user = str;
  if (takeOwnership) {
    value->user = rm_strndup(str, length);
  }
  value->length = length;
  value->obfuscated = Obfuscate_Text(str);
  return (ObfuscatedString*)value;
}

ObfuscatedSize* ObfuscateNumber(uint64_t num) {
  UserAndObfuscatedUInt64* value = rm_malloc(sizeof(*value));
  value->user = num;
  value->obfuscated = 0;
  return (ObfuscatedSize*)value;
};

void ObfuscatedString_Free(ObfuscatedString* hs, bool tookOwnership) {
  UserAndObfuscatedString* value = (UserAndObfuscatedString*)hs;
  if (tookOwnership) {
    rm_free((void*)value->user);
  }
  rm_free(value);
};

void ObfuscatedSize_Free(ObfuscatedSize* hn) {
  UserAndObfuscatedUInt64* value = (UserAndObfuscatedUInt64*)hn;
  rm_free(value);
};

ObfuscatedString *ObfuscatedString_Clone(const ObfuscatedString* value) {
  UserAndObfuscatedString* v = (UserAndObfuscatedString*)value;
  return ObfuscateString(v->user, v->length, true);
}

const char *ObfuscatedString_Get(const ObfuscatedString *value, bool obfuscate) {
  UserAndObfuscatedString* v = (UserAndObfuscatedString*)value;
  return obfuscate ? v->obfuscated : v->user;
}

RedisModuleString *ObfuscatedString_CreateString(ObfuscatedString* value, RedisModuleCtx* ctx) {
  UserAndObfuscatedString* text = (UserAndObfuscatedString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}