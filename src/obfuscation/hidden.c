#include "hidden.h"
#include "rmalloc.h"
#include "obfuscation_api.h"
#include "util/minmax.h"
#include "redis_index.h"
#include "query_node.h"
#include "reply_macros.h"

typedef struct {
  char* user;
  uint64_t length;
  char* obfuscated;
} UserAndObfuscatedString;

typedef struct {
  uint64_t user;
  uint64_t obfuscated;
} UserAndObfuscatedUInt64;

typedef struct {
  char *user;
  uint64_t length;
} UserString;

HiddenString* HideAndObfuscateString(const char* str, uint64_t length) {
  UserAndObfuscatedString* value = rm_malloc(sizeof(*value));
  value->user = rm_strdup(str);
  value->length = length;
  value->obfuscated = Obfuscate_Text(str);
  return (HiddenString*)value;
}

HiddenSize* HideAndObfuscateNumber(uint64_t num) {
  UserAndObfuscatedUInt64* value = rm_malloc(sizeof(*value));
  value->user = num;
  value->obfuscated = 0;
  return (HiddenSize*)value;
};

HiddenName* NewHiddenName(const char* name, uint64_t length) {
  UserString* value = rm_malloc(sizeof(*value));
  value->user = rm_strndup(name, length);
  value->length = length;
  return (HiddenName*)value;
};

void HiddenString_Free(HiddenString* hs) {
  UserAndObfuscatedString* value = (UserAndObfuscatedString*)hs;
  rm_free(value->user);
  rm_free(value);
};

void HiddenSize_Free(HiddenSize* hn) {
  UserAndObfuscatedUInt64* value = (UserAndObfuscatedUInt64*)hn;
  rm_free(value);
};

void HiddenName_Free(HiddenName* hn) {
  UserString* value = (UserString*)hn;
  rm_free(value->user);
  rm_free(value);
};

HiddenString *HiddenString_Clone(const HiddenString* value) {
    UserAndObfuscatedString* v = (UserAndObfuscatedString*)value;
    return HideAndObfuscateString(v->user, v->length);
}

const char *HiddenString_Get(const HiddenString *value, bool obfuscate) {
  UserAndObfuscatedString* v = (UserAndObfuscatedString*)value;
  return obfuscate ? v->obfuscated : v->user;
}

bool HiddenString_Equal(HiddenString *left, HiddenString *right) {
  UserAndObfuscatedString* l = (UserAndObfuscatedString*)left;
  UserAndObfuscatedString* r = (UserAndObfuscatedString*)right;
  return l->length == r->length && strncmp(l->obfuscated, r->obfuscated, l->length) == 0;
}

bool HiddenString_EqualC(HiddenString *left, const char *right) {
  UserAndObfuscatedString* l = (UserAndObfuscatedString*)left;
  return l->length == strlen(right) && strncmp(l->obfuscated, right, l->length) == 0;
}

void HiddenString_SaveToRdb(HiddenName* value, RedisModuleIO* rdb) {
  UserString* text = (UserString*)value;
  RedisModule_SaveStringBuffer(rdb, text->user, text->length + 1);
}

int HiddenName_CompareC(const HiddenName *left, const char *right, size_t right_length) {
  const UserString* l = (const UserString*)left;
  int result = strncmp(l->user, right, MIN(l->length, right_length));
  if (result != 0 || l->length == right_length) {
    return result;
  } else {
    return l->length < right_length ? -1 : 1;
  }
}

int HiddenName_Compare(const HiddenName* left, const HiddenName* right) {
  UserString* r = (UserString*)right;
  HiddenName_CompareC(left, r->user, r->length);
}

void HiddenName_Clone(HiddenName* src, HiddenName** dst) {
  UserString* s = (UserString*)src;
  if (*dst == NULL) {
    *dst = NewHiddenName(s->user, s->length);
  } else {
    UserString* d = (UserString*)*dst;
    if (s->length > d->length) {
      d->user = rm_realloc(d->user, s->length);
      d->length = s->length;
    }
    strncpy(d->user, s->user, s->length);
    if (d->length > s->length) {
      memset(d->user + s->length, 0, d->length - s->length);
    }
  }
}

void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb) {
  UserString* text = (UserString*)value;
  RedisModule_SaveStringBuffer(rdb, text->user, text->length + 1);
}

void HiddenName_SendInReplyAsString(HiddenName* value, RedisModule_Reply* reply) {
  UserString* text = (UserString*)value;
  if (isUnsafeForSimpleString(text->user)) {
    char *escaped = escapeSimpleString(text->user);
    RedisModule_Reply_SimpleString(reply, escaped);
    rm_free(escaped);
  } else {
    RedisModule_Reply_SimpleString(reply, text->user);
  }
}

void HiddenName_SendInReplyAsKeyValue(HiddenName* value, const char *key, RedisModule_Reply* reply) {
  UserString* text = (UserString*)value;
  REPLY_KVSTR_SAFE(key, text->user);
}

void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value) {
  UserString* text = (UserString*)value;
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(redisCtx, fmt, text->user);
  Redis_DeleteKey(redisCtx, str);
  RedisModule_FreeString(redisCtx, str);
}

const char* HiddenName_GetUnsafe(const HiddenName* value, size_t* length) {
  const UserString* text = (const UserString*)value;
  if (length != NULL) {
    *length = text->length;
  }
  return text->user;
}

RedisModuleString *HiddenString_CreateString(HiddenString* value, RedisModuleCtx* ctx) {
  UserAndObfuscatedString* text = (UserAndObfuscatedString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}

RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx) {
  UserString* text = (UserString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}