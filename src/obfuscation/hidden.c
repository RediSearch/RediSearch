#include "hidden.h"
#include "rmalloc.h"
#include "obfuscation_api.h"
#include "util/minmax.h"
#include "redis_index.h"
#include "query_node.h"
#include "reply_macros.h"

typedef struct {
  const char* user;
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
  value->user = str;
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

int HiddenName_Compare(HiddenName* left, HiddenName* right) {
  UserString* l = (UserString*)left;
  UserString* r = (UserString*)right;
  int result = strncmp(l->user, r->user, MIN(l->length, r->length));
  if (result != 0 || l->length == r->length) {
    return result;
  } else {
    return l->length < r->length ? -1 : 1;
  }
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
