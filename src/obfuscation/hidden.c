#include "hidden.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "redis_index.h"
#include "query_node.h"
#include "reply_macros.h"
#include "obfuscation/obfuscation_api.h"

typedef struct {
  const char *buffer;
  uint32_t length;
  uint16_t refcount;
  bool owner;
} HiddenNameImpl;

HiddenString *NewHiddenString(const char* name, size_t length, bool takeOwnership) {
  HiddenNameImpl* value = rm_malloc(sizeof(*value));
  if (takeOwnership) {
    value->buffer = rm_strndup(name, length);
  } else {
    value->buffer = name;
  }
  value->length = length;
  value->owner = takeOwnership;
  value->refcount = 1;
  return (HiddenString*)value;
};

void HiddenString_Free(const HiddenString* hn) {
  HiddenNameImpl* value = (HiddenNameImpl*)hn;
  if (--value->refcount == 0) {
    if (value->owner) {
      rm_free((void*)value->buffer);
    }
    rm_free(value);
  }
};

static inline int Compare(const char *left, size_t left_length, const char *right, size_t right_length) {
  int result = strncmp(left, right, MIN(left_length, right_length));
  if (result != 0 || left_length == right_length) {
    return result;
  } else {
    return (int)(left_length - right_length);
  }
}

static inline int CaseSensitiveCompare(const char *left, size_t left_length, const char *right, size_t right_length) {
  int result = strncasecmp(left, right, MIN(left_length, right_length));
  if (result != 0 || left_length == right_length) {
    return result;
  } else {
    return (int)(left_length - right_length);
  }
}

int HiddenString_CompareC(const HiddenString *left, const char *right, size_t right_length) {
  const HiddenNameImpl* l = (const HiddenNameImpl*)left;
  return Compare(l->buffer, l->length, right, right_length);
}

int HiddenString_Compare(const HiddenString* left, const HiddenString* right) {
  HiddenNameImpl* r = (HiddenNameImpl*)right;
  return HiddenString_CompareC(left, r->buffer, r->length);
}

int HiddenString_CaseInsensitiveCompare(const HiddenString *left, const HiddenString *right) {
  HiddenNameImpl* r = (HiddenNameImpl*)right;
  return HiddenString_CaseInsensitiveCompareC(left, r->buffer, r->length);
}

int HiddenString_CaseInsensitiveCompareC(const HiddenString *left, const char *right, size_t right_length) {
  HiddenNameImpl* l = (HiddenNameImpl*)left;
  return CaseSensitiveCompare(l->buffer, l->length, right, right_length);
}

HiddenString *HiddenName_Retain(HiddenString *value) {
  HiddenNameImpl* text = (HiddenNameImpl*)value;
  text->refcount++;
  return value;
}

void HiddenString_TakeOwnership(HiddenString *hidden) {
  HiddenNameImpl* impl = (HiddenNameImpl*)hidden;
  if (impl->owner) {
    return;
  }
  impl->buffer = rm_strndup(impl->buffer, impl->length);
  impl->owner = true;
}

void HiddenString_SaveToRdb(const HiddenString* value, RedisModuleIO* rdb) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  RedisModule_SaveStringBuffer(rdb, impl->buffer, impl->length + 1);
}

void HiddenString_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, const HiddenString* value) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(redisCtx, fmt, impl->buffer);
  Redis_DeleteKey(redisCtx, str);
  RedisModule_FreeString(redisCtx, str);
}

const char *HiddenString_GetUnsafe(const HiddenString* value, size_t* length) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  if (length != NULL) {
    *length = impl->length;
  }
  return impl->buffer;
}

RedisModuleString *HiddenString_CreateRedisModuleString(const HiddenString* value, RedisModuleCtx* ctx) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  return RedisModule_CreateString(ctx, impl->buffer, impl->length);
}

const char* FormatHiddenText(HiddenName *name, bool obfuscate) {
  return obfuscate ? Obfuscate_Text() : HiddenName_GetUnsafe(name, NULL);
}
