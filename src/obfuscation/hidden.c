#include "hidden.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "redis_index.h"
#include "query_node.h"
#include "reply_macros.h"

typedef struct {
  const char *user;
  uint32_t length;
  uint16_t refcount;
  bool owner;
} HiddenNameImpl;



HiddenString *NewHiddenString(const char* name, size_t length, bool takeOwnership) {
  HiddenNameImpl* value = rm_malloc(sizeof(*value));
  if (takeOwnership) {
    value->user = rm_strndup(name, length);
  } else {
    value->user = name;
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
      rm_free((void*)value->user);
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
  return Compare(l->user, l->length, right, right_length);
}

int HiddenString_Compare(const HiddenString* left, const HiddenString* right) {
  HiddenNameImpl* r = (HiddenNameImpl*)right;
  return HiddenString_CompareC(left, r->user, r->length);
}

int HiddenName_CompareEx(const HiddenName *left, const LooseHiddenName *right) {
  const HiddenNameImpl* r = (const HiddenNameImpl*)right;
  return HiddenName_CompareC(left, r->user, r->length);
}

int HiddenString_CaseInsensitiveCompare(const HiddenString *left, const HiddenString *right) {
  HiddenNameImpl* r = (HiddenNameImpl*)right;
  return HiddenString_CaseInsensitiveCompareC(left, r->user, r->length);
}

int HiddenString_CaseInsensitiveCompareC(const HiddenString *left, const char *right, size_t right_length) {
  HiddenNameImpl* l = (HiddenNameImpl*)left;
  return CaseSensitiveCompare(l->user, l->length, right, right_length);
}

HiddenString *HiddenString_Retain(HiddenString *value) {
  HiddenString_TakeOwnership(value);
  HiddenNameImpl* text = (HiddenNameImpl*)value;
  text->refcount++;
  return (HiddenString*)text;
}

void HiddenString_TakeOwnership(HiddenString *hidden) {
  HiddenNameImpl* impl = (HiddenNameImpl*)hidden;
  if (impl->owner) {
    return;
  }
  impl->user = rm_strndup(impl->user, impl->length);
  impl->owner = true;
}

void HiddenString_SaveToRdb(const HiddenString* value, RedisModuleIO* rdb) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  RedisModule_SaveStringBuffer(rdb, impl->user, impl->length + 1);
}

void HiddenString_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, const HiddenString* value) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(redisCtx, fmt, impl->user);
  Redis_DeleteKey(redisCtx, str);
  RedisModule_FreeString(redisCtx, str);
}

const char *HiddenString_GetUnsafe(const HiddenString* value, size_t* length) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  if (length != NULL) {
    *length = impl->length;
  }
  return impl->user;
}

RedisModuleString *HiddenString_CreateRedisModuleString(const HiddenString* value, RedisModuleCtx* ctx) {
  const HiddenNameImpl* impl = (const HiddenNameImpl*)value;
  return RedisModule_CreateString(ctx, impl->user, impl->length);
}
