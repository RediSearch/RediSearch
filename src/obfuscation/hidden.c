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
  Ownership owner;
} HiddenStringImpl;

HiddenString *NewHiddenStringEx(const char* name, size_t length, Ownership mode) {
  HiddenStringImpl* value = rm_malloc(sizeof(*value));
  if (mode == Take) {
    value->buffer = rm_strndup(name, length);
  } else {
    value->buffer = name;
    bool same_length = length == strlen(name);
    RS_LOG_ASSERT(same_length, "Length mismatch");
  }
  value->length = length;
  value->owner = mode;
  value->refcount = 1;
  return (HiddenString*)value;
};

HiddenString *NewHiddenString(const char *name, size_t length, bool takeOwnership) {
  return NewHiddenStringEx(name, length, takeOwnership ? Take : Borrow);
}

void HiddenString_Free(const HiddenString* hn) {
  HiddenStringImpl* value = (HiddenStringImpl*)hn;
  RS_LOG_ASSERT(value->refcount > 0, "Freeing a string with refcount 0");
  if (--value->refcount == 0) {
    if (value->owner != Borrow) {
      rm_free((void*)value->buffer);
    }
    rm_free(value);
  }
};

bool HiddenString_IsEmpty(const HiddenString *value) {
  const HiddenStringImpl* impl = (const HiddenStringImpl*)value;
  return impl->length == 0;
}

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
  const HiddenStringImpl* l = (const HiddenStringImpl*)left;
  return Compare(l->buffer, l->length, right, right_length);
}

int HiddenString_Compare(const HiddenString* left, const HiddenString* right) {
  HiddenStringImpl* r = (HiddenStringImpl*)right;
  return HiddenString_CompareC(left, r->buffer, r->length);
}

int HiddenString_CaseInsensitiveCompare(const HiddenString *left, const HiddenString *right) {
  HiddenStringImpl* r = (HiddenStringImpl*)right;
  return HiddenString_CaseInsensitiveCompareC(left, r->buffer, r->length);
}

int HiddenString_CaseInsensitiveCompareC(const HiddenString *left, const char *right, size_t right_length) {
  HiddenStringImpl* l = (HiddenStringImpl*)left;
  return CaseSensitiveCompare(l->buffer, l->length, right, right_length);
}

HiddenString *HiddenString_Retain(HiddenString *value) {
  HiddenStringImpl* text = (HiddenStringImpl*)value;
  text->refcount++;
  return value;
}

void HiddenString_TakeOwnership(HiddenString *hidden) {
  HiddenStringImpl* impl = (HiddenStringImpl*)hidden;
  if (impl->owner != Borrow) {
    return;
  }
  impl->buffer = rm_strndup(impl->buffer, impl->length);
  impl->owner = Take;
}

void HiddenString_SaveToRdb(const HiddenString* value, RedisModuleIO* rdb) {
  const HiddenStringImpl* impl = (const HiddenStringImpl*)value;
  RedisModule_SaveStringBuffer(rdb, impl->buffer, impl->length + 1);
}

void HiddenString_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, const HiddenString* value) {
  const HiddenStringImpl* impl = (const HiddenStringImpl*)value;
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(redisCtx, fmt, impl->buffer);
  Redis_DeleteKey(redisCtx, str);
  RedisModule_FreeString(redisCtx, str);
}

const char *HiddenString_GetUnsafe(const HiddenString* value, size_t* length) {
  const HiddenStringImpl* impl = (const HiddenStringImpl*)value;
  if (length != NULL) {
    *length = impl->length;
  }
  return impl->buffer;
}

RedisModuleString *HiddenString_CreateRedisModuleString(const HiddenString* value, RedisModuleCtx* ctx) {
  const HiddenStringImpl* impl = (const HiddenStringImpl*)value;
  return RedisModule_CreateString(ctx, impl->buffer, impl->length);
}
