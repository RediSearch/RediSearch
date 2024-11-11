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
} UserString;

HiddenString *NewHiddenString(const char* name, size_t length, bool takeOwnership) {
  UserString* value = rm_malloc(sizeof(*value));
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
  UserString* value = (UserString*)hn;
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
  const UserString* l = (const UserString*)left;
  return Compare(l->user, l->length, right, right_length);
}

int HiddenString_Compare(const HiddenString* left, const HiddenString* right) {
  UserString* r = (UserString*)right;
  return HiddenString_CompareC(left, r->user, r->length);
}

int HiddenName_CompareEx(const HiddenName *left, const LooseHiddenName *right) {
  const UserString* r = (const UserString*)right;
  return HiddenName_CompareC(left, r->user, r->length);
}

int HiddenString_CaseInsensitiveCompare(const HiddenString *left, const HiddenString *right) {
  UserString* r = (UserString*)right;
  return HiddenString_CaseInsensitiveCompareC(left, r->user, r->length);
}

int HiddenString_CaseInsensitiveCompareC(const HiddenString *left, const char *right, size_t right_length) {
  UserString* l = (UserString*)left;
  return CaseSensitiveCompare(l->user, l->length, right, right_length);
}

HiddenString *HiddenString_Retain(HiddenString *value) {
  HiddenString_TakeOwnership(value);
  UserString* text = (UserString*)value;
  text->refcount++;
  return (HiddenString*)text;
}

void HiddenString_TakeOwnership(HiddenString *hidden) {
  UserString* userString = (UserString*)hidden;
  if (userString->owner) {
    return;
  }
  userString->user = rm_strndup(userString->user, userString->length);
  userString->owner = true;
}

void HiddenString_SaveToRdb(const HiddenString* value, RedisModuleIO* rdb) {
  const UserString* text = (const UserString*)value;
  RedisModule_SaveStringBuffer(rdb, text->user, text->length + 1);
}

void HiddenString_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, const HiddenString* value) {
  const UserString* text = (const UserString*)value;
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(redisCtx, fmt, text->user);
  Redis_DeleteKey(redisCtx, str);
  RedisModule_FreeString(redisCtx, str);
}

const char *HiddenString_GetUnsafe(const HiddenString* value, size_t* length) {
  const UserString* text = (const UserString*)value;
  if (length != NULL) {
    *length = text->length;
  }
  return text->user;
}

RedisModuleString *HiddenString_CreateRedisModuleString(const HiddenString* value, RedisModuleCtx* ctx) {
  const UserString* text = (const UserString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}
