#include "hidden.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "redis_index.h"
#include "query_node.h"
#include "reply_macros.h"

typedef struct {
  const char *user;
  size_t length;
} UserString;

HiddenName *NewHiddenName(const char* name, uint64_t length, bool takeOwnership) {
  UserString* value = rm_malloc(sizeof(*value));
  if (takeOwnership) {
    value->user = rm_strndup(name, length);
  } else {
    value->user = name;
  }
  value->length = length;
  return (HiddenName*)value;
};

void HiddenName_Free(HiddenName* hn, bool tookOwnership) {
  UserString* value = (UserString*)hn;
  if (tookOwnership) {
    rm_free((void*)value->user);
  }
  rm_free(value);
};

static inline int Compare(const char *left, size_t left_length, const char *right, size_t right_length) {
  int result = strncmp(left, right, MIN(left_length, right_length));
  if (result != 0 || left_length == right_length) {
    return result;
  } else {
    return (int)(left_length - right_length);
  }
}

static inline int CaseInsensitiveCompare(const char *left, size_t left_length, const char *right, size_t right_length) {
  int result = strncasecmp(left, right, MIN(left_length, right_length));
  if (result != 0 || left_length == right_length) {
    return result;
  } else {
    return (int)(left_length - right_length);
  }
}

int HiddenName_CompareC(const HiddenName *left, const char *right, size_t right_length) {
  const UserString* l = (const UserString*)left;
  return Compare(l->user, l->length, right, right_length);
}

int HiddenName_Compare(const HiddenName* left, const HiddenName* right) {
  UserString* r = (UserString*)right;
  return HiddenName_CompareC(left, r->user, r->length);
}

int HiddenName_CaseInsensitiveCompare(HiddenName *left, HiddenName *right) {
  UserString* r = (UserString*)right;
  return HiddenName_CaseInsensitiveCompareC(left, r->user, r->length);
}

int HiddenName_CaseInsensitiveCompareC(HiddenName *left, const char *right, size_t right_length) {
  UserString* l = (UserString*)left;
  return CaseInsensitiveCompare(l->user, l->length, right, right_length);
}

HiddenName *HiddenName_Duplicate(const HiddenName *value) {
  const UserString* text = (const UserString*)value;
  return NewHiddenName(text->user, text->length, true);
}

void HiddenName_TakeOwnership(HiddenName *hidden) {
  UserString* userString = (UserString*)hidden;
  userString->user = rm_strndup(userString->user, userString->length);
}

void HiddenName_Clone(HiddenName* src, HiddenName** dst) {
  UserString* s = (UserString*)src;
  if (*dst == NULL) {
    *dst = NewHiddenName(s->user, s->length, true);
  } else {
    UserString* d = (UserString*)*dst;
    if (s->length > d->length) {
      d->user = rm_realloc((void*)d->user, s->length);
    }
    // strncpy will pad d->user with zeroes per documentation if there is room
    // also remember d->user[d->length] == '\0' due to rm_strdup
    strncpy((void*)d->user, s->user, d->length);
    // By setting the length we cause rm_realloc to potentially be called
    // in the future if this function is called again
    // But a reasonable allocator should do zero allocation work and identify the memory chunk is enough
    // That saves us from storing a capacity field
    d->length = s->length;
  }
}

void HiddenName_SaveToRdb(HiddenName* value, RedisModuleIO* rdb) {
  UserString* text = (UserString*)value;
  RedisModule_SaveStringBuffer(rdb, text->user, text->length + 1);
}

void HiddenName_DropFromKeySpace(RedisModuleCtx* redisCtx, const char* fmt, HiddenName* value) {
  UserString* text = (UserString*)value;
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(redisCtx, fmt, text->user);
  Redis_DeleteKey(redisCtx, str);
  RedisModule_FreeString(redisCtx, str);
}

const char *HiddenName_GetUnsafe(const HiddenName* value, size_t* length) {
  const UserString* text = (const UserString*)value;
  if (length != NULL) {
    *length = text->length;
  }
  return text->user;
}

RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx) {
  UserString* text = (UserString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}