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
  const char *user;
  uint64_t length;
} UserString;

HiddenString* HideAndObfuscateString(const char* str, uint64_t length, bool takeOwnership) {
  UserAndObfuscatedString* value = rm_malloc(sizeof(*value));
  value->user = str;
  if (takeOwnership) {
    value->user = rm_strndup(str, length);
  }
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

HiddenName *NewHiddenName(const char* name, uint64_t length, bool takeOwnership) {
  UserString* value = rm_malloc(sizeof(*value));
  value->user = name;
  if (takeOwnership) {
    value->user = rm_strndup(name, length);
  }
  value->length = length;
  return (HiddenName*)value;
};

void HiddenString_Free(HiddenString* hs, bool tookOwnership) {
  UserAndObfuscatedString* value = (UserAndObfuscatedString*)hs;
  if (tookOwnership) {
    rm_free((void*)value->user);
  }
  rm_free(value);
};

void HiddenSize_Free(HiddenSize* hn) {
  UserAndObfuscatedUInt64* value = (UserAndObfuscatedUInt64*)hn;
  rm_free(value);
};

void HiddenName_Free(HiddenName* hn, bool tookOwnership) {
  UserString* value = (UserString*)hn;
  if (tookOwnership) {
    rm_free((void*)value->user);
  }
  rm_free(value);
};

HiddenString *HiddenString_Clone(const HiddenString* value) {
    UserAndObfuscatedString* v = (UserAndObfuscatedString*)value;
    return HideAndObfuscateString(v->user, v->length, true);
}

const char *HiddenString_Get(const HiddenString *value, bool obfuscate) {
  UserAndObfuscatedString* v = (UserAndObfuscatedString*)value;
  return obfuscate ? v->obfuscated : v->user;
}

static inline int Compare(const char *left, size_t left_length, const char *right, size_t right_length) {
  int result = strncmp(left, right, MIN(left_length, right_length));
  if (result != 0 || left_length == right_length) {
    return result;
  } else {
    return left_length < right_length ? -1 : 1;
  }
}

static inline int CaseSensitiveCompare(const char *left, size_t left_length, const char *right, size_t right_length) {
  int result = strncasecmp(left, right, MIN(left_length, right_length));
  if (result != 0 || left_length == right_length) {
    return result;
  } else {
    return left_length < right_length ? -1 : 1;
  }
}

int HiddenString_Compare(HiddenString *left, HiddenString *right) {
  UserAndObfuscatedString* l = (UserAndObfuscatedString*)left;
  UserAndObfuscatedString* r = (UserAndObfuscatedString*)right;
  return Compare(l->user, l->length, r->user, r->length);
}

int HiddenString_CompareC(HiddenString *left, const char *right, size_t right_length) {
  UserAndObfuscatedString* l = (UserAndObfuscatedString*)left;
  return Compare(l->user, l->length, right, right_length);
}

int HiddenString_CaseInsensitiveCompareC(HiddenString *left, const char *right, size_t right_length) {
  UserAndObfuscatedString* l = (UserAndObfuscatedString*)left;
  return CaseSensitiveCompare(l->user, l->length, right, right_length);
}

int HiddenString_CaseInsensitiveCompare(HiddenString *left, HiddenString *right) {
  UserAndObfuscatedString* r = (UserAndObfuscatedString*)right;
  return HiddenString_CaseInsensitiveCompareC(left, r->user, r->length);
}

void HiddenString_SaveToRdb(HiddenName* value, RedisModuleIO* rdb) {
  UserString* text = (UserString*)value;
  RedisModule_SaveStringBuffer(rdb, text->user, text->length + 1);
}

int HiddenName_CompareC(const HiddenName *left, const char *right, size_t right_length) {
  const UserString* l = (const UserString*)left;
  return Compare(l->user, l->length, right, right_length);
}

int HiddenName_Compare(const HiddenName* left, const HiddenName* right) {
  UserString* r = (UserString*)right;
  return HiddenName_CompareC(left, r->user, r->length);
}

int HiddenName_CaseInsensitiveCompareC(HiddenName *left, const char *right, size_t right_length) {
  UserString* l = (UserString*)left;
  return CaseSensitiveCompare(l->user, l->length, right, right_length);
}

int HiddenName_CaseInsensitiveCompare(HiddenName *left, HiddenName *right) {
  UserString* r = (UserString*)right;
  return HiddenName_CaseInsensitiveCompareC(left, r->user, r->length);
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
      d->length = s->length;
    }
    strncpy((void*)d->user, s->user, s->length);
    if (d->length > s->length) {
      memset((void*)d->user + s->length, 0, d->length - s->length);
    }
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

RedisModuleString *HiddenString_CreateString(HiddenString* value, RedisModuleCtx* ctx) {
  UserAndObfuscatedString* text = (UserAndObfuscatedString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}

RedisModuleString *HiddenName_CreateString(HiddenName* value, RedisModuleCtx* ctx) {
  UserString* text = (UserString*)value;
  return RedisModule_CreateString(ctx, text->user, text->length);
}