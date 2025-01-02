#include "obfuscation/hidden_unicode.h"

HiddenUnicodeString *NewHiddenUnicodeString(const char *name) {
  return (HiddenUnicodeString*)sdsnew(name);
}

void HiddenUnicodeString_Free(const HiddenUnicodeString *hn) {
  sds value = (sds)hn;
  sdsfree(value);
}

int HiddenUnicodeString_Compare(const HiddenUnicodeString *left, const HiddenUnicodeString *right) {
  sds l = (sds)left;
  sds r = (sds)right;
  return sdscmp(l, r);
}

int HiddenUnicodeString_CompareC(const HiddenUnicodeString *left, sds right) {
  sds l = (sds)left;
  return sdscmp(l, right);
}

sds HiddenUnicodeString_GetUnsafe(const HiddenUnicodeString *value, size_t *length) {
  sds v = (sds)value;
  if (length) {
      *length = sdslen(v);
  }
  return v;
}

RedisModuleString *HiddenUnicodeString_CreateRedisModuleString(const HiddenUnicodeString* value, RedisModuleCtx* ctx) {
  sds v = (sds)value;
  return RedisModule_CreateString(ctx, v, sdslen(v));
}

void HiddenUnicodeString_SaveToRdb(const HiddenUnicodeString* value, RedisModuleIO* rdb) {
  sds v = (sds)value;
  RedisModule_SaveStringBuffer(rdb, v, sdslen(v) + 1);
}