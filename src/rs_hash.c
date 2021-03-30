#include "rs_hash.h"
#include "config.h"
#include "rmutil/rm_assert.h"

extern bool isCrdt;

typedef struct {
  RedisModuleCtx *ctx;
  SchemaRule *rule;
  arrayof(void *) arr;
} HashPrintArgs;

static void Hash_Cursor_cb(RedisModuleKey *key, RedisModuleString *field, RedisModuleString *value, void *privdata) {
  REDISMODULE_NOT_USED(key);
  HashPrintArgs *args = privdata;
  RedisModuleCtx *ctx = args->ctx;
  SchemaRule *rule = args->rule;
  
  // Do not reply with these fields
  size_t flen;
  const char *fieldStr = RedisModule_StringPtrLen(field, &flen);
  if (SchemaRule_IsAttrField(args->rule, fieldStr, flen)) {
    return;
  }

  // TODO: Remove once scan is used
  const char *fstr = RedisModule_StringPtrLen(field, NULL);
  const char *vstr = RedisModule_StringPtrLen(value, NULL);
  RedisModule_RetainString(ctx, field);
  RedisModule_RetainString(ctx, value);
  args->arr = array_ensure_append(args->arr, &field, 1, void *);
  args->arr = array_ensure_append(args->arr, &value, 1, void *);
}

int RS_ReplyWithHash(RedisModuleCtx *ctx, char *keyC, arrayof(void *) *replyArr, SchemaRule *rule) {
  array_clear(*replyArr);
  int rc = REDISMODULE_ERR;

  // Reply using scanning - from Redis 6.0.6
  if (isFeatureSupported(RM_SCAN_KEY_API_FIX) && !isCrdt) {
    RedisModuleString *keyR = RedisModule_CreateString(ctx, keyC, strlen(keyC));
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyR, REDISMODULE_READ);
    if (!key || RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
      RedisModule_ReplyWithNullArray(ctx);
      goto donescan;
    }

    HashPrintArgs args = { .ctx = ctx,
                           .arr = *replyArr,
                           .rule = rule, };

    RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
    while(RedisModule_ScanKey(key, cursor, Hash_Cursor_cb, &args));
    RedisModule_ScanCursorDestroy(cursor);

    int len = array_len(args.arr);
    RedisModule_ReplyWithArray(ctx, len);
    for (uint32_t i = 0; i < len; i++) {
      RedisModuleString *reply = args.arr[i];
      RedisModule_ReplyWithString(ctx, reply);
      RedisModule_FreeString(ctx, reply);
    } 

    rc = REDISMODULE_OK;
  donescan:
    *replyArr = args.arr;
    if (key) RedisModule_CloseKey(key);
    if (keyR) RedisModule_FreeString(ctx, keyR);

  // Reply using RM_Call()
  } else {
    arrayof(void *) arr = *replyArr;
    RedisModuleCallReply *field, *value;
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "HGETALL", "c", keyC);
    if (!reply || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
      RedisModule_ReplyWithNullArray(ctx);
      goto donecall;
    }

    size_t arrlen = 0;
    size_t len = RedisModule_CallReplyLength(reply);
    for (size_t i = 0; i < len; i += 2) {
      field = RedisModule_CallReplyArrayElement(reply, i);
      size_t flen = 0;
      const char *fstr = RedisModule_CallReplyStringPtr(field, &flen);
      // skip field if it is an attribute field
      if (SchemaRule_IsAttrField(rule, fstr, flen)){
        continue;
      }
      arr = array_ensure_append(arr, &field, 1, void *);

      value = RedisModule_CallReplyArrayElement(reply, i + 1);
      // const char *vstr = RedisModule_CallReplyStringPtr(value, NULL);
      arr = array_ensure_append(arr, &value, 1, void *);

      arrlen += 2;
    }

    RedisModule_ReplyWithArray(ctx, arrlen);
    for (size_t i = 0; i < arrlen; i++) {
      RedisModule_ReplyWithCallReply(ctx, arr[i]);
    }

    rc = REDISMODULE_OK;
donecall:
    RedisModule_FreeCallReply(reply);
    *replyArr = arr;
  }
  return rc;
}