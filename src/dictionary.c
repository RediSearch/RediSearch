#include "dictionary.h"
#include "redismodule.h"
#include "rmalloc.h"

Trie *SpellCheck_OpenDict(RedisModuleCtx *ctx, const char *dictName, int mode, RedisModuleKey **k) {
  RedisModuleString *keyName = RedisModule_CreateStringPrintf(ctx, DICT_KEY_FMT, dictName);

  *k = RedisModule_OpenKey(ctx, keyName, mode);

  RedisModule_FreeString(ctx, keyName);

  int type = RedisModule_KeyType(*k);
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    Trie *t = NULL;
    if (mode == REDISMODULE_WRITE) {
      t = new Trie();
      RedisModule_ModuleTypeSetValue(*k, TrieType, t);
    } else {
      RedisModule_CloseKey(*k);
    }
    return t;
  }

  if (type != REDISMODULE_KEYTYPE_MODULE || RedisModule_ModuleTypeGetType(*k) != TrieType) {
    RedisModule_CloseKey(*k);
    return NULL;
  }

  return RedisModule_ModuleTypeGetValue(*k);
}

int Dictionary_Add(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len,
                   char **err) {
  int valuesAdded = 0;
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_WRITE, &k);
  if (t == NULL) {
    *err = "could not open dict key";
    return -1;
  }

  for (int i = 0; i < len; ++i) {
    valuesAdded += t->Insert(values[i], 1, 1, NULL);
  }

  RedisModule_CloseKey(k);

  return valuesAdded;
}

int Dictionary_Del(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len,
                   char **err) {
  int valuesDeleted = 0;
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_WRITE, &k);
  if (t == NULL) {
    *err = "could not open dict key";
    return -1;
  }

  for (int i = 0; i < len; ++i) {
    size_t len;
    const char *val = RedisModule_StringPtrLen(values[i], &len);
    valuesDeleted += t->Delete((char *)val);
  }

  if (t->size == 0) {
    RedisModule_DeleteKey(k);
  }

  RedisModule_CloseKey(k);

  return valuesDeleted;
}

bool Dictionary_Dump(RedisModuleCtx *ctx, const char *dictName, char **err) {
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_READ, &k);
  if (t == NULL) {
    *err = "could not open dict key";
    return false;
  }

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  RedisModule_ReplyWithArray(ctx, t->size);

  TrieIterator it = t->Iterate("", 0, 0, 1);
  while (it.Next(&rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
    rm_free(res);
  }

  delete it.ctx;

  RedisModule_CloseKey(k);

  return true;
}

int DictDumpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *err = NULL;
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  const char *dictName = RedisModule_StringPtrLen(argv[1], NULL);

  char *error;
  bool retVal = Dictionary_Dump(ctx, dictName, &error);
  if (!retVal) {
    RedisModule_ReplyWithError(ctx, error);
  }

  return REDISMODULE_OK;
}

int DictDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *err = NULL;
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  const char *dictName = RedisModule_StringPtrLen(argv[1], NULL);

  char *error;
  int retVal = Dictionary_Del(ctx, dictName, argv + 2, argc - 2, &error);
  if (retVal < 0) {
    RedisModule_ReplyWithError(ctx, error);
  } else {
    RedisModule_ReplyWithLongLong(ctx, retVal);
  }
  return REDISMODULE_OK;
}

int DictAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *err = NULL;
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  const char *dictName = RedisModule_StringPtrLen(argv[1], NULL);

  char *error;
  int retVal = Dictionary_Add(ctx, dictName, argv + 2, argc - 2, &error);
  if (retVal < 0) {
    RedisModule_ReplyWithError(ctx, error);
  } else {
    RedisModule_ReplyWithLongLong(ctx, retVal);
  }

  return REDISMODULE_OK;
}
