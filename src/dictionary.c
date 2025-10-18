/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "dictionary.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/dict.h"
#include "rdb.h"
#include "resp3.h"
#include "rmutil/rm_assert.h"
#include "commands.h"
#include "config.h"

dict *spellCheckDicts = NULL;

Trie *SpellCheck_OpenDict(RedisModuleCtx *ctx, const char *dictName, int mode) {
  Trie *t = dictFetchValue(spellCheckDicts, dictName);
  if (!t && mode == REDISMODULE_WRITE) {
    t = NewTrie(NULL, Trie_Sort_Lex);
    dictAdd(spellCheckDicts, (char *)dictName, t);
  }
  return t;
}

int Dictionary_Add(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len) {
  int valuesAdded = 0;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_WRITE);
  RS_LOG_ASSERT_ALWAYS(t != NULL, "Failed to open dictionary in write mode");

  for (int i = 0; i < len; ++i) {
    valuesAdded += Trie_Insert(t, values[i], 1, 1, NULL);
  }

  return valuesAdded;
}

int Dictionary_Del(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len) {
  int valuesDeleted = 0;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_READ);
  if (t == NULL) {
    return 0;
  }

  for (int i = 0; i < len; ++i) {
    size_t valLen;
    const char *val = RedisModule_StringPtrLen(values[i], &valLen);
    valuesDeleted += Trie_Delete(t, val, valLen);
  }

  // Delete the dictionary if it's empty
  if (t->size == 0) {
    dictDelete(spellCheckDicts, dictName);
    TrieType_Free(t);
  }

  return valuesDeleted;
}

void Dictionary_Dump(RedisModuleCtx *ctx, const char *dictName) {
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_READ);
  if (t == NULL) {
    RedisModule_ReplyWithSet(ctx, 0);
    return;
  }

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  RedisModule_ReplyWithSet(ctx, t->size);

  TrieIterator *it = Trie_Iterate(t, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
    rm_free(res);
  }
  TrieIterator_Free(it);
}

int DictDumpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  const char *dictName = RedisModule_StringPtrLen(argv[1], NULL);

  Dictionary_Dump(ctx, dictName);

  return REDISMODULE_OK;
}

int DictDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  const char *dictName = RedisModule_StringPtrLen(argv[1], NULL);

  int retVal = Dictionary_Del(ctx, dictName, argv + 2, argc - 2);
  RedisModule_ReplyWithLongLong(ctx, retVal);

  RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}

int DictAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  const char *dictName = RedisModule_StringPtrLen(argv[1], NULL);

  int retVal = Dictionary_Add(ctx, dictName, argv + 2, argc - 2);

  RedisModule_ReplyWithLongLong(ctx, retVal);

  RedisModule_ReplicateVerbatim(ctx);

  return REDISMODULE_OK;
}

void Dictionary_Clear() {
  if (spellCheckDicts) {
    dictIterator *iter = dictGetIterator(spellCheckDicts);
    dictEntry *entry;
    while ((entry = dictNext(iter))) {
      Trie *val = dictGetVal(entry);
      TrieType_Free(val);
    }
    dictReleaseIterator(iter);
    dictEmpty(spellCheckDicts, NULL);
  }
}

void Dictionary_Free() {
  if (spellCheckDicts) {
    Dictionary_Clear();
    dictRelease(spellCheckDicts);
  }
}

size_t Dictionary_Size() {
  return dictSize(spellCheckDicts);
}

static void Propagate_Dict(RedisModuleCtx* ctx, const char* dictName, Trie* trie) {
  size_t termLen;
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  RedisModuleString **terms = rm_malloc(trie->size * sizeof(RedisModuleString*));
  size_t termsCount = 0;

  TrieIterator *it = Trie_Iterate(trie, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    terms[termsCount++] = RedisModule_CreateString(NULL, res, termLen);
    rm_free(res);
  }
  TrieIterator_Free(it);

  RS_ASSERT(termsCount == trie->size);
  RS_LOG_ASSERT(trie->size != 0, "Empty dictionary should not exist in the dictionary list");
  int rc = RedisModule_ClusterPropagateForSlotMigration(ctx, RS_DICT_ADD, "cv", dictName, terms, termsCount);
  if (rc != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Failed to propagate dictionary '%s' during slot migration. errno: %d", RSGlobalConfig.hideUserDataFromLog ? "****" : dictName, errno);
  }

  for (size_t i = 0; i < termsCount; ++i) {
    RedisModule_FreeString(NULL, terms[i]);
  }
  rm_free(terms);
}

void Dictionary_Propagate(RedisModuleCtx* ctx) {
  dictIterator *iter = dictGetIterator(spellCheckDicts);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    const char *dictName = dictGetKey(entry);
    Trie *trie = dictGetVal(entry);
    Propagate_Dict(ctx, dictName, trie);
  }
  dictReleaseIterator(iter);
}

static int SpellCheckDictAuxLoad(RedisModuleIO *rdb, int encver, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    Dictionary_Clear();
    return REDISMODULE_OK;
  }
  size_t len = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t i = 0; i < len; i++) {
    char *key = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    Trie *val = TrieType_GenericLoad(rdb, false);
    if (val == NULL) {
      RedisModule_Free(key);
      goto cleanup;
    }
    if (val->size) {
      dictAdd(spellCheckDicts, key, val);
    } else {
      TrieType_Free(val);
    }
    RedisModule_Free(key);
  }
  return REDISMODULE_OK;

cleanup:
  Dictionary_Clear();
  return REDISMODULE_ERR;
}

static void SpellCheckDictAuxSave(RedisModuleIO *rdb, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    return;
  }
  RedisModule_SaveUnsigned(rdb, dictSize(spellCheckDicts));
  dictIterator *iter = dictGetIterator(spellCheckDicts);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    const char *key = dictGetKey(entry);
    Trie *val = dictGetVal(entry);
    RS_LOG_ASSERT(val->size != 0, "Empty dictionary should not exist in the dictionary list");
    RedisModule_SaveStringBuffer(rdb, key, strlen(key) + 1 /* we save the /0*/);
    TrieType_GenericSave(rdb, val, false);
  }
  dictReleaseIterator(iter);
}

static void SpellCheckDictAuxSave2(RedisModuleIO *rdb, int when) {
  if (dictSize(spellCheckDicts)) {
    SpellCheckDictAuxSave(rdb, when);
  }
}

#define SPELL_CHECK_ENCVER_CURRENT 1
RedisModuleType *SpellCheckDictType;

int DictRegister(RedisModuleCtx *ctx) {
  spellCheckDicts = dictCreate(&dictTypeHeapStrings, NULL);
  RedisModuleTypeMethods spellCheckDictType = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .aux_load = SpellCheckDictAuxLoad,
      .aux_save = SpellCheckDictAuxSave,
      .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB | REDISMODULE_AUX_AFTER_RDB,
      .aux_save2 = SpellCheckDictAuxSave2,
  };
  SpellCheckDictType =
      RedisModule_CreateDataType(ctx, "scdtype00", SPELL_CHECK_ENCVER_CURRENT, &spellCheckDictType);
  if (SpellCheckDictType == NULL) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
