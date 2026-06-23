/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <errno.h>
#include "dictionary.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/dict.h"
#include "rdb.h"
#include "resp3.h"
#include "rmutil/rm_assert.h"
#include "commands.h"
#include "config.h"
#include "module.h"
#include "util/likely.h"
// The dictionaries live in memory as Rust `SpellCheckDictionary` values, but
// the RDB on-disk format is still the legacy C `Trie` blob so that RDBs remain
// loadable in both directions across the swap. `trie/trie.h` is needed only for
// that persistence bridge.
#include "trie/trie.h"

dict *spellCheckDicts = NULL;

SpellCheckDictionary *SpellCheck_OpenDict(RedisModuleCtx *ctx, const char *dictName, int mode) {
  SpellCheckDictionary *t = dictFetchValue(spellCheckDicts, dictName);
  if (!t && mode == REDISMODULE_WRITE) {
    t = NewSpellCheckDictionary();
    dictAdd(spellCheckDicts, (char *)dictName, t);
  }
  return t;
}

int Dictionary_Add(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len) {
  int valuesAdded = 0;
  SpellCheckDictionary *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_WRITE);
  RS_LOG_ASSERT_ALWAYS(t != NULL, "Failed to open dictionary in write mode");

  for (int i = 0; i < len; ++i) {
    size_t valLen;
    const char *val = RedisModule_StringPtrLen(values[i], &valLen);
    if (likely(SpellCheckDictionary_Add(t, val, valLen))) {
      valuesAdded++;
    }
  }

  return valuesAdded;
}

int Dictionary_Del(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len) {
  int valuesDeleted = 0;
  SpellCheckDictionary *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_READ);
  if (t == NULL) {
    return 0;
  }

  for (int i = 0; i < len; ++i) {
    size_t valLen;
    const char *val = RedisModule_StringPtrLen(values[i], &valLen);
    valuesDeleted += SpellCheckDictionary_Remove(t, val, valLen);
  }

  // Delete the dictionary if it's empty
  if (SpellCheckDictionary_Len(t) == 0) {
    dictDelete(spellCheckDicts, dictName);
    SpellCheckDictionary_Free(t);
  }

  return valuesDeleted;
}

void Dictionary_Dump(RedisModuleCtx *ctx, const char *dictName) {
  SpellCheckDictionary *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_READ);
  if (t == NULL) {
    RedisModule_ReplyWithSet(ctx, 0);
    return;
  }

  RedisModule_ReplyWithSet(ctx, SpellCheckDictionary_Len(t));

  const char *term = NULL;
  size_t termLen = 0;
  SpellCheckDictionaryIterator *it = SpellCheckDictionary_IterateAll(t);
  while (SpellCheckDictionaryIterator_Next(it, &term, &termLen)) {
    RedisModule_ReplyWithStringBuffer(ctx, term, termLen);
  }
  SpellCheckDictionaryIterator_Free(it);
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
      SpellCheckDictionary *val = dictGetVal(entry);
      SpellCheckDictionary_Free(val);
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

static void Propagate_Dict(RedisModuleCtx* ctx, const char* dictName, SpellCheckDictionary* scDict) {
  size_t count = SpellCheckDictionary_Len(scDict);

  RedisModuleString **terms = rm_malloc(count * sizeof(RedisModuleString*));
  size_t termsCount = 0;

  const char *term = NULL;
  size_t termLen = 0;
  SpellCheckDictionaryIterator *it = SpellCheckDictionary_IterateAll(scDict);
  while (SpellCheckDictionaryIterator_Next(it, &term, &termLen)) {
    terms[termsCount++] = RedisModule_CreateString(NULL, term, termLen);
  }
  SpellCheckDictionaryIterator_Free(it);

  RS_ASSERT(termsCount == count);
  RS_LOG_ASSERT(count != 0, "Empty dictionary should not exist in the dictionary list");
  int rc = RedisModule_ClusterPropagateForSlotMigration(ctx, CMD_FOR_ENV(RS_DICT_ADD), "cv", dictName, terms, termsCount);
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
    SpellCheckDictionary *scDict = dictGetVal(entry);
    Propagate_Dict(ctx, dictName, scDict);
  }
  dictReleaseIterator(iter);
}

// Build a transient legacy `Trie` holding every term of `scDict`, used only to
// write the RDB in the historical on-disk format. The caller owns the result
// and must free it with TrieType_Free.
static Trie *SpellCheckDict_ToTrie(SpellCheckDictionary *scDict) {
  Trie *t = NewTrie(NULL, Trie_Sort_Lex);
  const char *term = NULL;
  size_t termLen = 0;
  SpellCheckDictionaryIterator *it = SpellCheckDictionary_IterateAll(scDict);
  while (SpellCheckDictionaryIterator_Next(it, &term, &termLen)) {
    // Payload is NULL so TRIE_ERR_PAYLOAD_OVERFLOW cannot occur. The score is
    // irrelevant to a spell-check dictionary; it is never read back.
    Trie_InsertStringBuffer(t, term, termLen, 1, 1, NULL, 0);
  }
  SpellCheckDictionaryIterator_Free(it);
  return t;
}

// Drain a legacy `Trie` loaded from an RDB into a fresh `SpellCheckDictionary`.
// The caller retains ownership of `t`.
static SpellCheckDictionary *SpellCheckDict_FromTrie(Trie *t) {
  SpellCheckDictionary *scDict = NewSpellCheckDictionary();

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  TrieIterator *it = Trie_IterateAll(t);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    SpellCheckDictionary_Add(scDict, res, termLen);
    rm_free(res);
  }
  TrieIterator_Free(it);

  return scDict;
}

static int SpellCheckDictAuxLoad(RedisModuleIO *rdb, int encver, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    Dictionary_Clear();
    return REDISMODULE_OK;
  }
  size_t len = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t i = 0; i < len; i++) {
    char *key = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    Trie *trie = TrieType_GenericLoad(rdb, false, false, Trie_Sort_Lex);
    if (trie == NULL) {
      RedisModule_Free(key);
      goto cleanup;
    }
    if (Trie_Size(trie)) {
      SpellCheckDictionary *val = SpellCheckDict_FromTrie(trie);
      dictAdd(spellCheckDicts, key, val);
    }
    TrieType_Free(trie);
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
    SpellCheckDictionary *val = dictGetVal(entry);
    RS_LOG_ASSERT(SpellCheckDictionary_Len(val) != 0, "Empty dictionary should not exist in the dictionary list");
    RedisModule_SaveStringBuffer(rdb, key, strlen(key) + 1 /* we save the /0*/);
    Trie *trie = SpellCheckDict_ToTrie(val);
    TrieType_GenericSave(rdb, trie, false, false);
    TrieType_Free(trie);
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
