/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <errno.h>
#include <assert.h>
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
#include "trie_lex.h"

/* Spike: each spellcheck dict holds two synchronized stores.
 *   - `lex` is the Rust-backed lex-mode wrapper. Authoritative for size,
 *     iteration (Dump/Propagate), and RDB save/load. Insert/Delete return
 *     values come from this store.
 *   - `legacy` is the original C trie kept in lock-step so spell_check.c can
 *     continue running Levenshtein queries through `SpellCheck_OpenDict`.
 *     A follow-up branch is expected to port the Levenshtein consumer and
 *     retire `legacy`. */
typedef struct {
  TrieLex *lex;
  Trie *legacy;
} SpellDictEntry;

/* rune/trie_lex_rune are both uint16_t; spike assumes default rune width. */
_Static_assert(sizeof(rune) == sizeof(trie_lex_rune),
               "trie_lex spike requires 16-bit runes");
_Static_assert(sizeof(t_len) == sizeof(trie_lex_t_len),
               "trie_lex spike requires t_len == uint16_t");

dict *spellCheckDicts = NULL;

static SpellDictEntry *NewSpellDictEntry(void) {
  SpellDictEntry *e = rm_malloc(sizeof(*e));
  e->lex = TrieLex_New();
  e->legacy = NewTrie(NULL, Trie_Sort_Lex);
  return e;
}

static void FreeSpellDictEntry(SpellDictEntry *e) {
  if (!e) return;
  TrieLex_Free(e->lex);
  TrieType_Free(e->legacy);
  rm_free(e);
}

static SpellDictEntry *OpenSpellDictEntry(const char *dictName, int mode) {
  SpellDictEntry *e = dictFetchValue(spellCheckDicts, dictName);
  if (!e && mode == REDISMODULE_WRITE) {
    e = NewSpellDictEntry();
    dictAdd(spellCheckDicts, (char *)dictName, e);
  }
  return e;
}

Trie *SpellCheck_OpenDict(RedisModuleCtx *ctx, const char *dictName, int mode) {
  SpellDictEntry *e = OpenSpellDictEntry(dictName, mode);
  return e ? e->legacy : NULL;
}

int Dictionary_Add(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len) {
  int valuesAdded = 0;
  SpellDictEntry *e = OpenSpellDictEntry(dictName, REDISMODULE_WRITE);
  RS_LOG_ASSERT_ALWAYS(e != NULL, "Failed to open dictionary in write mode");

  for (int i = 0; i < len; ++i) {
    size_t vlen;
    const char *vstr = RedisModule_StringPtrLen(values[i], &vlen);
    int rc = TrieLex_InsertStringBuffer(e->lex, vstr, vlen, 1.0, 1);
    /* Mirror into the legacy trie. Always incr/score=1 to match prior
     * behavior of `Trie_Insert(t, values[i], 1, 1, NULL, 0)`. */
    Trie_InsertStringBuffer(e->legacy, vstr, vlen, 1, 1, NULL, 0);
    if (likely(rc == 1)) {
      valuesAdded++;
    }
  }

  return valuesAdded;
}

int Dictionary_Del(RedisModuleCtx *ctx, const char *dictName, RedisModuleString **values, int len) {
  int valuesDeleted = 0;
  SpellDictEntry *e = OpenSpellDictEntry(dictName, REDISMODULE_READ);
  if (e == NULL) {
    return 0;
  }

  for (int i = 0; i < len; ++i) {
    size_t vlen;
    const char *val = RedisModule_StringPtrLen(values[i], &vlen);
    int rc = TrieLex_Delete(e->lex, val, vlen);
    Trie_Delete(e->legacy, val, vlen);
    valuesDeleted += rc;
  }

  // Delete the dictionary if it's empty (per TrieLex, the authoritative size).
  if (TrieLex_Size(e->lex) == 0) {
    dictDelete(spellCheckDicts, dictName);
    FreeSpellDictEntry(e);
  }

  return valuesDeleted;
}

void Dictionary_Dump(RedisModuleCtx *ctx, const char *dictName) {
  SpellDictEntry *e = OpenSpellDictEntry(dictName, REDISMODULE_READ);
  if (e == NULL) {
    RedisModule_ReplyWithSet(ctx, 0);
    return;
  }

  trie_lex_rune *rstr = NULL;
  trie_lex_t_len slen = 0;
  size_t termLen;

  RedisModule_ReplyWithSet(ctx, TrieLex_Size(e->lex));

  TrieLexIterator *it = TrieLex_IterateAll(e->lex);
  while (TrieLexIterator_Next(it, &rstr, &slen, NULL)) {
    char *res = runesToStr((rune *)rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
    rm_free(res);
  }
  TrieLexIterator_Free(it);
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
      SpellDictEntry *val = dictGetVal(entry);
      FreeSpellDictEntry(val);
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

static void Propagate_Dict(RedisModuleCtx* ctx, const char* dictName, SpellDictEntry* e) {
  size_t termLen;
  trie_lex_rune *rstr = NULL;
  trie_lex_t_len slen = 0;

  size_t size = TrieLex_Size(e->lex);
  RedisModuleString **terms = rm_malloc(size * sizeof(RedisModuleString*));
  size_t termsCount = 0;

  TrieLexIterator *it = TrieLex_IterateAll(e->lex);
  while (TrieLexIterator_Next(it, &rstr, &slen, NULL)) {
    char *res = runesToStr((rune *)rstr, slen, &termLen);
    terms[termsCount++] = RedisModule_CreateString(NULL, res, termLen);
    rm_free(res);
  }
  TrieLexIterator_Free(it);

  RS_ASSERT(termsCount == size);
  RS_LOG_ASSERT(size != 0, "Empty dictionary should not exist in the dictionary list");
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
    SpellDictEntry *e = dictGetVal(entry);
    Propagate_Dict(ctx, dictName, e);
  }
  dictReleaseIterator(iter);
}

/* Rebuild the legacy `Trie *` shadow from the freshly-loaded TrieLex. We
 * iterate the lex store and reinsert each term into a fresh legacy trie so
 * Levenshtein consumers (`spell_check.c`) keep working unchanged. */
static Trie *RebuildLegacyFromLex(TrieLex *lex) {
  Trie *legacy = NewTrie(NULL, Trie_Sort_Lex);
  trie_lex_rune *rstr = NULL;
  trie_lex_t_len slen = 0;
  size_t termLen;
  TrieLexIterator *it = TrieLex_IterateAll(lex);
  while (TrieLexIterator_Next(it, &rstr, &slen, NULL)) {
    char *utf = runesToStr((rune *)rstr, slen, &termLen);
    Trie_InsertStringBuffer(legacy, utf, termLen, 1, 1, NULL, 0);
    rm_free(utf);
  }
  TrieLexIterator_Free(it);
  return legacy;
}

static int SpellCheckDictAuxLoad(RedisModuleIO *rdb, int encver, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    Dictionary_Clear();
    return REDISMODULE_OK;
  }
  size_t len = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t i = 0; i < len; i++) {
    char *key = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    TrieLex *lex = TrieLex_RdbLoad(rdb);
    if (lex == NULL) {
      RedisModule_Free(key);
      goto cleanup;
    }
    if (TrieLex_Size(lex)) {
      SpellDictEntry *e = rm_malloc(sizeof(*e));
      e->lex = lex;
      e->legacy = RebuildLegacyFromLex(lex);
      dictAdd(spellCheckDicts, key, e);
    } else {
      TrieLex_Free(lex);
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
    SpellDictEntry *e = dictGetVal(entry);
    RS_LOG_ASSERT(TrieLex_Size(e->lex) != 0, "Empty dictionary should not exist in the dictionary list");
    RedisModule_SaveStringBuffer(rdb, key, strlen(key) + 1 /* we save the /0*/);
    TrieLex_RdbSave(rdb, e->lex);
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
