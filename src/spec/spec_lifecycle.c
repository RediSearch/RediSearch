/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"
#include "spec_registry.h"
#include "spec_field_parse.h"
#include "rlookup_load_document.h"

#include <math.h>
#include <ctype.h>

#include "triemap.h"
#include "util/logging.h"
#include "util/misc.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "rmutil/rm_assert.h"
#include "trie/trie_type.h"
#include "rmalloc.h"
#include "config.h"
#include "cursor.h"
#include "tag_index.h"
#include "redis_index.h"
#include "indexer.h"
#include "suffix.h"
#include "alias.h"
#include "module.h"
#include "aggregate/expr/expression.h"
#include "rules.h"
#include "dictionary.h"
#include "doc_types.h"
#include "rdb.h"
#include "commands.h"
#include "obfuscation/obfuscation_api.h"
#include "util/workers.h"
#include "info/global_stats.h"
#include "debug_commands.h"
#include "reply_macros.h"
#include "notifications.h"
#include "info/field_spec_info.h"
#include "rs_wall_clock.h"
#include "util/redis_mem_info.h"
#include "search_disk.h"
#include "search_disk_utils.h"

#define INITIAL_DOC_TABLE_SIZE 1000

///////////////////////////////////////////////////////////////////////////////////////////////

extern DebugCTX globalDebugCtx;

//---------------------------------------------------------------------------------------------

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CreateTextId(IndexSpec *sp, t_fieldIndex index) {
  size_t length = array_len(sp->fieldIdToIndex);
  if (length >= SPEC_MAX_FIELD_ID) {
    return -1;
  }

  array_ensure_append_1(sp->fieldIdToIndex, index);
  return length;
}

// Assuming the spec is properly locked for writing before calling this function.
void IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  int isNew = Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL, 1);
  if (isNew) {
    sp->stats.scoring.numTerms++;
    sp->stats.termsSize += len;
  }
}

// For testing purposes only
void Spec_AddToDict(RefManager *rm) {
  IndexSpec* spec = ((IndexSpec*)__RefManager_Get_Object(rm));
  dictAdd(specDict_g, (void*)spec->specName, (void *)rm);
}

///////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Free resources of unlinked index spec
 */
static void IndexSpec_FreeUnlinkedData(IndexSpec *spec) {

  // Free all documents metadata
  DocTable_Free(&spec->docs);
  // Free TEXT field trie and inverted indexes
  if (spec->terms) {
    TrieType_Free(spec->terms);
  }
  // Free TEXT TAG NUMERIC VECTOR and GEOSHAPE fields trie and inverted indexes
  if (spec->keysDict) {
    dictRelease(spec->keysDict);
  }
  // Free missingFieldDict
  if (spec->missingFieldDict) {
    dictRelease(spec->missingFieldDict);
  }
  // Free existing docs inverted index
  if (spec->existingDocs) {
    InvertedIndex_Free(spec->existingDocs);
  }
  // Free synonym data
  if (spec->smap) {
    SynonymMap_Free(spec->smap);
  }
  // Destroy spec rule
  if (spec->rule) {
    SchemaRule_Free(spec->rule);
    spec->rule = NULL;
  }
  // Free fields cache data
  IndexSpecCache_Decref(spec->spcache);
  spec->spcache = NULL;

  array_free(spec->fieldIdToIndex);
  spec->fieldIdToIndex = NULL;

  // Free fields data
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      FieldSpec_Cleanup(&spec->fields[i]);
    }
    rm_free(spec->fields);
  }
  // Free suffix trie
  if (spec->suffix) {
    TrieType_Free(spec->suffix);
  }

  // Close disk index before freeing spec name (needs the name for tracking)
  if (spec->diskSpec) SearchDisk_CloseIndex(NULL, spec->diskSpec);

  // Free spec name (after disk close, which needs the name)
  HiddenString_Free(spec->specName, true);
  rm_free(spec->obfuscatedName);

  // Destroy the spec's lock
  pthread_rwlock_destroy(&spec->rwlock);

  // Free spec struct
  rm_free(spec);

  removePendingIndexDrop();
}

/*
 * This function unlinks the index spec from any global structures and frees
 * all struct that requires acquiring the GIL.
 * Other resources are freed using IndexSpec_FreeData.
 */
void IndexSpec_Free(IndexSpec *spec) {
  // Stop scanner
  // Scanner has a weak reference to the spec, so at this point it will cancel itself and free
  // next time it will try to acquire the spec.

  // For temporary index
  // This function might be called from any thread, and we cannot deal with timers without the GIL.
  // At this point we should have already stopped the timer.
  RS_ASSERT(!spec->isTimerSet);
  // Stop and destroy garbage collector
  // We can't free it now, because it either runs at the moment or has a timer set which we can't
  // deal with without the GIL.
  // It will free itself when it discovers that the index was freed.
  // On the worst case, it just finishes the current run and will schedule another run soon.
  // In this case the GC will be freed on the next run, in `forkGcRunIntervalSec` seconds.
  if (RS_IsMock && spec->gc) {
    GCContext_StopMock(spec->gc);
  }

  // Free stopwords list (might use global pointer to default list)
  if (spec->stopwords) {
    StopWordList_Unref(spec->stopwords);
    spec->stopwords = NULL;
  }

  IndexError_Clear(spec->stats.indexError);
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      IndexError_Clear((spec->fields[i]).indexError);
    }
  }

  // Free unlinked index spec on a second thread
  if (RSGlobalConfig.freeResourcesThread == false || SearchDisk_IsEnabled()) {
    IndexSpec_FreeUnlinkedData(spec);
  } else {
    CleanPool_AddWork((void (*)(void *))IndexSpec_FreeUnlinkedData, spec);
  }
}

//---------------------------------------------------------------------------------------------

// Assuming the spec is properly locked before calling this function.
void IndexSpec_InitializeSynonym(IndexSpec *sp) {
  if (!sp->smap) {
    sp->smap = SynonymMap_New(false);
    sp->flags |= Index_HasSmap;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec_InitLock(IndexSpec *sp) {
  int res = 0;
  pthread_rwlockattr_t attr;
  res = pthread_rwlockattr_init(&attr);
  RS_ASSERT(res == 0);
#if !defined(__APPLE__) && !defined(__FreeBSD__) && defined(__GLIBC__)
  int pref = PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP;
  res = pthread_rwlockattr_setkind_np(&attr, pref);
  RS_ASSERT(res == 0);
#endif

  pthread_rwlock_init(&sp->rwlock, &attr);
}

// Helper function for initializing an index spec
// Solves issues where a field is initialized in index creation but not when loading from RDB
void initializeIndexSpec(IndexSpec *sp, const HiddenString *name, IndexFlags flags,
                         int16_t numFields) {
  sp->flags = flags;
  sp->numFields = numFields;
  sp->fields = rm_calloc(numFields, sizeof(FieldSpec));
  sp->specName = name;
  sp->obfuscatedName = IndexSpec_FormatObfuscatedName(name);
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->suffix = NULL;
  sp->suffixMask = (t_fieldMask)0;
  sp->keysDict = NULL;
  sp->getValue = NULL;
  sp->getValueCtx = NULL;
  sp->timeout = 0;
  sp->isTimerSet = false;
  sp->timerId = 0;

  sp->scanner = NULL;
  sp->scan_in_progress = false;
  sp->monitorDocumentExpiration = RSGlobalConfig.monitorExpiration;
  sp->monitorFieldExpiration = RSGlobalConfig.monitorExpiration &&
                               RedisModule_HashFieldMinExpire != NULL;
  sp->used_dialects = 0;

  memset(&sp->stats, 0, sizeof(sp->stats));
  sp->stats.indexError = IndexError_Init();

  sp->fieldIdToIndex = array_new(t_fieldIndex, 0);
  sp->terms = NewTrie(NULL, Trie_Sort_Lex);

  IndexSpec_InitLock(sp);
  // First, initialise fields IndexError for every field
  // In the RDB flow if some fields are not loaded correctly, we will free the spec and attempt to cleanup all the fields.
  for (t_fieldIndex i = 0; i < sp->numFields; i++) {
    initializeFieldSpec(sp->fields + i, i);
  }
}

IndexSpec *NewIndexSpec(const HiddenString *name) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  initializeIndexSpec(sp, name, INDEX_DEFAULT_FLAGS, 0);
  sp->stopwords = DefaultStopWordList();
  return sp;
}

// Assuming the spec is properly locked before calling this function.
FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path) {
  FieldSpec* fields = sp->fields;
  fields = rm_realloc(fields, sizeof(*fields) * (sp->numFields + 1));
  RS_LOG_ASSERT_FMT(fields, "Failed to allocate memory for %d fields", sp->numFields + 1);
  sp->fields = fields;
  FieldSpec *fs = sp->fields + sp->numFields;
  memset(fs, 0, sizeof(*fs));
  initializeFieldSpec(fs, sp->numFields);
  ++sp->numFields;
  fs->fieldName = NewHiddenString(name, strlen(name), true);
  fs->fieldPath = (path) ? NewHiddenString(path, strlen(path), true) : fs->fieldName;
  fs->ftId = RS_INVALID_FIELD_ID;
  fs->ftWeight = 1.0;
  fs->sortIdx = -1;
  fs->tagOpts.tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  if (!(sp->flags & Index_FromLLAPI)) {
    RS_LOG_ASSERT((sp->rule), "index w/o a rule?");
    switch (sp->rule->type) {
      case DocumentType_Hash:
        fs->tagOpts.tagSep = TAG_FIELD_DEFAULT_HASH_SEP; break;
      case DocumentType_Json:
        fs->tagOpts.tagSep = TAG_FIELD_DEFAULT_JSON_SEP; break;
      case DocumentType_Unsupported:
        RS_ABORT("shouldn't get here");
        break;
    }
  }
  return fs;
}

uint64_t CharBuf_HashFunction(const void *key) {
  const CharBuf *cb = key;
  return RS_dictGenHashFunction(cb->buf, cb->len);
}

void *CharBuf_KeyDup(void *privdata, const void *key) {
  const CharBuf *cb = key;
  CharBuf *newcb = rm_malloc(sizeof(*newcb));
  newcb->len = cb->len;
  newcb->buf = rm_malloc(cb->len);
  memcpy(newcb->buf, cb->buf, cb->len);
  return newcb;
}

int CharBuf_KeyCompare(void *privdata, const void *key1, const void *key2) {
  const CharBuf *cb1 = key1;
  const CharBuf *cb2 = key2;
  if (cb1->len != cb2->len) {
    return 0;
  }
  return (memcmp(cb1->buf, cb2->buf, cb1->len) == 0);
}

void CharBuf_KeyDestructor(void *privdata, void *key) {
  CharBuf *cb = key;
  rm_free(cb->buf);
  rm_free(cb);
}

void InvIndFreeCb(void *privdata, void *val) {
  InvertedIndex *idx = val;
  InvertedIndex_Free(idx);
}

static dictType invIdxDictType = {
  .hashFunction = CharBuf_HashFunction,
  .keyDup = CharBuf_KeyDup,
  .valDup = NULL, // Taking and owning the InvertedIndex pointer
  .keyCompare = CharBuf_KeyCompare,
  .keyDestructor = CharBuf_KeyDestructor,
  .valDestructor = InvIndFreeCb,
};

static dictType missingFieldDictType = {
        .hashFunction = hiddenNameHashFunction,
        .keyDup = hiddenNameKeyDup,
        .valDup = NULL,
        .keyCompare = hiddenNameKeyCompare,
        .keyDestructor = hiddenNameKeyDestructor,
        .valDestructor = InvIndFreeCb,
};

// Only used on new specs so it's thread safe
void IndexSpec_MakeKeyless(IndexSpec *sp) {
  sp->keysDict = dictCreate(&invIdxDictType, NULL);
  sp->missingFieldDict = dictCreate(&missingFieldDictType, NULL);
}

// Only used on new specs so it's thread safe
void IndexSpec_StartGCFromSpec(StrongRef global, IndexSpec *sp, uint32_t gcPolicy) {
  sp->gc = GCContext_CreateGC(global, gcPolicy);
  GCContext_Start(sp->gc);
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
// Only used on new specs so it's thread safe
void IndexSpec_StartGC(StrongRef global, IndexSpec *sp, GCPolicy gcPolicy) {
  RS_LOG_ASSERT(!sp->gc, "GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.gcConfigParams.enableGC && !(sp->flags & Index_Temporary)) {
    sp->gc = GCContext_CreateGC(global, gcPolicy);
    GCContext_Start(sp->gc);

    const char* name = IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog);
    RedisModule_Log(RSDummyContext, "verbose", "Starting GC for %s", name);
    RedisModule_Log(RSDummyContext, "debug", "Starting GC %p for %s", sp->gc, name);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  if (!spec->rule) {
    RedisModule_Log(ctx, "warning", "Index spec '%s': no rule found", IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog));
    return REDISMODULE_ERR;
  }

  QueryError status = QueryError_Default();

  if(spec->scan_failed_OOM) {
    QueryError_SetWithoutUserDataFmt(&status, QUERY_ERROR_CODE_INDEX_BG_OOM_FAIL, "Index background scan did not complete due to OOM. New documents will not be indexed.");
    IndexError_AddQueryError(&spec->stats.indexError, &status, key);
    QueryError_ClearError(&status);
    return REDISMODULE_ERR;
  }

  rs_wall_clock startDocTime;
  rs_wall_clock_init(&startDocTime);

  Document doc = {0};
  Document_Init(&doc, key, DEFAULT_SCORE, DEFAULT_LANGUAGE, type);
  // if a key does not exit, is not a hash or has no fields in index schema

  int rv = REDISMODULE_ERR;
  switch (type) {
  case DocumentType_Hash:
    rv = Document_LoadSchemaFieldHash(&doc, &sctx, &status);
    break;
  case DocumentType_Json:
    rv = Document_LoadSchemaFieldJson(&doc, &sctx, &status);
    break;
  case DocumentType_Unsupported:
    RS_ABORT("Should receive valid type");
    break;
  }

  if (rv != REDISMODULE_OK) {
    // we already unlocked the spec but we can increase this value atomically
    IndexError_AddQueryError(&spec->stats.indexError, &status, doc.docKey);

    // if a document did not load properly, it is deleted
    // to prevent mismatch of index and hash
    IndexSpec_DeleteDoc(spec, ctx, key);
    QueryError_ClearError(&status);
    Document_Free(&doc);
    return REDISMODULE_ERR;
  }

  unsigned int numOps = doc.numFields != 0 ? doc.numFields: 1;
  IndexerYieldWhileLoading(ctx, numOps, REDISMODULE_YIELD_FLAG_CLIENTS);
  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_IncrActiveWrites(spec);

  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(spec, &doc, &status);
  aCtx->stateFlags |= ACTX_F_NOFREEDOC;
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE);

  Document_Free(&doc);

  spec->stats.totalIndexTime += rs_wall_clock_elapsed_ns(&startDocTime);
  IndexSpec_DecrActiveWrites(spec);
  RedisSearchCtx_UnlockSpec(&sctx);
  return REDISMODULE_OK;
}

inline static bool isSpecOnDisk(const IndexSpec *sp) {
  return SearchDisk_IsEnabled();
}

void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  t_docId id = 0;
  uint32_t docLen = 0;
  if (isSpecOnDisk(spec)) {
    RS_LOG_ASSERT(spec->diskSpec, "disk handle is unexpectedly NULL");
    size_t len;
    const char *keyStr = RedisModule_StringPtrLen(key, &len);

    // Delete the document
    SearchDisk_DeleteDocument(spec->diskSpec, keyStr, len, &docLen, &id);

    if (id == 0) {
      // Nothing to delete
      return;
    }
  } else {
    RSDocumentMetadata *md = DocTable_PopR(&spec->docs, key);
    if (!md) {
      // Nothing to delete
      return;
    }

    id = md->id;
    docLen = md->docLen;

    DMD_Return(md);
  }

  // Update the stats
  RS_LOG_ASSERT(spec->stats.scoring.totalDocsLen >= docLen, "totalDocsLen is smaller than docLen");
  spec->stats.scoring.totalDocsLen -= docLen;
  RS_LOG_ASSERT(spec->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
  spec->stats.scoring.numDocuments--;

  // Increment the index's garbage collector's scanning frequency after document deletions
  if (spec->gc) {
    GCContext_OnDelete(spec->gc);
  }

  // VecSim fields clear deleted data on the fly
  if (spec->flags & Index_HasVecSim) {
    for (int i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
        // ctx is NULL because we don't create the index here
        VecSimIndex *vecsim = openVectorIndex(NULL, spec->fields + i, DONT_CREATE_INDEX);
        if(!vecsim) continue;
        VecSimIndex_DeleteVector(vecsim, id);
      }
    }
  }

  if (spec->flags & Index_HasGeometry) {
    GeometryIndex_RemoveId(spec, id);
  }
}

int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  IndexSpec_IncrActiveWrites(spec);
  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_DeleteDoc_Unsafe(spec, ctx, key);
  IndexSpec_DecrActiveWrites(spec);
  RedisSearchCtx_UnlockSpec(&sctx);

  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// =============================================================================
// Compaction FFI Functions (called by Rust during GC)
// =============================================================================

// Acquire IndexSpec write lock
void IndexSpec_AcquireWriteLock(IndexSpec* sp) {
  pthread_rwlock_wrlock(&sp->rwlock);
}

// Release IndexSpec write lock
void IndexSpec_ReleaseWriteLock(IndexSpec* sp) {
  pthread_rwlock_unlock(&sp->rwlock);
}

// Update a term's document count in the Serving Trie
// Note: term is NOT null-terminated; term_len specifies the length
// Returns true if the term was completely emptied and deleted from the trie.
bool IndexSpec_DecrementTrieTermCount(IndexSpec* sp, const char* term, size_t term_len,
                                  size_t doc_count_decrement) {
  if (!sp->terms || doc_count_decrement == 0) {
    return false;
  }
  // Decrement the numDocs count for this term in the trie
  // If numDocs reaches 0, the node will be deleted
  TrieDecrResult result = Trie_DecrementNumDocs(sp->terms, term, term_len, doc_count_decrement);
  RS_ASSERT(result != TRIE_DECR_NOT_FOUND);
  return result == TRIE_DECR_DELETED;
}

// Update IndexScoringStats based on compaction delta
// Note: num_docs and totalDocsLen are updated at delete time, NOT by GC.
// GC only updates numTerms (when terms become completely empty).
void IndexSpec_DecrementNumTerms(IndexSpec* sp, uint64_t num_terms_removed) {
  if (num_terms_removed == 0) {
    return;
  }

  RS_ASSERT(num_terms_removed <= sp->stats.scoring.numTerms);
  sp->stats.scoring.numTerms -= num_terms_removed;
}
