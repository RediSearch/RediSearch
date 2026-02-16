/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "search_disk.h"
#include "config.h"
#include "index_result/query_term/query_term.h"
#include "spec.h"
#include "trie/trie_type.h"

RedisSearchDiskAPI *disk = NULL;
RedisSearchDisk *disk_db = NULL;

// Global flag to control async I/O (enabled by default, can be toggled via debug command)
static bool asyncIOEnabled = true;

// Weak default implementations for when disk API is not available
__attribute__((weak))
bool SearchDisk_HasAPI() {
  return false;
}

__attribute__((weak))
RedisSearchDiskAPI *SearchDisk_GetAPI() {
  return NULL;
}

bool SearchDisk_Initialize(RedisModuleCtx *ctx) {
  if (!SearchDisk_HasAPI()) {
    RedisModule_Log(ctx, "notice", "RediSearch_Disk API not available");
    return false;
  }

  disk = SearchDisk_GetAPI();
  if (!disk) {
    RedisModule_Log(ctx, "warning", "RediSearch disk API disabled");
    return false;
  }
  RedisModule_Log(ctx, "warning", "RediSearch disk API enabled");

  disk_db = disk->basic.open(ctx);

  return disk_db != NULL;
}

void SearchDisk_Close() {
  if (disk && disk_db) {
    disk->basic.close(disk_db);
    disk_db = NULL;
  }
}

// Basic API wrappers
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(const char *indexName, size_t indexNameLen, DocumentType type) {
    RS_ASSERT(disk_db);
    return disk->basic.openIndexSpec(disk_db, indexName, indexNameLen, type);
}

void SearchDisk_MarkIndexForDeletion(RedisSearchDiskIndexSpec *index) {
    RS_ASSERT(disk_db);
    disk->index.markToBeDeleted(index);
}

void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index) {
    RS_ASSERT(disk_db && index);
    disk->basic.closeIndexSpec(disk_db, index);
}

void SearchDisk_IndexSpecRdbSave(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index) {
  RS_ASSERT(disk && index);
  disk->basic.indexSpecRdbSave(rdb, index);
}

int SearchDisk_IndexSpecRdbLoad(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index) {
  RS_ASSERT(disk);
  return disk->basic.indexSpecRdbLoad(rdb, index);
}

// Index API wrappers
bool SearchDisk_IndexDocument(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask, uint32_t freq) {
    RS_ASSERT(disk && index);
    return disk->index.indexDocument(index, term, termLen, docId, fieldMask, freq);
}

QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, RSToken *tok, int tokenId, t_fieldMask fieldMask, double weight, double idf, double bm25_idf) {
    RS_ASSERT(disk && index && tok);
    RSQueryTerm *term = NewQueryTerm(tok, tokenId);
    term->idf = idf;
    term->bm25_idf = bm25_idf;
    QueryIterator *it = disk->index.newTermIterator(index, term, fieldMask, weight);
    if (!it) {
        Term_Free(term);
    }
    return it;
}

QueryIterator* SearchDisk_NewWildcardIterator(RedisSearchDiskIndexSpec *index, double weight) {
    RS_ASSERT(disk && index);
    return disk->index.newWildcardIterator(index, weight);
}

void SearchDisk_RunGC(RedisSearchDiskIndexSpec *index, const SearchDisk_CompactionCallbacks *callbacks) {
    RS_ASSERT(disk && index);
    disk->index.runGC(index, callbacks);
}

t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t docLen, uint32_t *oldLen, t_expirationTimePoint documentTtl) {
    RS_ASSERT(disk && handle);
    return disk->docTable.putDocument(handle, key, keyLen, score, flags, maxTermFreq, docLen, oldLen, documentTtl);
}

bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd, struct timespec *current_time) {
    RS_ASSERT(disk && handle && current_time);
    return disk->docTable.getDocumentMetadata(handle, docId, dmd, &sdsnewlen, *current_time);
}

bool SearchDisk_DocIdDeleted(RedisSearchDiskIndexSpec *handle, t_docId docId) {
    RS_ASSERT(disk && handle);
    return disk->docTable.isDocIdDeleted(handle, docId);
}

t_docId SearchDisk_GetMaxDocId(RedisSearchDiskIndexSpec *handle) {
    RS_ASSERT(disk && handle);
    return disk->docTable.getMaxDocId(handle);
}

uint64_t SearchDisk_GetDeletedIdsCount(RedisSearchDiskIndexSpec *handle) {
    RS_ASSERT(disk && handle);
    return disk->docTable.getDeletedIdsCount(handle);
}

size_t SearchDisk_GetDeletedIds(RedisSearchDiskIndexSpec *handle, t_docId *buffer, size_t buffer_size) {
    RS_ASSERT(disk && handle);
    return disk->docTable.getDeletedIds(handle, buffer, buffer_size);
}

RedisSearchDiskAsyncReadPool SearchDisk_CreateAsyncReadPool(RedisSearchDiskIndexSpec *handle, uint16_t max_concurrent) {
    RS_ASSERT(disk && handle);
    return disk->docTable.createAsyncReadPool(handle, max_concurrent);
}

bool SearchDisk_AddAsyncRead(RedisSearchDiskAsyncReadPool pool, t_docId docId, uint64_t user_data) {
    RS_ASSERT(disk && pool);
    return disk->docTable.addAsyncRead(pool, docId, user_data);
}

// Callback to allocate a new RSDocumentMetadata with ref_count=1 and keyPtr set
static RSDocumentMetadata* allocateDMD(const void* key_data, size_t key_len) {
    RSDocumentMetadata* dmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
    if (dmd) {
        dmd->ref_count = 1;
        dmd->keyPtr = sdsnewlen(key_data, key_len);
    }
    return dmd;
}

uint16_t SearchDisk_PollAsyncReads(RedisSearchDiskAsyncReadPool pool, uint32_t timeout_ms, arrayof(AsyncReadResult) results, arrayof(uint64_t) failed_user_data, const t_expirationTimePoint* expiration_point) {
    RS_ASSERT(disk && pool);
    AsyncPollResult pollResult = disk->docTable.pollAsyncReads(pool, timeout_ms, results, array_cap(results), failed_user_data, array_cap(failed_user_data), *expiration_point, &allocateDMD);
    array_set_len(results, pollResult.ready_count);
    array_set_len(failed_user_data, pollResult.failed_count);
    return pollResult.pending_count;
}

void SearchDisk_FreeAsyncReadPool(RedisSearchDiskAsyncReadPool pool) {
    RS_ASSERT(disk);
    if (pool) {
        disk->docTable.freeAsyncReadPool(pool);
    }
}

bool SearchDisk_IsAsyncIOSupported() {
    if (!disk || !disk_db) {
        return false;
    }
    // Check if the underlying disk backend supports async I/O
    return disk->basic.isAsyncIOSupported(disk_db);
}

void SearchDisk_SetAsyncIOEnabled(bool enabled) {
    asyncIOEnabled = enabled;
}

bool SearchDisk_GetAsyncIOEnabled() {
    return asyncIOEnabled;
}

void SearchDisk_DeleteDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, uint32_t *oldLen, t_docId *id) {
    RS_ASSERT(disk && handle);
    disk->index.deleteDocument(handle, key, keyLen, oldLen, id);
}

bool SearchDisk_CheckEnableConfiguration(RedisModuleCtx *ctx) {
  bool isFlexConfigured = false;
  char *isFlexEnabledStr = getRedisConfigValue(ctx, "bigredis-enabled");
  if (isFlexEnabledStr && !strcasecmp(isFlexEnabledStr, "yes")) {
    isFlexConfigured = true;
  } // Default is false, so nothing to change in that case.
  rm_free(isFlexEnabledStr);
  return isFlexConfigured;
}

bool SearchDisk_IsEnabled() {
  return isFlex;
}

bool SearchDisk_IsEnabledForValidation() {
  return isFlex || RSGlobalConfig.simulateInFlex;
}

// Vector API wrappers
void* SearchDisk_CreateVectorIndex(RedisSearchDiskIndexSpec *index, const VecSimParamsDisk *params) {
    RS_ASSERT(disk && index && params);
    RS_ASSERT(disk->vector.createVectorIndex);
    return disk->vector.createVectorIndex(index, params);
}

void SearchDisk_FreeVectorIndex(void *vecIndex) {
    RS_ASSERT(disk);
    // Assert that if vecIndex is not NULL, the free function must be set
    // to avoid silent memory leaks from partially implemented API
    RS_ASSERT(!vecIndex || disk->vector.freeVectorIndex);
    disk->vector.freeVectorIndex(vecIndex);
}

uint64_t SearchDisk_CollectIndexMetrics(RedisSearchDiskIndexSpec* index) {
  RS_ASSERT(disk && disk_db && index);
  return disk->metrics.collectIndexMetrics(disk_db, index);
}

void SearchDisk_OutputInfoMetrics(RedisModuleInfoCtx* ctx) {
  RS_ASSERT(disk && disk_db && ctx);
  disk->metrics.outputInfoMetrics(disk_db, ctx);
}

// =============================================================================
// Compaction Callbacks Implementation (Phase 3c)
// =============================================================================

// Acquire IndexSpec write lock
static void SearchDisk_AcquireWriteLock(void* ctx) {
  IndexSpec* sp = (IndexSpec*)ctx;
  pthread_rwlock_wrlock(&sp->rwlock);
}

// Release IndexSpec write lock
static void SearchDisk_ReleaseWriteLock(void* ctx) {
  IndexSpec* sp = (IndexSpec*)ctx;
  pthread_rwlock_unlock(&sp->rwlock);
}

// Update a term's document count in the Serving Trie
// Note: term is NOT null-terminated; term_len specifies the length
static void SearchDisk_UpdateTrieTerm(void* ctx, const char* term, size_t term_len,
                                       size_t doc_count_decrement) {
  IndexSpec* sp = (IndexSpec*)ctx;
  if (!sp->terms || doc_count_decrement == 0) {
    return;
  }
  // Decrement the numDocs count for this term in the trie
  // If numDocs reaches 0, the node will be deleted
  TrieDecrResult result = Trie_DecrementNumDocs(sp->terms, term, term_len, doc_count_decrement);
}

// Update IndexScoringStats based on compaction delta
// Note: num_docs and totalDocsLen are updated at delete time, NOT by GC.
// GC only updates numTerms (when terms become completely empty).
static void SearchDisk_UpdateScoringStats(void* ctx,
                                           const SearchDisk_ScoringStatsDelta* delta) {
  IndexSpec* sp = (IndexSpec*)ctx;
  if (!delta || delta->num_terms_removed == 0) {
    return;
  }

  // Decrement numTerms (clamp to 0 to avoid underflow)
  if (delta->num_terms_removed >= sp->stats.scoring.numTerms) {
    sp->stats.scoring.numTerms = 0;
  } else {
    sp->stats.scoring.numTerms -= delta->num_terms_removed;
  }
}

// Factory function to create a populated CompactionCallbacks struct
SearchDisk_CompactionCallbacks SearchDisk_CreateCompactionCallbacks(IndexSpec* sp) {
  SearchDisk_CompactionCallbacks callbacks = {
    .acquire_write_lock = SearchDisk_AcquireWriteLock,
    .release_write_lock = SearchDisk_ReleaseWriteLock,
    .update_trie_term = SearchDisk_UpdateTrieTerm,
    .update_scoring_stats = SearchDisk_UpdateScoringStats,
    .ctx = sp,
  };
  return callbacks;
}
