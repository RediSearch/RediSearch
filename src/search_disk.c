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

  disk_db = disk->basic.open(ctx, "redisearch");
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
    RS_ASSERT(index);
    disk->basic.closeIndexSpec(index);
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
bool SearchDisk_IndexDocument(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask) {
    RS_ASSERT(disk && index);
    return disk->index.indexDocument(index, term, termLen, docId, fieldMask);
}

QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_fieldMask fieldMask, double weight) {
    RS_ASSERT(disk && index && term);
    return disk->index.newTermIterator(index, term, termLen, fieldMask, weight);
}

QueryIterator* SearchDisk_NewWildcardIterator(RedisSearchDiskIndexSpec *index, double weight) {
    RS_ASSERT(disk && index);
    return disk->index.newWildcardIterator(index, weight);
}

t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t docLen, uint32_t *oldLen, t_expirationTimePoint documentTtl) {
    RS_ASSERT(disk && handle);
    return disk->docTable.putDocument(handle, key, keyLen, score, flags, maxTermFreq, docLen, oldLen, documentTtl);
}

bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd) {
    RS_ASSERT(disk && handle);
    return disk->docTable.getDocumentMetadata(handle, docId, dmd, &sdsnewlen);
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

RedisSearchDiskAsyncReadPool *SearchDisk_CreateAsyncReadPool(RedisSearchDiskIndexSpec *handle, uint16_t max_concurrent) {
    RS_ASSERT(disk && handle);
    return disk->docTable.createAsyncReadPool(handle, max_concurrent);
}

bool SearchDisk_AddAsyncRead(RedisSearchDiskAsyncReadPool *pool, t_docId docId, uint64_t user_data) {
    RS_ASSERT(disk && pool);
    return disk->docTable.addAsyncRead(pool, docId, user_data);
}

AsyncPollResult SearchDisk_PollAsyncReads(RedisSearchDiskAsyncReadPool *pool, uint32_t timeout_ms, AsyncReadResult *results, uint16_t results_capacity) {
    RS_ASSERT(disk && pool);
    return disk->docTable.pollAsyncReads(pool, timeout_ms, results, results_capacity, &sdsnewlen);
}

void SearchDisk_FreeAsyncReadPool(RedisSearchDiskAsyncReadPool *pool) {
    RS_ASSERT(disk);
    if (pool) {
        disk->docTable.freeAsyncReadPool(pool);
    }
}

bool SearchDisk_IsAsyncIOSupported() {
    RS_ASSERT(disk);
    // Check both the global flag and the underlying disk support
    return asyncIOEnabled && disk->basic.isAsyncIOSupported(disk_db);
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

bool SearchDisk_CollectDocTableMetrics(RedisSearchDiskIndexSpec* index, DiskColumnFamilyMetrics* metrics) {
  RS_ASSERT(disk && index && metrics);
  return disk->metrics.collectDocTableMetrics(index, metrics);
}

bool SearchDisk_CollectTextInvertedIndexMetrics(RedisSearchDiskIndexSpec* index, DiskColumnFamilyMetrics* metrics) {
  RS_ASSERT(index && metrics);
  return disk->metrics.collectTextInvertedIndexMetrics(index, metrics);
}
