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

t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t docLen, uint32_t *oldLen) {
    RS_ASSERT(disk && handle);
    return disk->docTable.putDocument(handle, key, keyLen, score, flags, maxTermFreq, docLen, oldLen);
}

bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd) {
    RS_ASSERT(disk && handle);
    return disk->docTable.getDocumentMetadata(handle, docId, dmd, &sdsnewlen);
}

t_docId SearchDisk_GetId(RedisSearchDiskIndexSpec *handle, const char *key, size_t len) {
  RS_ASSERT(disk && handle);
  return disk->docTable.getId(handle, key, len);
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
void* SearchDisk_CreateVectorIndex(RedisSearchDiskIndexSpec *index, const struct VecSimHNSWDiskParams *params) {
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
