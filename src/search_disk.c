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
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(const char *indexName, DocumentType type) {
    RS_ASSERT(disk_db);
    return disk->basic.openIndexSpec(disk_db, indexName, type);
}

void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index) {
    RS_ASSERT(index);
    disk->basic.closeIndexSpec(index);
}

// Index API wrappers
bool SearchDisk_IndexDocument(RedisSearchDiskIndexSpec *index, const char *term, t_docId docId, t_fieldMask fieldMask) {
    RS_ASSERT(disk && index);
    return disk->index.indexDocument(index, term, docId, fieldMask);
}

QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, const char *term, t_fieldMask fieldMask, double weight) {
    RS_ASSERT(disk && index && term);
    return disk->index.newTermIterator(index, term, fieldMask, weight);
}

QueryIterator* SearchDisk_NewWildcardIterator(RedisSearchDiskIndexSpec *index, double weight) {
    RS_ASSERT(disk && index);
    return disk->index.newWildcardIterator(index, weight);
}

t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, float score, uint32_t flags, uint32_t maxFreq) {
    RS_ASSERT(disk && handle);
    return disk->docTable.putDocument(handle, key, score, flags, maxFreq);
}

bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd) {
    RS_ASSERT(disk && handle);
    return disk->docTable.getDocumentMetadata(handle, docId, dmd, &sdsnewlen);
}

bool SearchDisk_DocIdDeleted(RedisSearchDiskIndexSpec *handle, t_docId docId) {
    RS_ASSERT(disk && handle);
    return disk->docTable.isDocIdDeleted(handle, docId);
}

bool SearchDisk_IsEnabled(RedisModuleCtx *ctx) {
  bool isFlex = false;
  char *isFlexStr = getRedisConfigValue(ctx, "bigredis-enabled");
  if (isFlexStr && !strcasecmp(isFlexStr, "yes")) {
    isFlex = true;
  } // Default is false, so nothing to change in that case.
  rm_free(isFlexStr);
  return isFlex;
}
