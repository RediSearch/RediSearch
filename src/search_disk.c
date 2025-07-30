#include "search_disk.h"

RedisSearchDiskAPI *disk = NULL;
RedisSearchDisk *db = NULL;

bool SearchDisk_Initialize(RedisModuleCtx *ctx) {
  if (!SearchDisk_GetAPI) {
    return false;
  }
  disk = SearchDisk_GetAPI(ctx);
  if (!disk) {
    RedisModule_Log(ctx, "warning", "Could not find RediSearch_Disk API");
    return false;
  }
  db = disk->basic.open(ctx, "redisearch");
  return db != NULL;
}

void SearchDisk_Close() {
  if (disk && db) {
    disk->basic.close(db);
    db = NULL;
  }
}

// Basic API wrappers
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(const char *indexName, DocumentType type) {
    if (!db) {
        return NULL;
    }
    return disk->basic.openIndexSpec(db, indexName, type);
}

void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index) {
    if (disk && index) {
        disk->basic.closeIndexSpec(index);
    }
}

// Index API wrappers
bool SearchDisk_IndexDocument(RedisSearchDiskIndexSpec *index, const char *term, t_docId docId, t_fieldMask fieldMask) {
    if (!disk || !index) {
        return false;
    }
    return disk->index.indexDocument(index, term, docId, fieldMask);
}

IndexIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, const char *term, double weight) {
    if (!disk || !index || !term) {
        return NULL;
    }
    return disk->index.newTermIterator(index, term, weight);
}

IndexIterator* SearchDisk_NewWildcardIterator(RedisSearchDiskIndexSpec *index) {
    if (!disk || !index) {
        return NULL;
    }
    return disk->index.newWildcardIterator(index);
}

t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, double score, uint32_t flags, uint32_t maxFreq) {
    if (!disk || !handle) {
        return 0;
    }
    return disk->docTable.putDocument(handle, key, score, flags, maxFreq);
}

bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd) {
    if (!disk || !handle) {
        return false;
    }
    return disk->docTable.getDocumentMetadata(handle, docId, dmd, &sdsnewlen);
}

bool SearchDisk_DocIdDeleted(RedisSearchDiskIndexSpec *handle, t_docId docId) {
    if (!disk || !handle) {
        return true;
    }
    return disk->docTable.isDocIdDeleted(handle, docId);
}
