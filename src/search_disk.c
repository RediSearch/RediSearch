#include "search_disk.h"
#include "config.h"
RedisSearchDiskAPI *disk = NULL;
RedisSearchDisk *disk_db = NULL;
RedisSearchDiskMemObject *disk_mem_obj = NULL;

/* declaration of the type for redis registration. */
RedisModuleType *SearchDiskType;

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
  if (disk) {
    // Already initialized
    return true;
  }
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

  disk_db = disk->basic.open(ctx, "redisearch", disk_mem_obj); // Memory Object is empty by now
  return disk_db != NULL;
}

void SearchDisk_ReOpen(RedisModuleCtx *ctx) {
  RS_ASSERT(disk);
  if (disk_db) {
    disk->basic.close(disk_db);
    disk_db = NULL;
  }
  disk_db = disk->basic.open(ctx, "redisearch", disk_mem_obj);
}

void SearchDisk_Close() {
  if (disk && disk_db) {
    disk->basic.close(disk_db);
    disk_db = NULL;
  }
  if (disk_mem_obj) {
    disk->memObject.free(disk_mem_obj);
    disk_mem_obj = NULL;
  }
}

static void SearchDisk_Close_Dummy(void *value) {
  // Do nothing
}

void *SearchDisk_LoadFromRDB(RedisModuleIO *rdb, int encver) {
  if (!disk) return NULL;
  disk_mem_obj = disk->memObject.fromRDB(rdb);
  return disk_mem_obj;
}

void SearchDisk_SaveToRDB(RedisModuleIO *rdb, void *value) {
  if (!disk) return;
  disk->memObject.toRDB((RedisSearchDiskMemObject*)value, rdb);
}

int SearchDisk_AuxLoadFromRDB(RedisModuleIO *rdb, int encver, int when) {
  if (!disk) return REDISMODULE_ERR;
  disk_mem_obj = disk->memObject.fromRDB(rdb);
  if (!disk_mem_obj) return REDISMODULE_ERR;
  return REDISMODULE_OK;
}

void SearchDisk_AuxSaveToRDB(RedisModuleIO *rdb, int when) {
  if (!disk || !disk_mem_obj) return;
  disk->memObject.toRDB(disk_mem_obj, rdb);
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

t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, double score, uint32_t flags, uint32_t maxFreq) {
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

int SearchDisk_RegisterType(RedisModuleCtx *ctx) {
  RS_ASSERT(disk);
  // rdb_load and aux_load are equivalent because disk_db is a singleton, so no multiple instances of this type are created
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
    .rdb_load = SearchDisk_LoadFromRDB,
    .rdb_save = SearchDisk_SaveToRDB,
    .aux_load = SearchDisk_AuxLoadFromRDB,
    .aux_save2 = SearchDisk_AuxSaveToRDB,
    .free = SearchDisk_Close_Dummy,
    .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB, // Not strictly necessary, but we want to be called before RDB
  };
  SearchDiskType = RedisModule_CreateDataType(ctx, "ft_search_disk0", TRIE_ENCVER_CURRENT, &tm);
  if (SearchDiskType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
