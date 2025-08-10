#include <rocksdb/db.h>
#include "disk/database_api.h"
#include "disk/database.h"

#ifdef __cplusplus
extern "C" {
#endif

void DiskDatabase_Delete(RedisModuleCtx *ctx, const char *db_path) {
    rocksdb::Options options;
    rocksdb::Status status = rocksdb::DestroyDB(db_path, options);
    if (!status.ok()) {
        RedisModule_Log(ctx, "error", "Failed to delete database: %s", status.ToString().c_str());
    }
}

DiskDatabase* DiskDatabase_Create(RedisModuleCtx *ctx, const char *db_path) {
    search::disk::Database* db = search::disk::Database::Create(ctx, db_path);
    return reinterpret_cast<DiskDatabase*>(db);
}


void DiskDatabase_Destroy(DiskDatabase *db) {
    delete reinterpret_cast<search::disk::Database*>(db);
}

struct DiskIndex* DiskDatabase_OpenIndex(DiskDatabase *db, const char *indexName, DocumentType docType) {
    search::disk::Database* database = reinterpret_cast<search::disk::Database*>(db);
    if (!database) return nullptr;
    search::disk::Database::Index* index = database->OpenIndex(indexName, docType);
    return reinterpret_cast<DiskIndex*>(index);
}


void DiskDatabase_ListKeys(DiskIndex *handle, DiskDatabase_ListKeysCallback callback, void *ctx) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return;

    search::disk::Column& invertedIndex = index->GetInvertedIndex();
    search::disk::Iterator* it = invertedIndex.template CreateIterator<search::disk::Iterator>();
    if (!it->SeekToFirst()) {
        delete it;
        return;
    }
    do {
        std::optional<std::string> key = it->GetCurrentKey();
        if (key) {
            callback(key->c_str(), ctx);
        }
    } while (it->Next());
    delete it;
}

void DiskDatabase_CompactIndex(DiskIndex *idx) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(idx);
    if (!index) return;

    index->Compact();
}

#ifdef __cplusplus
}
#endif