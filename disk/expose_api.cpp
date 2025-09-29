#include "expose_api.h"
#include "doc_table/doc_table_disk_c.h"
#include "inverted_index/inverted_index_api.h"
#include "database_api.h"

#ifdef __cplusplus
extern "C" {
#endif

static void DoNothing(RedisSearchDiskIndexSpec *index) {
}

bool SearchDisk_HasAPI() {
    return true;
}

RedisSearchDiskAPI *SearchDisk_GetAPI() {
    static RedisSearchDiskAPI api = {
        .basic = {
            .open = reinterpret_cast<RedisSearchDisk *(*)(RedisModuleCtx *, const char *)>(DiskDatabase_Create),
            .openIndexSpec = reinterpret_cast<RedisSearchDiskIndexSpec *(*)(RedisSearchDisk *, const char *, DocumentType)>(DiskDatabase_OpenIndex),
            .closeIndexSpec = reinterpret_cast<void (*)(RedisSearchDiskIndexSpec *)>(DoNothing),
            .close = reinterpret_cast<void (*)(RedisSearchDisk *)>(DiskDatabase_Destroy)
        },
        .index = {
            .indexDocument = reinterpret_cast<bool (*)(RedisSearchDiskIndexSpec *, const char *, t_docId, t_fieldMask)>(DiskDatabase_IndexDocument),
            .newTermIterator = reinterpret_cast<QueryIterator *(*)(RedisSearchDiskIndexSpec *, const char *, t_fieldMask, double)>(NewDiskInvertedIndexIterator),
            .newWildcardIterator = reinterpret_cast<QueryIterator *(*)(RedisSearchDiskIndexSpec *, double)>(DocTableDisk_NewQueryIterator)
        },
        .docTable = {
            .putDocument = reinterpret_cast<t_docId (*)(RedisSearchDiskIndexSpec *, const char *, double, uint32_t, uint32_t)>(DocTableDisk_Put),
            .isDocIdDeleted = reinterpret_cast<bool (*)(RedisSearchDiskIndexSpec *, t_docId)>(DocTableDisk_DocIdDeleted),
            .getDocumentMetadata = reinterpret_cast<bool (*)(RedisSearchDiskIndexSpec *, t_docId, RSDocumentMetadata *, AllocateKeyCallback)>(DocTableDisk_GetDmd)
        }
    };
    return &api;
}

#ifdef __cplusplus
}
#endif
