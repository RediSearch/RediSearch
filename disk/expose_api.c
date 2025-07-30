#include "expose_api.h"
#include "doc_table/doc_table_disk_c.h"
#include "inverted_index/inverted_index_api.h"
#include "database_api.h"

#ifdef __cplusplus
extern "C" {
#endif

static void DoNothing(RedisSearchDiskIndexSpec *index) {
}

RedisSearchDiskAPI *SearchDisk_GetAPI(RedisModuleCtx *ctx) {
    static RedisSearchDiskAPI api = {.basic = {.open = DiskDatabase_Create,
                                              .openIndexSpec = DiskDatabase_OpenIndex,
                                              .closeIndexSpec = DoNothing,
                                              .close = DiskDatabase_Destroy},
                                     .index = {.indexDocument = DiskDatabase_IndexDocument,
                                               .newTermIterator = NewDiskInvertedIndexIterator,
                                               .newWildcardIterator = DocTableDisk_NewIndexIterator},
                                     .docTable = {.putDocument = DocTableDisk_Put,
                                                 .isDocIdDeleted = DocTableDisk_DocIdDeleted,
                                                 .getDocumentMetadata = DocTableDisk_GetDmd}};
    return &api;
}

#ifdef __cplusplus
}
#endif