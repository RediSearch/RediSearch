#ifndef __DOC_TABLE_H__
#define __DOC_TABLE_H__
#include <stdlib.h>
#include "redismodule.h"
#include "types.h"

#pragma pack(1)
typedef struct {
    float score;
    u_short flags;
} DocumentMetadata;
#pragma pack()


typedef struct {
    RedisModuleCtx *ctx;
    RedisModuleKey *key;    
} DocTable;


#define DOCTABLE_KEY "__redis_dmd__"
#define DOCTABLE_DOCID_KEY_FMT "d:%d"
int InitDocTable(RedisModuleCtx *ctx, DocTable *t); 
int DocTable_GetMetadata(DocTable *t, t_docId docId, DocumentMetadata *md);
int DocTable_PutDocument(DocTable *t, t_docId docId, double score, u_short flags);

 
#endif