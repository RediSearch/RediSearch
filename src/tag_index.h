#ifndef RS_TAG_INDEX_H_
#define RS_TAG_INDEX_H_

#include "redismodule.h"
#include "doc_table.h"
#include "document.h"
#include "value.h"
#include "geo_index.h"

typedef struct { TrieMap *values; } TagIndex;

RedisModuleString *TagIndex_FormatName(RedisSearchCtx *sctx, const char *field);
TagIndex *NewTagIndex();
Vector *TagIndex_Preprocess(const TagFieldOptions *opts, const DocumentField *data);
size_t TagIndex_Index(TagIndex *idx, Vector *values, t_docId docId);
IndexIterator *TagIndex_OpenReader(TagIndex *idx, DocTable *dt, const char *value, size_t len);
TagIndex *TagIndex_Open(RedisModuleCtx *ctx, RedisModuleString *formattedKey, int openWrite,
                        RedisModuleKey **keyp);
#define TAGIDX_CURRENT_VERSION 1
extern RedisModuleType *TagIndexType;
int TagIndex_RegisterType(RedisModuleCtx *ctx);

#endif