#ifndef RS_TAG_INDEX_H_
#define RS_TAG_INDEX_H_

#include "redismodule.h"
#include "doc_table.h"
#include "document.h"
#include "value.h"
#include "geo_index.h"

typedef enum {
  TagTokenizer_CaseSensitive = 0x01,
  TagTokenizer_TrimSpace = 0x02,
  TagTokenizer_RemoveAccents = 0x04,
} TagTokenizerFlags;

typedef struct {
  char separator;
  TagTokenizerFlags flags;
} TagTokenizeCtx;

typedef struct {
  TrieMap *values;
  TagTokenizeCtx tokCtx;
} TagIndex;

const char *TagIndex_FormatName(const char *namespace, const char *field);
TagIndex *NewTagIndex();
Vector *TagIndex_Preprocess(TagIndex *idx, DocumentField *data);
size_t TagIndex_Index(TagIndex *idx, Vector *values, t_docId docId);
IndexIterator *TagIndex_OpenReader(TagIndex *idx, DocTable *dt, const char *value, size_t len);

#define TAGIDX_CURRENT_VERSION 1
extern RedisModuleType *TagIndexType;
int TagIndex_RegisterType(RedisModuleCtx *ctx);

#endif