#include "tag_index.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"

TagIndex *NewTagIndex() {
  TagIndex *idx = rm_new(TagIndex);
  idx->values = NewTrieMap();
  return idx;
}

static inline char *mySep(char sep, char **s) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if (*pos == sep) {
      *pos = '\0';
      *s = (char *)++pos;
      break;
    }
  }

  if (!*pos) {
    *s = NULL;
  }
  return orig;
}

Vector *TagIndex_Preprocess(TagIndex *idx, DocumentField *data) {
  size_t sz;
  char *p = (char *)RedisModule_StringPtrLen(data->text, &sz);
  if (!p) return NULL;
  Vector *ret = NewVector(char *, 4);
  p = strndup(p, sz);
  while (p) {
    // get the next token
    char *tok = mySep(idx->tokCtx.separator, &p);
    // this means we're at the end
    if (tok == NULL) break;

    Vector_Push(ret, tok);
  }

  return ret;
}

static inline size_t tagIndex_Put(TagIndex *idx, char *value, size_t len, t_docId docId) {

  InvertedIndex *iv = TrieMap_Find(idx->values, value, len);
  if (iv == TRIEMAP_NOTFOUND) {
    iv = NewInvertedIndex(Index_DocIdsOnly, 1);
    TrieMap_Add(idx->values, value, len, iv, NULL);
  }

  IndexEncoder enc = InvertedIndex_GetEncoder(Index_DocIdsOnly);
  RSIndexResult rec = {.type = RSResultType_Virtual, .docId = docId, .offsetsSz = 0, .freq = 0};

  return InvertedIndex_WriteEntryGeneric(iv, enc, docId, &rec);
}

size_t TagIndex_Index(TagIndex *idx, Vector *values, t_docId docId) {

  char *tok;
  size_t ret = 0;
  for (size_t i = 0; i < Vector_Size(values); i++) {
    Vector_Get(values, i, &tok);
    if (tok && *tok != '\0') {
      ret += tagIndex_Put(idx, tok, strlen(tok), docId);
    }
  }

  return ret;
}

IndexIterator *TagIndex_OpenReader(TagIndex *idx, DocTable *dt, const char *value, size_t len) {
  InvertedIndex *iv = TrieMap_Find(idx->values, (char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv) {
    return NULL;
  }

  IndexReader *r = NewTermIndexReader(iv, dt, RS_FIELDMASK_ALL, NULL);
  if (!r) {
    return NULL;
  }
  return NewReadIterator(r);
}

TagIndex *TagIndex_Open(RedisModuleCtx *ctx, RedisModuleString *formattedKey, int openWrite,
                        RedisModuleKey **keyp) {
  RedisModuleKey *key_s = NULL;
  if (!keyp) {
    keyp = &key_s;
  }

  *keyp = RedisModule_OpenKey(ctx, formattedKey,
                              REDISMODULE_READ | (openWrite ? REDISMODULE_WRITE : 0));

  int type = RedisModule_KeyType(*keyp);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(*keyp) != TagIndexType) {
    return NULL;
  }

  /* Create an empty value object if the key is currently empty. */
  TagIndex *ret = NULL;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    if (openWrite) {
      ret = NewTagIndex();
      RedisModule_ModuleTypeSetValue((*keyp), TagIndexType, ret);
    }
  } else {
    ret = RedisModule_ModuleTypeGetValue(*keyp);
  }
  return ret;
}

RedisModuleType *TagIndexType;

void *TagIndex_RdbLoad(RedisModuleIO *rdb, int encver) {
  return NULL;
}
void TagIndex_RdbSave(RedisModuleIO *rdb, void *value) {
}
void TagIndex_Digest(RedisModuleDigest *digest, void *value) {
}

void TagIndex_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}

void TagIndex_Free(void *p) {
}

size_t TagIndex_MemUsage(const void *value) {
}

int TagIndex_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = TagIndex_RdbLoad,
                               .rdb_save = TagIndex_RdbSave,
                               .aof_rewrite = TagIndex_AofRewrite,
                               .free = TagIndex_Free,
                               .mem_usage = TagIndex_MemUsage};

  TagIndexType = RedisModule_CreateDataType(ctx, "ft_attidx", TAGIDX_CURRENT_VERSION, &tm);
  if (TagIndexType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create attribute index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
