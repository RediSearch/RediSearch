#include "tag_index.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "rmutil/util.h"
#include <assert.h>

TagIndex *NewTagIndex() {
  TagIndex *idx = rm_new(TagIndex);
  idx->values = NewTrieMap();
  return idx;
}

static inline char *mySep(char sep, char **s, int trimSpace, size_t *toklen) {

  char *orig = *s;
  char *end = orig;
  // trim spaces before start of token
  if (trimSpace) {
    while (isspace(*orig)) orig++;
  }
  char *pos = *s;
  for (; *pos; ++pos) {
    if (*pos == sep) {
      *pos = '\0';
      end = pos;
      *s = (char *)++pos;
      break;
    }
  }

  if (!*pos) {
    end = pos;
    *s = NULL;
  }
  // trim trailing spaces
  if (trimSpace) {
    char *x = end - 1;
    while (isspace(*x) && x >= orig) {
      *x-- = 0;
    }
    if (*x) end = x + 1;
    // consume an all space string
    if (x == orig) end = x;
  }
  *toklen = end - orig;
  return orig;
}

Vector *TagIndex_Preprocess(const TagFieldOptions *opts, const DocumentField *data) {
  size_t sz;
  char *p = (char *)RedisModule_StringPtrLen(data->text, &sz);
  if (!p) return NULL;
  Vector *ret = NewVector(char *, 4);
  p = strndup(p, sz);
  while (p) {
    // get the next token
    size_t toklen;
    char *tok = mySep(opts->separator, &p, 1, &toklen);
    // this means we're at the end
    if (tok == NULL) break;
    if (toklen > 0) {
      Vector_Push(ret, tok);
    }
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
      // printf("Indexing token '%s'\n", tok);
      ret += tagIndex_Put(idx, tok, strlen(tok), docId);
    }
  }

  return ret;
}

IndexIterator *TagIndex_OpenReader(TagIndex *idx, DocTable *dt, const char *value, size_t len) {

  InvertedIndex *iv = TrieMap_Find(idx->values, (char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv) {
    printf("Term '%.*s' not found, iv %p\n", (int)len, value, iv);
    return NULL;
  }

  IndexReader *r = NewTermIndexReader(iv, dt, RS_FIELDMASK_ALL, NULL);
  if (!r) {
    return NULL;
  }
  return NewReadIterator(r);
}

RedisModuleString *TagIndex_FormatName(RedisSearchCtx *sctx, const char *field) {
  return RedisModule_CreateStringPrintf(sctx->redisCtx, "tag:%s/%s", sctx->spec->name, field);
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
  unsigned long long elems = RedisModule_LoadUnsigned(rdb);
  printf("need to load %zd elems\n", elems);
  TagIndex *idx = NewTagIndex();

  while (elems--) {
    size_t slen;
    char *s = RedisModule_LoadStringBuffer(rdb, &slen);
    InvertedIndex *inv = InvertedIndex_RdbLoad(rdb, INVERTED_INDEX_ENCVER);
    assert(inv != NULL);
    TrieMap_Add(idx->values, s, slen, inv, NULL);
  }
  return idx;
}
void TagIndex_RdbSave(RedisModuleIO *rdb, void *value) {
  TagIndex *idx = value;
  RedisModule_SaveUnsigned(rdb, idx->values->cardinality);
  TrieMapIterator *it = TrieMap_Iterate(idx->values, "", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  size_t count = 0;
  while (TrieMapIterator_Next(it, &str, &slen, &ptr)) {
    count++;
    RedisModule_SaveStringBuffer(rdb, str, slen);
    InvertedIndex *inv = ptr;
    InvertedIndex_RdbSave(rdb, inv);
  }
  assert(count == idx->values->cardinality);
  TrieMapIterator_Free(it);
}
void TagIndex_Digest(RedisModuleDigest *digest, void *value) {
}

void TagIndex_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RMUtil_DefaultAofRewrite(aof, key, value);
}

void TagIndex_Free(void *p) {
  TagIndex *idx = p;
  TrieMap_Free(idx->values, InvertedIndex_Free);
  rm_free(idx);
}

size_t TagIndex_MemUsage(const void *value) {
  const TagIndex *idx = value;
  size_t sz = sizeof(*idx);

  TrieMapIterator *it = TrieMap_Iterate(idx->values, "", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  while (TrieMapIterator_Next(it, &str, &slen, &ptr)) {
    sz += slen + InvertedIndex_MemUsage((InvertedIndex *)ptr);
  }
  TrieMapIterator_Free(it);
  return sz;
}

int TagIndex_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = TagIndex_RdbLoad,
                               .rdb_save = TagIndex_RdbSave,
                               .aof_rewrite = TagIndex_AofRewrite,
                               .free = TagIndex_Free,
                               .mem_usage = TagIndex_MemUsage};

  TagIndexType = RedisModule_CreateDataType(ctx, "ft_tagidx", TAGIDX_CURRENT_VERSION, &tm);
  if (TagIndexType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create attribute index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
