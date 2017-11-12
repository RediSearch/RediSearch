#include "tag_index.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "rmutil/util.h"
#include <assert.h>

/* See tag_index.h for documentation  */
TagIndex *NewTagIndex() {
  TagIndex *idx = rm_new(TagIndex);
  idx->values = NewTrieMap();
  return idx;
}

/* read the next token from the string */
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

char *strtolower(char *str);

/* Preprocess a document tag field, returning a vector of all tags split from the content */
Vector *TagIndex_Preprocess(const TagFieldOptions *opts, const DocumentField *data) {
  size_t sz;
  char *p = (char *)RedisModule_StringPtrLen(data->text, &sz);
  if (!p) return NULL;
  Vector *ret = NewVector(char *, 4);
  char *pp = p = strndup(p, sz);
  while (p) {
    // get the next token
    size_t toklen;
    char *tok = mySep(opts->separator, &p, 1, &toklen);
    // this means we're at the end
    if (tok == NULL) break;
    if (toklen > 0) {
      // lowercase the string (TODO: non latin lowercase)
      if (!(opts->flags & TagField_CaseSensitive)) {
        tok = strtolower(tok);
      }
      tok = strndup(tok, toklen);
      Vector_Push(ret, tok);
    }
  }
  free(pp);
  return ret;
}

/* Ecode a single docId into a specific tag value */
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

/* Index a vector of pre-processed tags for a docId */
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

struct TagReaderCtx {
  TagIndex *idx;
  IndexIterator *it;
};

static void TagReader_OnReopen(RedisModuleKey *k, void *privdata) {

  struct TagReaderCtx *ctx = privdata;

  // If the key has been deleted we'll get a NULL here, so we just mark ourselves as EOF
  if (k == NULL || RedisModule_ModuleTypeGetType(k) != TagIndexType) {
    ctx->it->Abort(ctx->it->ctx);
    return;
  }

  // If the key is valid, we just reset the reader's buffer reader to the current block pointer
  ctx->idx = RedisModule_ModuleTypeGetValue(k);
  IndexReader *ir = ctx->it->ctx;
  ir->idx = ir->idx;

  // the gc marker tells us if there is a chance the keys has undergone GC while we were asleep
  if (ir->gcMarker == ir->idx->gcMarker) {
    // no GC - we just go to the same offset we were at
    size_t offset = ir->br.pos;
    ir->br = NewBufferReader(ir->idx->blocks[ir->currentBlock].data);
    ir->br.pos = offset;
  } else {
    // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
    // anymore. This means that we need to seek to last docId we were at

    // reset the state of the reader
    t_docId lastId = ir->lastId;
    ir->br = NewBufferReader(ir->idx->blocks[ir->currentBlock].data);
    ir->lastId = 0;

    // seek to the previous last id
    RSIndexResult *dummy = NULL;
    IR_SkipTo(ir, lastId, &dummy);
  }
}

/* Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
 * Returns NULL if there is no such tag in the index */
IndexIterator *TagIndex_OpenReader(TagIndex *idx, DocTable *dt, const char *value, size_t len,
                                   ConcurrentSearchCtx *csx, RedisModuleKey *k,
                                   RedisModuleString *keyName) {

  InvertedIndex *iv = TrieMap_Find(idx->values, (char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv) {
    return NULL;
  }

  IndexReader *r = NewTermIndexReader(iv, dt, RS_FIELDMASK_ALL, NULL);
  if (!r) {
    return NULL;
  }
  IndexIterator *it = NewReadIterator(r);

  // register the on reopen function
  if (csx) {
    struct TagReaderCtx *tc = malloc(sizeof(*tc));
    tc->idx = idx;
    tc->it = it;
    ConcurrentSearch_AddKey(csx, k, REDISMODULE_READ, keyName, TagReader_OnReopen, tc, free,
                            ConcurrentKey_SharedKey | ConcurrentKey_SharedKeyString);
  }

  return it;
}

/* Format the key name for a tag index */
RedisModuleString *TagIndex_FormatName(RedisSearchCtx *sctx, const char *field) {
  return RedisModule_CreateStringPrintf(sctx->redisCtx, TAG_INDEX_KEY_FMT, sctx->spec->name, field);
}

/* Open the tag index in redis */
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

/* Serialize all the tags in the index to the redis client */
void TagIndex_SerializeValues(TagIndex *idx, RedisModuleCtx *ctx) {
  TrieMapIterator *it = TrieMap_Iterate(idx->values, "", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  long long count = 0;
  while (TrieMapIterator_Next(it, &str, &slen, &ptr)) {
    ++count;
    RedisModule_ReplyWithStringBuffer(ctx, str, slen);
  }

  RedisModule_ReplySetArrayLength(ctx, count);

  TrieMapIterator_Free(it);
}

RedisModuleType *TagIndexType;

void *TagIndex_RdbLoad(RedisModuleIO *rdb, int encver) {
  unsigned long long elems = RedisModule_LoadUnsigned(rdb);
  TagIndex *idx = NewTagIndex();

  while (elems--) {
    size_t slen;
    char *s = RedisModule_LoadStringBuffer(rdb, &slen);
    InvertedIndex *inv = InvertedIndex_RdbLoad(rdb, INVERTED_INDEX_ENCVER);
    assert(inv != NULL);
    TrieMap_Add(idx->values, s, slen, inv, NULL);
    rm_free(s);
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
