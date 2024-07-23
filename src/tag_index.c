/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "tag_index.h"
#include "suffix.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "rmutil/util.h"
#include "util/misc.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"
#include "resp3.h"

extern RedisModuleCtx *RSDummyContext;

static uint32_t tagUniqueId = 0;

// Tags are limited to 4096 each
#define MAX_TAG_LEN 0x1000
/* See tag_index.h for documentation  */
TagIndex *NewTagIndex() {
  TagIndex *idx = rm_new(TagIndex);
  idx->values = NewTrieMap();
  idx->uniqueId = tagUniqueId++;
  idx->suffix = NULL;
  return idx;
}

/* read the next token from the string */
char *TagIndex_SepString(char sep, char **s, size_t *toklen, bool indexEmpty) {

  char *start = *s;

  if (!indexEmpty) {
    // find the first none space and none separator char
    while (*start && (isspace(*start) || *start == sep)) {
      start++;
    }
  } else {
    // We wish to index empty strings as well as non-empty strings, while
    // trimming the spaces if found.
    bool found_space = isspace(*start);
    while (isspace(*start)) {
      start++;
    }

    // If we found an empty value, and we wish to index it, return it.
    if (*start == sep) {
      *s = ++start;
      return "";
    } else if (*start == '\0' && found_space) {
      *s = start;
      return "";
    }
  }

  if (*start == '\0') {
    // Done
    *s = start;
    return NULL;
  }

  // Non-empty term
  char *end = start;
  char *lastChar = start;
  for (; *end; ++end) {
    if (*end == sep) {
      end++;
      break;
    }
    if (!isspace(*end)) {
      lastChar = end;
    }
  }

  *(lastChar + 1) = '\0';
  *s = end;

  *toklen = lastChar - start + 1;
  return start;
}

static int tokenizeTagString(const char *str, const FieldSpec *fs, char ***resArray) {
  char sep = fs->tagOpts.tagSep;
  TagFieldFlags flags = fs->tagOpts.tagFlags;
  bool indexEmpty = FieldSpec_IndexesEmpty(fs);

  if (sep == TAG_FIELD_DEFAULT_JSON_SEP) {
    char *tok = rm_strdup(str);
    if (!(flags & TagField_CaseSensitive)) { // check case sensitive
      tok = strtolower(tok);
    }
    array_append(*resArray, tok);
    return REDISMODULE_OK;
  }

  char *tok;
  char *p;
  char *pp = p = rm_strdup(str);
  uint len = strlen(p);
  bool last_is_sep = (len > 0) && (*(p + len - 1) == sep);
  while (p) {
    // get the next token
    size_t toklen = 0;
    tok = TagIndex_SepString(sep, &p, &toklen, indexEmpty);

    if (tok) {
      // lowercase the string (TODO: non latin lowercase)
      if (!(flags & TagField_CaseSensitive)) { // check case sensitive
        tok = strtolower(tok);
      }
      tok = rm_strndup(tok, MIN(toklen, MAX_TAG_LEN));
      array_append(*resArray, tok);
    } else {
      break;
    }
  }

  // If the field indexes empty fields, index the case of an empty field, or a
  // field that ends with a separator as well.
  if (indexEmpty) {
    if (p == pp || last_is_sep)
    tok = rm_strdup("");
    array_append(*resArray, tok);
  }

  rm_free(pp);
  return REDISMODULE_OK;
}

int TagIndex_Preprocess(const FieldSpec *fs, const DocumentField *data, FieldIndexerData *fdata) {
  arrayof(char*) arr = array_new(char *, 4);
  const char *str;
  int ret = 1;
  switch (data->unionType) {
  case FLD_VAR_T_RMS:
    str = (char *)RedisModule_StringPtrLen(data->text, NULL);
    tokenizeTagString(str, fs, &arr);
    break;
  case FLD_VAR_T_CSTR:
    tokenizeTagString(data->strval, fs, &arr);
    break;
  case FLD_VAR_T_ARRAY:
    for (int i = 0; i < data->arrayLen; i++) {
      tokenizeTagString(data->multiVal[i], fs, &arr);
    }
    break;
  case FLD_VAR_T_NULL:
    fdata->isNull = 1;
    ret = 0;
    break;
  case FLD_VAR_T_GEO:
  case FLD_VAR_T_NUM:
  case FLD_VAR_T_BLOB_ARRAY:
  case FLD_VAR_T_GEOMETRY:
    RS_LOG_ASSERT(0, "nope")
  }
  fdata->tags = arr;
  return ret;
}

struct InvertedIndex *TagIndex_OpenIndex(TagIndex *idx, const char *value,
                                          size_t len, int create, size_t *sz) {
  *sz = 0;
  InvertedIndex *iv = TrieMap_Find(idx->values, value, len);
  if (iv == TRIEMAP_NOTFOUND) {
    if (create) {
      iv = NewInvertedIndex(Index_DocIdsOnly, 1, sz);
      TrieMap_Add(idx->values, value, len, iv, NULL);
    }
  }
  return iv;
}

// Encode a single docId into a specific tag value
// Returns the number of bytes occupied by the encoded entry plus the size of
// the inverted index (if a new inverted index was created)
static inline size_t tagIndex_Put(TagIndex *idx, const char *value, size_t len, t_docId docId) {
  size_t sz;
  IndexEncoder enc = InvertedIndex_GetEncoder(Index_DocIdsOnly);
  RSIndexResult rec = {.type = RSResultType_Virtual, .docId = docId, .offsetsSz = 0, .freq = 0};
  InvertedIndex *iv = TagIndex_OpenIndex(idx, value, len, 1, &sz);
  return InvertedIndex_WriteEntryGeneric(iv, enc, docId, &rec) + sz;
}

/* Index a vector of pre-processed tags for a docId */
size_t TagIndex_Index(TagIndex *idx, const char **values, size_t n, t_docId docId) {
  if (!values) return 0;
  size_t ret = 0;
  for (size_t ii = 0; ii < n; ++ii) {
    const char *tok = values[ii];
    if (tok) {
      ret += tagIndex_Put(idx, tok, strlen(tok), docId);

      if (idx->suffix && (*tok != '\0')) { // add to suffix TrieMap
        addSuffixTrieMap(idx->suffix, tok, strlen(tok));
      }
    }
  }
  return ret;
}

typedef struct {
  TagIndex *idx;
  IndexIterator **its;
} TagConcCtx;

static void TagReader_OnReopen(void *privdata) {
  TagConcCtx *ctx = privdata;
  IndexIterator **its = ctx->its;
  TagIndex *idx = NULL;
  size_t nits = array_len(its);

  // If the key is valid, we just reset the reader's buffer reader to the current block pointer
  for (size_t ii = 0; ii < nits; ++ii) {
    IndexReader *ir = its[ii]->ctx;
    if (ir->record->type == RSResultType_Term) {
      size_t sz;
      // we need to reopen the inverted index to make sure its still valid.
      // the GC might have deleted it by now.
      InvertedIndex *idx = TagIndex_OpenIndex(ctx->idx, ir->record->term.term->str,
                                              ir->record->term.term->len, 0, &sz);
      if (idx == TRIEMAP_NOTFOUND || ir->idx != idx) {
        // the inverted index was collected entirely by GC, lets stop searching.
        // notice, it might be that a new inverted index was created, we will not
        // continue read those results and we are not promise that documents
        // that was added during cursor life will be returned by the cursor.
        IR_Abort(ir);
        return;
      }
    }
    // Use generic `OnReopen` callback for all readers
    IndexReader_OnReopen(ir);
  }
}

static void concCtxFree(void *p) {
  TagConcCtx *tctx = p;
  if (tctx->its) {
    array_free(tctx->its);
  }
  rm_free(p);
}

void TagIndex_RegisterConcurrentIterators(TagIndex *idx, ConcurrentSearchCtx *conc,
                                          array_t *iters) {
  TagConcCtx *tctx = rm_calloc(1, sizeof(*tctx));
  tctx->idx = idx;
  tctx->its = (IndexIterator **)iters;
  ConcurrentSearch_AddKey(conc, TagReader_OnReopen, tctx, concCtxFree);
}

IndexIterator *TagIndex_GetReader(const RedisSearchCtx *sctx, InvertedIndex *iv, const char *value, size_t len,
                                   double weight) {
  RSToken tok = {.str = (char *)value, .len = len};
  RSQueryTerm *t = NewQueryTerm(&tok, 0);
  IndexReader *r = NewTermIndexReaderEx(iv, sctx, RS_FIELDMASK_ALL, t, weight);
  if (!r) {
    return NULL;
  }
  return NewReadIterator(r);
}

/* Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
 * Returns NULL if there is no such tag in the index */
IndexIterator *TagIndex_OpenReader(TagIndex *idx, const RedisSearchCtx *sctx, const char *value, size_t len,
                                   double weight) {

  InvertedIndex *iv = TrieMap_Find(idx->values, (char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv || iv->numDocs == 0) {
    return NULL;
  }
  return TagIndex_GetReader(sctx, iv, value, len, weight);
}

/* Format the key name for a tag index */
RedisModuleString *TagIndex_FormatName(RedisSearchCtx *sctx, const char *field) {
  return RedisModule_CreateStringPrintf(sctx->redisCtx, TAG_INDEX_KEY_FMT, sctx->spec->name, field);
}

static TagIndex *openTagKeyDict(const RedisSearchCtx *ctx, RedisModuleString *key, int openWrite) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, key);
  if (kdv) {
    return kdv->p;
  }
  if (!openWrite) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->p = NewTagIndex();
  kdv->dtor = TagIndex_Free;
  dictAdd(ctx->spec->keysDict, key, kdv);
  return kdv->p;
}

/* Open the tag index */
TagIndex *TagIndex_Open(RedisSearchCtx *sctx, RedisModuleString *formattedKey, int openWrite,
                        RedisModuleKey **keyp) {
  return openTagKeyDict(sctx, formattedKey, openWrite);
}

/* Serialize all the tags in the index to the redis client */
void TagIndex_SerializeValues(TagIndex *idx, RedisModuleCtx *ctx) {
  TrieMapIterator *it = TrieMap_Iterate(idx->values, "", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  RedisModule_ReplyWithSetOrArray(ctx, REDISMODULE_POSTPONED_LEN);
  long long count = 0;
  while (TrieMapIterator_Next(it, &str, &slen, &ptr)) {
    ++count;
    RedisModule_ReplyWithStringBuffer(ctx, str, slen);
  }

  RedisModule_ReplySetSetOrArrayLength(ctx, count);

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
    RS_LOG_ASSERT(inv, "loading inverted index from rdb failed");
    TrieMap_Add(idx->values, s, MIN(slen, MAX_TAG_LEN), inv, NULL);
    RedisModule_Free(s);
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
  RS_LOG_ASSERT(count == idx->values->cardinality, "not all inverted indexes save to rdb");
  TrieMapIterator_Free(it);
}

void TagIndex_Free(void *p) {
  TagIndex *idx = p;
  TrieMap_Free(idx->values, InvertedIndex_Free);
  TrieMap_Free(idx->suffix, suffixTrieMap_freeCallback);
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
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .free = TagIndex_Free,
                               .mem_usage = TagIndex_MemUsage};

  TagIndexType = RedisModule_CreateDataType(ctx, "ft_tagidx", TAGIDX_CURRENT_VERSION, &tm);
  if (TagIndexType == NULL) {
    RedisModule_Log(ctx, "warning", "Could not create attribute index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

size_t TagIndex_GetOverhead(IndexSpec *sp, FieldSpec *fs) {
  size_t overhead = 0;
  TagIndex *idx = NULL;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(RSDummyContext, sp);
  RedisModuleString *keyName = TagIndex_FormatName(&sctx, fs->name);
  idx = TagIndex_Open(&sctx, keyName, 0, NULL);
  RedisModule_FreeString(RSDummyContext, keyName);
  if (idx) {
    overhead = TrieMap_MemUsage(idx->values);     // Values' size are counted in stats.invertedSize
    if (idx->suffix) {
      overhead += TrieMap_MemUsage(idx->suffix);
    }
  }
  return overhead;
}
