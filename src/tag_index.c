#include "tag_index.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "rmutil/util.h"
#include "util/misc.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define TAGIDX_CURRENT_VERSION 1

static uint32_t tagUniqueId = 0;

// Tags are limited to 4096 each
#define MAX_TAG_LEN 0x1000

//---------------------------------------------------------------------------------------------

// See tag_index.h for documentation
TagIndex::TagIndex() {
  values = new TrieMap();
  uniqueId = tagUniqueId++;
}

//---------------------------------------------------------------------------------------------

// read the next token from the string
char *TagIndex::SepString(char sep, char **s, size_t *toklen) {
  char *start = *s;

  // find the first none space and none separator char
  while (*start && (isspace(*start) || *start == sep)) {
    start++;
  }

  if (*start == '\0') {
    // no token found
    *s = start;
    return NULL;
  }

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

//---------------------------------------------------------------------------------------------

char *strtolower(char *str);

// Preprocess a document tag field, returning a vector of all tags split from the content
char **TagIndex::Preprocess(char sep, TagFieldFlags flags, const DocumentField *data) {
  size_t sz;
  char *p = (char *)RedisModule_StringPtrLen(data->text, &sz);
  if (!p || sz == 0) return NULL;
  char **ret = array_new(char *, 4);
  char *pp = p = rm_strndup(p, sz);
  while (p) {
    // get the next token
    size_t toklen;
    char *tok = SepString(sep, &p, &toklen);
    // this means we're at the end
    if (tok == NULL) break;
    if (toklen > 0) {
      // lowercase the string (TODO: non latin lowercase)
      if (!(flags & TagField_CaseSensitive)) {
        tok = strtolower(tok);
      }
      tok = rm_strndup(tok, MIN(toklen, MAX_TAG_LEN));
      ret = array_append(ret, tok);
    }
  }
  rm_free(pp);
  return ret;
}

//---------------------------------------------------------------------------------------------

struct InvertedIndex *TagIndex::OpenIndex(const char *value, size_t len, int create) {
  InvertedIndex *iv = values->Find((char *)value, len);
  if (iv == TRIEMAP_NOTFOUND) {
    if (create) {
      iv = new InvertedIndex(Index_DocIdsOnly, 1);
      values->Add((char *)value, len, iv, NULL);
    }
  }
  return iv;
}

//---------------------------------------------------------------------------------------------

// Ecode a single docId into a specific tag value
size_t TagIndex::Put(const char *value, size_t len, t_docId docId) {
  IndexEncoder enc = InvertedIndex::GetEncoder(Index_DocIdsOnly);
  const IndexResult rec(RSResultType_Virtual, docId);
  InvertedIndex *iv = OpenIndex(value, len, 1);
  return iv->WriteEntryGeneric(enc, docId, rec);
}

//---------------------------------------------------------------------------------------------

// Index a vector of pre-processed tags for a docId
size_t TagIndex::Index(const Tags &tags, size_t n, t_docId docId) {
  if (!values) return 0;
  size_t ret = 0;
  for (size_t ii = 0; ii < n; ++ii) {
    const char *tok = tags[ii];
    if (tok && *tok != '\0') {
      ret += Put(tok, strlen(tok), docId);
    }
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

struct TagConc {
  ~TagConc() {
    if (its) {
      array_free(its);
    }
  }

  arrayof(IndexIterator*) its;
  uint32_t uid;
};

//---------------------------------------------------------------------------------------------

static void TagReader_OnReopen(RedisModuleKey *k, void *privdata) {
  TagConc *ctx = privdata;
  IndexIterator **its = ctx->its;
  TagIndex *idx = NULL;
  size_t nits = array_len(its);
  // If the key has been deleted we'll get a NULL here, so we just mark ourselves as EOF
  if (k == NULL || RedisModule_ModuleTypeGetType(k) != TagIndexType ||
      (idx = RedisModule_ModuleTypeGetValue(k))->uniqueId != ctx->uid) {
    for (size_t ii = 0; ii < nits; ++ii) {
      its[ii]->Abort();
    }
    return;
  }

  // If the key is valid, we just reset the reader's buffer reader to the current block pointer
  for (size_t ii = 0; ii < nits; ++ii) {
    IndexReader *ir = its[ii]->ir;

    // the gc marker tells us if there is a chance the keys has undergone GC while we were asleep
    if (ir->gcMarker == ir->idx->gcMarker) {
      // no GC - we just go to the same offset we were at
      size_t offset = ir->br.pos;
      ir->br.Set(&ir->idx->blocks[ir->currentBlock].buf, offset);
    } else {
      // if there has been a GC cycle on this key while we were asleep, the offset might not be
      // valid anymore. This means that we need to seek to last docId we were at

      // reset the state of the reader
      t_docId lastId = ir->lastId;
      ir->currentBlock = 0;
      ir->br.Set(&ir->idx->blocks[ir->currentBlock].buf);
      ir->lastId = 0;

      // seek to the previous last id
      IndexResult *dummy = NULL;
      ir->SkipTo(lastId, &dummy);
    }
  }
}

//---------------------------------------------------------------------------------------------

void TagIndex::RegisterConcurrentIterators(ConcurrentSearchCtx *conc,
    RedisModuleKey *key, RedisModuleString *keyname, array_t *iters) {
  // TagConc *tctx = rm_calloc(1, sizeof(*tctx));
  // tctx->uid = uniqueId;
  // tctx->its = (IndexIterator **)iters;
  ConcurrentKey *concKey(key, keyname, REDISMODULE_READ);
  conc->AddKey(concKey);
  // conc->AddKey(key, REDISMODULE_READ, keyname, TagReader_OnReopen, tctx, concCtxFree);
}

//---------------------------------------------------------------------------------------------

// Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
// Returns NULL if there is no such tag in the index.

IndexIterator *TagIndex::OpenReader(IndexSpec *sp, const char *value, size_t len, double weight) {
  InvertedIndex *iv = values->Find((char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv || iv->numDocs == 0) {
    return NULL;
  }

  RSToken tok(value, len);
  RSQueryTerm *t = new RSQueryTerm(tok, 0);
  IndexReader *r = new TermIndexReader(iv, sp, RS_FIELDMASK_ALL, t, weight);
  return r->NewReadIterator();
}

//---------------------------------------------------------------------------------------------

#define TAG_INDEX_KEY_FMT "tag:%s/%s"

// Format the key name for a tag index
static RedisModuleString *TagIndex::FormatName(RedisSearchCtx *sctx, const char *field) {
  return RedisModule_CreateStringPrintf(sctx->redisCtx, TAG_INDEX_KEY_FMT, sctx->spec->name, field);
}

//---------------------------------------------------------------------------------------------

static TagIndex *openTagKeyDict(RedisSearchCtx *ctx, RedisModuleString *key, int openWrite) {
  if (ctx->spec->keysDict.contains(key)) {
    BaseIndex *index = ctx->spec->keysDict[key];
    try {
      TagIndex *val = dynamic_cast<TagIndex *>(index);
      return val;
    } catch (std::bad_cast) {
      throw Error("error: invalid index type...");
    }
  }

  if (!openWrite) {
    return NULL;
  }

  TagIndex *val = new TagIndex();
  ctx->spec->keysDict.insert({key, val});
  return val;
}

//---------------------------------------------------------------------------------------------

// Open the tag index in redis
static TagIndex *TagIndex::Open(RedisSearchCtx *sctx, RedisModuleString *formattedKey, int openWrite,
                                RedisModuleKey **keyp) {
  TagIndex *idx = NULL;
  if (!sctx->spec->keysDict.empty()) {
    RedisModuleKey *key_s = NULL;
    if (!keyp) {
      keyp = &key_s;
    }

    *keyp = RedisModule_OpenKey(sctx->redisCtx, formattedKey,
                                REDISMODULE_READ | (openWrite ? REDISMODULE_WRITE : 0));

    int type = RedisModule_KeyType(*keyp);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(*keyp) != TagIndexType) {
      return NULL;
    }

    // Create an empty value object if the key is currently empty
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
      if (openWrite) {
        idx = new TagIndex();
        RedisModule_ModuleTypeSetValue(*keyp, TagIndexType, idx);
      }
    } else {
      idx = RedisModule_ModuleTypeGetValue(*keyp);
    }
  } else {
    idx = openTagKeyDict(sctx, formattedKey, openWrite);
  }

  return idx;
}

//---------------------------------------------------------------------------------------------

// Serialize all the tags in the index to the redis client
void TagIndex::SerializeValues(RedisModuleCtx *ctx) {
  TrieMapIterator *it = values->Iterate("", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  long long count = 0;
  while (it->Next(&str, &slen, &ptr)) {
    ++count;
    RedisModule_ReplyWithStringBuffer(ctx, str, slen);
  }

  RedisModule_ReplySetArrayLength(ctx, count);
}

///////////////////////////////////////////////////////////////////////////////////////////////

RedisModuleType *TagIndexType;

void *TagIndex_RdbLoad(RedisModuleIO *rdb, int encver) {
  unsigned long long elems = RedisModule_LoadUnsigned(rdb);
  TagIndex *idx = new TagIndex();

  while (elems--) {
    size_t slen;
    char *s = RedisModule_LoadStringBuffer(rdb, &slen);
    InvertedIndex *inv = InvertedIndex_RdbLoad(rdb, INVERTED_INDEX_ENCVER);
    RS_LOG_ASSERT(inv, "loading inverted index from rdb failed");
    idx->values->Add(s, MIN(slen, MAX_TAG_LEN), inv, NULL);
    RedisModule_Free(s);
  }
  return idx;
}

//---------------------------------------------------------------------------------------------

void TagIndex_RdbSave(RedisModuleIO *rdb, void *value) {
  TagIndex *idx = value;
  RedisModule_SaveUnsigned(rdb, idx->values->cardinality);
  TrieMapIterator *it = idx->values->Iterate("", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  size_t count = 0;
  while (it->Next(&str, &slen, &ptr)) {
    count++;
    RedisModule_SaveStringBuffer(rdb, str, slen);
    InvertedIndex *inv = ptr;
    InvertedIndex_RdbSave(rdb, inv);
  }
  RS_LOG_ASSERT(count == idx->values->cardinality, "not all inverted indexes save to rdb");
}

//---------------------------------------------------------------------------------------------

void TagIndex_Free(void *p) {
  TagIndex *idx = p;
  delete idx->values; // TrieMap_Free(idx->values, InvertedIndex_Free);
  rm_free(idx);
}

//---------------------------------------------------------------------------------------------

size_t TagIndex_MemUsage(const void *value) {
  const TagIndex *idx = value;
  size_t sz = sizeof(*idx);

  TrieMapIterator *it = idx->values->Iterate("", 0);

  char *str;
  tm_len_t slen;
  void *ptr;
  while (it->Next(&str, &slen, &ptr)) {
    sz += slen + InvertedIndex_MemUsage((InvertedIndex *)ptr);
  }
  return sz;
}

//---------------------------------------------------------------------------------------------

int TagIndex_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {version: REDISMODULE_TYPE_METHOD_VERSION,
                               rdb_load: TagIndex_RdbLoad,
                               rdb_save: TagIndex_RdbSave,
                               aof_rewrite: GenericAofRewrite_DisabledHandler,
                               free: TagIndex_Free,
                               mem_usage: TagIndex_MemUsage};

  TagIndexType = RedisModule_CreateDataType(ctx, "ft_tagidx", TAGIDX_CURRENT_VERSION, &tm);
  if (TagIndexType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create attribute index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
