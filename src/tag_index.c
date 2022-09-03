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

TagIndex::Tags::Tags(char sep, TagFieldFlags flags, const DocumentField *data) {
  size_t sz;
  char *p = (char *)RedisModule_StringPtrLen(data->text, &sz);
  if (!p || sz == 0) return;
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
	  tags.emplace_back(tok, MIN(toklen, MAX_TAG_LEN));
    }
  }
  rm_free(pp);
}

//---------------------------------------------------------------------------------------------

struct InvertedIndex *TagIndex::OpenIndex(std::string_view value, bool create) {
  InvertedIndex *iv = values->Find(value);
  if (iv == TRIEMAP_NOTFOUND) {
    if (create) {
      iv = new InvertedIndex(Index_DocIdsOnly, 1);
      values->Add(value, iv, NULL);
    }
  }
  return iv;
}

//---------------------------------------------------------------------------------------------

// Ecode a single docId into a specific tag value

size_t TagIndex::Put(std::string_view tok, t_docId docId) {
  IndexEncoder enc = InvertedIndex::GetEncoder(Index_DocIdsOnly);
  VirtualResult rec{docId};
  InvertedIndex *iv = OpenIndex(tok, true);
  return iv->WriteEntryGeneric(enc, docId, rec);
}

//---------------------------------------------------------------------------------------------

// Index a vector of pre-processed tags for a docId
size_t TagIndex::Index(const Tags &tags, t_docId docId) {
  if (!values) return 0;
  size_t ret = 0;
  for (size_t i = 0; i < tags.tags.size(); ++i) {
    std::string_view tok = tags[i];
    if (!tok.empty()) {
      ret += Put(tok, docId);
    }
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

void TagConcKey::Reopen() {
  TagIndex *idx = NULL;
  size_t nits = its.size();
  // If the key has been deleted we'll get a NULL here, so we just mark ourselves as EOF
  if (key == NULL || RedisModule_ModuleTypeGetType(key) != TagIndexType ||
      (idx = RedisModule_ModuleTypeGetValue(key))->uniqueId != uid) {
    for (size_t i = 0; i < nits; ++i) {
      its[i]->Abort();
    }
    return;
  }

  // If the key is valid, we just reset the reader's buffer reader to the current block pointer
  for (size_t i = 0; i < nits; ++i) {
    IndexReader *ir = its[i]->ir;

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

void TagIndex::RegisterConcurrentIterators(ConcurrentSearch *conc, RedisModuleKey *key,
                                           RedisModuleString *keyname, IndexIterators iters) {
  conc->AddKey(TagConcKey(key, keyname, iters));
}

//---------------------------------------------------------------------------------------------

// Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
// Returns NULL if there is no such tag in the index.

IndexIterator *TagIndex::OpenReader(IndexSpec *sp, std::string_view value, double weight) {
  InvertedIndex *iv = values->Find(value);
  if (iv == TRIEMAP_NOTFOUND || !iv || iv->numDocs == 0) {
    return NULL;
  }

  RSToken tok{value};
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
  TrieMapIterator *it = values->Iterate("");

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
    idx->values->Add(std::string_view{s, MIN(slen, MAX_TAG_LEN)}, inv, NULL);
    RedisModule_Free(s);
  }
  return idx;
}

//---------------------------------------------------------------------------------------------

void TagIndex_RdbSave(RedisModuleIO *rdb, void *value) {
  TagIndex *idx = value;
  RedisModule_SaveUnsigned(rdb, idx->values->cardinality);
  TrieMapIterator *it = idx->values->Iterate("");

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

  TrieMapIterator *it = idx->values->Iterate("");

  char *str;
  tm_len_t slen;
  void *ptr;
  while (it->Next(&str, &slen, &ptr)) {
    sz += slen + InvertedIndex_MemUsage((InvertedIndex *)ptr);
  }

  delete it;
  delete idx;

  return sz;
}

//---------------------------------------------------------------------------------------------

int TagIndex_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {version: REDISMODULE_TYPE_METHOD_VERSION,
                               rdb_load: TagIndex_RdbLoad,
                               rdb_save: TagIndex_RdbSave,
                               aof_rewrite: GenericAofRewrite_DisabledHandler,
                               mem_usage: TagIndex_MemUsage,
                               free: TagIndex_Free};

  TagIndexType = RedisModule_CreateDataType(ctx, "ft_tagidx", TAGIDX_CURRENT_VERSION, &tm);
  if (TagIndexType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create attribute index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
