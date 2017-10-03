#include "attribute_index.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"

AttributeIndex *NewAttributeIndex(const char *namespace, const char *fieldName) {
  AttributeIndex *idx = rm_new(AttributeIndex);
  idx->numFields = 1;
  idx->fields = rm_calloc(1, sizeof(*idx->fields));
  idx->fields[0] = rm_strdup(fieldName);
  idx->values = NewTrieMap();
  return idx;
}

const char *AttributeIndex_Encode(Attribute *attrs, size_t num, size_t *sz) {
  return NULL;
}

static size_t attributeIndex_EncodeNumber(BufferWriter *bw, double num) {
  // TODO: implement me
  return 0;
}

static size_t attributeIndex_EncodeString(BufferWriter *bw, const char *str, size_t len) {
  // TODO: Normalize here
  return Buffer_Write(bw, (void *)str, len);
}

const char *AttributeIndex_EncodeSingle(Attribute *attr, size_t *sz) {
  switch (attr->t) {
    case AttributeType_String:
      return attr->strval;
    default:
      return NULL;
  }
}

static inline char *mySep(char **s) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if (*pos == ',') {
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

Vector *AttributeIndex_Preprocess(AttributeIndex *idx, DocumentField *data) {
  Vector *ret = NewVector(char *, 4);
  size_t sz;
  char *p = (char *)RedisModule_StringPtrLen(data->text, &sz);
  while (p) {
    // get the next token
    char *tok = mySep(&p);
    // this means we're at the end
    if (tok == NULL) break;

    Vector_Push(ret, tok);
  }

  return ret;
}

static inline size_t attributeIndex_Put(AttributeIndex *idx, char *value, t_docId docId) {

  InvertedIndex *iv = TrieMap_Find(idx->values, value, strlen(value));
  if (iv == TRIEMAP_NOTFOUND) {
    iv = NewInvertedIndex(Index_DocIdsOnly, 1);
    TrieMap_Add(idx->values, value, strlen(value), iv, NULL);
  }

  IndexEncoder enc = InvertedIndex_GetEncoder(Index_DocIdsOnly);
  RSIndexResult rec = {.type = RSResultType_Virtual, .docId = docId, .offsetsSz = 0, .freq = 0};

  return InvertedIndex_WriteEntryGeneric(iv, enc, docId, &rec);
}

size_t AttributeIndex_Index(AttributeIndex *idx, Vector *values, t_docId docId) {

  char *tok;
  size_t ret = 0;
  for (size_t i = 0; i < Vector_Size(values); i++) {
    Vector_Get(values, i, &tok);
    if (tok && *tok != '\0') {
      ret += attributeIndex_Put(idx, tok, docId);
    }
  }

  return ret;
}

IndexIterator *AttributeIndex_OpenReader(AttributeIndex *idx, DocTable *dt, const char *value,
                                         size_t len) {
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

AttributeIndex *AttributeIndex_Load(RedisModuleCtx *ctx, RedisModuleString *formattedKey,
                                    int openWrite, RedisModuleKey **keyp) {
  RedisModuleKey *key_s = NULL;
  if (!keyp) {
    keyp = &key_s;
  }

  *keyp = RedisModule_OpenKey(ctx, formattedKey,
                              REDISMODULE_READ | (openWrite ? REDISMODULE_WRITE : 0));

  int type = RedisModule_KeyType(*keyp);
  if (type != REDISMODULE_KEYTYPE_EMPTY &&
      RedisModule_ModuleTypeGetType(*keyp) != AttributeIndexType) {
    return NULL;
  }

  /* Create an empty value object if the key is currently empty. */
  AttributeIndex *ret = NULL;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    if (openWrite) {
      ret = NewAttributeIndex();
      RedisModule_ModuleTypeSetValue((*idxKey), NumericIndexType, t);
    }
  } else {
    t = RedisModule_ModuleTypeGetValue(*idxKey);
  }
  return t;
  if (*keyp == NULL || RedisModule_KeyType(*keyp) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(*keyp) != AttributeIndexType) {
    return NULL;
  }

  AttributeIndex *ret = RedisModule_ModuleTypeGetValue(*keyp);
  return ret;
}

extern RedisModuleType *AttributeIndexType;

void *AttributeIndex_RdbLoad(RedisModuleIO *rdb, int encver) {
  return NULL;
}
void AttributeIndex_RdbSave(RedisModuleIO *rdb, void *value) {
}
void AttributeIndex_Digest(RedisModuleDigest *digest, void *value) {
}

void AttributeIndex_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}

void AttributeIndex_Free(void *p) {
}

size_t AttributeIndex_MemUsage(const void *value) {
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = AttributeIndex_RdbLoad,
                               .rdb_save = AttributeIndex_RdbSave,
                               .aof_rewrite = AttributeIndex_AofRewrite,
                               .free = AttributeIndex_Free,
                               .mem_usage = AttributeIndex_MemUsage};

  AttributeIndexType = RedisModule_CreateDataType(ctx, "ft_attidx", ATTRIDX_CURRENT_VERSION, &tm);
  if (AttributeIndexType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create attribute index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
