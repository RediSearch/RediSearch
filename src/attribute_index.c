#include "attribute_index.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"

AttributeIndex *NewAttributeIndex(const char *namespace, const char *fieldName) {
  AttributeIndex *idx = rm_new(AttributeIndex);
  idx->numFields = 1;
  idx->fields = rm_calloc(1, sizeof(*idx->fields));
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
  if (!iv) {
    iv = NewInvertedIndex(Index_DocIdsOnly, 1);
    TrieMap_Add(idx->values, value, strlen(value), iv, NULL);
  }

  IndexEncoder enc = InvertedIndex_GetEncoder(iv->flags);
  RSIndexResult rec = {.type = RSResultType_Virtual, .docId = docId, .offsetsSz = 0, .freq = 0};

  return InvertedIndex_WriteEntryGeneric(iv, enc, docId, NULL);
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
  if (!iv) {
    return NULL;
  }
  IndexDecoder dec = InvertedIndex_GetDecoder(iv->flags);
  if (!dec) {
    return NULL;
  }

  IndexReader *r = NewTermIndexReader(iv, dt, RS_FIELDMASK_ALL, NULL);
  if (!r) {
    return NULL;
  }
  return NewReadIterator(r);
}