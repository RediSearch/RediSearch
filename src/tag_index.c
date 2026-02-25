/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "tag_index.h"
#include "suffix.h"
#include "rmalloc.h"
#include "rmutil/vector.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "rmutil/util.h"
#include "triemap.h"
#include "util/misc.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"
#include "resp3.h"
#include "iterators/inverted_index_iterator.h"
#include "search_disk.h"
#include "spec.h"

extern RedisModuleCtx *RSDummyContext;

static uint32_t tagUniqueId = 0;

// Tags are limited to 4096 each
#define MAX_TAG_LEN 0x1000
/* See tag_index.h for documentation  */
TagIndex *NewTagIndex(RedisSearchDiskIndexSpec *diskSpec, t_fieldIndex fieldIndex) {
  TagIndex *idx = rm_new(TagIndex);
  idx->values = NewTrieMap();
  idx->uniqueId = tagUniqueId++;
  idx->suffix = NULL;
  idx->diskSpec = diskSpec;
  idx->fieldIndex = fieldIndex;
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
      size_t len = strlen(tok);
      char *dst = unicode_tolower(tok, &len);
      if (dst) {
        rm_free(tok);
        tok = dst;
      } else {
        // No memory allocation, just ensure null termination
        tok[len] = '\0';
      }
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
      // normalize the string
      if (!(flags & TagField_CaseSensitive)) { // check case sensitive
        char *longer_dst = unicode_tolower(tok, &toklen);
        if (longer_dst) {
          tok = longer_dst;
        } else {
          tok = rm_strndup(tok, MIN(toklen, MAX_TAG_LEN));
        }
      } else {
        tok = rm_strndup(tok, MIN(toklen, MAX_TAG_LEN));
      }

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
    RS_ABORT("nope")
    break;
  }
  fdata->tags = arr;
  return ret;
}

struct InvertedIndex *TagIndex_OpenIndex(const TagIndex *idx, const char *value,
                                          size_t len, int create_if_missing, size_t *sz) {
  *sz = 0;
  InvertedIndex *iv = TrieMap_Find(idx->values, value, len);
  if (iv == TRIEMAP_NOTFOUND) {
    if (create_if_missing) {
      iv = NewInvertedIndex(Index_DocIdsOnly, sz);
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
  RSIndexResult rec = {.data.tag = RSResultData_Virtual, .docId = docId, .freq = 0};
  InvertedIndex *iv = TagIndex_OpenIndex(idx, value, len, CREATE_INDEX, &sz);
  return InvertedIndex_WriteEntryGeneric(iv, &rec) + sz;
}

/* Index a vector of pre-processed tags for a docId */
bool TagIndex_Index(TagIndex *idx, const char **values, size_t n, t_docId docId, IndexStats *stats) {
  if (!values) return true;

  if (idx->diskSpec) {
    // DISK MODE: Index to disk and add tags to TrieMap with NULL sentinel
    if (!SearchDisk_IndexTags(idx->diskSpec, values, n, docId, idx->fieldIndex)) {
      return false;
    }

    // Also populate TrieMap with NULL sentinels for tag enumeration
    for (size_t ii = 0; ii < n; ++ii) {
      const char *tok = values[ii];
      if (tok) {
        TrieMap_Add(idx->values, tok, strlen(tok), NULL, NULL);

        if (idx->suffix && (*tok != '\0')) {
          addSuffixTrieMap(idx->suffix, tok, strlen(tok));
        }
      }
    }
  } else {
    // MEMORY MODE
    for (size_t ii = 0; ii < n; ++ii) {
      const char *tok = values[ii];
      if (tok) {
        stats->invertedSize += tagIndex_Put(idx, tok, strlen(tok), docId);

        if (idx->suffix && (*tok != '\0')) { // add to suffix TrieMap
          addSuffixTrieMap(idx->suffix, tok, strlen(tok));
        }
      }
    }
  }

  stats->numRecords++;
  return true;
}

static QueryIterator *TagIndex_GetReader(const TagIndex *idx, const RedisSearchCtx *sctx, InvertedIndex *iv,
                                         const char *value, size_t len, double weight, t_fieldIndex fieldIndex) {
  RSToken tok = {.str = (char *)value, .len = len};
  RSQueryTerm *t = NewQueryTerm(&tok, 0);
  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = fieldIndex};
  return NewInvIndIterator_TagQuery(iv, idx, sctx, fieldMaskOrIndex, t, weight);
}

// Helper: Get iterator from TrieMap iterator value
// In disk mode: ptr is ignored, calls disk API with tag string
// In memory mode: ptr is InvertedIndex*, uses it directly
QueryIterator *TagIndex_GetIteratorFromTrieMapValue(TagIndex *idx, const RedisSearchCtx *sctx,
                                                    const char *tag, size_t len, void *ptr,
                                                    double weight, t_fieldIndex fieldIndex) {
  if (idx->diskSpec) {
    // DISK MODE: Use tag string to query disk
    RSToken tok = {.str = (char *)tag, .len = len};
    return SearchDisk_NewTagIterator(idx->diskSpec, &tok, fieldIndex, weight);
  }

  // MEMORY MODE: Use InvertedIndex from TrieMap
  InvertedIndex *iv = (InvertedIndex *)ptr;
  if (!iv || InvertedIndex_NumDocs(iv) == 0) {
    return NULL;
  }
  return TagIndex_GetReader(idx, sctx, iv, tag, len, weight, fieldIndex);
}

/* Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
 * Returns NULL if there is no such tag in the index */
QueryIterator *TagIndex_OpenReader(TagIndex *idx, const RedisSearchCtx *sctx, const char *value, size_t len,
                                   double weight, t_fieldIndex fieldIndex) {
  if (!idx) {
    return NULL;
  }

  if (idx->diskSpec) {
    // DISK MODE: Direct disk API call
    RSToken tok = {.str = (char *)value, .len = len};
    return SearchDisk_NewTagIterator(idx->diskSpec, &tok, fieldIndex, weight);
  }

  // MEMORY MODE: Look up in TrieMap
  InvertedIndex *iv = TrieMap_Find(idx->values, (char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv || InvertedIndex_NumDocs(iv) == 0) {
    return NULL;
  }
  return TagIndex_GetReader(idx, sctx, iv, value, len, weight, fieldIndex);
}

/* Open the tag index, returning NULL if it doesn't exist. */
TagIndex *TagIndex_Open(const FieldSpec *spec) {
  RS_ASSERT(FIELD_IS(spec, INDEXFLD_T_TAG));
  return spec->tagOpts.tagIndex;
}

/* Open the tag index, creating it if it doesn't exist. */
TagIndex *TagIndex_Ensure(FieldSpec *spec, RedisSearchDiskIndexSpec *diskSpec) {
  RS_ASSERT(FIELD_IS(spec, INDEXFLD_T_TAG));
  if (!spec->tagOpts.tagIndex) {
    spec->tagOpts.tagIndex = NewTagIndex(diskSpec, spec->index);
  }
  return spec->tagOpts.tagIndex;
}

/* Serialize all the tags in the index to the redis client */
void TagIndex_SerializeValues(TagIndex *idx, RedisModuleCtx *ctx) {
  TrieMapIterator *it = TrieMap_Iterate(idx->values);

  char *str;
  tm_len_t slen;
  void *ptr;
  RedisModule_ReplyWithSet(ctx, REDISMODULE_POSTPONED_LEN);
  long long count = 0;
  while (TrieMapIterator_Next(it, &str, &slen, &ptr)) {
    ++count;
    RedisModule_ReplyWithStringBuffer(ctx, str, slen);
  }

  RedisModule_ReplySetSetLength(ctx, count);

  TrieMapIterator_Free(it);
}

void TagIndex_Free(TagIndex *idx) {
  // In disk mode, values are NULL sentinels - pass NULL to use RedisModule_Free (no-op on NULL)
  // In memory mode, values are InvertedIndex pointers
  freeCB valueFree = idx->diskSpec ? NULL : (freeCB)InvertedIndex_Free;
  TrieMap_Free(idx->values, valueFree);
  TrieMap_Free(idx->suffix, suffixTrieMap_freeCallback);
  rm_free(idx);
}

size_t TagIndex_GetOverhead(const FieldSpec *fs) {
  size_t overhead = 0;
  TagIndex *idx = TagIndex_Open(fs);
  if (idx) {
    overhead = TrieMap_MemUsage(idx->values);     // Values' size are counted in stats.invertedSize
    if (idx->suffix) {
      overhead += TrieMap_MemUsage(idx->suffix);
    }
  }
  return overhead;
}
