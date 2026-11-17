/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "tag_index.h"

#include <ctype.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <time.h>

#include "suffix.h"
#include "rmalloc.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "triemap_ffi.h"
#include "rmutil/rm_assert.h"
#include "iterators_ffi.h"
#include "inverted_index_ffi.h"
#include "metrics_ffi.h"
#include "query_term_ffi.h"
#include "search_disk.h"
#include "spec.h"
#include "field.h"
#include "index_result_rs.h"
#include "query.h"
#include "redisearch.h"
#include "util/strconv.h"
#include "varint_ffi.h"

extern RedisModuleCtx *RSDummyContext;

static uint32_t tagUniqueId = 0;

// Tags are limited to 4096 each
#define MAX_TAG_LEN 0x1000
/* See tag_index.h for documentation  */
TagIndex *NewTagIndex(RedisSearchDiskIndexSpec *diskSpec, t_fieldIndex fieldIndex,
                      bool withSuffix, bool storesElements) {
  TagIndex *idx = rm_new(TagIndex);
  idx->values = NewTrieMap();
  idx->uniqueId = tagUniqueId++;
  idx->suffix = withSuffix ? NewTrieMap() : NULL;
  idx->diskSpec = diskSpec;
  idx->fieldIndex = fieldIndex;
  idx->storesElements = storesElements;
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
      uint32_t numTags = array_len(arr);
      tokenizeTagString(data->multiVal[i], fs, &arr);
      if (data->elements) {
        for (; numTags < array_len(arr); ++numTags) {
          array_ensure_append_1(fdata->tagElements, data->elements[i]);
        }
      }
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
      iv = NewInvertedIndex(idx->storesElements ? Index_StoreTermOffsets : Index_DocIdsOnly, sz);
      TrieMap_Add(idx->values, value, len, iv, NULL);
    }
  }
  return iv;
}

// Write `rec` to the inverted index of a specific tag value
// Returns the number of bytes occupied by the encoded entry plus the size of
// the inverted index (if a new inverted index was created)
static inline size_t tagIndex_WriteRecord(TagIndex *idx, const char *value, size_t len,
                                          RSIndexResult *rec, IndexStats *stats,
                                          size_t *numRecords) {
  size_t sz;
  InvertedIndex *iv = TagIndex_OpenIndex(idx, value, len, CREATE_INDEX, &sz);
  uint32_t numDocs = InvertedIndex_NumDocs(iv);
  AddRecordOutcome r = InvertedIndex_WriteEntryGeneric(iv, rec);
  if (InvertedIndex_NumDocs(iv) > numDocs) {
    (*numRecords)++;
  }
  IndexStats_BlockCountAdd(stats, r.blocks_added);
  return r.mem_growth + sz;
}

// Encode a single docId into a specific tag value
static inline size_t tagIndex_Put(TagIndex *idx, const char *value, size_t len, t_docId docId,
                                  bool hasFieldExpiration, IndexStats *stats, size_t *numRecords) {
  RSIndexResult rec = {.data.tag = RSResultData_Virtual, .docId = docId, .freq = 0,
                       .hasFieldExpiration = hasFieldExpiration, .metrics = MetricsVec_New()};
  return tagIndex_WriteRecord(idx, value, len, &rec, stats, numRecords);
}

// One occurrence of a tag in a document: the tag and the number of the element that holds it.
typedef struct {
  const char *tok;
  uint32_t element;
} TagOccurrence;

static int cmpTagOccurrence(const void *a, const void *b) {
  const TagOccurrence *ta = a, *tb = b;
  int rc = strcmp(ta->tok, tb->tok);
  return rc ? rc : (ta->element > tb->element) - (ta->element < tb->element);
}

// Initial capacity, in bytes, of the writer of the element numbers of one entry. It grows.
#define ELEMENT_NUMBERS_INITIAL_CAP 8

/* Memory-mode helper for a member of a NESTED group: write one entry per distinct tag, holding
 * the element numbers where the tag occurs. The inverted index drops a second entry for the
 * same document, so all the element numbers of a tag must go into one entry. */
static void TagIndex_WriteElementPostings(TagIndex *idx, const TagIndexIndexCtx *indexCtx) {
  TagOccurrence *sorted = rm_malloc(indexCtx->n * sizeof(*sorted));
  size_t n = 0;
  for (size_t ii = 0; ii < indexCtx->n; ++ii) {
    if (indexCtx->values[ii]) {
      sorted[n++] =
          (TagOccurrence){.tok = indexCtx->values[ii], .element = indexCtx->elements[ii]};
    }
  }
  qsort(sorted, n, sizeof(*sorted), cmpTagOccurrence);

  IndexStats *stats = indexCtx->stats;
  VarintVectorWriter *vw = NewVarintVectorWriter(ELEMENT_NUMBERS_INITIAL_CAP);
  size_t numRecords = 0;
  for (size_t start = 0; start < n;) {
    const char *tok = sorted[start].tok;
    VVW_Reset(vw);
    size_t end = start;
    for (; end < n && !strcmp(sorted[end].tok, tok); ++end) {
      if (end == start || sorted[end].element != sorted[end - 1].element) {
        VVW_Write(vw, sorted[end].element);
      }
    }
    start = end;

    RSIndexResult rec = {.data.term_tag = RSResultData_Term,
                         .data.term.borrowed.tag = RSTermRecord_Borrowed,
                         .docId = indexCtx->docId,
                         .freq = 1,
                         .fieldMask = RS_FIELDMASK_ALL,
                         .hasFieldExpiration = indexCtx->hasFieldExpiration,
                         .metrics = MetricsVec_New()};
    rec.data.term.borrowed.offsets.data = (uint8_t *)VVW_GetByteData(vw);
    rec.data.term.borrowed.offsets.len = VVW_GetByteLength(vw);
    stats->invertedSize += tagIndex_WriteRecord(idx, tok, strlen(tok), &rec, stats, &numRecords);
  }
  stats->numRecords += numRecords;
  VVW_Free(vw);
  rm_free(sorted);
}

/* Memory-mode helper: write the per-tag inverted-index postings for `docId`.
 * `tagIndex_Put` also inserts the matching `InvertedIndex*` into `idx->values`
 * if it is not already there. */
static void TagIndex_WritePostings(TagIndex *idx, const char **values, size_t n,
                                     t_docId docId, bool hasFieldExpiration, IndexStats *stats) {
  if (!values) return;
  size_t numRecords = 0;
  for (size_t ii = 0; ii < n; ++ii) {
    const char *tok = values[ii];
    if (tok) {
      stats->invertedSize +=
          tagIndex_Put(idx, tok, strlen(tok), docId, hasFieldExpiration, stats, &numRecords);
    }
  }
  stats->numRecords += numRecords;
}

static void *tagIndex_AddCount(void *oldValue, void *newValue) {
  return (void *)((uintptr_t)oldValue + (uintptr_t)newValue);
}

static void *tagIndex_SubtractCount(void *oldValue, void *newValue) {
  RS_ASSERT((uintptr_t)newValue <= (uintptr_t)oldValue);
  return (void *)((uintptr_t)oldValue - (uintptr_t)newValue);
}

static void tagIndex_FreeCount(void *value) {
}

void TagIndex_Commit(TagIndex *idx, const char **values, size_t n, IndexStats *stats) {
  if (!values) return;
  size_t numRecords = 0;
  for (size_t ii = 0; ii < n; ++ii) {
    const char *tok = values[ii];
    if (!tok) continue;
    numRecords++;
    size_t len = strlen(tok);
    if (idx->diskSpec) {
      TrieMap_Add(idx->values, tok, len, (void *)(uintptr_t)1, tagIndex_AddCount);
    }
    if (idx->suffix && (*tok != '\0')) {
      addSuffixTrieMap(idx->suffix, tok, len);
    }
  }
  if (idx->diskSpec) {
    stats->numRecords += numRecords;
  }
}

/* Phase 1 (index) for a vector of pre-processed tags. Writes the per-tag
 * postings only — the matching trie / suffix-trie / `numRecords` updates run
 * later from `tagApplier` via `TagIndex_Commit`.
 *
 * In disk mode the postings are staged onto `batch` (committed by
 * `commitDocument`). In memory mode they are written inline into the per-tag
 * `InvertedIndex` and `batch` is ignored. */
bool TagIndex_Index(RedisModuleCtx *ctx, TagIndex *idx, const TagIndexIndexCtx *indexCtx) {
  RS_LOG_ASSERT(indexCtx, "TagIndex_Index requires an indexing context");
  if (idx->diskSpec) {
    if (!indexCtx->values) return true;
    return SearchDisk_IndexTags(ctx, idx->diskSpec, indexCtx->batch, indexCtx->values, indexCtx->n,
                                indexCtx->docId, idx->fieldIndex);
  }
  if (idx->storesElements) {
    if (indexCtx->n) {
      RS_LOG_ASSERT(indexCtx->elements,
                    "a tag index that stores elements needs the element of each tag");
      TagIndex_WriteElementPostings(idx, indexCtx);
    }
    return true;
  }
  TagIndex_WritePostings(idx, indexCtx->values, indexCtx->n, indexCtx->docId,
                         indexCtx->hasFieldExpiration, indexCtx->stats);
  return true;
}

static QueryIterator *TagIndex_GetReader(const TagIndex *idx, const RedisSearchCtx *sctx,
                                         InvertedIndex *iv, const char *value, size_t len,
                                         double weight, t_fieldIndex fieldIndex,
                                         TagElements elements) {
  RSToken tok = {.str = (char *)value, .len = len};
  RSQueryTerm *t = NewQueryTerm(&tok, 0);
  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = fieldIndex};
  return NewInvIndIterator_TagQuery(iv, idx, sctx, fieldMaskOrIndex, t, weight, elements);
}

/* Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
 * Returns NULL if there is no such tag in the index. On a disk-index creation failure, returns
 * NULL and populates `status` (when non-null) with the cause. */
QueryIterator *TagIndex_OpenReader(TagIndex *idx, const RedisSearchCtx *sctx, const char *value, size_t len,
                                   double weight, t_fieldIndex fieldIndex, TagElements elements,
                                   QueryError *status) {
  if (!idx) {
    return NULL;
  }

  if (idx->diskSpec) {
    // DISK MODE: Direct disk API call
    RSToken tok = {.str = (char *)value, .len = len};
    return SearchDisk_NewTagIterator(idx->diskSpec, sctx, &tok, fieldIndex, weight, status);
  }

  // MEMORY MODE: Look up in TrieMap
  InvertedIndex *iv = TrieMap_Find(idx->values, (char *)value, len);
  if (iv == TRIEMAP_NOTFOUND || !iv || InvertedIndex_NumDocs(iv) == 0) {
    return NULL;
  }
  return TagIndex_GetReader(idx, sctx, iv, value, len, weight, fieldIndex, elements);
}

/* Open the tag index, returning NULL if it doesn't exist. */
TagIndex *TagIndex_Open(const FieldSpec *spec) {
  RS_ASSERT(FIELD_IS(spec, INDEXFLD_T_TAG));
  return spec->tagOpts.tagIndex;
}

/* Open the tag index, creating it if it doesn't exist. */
TagIndex *TagIndex_Ensure(FieldSpec *spec, RedisSearchDiskIndexSpec *diskSpec, bool withSuffix) {
  RS_ASSERT(FIELD_IS(spec, INDEXFLD_T_TAG));
  if (!spec->tagOpts.tagIndex) {
    spec->tagOpts.tagIndex =
        NewTagIndex(diskSpec, spec->index, withSuffix, FieldSpec_IsNestedMember(spec));
  }
  return spec->tagOpts.tagIndex;
}

uint32_t TagIndex_GetId(const TagIndex *idx) {
  return idx->uniqueId;
}

bool TagIndex_HasSuffix(const TagIndex *idx) {
  return idx->suffix != NULL;
}

bool TagIndex_HasDiskSpec(const TagIndex *idx) {
  return idx->diskSpec != NULL;
}

TrieMapIterator *TagIndex_IterateValues(const TagIndex *idx) {
  return TrieMap_Iterate(idx->values);
}

size_t TagIndex_NUniqueValues(const TagIndex *idx) {
  return TrieMap_NUniqueKeys(idx->values);
}

int TagIndex_DeleteTagValue(TagIndex *idx, const char *tagVal, size_t tagValLen) {
  return TrieMap_Delete(idx->values, tagVal, tagValLen,
                        idx->diskSpec ? tagIndex_FreeCount : (freeCB)InvertedIndex_Free);
}

bool TagIndex_DecrementValueCount(TagIndex *idx, const char *tag, size_t len, size_t count) {
  RS_ASSERT(idx->diskSpec);
  RS_ASSERT(count > 0);
  int added = TrieMap_Add(idx->values, tag, len, (void *)(uintptr_t)count, tagIndex_SubtractCount);
  RS_ASSERT(!added);
  if (TrieMap_Find(idx->values, tag, len) != NULL) {
    return false;
  }
  TrieMap_Delete(idx->values, tag, len, tagIndex_FreeCount);
  if (idx->suffix && len) {
    TagIndex_DeleteTagSuffix(idx, tag, len);
  }
  return true;
}

void TagIndex_DeleteTagSuffix(TagIndex *idx, const char *tagVal, size_t tagValLen) {
  deleteSuffixTrieMap(idx->suffix, tagVal, tagValLen);
}

TrieMapIterator *TagIndex_IterateSuffix(const TagIndex *idx) {
  return idx->suffix ? TrieMap_Iterate(idx->suffix) : NULL;
}

/* Return a list of list of terms which match the suffix or contains term or NULL */
arrayof(char **)
    TagIndex_GetSuffixMatches(const TagIndex *idx, const char *str, uint32_t len, bool prefix,
                           struct timespec timeout, bool skipTimeoutChecks) {
  return idx->suffix ? GetList_SuffixTrieMap(idx->suffix, str, len, prefix, timeout, skipTimeoutChecks) : NULL;
}

arrayof(char *)
    TagIndex_GetSuffixWildcardMatches(const TagIndex *idx, const char *pattern, uint32_t len,
                                               struct timespec timeout, long long maxPrefixExpansions, bool skipTimeoutChecks) {
  return idx->suffix ? GetList_SuffixTrieMap_Wildcard(idx->suffix, pattern, len, timeout, maxPrefixExpansions, skipTimeoutChecks) : NULL;
}

/* Serialize all the tags in the index to the redis client */
void TagIndex_SerializeValues(TagIndex *idx, RedisModuleCtx *ctx) {
  TrieMapIterator *it = TagIndex_IterateValues(idx);

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
  freeCB valueFree = idx->diskSpec ? tagIndex_FreeCount : (freeCB)InvertedIndex_Free;
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
