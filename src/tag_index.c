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
#include "triemap_ffi.h"
#include "util/misc.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"
#include "resp3.h"
#include "iterators_ffi.h"
#include "inverted_index_ffi.h"
#include "metrics_ffi.h"
#include "query_term_ffi.h"
#include "search_disk.h"
#include "spec.h"

#include "redisearch_rs/headers/tag_index.h"

extern RedisModuleCtx *RSDummyContext;

static uint32_t tagUniqueId = 0;

// Tags are limited to 4096 each
#define MAX_TAG_LEN 0x1000
/* See tag_index.h for documentation  */
TagIndex *NewTagIndex(RedisSearchDiskIndexSpec *diskSpec, t_fieldIndex fieldIndex,
                      bool withSuffix) {
  return TagIndex2_New(
    tagUniqueId++,
    diskSpec,
    fieldIndex,
    withSuffix
  );
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
    if (p == pp || last_is_sep) {
      tok = rm_strdup("");
      array_append(*resArray, tok);
    }
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

/* Apply the in-memory tag-trie updates for a vector of tag tokens (Phase 3).
 *
 * Called from `tagApplier` in both modes:
 *   - Disk mode: runs after a successful batch commit. Inserts NULL sentinels
 *     into `idx->values` (postings live on disk).
 *   - Memory mode: the trie already holds `InvertedIndex*` pointers written by
 *     `TagIndex_Index` (via `TagIndex2_Index`), so the trie insert is skipped
 *     to preserve them.
 *
 * Record accounting follows the phase that writes the posting:
 *   - Memory mode writes postings inline in `TagIndex_Index` and counts only
 *     records accepted by the inverted index.
 *   - Disk mode reaches this function after the batch commit, so committed tag
 *     values are counted here while applying the matching in-memory metadata.
 * Infallible. */
void TagIndex_Commit(TagIndex *idx, const char **values, size_t n, IndexStats *stats) {
  // In disk mode the postings are written during this phase, so the committed
  // tag values are counted here; in memory mode they were already counted in
  // `TagIndex_Index` and `TagIndex2_Commit` returns 0.
  stats->numRecords += TagIndex2_Commit(idx, values, n);
}

/* Phase 1 (index) for a vector of pre-processed tags. Writes the per-tag
 * postings only — the matching trie / suffix-trie / `numRecords` updates run
 * later from `tagApplier` via `TagIndex_Commit`.
 *
 * In disk mode the postings are staged onto `batch` (committed by
 * `commitDocument`). In memory mode they are written inline into the per-tag
 * `InvertedIndex` and `batch` is ignored. */
bool TagIndex_Index(RedisModuleCtx *ctx, TagIndex *idx, SearchDiskWriteBatchHandle *batch,
                    const char **values, size_t n, t_docId docId, IndexStats *stats) {
  TagIndexWriteResult r = TagIndex2_Index(
    idx,
    ctx,
    batch,
    values,
    n,
    docId
  );
  stats->numRecords += r.num_records;
  stats->invertedSize += r.size_delta;
  IndexStats_BlockCountAdd(stats, (int64_t)r.blocks_added);
  // In disk mode `ok` reflects whether `SearchDisk_IndexTags` succeeded; in
  // memory mode it is always true.
  return r.ok;
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
                                                    double weight, t_fieldIndex fieldIndex,
                                                    QueryError *status) {
  return TagIndex2_GetIteratorFromTrieMapValue(idx, sctx, tag, len, ptr, weight, fieldIndex, status);
}

/* Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
 * Returns NULL if there is no such tag in the index. On a disk-index creation failure, returns
 * NULL and populates `status` (when non-null) with the cause. */
QueryIterator *TagIndex_OpenReader(TagIndex *idx, const RedisSearchCtx *sctx, const char *value, size_t len,
                                   double weight, t_fieldIndex fieldIndex, QueryError *status) {
  if (!idx) {
    return NULL;
  }

  // TagIndex2_OpenReader only reads `sctx`; the const cast matches the query
  // paths elsewhere that hand a read-only search context to the reader.
  return TagIndex2_OpenReader(idx, (RedisSearchCtx *)sctx, value, len, weight, fieldIndex, status);
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
    spec->tagOpts.tagIndex = NewTagIndex(diskSpec, spec->index, withSuffix);
  }
  return spec->tagOpts.tagIndex;
}

uint32_t TagIndex_GetId(const TagIndex *idx) {
  return TagIndex2_GetId(idx);
}

bool TagIndex_HasSuffix(const TagIndex *idx) {
  return TagIndex2_HasSuffix(idx);
}

bool TagIndex_HasDiskSpec(const TagIndex *idx) {
  return TagIndex2_HasDiskSpec(idx);
}

size_t TagIndex_NUniqueValues(const TagIndex *idx) {
  return TagIndex2_NUniqueValues(idx);
}

ValueIterator *TagIndex_IterateValuesWithFilter(TagIndex *idx, const char *tagVal,
                                                 size_t tagValLen, tag_iter_mode mode) {
  return TagIndex2_IterateValuesWithFilter(idx, tagVal, tagValLen, (enum tm_iter_mode)mode);
}

void TagIndex_IterateRangeValues(const TagIndex *idx, const char *min, int minlen, bool includeMin,
                                 const char *max, int maxlen, bool includeMax,
                                 TrieMapRangeCallback callback, void *ctx) {
  TagIndex2_IterateRange(idx, min, minlen, includeMin, max, maxlen, includeMax, callback, ctx);
}

ValueIterator *TagIndex_IterateSuffix(const TagIndex *idx) {
  return TagIndex2_IterateSuffix(idx);
}

/* Return a list of terms which match the suffix or contains term or NULL */
arrayof(char *)
    TagIndex_GetSuffixMatches(const TagIndex *idx, const char *str, uint32_t len, bool prefix,
                           struct timespec timeout, bool skipTimeoutChecks) {
  if (!TagIndex_HasSuffix(idx)) {
    return NULL;
  }
  return TagIndex2_GetSuffixMatches(idx, str, len, prefix, timeout, skipTimeoutChecks);
}

arrayof(char *)
    TagIndex_GetSuffixWildcardMatches(const TagIndex *idx, const char *pattern, uint32_t len,
                                               struct timespec timeout, long long maxPrefixExpansions, bool skipTimeoutChecks) {
  if (!TagIndex_HasSuffix(idx)) {
    return NULL;
  }
  return TagIndex2_GetSuffixWildcardMatches(idx, pattern, len, timeout, maxPrefixExpansions, skipTimeoutChecks);
}

/* Serialize all the tags in the index to the redis client */
void TagIndex_SerializeValues(TagIndex *idx, RedisModuleCtx *ctx) {
  ValueIterator *it = TagIndex2_IterateValues(idx);

  char *str;
  tm_len_t slen;
  TagIndexValue *tiv;
  RedisModule_ReplyWithSet(ctx, REDISMODULE_POSTPONED_LEN);
  long long count = 0;
  while (TagIndex2_ValueIterator_Next(it, &str, &slen, &tiv)) {
    ++count;
    RedisModule_ReplyWithStringBuffer(ctx, str, slen);
  }

  RedisModule_ReplySetSetLength(ctx, count);

  TagIndex2_ValueIterator_Free(it);
}

void TagIndex_Free(TagIndex **idx) {
  TagIndex2_Free(idx);
}

size_t TagIndex_GetOverhead(const FieldSpec *fs) {
  size_t overhead = 0;
  TagIndex *idx = TagIndex_Open(fs);
  if (idx) {
    overhead = TagIndex2_GetOverhead(idx);
  }
  return overhead;
}
