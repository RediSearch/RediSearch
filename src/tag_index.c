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

#include "rmalloc.h"
#include "redis_index.h"
#include "rmutil/rm_assert.h"
#include "search_disk.h"
#include "spec.h"
#include "field.h"
#include "query.h"
#include "redisearch.h"
#include "util/strconv.h"

extern RedisModuleCtx *RSDummyContext;

// Tags are limited to 4096 each
#define MAX_TAG_LEN 0x1000

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
    if (!(flags & TagField_CaseSensitive)) {  // check case sensitive
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
      if (!(flags & TagField_CaseSensitive)) {  // check case sensitive
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
    if (p == pp || last_is_sep) tok = rm_strdup("");
    array_append(*resArray, tok);
  }

  rm_free(pp);
  return REDISMODULE_OK;
}

int TagIndex_Preprocess(const FieldSpec *fs, const DocumentField *data, FieldIndexerData *fdata) {
  arrayof(char *) arr = array_new(char *, 4);
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

/* Everything below forwards to `src/redisearch_rs/c_entrypoint/tag_index_ffi`.
 * The tokenizer above stays in C because it is the only part of the tag field
 * that reads the schema (`tagSep`, case sensitivity, INDEXEMPTY). */

/* See tag_index.h for documentation  */
TagIndex *NewTagIndex(RedisSearchDiskIndexSpec *diskSpec, t_fieldIndex fieldIndex,
                      bool withSuffix) {
  return Rust_TagIndex_New(diskSpec, fieldIndex, withSuffix);
}

void TagIndex_Free(TagIndex **idx) {
  Rust_TagIndex_Free(idx);
}

/* See tag_index.h for documentation  */
void TagIndex_Commit(TagIndex *idx, const char **values, size_t n, IndexStats *stats) {
  if (!values) return;
  // Disk mode writes its postings during this phase, so the committed tag values
  // are counted here; memory mode counted them in `TagIndex_Index` and gets 0.
  stats->numRecords += Rust_TagIndex_Commit(idx, values, n);
}

/* See tag_index.h for documentation  */
bool TagIndex_Index(RedisModuleCtx *ctx, TagIndex *idx, const TagIndexIndexCtx *indexCtx) {
  RS_LOG_ASSERT(indexCtx, "TagIndex_Index requires an indexing context");
  // A NULL tag vector is a no-op, as it was before the Rust switch. Keeping the
  // check here means `Rust_TagIndex_Index` never sees a NULL `values` with n > 0,
  // which would break its precondition.
  if (!indexCtx->values) return true;

  TagIndexWriteResult r =
      Rust_TagIndex_Index(idx, ctx, indexCtx->batch, indexCtx->values, indexCtx->n, indexCtx->docId,
                          indexCtx->hasFieldExpiration);
  IndexStats *stats = indexCtx->stats;
  stats->numRecords += r.num_records;
  stats->invertedSize += r.size_delta;
  IndexStats_BlockCountAdd(stats, (int64_t)r.blocks_added);
  // In disk mode `ok` reflects whether the disk write succeeded; in memory mode
  // indexing is infallible and it is always true.
  return r.ok;
}

/* See tag_index.h for documentation  */
QueryIterator *TagIndex_OpenReader(TagIndex *idx, const RedisSearchCtx *sctx, const char *value,
                                   size_t len, double weight, t_fieldIndex fieldIndex,
                                   QueryError *status) {
  if (!idx) {
    return NULL;
  }
  // `Rust_TagIndex_OpenReader` only reads `sctx`; the const cast matches the
  // query paths elsewhere that hand a read-only search context to a reader.
  // `fieldIndex` is not forwarded: the Rust index stores its own field index,
  // handed to it at `Rust_TagIndex_New`.
  return Rust_TagIndex_OpenReader(idx, (RedisSearchCtx *)sctx, value, len, weight, status);
}

/* Open the tag index, returning NULL if it doesn't exist. */
TagIndex *TagIndex_Open(const FieldSpec *spec) {
  return spec->tagOpts.tagIndex;
}

/* Open the tag index, creating it if it doesn't exist. */
TagIndex *TagIndex_Ensure(FieldSpec *spec, RedisSearchDiskIndexSpec *diskSpec, bool withSuffix) {
  if (!spec->tagOpts.tagIndex) {
    spec->tagOpts.tagIndex = NewTagIndex(diskSpec, spec->index, withSuffix);
  }

  return spec->tagOpts.tagIndex;
}

uint32_t TagIndex_GetId(const TagIndex *idx) {
  return Rust_TagIndex_GetId(idx);
}

bool TagIndex_HasSuffix(const TagIndex *idx) {
  return Rust_TagIndex_HasSuffix(idx);
}

bool TagIndex_HasDiskSpec(const TagIndex *idx) {
  return Rust_TagIndex_HasDiskSpec(idx);
}

ValueIterator *TagIndex_IterateValues(const TagIndex *idx) {
  return Rust_TagIndex_IterateValues(idx);
}

size_t TagIndex_NUniqueValues(const TagIndex *idx) {
  return Rust_TagIndex_NUniqueValues(idx);
}

ValueIterator *TagIndex_IterateValuesWithFilter(TagIndex *idx, const char *tagVal, size_t tagValLen,
                                                tag_iter_mode mode) {
  return Rust_TagIndex_IterateValuesWithFilter(idx, tagVal, tagValLen, (enum tm_iter_mode)mode);
}

ValueIterator *TagIndex_IterateSuffix(const TagIndex *idx) {
  return Rust_TagIndex_IterateSuffix(idx);
}

/* See tag_index.h for documentation  */
arrayof(char *)
    TagIndex_GetSuffixMatches(const TagIndex *idx, const char *str, uint32_t len, bool prefix,
                              struct timespec timeout, bool skipTimeoutChecks) {
  if (!TagIndex_HasSuffix(idx)) {
    return NULL;
  }
  return Rust_TagIndex_GetSuffixMatches(idx, str, len, prefix, timeout, skipTimeoutChecks);
}

/* See tag_index.h for documentation  */
arrayof(char *)
    TagIndex_GetSuffixWildcardMatches(const TagIndex *idx, const char *pattern, uint32_t len,
                                      struct timespec timeout, long long maxPrefixExpansions,
                                      bool skipTimeoutChecks) {
  if (!TagIndex_HasSuffix(idx)) {
    return NULL;
  }
  return Rust_TagIndex_GetSuffixWildcardMatches(idx, pattern, len, timeout, maxPrefixExpansions,
                                                skipTimeoutChecks);
}

/* Serialize all the tags in the index to the redis client */
void TagIndex_SerializeValues(TagIndex *idx, RedisModuleCtx *ctx) {
  ValueIterator *it = TagIndex_IterateValues(idx);

  char *str;
  tm_len_t slen;
  RedisModule_ReplyWithSet(ctx, REDISMODULE_POSTPONED_LEN);
  long long count = 0;
  while (Rust_TagIndex_ValueIterator_NextKey(it, &str, &slen)) {
    ++count;
    RedisModule_ReplyWithStringBuffer(ctx, str, slen);
  }

  RedisModule_ReplySetSetLength(ctx, count);

  Rust_TagIndex_ValueIterator_Free(it);
}

size_t TagIndex_GetOverhead(const FieldSpec *fs) {
  TagIndex *idx = TagIndex_Open(fs);
  return idx ? Rust_TagIndex_GetOverhead(idx) : 0;
}
