/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "info_command.h"

#include <string.h>

#include "coord/rmr/rmr.h"
#include "rmalloc.h"
#include "rmutil/strings.h"

static const char *infoFields[] = {
    "index_name",
    "index_options",
    "index_definition",
    "attributes",
    "num_docs",
    "max_doc_id",
    "num_terms",
    "num_records",
    "inverted_sz_mb",
    "vector_index_sz_mb",
    "total_inverted_index_blocks",
    "offset_vectors_sz_mb",
    "doc_table_size_mb",
    "sortable_values_size_mb",
    "key_table_size_mb",
    "tag_overhead_sz_mb",
    "text_overhead_sz_mb",
    "column_families",
    "total_index_memory_sz_mb",
    "geoshapes_sz_mb",
    "records_per_doc_avg",
    "bytes_per_record_avg",
    "offsets_per_term_avg",
    "offset_bits_per_record_avg",
    "hash_indexing_failures",
    "total_indexing_time",
    "indexing",
    "percent_indexed",
    "number_of_uses",
    "number_of_admin_ops",
    "cleaning",
    "gc_stats",
    "cursor_stats",
    "stopwords_list",
    "dialect_stats",
    "Index Errors",
    "field statistics",
};

bool InfoSelection_Matches(const InfoSelection *selection, const char *key) {
  if (!selection || !selection->count) return true;
  size_t keyLen = strlen(key);
  for (int i = 0; i < selection->count; ++i) {
    size_t len;
    const char *arg = RedisModule_StringPtrLen(selection->args[i], &len);
    if ((selection->prefix ? len <= keyLen : len == keyLen) && !memcmp(arg, key, len)) {
      return true;
    }
  }
  return false;
}

int InfoSelection_Parse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        InfoSelection *selection) {
  *selection = (InfoSelection){0};
  if (argc == 2) return REDISMODULE_OK;
  if (argc < 4) {
    RedisModule_WrongArity(ctx);
    return REDISMODULE_ERR;
  }
  bool prefix = RMUtil_StringEqualsCaseC(argv[2], "PREFIX");
  if ((!prefix && !RMUtil_StringEqualsCaseC(argv[2], "FIELDS")) || (prefix && argc != 4)) {
    RedisModule_ReplyWithError(
        ctx, "Invalid FT.INFO option: use PREFIX <prefix> or FIELDS <field> [field ...]");
    return REDISMODULE_ERR;
  }
  *selection = (InfoSelection){.args = argv + 3, .count = argc - 3, .prefix = prefix};
  if (!prefix) {
    for (int i = 0; i < selection->count; ++i) {
      InfoSelection field = {.args = selection->args + i, .count = 1};
      bool found = false;
      for (size_t j = 0; j < sizeof(infoFields) / sizeof(*infoFields); ++j) {
        if (InfoSelection_Matches(&field, infoFields[j])) {
          found = true;
          break;
        }
      }
      if (!found) {
        RedisModule_ReplyWithErrorFormat(ctx, "Unknown FT.INFO field: %s",
                                         RedisModule_StringPtrLen(selection->args[i], NULL));
        return REDISMODULE_ERR;
      }
    }
  }
  return REDISMODULE_OK;
}

static void InfoSelection_Free(struct MRCtx *ctx) {
  InfoSelection *selection = MRCtx_GetPrivData(ctx);
  for (int i = 0; i < selection->count; ++i) RedisModule_FreeString(NULL, selection->args[i]);
  rm_free(selection->args);
  rm_free(selection);
}

struct MRCtx *InfoSelection_CreateRequest(RedisModuleCtx *ctx, const InfoSelection *selection,
                                          int numShards) {
  InfoSelection *copy = rm_malloc(sizeof(*copy));
  *copy = *selection;
  copy->expectedShards = numShards;
  copy->args = rm_calloc(copy->count, sizeof(*copy->args));
  for (int i = 0; i < copy->count; ++i) {
    copy->args[i] = RedisModule_CreateStringFromString(NULL, selection->args[i]);
  }
  struct MRCtx *mc = MR_CreateCtx(ctx, NULL, copy, numShards);
  MRCtx_SetFreePrivDataCB(mc, InfoSelection_Free);
  return mc;
}
