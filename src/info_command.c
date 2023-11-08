/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redismodule.h"
#include "spec.h"
#include "inverted_index.h"
#include "vector_index.h"
#include "cursor.h"

#define REPLY_KVNUM(n, k, v)                       \
  do {                                             \
    RedisModule_ReplyWithSimpleString(ctx, (k));   \
    RedisModule_ReplyWithDouble(ctx, (double)(v)); \
    n += 2;                                        \
  } while (0)

#define REPLY_KVINT(n, k, v)                       \
  do {                                             \
    RedisModule_ReplyWithSimpleString(ctx, (k));   \
    RedisModule_ReplyWithLongLong(ctx, (long long)(v)); \
    n += 2;                                        \
  } while (0)

#define REPLY_KVSTR(n, k, v)                     \
  do {                                           \
    RedisModule_ReplyWithSimpleString(ctx, (k)); \
    RedisModule_ReplyWithSimpleString(ctx, (v)); \
    n += 2;                                      \
  } while (0)

static int renderIndexOptions(RedisModuleCtx *ctx, IndexSpec *sp) {

#define ADD_NEGATIVE_OPTION(flag, str)                            \
  do {                                                            \
    if (!(sp->flags & (flag))) {                                  \
      RedisModule_ReplyWithStringBuffer(ctx, (str), strlen(str)); \
      n++;                                                        \
    }                                                             \
  } while (0)

  RedisModule_ReplyWithSimpleString(ctx, "index_options");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int n = 0;
  ADD_NEGATIVE_OPTION(Index_StoreFreqs, SPEC_NOFREQS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreFieldFlags, SPEC_NOFIELDS_STR);
  ADD_NEGATIVE_OPTION((Index_StoreTermOffsets|Index_StoreByteOffsets), SPEC_NOOFFSETS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreByteOffsets, SPEC_NOHL_STR);
  if (sp->flags & Index_WideSchema) {
    RedisModule_ReplyWithSimpleString(ctx, SPEC_SCHEMA_EXPANDABLE_STR);
    n++;
  }
  RedisModule_ReplySetArrayLength(ctx, n);
  return 2;
}

static int renderIndexDefinitions(RedisModuleCtx *ctx, IndexSpec *sp) {
  int n = 0;
  SchemaRule *rule = sp->rule;
  RedisModule_ReplyWithSimpleString(ctx, "index_definition");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  REPLY_KVSTR(n, "key_type", DocumentType_ToString(rule->type));

  int num_prefixes = array_len(rule->prefixes);
  if (num_prefixes) {
    RedisModule_ReplyWithSimpleString(ctx, "prefixes");
    RedisModule_ReplyWithArray(ctx, num_prefixes);
    for (int i = 0; i < num_prefixes; ++i) {
      RedisModule_ReplyWithSimpleString(ctx, rule->prefixes[i]);
    }
    n += 2;
  }

  if (rule->filter_exp_str) {
    REPLY_KVSTR(n, "filter", rule->filter_exp_str);
  }

  if (rule->lang_default) {
    REPLY_KVSTR(n, "default_language", RSLanguage_ToString(rule->lang_default));
  }

  if (rule->lang_field) {
    REPLY_KVSTR(n, "language_field", rule->lang_field);
  }

  if (rule->score_default) {
    REPLY_KVNUM(n, "default_score", rule->score_default);
  }

  if (rule->score_field) {
    REPLY_KVSTR(n, "score_field", rule->score_field);
  }

  if (rule->payload_field) {
    REPLY_KVSTR(n, "payload_field", rule->payload_field);
  }

  RedisModule_ReplySetArrayLength(ctx, n);
  return 2;
}

/* FT.INFO {index}
 *  Provide info and stats about an index
 */
int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int n = 0;

  REPLY_KVSTR(n, "index_name", sp->name);

  n += renderIndexOptions(ctx, sp);

  n += renderIndexDefinitions(ctx, sp);

  RedisModule_ReplyWithSimpleString(ctx, "attributes");
  RedisModule_ReplyWithArray(ctx, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithSimpleString(ctx, "identifier");
    RedisModule_ReplyWithSimpleString(ctx, sp->fields[i].path);
    RedisModule_ReplyWithSimpleString(ctx, "attribute");
    RedisModule_ReplyWithSimpleString(ctx, sp->fields[i].name);
    int nn = 4;
    const FieldSpec *fs = sp->fields + i;

    // RediSearch_api - No coverage
    if (fs->options & FieldSpec_Dynamic) {
      REPLY_KVSTR(nn, "type", "<DYNAMIC>");
      size_t ntypes = 0;

      nn += 2;
      RedisModule_ReplyWithSimpleString(ctx, "types");
      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      for (size_t jj = 0; jj < INDEXFLD_NUM_TYPES; ++jj) {
        if (FIELD_IS(fs, INDEXTYPE_FROM_POS(jj))) {
          ntypes++;
          RedisModule_ReplyWithSimpleString(ctx, FieldSpec_GetTypeNames(jj));
        }
      }
      RedisModule_ReplySetArrayLength(ctx, ntypes);
    } else {
      REPLY_KVSTR(nn, "type", FieldSpec_GetTypeNames(INDEXTYPE_TO_POS(fs->types)));
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      REPLY_KVNUM(nn, SPEC_WEIGHT_STR, fs->ftWeight);
    }

    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[2];
      sprintf(buf, "%c", fs->tagOpts.tagSep);
      REPLY_KVSTR(nn, SPEC_TAG_SEPARATOR_STR, buf);
      if (fs->tagOpts.tagFlags & TagField_CaseSensitive) {
        RedisModule_ReplyWithSimpleString(ctx, SPEC_TAG_CASE_SENSITIVE_STR);
        ++nn;
      }
    }
    if (FieldSpec_IsSortable(fs)) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_SORTABLE_STR);
      ++nn;
    }
    if (FieldSpec_IsUnf(fs)) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_UNF_STR);
      ++nn;
    }
    if (FieldSpec_IsNoStem(fs)) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_NOSTEM_STR);
      ++nn;
    }
    if (!FieldSpec_IsIndexable(fs)) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_NOINDEX_STR);
      ++nn;
    }
    if (FieldSpec_HasSuffixTrie(fs)) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_WITHSUFFIXTRIE_STR);
      ++nn;
    }
    RedisModule_ReplySetArrayLength(ctx, nn);
  }
  n += 2;

  REPLY_KVNUM(n, "num_docs", sp->stats.numDocuments);
  REPLY_KVNUM(n, "max_doc_id", sp->docs.maxDocId);
  REPLY_KVNUM(n, "num_terms", sp->stats.numTerms);
  REPLY_KVNUM(n, "num_records", sp->stats.numRecords);
  REPLY_KVNUM(n, "inverted_sz_mb", sp->stats.invertedSize / (float)0x100000);
  REPLY_KVNUM(n, "vector_index_sz_mb", sp->stats.vectorIndexSize / (float)0x100000);
  REPLY_KVNUM(n, "total_inverted_index_blocks", TotalIIBlocks);
  // REPLY_KVNUM(n, "inverted_cap_mb", sp->stats.invertedCap / (float)0x100000);

  // REPLY_KVNUM(n, "inverted_cap_ovh", 0);
  //(float)(sp->stats.invertedCap - sp->stats.invertedSize) / (float)sp->stats.invertedCap);

  REPLY_KVNUM(n, "offset_vectors_sz_mb", sp->stats.offsetVecsSize / (float)0x100000);
  // REPLY_KVNUM(n, "skip_index_size_mb", sp->stats.skipIndexesSize / (float)0x100000);
  //  REPLY_KVNUM(n, "score_index_size_mb", sp->stats.scoreIndexesSize / (float)0x100000);

  REPLY_KVNUM(n, "doc_table_size_mb", sp->docs.memsize / (float)0x100000);
  REPLY_KVNUM(n, "sortable_values_size_mb", sp->docs.sortablesSize / (float)0x100000);

  REPLY_KVNUM(n, "key_table_size_mb", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  REPLY_KVNUM(n, "records_per_doc_avg",
              (float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  REPLY_KVNUM(n, "bytes_per_record_avg",
              (float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  REPLY_KVNUM(n, "offsets_per_term_avg",
              (float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  REPLY_KVNUM(n, "offset_bits_per_record_avg",
              8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);
  REPLY_KVNUM(n, "hash_indexing_failures", sp->stats.indexingFailures);
  REPLY_KVNUM(n, "total_indexing_time", sp->stats.totalIndexTime / 1000.0);
  REPLY_KVNUM(n, "indexing", !!global_spec_scanner || sp->scan_in_progress);

  IndexesScanner *scanner = global_spec_scanner ? global_spec_scanner : sp->scanner;
  double percent_indexed = IndexesScanner_IndexedPercent(scanner, sp);
  REPLY_KVNUM(n, "percent_indexed", percent_indexed);

  REPLY_KVINT(n, "number_of_uses", sp->counter);

  if (sp->gc) {
    RedisModule_ReplyWithSimpleString(ctx, "gc_stats");
    GCContext_RenderStats(sp->gc, ctx);
    n += 2;
  }

  Cursors_RenderStats(&RSCursors, &RSCursorsCoord, sp->name, ctx);
  n += 2;

  if (sp->flags & Index_HasCustomStopwords) {
    ReplyWithStopWordsList(ctx, sp->stopwords);
    n += 2;
  }

  RedisModule_ReplyWithSimpleString(ctx, "dialect_stats");
  RedisModule_ReplyWithArray(ctx, 2 * (MAX_DIALECT_VERSION - MIN_DIALECT_VERSION + 1));
  for (int dialect = MIN_DIALECT_VERSION; dialect <= MAX_DIALECT_VERSION; ++dialect) {
    RedisModule_ReplyWithPrintf(ctx, "dialect_%d", dialect);
    RedisModule_ReplyWithLongLong(ctx, GET_DIALECT(sp->used_dialects, dialect));
  }
  n += 2;

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
}
