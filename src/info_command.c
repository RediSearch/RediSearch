/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "spec.h"
#include "inverted_index.h"
#include "vector_index.h"
#include "cursor.h"
#include "resp3.h"
#include "geometry/geometry_api.h"
#include "geometry_index.h"
#include "redismodule.h"
#include "reply_macros.h"

static void renderIndexOptions(RedisModule_Reply *reply, IndexSpec *sp) {

#define ADD_NEGATIVE_OPTION(flag, str)               \
  do {                                               \
    if (!(sp->flags & (flag))) {                     \
      RedisModule_Reply_SimpleString(reply, (str));  \
    }                                                \
  } while (0)

  RedisModule_ReplyKV_Array(reply, "index_options");
  ADD_NEGATIVE_OPTION(Index_StoreFreqs, SPEC_NOFREQS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreFieldFlags, SPEC_NOFIELDS_STR);
  ADD_NEGATIVE_OPTION((Index_StoreTermOffsets|Index_StoreByteOffsets), SPEC_NOOFFSETS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreByteOffsets, SPEC_NOHL_STR);
  if (sp->flags & Index_WideSchema) {
    RedisModule_Reply_SimpleString(reply, SPEC_SCHEMA_EXPANDABLE_STR);
  }
  RedisModule_Reply_ArrayEnd(reply);
}

static void renderIndexDefinitions(RedisModule_Reply *reply, IndexSpec *sp) {
  SchemaRule *rule = sp->rule;

  RedisModule_ReplyKV_Map(reply, "index_definition"); // index_definition

  REPLY_KVSTR("key_type", DocumentType_ToString(rule->type));

  int num_prefixes = array_len(rule->prefixes);
  if (num_prefixes) {
    RedisModule_ReplyKV_Array(reply, "prefixes");
    for (int i = 0; i < num_prefixes; ++i) {
      REPLY_SIMPLE_SAFE(rule->prefixes[i]);
    }
    RedisModule_Reply_ArrayEnd(reply);
  }

  if (rule->filter_exp_str) {
    REPLY_KVSTR_SAFE("filter", rule->filter_exp_str);
  }

  if (rule->lang_default) {
    REPLY_KVSTR("default_language", RSLanguage_ToString(rule->lang_default));
  }

  if (rule->lang_field) {
    REPLY_KVSTR_SAFE("language_field", rule->lang_field);
  }

  if (rule->score_default) {
    REPLY_KVNUM("default_score", rule->score_default);
  }

  if (rule->score_field) {
    REPLY_KVSTR_SAFE("score_field", rule->score_field);
  }

  if (rule->payload_field) {
    REPLY_KVSTR_SAFE("payload_field", rule->payload_field);
  }

  if (rule->index_all) {
    REPLY_KVSTR_SAFE("indexes_all", "true");
  } else {
    REPLY_KVSTR_SAFE("indexes_all", "false");
  }

  RedisModule_Reply_MapEnd(reply); // index_definition
}

/* FT.INFO {index}
 *  Provide info and stats about an index
 */
int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) return RedisModule_WrongArity(ctx);

  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[1], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  bool has_map = RedisModule_HasMap(reply);

  RedisModule_Reply_Map(reply); // top

  REPLY_KVSTR_SAFE("index_name", sp->name);

  renderIndexOptions(reply, sp);
  renderIndexDefinitions(reply, sp);

  RedisModule_ReplyKV_Array(reply, "attributes"); // >attrbutes
  size_t geom_idx_sz = 0;

  for (int i = 0; i < sp->numFields; i++) {
    RedisModule_Reply_Map(reply); // >>field

    REPLY_KVSTR_SAFE("identifier", sp->fields[i].path);
    REPLY_KVSTR_SAFE("attribute", sp->fields[i].name);

    const FieldSpec *fs = &sp->fields[i];

    // RediSearch_api - No coverage
    if (fs->options & FieldSpec_Dynamic) {
      REPLY_KVSTR("type", "<DYNAMIC>");
      size_t ntypes = 0;

      RedisModule_ReplyKV_Array(reply, "types"); // >>>types
      for (size_t jj = 0; jj < INDEXFLD_NUM_TYPES; ++jj) {
        if (FIELD_IS(fs, INDEXTYPE_FROM_POS(jj))) {
          ntypes++;
          RedisModule_Reply_SimpleString(reply, FieldSpec_GetTypeNames(jj));
        }
      }
      RedisModule_Reply_ArrayEnd(reply); // >>>types
    } else {
      REPLY_KVSTR("type", FieldSpec_GetTypeNames(INDEXTYPE_TO_POS(fs->types)));
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      REPLY_KVNUM(SPEC_WEIGHT_STR, fs->ftWeight);
    }

    bool reply_SPEC_TAG_CASE_SENSITIVE_STR = false;
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[2] = {fs->tagOpts.tagSep, 0}; // Convert the separator to a C string
      REPLY_KVSTR_SAFE(SPEC_TAG_SEPARATOR_STR, buf);

      if (fs->tagOpts.tagFlags & TagField_CaseSensitive) {
        reply_SPEC_TAG_CASE_SENSITIVE_STR = true;
      }
    }

    if (FIELD_IS(fs, INDEXFLD_T_GEOMETRY)) {
      REPLY_KVSTR("coord_system", GeometryCoordsToName(fs->geometryOpts.geometryCoords));
      const GeometryIndex *idx = OpenGeometryIndex(ctx, sp, NULL, fs);
      const GeometryApi *api = GeometryApi_Get(idx);
      geom_idx_sz += api->report(idx);
    }

    if (FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
      VecSimParams vec_params = fs->vectorOpts.vecSimParams;
      VecSimAlgo field_algo = vec_params.algo;
      AlgoParams algo_params = vec_params.algoParams;

      if (field_algo == VecSimAlgo_TIERED) {
        VecSimParams *primary_params = algo_params.tieredParams.primaryIndexParams;
        if (primary_params->algo == VecSimAlgo_HNSWLIB) {
          REPLY_KVSTR("algorithm", VecSimAlgorithm_ToString(primary_params->algo));
          HNSWParams hnsw_params = primary_params->algoParams.hnswParams;
          REPLY_KVSTR("data_type", VecSimType_ToString(hnsw_params.type));
          REPLY_KVINT("dim", hnsw_params.dim);
          REPLY_KVSTR("distance_metric", VecSimMetric_ToString(hnsw_params.metric));
          REPLY_KVINT("M", hnsw_params.M);
          REPLY_KVINT("ef_construction", hnsw_params.efConstruction);
        }
      } else if (field_algo == VecSimAlgo_BF) {
        REPLY_KVSTR("algorithm", VecSimAlgorithm_ToString(field_algo));
        REPLY_KVSTR("data_type", VecSimType_ToString(algo_params.bfParams.type));
        REPLY_KVINT("dim", algo_params.bfParams.dim);
        REPLY_KVSTR("distance_metric", VecSimMetric_ToString(algo_params.bfParams.metric));
      }
    }

    if (has_map) {
      RedisModule_ReplyKV_Array(reply, "flags"); // >>>flags
    }

    if (reply_SPEC_TAG_CASE_SENSITIVE_STR) {
        RedisModule_Reply_SimpleString(reply, SPEC_TAG_CASE_SENSITIVE_STR);
    }
    if (FieldSpec_IsSortable(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_SORTABLE_STR);
    }
    if (FieldSpec_IsUnf(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_UNF_STR);
    }
    if (FieldSpec_IsNoStem(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_NOSTEM_STR);
    }
    if (!FieldSpec_IsIndexable(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_NOINDEX_STR);
    }
    if (FieldSpec_HasSuffixTrie(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_WITHSUFFIXTRIE_STR);
    }
    if (FieldSpec_IndexesEmpty(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_INDEXEMPTY_STR);
    }
    if (FieldSpec_IndexesMissing(fs)) {
      RedisModule_Reply_SimpleString(reply, SPEC_INDEXMISSING_STR);
    }

    if (has_map) {
      RedisModule_Reply_ArrayEnd(reply); // >>>flags
    }
    RedisModule_Reply_MapEnd(reply); // >>field
  }

  RedisModule_Reply_ArrayEnd(reply); // >attributes

  // Lock the spec
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RedisSearchCtx_LockSpecRead(&sctx);

  REPLY_KVINT("num_docs", sp->stats.numDocuments);
  REPLY_KVINT("max_doc_id", sp->docs.maxDocId);
  REPLY_KVINT("num_terms", sp->stats.numTerms);
  REPLY_KVINT("num_records", sp->stats.numRecords);
  REPLY_KVNUM("inverted_sz_mb", sp->stats.invertedSize / (float)0x100000);
  REPLY_KVNUM("vector_index_sz_mb", IndexSpec_VectorIndexSize(sp) / (float)0x100000);
  REPLY_KVINT("total_inverted_index_blocks", TotalIIBlocks);
  // REPLY_KVNUM("inverted_cap_mb", sp->stats.invertedCap / (float)0x100000);

  // REPLY_KVNUM("inverted_cap_ovh", 0);
  //(float)(sp->stats.invertedCap - sp->stats.invertedSize) / (float)sp->stats.invertedCap);

  REPLY_KVNUM("offset_vectors_sz_mb", sp->stats.offsetVecsSize / (float)0x100000);
  // REPLY_KVNUM("skip_index_size_mb", sp->stats.skipIndexesSize / (float)0x100000);
  // REPLY_KVNUM("score_index_size_mb", sp->stats.scoreIndexesSize / (float)0x100000);

  REPLY_KVNUM("doc_table_size_mb", sp->docs.memsize / (float)0x100000);
  REPLY_KVNUM("sortable_values_size_mb", sp->docs.sortablesSize / (float)0x100000);

  size_t dt_tm_size = TrieMap_MemUsage(sp->docs.dim.tm);
  REPLY_KVNUM("key_table_size_mb", dt_tm_size / (float)0x100000);
  size_t tags_overhead = IndexSpec_collect_tags_overhead(sp);
  REPLY_KVNUM("tag_overhead_sz_mb", tags_overhead / (float)0x100000);
  size_t text_overhead = IndexSpec_collect_text_overhead(sp);
  REPLY_KVNUM("text_overhead_sz_mb", text_overhead / (float)0x100000);
  REPLY_KVNUM("total_index_memory_sz_mb", IndexSpec_TotalMemUsage(sp, dt_tm_size,
    tags_overhead, text_overhead) / (float)0x100000);
  REPLY_KVNUM("geoshapes_sz_mb", geom_idx_sz / (float)0x100000);
  REPLY_KVNUM("records_per_doc_avg",
              (float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  REPLY_KVNUM("bytes_per_record_avg",
              (float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  REPLY_KVNUM("offsets_per_term_avg",
              (float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  REPLY_KVNUM("offset_bits_per_record_avg",
              8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);
  // TODO: remove this once "hash_indexing_failures" is deprecated
  // Legacy for not breaking changes
  REPLY_KVINT("hash_indexing_failures", sp->stats.indexError.error_count);
  REPLY_KVNUM("total_indexing_time", (float)(sp->stats.totalIndexTime / (float)CLOCKS_PER_MILLISEC));
  REPLY_KVINT("indexing", !!global_spec_scanner || sp->scan_in_progress);

  IndexesScanner *scanner = global_spec_scanner ? global_spec_scanner : sp->scanner;
  double percent_indexed = IndexesScanner_IndexedPercent(scanner, sp);
  REPLY_KVNUM("percent_indexed", percent_indexed);

  REPLY_KVINT("number_of_uses", sp->counter);

  REPLY_KVINT("cleaning", CleanInProgressOrPending());

  if (sp->gc) {
    RedisModule_ReplyKV_Map(reply, "gc_stats");
    GCContext_RenderStats(sp->gc, reply);
    RedisModule_Reply_MapEnd(reply);
  }

  Cursors_RenderStats(&g_CursorsList, &g_CursorsListCoord, sp, reply);

  // Unlock spec
  RedisSearchCtx_UnlockSpec(&sctx);

  if (sp->flags & Index_HasCustomStopwords) {
    ReplyWithStopWordsList(reply, sp->stopwords);
  }

  REPLY_KVMAP("dialect_stats");
  for (int dialect = MIN_DIALECT_VERSION; dialect <= MAX_DIALECT_VERSION; ++dialect) {
    char *dialect_i;
    rm_asprintf(&dialect_i, "dialect_%d", dialect);
    REPLY_KVINT(dialect_i, GET_DIALECT(sp->used_dialects, dialect));
    rm_free(dialect_i);
  }
  REPLY_MAP_END;

  // Global index error stats
  bool with_times = (argc > 2 && !strcmp(RedisModule_StringPtrLen(argv[2], NULL), WITH_INDEX_ERROR_TIME));
  RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
  IndexError_Reply(&sp->stats.indexError, reply, with_times);

  REPLY_KVARRAY("field statistics"); // Field statistics
  for (int i = 0; i < sp->numFields; i++) {
    const FieldSpec *fs = &sp->fields[i];
    FieldSpecInfo info = FieldSpec_GetInfo(fs);
    FieldSpecInfo_Reply(&info, reply, with_times);
  }
  REPLY_ARRAY_END; // >Field statistics

  RedisModule_Reply_MapEnd(reply); // top
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}
