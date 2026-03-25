/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec_info.h"
#include "spec.h"
#include "search_disk.h"
#include "numeric_index.h"
#include "tag_index.h"
#include "trie/trie_type.h"
#include "triemap.h"
#include "info/index_error.h"
#include "gc.h"
#include "cursor.h"
#include "stopwords.h"
#include "doc_types.h"
#include "obfuscation/obfuscation_api.h"
#include "info/field_spec_info.h"
#include "reply_macros.h"
#include "util/arr.h"

//---------------------------------------------------------------------------------------------

double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp) {
  if (scanner || sp->scan_in_progress) {
    if (scanner) {
      size_t totalKeys = RedisModule_DbSize(ctx);
      return totalKeys > 0 ? (double)scanner->scannedKeys / totalKeys : 0;
    } else {
      return 0;
    }
  } else {
    return 1.0;
  }
}

size_t IndexSpec_collect_numeric_overhead(IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the numeric tree index
  size_t overhead = 0;
  for (size_t i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO)) {
      NumericRangeTree *rt = openNumericOrGeoIndex(sp, fs, DONT_CREATE_INDEX);
      // Numeric index was not initialized yet
      if (!rt) {
        continue;
      }

      overhead += NumericRangeTree_BaseSize();
    }
  }
  return overhead;
}

size_t IndexSpec_collect_tags_overhead(const IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the tags
  size_t overhead = 0;
  for (size_t i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      overhead += TagIndex_GetOverhead(fs);
    }
  }
  return overhead;
}

size_t IndexSpec_collect_text_overhead(const IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the text suffixes
  size_t overhead = 0;
  // Collect overhead from sp->terms
  overhead += TrieType_MemUsage(sp->terms);
  // Collect overhead from sp->suffix
  if (sp->suffix) {
    // TODO: Count the values' memory as well
    overhead += TrieType_MemUsage(sp->suffix);
  }
  return overhead;
}

size_t IndexSpec_TotalMemUsage(IndexSpec *sp, size_t doctable_tm_size, size_t tags_overhead,
  size_t text_overhead, size_t vector_overhead) {
  size_t res = 0;

  // For disk indexes, add storage + in-memory components.
  if (sp->diskSpec) {
    res += SearchDisk_CollectIndexMetrics(sp->diskSpec);
  }

  res += sp->docs.memsize;
  res += sp->docs.sortablesSize;
  res += doctable_tm_size ? doctable_tm_size : TrieMap_MemUsage(sp->docs.dim.tm);
  res += text_overhead ? text_overhead :  IndexSpec_collect_text_overhead(sp);
  res += tags_overhead ? tags_overhead : IndexSpec_collect_tags_overhead(sp);
  res += IndexSpec_collect_numeric_overhead(sp);
  res += sp->stats.invertedSize;
  res += sp->stats.offsetVecsSize;
  res += sp->stats.termsSize;
  res += vector_overhead;
  return res;
}

static void RSIndexStats_FromScoringStats(const ScoringIndexStats *scoring, RSIndexStats *stats) {
  stats->numDocs = scoring->numDocuments;
  stats->numTerms = scoring->numTerms;
  stats->avgDocLen = stats->numDocs ? (double)scoring->totalDocsLen / (double)scoring->numDocuments : 0;
}

/* Initialize some index stats that might be useful for scoring functions */
// Assuming the spec is properly locked before calling this function
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats) {
  RSIndexStats_FromScoringStats(&sp->stats.scoring, stats);
}

size_t IndexSpec_GetIndexErrorCount(const IndexSpec *sp) {
  return IndexError_ErrorCount(&sp->stats.indexError);
}

void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp, bool obfuscate, bool skip_unsafe_ops) {
  const char* indexName = IndexSpec_FormatName(sp, obfuscate);
  RedisModule_InfoAddSection(ctx, indexName);

  // Index flags
  if (sp->flags & ~(Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_StoreByteOffsets) || sp->flags & Index_WideSchema) {
    RedisModule_InfoBeginDictField(ctx, "index_options");
    if (!(sp->flags & (Index_StoreFreqs)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOFREQS_STR, "ON");
    if (!(sp->flags & (Index_StoreFieldFlags)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOFIELDS_STR, "ON");
    if (!(sp->flags & (Index_StoreTermOffsets | Index_StoreByteOffsets)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOOFFSETS_STR, "ON");
    if (!(sp->flags & (Index_StoreByteOffsets)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOHL_STR, "ON");
    if (sp->flags & Index_WideSchema)
      RedisModule_InfoAddFieldCString(ctx, SPEC_SCHEMA_EXPANDABLE_STR, "ON");
    RedisModule_InfoEndDictField(ctx);
  }

  // Index definition
  RedisModule_InfoBeginDictField(ctx, "index_definition");
  SchemaRule *rule = sp->rule;
  RedisModule_InfoAddFieldCString(ctx, "type", (char*)DocumentType_ToString(rule->type));
  if (rule->filter_exp_str) {
    const char *filter = HiddenString_GetUnsafe(rule->filter_exp_str, NULL);
    if (obfuscate) {
      RedisModule_InfoAddFieldCString(ctx, "filter", Obfuscate_Text(filter));
    } else {
      RedisModule_InfoAddFieldCString(ctx, "filter", filter);
    }
  }
  if (rule->lang_default)
    RedisModule_InfoAddFieldCString(ctx, "default_language", (char*)RSLanguage_ToString(rule->lang_default));
  if (rule->lang_field)
    RedisModule_InfoAddFieldCString(ctx, "language_field", rule->lang_field);
  if (rule->score_default)
    RedisModule_InfoAddFieldDouble(ctx, "default_score", rule->score_default);
  if (rule->score_field)
    RedisModule_InfoAddFieldCString(ctx, "score_field", rule->score_field);
  if (rule->payload_field)
    RedisModule_InfoAddFieldCString(ctx, "payload_field", rule->payload_field);
  // Prefixes
  int num_prefixes = array_len(rule->prefixes);
  if (num_prefixes && !skip_unsafe_ops) {
    const char *first_prefix = HiddenUnicodeString_GetUnsafe(rule->prefixes[0], NULL);
    if (first_prefix && first_prefix[0] != '\0') {
      // Skip when unsafe operations should be avoided (e.g., in signal handler) due to memory allocations
      arrayof(char) prefixes = array_new(char, 512);
      for (int i = 0; i < num_prefixes; ++i) {
        const char *prefix = HiddenUnicodeString_GetUnsafe(rule->prefixes[i], NULL);
        const char *prefix_to_use = obfuscate ? Obfuscate_Prefix(prefix) : prefix;
        prefixes = array_ensure_append_1(prefixes, "\"");
        prefixes = array_ensure_append_n(prefixes, prefix_to_use, strlen(prefix_to_use));
        prefixes = array_ensure_append_n(prefixes, "\",", 2);
      }
      prefixes[array_len(prefixes)-1] = '\0';
      RedisModule_InfoAddFieldCString(ctx, "prefixes", prefixes);
      array_free(prefixes);
    }
  }
  RedisModule_InfoEndDictField(ctx);

  // Attributes
  for (int i = 0; i < sp->numFields; i++) {
    const FieldSpec *fs = sp->fields + i;
    char title[28];
    snprintf(title, sizeof(title), "%s_%d", "field", (i+1));
    RedisModule_InfoBeginDictField(ctx, title);

    // if we can't perform allocation then use a local buffer to format the field name
    if (skip_unsafe_ops) {
      char path[MAX_OBFUSCATED_PATH_NAME];
      char name[MAX_OBFUSCATED_FIELD_NAME];
      Obfuscate_FieldPath(fs->index, path);
      Obfuscate_Field(fs->index, name);
      RedisModule_InfoAddFieldCString(ctx, "identifier", path);
      RedisModule_InfoAddFieldCString(ctx, "attribute", name);
    } else {
      const char *path = FieldSpec_FormatPath(fs, obfuscate);
      const char *name = FieldSpec_FormatName(fs, obfuscate);
      RedisModule_InfoAddFieldCString(ctx, "identifier", path);
      RedisModule_InfoAddFieldCString(ctx, "attribute", name);
      rm_free((void*)path);
      rm_free((void*)name);
    }

    if (fs->options & FieldSpec_Dynamic)
      RedisModule_InfoAddFieldCString(ctx, "type", "<DYNAMIC>");
    else
      RedisModule_InfoAddFieldCString(ctx, "type", (char*)FieldSpec_GetTypeNames(INDEXTYPE_TO_POS(fs->types)));

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT))
      RedisModule_InfoAddFieldDouble(ctx,  SPEC_WEIGHT_STR, fs->ftWeight);
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[4];
      snprintf(buf, sizeof(buf), "\"%c\"", fs->tagOpts.tagSep);
      RedisModule_InfoAddFieldCString(ctx, SPEC_TAG_SEPARATOR_STR, buf);
    }
    if (FieldSpec_IsSortable(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_SORTABLE_STR, "ON");
    if (FieldSpec_IsNoStem(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOSTEM_STR, "ON");
    if (!FieldSpec_IsIndexable(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOINDEX_STR, "ON");

    RedisModule_InfoEndDictField(ctx);
  }

  // More properties
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_docs", sp->stats.scoring.numDocuments);

  RedisModule_InfoBeginDictField(ctx, "index_properties");
  RedisModule_InfoAddFieldULongLong(ctx, "max_doc_id", sp->docs.maxDocId);
  RedisModule_InfoAddFieldLongLong(ctx, "num_terms", sp->stats.scoring.numTerms);
  RedisModule_InfoAddFieldLongLong(ctx, "num_records", sp->stats.numRecords);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_properties_in_mb");
  RedisModule_InfoAddFieldDouble(ctx, "inverted_size", sp->stats.invertedSize / (float)0x100000);
  if (!skip_unsafe_ops) {
    // Skip when unsafe - calls dictFetchValue which can trigger dict rehashing with rm_free
    RedisModule_InfoAddFieldDouble(ctx, "vector_index_size", IndexSpec_VectorIndexesSize(sp) / (float)0x100000);
  }
  RedisModule_InfoAddFieldDouble(ctx, "offset_vectors_size", sp->stats.offsetVecsSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "doc_table_size", sp->docs.memsize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "sortable_values_size", sp->docs.sortablesSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "key_table_size", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  if (!skip_unsafe_ops) {
    // Skip when unsafe - tag overhead calls dictFetchValue which can trigger dict rehashing with rm_free
    RedisModule_InfoAddFieldDouble(ctx, "tag_overhead_size_mb", IndexSpec_collect_tags_overhead(sp) / (float)0x100000);
    RedisModule_InfoAddFieldDouble(ctx, "text_overhead_size_mb", IndexSpec_collect_text_overhead(sp) / (float)0x100000);
    RedisModule_InfoAddFieldDouble(ctx, "total_index_memory_sz_mb", IndexSpec_TotalMemUsage(sp, 0, 0, 0, 0) / (float)0x100000);
  }
  RedisModule_InfoEndDictField(ctx);

  // TotalIIBlocks is safe - just an atomic read, no locks or allocations
  RedisModule_InfoAddFieldULongLong(ctx, "total_inverted_index_blocks", TotalIIBlocks());

  RedisModule_InfoBeginDictField(ctx, "index_properties_averages");
  RedisModule_InfoAddFieldDouble(ctx, "records_per_doc_avg",(float)sp->stats.numRecords / (float)sp->stats.scoring.numDocuments);
  RedisModule_InfoAddFieldDouble(ctx, "bytes_per_record_avg",(float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  RedisModule_InfoAddFieldDouble(ctx, "offsets_per_term_avg",(float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  RedisModule_InfoAddFieldDouble(ctx, "offset_bits_per_record_avg",8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_failures");
  RedisModule_InfoAddFieldLongLong(ctx, "hash_indexing_failures", sp->stats.indexError.error_count);
  RedisModule_InfoAddFieldLongLong(ctx, "indexing", !!global_spec_scanner || sp->scan_in_progress);
  RedisModule_InfoEndDictField(ctx);

  // Garbage collector - safe to call, just reads struct fields
  if (sp->gc) {
    GCContext_RenderStatsForInfo(sp->gc, ctx);
  }

  // Cursor stats - safe to call, uses trylock and won't deadlock
  Cursors_RenderStatsForInfo(&g_CursorsList, &g_CursorsListCoord, sp, ctx);

  // Stop words
  if (!skip_unsafe_ops && (sp->flags & Index_HasCustomStopwords)) {
    // Skip when unsafe operations should be avoided - AddStopWordsListToInfo allocates memory
    AddStopWordsListToInfo(ctx, sp->stopwords);
  }
}
