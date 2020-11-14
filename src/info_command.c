
#include "redismodule.h"
#include "spec.h"
#include "inverted_index.h"
#include "cursor.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2

#define REPLY_KVSTR(n, k, v)                 \
  RedisModule_ReplyWithSimpleString(ctx, k); \
  RedisModule_ReplyWithSimpleString(ctx, v); \
  n += 2

//---------------------------------------------------------------------------------------------

static int renderIndexOptions(RedisModuleCtx *ctx, IndexSpec *sp) {

#define ADD_NEGATIVE_OPTION(flag, str)                        \
  if (!(sp->flags & flag)) {                                  \
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str)); \
    n++;                                                      \
  }

  RedisModule_ReplyWithSimpleString(ctx, "index_options");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int n = 0;
  ADD_NEGATIVE_OPTION(Index_StoreFreqs, SPEC_NOFREQS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreFieldFlags, SPEC_NOFIELDS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreTermOffsets, SPEC_NOOFFSETS_STR);
  if (sp->flags & Index_WideSchema) {
    RedisModule_ReplyWithSimpleString(ctx, SPEC_SCHEMA_EXPANDABLE_STR);
    n++;
  }
  RedisModule_ReplySetArrayLength(ctx, n);
  return 2;
}

//---------------------------------------------------------------------------------------------

/* FT.INFO {index}
 *  Provide info and stats about an index
 */
int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  if (argc < 2) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int n = 0;

  REPLY_KVSTR(n, "index_name", sp->name);

  n += renderIndexOptions(ctx, sp);

  RedisModule_ReplyWithSimpleString(ctx, "fields");
  RedisModule_ReplyWithArray(ctx, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithSimpleString(ctx, sp->fields[i].name);
    int nn = 1;
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
          RedisModule_ReplyWithSimpleString(ctx, SpecTypeNames[jj]);
        }
      }
      RedisModule_ReplySetArrayLength(ctx, ntypes);
    } else {
      REPLY_KVSTR(nn, "type", SpecTypeNames[INDEXTYPE_TO_POS(fs->types)]);
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      REPLY_KVNUM(nn, SPEC_WEIGHT_STR, fs->ftWeight);
    }

    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[2];
      sprintf(buf, "%c", fs->tagSep);
      REPLY_KVSTR(nn, SPEC_SEPARATOR_STR, buf);
    }
    if (FieldSpec_IsSortable(fs)) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_SORTABLE_STR);
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
    RedisModule_ReplySetArrayLength(ctx, nn);
  }
  n += 2;

  REPLY_KVNUM(n, "num_docs", sp->stats.numDocuments);
  REPLY_KVNUM(n, "max_doc_id", sp->docs.maxDocId);
  REPLY_KVNUM(n, "num_terms", sp->stats.numTerms);
  REPLY_KVNUM(n, "num_records", sp->stats.numRecords);
  REPLY_KVNUM(n, "inverted_sz_mb", sp->stats.invertedSize / (float)0x100000);
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

  if (sp->gc) {
    RedisModule_ReplyWithSimpleString(ctx, "gc_stats");
    sp->gc->RenderStats(ctx);
    n += 2;
  }

  RedisModule_ReplyWithSimpleString(ctx, "cursor_stats");
  RSCursors->RenderStats(sp->name, ctx);
  n += 2;

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords->ReplyWithStopWordsList(ctx);
    n += 2;
  }

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
