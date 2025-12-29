/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "info_command.h"
#include "resp3.h"
#include "info/field_spec_info.h"
#include "../src/reply_macros.h"

// Type of field returned in INFO
typedef enum {
  InfoField_WholeSum,
  InfoField_DoubleSum,
  InfoField_DoubleAverage,
  InfoField_Max,
} InfoFieldType;

// Field specification
typedef struct {
  const char *name;
  InfoFieldType type;
} InfoFieldSpec;

static InfoFieldSpec toplevelSpecs_g[] = {
    {.name = "num_docs", .type = InfoField_WholeSum},
    {.name = "max_doc_id", .type = InfoField_Max},
    {.name = "num_terms", .type = InfoField_WholeSum},
    {.name = "num_records", .type = InfoField_WholeSum},
    {.name = "inverted_sz_mb", .type = InfoField_DoubleSum},
    {.name = "total_inverted_index_blocks", .type = InfoField_WholeSum},
    {.name = "vector_index_sz_mb", .type = InfoField_DoubleSum},
    {.name = "offset_vectors_sz_mb", .type = InfoField_DoubleSum},
    {.name = "doc_table_size_mb", .type = InfoField_DoubleSum},
    {.name = "sortable_values_size_mb", .type = InfoField_DoubleSum},
    {.name = "key_table_size_mb", .type = InfoField_DoubleSum},
    {.name = "tag_overhead_sz_mb", .type = InfoField_DoubleSum},
    {.name = "text_overhead_sz_mb", .type = InfoField_DoubleSum},
    {.name = "total_index_memory_sz_mb", .type = InfoField_DoubleSum},
    {.name = "geoshapes_sz_mb", .type = InfoField_DoubleSum},
    {.name = "records_per_doc_avg", .type = InfoField_DoubleAverage},
    {.name = "bytes_per_record_avg", .type = InfoField_DoubleAverage},
    {.name = "offsets_per_term_avg", .type = InfoField_DoubleAverage},
    {.name = "offset_bits_per_record_avg", .type = InfoField_DoubleAverage},
    {.name = "indexing", .type = InfoField_WholeSum},
    {.name = "percent_indexed", .type = InfoField_DoubleAverage},
    {.name = "hash_indexing_failures", .type = InfoField_WholeSum},
    {.name = "number_of_uses", .type = InfoField_Max},
    {.name = "cleaning", .type = InfoField_WholeSum}};

static InfoFieldSpec gcSpecs[] = {
    {.name = "bytes_collected", .type = InfoField_WholeSum},
    {.name = "total_ms_run", .type = InfoField_WholeSum},
    {.name = "total_cycles", .type = InfoField_WholeSum},
    {.name = "average_cycle_time_ms", .type = InfoField_DoubleAverage},
    {.name = "last_run_time_ms", .type = InfoField_Max},
    {.name = "gc_numeric_trees_missed", .type = InfoField_WholeSum},
    {.name = "gc_blocks_denied", .type = InfoField_WholeSum}};

static InfoFieldSpec cursorSpecs[] = {
    {.name = "global_idle", .type = InfoField_WholeSum},
    {.name = "global_total", .type = InfoField_WholeSum},
    {.name = "index_capacity", .type = InfoField_WholeSum},
    {.name = "index_total", .type = InfoField_WholeSum},
};

static InfoFieldSpec dialectSpecs[] = {
    {.name = "dialect_1", .type = InfoField_Max},
    {.name = "dialect_2", .type = InfoField_Max},
    {.name = "dialect_3", .type = InfoField_Max},
    {.name = "dialect_4", .type = InfoField_Max},
};

#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof(*arr))
#define NUM_FIELDS_SPEC (ARRAY_SIZE(toplevelSpecs_g))
#define NUM_GC_FIELDS_SPEC (ARRAY_SIZE(gcSpecs))
#define NUM_CURSOR_FIELDS_SPEC (ARRAY_SIZE(cursorSpecs))
#define NUM_DIALECT_FIELDS_SPEC (ARRAY_SIZE(dialectSpecs))

// Variant value type
typedef struct {
  int isSet;
  union {
    size_t total_l;
    double total_d;
    struct {
      double sum;
      double count;
    } avg;
    struct {
      char *str;
      size_t len;
    } str;
  } u;
} InfoValue;

// State object for parsing and replying INFO
typedef struct {
  const char *indexName;
  size_t indexNameLen;
  MRReply *indexDef;
  MRReply *indexSchema;
  MRReply *indexOptions;
  size_t *errorIndexes;
  InfoValue toplevelValues[NUM_FIELDS_SPEC];
  AggregatedFieldSpecInfo *fieldSpecInfo_arr;
  IndexError indexError;
  InfoValue gcValues[NUM_GC_FIELDS_SPEC];
  InfoValue cursorValues[NUM_CURSOR_FIELDS_SPEC];
  MRReply *stopWordList;
  InfoValue dialectValues[NUM_DIALECT_FIELDS_SPEC];
} InfoFields;

/**
 * Read a single KV array (i.e. array with alternating key-value entries)
 * - array is the source array type
 * - dsts is the destination
 * - specs describes the destination types and corresponding field names
 * - numFields - the number of specs and dsts
 * - onlyScalars - because special handling is done in toplevel mode
 */
static void processKvArray(InfoFields *fields, MRReply *array, InfoValue *dsts,
                           InfoFieldSpec *specs, size_t numFields, int onlyScalars, QueryError *error);

/** Reply with a KV array, the values are emitted per name and type */
static void replyKvArray(RedisModule_Reply *reply, InfoFields *fields, InfoValue *values,
                         InfoFieldSpec *specs, size_t numFields);

// Writes field data to the target
static void convertField(InfoValue *dst, MRReply *src, InfoFieldType type) {
  switch (type) {
    case InfoField_WholeSum: {
      long long tmp;
      MRReply_ToInteger(src, &tmp);
      dst->u.total_l += tmp;
      break;
    }
    case InfoField_DoubleSum: {
      double d;
      MRReply_ToDouble(src, &d);
      dst->u.total_d += d;
      break;
    }
    case InfoField_DoubleAverage: {
      dst->u.avg.count++;
      double d;
      MRReply_ToDouble(src, &d);
      dst->u.avg.sum += d;
      break;
    }
    case InfoField_Max: {
      long long newVal;
      MRReply_ToInteger(src, &newVal);
      if (dst->u.total_l < newVal) {
        dst->u.total_l = newVal;
      }
      break;
    }
  }
  dst->isSet = 1;
}

// Extract an array of FieldSpecInfo from MRReply
void handleFieldStatistics(InfoFields *fields, MRReply *src, QueryError *error) {
  // Input validations
  RS_ASSERT(src && fields);
  RS_ASSERT(MRReply_Type(src) == MR_REPLY_ARRAY);

  size_t len = MRReply_Length(src);
  if (!fields->fieldSpecInfo_arr) {
    // Lazy initialization
    fields->fieldSpecInfo_arr = array_newlen(AggregatedFieldSpecInfo, len);
    for (size_t i = 0; i < len; i++) {
      fields->fieldSpecInfo_arr[i] = AggregatedFieldSpecInfo_Init();
    }
  }

  // Something went wrong (number of fields mismatch)
  if (array_len(fields->fieldSpecInfo_arr) != len) {
    QueryError_SetError(error, QUERY_ERROR_CODE_BAD_VAL, "Inconsistent index state");
    return;
  }

  for (size_t i = 0; i < len; i++) {
    MRReply *serializedFieldSpecInfo = MRReply_ArrayElement(src, i);
    AggregatedFieldSpecInfo fieldSpecInfo = AggregatedFieldSpecInfo_Deserialize(serializedFieldSpecInfo);
    AggregatedFieldSpecInfo_Combine(&fields->fieldSpecInfo_arr[i], &fieldSpecInfo);
    AggregatedFieldSpecInfo_Clear(&fieldSpecInfo); // Free Resources
  }
}

static void handleIndexError(InfoFields *fields, MRReply *src) {
  // Check if indexError is initialized
  if (!IndexError_LastError(&fields->indexError)) {
    fields->indexError = IndexError_Init();
  }
  IndexError indexError = IndexError_Deserialize(src, INDEX_ERROR_WITH_OOM_STATUS);
  IndexError_Combine(&fields->indexError, &indexError);
  IndexError_Clear(indexError); // Free Resources
}

struct InfoFieldTypeAndValue {
  InfoValue *value;
  InfoFieldType type;
};

static struct InfoFieldTypeAndValue findInfoTypeAndValue(InfoValue *values, InfoFieldSpec *specs, size_t numFields, const char *name) {
  struct InfoFieldTypeAndValue result = {.value = NULL, .type = InfoField_WholeSum};
  for (size_t ii = 0; ii < numFields; ++ii) {
    if (!strcmp(specs[ii].name, name)) {
      result.value = values + ii;
      result.type = specs[ii].type;
      return result;
    }
  }
  return result;
}

// Recompute the average cycle time based on total cycles and total ms run
static void recomputeAverageCycleTimeMs(InfoValue* gcValues, InfoFieldSpec* gcSpecs, size_t numFields) {
  struct InfoFieldTypeAndValue avg_cycle_time_ms = findInfoTypeAndValue(gcValues, gcSpecs, numFields, "average_cycle_time_ms");
  if (!avg_cycle_time_ms.value) {
    return;
  }
  avg_cycle_time_ms.value->isSet = 0;

  struct InfoFieldTypeAndValue total_cycles = findInfoTypeAndValue(gcValues, gcSpecs, numFields, "total_cycles");
  struct InfoFieldTypeAndValue total_ms = findInfoTypeAndValue(gcValues, gcSpecs, numFields, "total_ms_run");
  if (total_cycles.value && total_ms.value && avg_cycle_time_ms.type == InfoField_DoubleAverage) {
    avg_cycle_time_ms.value->u.avg.count = total_cycles.value->u.total_l;
    avg_cycle_time_ms.value->u.avg.sum = total_ms.value->u.total_l;
    avg_cycle_time_ms.value->isSet = 1;
  }
}

// Handle fields which aren't InfoValue types
static void handleSpecialField(InfoFields *fields, const char *name, MRReply *value, QueryError *error) {
  if (!strcmp(name, "index_name")) {
    if (fields->indexName) {
      return;
    }
    fields->indexName = MRReply_String(value, &fields->indexNameLen);
  } else if (!strcmp(name, "attributes")) {
    if (!fields->indexSchema) {
      fields->indexSchema = value;
    }
  } else if (!strcmp(name, "index_definition")) {
    if (!fields->indexDef) {
      fields->indexDef = value;
    }
  } else if (!strcmp(name, "index_options")) {
    if (!fields->indexOptions) {
      fields->indexOptions = value;
    }
  } else if (!strcmp(name, "stopwords_list")) {
    if (!fields->stopWordList) {
      fields->stopWordList = value;
    }
  } else if (!strcmp(name, "gc_stats")) {
    processKvArray(fields, value, fields->gcValues, gcSpecs, NUM_GC_FIELDS_SPEC, 1, error);
    recomputeAverageCycleTimeMs(fields->gcValues, gcSpecs, NUM_GC_FIELDS_SPEC);
  } else if (!strcmp(name, "cursor_stats")) {
    processKvArray(fields, value, fields->cursorValues, cursorSpecs, NUM_CURSOR_FIELDS_SPEC, 1, error);
  } else if (!strcmp(name, "dialect_stats")) {
    processKvArray(fields, value, fields->dialectValues, dialectSpecs, NUM_DIALECT_FIELDS_SPEC, 1, error);
  } else if (!strcmp(name, "field statistics")) {
    handleFieldStatistics(fields, value, error);
  } else if (!strcmp(name, IndexError_ObjectName)) {
    handleIndexError(fields, value);
  }
}

static void processKvArray(InfoFields *fields, MRReply *array, InfoValue *dsts, InfoFieldSpec *specs,
                           size_t numFields, int onlyScalarValues, QueryError *error) {
  if (MRReply_Type(array) != MR_REPLY_ARRAY && MRReply_Type(array) != MR_REPLY_MAP) {
    return;
  }
  size_t numElems = MRReply_Length(array);
  if (numElems % 2 != 0) {
    return;
  }

  for (size_t ii = 0; ii < numElems; ii += 2) {
    // @@ MapElementByIndex
    MRReply *value = MRReply_ArrayElement(array, ii + 1);
    const char *s = MRReply_String(MRReply_ArrayElement(array, ii), NULL);
    struct InfoFieldTypeAndValue field = findInfoTypeAndValue(dsts, specs, numFields, s);
    if (field.value) {
      convertField(field.value, value, field.type);
    } else if (!onlyScalarValues) {
      handleSpecialField(fields, s, value, error);
      if (QueryError_HasError(error)) {
        break;
      }
    }
  }
}

static void cleanInfoReply(InfoFields *fields) {
  if (fields->fieldSpecInfo_arr) {
    // Clear the info fields
    for (size_t i = 0; i < array_len(fields->fieldSpecInfo_arr); i++) {
      AggregatedFieldSpecInfo_Clear(&fields->fieldSpecInfo_arr[i]);
    }
    array_free(fields->fieldSpecInfo_arr);
    fields->fieldSpecInfo_arr = NULL;
  }
  IndexError_Clear(fields->indexError);
  rm_free(fields->errorIndexes);
}

static void replyKvArray(RedisModule_Reply *reply, InfoFields *fields, InfoValue *values,
                           InfoFieldSpec *specs, size_t numSpecs) {
  for (size_t ii = 0; ii < numSpecs; ++ii) {
    InfoValue *source = values + ii;
    if (!source->isSet) {
      continue;
    }

    const char *key = specs[ii].name;

    int type = specs[ii].type;
    if (type == InfoField_WholeSum || type == InfoField_Max) {
      RedisModule_ReplyKV_LongLong(reply, key, source->u.total_l);
    } else if (type == InfoField_DoubleSum) {
      RedisModule_ReplyKV_Double(reply, key, source->u.total_d);
    } else if (type == InfoField_DoubleAverage) {
      if (source->u.avg.count) {
        RedisModule_ReplyKV_Double(reply, key, source->u.avg.sum / source->u.avg.count);
      } else {
        RedisModule_ReplyKV_Double(reply, key, 0);
      }
    } else {
      RedisModule_ReplyKV_Null(reply, key);
    }
  }
}

static void generateFieldsReply(InfoFields *fields, RedisModule_Reply *reply, bool obfuscate) {
  RedisModule_Reply_Map(reply);

  // Respond with the name, schema, and options
  if (fields->indexName) {
    REPLY_KVSTR_SAFE("index_name", fields->indexName);
  }

  if (fields->indexOptions) {
    RedisModule_ReplyKV_MRReply(reply, "index_options", fields->indexOptions);
  }

  if (fields->indexDef) {
    RedisModule_ReplyKV_MRReply(reply, "index_definition", fields->indexDef);
  }

  if (fields->indexSchema) {
    RedisModule_ReplyKV_MRReply(reply, "attributes", fields->indexSchema);
  }

  RedisModule_ReplyKV_Map(reply, "gc_stats");
  replyKvArray(reply, fields, fields->gcValues, gcSpecs, NUM_GC_FIELDS_SPEC);
  RedisModule_Reply_MapEnd(reply);

  RedisModule_ReplyKV_Map(reply, "cursor_stats");
  replyKvArray(reply, fields, fields->cursorValues, cursorSpecs, NUM_CURSOR_FIELDS_SPEC);
  RedisModule_Reply_MapEnd(reply);

  if (fields->stopWordList) {
    RedisModule_ReplyKV_MRReply(reply, "stopwords_list", fields->stopWordList);
  }

  RedisModule_ReplyKV_Map(reply, "dialect_stats");
  replyKvArray(reply, fields, fields->dialectValues, dialectSpecs, NUM_DIALECT_FIELDS_SPEC);
  RedisModule_Reply_MapEnd(reply);

  replyKvArray(reply, fields, fields->toplevelValues, toplevelSpecs_g, NUM_FIELDS_SPEC);


  // Global index error stats
  RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
  IndexError_Reply(&fields->indexError, reply, 0, obfuscate, INDEX_ERROR_WITH_OOM_STATUS);

  if (fields->fieldSpecInfo_arr) {
    RedisModule_ReplyKV_Array(reply, "field statistics"); //Field statistics
    for (size_t i = 0; i < array_len(fields->fieldSpecInfo_arr); ++i) {
      AggregatedFieldSpecInfo_Reply(&fields->fieldSpecInfo_arr[i], reply, 0, obfuscate);
    }
    RedisModule_Reply_ArrayEnd(reply); // >Field statistics
  }


  RedisModule_Reply_MapEnd(reply);
}

int InfoReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {
  // Summarize all aggregate replies
  InfoFields fields = { .indexError = IndexError_Init() };
  size_t numErrored = 0;
  MRReply *firstError = NULL;
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  if (count == 0) {
    return RedisModule_ReplyWithError(ctx, "RQE_CLUSTER_NO_RESPONSES: ERR no responses received");
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  QueryError error = QueryError_Default();

  for (size_t ii = 0; ii < count; ++ii) {
    int type = MRReply_Type(replies[ii]);
    if (type == MR_REPLY_ERROR) {
      if (!fields.errorIndexes) {
        fields.errorIndexes = rm_calloc(count, sizeof(*fields.errorIndexes));
      }
      fields.errorIndexes[ii] = 1;
      numErrored++;
      if (!firstError) {
        firstError = replies[ii];
      }
      continue;
    }

    if (type != MR_REPLY_ARRAY && type != MR_REPLY_MAP) {
      continue;  // Ooops!
    }

    size_t numElems = MRReply_Length(replies[ii]);
    RS_ASSERT(numElems % 2 == 0);
    processKvArray(&fields, replies[ii], fields.toplevelValues, toplevelSpecs_g, NUM_FIELDS_SPEC, 0, &error);
    if (QueryError_HasError(&error)) {
      break;
    }
  }

  // Now we've received all the replies.
  if (numErrored == count) {
    // Reply with error
    MR_ReplyWithMRReply(reply, firstError);
  } else if (QueryError_HasError(&error)) {
    // Reply with error
    RedisModule_Reply_Error(reply, QueryError_GetUserError(&error));
  } else {
    generateFieldsReply(&fields, reply, false);
  }

  QueryError_ClearError(&error);
  cleanInfoReply(&fields);
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}
