/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "info_command.h"
#include "resp3.h"
#include "info/field_spec_info.h"

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
    {.name = "current_hz", .type = InfoField_DoubleAverage},
    {.name = "bytes_collected", .type = InfoField_WholeSum},
    {.name = "effectiv_cycles_rate", .type = InfoField_DoubleAverage}};

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
      double avg;
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
  FieldSpecInfo *fieldSpecInfo_arr;
  IndexError indexError;
  InfoValue gcValues[NUM_GC_FIELDS_SPEC];
  InfoValue cursorValues[NUM_CURSOR_FIELDS_SPEC];
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
                           InfoFieldSpec *specs, size_t numFields, int onlyScalars);

/** Reply with a KV array, the values are emitted per name and type */
static void replyKvArray(RedisModule_Reply *reply, InfoFields *fields, InfoValue *values,
                         InfoFieldSpec *specs, size_t numFields);

// Writes field data to the target
static void convertField(InfoValue *dst, MRReply *src, const InfoFieldSpec *spec) {
  int type = spec->type;

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
      dst->u.avg.avg += d;
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
void handleFieldStatistics(MRReply *src, InfoFields *fields) {
  // Input validations
  RedisModule_Assert(src && fields);
  RedisModule_Assert(MRReply_Type(src) == MR_REPLY_ARRAY);

  size_t len = MRReply_Length(src);
  if (!fields->fieldSpecInfo_arr) {
    // Lazy initialization
    fields->fieldSpecInfo_arr = array_new(FieldSpecInfo, len);
    for(size_t i=0; i<len; i++) {
      FieldSpecInfo fieldSpecInfo = FieldSpecInfo_Init();
      fields->fieldSpecInfo_arr = array_append(fields->fieldSpecInfo_arr, fieldSpecInfo);
    }
  }

  for(size_t i = 0; i < len; i++) {
    MRReply *serilizedFieldSpecInfo = MRReply_ArrayElement(src, i);
    FieldSpecInfo fieldSpecInfo = FieldSpecInfo_Deserialize(serilizedFieldSpecInfo);
    FieldSpecInfo_OpPlusEquals(&fields->fieldSpecInfo_arr[i], &fieldSpecInfo);
  }
}

static void handleIndexError(InfoFields *fields, MRReply *src) {
  // Check if indexError is initialized
  if(!IndexError_LastError(&fields->indexError)) {
    fields->indexError = IndexError_Init();
  }
  IndexError indexError = IndexError_Deserialize(src);
  IndexError_OpPlusEquals(&fields->indexError, &indexError);
}

// Handle fields which aren't InfoValue types
static void handleSpecialField(InfoFields *fields, const char *name, MRReply *value) {
  if (!strcmp(name, "index_name")) {
    if (fields->indexName) {
      return;
    }
    fields->indexName = MRReply_String(value, &fields->indexNameLen);
    const char *curlyIdx = index(fields->indexName, '{');
    if (curlyIdx != NULL) {
      fields->indexNameLen = curlyIdx - fields->indexName;
    }
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
  } else if (!strcmp(name, "gc_stats")) {
    processKvArray(fields, value, fields->gcValues, gcSpecs, NUM_GC_FIELDS_SPEC, 1);

  } else if (!strcmp(name, "cursor_stats")) {
    processKvArray(fields, value, fields->cursorValues, cursorSpecs, NUM_CURSOR_FIELDS_SPEC, 1);
  } else if (!strcmp(name, "dialect_stats")) {
    processKvArray(fields, value, fields->dialectValues, dialectSpecs, NUM_DIALECT_FIELDS_SPEC, 1);
  } else if (!strcmp(name, "field statistics")) {
    handleFieldStatistics(value, fields);
  } else if (!strcmp(name, IndexError_ObjectName)) {
    handleIndexError(fields, value);
  }
}

static void processKvArray(InfoFields *fields, MRReply *array, InfoValue *dsts, InfoFieldSpec *specs,
                           size_t numFields, int onlyScalarValues) {
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

    for (size_t jj = 0; jj < numFields; ++jj) {
      const char *name = specs[jj].name;
      if (!strcmp(s, name)) {
        convertField(dsts + jj, value, specs + jj);
        goto next_elem;
      }
    }

    if (!onlyScalarValues) {
      handleSpecialField(fields, s, value);
    }

next_elem:
      continue;
  }
}

static void cleanInfoReply(InfoFields *fields) {
  if (fields->fieldSpecInfo_arr) {
    // Clear the info fields
    for (size_t i = 0; i < array_len(fields->fieldSpecInfo_arr); i++) {
      FieldSpecInfo_Clear(&fields->fieldSpecInfo_arr[i]);
    }
    array_free(fields->fieldSpecInfo_arr);
    fields->fieldSpecInfo_arr = NULL;
  }
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
        RedisModule_ReplyKV_Double(reply, key, source->u.avg.avg / source->u.avg.count);
      } else {
        RedisModule_ReplyKV_Double(reply, key, 0);
      }
    } else {
      RedisModule_ReplyKV_Null(reply, key);
    }
  }
}

static void generateFieldsReply(InfoFields *fields, RedisModule_Reply *reply) {
  RedisModule_Reply_Map(reply);

  // Respond with the name, schema, and options
  if (fields->indexName) {
    RedisModule_ReplyKV_StringBuffer(reply, "index_name", fields->indexName, fields->indexNameLen);
  }
  
  if (fields->indexDef) {
    RedisModule_ReplyKV_MRReply(reply, "index_definition", fields->indexDef);
  }

  if (fields->indexSchema) {
    RedisModule_ReplyKV_MRReply(reply, "attributes", fields->indexSchema);
  }

  if (fields->fieldSpecInfo_arr) {
      RedisModule_ReplyKV_Array(reply, "field statistics"); //Field statistics
      for (size_t i = 0; i < array_len(fields->fieldSpecInfo_arr); ++i) {
        FieldSpecInfo_Reply(&fields->fieldSpecInfo_arr[i], reply);
      }
      RedisModule_Reply_ArrayEnd(reply); // >Field statistics
  }

  if (fields->indexOptions) {
    RedisModule_ReplyKV_MRReply(reply, "index_options", fields->indexOptions);
  }

  RedisModule_ReplyKV_Map(reply, "gc_stats");
  replyKvArray(reply, fields, fields->gcValues, gcSpecs, NUM_GC_FIELDS_SPEC);
  RedisModule_Reply_MapEnd(reply);

  RedisModule_ReplyKV_Map(reply, "cursor_stats");
  replyKvArray(reply, fields, fields->cursorValues, cursorSpecs, NUM_CURSOR_FIELDS_SPEC);
  RedisModule_Reply_MapEnd(reply);

  RedisModule_ReplyKV_Map(reply, "dialect_stats");
  replyKvArray(reply, fields, fields->dialectValues, dialectSpecs, NUM_DIALECT_FIELDS_SPEC);
  RedisModule_Reply_MapEnd(reply);

  replyKvArray(reply, fields, fields->toplevelValues, toplevelSpecs_g, NUM_FIELDS_SPEC);

    
  // Global index error stats
  RedisModule_Reply_SimpleString(reply, IndexError_ObjectName);
  IndexError_Reply(&fields->indexError, reply);


  RedisModule_Reply_MapEnd(reply);
}

int InfoReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {
  // Summarize all aggregate replies
  InfoFields fields = {0};
  size_t numErrored = 0;
  MRReply *firstError = NULL;
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  if (count == 0) {
    return RedisModule_ReplyWithError(ctx, "ERR no responses received");
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  for (size_t ii = 0; ii < count; ++ii) {
    if (MRReply_Type(replies[ii]) == MR_REPLY_ERROR) {
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

    if (MRReply_Type(replies[ii]) != MR_REPLY_ARRAY && MRReply_Type(replies[ii]) != MR_REPLY_MAP) {
      continue;  // Ooops!
    }

    int type = MRReply_Type(replies[ii]);
    if (type == MR_REPLY_ARRAY || type == MR_REPLY_MAP) {
      size_t numElems = MRReply_Length(replies[ii]);
      if (numElems % 2 != 0) {
        printf("Uneven INFO Reply!!!?\n");
      }
    }
    processKvArray(&fields, replies[ii], fields.toplevelValues, toplevelSpecs_g, NUM_FIELDS_SPEC, 0);
  }

  // Now we've received all the replies.
  if (numErrored == count) {
    // Reply with error
    MR_ReplyWithMRReply(reply, firstError);
  } else {
    generateFieldsReply(&fields, reply);
  }

  cleanInfoReply(&fields);
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}
