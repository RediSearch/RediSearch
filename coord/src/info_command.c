#include "info_command.h"

// Type of field returned in INFO
typedef enum {
  InfoField_WholeSum,
  InfoField_DoubleSum,
  InfoField_DoubleAverage,
  InfoField_Max
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
    {.name = "offset_vectors_sz_mb", .type = InfoField_DoubleSum},
    {.name = "doc_table_size_mb", .type = InfoField_DoubleSum},
    {.name = "key_table_size_mb", .type = InfoField_DoubleSum},
    {.name = "records_per_doc_avg", .type = InfoField_DoubleAverage},
    {.name = "bytes_per_record_avg", .type = InfoField_DoubleAverage},
    {.name = "offsets_per_term_avg", .type = InfoField_DoubleAverage},
    {.name = "offset_bits_per_record_avg", .type = InfoField_DoubleAverage},
    {.name = "indexing", .type = InfoField_WholeSum},
    {.name = "percent_indexed", .type = InfoField_DoubleAverage},
    {.name = "hash_indexing_failures", .type = InfoField_WholeSum}};

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

#define NUM_FIELDS_SPEC (sizeof(toplevelSpecs_g) / sizeof(InfoFieldSpec))
#define NUM_GC_FIELDS_SPEC (sizeof(gcSpecs) / sizeof(InfoFieldSpec))
#define NUM_CURSOR_FIELDS_SPEC (sizeof(cursorSpecs) / sizeof(InfoFieldSpec))

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
  InfoValue gcValues[NUM_GC_FIELDS_SPEC];
  InfoValue cursorValues[NUM_CURSOR_FIELDS_SPEC];
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
static size_t replyKvArray(InfoFields *fields, RedisModuleCtx *ctx, InfoValue *values,
                           InfoFieldSpec *specs, size_t numFields);

// Writes field data to the target
static void convertField(InfoValue *dst, MRReply *src, const InfoFieldSpec *spec) {
  int type = spec->type;

  if (type == InfoField_WholeSum) {
    long long tmp;
    MRReply_ToInteger(src, &tmp);
    dst->u.total_l += tmp;
  } else if (type == InfoField_DoubleSum) {
    double d;
    MRReply_ToDouble(src, &d);
    dst->u.total_d += d;
  } else if (type == InfoField_DoubleAverage) {
    dst->u.avg.count++;
    double d;
    MRReply_ToDouble(src, &d);
    dst->u.avg.avg += d;
  } else if (type == InfoField_Max) {
    long long newVal;
    MRReply_ToInteger(src, &newVal);
    if (dst->u.total_l < newVal) {
      dst->u.total_l = newVal;
    }
  }
  dst->isSet = 1;
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
  }
}

static void processKvArray(InfoFields *ctx, MRReply *array, InfoValue *dsts, InfoFieldSpec *specs,
                           size_t numFields, int onlyScalarValues) {
  if (MRReply_Type(array) != MR_REPLY_ARRAY) {
    return;
  }
  size_t numElems = MRReply_Length(array);
  if (numElems % 2 != 0) {
    return;
  }

  for (size_t ii = 0; ii < numElems; ii += 2) {
    MRReply *value = MRReply_ArrayElement(array, ii + 1);
    const char *s = MRReply_String(MRReply_ArrayElement(array, ii), NULL);

    for (size_t jj = 0; jj < numFields; ++jj) {
      if (!strcmp(s, specs[jj].name)) {
        convertField(dsts + jj, value, specs + jj);
        goto next_elem;
      }
    }

    if (!onlyScalarValues) {
      handleSpecialField(ctx, s, value);
    }

  next_elem:
    continue;
  }
}

static void cleanInfoReply(InfoFields *fields) {
  free(fields->errorIndexes);
}

static size_t replyKvArray(InfoFields *fields, RedisModuleCtx *ctx, InfoValue *values,
                           InfoFieldSpec *specs, size_t numSpecs) {
  size_t n = 0;
  for (size_t ii = 0; ii < numSpecs; ++ii) {
    InfoValue *source = values + ii;
    if (!source->isSet) {
      continue;
    }

    n += 2;
    RedisModule_ReplyWithSimpleString(ctx, specs[ii].name);
    int type = specs[ii].type;

    if (type == InfoField_WholeSum || type == InfoField_Max) {
      RedisModule_ReplyWithLongLong(ctx, source->u.total_l);
    } else if (type == InfoField_DoubleSum) {
      RedisModule_ReplyWithDouble(ctx, source->u.total_l);
    } else if (type == InfoField_DoubleAverage) {
      if (source->u.avg.count) {
        RedisModule_ReplyWithDouble(ctx, source->u.avg.avg / source->u.avg.count);
      } else {
        RedisModule_ReplyWithDouble(ctx, 0);
      }
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }
  return n;
}

static void generateFieldsReply(InfoFields *fields, RedisModuleCtx *ctx) {
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t n = 0;

  // Respond with the name, schema, and options
  if (fields->indexName) {
    RedisModule_ReplyWithSimpleString(ctx, "index_name");
    RedisModule_ReplyWithStringBuffer(ctx, fields->indexName, fields->indexNameLen);
    n += 2;
  }
  if (fields->indexDef) {
    RedisModule_ReplyWithSimpleString(ctx, "index_definition");
    MR_ReplyWithMRReply(ctx, fields->indexDef);
    n += 2;
  }
  if (fields->indexSchema) {
    RedisModule_ReplyWithSimpleString(ctx, "attributes");
    MR_ReplyWithMRReply(ctx, fields->indexSchema);
    n += 2;
  }

  if (fields->indexOptions) {
    RedisModule_ReplyWithSimpleString(ctx, "index_options");
    MR_ReplyWithMRReply(ctx, fields->indexOptions);
    n += 2;
  }

  RedisModule_ReplyWithSimpleString(ctx, "gc_stats");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t nGcStats = replyKvArray(fields, ctx, fields->gcValues, gcSpecs, NUM_GC_FIELDS_SPEC);
  RedisModule_ReplySetArrayLength(ctx, nGcStats);
  n += 2;

  RedisModule_ReplyWithSimpleString(ctx, "cursor_stats");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t nCursorStats =
      replyKvArray(fields, ctx, fields->cursorValues, cursorSpecs, NUM_CURSOR_FIELDS_SPEC);
  RedisModule_ReplySetArrayLength(ctx, nCursorStats);
  n += 2;

  n += replyKvArray(fields, ctx, fields->toplevelValues, toplevelSpecs_g, NUM_FIELDS_SPEC);
  RedisModule_ReplySetArrayLength(ctx, n);
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

  for (size_t ii = 0; ii < count; ++ii) {
    if (MRReply_Type(replies[ii]) == MR_REPLY_ERROR) {
      if (!fields.errorIndexes) {
        fields.errorIndexes = calloc(count, sizeof(*fields.errorIndexes));
      }
      fields.errorIndexes[ii] = 1;
      numErrored++;
      if (!firstError) {
        firstError = replies[ii];
      }
      continue;
    }
    if (MRReply_Type(replies[ii]) != MR_REPLY_ARRAY) {
      continue;  // Ooops!
    }

    size_t numElems = MRReply_Length(replies[ii]);
    if (numElems % 2 != 0) {
      printf("Uneven INFO Reply!!!?\n");
    }
    processKvArray(&fields, replies[ii], fields.toplevelValues, toplevelSpecs_g, NUM_FIELDS_SPEC,
                   0);
  }

  // Now we've received all the replies.
  if (numErrored == count) {
    // Reply with error
    MR_ReplyWithMRReply(ctx, firstError);
  } else {
    generateFieldsReply(&fields, ctx);
  }

  cleanInfoReply(&fields);
  return REDISMODULE_OK;
}
