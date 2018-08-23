#include "reducer.h"
#include "aggregate.h"
#include <rmutil/cmdparse.h>
#include <string.h>
#include <err.h>

static Reducer *NewCountArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                             char **err) {
  return NewCount(ctx, alias);
}

static Reducer *NewSumArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {

  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for SUM");
    return NULL;
  }
  return NewSum(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewToListArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                              char **err) {

  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for TOLIST");
    return NULL;
  }
  return NewToList(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewMinArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for MIN");
    return NULL;
  }
  return NewMin(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewMaxArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for MAX");
    return NULL;
  }
  return NewMax(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewAvgArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for AVG");
    return NULL;
  }
  return NewAvg(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewCountDistinctArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                     const char *alias, char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for COUNT_DISTINCT");
    return NULL;
  }
  return NewCountDistinct(ctx, alias, RSKEY(RSValue_StringPtrLen(args[0], NULL)));
}

static Reducer *NewCountDistinctishArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                        const char *alias, char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for COUNT_DISTINCTISH");
    return NULL;
  }
  return NewCountDistinctish(ctx, alias, RSKEY(RSValue_StringPtrLen(args[0], NULL)));
}

/* REDUCE FRIST_VALUE {nargs} @property [BY @property DESC|ASC] */
static Reducer *NewFirstValueArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                  const char *alias, char **err) {
  char *prop = NULL;
  char *by = NULL;
  char *sortBy = NULL;
  char *asc = NULL;
  // Parse all and make sure we were valid
  if (!RSValue_ArrayAssign(args, argc, "s?sss", &prop, &by, &sortBy, &asc) ||
      (by && strcasecmp(by, "BY")) || (asc && strcasecmp(asc, "ASC") && strcasecmp(asc, "DESC"))) {
    SET_ERR(err, "Invalid arguments for FIRST_VALUE");
    return NULL;
  }
  // printf("prop: %s, by: %s, sortBy: %s, asc: %s\n", prop, by, sortBy, asc);
  int ascend = 1;
  if (asc && !strcasecmp(asc, "DESC")) ascend = 0;

  return NewFirstValue(ctx, RSKEY(prop), RSKEY(sortBy), ascend, alias);
}

static Reducer *NewStddevArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                              char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for STDDEV");
    return NULL;
  }
  return NewStddev(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewQuantileArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                                char **err) {
  if (argc < 2 || argc > 3 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for QUANTILE");
    return NULL;
  }
  const char *property = RSKEY(RSValue_StringPtrLen(args[0], NULL));

  double pct;
  if (!RSValue_ToNumber(args[1], &pct)) {
    SET_ERR(err, "Could not parse percent for QUANTILE(key, pct)");
    return NULL;
  }

  if (pct <= 0 || pct >= 1) {
    SET_ERR(err, "Quantile must be between 0.0 and 1.0 (exclusive) )");
    return NULL;
  }

  double resolution = 500;
  if (argc > 2) {
    if (!RSValue_ToNumber(args[2], &resolution)) {
      SET_ERR(err, "Could not parse resolution");
      return NULL;
    } else if (resolution < 1 || resolution > MAX_SAMPLE_SIZE) {
      SET_ERR(err, "Invalid resolution");
      return NULL;
    }
  }

  return NewQuantile(ctx, property, alias, pct, resolution);
}

static Reducer *NewRandomSampleArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                    const char *alias, char **err) {
  if (argc != 2 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for RANDOM_SAMPLE");
    return NULL;
  }
  const char *property = RSKEY(RSValue_StringPtrLen(args[0], NULL));

  double d;
  if (!RSValue_ToNumber(args[1], &d)) {
    SET_ERR(err, "Could not parse size for random sample");
    return NULL;
  }
  int size = (int)d;
  if (size <= 0 || size >= MAX_SAMPLE_SIZE) {
    SET_ERR(err, "Invalid size for random sample");
  }
  return NewRandomSample(ctx, size, property, alias);
}

static Reducer *NewHllArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for HLL");
    return NULL;
  }
  return NewHLL(ctx, alias, RSKEY(RSValue_StringPtrLen(args[0], NULL)));
}

static Reducer *NewHllSumArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                              char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    SET_ERR(err, "Invalid arguments for HLL_SUM");
    return NULL;
  }
  return NewHLLSum(ctx, alias, RSKEY(RSValue_StringPtrLen(args[0], NULL)));
}

typedef Reducer *(*ReducerFactory)(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                   const char *alias, char **err);

static struct {
  const char *k;
  ReducerFactory f;
  RSValueType retType;
} reducers_g[] = {
    {"sum", NewSumArgs, RSValue_Number},
    {"min", NewMinArgs, RSValue_Number},
    {"max", NewMaxArgs, RSValue_Number},
    {"avg", NewAvgArgs, RSValue_Number},
    {"count", NewCountArgs, RSValue_Number},
    {"count_distinct", NewCountDistinctArgs, RSValue_Number},
    {"count_distinctish", NewCountDistinctishArgs, RSValue_Number},
    {"tolist", NewToListArgs, RSValue_Array},
    {"quantile", NewQuantileArgs, RSValue_Number},
    {"stddev", NewStddevArgs, RSValue_Number},
    {"first_value", NewFirstValueArgs, RSValue_String},
    {"random_sample", NewRandomSampleArgs, RSValue_Array},
    {"hll", NewHllArgs, RSValue_String},
    {"hll_sum", NewHllSumArgs, RSValue_Number},

    {NULL, NULL},
};

Reducer *GetReducer(RedisSearchCtx *ctx, const char *name, const char *alias, RSValue **args,
                    size_t argc, QueryError *status) {
  for (int i = 0; reducers_g[i].k != NULL; i++) {
    if (!strcasecmp(reducers_g[i].k, name)) {
      Reducer *r = reducers_g[i].f(ctx, args, argc, alias, &status->detail);
      if (!r) {
        QueryError_MaybeSetCode(status, QUERY_EREDUCERINIT);
      }
      return r;
    }
  }
  QueryError_SetErrorFmt(status, QUERY_ENOREDUCER, "Missing reducer for '%s'", name);
  return NULL;
}

RSValueType GetReducerType(const char *name) {
  for (int i = 0; reducers_g[i].k != NULL; i++) {
    if (!strcasecmp(reducers_g[i].k, name)) {
      return reducers_g[i].retType;
    }
  }

  return RSValue_Null;
}
