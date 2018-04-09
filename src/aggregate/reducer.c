#include "reducer.h"
#include "aggregate.h"
#include <rmutil/cmdparse.h>
#include <string.h>

static Reducer *NewCountArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                             char **err) {
  return NewCount(ctx, alias);
}

static Reducer *NewSumArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {

  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for SUM");
    return NULL;
  }
  return NewSum(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewToListArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                              char **err) {

  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for TOLIST");
    return NULL;
  }
  return NewToList(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewMinArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for MIN");
    return NULL;
  }
  return NewMin(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewMaxArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for MAX");
    return NULL;
  }
  return NewMax(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewAvgArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                           char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for AVG");
    return NULL;
  }
  return NewAvg(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewCountDistinctArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                     const char *alias, char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for COUNT_DISTINCT");
    return NULL;
  }
  return NewCountDistinct(ctx, alias, RSKEY(RSValue_StringPtrLen(args[0], NULL)));
}

static Reducer *NewCountDistinctishArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc,
                                        const char *alias, char **err) {
  if (argc != 1 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for COUNT_DISTINCTISH");
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
    *err = strdup("Invalid arguments for FIRST_VALUE");
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
    *err = strdup("Invalid arguments for STDDEV");
    return NULL;
  }
  return NewStddev(ctx, RSKEY(RSValue_StringPtrLen(args[0], NULL)), alias);
}

static Reducer *NewQuantileArgs(RedisSearchCtx *ctx, RSValue **args, size_t argc, const char *alias,
                                char **err) {
  if (argc != 2 || !RSValue_IsString(args[0])) {
    *err = strdup("Invalid arguments for QUANTILE");
    return NULL;
  }
  const char *property = RSKEY(RSValue_StringPtrLen(args[0], NULL));

  double pct;
  if (!RSValue_ToNumber(args[1], &pct)) {
    *err = strdup("Could not parse percent for QUANTILE(key, pct)");
    return NULL;
  }

  if (pct <= 0 || pct >= 1) {
    *err = strdup("Quantile must be between 0.0 and 1.0 (exclusive) )");
  }
  return NewQuantile(ctx, property, alias, pct);
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

    {NULL, NULL},
};

Reducer *GetReducer(RedisSearchCtx *ctx, const char *name, const char *alias, RSValue **args,
                    size_t argc, char **err) {
  for (int i = 0; reducers_g[i].k != NULL; i++) {
    if (!strcasecmp(reducers_g[i].k, name)) {
      return reducers_g[i].f(ctx, args, argc, alias, err);
    }
  }

  asprintf(err, "Could not find reducer '%s'", name);
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
