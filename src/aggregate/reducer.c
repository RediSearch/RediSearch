#include "reducer.h"
#include <rmutil/cmdparse.h>
#include <string.h>

static Reducer *NewCountArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  return NewCount(ctx, alias);
}

static Reducer *NewSumArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {

  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for SUM");
    return NULL;
  }
  return NewSum(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

static Reducer *NewToListArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {

  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for TOLIST");
    return NULL;
  }
  return NewToList(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

static Reducer *NewMinArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for MIN");
    return NULL;
  }
  return NewMin(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

static Reducer *NewMaxArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for MAX");
    return NULL;
  }
  return NewMax(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

static Reducer *NewAvgArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for AVG");
    return NULL;
  }
  return NewAvg(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

static Reducer *NewCountDistinctArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias,
                                     char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for COUNT_DISTINCT");
    return NULL;
  }
  return NewCountDistinct(ctx, alias, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))));
}

static Reducer *NewCountDistinctishArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias,
                                        char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for COUNT_DISTINCTISH");
    return NULL;
  }
  return NewCountDistinctish(ctx, alias, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))));
}

/* REDUCE FRIST_VALUE {nargs} @property [BY @property DESC|ASC] */
static Reducer *NewFirstValueArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias,
                                  char **err) {
  char *prop = NULL;
  char *by = NULL;
  char *sortBy = NULL;
  char *asc = NULL;
  // Parse all and make sure we were valid
  if (CMDPARSE_ERR == CmdArg_ArrayAssign(args, "s?sss", &prop, &by, &sortBy, &asc) ||
      (by && strcasecmp(by, "BY")) || (asc && strcasecmp(asc, "ASC") && strcasecmp(asc, "DESC"))) {
    *err = strdup("Invalid arguments for FIRST_VALUE");
    return NULL;
  }
  // printf("prop: %s, by: %s, sortBy: %s, asc: %s\n", prop, by, sortBy, asc);
  int ascend = 1;
  if (asc && !strcasecmp(asc, "DESC")) ascend = 0;

  return NewFirstValue(ctx, RSKEY(prop), RSKEY(sortBy), ascend, alias);
}

static Reducer *NewStddevArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for STDDEV");
    return NULL;
  }
  return NewStddev(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

static Reducer *NewQuantileArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias,
                                char **err) {
  if (args->len != 2 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for QUANTILE");
    return NULL;
  }
  const char *property = RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0)));

  double pct;
  if (!CmdArg_ParseDouble(CMDARRAY_ELEMENT(args, 1), &pct)) {
    *err = strdup("Could not parse percent for QUANTILE(key, pct)");
    return NULL;
  }

  if (pct <= 0 || pct >= 1) {
    *err = strdup("Quantile must be between 0.0 and 1.0 (exclusive) )");
  }
  return NewQuantile(ctx, property, alias, pct);
}

typedef Reducer *(*ReducerFactory)(RedisSearchCtx *ctx, CmdArray *args, const char *alias,
                                   char **err);

static struct {
  const char *k;
  ReducerFactory f;
} reducers_g[] = {
    {"sum", NewSumArgs},
    {"min", NewMinArgs},
    {"max", NewMaxArgs},
    {"avg", NewAvgArgs},
    {"count", NewCountArgs},
    {"count_distinct", NewCountDistinctArgs},
    {"count_distinctish", NewCountDistinctishArgs},
    {"tolist", NewToListArgs},
    {"quantile", NewQuantileArgs},
    {"stddev", NewStddevArgs},
    {"first_value", NewFirstValueArgs},

    {NULL, NULL},
};

Reducer *GetReducer(RedisSearchCtx *ctx, const char *name, const char *alias, CmdArray *args,
                    char **err) {
  for (int i = 0; reducers_g[i].k != NULL; i++) {
    if (!strcasecmp(reducers_g[i].k, name)) {
      return reducers_g[i].f(ctx, args, alias, err);
    }
  }

  asprintf(err, "Could not find reducer '%s'", name);
  return NULL;
}
