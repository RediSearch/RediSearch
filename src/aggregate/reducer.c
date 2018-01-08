#include "reducer.h"
#include <rmutil/cmdparse.h>
#include <string.h>

Reducer *NewCountArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  return NewCount(ctx, alias);
}

Reducer *NewSumArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {

  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for SUM");
    return NULL;
  }
  return NewSum(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

Reducer *NewToListArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {

  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for TOLIST");
    return NULL;
  }
  return NewToList(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

Reducer *NewMinArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for MIN");
    return NULL;
  }
  return NewMin(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

Reducer *NewMaxArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for MAX");
    return NULL;
  }
  return NewMax(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

Reducer *NewAvgArgs(RedisSearchCtx *ctx, CmdArray *args, const char *alias, char **err) {
  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for AVG");
    return NULL;
  }
  return NewAvg(ctx, RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0))), alias);
}

typedef Reducer *(*ReducerFactory)(RedisSearchCtx *ctx, CmdArray *args, const char *alias,
                                   char **err);

static struct {
  const char *k;
  ReducerFactory f;
} reducers_g[] = {
    {"sum", NewSumArgs},     {"min", NewMinArgs},       {"max", NewMaxArgs}, {"avg", NewAvgArgs},
    {"count", NewCountArgs}, {"tolist", NewToListArgs}, {NULL, NULL},
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
