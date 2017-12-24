#include "reducer.h"
#include <rmutil/cmdparse.h>
#include <string.h>

Reducer *NewCountArgs(CmdArray *args, const char *alias, char **err) {
  return NewCount(alias);
}

Reducer *NewSumArgs(CmdArray *args, const char *alias, char **err) {

  if (args->len != 1 || CMDARRAY_ELEMENT(args, 0)->type != CmdArg_String) {
    *err = strdup("Invalid arguments for SUM");
    return NULL;
  }
  return NewSum(CMDARG_STRPTR(CMDARRAY_ELEMENT(args, 0)), alias);
}

typedef Reducer *(*ReducerFactory)(CmdArray *args, const char *alias, char **err);

static struct {
  const char *k;
  ReducerFactory f;
} reducers_g[] = {
    {"sum", NewSumArgs},
    {"count", NewCountArgs},
    {NULL, NULL},
};

Reducer *GetReducer(const char *name, const char *alias, CmdArray *args, char **err) {
  for (int i = 0; reducers_g[i].k != NULL; i++) {
    if (!strcasecmp(reducers_g[i].k, name)) {
      return reducers_g[i].f(args, alias, err);
    }
  }

  asprintf(err, "Could not find reducer '%s'", name);
  return NULL;
}
