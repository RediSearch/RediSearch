#include "reducer.h"

Reducer *NewCountArgs(CmdArray *args, const char *alias, char **err) {
  return NewCount(alias);
}

Reducer *NewSumArgs(CmdArray *args, const char *alias, char **err) {
  return NULL;
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
