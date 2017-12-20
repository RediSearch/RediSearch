#include "reducer.h"

typedef Reducer *(*ReducerFactory)(CmdArray *args, char **err);

static struct {
  const char *k;
  ReducerFactory f;
} reducers_g[] = {
    {"sum", NewSumArgs},
    {"count", NewCountArgs},
};

Reducer *GetReducer(const char *name, CmdArray *args, char **err) {
}
