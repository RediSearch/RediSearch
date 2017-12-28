#include "project.h"
#include <math.h>

/* A macro to generate math projection functions for wrapping math functions that are applied to a
 * double and return double */
#define NUMERIC_PROJECTION_WRAPPER(f, math_func)                                                   \
  static int f(ResultProcessorCtx *ctx, SearchResult *res) {                                       \
    ProjectorCtx *pc = ctx->privdata;                                                              \
    /* this will return EOF if needed */                                                           \
    ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);                                              \
    RSValue v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),           \
                                      pc->properties->keys[0]);                                    \
    if (v.t == RSValue_Number) {                                                                   \
      RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0],                \
                     RS_NumVal(math_func(v.numval)));                                              \
    } else {                                                                                       \
      RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], RS_NullVal()); \
    }                                                                                              \
    return RS_RESULT_OK;                                                                           \
  }

#define GENERIC_PROJECTOR_FACTORY(f, next_func)                                                \
  ResultProcessor *f(ResultProcessor *upstream, const char *alias, CmdArg *args, char **err) { \
    return NewProjectorGeneric(next_func, upstream, alias, args, NULL, 1, 1, err);             \
  }
// FLOOR projection
NUMERIC_PROJECTION_WRAPPER(floor_Next, floor);
GENERIC_PROJECTOR_FACTORY(NewFloorArgs, floor_Next);

// ABS projection
NUMERIC_PROJECTION_WRAPPER(abs_Next, fabs);
GENERIC_PROJECTOR_FACTORY(NewAbsArgs, abs_Next);

// CEIL projection
NUMERIC_PROJECTION_WRAPPER(ceil_Next, ceil);
GENERIC_PROJECTOR_FACTORY(NewCeilArgs, ceil_Next);
