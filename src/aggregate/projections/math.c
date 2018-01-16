#include "project.h"
#include <math.h>

/* A macro to generate math projection functions for wrapping math functions that are applied to a
 * double and return double */
#define NUMERIC_PROJECTION_WRAPPER(f, math_func)                                                   \
  static int f(ResultProcessorCtx *ctx, SearchResult *res) {                                       \
    ProjectorCtx *pc = ctx->privdata;                                                              \
    /* this will return EOF if needed */                                                           \
    ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);                                              \
    RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),          \
                                       pc->properties->keys[0]);                                   \
    if (v && v->t == RSValue_Number) {                                                             \
      RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0],                \
                     RS_NumVal(math_func(v->numval)));                                             \
    } else {                                                                                       \
      if (v) {                                                                                     \
        double d; /* Try to parse as number                           */                           \
        if (RSValue_ToNumber(v, &d)) {                                                             \
          RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0],            \
                         RS_NumVal(d));                                                            \
          goto end;                                                                                \
        }                                                                                          \
      }                                                                                            \
      RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], RS_NullVal()); \
    }                                                                                              \
  end:                                                                                             \
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

NUMERIC_PROJECTION_WRAPPER(sqrt_Next, sqrt);
GENERIC_PROJECTOR_FACTORY(NewSqrtArgs, sqrt_Next);

NUMERIC_PROJECTION_WRAPPER(log_Next, log);
GENERIC_PROJECTOR_FACTORY(NewLogArgs, log_Next);

NUMERIC_PROJECTION_WRAPPER(log2_Next, log2);
GENERIC_PROJECTOR_FACTORY(NewLog2Args, log2_Next);

typedef struct {
  union {
    RSValue val;
    const char *prop;
  };
  int isValue;
} valueOrProp;

typedef struct {
  int len;
  valueOrProp params[];
} dynamicExpr;

static inline RSValue *getVaueOrProp(SearchResult *r, valueOrProp *vp, RSSortingTable *tbl) {
  if (vp->isValue) {
    return &vp->val;
  }
  return SearchResult_GetValue(r, tbl, vp->prop);
}

static int add_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  ProjectorCtx *pc = ctx->privdata;
  dynamicExpr *dx = pc->privdata;

  double sum = 0;
  int ok = 1;
  for (int i = 0; i < dx->len; i++) {
    RSValue *v = getVaueOrProp(res, &dx->params[i],
                               ctx->qxc->sctx->spec ? ctx->qxc->sctx->spec->sortables : NULL);
    if (v && v->t == RSValue_Number) {
      sum += v->numval;
    } else if (v) {
      double d = 0;
      if (RSValue_ToNumber(v, &d)) {
        sum += d;
      } else {
        ok = 0;
        break;
      }
    }
  }

  RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : "sum", ok ? RS_NumVal(sum) : RS_NullVal());
  return RS_RESULT_OK;
}

ResultProcessor *NewAddProjection(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                  char **err) {

  if (CMDARG_ARRLEN(args) < 1) {
    RETURN_ERROR(err, "Invalid or missing arguments for projection ADD%s", "");
  }

  dynamicExpr *dx = malloc(sizeof(dynamicExpr) + CMDARG_ARRLEN(args) * sizeof(valueOrProp));
  dx->len = CMDARG_ARRLEN(args);
  for (size_t i = 0; i < dx->len; i++) {
    const char *p = CMDARG_STRPTR(CMDARG_ARRELEM(args, i));
    if (*p == '@') {
      dx->params[i].isValue = 0;
      dx->params[i].prop = RSKEY(p);
    } else {
      if (!RSValue_ParseNumber(p, CMDARG_STRLEN(CMDARG_ARRELEM(args, i)), &dx->params[i].val)) {
        RETURN_ERROR(err, "Could not parse argument %s", p);
      }
      dx->params[i].isValue = 1;
    }
  }

  ProjectorCtx *ctx = malloc(sizeof(ProjectorCtx));
  ctx->alias = alias;
  ctx->privdata = dx;
  ctx->properties = NULL;
  ResultProcessor *proc = NewResultProcessor(upstream, ctx);
  proc->Next = add_Next;
  proc->Free = ProjectorCtx_GenericFree;
  return proc;
}
