#include "project.h"
#include <math.h>

/* A macro to generate math projection functions for wrapping math functions that are applied to a
 * double and return double */
#define NUMERIC_PROJECTION_WRAPPER(f, math_func)                                            \
  static int f(ResultProcessorCtx *ctx, SearchResult *res) {                                \
    ProjectorCtx *pc = ctx->privdata;                                                       \
    /* this will return EOF if needed */                                                    \
    ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);                                       \
    RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),   \
                                       &pc->properties->keys[0]);                           \
    if (v && v->t == RSValue_Number) {                                                      \
      RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0].key,     \
                     RS_NumVal(math_func(v->numval)));                                      \
    } else {                                                                                \
      if (v) {                                                                              \
        double d; /* Try to parse as number                           */                    \
        if (RSValue_ToNumber(v, &d)) {                                                      \
          RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0].key, \
                         RS_NumVal(d));                                                     \
          goto end;                                                                         \
        }                                                                                   \
      }                                                                                     \
      RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0].key,     \
                     RS_NullVal());                                                         \
    }                                                                                       \
  end:                                                                                      \
    return RS_RESULT_OK;                                                                    \
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
    RSKey prop;
  };
  int isValue;
} valueOrProp;

typedef enum { BinaryFunc_Add, BinaryFunc_Div, BinaryFunc_Mul, BinaryFunc_Mod } BinaryFuncType;

typedef struct {
  size_t len;
  const char *alias;
  valueOrProp params[];
} dynamicExpr;

static inline int getValueOrProp(SearchResult *r, valueOrProp *vp, RSSortingTable *tbl,
                                 double *out) {
  if (vp->isValue) {
    *out = vp->val.numval;
    return 1;
  }
  RSValue *val = SearchResult_GetValue(r, tbl, &vp->prop);
  if (val == NULL) {
    return 0;
  }
  return RSValue_ToNumber(val, out);
}

static int binfunc_NextCommon(ResultProcessorCtx *ctx, SearchResult *res, BinaryFuncType type) {
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  ProjectorCtx *pc = ctx->privdata;
  dynamicExpr *dx = pc->privdata;

  double sum = 0;
  int ok = 1;
  RSSortingTable *stbl = ctx->qxc->sctx->spec ? ctx->qxc->sctx->spec->sortables : NULL;
  for (int i = 0; i < dx->len; i++) {
    int status;
    double cur;
    if (!(ok = getValueOrProp(res, &dx->params[i], stbl, &cur))) {
      break;
    }
    if (!ok) {
      break;
    }
    switch (type) {
      case BinaryFunc_Add:
        sum += cur;
        break;
      case BinaryFunc_Div:
        if (cur) {
          sum /= cur;
        }
        break;
      case BinaryFunc_Mod:
        if ((int64_t)cur) {
          sum = (int64_t)(sum) % (int64_t)cur;
        }
        break;
      case BinaryFunc_Mul:
        sum *= cur;
        break;
    }
  }

  RSFieldMap_Set(&res->fields, pc->alias, ok ? RS_NumVal(sum) : RS_NullVal());
  return RS_RESULT_OK;
}

typedef struct {
  int (*nextfn)(ResultProcessorCtx *, SearchResult *);
  BinaryFuncType type;
  const char *defaultAlias;
} BinaryFunction;

#define GEN_BINFUNC(varname, alias, funcname, type_)                \
  static int funcname(ResultProcessorCtx *ctx, SearchResult *res) { \
    return binfunc_NextCommon(ctx, res, type_);                     \
  }                                                                 \
  static BinaryFunction varname = {.defaultAlias = alias, .type = type_, .nextfn = funcname};

GEN_BINFUNC(mulFunc_g, "mul", mul_Next, BinaryFunc_Mul)
GEN_BINFUNC(divFunc_g, "div", div_Next, BinaryFunc_Div)
GEN_BINFUNC(modFunc_g, "mod", mod_Next, BinaryFunc_Mod)
GEN_BINFUNC(addFunc_g, "add", add_Next, BinaryFunc_Add)

static ResultProcessor *newBinfuncProjectionCommon(ResultProcessor *upstream, const char *alias,
                                                   const BinaryFunction *binfunc, CmdArg *args,
                                                   char **err) {

  if (CMDARG_ARRLEN(args) < 1 || (binfunc->type == BinaryFunc_Mod && CMDARG_ARRLEN(args) != 1)) {
    RETURN_ERROR(err, "Invalid or missing arguments for projection %s", binfunc->defaultAlias);
  }

  dynamicExpr *dx = malloc(sizeof(dynamicExpr) + CMDARG_ARRLEN(args) * sizeof(valueOrProp));
  dx->len = CMDARG_ARRLEN(args);
  for (size_t i = 0; i < dx->len; i++) {
    const char *p = CMDARG_STRPTR(CMDARG_ARRELEM(args, i));
    if (*p == '@') {
      dx->params[i].isValue = 0;
      dx->params[i].prop = RS_KEY(RSKEY(p));
    } else {
      if (!RSValue_ParseNumber(p, CMDARG_STRLEN(CMDARG_ARRELEM(args, i)), &dx->params[i].val)) {
        RETURN_ERROR(err, "Could not parse argument %s", p);
      }
      dx->params[i].isValue = 1;
    }
  }

  ProjectorCtx *ctx = malloc(sizeof(ProjectorCtx));
  if (!alias) {
    ctx->alias = binfunc->defaultAlias;
  }
  ctx->privdata = dx;
  ctx->properties = NULL;
  ResultProcessor *proc = NewResultProcessor(upstream, ctx);
  proc->Next = binfunc->nextfn;
  proc->Free = ProjectorCtx_GenericFree;
  return proc;
}

#define BINARY_FACTORY(name, binfunc)                                                             \
  ResultProcessor *name(ResultProcessor *upstream, const char *alias, CmdArg *args, char **err) { \
    return newBinfuncProjectionCommon(upstream, alias, binfunc, args, err);                       \
  }

BINARY_FACTORY(NewAddProjection, &addFunc_g)
BINARY_FACTORY(NewMulProjection, &mulFunc_g)
BINARY_FACTORY(NewDivProjection, &divFunc_g)
BINARY_FACTORY(NewModProjection, &modFunc_g)