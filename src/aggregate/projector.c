#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>
#include <math.h>
#include <ctype.h>
#include "project.h"
#include <aggregate/expr/expression.h>
#include <aggregate/functions/function.h>
typedef struct {
  RSExpr *exp;
  const char *alias;
  RSSortingTable *sortables;
  RSExprEvalCtx ctx;
  RSValue val;
} ProjectorCtx;

static ProjectorCtx *NewProjectorCtx(const char *alias) {
  ProjectorCtx *ret = malloc(sizeof(*ret));
  ret->alias = alias;
  return ret;
}

void Projector_Free(ResultProcessor *p) {
  ProjectorCtx *pc = p->ctx.privdata;

  RSFunctionEvalCtx_Free(pc->ctx.fctx);
  RSExpr_Free(pc->exp);
  free(pc);
  free(p);
}

int Projector_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  RESULTPROCESSOR_MAYBE_RET_EOF(ctx->upstream, res, 1);
  ProjectorCtx *pc = ctx->privdata;
  pc->ctx.r = res;
  pc->ctx.fctx->res = res;
  char *err;
  int rc = RSExpr_Eval(&pc->ctx, pc->exp, &pc->val, &err);
  if (rc == EXPR_EVAL_OK) {
    RSValue *a = RS_NewValue(RSValue_Null);
    *a = pc->val;
    a->allocated = 1;
    a->refcount = 0;

    RSFieldMap_Set(&res->fields, pc->alias, a);
  } else {
    RSFieldMap_Set(&res->fields, pc->alias, RS_NullVal());
  }
  return RS_RESULT_OK;
}

ResultProcessor *NewProjector(RedisSearchCtx *sctx, ResultProcessor *upstream, const char *alias,
                              const char *expr, size_t len, QueryError *status) {

  ProjectorCtx *ctx = NewProjectorCtx(alias);
  ctx->ctx.sctx = sctx;
  ctx->ctx.sortables = sctx && sctx->spec ? sctx->spec->sortables : NULL;
  ctx->ctx.fctx = RS_NewFunctionEvalCtx();
  ctx->exp = RSExpr_Parse(expr, len, &status->detail);
  QueryError_MaybeSetCode(status, QUERY_EEXPR);
  if (!ctx->exp) {
    free(ctx);
    return NULL;
  }
  ResultProcessor *proc = NewResultProcessor(upstream, ctx);
  proc->Next = Projector_Next;
  proc->Free = Projector_Free;
  return proc;
}
