#include <redisearch.h>
#include <result_processor.h>
#include <rmutil/cmdparse.h>
#include <aggregate/expr/expression.h>
#include <aggregate/functions/function.h>
#include "aggregate.h"

typedef struct {
  RSExpr *exp;
  RSSortingTable *sortables;
  RSExprEvalCtx ctx;
  RSValue val;
} FilterCtx;

static FilterCtx *NewFilterCtx() {
  FilterCtx *ret = malloc(sizeof(*ret));
  return ret;
}

void Filter_Free(ResultProcessor *p) {
  FilterCtx *pc = p->ctx.privdata;

  RSFunctionEvalCtx_Free(pc->ctx.fctx);
  RSExpr_Free(pc->exp);
  free(pc);
  free(p);
}

int Filter_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  FilterCtx *pc = ctx->privdata;

  char *err;
  do {  // read while we either get EOF or the filter expr evaluates to true
    RESULTPROCESSOR_MAYBE_RET_EOF(ctx->upstream, res, 1);
    pc->ctx.r = res;
    pc->ctx.fctx->res = res;
    int rc = RSExpr_Eval(&pc->ctx, pc->exp, &pc->val, &err);
    if (rc == EXPR_EVAL_OK) {
      if (RSValue_BoolTest(&pc->val)) {
        return RS_RESULT_OK;
      }
    }
  } while (1);
  return RS_RESULT_EOF;
}

ResultProcessor *NewFilter(RedisSearchCtx *sctx, ResultProcessor *upstream, const char *expr,
                           size_t len, QueryError *status) {

  FilterCtx *ctx = NewFilterCtx();
  ctx->ctx.sctx = sctx;
  ctx->ctx.sortables = sctx && sctx->spec ? sctx->spec->sortables : NULL;
  ctx->ctx.fctx = RS_NewFunctionEvalCtx();
  ctx->exp = RSExpr_Parse(expr, len, &status->detail);
  if (!ctx->exp) {
    QueryError_MaybeSetCode(status, QUERY_EEXPR);
    free(ctx);
    return NULL;
  }
  ResultProcessor *proc = NewResultProcessor(upstream, ctx);
  proc->Next = Filter_Next;
  proc->Free = Filter_Free;
  return proc;
}
