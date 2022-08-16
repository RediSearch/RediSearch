#include "query_optimizer.h"

void QOptimizer_Parse(AREQ *req) {
  QOptimizer *opt = req->optimizer;

  PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
  if (arng) {
    opt->limit = arng->limit + arng->offset;
    if (arng->sortKeys) {
        const char *name = opt->fieldName = arng->sortKeys[0];
        opt->field = IndexSpec_GetField(req->sctx->spec, name, strlen(name));
        opt->asc = arng->sortAscMap & 0x01;
    }
  }
}

void QOptimizer_QueryNodes(QueryNode *root, QOptimizer *opt) {
  return;
}

void QOptimizer_Iterators(AREQ *req, QOptimizer *opt) {
  return;
}
