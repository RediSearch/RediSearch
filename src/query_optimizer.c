#include "query_optimizer.h"
#include "optimizer_reader.h"
#include "numeric_index.h"
#include "ext/default.h"

QOptimizer *QOptimizer_New() {
  return rm_calloc(1, sizeof(QOptimizer));
}

void QOptimizer_Free(QOptimizer *opt) {
  if (opt->sortbyNode) {
    QueryNode_Free(opt->sortbyNode);
  }
  rm_free(opt);
}

void QOptimizer_Parse(AREQ *req) {
  QOptimizer *opt = req->optimizer;
  opt->sctx = req->sctx;
  opt->conc = &req->conc;

  // get FieldSpec of sortby field and results limit
  PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
  if (arng) {
    opt->limit = arng->limit + arng->offset;
    if (IsSearch(req) && !opt->limit) {
      opt->limit = DEFAULT_LIMIT;
    }
    if (arng->sortKeys) {
      const char *name = arng->sortKeys[0];
      const FieldSpec *field = IndexSpec_GetField(req->sctx->spec, name, strlen(name));
      if (field && field->types == INDEXFLD_T_NUMERIC) {
        opt->field = field;
        opt->fieldName = name;
        opt->asc = arng->sortAscMap & 0x01;
      } else {
        // sortby other fields, no optimization
        opt->type = Q_OPT_NONE;
      }
    }
  }

  // get scorer function if there is no sortby
  if (opt->field) {
    opt->scorerType = SCORER_TYPE_NONE;
  } else {
    const char *scorer = req->searchopts.scorerName;
    if (!scorer) {      // default is TFIDF
      opt->scorerType = SCORER_TYPE_TERM;
    } else if (!strcmp(scorer, DEFAULT_SCORER_NAME)) {  // TFIDF
      opt->scorerType = SCORER_TYPE_TERM;
    } else if (!strcmp(scorer, TFIDF_DOCNORM_SCORER_NAME)) {
      opt->scorerType = SCORER_TYPE_TERM;
    } else if (!strcmp(scorer, DISMAX_SCORER_NAME)) {
      opt->scorerType = SCORER_TYPE_TERM;
    } else if (!strcmp(scorer, BM25_SCORER_NAME)) {
      opt->scorerType = SCORER_TYPE_TERM;
    } else if (!strcmp(scorer, DOCSCORE_SCORER)) {
      opt->scorerType = SCORER_TYPE_DOC;
    } else if (!strcmp(scorer, HAMMINGDISTANCE_SCORER)) {
      opt->scorerType = SCORER_TYPE_DOC;
    }
  }
}

#define INVALUD_PTR ((void *)0xcafecafe) // Replace with BAD_POINTER

/* the function receives the QueryNode tree root and attempts to:
 * 1. find TEXT fields that need to be scored for some scorers
 * 2. find the numeric field used as SORTBY field  */
static QueryNode *checkQueryTypes(QueryNode *node, const char *name, QueryNode **parent,
                                  bool *reqScore) {
  QueryNode *ret = NULL;
  switch (node->type) {
    case QN_NUMERIC:
      // add support for multiple ranges on field
      if (name && !strcmp(name, node->nn.nf->fieldName)) {
        ret = node;
      }
      break;

    case QN_PHRASE:  // INTERSECT
      // weight is different than 1
      if (node->opts.weight != 1) {
        break;
      }
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        QueryNode *cur = checkQueryTypes(node->children[i], name, parent, reqScore);
        // we want to return numeric node and have its parent so we can remove it later.
        if (cur && cur->type == QN_NUMERIC && *parent == NULL) {
          if (ret != NULL || cur == INVALUD_PTR) {
            return INVALUD_PTR;
          }
          ret = cur;
        }
      }
      if (ret && parent) *parent = node;
      break;

    case QN_TOKEN:           // TEXT
    case QN_FUZZY:           // TEXT
    case QN_PREFIX:          // TEXT
    case QN_WILDCARD_QUERY:  // TEXT
    case QN_LEXRANGE:        // TEXT
      *reqScore = true;
      break;

    case QN_OPTIONAL:  // can't score optional ??
    case QN_NOT:       // can't score not      ??
    case QN_UNION:     // TODO
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        // ignore return value from a union since sortby optimization cannot be achieved.
        // check if it contains TEXT fields.
        checkQueryTypes(node->children[i], NULL, NULL, reqScore);
      }
      break;

    case QN_GEO:       // TODO: ADD GEO support
    case QN_GEOMETRY:
    case QN_IDS:       // NO SCORE
    case QN_TAG:       // NO SCORE
    case QN_VECTOR:    // NO SCORE
    case QN_WILDCARD:  // No SCORE
    case QN_NULL:
    case QN_MISSING:
      break;
  }
  return ret;
}

size_t QOptimizer_EstimateLimit(size_t numDocs, size_t estimate, size_t limit) {
  if (numDocs == 0 || estimate == 0) {
    return 0;
  }

  double ratio = (double)estimate / (double)numDocs;
  size_t newEstimate = (limit / ratio) + 1;
  // printf("numDocs %ld childEstimate %ld limit %ld required: %ld\n", numDocs, estimate, limit, newEstimate);

  return newEstimate;
}

void QOptimizer_QueryNodes(QueryNode *root, QOptimizer *opt) {
  const FieldSpec *field = opt->field;
  bool isSortby = !!field;
  const char *name = opt->fieldName;
  bool hasOther = false;

  if (root->type == QN_WILDCARD) {
    opt->scorerType = SCORER_TYPE_NONE;
  }

  // find the sortby numeric node and remove it from query node tree
  QueryNode *parentNode = NULL;
  QueryNode *numSortbyNode = checkQueryTypes(root, name, &parentNode, &opt->scorerReq);
  if (numSortbyNode && numSortbyNode != INVALUD_PTR) {
    RS_LOG_ASSERT(numSortbyNode->type == QN_NUMERIC, "found it");
    // numeric is part of an intersect. remove it for optimizer reader
    if (parentNode) {
      for (int i = 0; i < QueryNode_NumChildren(parentNode); ++i) {
        if (parentNode->children[i] == numSortbyNode) {
          array_del_fast(parentNode->children, i);
          break;
        }
      }
      numSortbyNode->nn.nf->limit = opt->limit;
      numSortbyNode->nn.nf->asc = opt->asc;
      opt->sortbyNode = numSortbyNode;
      opt->nf = numSortbyNode->nn.nf;
    } else {
      // tree has only numeric range. scan range large enough for requested limit
      opt->type = Q_OPT_PARTIAL_RANGE;
      return;
    }
  }

  // there is no sorting field and scorer is required - we must check all results
  if ((!isSortby && opt->scorerReq) || (root->type == QN_VECTOR && root->vn.vq->type == VECSIM_QT_KNN)) {
    opt->type = Q_OPT_NONE;
    return;
  }

  // there are no other filter except for our numeric
  // if has sortby, use limited range
  // else, return after enough result found
  if (!opt->scorerReq) {
    if (isSortby) {
      opt->type = Q_OPT_PARTIAL_RANGE;
      return;
    } else {
      opt->type = Q_OPT_NO_SORTER;
      // No need for scorer, and there is no sorter. we can avoid calculating scores
      opt->scorerType = SCORER_TYPE_NONE;
      return;
    }
  }
  opt->type = Q_OPT_UNDECIDED;
}

// creates an intersect from root and numeric
static void updateRootIter(AREQ *req, IndexIterator *root, IndexIterator *new) {
  if (root->type == INTERSECT_ITERATOR) {
    AddIntersectIterator(root, new);
  } else {
    IndexIterator **its = rm_malloc(2 * sizeof(*its));
    its[0] = req->rootiter;
    its[1] = new;
    // use slop==-1 and inOrder==0 since not applicable
    // use weight 1 since we checked at `checkQueryTypes`
    req->rootiter = NewIntersectIterator(its, 2, NULL, 0, -1, 0, 1);
  }
}

void QOptimizer_Iterators(AREQ *req, QOptimizer *opt) {
  IndexSpec *spec = req->sctx->spec;
  IndexIterator *root = req->rootiter;

  switch (opt->type) {
    case Q_OPT_HYBRID:
      RS_LOG_ASSERT(0, "cannot be decided earlier");

    // Nothing to do here
    case Q_OPT_NO_SORTER:
    case Q_OPT_NONE:
    case Q_OPT_FILTER:
      return;

    // limit range to number of required LIMIT
    case Q_OPT_PARTIAL_RANGE: {
      if (root->type == WILDCARD_ITERATOR) {
        req->rootiter = NewOptimizerIterator(opt, root, &req->ast.config);
      } else if (req->ast.root->type == QN_NUMERIC) {
        // trim the union numeric iterator to have the minimal number of ranges
        if (root->type == UNION_ITERATOR) {
          trimUnionIterator(root, 0, opt->limit, opt->asc);
        }
      } else {
        req->rootiter = NewOptimizerIterator(opt, root, &req->ast.config);
      }
      return;
    }
    case Q_OPT_UNDECIDED: {
      if (!opt->field) {
        // TODO: For now set to NONE. Maybe add use of FILTER
        opt->type = Q_OPT_NONE;
        const char* sortByNodeFieldName = opt->sortbyNode->nn.nf->fieldName;
        const FieldSpec *fs = IndexSpec_GetField(spec, sortByNodeFieldName, strlen(sortByNodeFieldName));
        FieldIndexFilterContext filterCtx = {.fieldIndex = fs->index};
        IndexIterator *numericIter = NewNumericFilterIterator(req->sctx, opt->sortbyNode->nn.nf,
                                                             &req->conc, INDEXFLD_T_NUMERIC, &req->ast.config,
                                                             &filterCtx);
        updateRootIter(req, root, numericIter);
        return;
      }
      opt->type = Q_OPT_HYBRID;
      // replace root with OptimizerIterator
      req->rootiter = NewOptimizerIterator(opt, root, &req->ast.config);
    }
  }
}

void QOptimizer_UpdateTotalResults(AREQ *req) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
    size_t reqLimit = arng && arng->isLimited ? arng->limit : DEFAULT_LIMIT;
    size_t reqOffset = arng && arng->isLimited ? arng->offset : 0;
    req->qiter.totalResults = req->qiter.totalResults > reqOffset ?
                              req->qiter.totalResults - reqOffset : 0;
    if(req->qiter.totalResults > reqLimit) {
      req->qiter.totalResults = reqLimit;
    }
}

const char *QOptimizer_PrintType(QOptimizer *opt) {
  switch (opt->type) {
    case Q_OPT_NONE:
     return "No optimization";
    case Q_OPT_PARTIAL_RANGE:
     return "Query partial range";
    case Q_OPT_NO_SORTER:
     return "Quick return";
    case Q_OPT_HYBRID:
     return "Hybrid";
    case Q_OPT_UNDECIDED:
      return "Undecided";
    case Q_OPT_FILTER:
      return "Filter";
  }
  return NULL;
}
