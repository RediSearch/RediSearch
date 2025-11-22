/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "query_optimizer.h"
#include "iterators/optimizer_reader.h"
#include "numeric_index.h"
#include "ext/default.h"
#include "iterators/union_iterator.h"
#include "iterators/intersection_iterator.h"

/********************* Horrific hacks moved from index.c *********************/

static inline IteratorStatus UI_ReadUnsorted(QueryIterator *ctx) {
  UnionIterator *ui = (UnionIterator*)ctx;

  IndexResult_ResetAggregate(ui->base.current);
  while (ui->num > 0) {
    if (ui->its[ui->num - 1]->Read(ui->its[ui->num - 1]) == ITERATOR_OK) {
      AggregateResult_AddChild(ui->base.current, ui->its[ui->num - 1]->current);
      ui->base.lastDocId = ui->base.current->docId;
      return ITERATOR_OK;
    }
    ui->num--;
  }
  return ITERATOR_EOF;
}

void trimUnionIterator(QueryIterator *iter, size_t offset, size_t limit, bool asc) {
  RS_LOG_ASSERT(iter->type == UNION_ITERATOR, "trim applies to union iterators only");
  UnionIterator *ui = (UnionIterator *)iter;
  if (ui->num_orig <= 2) { // nothing to trim
    return;
  }

  size_t curTotal = 0;
  int i;
  if (offset == 0) {
    if (asc) {
      for (i = 1; i < ui->num; ++i) {
        QueryIterator *it = ui->its_orig[i];
        curTotal += it->NumEstimated(it);
        if (curTotal > limit) {
          ui->num = i + 1;
          memset(ui->its + ui->num, 0, ui->num_orig - ui->num);
          break;
        }
      }
    } else {  //desc
      for (i = ui->num - 2; i > 0; --i) {
        QueryIterator *it = ui->its_orig[i];
        curTotal += it->NumEstimated(it);
        if (curTotal > limit) {
          ui->num -= i;
          memmove(ui->its, ui->its + i, ui->num);
          memset(ui->its + ui->num, 0, ui->num_orig - ui->num);
          break;
        }
      }
    }
  } else {
    UI_SyncIterList(ui);
  }
  iter->Read = UI_ReadUnsorted;
}

void AddIntersectIterator(QueryIterator *parentIter, QueryIterator *childIter) {
  RS_LOG_ASSERT(parentIter->type == INTERSECT_ITERATOR, "add applies to intersect iterators only");
  IntersectionIterator *ii = (IntersectionIterator *)parentIter;
  ii->num_its++;
  ii->its = rm_realloc(ii->its, ii->num_its);
  ii->its[ii->num_its - 1] = childIter;
}

/********************* End of horrific hacks moved from index.c *********************/

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
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  opt->sctx = sctx;

  // get FieldSpec of sortby field and results limit
  PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
  if (arng) {
    opt->limit = arng->limit + arng->offset;
    if (IsSearch(req) && !opt->limit) {
      opt->limit = DEFAULT_LIMIT;
    }
    if (arng->sortKeys) {
      const char *name = arng->sortKeys[0];
      const FieldSpec *field = IndexSpec_GetFieldWithLength(sctx->spec, name, strlen(name));
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
    if (!scorer || !strcmp(scorer, BM25_STD_SCORER_NAME)) {      // default is BM25STD
      opt->scorerType = SCORER_TYPE_TERM;
    } else if (!strcmp(scorer, TFIDF_SCORER_NAME)) {
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
      if (name && !HiddenString_CompareC(node->nn.nf->fieldSpec->fieldName, name, strlen(name))) {
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
      numSortbyNode->nn.nf->ascending = opt->asc;
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
static void updateRootIter(AREQ *req, QueryIterator *root, QueryIterator *new) {
  if (root->type == INTERSECT_ITERATOR) {
    AddIntersectIterator(root, new);
  } else {
    QueryIterator **its = rm_malloc(2 * sizeof(*its));
    its[0] = req->rootiter;
    its[1] = new;
    // use slop==-1 and inOrder==0 since not applicable
    // use weight 1 since we checked at `checkQueryTypes`
    req->rootiter = NewIntersectionIterator(its, 2, -1, 0, 1);
  }
}

void QOptimizer_Iterators(AREQ *req, QOptimizer *opt) {
  IndexSpec *spec = AREQ_SearchCtx(req)->spec;
  QueryIterator *root = req->rootiter;

  switch (opt->type) {
    case Q_OPT_HYBRID:
      RS_ABORT("cannot be decided earlier");

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
        const FieldSpec *fs = opt->sortbyNode->nn.nf->fieldSpec;
        FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = fs->index}, .predicate = FIELD_EXPIRATION_DEFAULT};
        QueryIterator *numericIter = NewNumericFilterIterator(AREQ_SearchCtx(req), opt->sortbyNode->nn.nf, INDEXFLD_T_NUMERIC,
                                                              &req->ast.config, &filterCtx);
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
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
    // For LIMIT 0 0, we want to return the full count (not cap to 0)
    // For Implicit LIMIT (no LIMIT provided), we want to return the full count
    // if (IsAggregate(req) && HasWithCount(req)) {
    //   if (!(arng && arng->isLimited && arng->limit > 0)) {
    //     return;
    //   }
    // }
    // FT.AGGREGATE + WITHCOUNT with explicit LIMIT > 0, cap totalResults to the
    // LIMIT value (similar to optimized FT.SEARCH)
    size_t reqLimit = arng && arng->isLimited ? arng->limit : DEFAULT_LIMIT;
    size_t reqOffset = arng && arng->isLimited ? arng->offset : 0;
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
    qctx->totalResults = qctx->totalResults > reqOffset ?
                              qctx->totalResults - reqOffset : 0;
    if(qctx->totalResults > reqLimit) {
      qctx->totalResults = reqLimit;
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
