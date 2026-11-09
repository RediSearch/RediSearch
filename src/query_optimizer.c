/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "query_optimizer.h"

#include <string.h>

#include "iterators/optimizer_reader.h"
#include "iterators_ffi.h"
#include "query_eval_ffi.h"
#include "aggregate/aggregate_plan.h"
#include "field.h"
#include "field_spec.h"
#include "obfuscation/hidden.h"
#include "query_error.h"
#include "query_error_ffi.h"
#include "query_internal.h"
#include "query_types.h"
#include "redismodule.h"
#include "result_processor.h"
#include "result_processor_ffi.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "rqe_iterator_type.h"
#include "spec.h"
#include "util/arr/arr.h"
#include "vector_index.h"

QOptimizer *QOptimizer_New() {
  return rm_calloc(1, sizeof(QOptimizer));
}

void QOptimizer_Free(QOptimizer *opt) {
  if (opt->sortbyNode) {
    QueryNode_Free(opt->sortbyNode);
  }
  rm_free(opt);
}

static bool planHasSortKeys(const AGGPlan *pln) {
  DLLIST_FOREACH(nn, &pln->steps) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    if (stp->type == PLN_T_ARRANGE && array_len(((const PLN_ArrangeStep *)stp)->sortKeys)) {
      return true;
    }
  }
  return false;
}

/* Resolve the sort keys after the first to the sorting-vector slots the optimizer's heap reads
 * them from. The heap only keeps `limit` results, so among results tied on the numeric key it must
 * keep the ones the sorter would rank first; it can only do so for keys in the sorting vector. */
static bool resolveTieBreakKeys(QOptimizer *opt, const IndexSpec *spec,
                                const PLN_ArrangeStep *arng) {
  size_t numKeys = array_len(arng->sortKeys);
  for (size_t i = 1; i < numKeys; i++) {
    const char *name = arng->sortKeys[i];
    const FieldSpec *field = IndexSpec_GetFieldWithLength(spec, name, strlen(name));
    if (!field || !FieldSpec_IsSortable(field)) {
      return false;
    }
    opt->tieBreakSortIdx[i - 1] = field->sortIdx;
  }
  opt->numTieBreakKeys = numKeys - 1;
  return true;
}

void QOptimizer_Parse(AREQ *req) {
  QOptimizer *opt = req->optimizer;
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  opt->sctx = sctx;

  // get FieldSpec of sortby field and results limit
  PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
  if (arng) {
    opt->limit = arng->limit + arng->offset;
    if (!opt->limit) {
      opt->limit = DEFAULT_LIMIT;
    }
    if (array_len(arng->sortKeys)) {
      const char *name = arng->sortKeys[0];
      const FieldSpec *field = IndexSpec_GetFieldWithLength(sctx->spec, name, strlen(name));
      if (field && field->types == INDEXFLD_T_NUMERIC &&
          resolveTieBreakKeys(opt, sctx->spec, arng)) {
        opt->field = field;
        opt->fieldName = name;
        opt->asc = arng->sortAscMap & 0x01;
        opt->sortAscMap = arng->sortAscMap;
      } else {
        // sortby a non-numeric field first, or by a later key the heap cannot read, no optimization
        opt->type = Q_OPT_NONE;
      }
    }
  }

  // Q_OPT_NO_SORTER drops every sorter in the pipeline, but AGPLN_GetArrangeStep
  // only sees the arrange step after the last GROUPBY. A SORTBY anywhere earlier
  // must still be sorted, so rule NO_SORTER out.
  if (!opt->field && opt->type != Q_OPT_NONE && planHasSortKeys(AREQ_AGGPlan(req))) {
    opt->type = Q_OPT_NONE;
  }

  // get scorer function if there is no sortby
  if (opt->field) {
    opt->scorerType = SCORER_TYPE_NONE;
  } else {
    const char *scorer = req->searchopts.scorerName;
    // Scorers reading only document metadata, never term data.
    if (scorer && (!strcmp(scorer, DOCSCORE_SCORER) ||
                   !strcmp(scorer, HAMMINGDISTANCE_SCORER))) {
      opt->scorerType = SCORER_TYPE_DOC;
    } else {
      // Every other scorer, including the default and any extension-provided
      // one, needs scoring. Claiming otherwise drops the scorer and sorter from
      // the pipeline, so results come back unranked.
      opt->scorerType = SCORER_TYPE_TERM;
    }
  }
}

#define INVALUD_PTR ((void *)0xcafecafe) // Replace with BAD_POINTER

/* the function receives the QueryNode tree root and attempts to:
 * 1. find TEXT fields that need to be scored for some scorers
 * 2. find the numeric field used as SORTBY field  */
static QueryNode *checkQueryTypes(QueryNode *node, t_fieldIndex targetFieldIndex,
                                  QueryNode **parent, bool *reqScore) {
  QueryNode *ret = NULL;
  switch (node->type) {
    case QN_NUMERIC:
      // add support for multiple ranges on field
      if (targetFieldIndex != RS_INVALID_FIELD_INDEX && node->nn.nf->fieldIndex == targetFieldIndex) {
        ret = node;
      }
      break;

    case QN_PHRASE:  // INTERSECT
      // weight is different than 1
      if (node->opts.weight != 1) {
        break;
      }
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        QueryNode *cur = checkQueryTypes(node->children[i], targetFieldIndex, parent, reqScore);
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
      *reqScore = true;
      break;

    case QN_OPTIONAL:  // can't score optional ??
    case QN_NOT:       // can't score not      ??
    case QN_UNION:     // TODO
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        // ignore return value from a union since sortby optimization cannot be achieved.
        // check if it contains TEXT fields.
        checkQueryTypes(node->children[i], RS_INVALID_FIELD_INDEX, NULL, reqScore);
      }
      break;

    case QN_GEO:       // TODO: ADD GEO support
    case QN_GEOMETRY:
    case QN_IDS:       // NO SCORE
    case QN_TAG:       // NO SCORE
    case QN_LEXRANGE:  // NO SCORE, always a tag child
    case QN_VECTOR:    // NO SCORE
    case QN_WILDCARD:  // No SCORE
    case QN_NULL:
    case QN_MISSING:
      break;
  }
  return ret;
}

/* Whether every document matching `node` has its sort field inside some range, and if so, which.
 * The node stays in the tree as the exact filter; the range only bounds the optimizer's walk, so
 * it may be wider than the matches but never narrower. A range on the field bounds itself, an
 * intersection is bounded by any range it contains on the field, and a union by the hull of its
 * branches when each of them is bounded. A union with an unbounded branch, and every other node,
 * bounds nothing. */
static bool impliedSortFieldBounds(const QueryNode *node, t_fieldIndex fieldIndex,
                                   SortFieldBounds *bounds) {
  switch (node->type) {
    case QN_NUMERIC: {
      const NumericFilter *nf = node->nn.nf;
      if (nf->fieldIndex != fieldIndex) {
        return false;
      }
      *bounds = (SortFieldBounds){.min = nf->min,
                                  .max = nf->max,
                                  .minInclusive = nf->minInclusive,
                                  .maxInclusive = nf->maxInclusive};
      return true;
    }

    case QN_PHRASE:  // INTERSECT
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        if (impliedSortFieldBounds(node->children[i], fieldIndex, bounds)) {
          return true;
        }
      }
      return false;

    case QN_UNION: {
      if (QueryNode_NumChildren(node) == 0) {
        return false;
      }
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        SortFieldBounds branch;
        if (!impliedSortFieldBounds(node->children[i], fieldIndex, &branch)) {
          return false;
        }
        if (i == 0) {
          *bounds = branch;
          continue;
        }
        if (branch.min < bounds->min) {
          bounds->min = branch.min;
          bounds->minInclusive = branch.minInclusive;
        } else if (branch.min == bounds->min) {
          bounds->minInclusive |= branch.minInclusive;
        }
        if (branch.max > bounds->max) {
          bounds->max = branch.max;
          bounds->maxInclusive = branch.maxInclusive;
        } else if (branch.max == bounds->max) {
          bounds->maxInclusive |= branch.maxInclusive;
        }
      }
      return true;
    }

    default:
      return false;
  }
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
  t_fieldIndex targetFieldIndex = field ? field->index : RS_INVALID_FIELD_INDEX;
  bool hasOther = false;

  if (root->type == QN_WILDCARD) {
    opt->scorerType = SCORER_TYPE_NONE;
  }

  // find the sortby numeric node and remove it from query node tree
  QueryNode *parentNode = NULL;
  QueryNode *numSortbyNode = checkQueryTypes(root, targetFieldIndex, &parentNode, &opt->scorerReq);
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
      if (opt->type == Q_OPT_UNDECIDED) {
        opt->type = Q_OPT_PARTIAL_RANGE;
      }
      return;
    }
  }

  // No range on the sort field could be lifted out of the tree, but the query may still confine
  // the field to one, such as a keyset cursor's `@n:[(c +inf] | (@n:[c c] ...)`.
  if (isSortby && !opt->nf) {
    opt->hasImpliedBounds = impliedSortFieldBounds(root, targetFieldIndex, &opt->impliedBounds);
  }

  // there is no sorting field and scorer is required - we must check all results
  if ((!isSortby && opt->scorerReq) || (root->type == QN_VECTOR && root->vn.vq->type == VECSIM_QT_KNN)) {
    if (opt->type == Q_OPT_UNDECIDED) {
      opt->type = Q_OPT_NONE;
    }
    return;
  }

  // there are no other filter except for our numeric
  // if has sortby, use limited range
  // else, return after enough result found
  if (!opt->scorerReq) {
    if (isSortby) {
      if (opt->type == Q_OPT_UNDECIDED) {
        opt->type = Q_OPT_PARTIAL_RANGE;
      }
      return;
    } else {
      if (opt->type == Q_OPT_UNDECIDED) {
        opt->type = Q_OPT_NO_SORTER;
      }
      // No need for scorer, and there is no sorter. we can avoid calculating scores
      opt->scorerType = SCORER_TYPE_NONE;
      return;
    }
  }
}

// creates an intersect from root and numeric
static void updateRootIter(AREQ *req, QueryIterator *root, QueryIterator *new) {
  if (root->type == INTERSECT_ITERATOR) {
    AddIntersectionIteratorChild(root, new);
  } else {
    QueryIterator **its = rm_malloc(2 * sizeof(*its));
    its[0] = root;
    its[1] = new;
    // use slop==-1 and inOrder==0 since not applicable
    // use weight 1 since we checked at `checkQueryTypes`
    QueryIteratorTree_SetRoot(req->iteratorTree, NewIntersectionIterator(its, 2, -1, 0, 1));
  }
}

// wraps root with an OptimizerIterator, leaving root in place if that fails
static int setOptimizerRoot(AREQ *req, QOptimizer *opt, QueryIterator *root, QueryError *status) {
  QueryIterator *optimized = NewOptimizerIterator(opt, root, &req->ast.config);
  if (!optimized) {
    QueryError_SetError(status, QUERY_ERROR_CODE_LIMIT, "OFFSET/LIMIT too large for optimizer allocation");
    return REDISMODULE_ERR;
  }
  QueryIteratorTree_SetRoot(req->iteratorTree, optimized);
  return REDISMODULE_OK;
}

int QOptimizer_Iterators(AREQ *req, QOptimizer *opt, QueryError *status) {
  IndexSpec *spec = AREQ_SearchCtx(req)->spec;
  QueryIterator *root = QueryIteratorTree_Root(req->iteratorTree);

  // OptimizerIterator and the Q_OPT_UNDECIDED numeric fallback both rely on
  // the RAM DocTable / NumericRangeTree. AREQ_Compile rejects the queries that
  // would set QEXEC_OPTIMIZE on disk specs (DIALECT 4 and WITHOUTCOUNT), so this
  // function should never be entered for them.
  RS_ASSERT(!spec->diskSpec);

  switch (opt->type) {
    case Q_OPT_HYBRID:
      RS_ABORT("cannot be decided earlier");

    // Nothing to do here
    case Q_OPT_NO_SORTER:
    case Q_OPT_NONE:
    case Q_OPT_FILTER:
      return REDISMODULE_OK;

    // limit range to number of required LIMIT
    case Q_OPT_PARTIAL_RANGE: {
      if (root->type == WILDCARD_ITERATOR) {
        if (setOptimizerRoot(req, opt, root, status) != REDISMODULE_OK) {
          return REDISMODULE_ERR;
        }
      } else if (req->ast.root->type == QN_NUMERIC) {
        // trim the union numeric iterator to have the minimal number of ranges
        if (root->type == UNION_ITERATOR) {
          TrimUnionIterator(root, opt->limit, opt->asc);
        }
      } else {
        if (setOptimizerRoot(req, opt, root, status) != REDISMODULE_OK) {
          return REDISMODULE_ERR;
        }
      }
      return REDISMODULE_OK;
    }
    case Q_OPT_UNDECIDED: {
      if (!opt->field) {
        // TODO: For now set to NONE. Maybe add use of FILTER
        opt->type = Q_OPT_NONE;
        // Read `fieldIndex` rather than deriving it from a FieldSpec* captured earlier,
        // which could already be freed by now.
        FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = opt->sortbyNode->nn.nf->fieldIndex}, .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};
        QueryIterator *numericIter = NewNumericFilterIterator(AREQ_SearchCtx(req), opt->sortbyNode->nn.nf, INDEXFLD_T_NUMERIC,
                                                              &req->ast.config, &filterCtx);
        updateRootIter(req, root, numericIter);
        return REDISMODULE_OK;
      }
      opt->type = Q_OPT_HYBRID;
      // replace root with OptimizerIterator
      if (setOptimizerRoot(req, opt, root, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    }
  }
  return REDISMODULE_OK;
}

void QOptimizer_UpdateTotalResults(AREQ *req) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
    size_t reqLimit = arng && arng->isLimited ? arng->limit : DEFAULT_LIMIT;
    size_t reqOffset = arng && arng->isLimited ? arng->offset : 0;
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
    // Fold the loader-drop correction into the total before offset/limit rewrites it,
    // then clear it so it isn't subtracted again at reply time. This keeps the
    // skippedResults <= totalResults invariant that QITR_ReportedTotal asserts.
    qctx->totalResults = QITR_ReportedTotal(qctx);
    qctx->skippedResults = 0;
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
