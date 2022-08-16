#include "query_optimizer.h"
#include "numeric_index.h"

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

#define INVALUD_PTR ((void *)0xcafecafe)

// TODO: think of location
static QueryNode *checkQueryTypes(QueryNode *node, const char *name, QueryNode **parent,
                                  bool *otherType) {
  QueryNode *ret = NULL;
  switch (node->type) {
    case QN_NUMERIC:
      // add support for multiple ranges on field
      if (name && !strcmp(name, node->nn.nf->fieldName)) {
        ret = node;
      }
      break;

    case QN_PHRASE:  // INTERSECT
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        QueryNode *cur = checkQueryTypes(node->children[i], name, parent, otherType);
        if (cur != NULL && *parent != NULL) {
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
    case QN_LEXRANGE:        // score lex        ??
      *otherType = true;
      break;

    case QN_OPTIONAL:  // can't score optional ??
    case QN_NOT:       // can't score not      ??
    case QN_UNION:     // TODO
      for (int i = 0; i < QueryNode_NumChildren(node); ++i) {
        // ignore return value from a union
        checkQueryTypes(node->children[i], NULL, NULL, otherType);
      }
      break;

    case QN_GEO:       // TODO: ADD GEO support
    case QN_IDS:       // NO SCORE
    case QN_TAG:       // NO SCORE
    case QN_VECTOR:    // NO SCORE
    case QN_WILDCARD:  // No SCORE
    case QN_NULL:
      break;
  }
  return ret;
}

void QOptimizer_QueryNodes(QueryNode *root, QOptimizer *opt) {
  const char *name = opt->fieldName;
  const FieldSpec *field = opt->field;
  bool isSortby = !!field;
  // bool isSortable = field->options & FieldSpec_Sortable;
  bool hasOther = false;

  // TODO: finetune for cases with  scorer for statistics only like coordinator
  if (!isSortby && opt->scorerReq) {
    opt->type = Q_OPT_NONE;
    return;
  }

  // find the sortby numeric node and remove it from query node tree
  QueryNode *parentNode = NULL;
  QueryNode *numSortbyNode = checkQueryTypes(root, name, &parentNode, &hasOther);
  if (numSortbyNode && numSortbyNode != INVALUD_PTR) {
    RS_LOG_ASSERT(numSortbyNode->type == QN_NUMERIC, "found it");
    if (parentNode) {
      for (int i = 0; i < QueryNode_NumChildren(parentNode); ++i) {
        if (parentNode->children[i] == numSortbyNode) {
          array_del_fast(parentNode->children, i);
          break;
        }
      }
    }
    numSortbyNode->nn.nf->limit = opt->limit;
    numSortbyNode->nn.nf->asc = opt->asc;
  } else {
    numSortbyNode = NULL;
  }
  opt->sortbyNode = numSortbyNode;

  // there are no other filter except for our numeric
  // if has sortby, use limited range
  // else, return after enough result found
  if (!hasOther) {
    opt->type = isSortby ? Q_OPT_PARTIAL_RANGE : Q_OPT_NO_SORTER;
    return;
  }
  opt->type = Q_OPT_UNDECIDED;
}

void QOptimizer_Iterators(AREQ *req, QOptimizer *opt) {
  IndexIterator *iter = req->rootiter;

  // no other filter and no sortby, can return early. nothing to do here
  if (opt->type == Q_OPT_NO_SORTER || opt->type == Q_OPT_NONE) {
    return;
  }

  // limit range to number of required LIMIT
  if (opt->type == Q_OPT_PARTIAL_RANGE) {
    if (iter->type == UNION_ITERATOR) {
      // trim the union numeric iterator to have the minimal number of ranges
      trimUnionIterator(iter, 0, opt->limit, opt->asc, true);
    } else if (iter->type == WILDCARD_ITERATOR) {
      // replace the root iterator with a numeric iterator with minimal number of ranges
      iter->Free(iter);
      // TODO: check leakage
      NumericFilter *nf = NewNumericFilter(NF_NEGATIVE_INFINITY, NF_INFINITY, 1, 1, 0,
                                           req->optimizer->limit, req->optimizer->asc);
      nf->fieldName = rm_strdup(req->optimizer->fieldName);
      req->rootiter = NewNumericFilterIterator(req->sctx, nf, NULL, INDEXFLD_T_NUMERIC);
    }
    return;
  }

  RS_LOG_ASSERT(opt->type == Q_OPT_UNDECIDED, "No optimization applied yet");

  switch (req->rootiter->type) {
    case WILDCARD_ITERATOR:
      RS_LOG_ASSERT(0, "treated");
    default:
      break;
      // TODO
  }
}
*/