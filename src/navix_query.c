/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "navix_query.h"
#include "numeric_filter.h"
#include "query_node.h"
#include "rmalloc.h"
#include "spec.h"
#include "util/arr.h"
#include <string.h>

struct NavixQuery {
  FilterSummaryContext context;
  VecSimNavixStats stats;
  NavixQuery* previous;
};

typedef struct {
  FilterQuickCheckPredicate nodes[256];
  size_t used;
  bool overflow;
} PredicateArena;

static void lower(PredicateArena* arena, FilterQuickCheckPredicate* out, const QueryNode* node,
                  const IndexSpec* spec, size_t depth, int tagField) {
  *out = (FilterQuickCheckPredicate){.type = FilterQuickCheck_UNKNOWN};
  if (!node || depth >= 32) return;
  if (tagField >= 0) {
    // Tag evaluation already normalized exact token children in the AST.
    // Expansions and multiword tags stay UNKNOWN in this first bridge.
    if (node->type == QN_TOKEN && node->tn.str && !memchr(node->tn.str, '\0', node->tn.len)) {
      *out = (FilterQuickCheckPredicate){
          .type = FilterQuickCheck_TERM,
          .term = {FilterQuickCheck_TAG, tagField, node->tn.str, node->tn.len}};
    }
    return;
  }
  if (node->type == QN_NUMERIC) {
    const NumericFilter* nf = node->nn.nf;
    if (nf && !nf->geoFilter && nf->fieldIndex < spec->numFields &&
        FIELD_IS(&spec->fields[nf->fieldIndex], INDEXFLD_T_NUMERIC)) {
      *out = (FilterQuickCheckPredicate){
          .type = FilterQuickCheck_NUMERIC,
          .numeric = {nf->fieldIndex, nf->min, nf->max, nf->minInclusive, nf->maxInclusive}};
    }
    return;
  }
  if (node->type != QN_PHRASE && node->type != QN_UNION && node->type != QN_TAG) return;
  if (node->type == QN_TAG) {
    if (node->tag.fieldIndex >= spec->numFields) return;
    tagField = node->tag.fieldIndex;
  }
  size_t count = array_len(node->children);
  if (!count) return;
  if (count > 256 - arena->used) {
    arena->overflow = true;
    return;
  }
  FilterQuickCheckPredicate* children = arena->nodes + arena->used;
  arena->used += count;
  out->type = node->type == QN_PHRASE ? FilterQuickCheck_AND : FilterQuickCheck_OR;
  out->operands.children = children;
  out->operands.len = count;
  for (size_t i = 0; i < count; ++i) {
    lower(arena, &children[i], node->children[i], spec, depth + 1, tagField);
  }
}

NavixQuery* NavixQuery_New(IndexSpec* spec, const QueryNode* filter, NavixQuery* previous,
                           VecSimNavixParams* params, bool blind) {
  PredicateArena arena = {.used = 1};
  lower(&arena, &arena.nodes[0], filter, spec, 0, -1);
  NavixQuery* query = rm_calloc(1, sizeof(*query));
  query->context = FilterSummary_CreateContext(spec, arena.overflow ? NULL : &arena.nodes[0]);
  query->previous = previous;
  *params = (VecSimNavixParams){.mayMatch = FilterSummary_MayMatch,
                                .context = &query->context,
                                .stats = &query->stats,
                                .forceBlind = blind};
  return query;
}

void NavixQuery_Free(NavixQuery* query) {
  while (query) {
    NavixQuery* previous = query->previous;
    FilterSummary_FreeContext(&query->context);
    rm_free(query);
    query = previous;
  }
}
