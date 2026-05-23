/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <string.h>
#include "dfa_filter.h"
#include "rune_util.h"
#include "rmalloc.h"

static rune runeLower(rune r) {
  uint32_t lowered = 0;
  const char *map = 0;
  map = nu_tolower((uint32_t)r);
  if (!map) {
    return r;
  }
  nu_casemap_read(map, &lowered);
  return (rune)lowered;
}

DFAFilter *NewDFAFilter(rune *str, size_t len, int maxDist, int prefixMode) {
  Vector *cache = NewVector(dfaNode *, 8);

  SparseAutomaton a = NewSparseAutomaton(str, len, maxDist);

  sparseVector *v = SparseAutomaton_Start(&a);
  dfaNode *dr = dfaNode_new(0, v);
  dfaCache_put(cache, dr);
  dfa_build(dr, &a, cache);

  DFAFilter *ret = rm_malloc(sizeof(*ret));
  ret->cache = cache;
  ret->stack = NewVector(dfaNode *, 8);
  ret->distStack = NewVector(int, 8);
  ret->a = a;
  ret->prefixMode = prefixMode;
  Vector_Push(ret->stack, dr);
  Vector_Push(ret->distStack, (maxDist + 1));

  return ret;
}

void DFAFilter_Free(DFAFilter *fc) {
  for (int i = 0; i < Vector_Size(fc->cache); i++) {
    dfaNode *dn;
    Vector_Get(fc->cache, i, &dn);

    if (dn) dfaNode_free(dn);
  }

  Vector_Free(fc->cache);
  Vector_Free(fc->stack);
  Vector_Free(fc->distStack);
}

// Emit the running minimum edit distance into the caller-supplied matchCtx
// (an `int *`). On each rune step we track minDist = MIN over all accept-state
// costs reached along the current DFA path, kept in parallel with the DFA
// state stack via distStack. dn->distance is the cost stored at the last
// entry of the DFA state's sparse vector — when dn->match is true this is
// min Levenshtein(query, input consumed so far) (see SparseAutomaton_IsMatch).
// In prefix mode, once the prefix accepts, minDist is preserved across the
// NULL stack entries pushed for the remainder of the term, so *matchCtx ends
// up holding the cost recorded at the accept boundary.
FilterCode FilterFunc(rune b, void *ctx, int *matched, void *matchCtx, runeTransform rTransform) {
  DFAFilter *fc = ctx;
  dfaNode *dn;
  int minDist;

  Vector_Get(fc->stack, Vector_Size(fc->stack) - 1, &dn);
  Vector_Get(fc->distStack, Vector_Size(fc->distStack) - 1, &minDist);

  // a null node means we're in prefix mode, and we're done matching our prefix
  if (dn == NULL) {
    *matched = 1;
    Vector_Push(fc->stack, NULL);
    Vector_Push(fc->distStack, minDist);
    return F_CONTINUE;
  }

  *matched = dn->match;

  if (*matched) {
    int *pdist = matchCtx;
    if (pdist) {
      *pdist = MIN(dn->distance, minDist);
    }
  }

  rune transformedRune = rTransform(b);

  // get the next state change
  dfaNode *next = dfaNode_getEdge(dn, transformedRune);
  if (!next) next = dn->fallback;

  // we can continue - push the state on the stack
  if (next) {
    if (next->match) {
      *matched = 1;
      int *pdist = matchCtx;
      if (pdist) {
        *pdist = MIN(next->distance, minDist);
      }
      //    if (fc->prefixMode) next = NULL;
    }
    Vector_Push(fc->stack, next);
    Vector_Push(fc->distStack, MIN(next->distance, minDist));
    return F_CONTINUE;
  } else if (fc->prefixMode && *matched) {
    Vector_Push(fc->stack, NULL);
    Vector_Push(fc->distStack, minDist);
    return F_CONTINUE;
  }

  return F_STOP;
}

// This function is used by FT.SUGGET flow
FilterCode FoldingFilterFunc(rune b, void *ctx, int *matched, void *matchCtx) {
  return FilterFunc(b, ctx, matched, matchCtx, runeFold);
}

// This function is used by TEXT fuzzy search flow
FilterCode LoweringFilterFunc(rune b, void *ctx, int *matched, void *matchCtx) {
  return FilterFunc(b, ctx, matched, matchCtx, runeLower);
}

void StackPop(void *ctx, int numLevels) {
  DFAFilter *fc = ctx;

  for (int i = 0; i < numLevels; i++) {
    Vector_Pop(fc->stack, NULL);
    Vector_Pop(fc->distStack, NULL);
  }
}
