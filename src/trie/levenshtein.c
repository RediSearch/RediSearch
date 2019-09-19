#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <string.h>
#include "levenshtein.h"
#include "rune_util.h"
#include "rmalloc.h"

// NewSparseAutomaton creates a new automaton for the string s, with a given max
// edit distance check
SparseAutomaton NewSparseAutomaton(const rune *s, size_t len, int maxEdits) {
  return (SparseAutomaton){s, len, maxEdits};
}

// Start initializes the automaton's state vector and returns it for further
// iteration
sparseVector *SparseAutomaton_Start(SparseAutomaton *a) {
  int vals[a->max + 1];
  for (int i = 0; i < a->max + 1; i++) {
    vals[i] = i;
  }

  return newSparseVector(vals, a->max + 1);
}

// Step returns the next state of the automaton given a previous state and a
// character to check
sparseVector *SparseAutomaton_Step(SparseAutomaton *a, sparseVector *state, rune c) {
  sparseVector *newVec = newSparseVectorCap(state->len);

  if (state->len) {
    sparseVectorEntry e = state->entries[0];
    if (e.idx == 0 && e.val < a->max) {
      sparseVector_append(&newVec, 0, e.val + 1);
    }
  }

  for (int j = 0; j < state->len; j++) {
    sparseVectorEntry *entry = &state->entries[j];

    if (entry->idx == a->len) {
      break;
    }

    register int val = state->entries[j].val;
    // increase the cost by 1
    if (a->string[entry->idx] != c) ++val;

    if (newVec->len && newVec->entries[newVec->len - 1].idx == entry->idx) {
      val = MIN(val, newVec->entries[newVec->len - 1].val + 1);
    }

    if (j + 1 < state->len && state->entries[j + 1].idx == entry->idx + 1) {
      val = MIN(val, state->entries[j + 1].val + 1);
    }

    if (val <= a->max) {
      sparseVector_append(&newVec, entry->idx + 1, val);
    }
  }
  return newVec;
}

// IsMatch returns true if the current state vector represents a string that is
// within the max
// edit distance from the initial automaton string
inline int SparseAutomaton_IsMatch(SparseAutomaton *a, sparseVector *v) {
  return v->len && v->entries[v->len - 1].idx == a->len;
}

// CanMatch returns true if there is a possibility that feeding the automaton
// with more steps will
// yield a match. Once CanMatch is false there is no point in continuing
// iteration
inline int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v) {
  return v->len > 0;
}

dfaNode *__newDfaNode(int distance, sparseVector *state) {
  dfaNode *ret = rm_calloc(1, sizeof(dfaNode));
  ret->fallback = NULL;
  ret->distance = distance;
  ret->v = state;
  ret->edges = NULL;
  ret->numEdges = 0;

  return ret;
}

void __dfaNode_free(dfaNode *d) {
  sparseVector_free(d->v);
  if (d->edges) rm_free(d->edges);
  rm_free(d);
}

int __sv_equals(sparseVector *sv1, sparseVector *sv2) {
  if (sv1->len != sv2->len) return 0;

  for (int i = 0; i < sv1->len; i++) {
    if (sv1->entries[i].idx != sv2->entries[i].idx || sv1->entries[i].val != sv2->entries[i].val) {
      return 0;
    }
  }

  return 1;
}

dfaNode *__dfn_getCache(Vector *cache, sparseVector *v) {
  size_t n = Vector_Size(cache);
  for (int i = 0; i < n; i++) {
    dfaNode *dfn;
    Vector_Get(cache, i, &dfn);

    if (__sv_equals(v, dfn->v)) {
      return dfn;
    }
  }
  return NULL;
}

void __dfn_putCache(Vector *cache, dfaNode *dfn) {
  Vector_Push(cache, dfn);
}

inline dfaNode *__dfn_getEdge(dfaNode *n, rune r) {
  for (int i = 0; i < n->numEdges; i++) {
    if (n->edges[i].r == r) {
      return n->edges[i].n;
    }
  }
  return NULL;
}

void __dfn_addEdge(dfaNode *n, rune r, dfaNode *child) {
  n->edges = rm_realloc(n->edges, sizeof(dfaEdge) * (n->numEdges + 1));
  n->edges[n->numEdges++] = (dfaEdge){.r = r, .n = child};
}

void dfa_build(dfaNode *parent, SparseAutomaton *a, Vector *cache) {
  parent->match = SparseAutomaton_IsMatch(a, parent->v);

  for (int i = 0; i < parent->v->len; i++) {
    if (parent->v->entries[i].idx < a->len) {
      rune c = a->string[parent->v->entries[i].idx];
      // printf("%c ---> ", c);
      dfaNode *edge = __dfn_getEdge(parent, c);
      if (edge == NULL) {
        sparseVector *nv = SparseAutomaton_Step(a, parent->v, c);

        if (nv->len > 0) {
          dfaNode *dfn = __dfn_getCache(cache, nv);
          if (dfn == NULL) {
            int dist = nv->entries[nv->len - 1].val;
            edge = __newDfaNode(dist, nv);
            __dfn_addEdge(parent, c, edge);
            __dfn_putCache(cache, edge);
            dfa_build(edge, a, cache);
            continue;
          } else {
            __dfn_addEdge(parent, c, dfn);
          }
        }
        sparseVector_free(nv);
      }
    }
  }

  // if (parent->distance < a->max) {
  sparseVector *nv = SparseAutomaton_Step(a, parent->v, 1);
  if (nv->len > 0) {
    dfaNode *dfn = __dfn_getCache(cache, nv);
    if (dfn) {
      parent->fallback = dfn;
    } else {
      int dist = nv->entries[nv->len - 1].val;
      // printf("DEFAULT EDGE! edge %s - dist %d\n", a->string, dist);
      parent->fallback = __newDfaNode(dist, nv);
      __dfn_putCache(cache, parent->fallback);
      dfa_build(parent->fallback, a, cache);
      return;
    }
  }
  sparseVector_free(nv);

  //}
}

DFAFilter NewDFAFilter(rune *str, size_t len, int maxDist, int prefixMode) {
  Vector *cache = NewVector(dfaNode *, 8);

  SparseAutomaton a = NewSparseAutomaton(str, len, maxDist);

  sparseVector *v = SparseAutomaton_Start(&a);
  dfaNode *dr = __newDfaNode(0, v);
  __dfn_putCache(cache, dr);
  dfa_build(dr, &a, cache);

  DFAFilter ret;
  ret.cache = cache;
  ret.stack = NewVector(dfaNode *, 8);
  ret.distStack = NewVector(int, 8);
  ret.a = a;
  ret.prefixMode = prefixMode;
  Vector_Push(ret.stack, dr);
  Vector_Push(ret.distStack, (maxDist + 1));

  return ret;
}

void DFAFilter_Free(DFAFilter *fc) {
  for (int i = 0; i < Vector_Size(fc->cache); i++) {
    dfaNode *dn;
    Vector_Get(fc->cache, i, &dn);

    if (dn) __dfaNode_free(dn);
  }

  Vector_Free(fc->cache);
  Vector_Free(fc->stack);
  Vector_Free(fc->distStack);
}

FilterCode FilterFunc(rune b, void *ctx, int *matched, void *matchCtx) {
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
    // printf("MATCH %c, dist %d\n", b, dn->distance);
    int *pdist = matchCtx;
    if (pdist) {
      *pdist = MIN(dn->distance, minDist);
    }
  }

  rune foldedRune = runeFold(b);

  // get the next state change
  dfaNode *next = __dfn_getEdge(dn, foldedRune);
  if (!next) next = dn->fallback;

  // we can continue - push the state on the stack
  if (next) {
    if (next->match) {
      // printf("MATCH NEXT %c, dist %d\n", b, next->distance);
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

void StackPop(void *ctx, int numLevels) {
  DFAFilter *fc = ctx;

  for (int i = 0; i < numLevels; i++) {
    Vector_Pop(fc->stack, NULL);
    Vector_Pop(fc->distStack, NULL);
  }
}
