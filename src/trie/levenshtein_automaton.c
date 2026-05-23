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
#include "levenshtein_automaton.h"
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

dfaNode *dfaNode_new(int distance, sparseVector *state) {
  dfaNode *ret = rm_calloc(1, sizeof(dfaNode));
  ret->fallback = NULL;
  ret->distance = distance;
  ret->v = state;
  ret->edges = NULL;
  ret->numEdges = 0;

  return ret;
}

void dfaNode_free(dfaNode *d) {
  sparseVector_free(d->v);
  if (d->edges) rm_free(d->edges);
  rm_free(d);
}

dfaNode *dfaCache_get(Vector *cache, sparseVector *v) {
  size_t n = Vector_Size(cache);
  for (int i = 0; i < n; i++) {
    dfaNode *dfn;
    Vector_Get(cache, i, &dfn);

    if (sv_equals(v, dfn->v)) {
      return dfn;
    }
  }
  return NULL;
}

void dfaCache_put(Vector *cache, dfaNode *dfn) {
  Vector_Push(cache, dfn);
}

inline dfaNode *dfaNode_getEdge(dfaNode *n, rune r) {
  for (int i = 0; i < n->numEdges; i++) {
    if (n->edges[i].r == r) {
      return n->edges[i].n;
    }
  }
  return NULL;
}

void dfaNode_addEdge(dfaNode *n, rune r, dfaNode *child) {
  n->edges = rm_realloc(n->edges, sizeof(dfaEdge) * (n->numEdges + 1));
  n->edges[n->numEdges++] = (dfaEdge){.r = r, .n = child};
}

void dfa_build(dfaNode *parent, SparseAutomaton *a, Vector *cache) {
  parent->match = SparseAutomaton_IsMatch(a, parent->v);

  for (int i = 0; i < parent->v->len; i++) {
    if (parent->v->entries[i].idx < a->len) {
      rune c = a->string[parent->v->entries[i].idx];
      dfaNode *edge = dfaNode_getEdge(parent, c);
      if (edge == NULL) {
        sparseVector *nv = SparseAutomaton_Step(a, parent->v, c);

        if (nv->len > 0) {
          dfaNode *dfn = dfaCache_get(cache, nv);
          if (dfn == NULL) {
            int dist = nv->entries[nv->len - 1].val;
            edge = dfaNode_new(dist, nv);
            dfaNode_addEdge(parent, c, edge);
            dfaCache_put(cache, edge);
            dfa_build(edge, a, cache);
            continue;
          } else {
            dfaNode_addEdge(parent, c, dfn);
          }
        }
        sparseVector_free(nv);
      }
    }
  }

  // if (parent->distance < a->max) {
  sparseVector *nv = SparseAutomaton_Step(a, parent->v, 1);
  if (nv->len > 0) {
    dfaNode *dfn = dfaCache_get(cache, nv);
    if (dfn) {
      parent->fallback = dfn;
    } else {
      int dist = nv->entries[nv->len - 1].val;
      parent->fallback = dfaNode_new(dist, nv);
      dfaCache_put(cache, parent->fallback);
      dfa_build(parent->fallback, a, cache);
      return;
    }
  }
  sparseVector_free(nv);

  //}
}
