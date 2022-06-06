#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <string.h>
#include "levenshtein.h"
#include "rune_util.h"
#include "rmalloc.h"

//-----------------------------------------------------------------------------

// Start initializes the automaton's state vector and returns it for further
// iteration
sparseVector *SparseAutomaton::Start() {
  int vals[max + 1];
  for (int i = 0; i < max + 1; i++) {
    vals[i] = i;
  }

  return new sparseVector(vals, max + 1);
}

//-----------------------------------------------------------------------------

// Step returns the next state of the automaton given a previous state and a
// character to check
sparseVector *SparseAutomaton::Step(sparseVector *state, rune c) {
  sparseVector *newVec = new sparseVector(state->len);

  if (state->len) {
    sparseVectorEntry e = state->entries[0];
    if (e.idx == 0 && e.val < max) {
      newVec->append(0, e.val + 1);
    }
  }

  for (int j = 0; j < state->len; j++) {
    sparseVectorEntry *entry = &state->entries[j];

    if (entry->idx == len) {
      break;
    }

    register int val = state->entries[j].val;
    // increase the cost by 1
    if (string[entry->idx] != c) ++val;

    if (newVec->len && newVec->entries[newVec->len - 1].idx == entry->idx) {
      val = MIN(val, newVec->entries[newVec->len - 1].val + 1);
    }

    if (j + 1 < state->len && state->entries[j + 1].idx == entry->idx + 1) {
      val = MIN(val, state->entries[j + 1].val + 1);
    }

    if (val <= max) {
      newVec->append(entry->idx + 1, val);
    }
  }
  return newVec;
}

//-----------------------------------------------------------------------------

// IsMatch returns true if the current state vector represents a string that is
// within the max
// edit distance from the initial automaton string
inline bool SparseAutomaton::IsMatch(sparseVector *v) const {
  return v->len && v->entries[v->len - 1].idx == len;
}

//-----------------------------------------------------------------------------

// CanMatch returns true if there is a possibility that feeding the automaton
// with more steps will yield a match.
// Once CanMatch is false there is no point in continuing iteration.
inline bool SparseAutomaton::CanMatch(sparseVector *v) const {
  return v->len > 0;
}

//-----------------------------------------------------------------------------

dfaNode::dfaNode(int distance, sparseVector *state) {
  fallback = NULL;
  distance = distance;
  v = state;
  edges = NULL;
  numEdges = 0;
}

//-----------------------------------------------------------------------------

dfaNode::~dfaNode() {
  delete v;
  if (edges) rm_free(edges);
}

//-----------------------------------------------------------------------------

static bool sparseVector::equals(sparseVector *sv1, sparseVector *sv2) {
  if (sv1->len != sv2->len) return 0;

  for (int i = 0; i < sv1->len; i++) {
    if (sv1->entries[i].idx != sv2->entries[i].idx || sv1->entries[i].val != sv2->entries[i].val) {
      return false;
    }
  }

  return true;
}

//-----------------------------------------------------------------------------

static dfaNode *dfaNode::getCache(Vector *cache, sparseVector *v) {
  size_t n = cache->Size();
  for (int i = 0; i < n; i++) {
    dfaNode *dfn;
    cache->Get(i, &dfn);

    if (sparseVector::equals(v, dfn->v)) {
      return dfn;
    }
  }
  return NULL;
}

//-----------------------------------------------------------------------------

void dfaNode::putCache(Vector *cache) {
  cache->Push(this);
}

//-----------------------------------------------------------------------------

// Get an edge for a dfa node given the next rune

inline dfaNode *dfaNode::getEdge(rune r) {
  for (int i = 0; i < numEdges; i++) {
    if (edges[i].r == r) {
      return edges[i].n;
    }
  }
  return NULL;
}

//-----------------------------------------------------------------------------

void dfaNode::addEdge(rune r, dfaNode *child) {
  edges = rm_realloc(edges, sizeof(dfaEdge) * (numEdges + 1));
  edges[numEdges++] = new dfaEdge(child, r);
}

//-----------------------------------------------------------------------------

void dfaNode::build(SparseAutomaton *a, Vector *cache) {
  match = a->IsMatch(v);

  for (int i = 0; i < v->len; i++) {
    if (v->entries[i].idx < a->len) {
      rune c = a->string[v->entries[i].idx];
      // printf("%c ---> ", c);
      dfaNode *edge = getEdge(c);
      if (edge == NULL) {
        sparseVector *nv = a->Step(v, c);

        if (nv->len > 0) {
          dfaNode *dfn = getCache(cache, nv);
          if (dfn == NULL) {
            int dist = nv->entries[nv->len - 1].val;
            edge = new dfaNode(dist, nv);
            addEdge(c, edge);
            edge->putCache(cache);
            edge->build(edge, a, cache);
            continue;
          } else {
            addEdge(c, dfn);
          }
        }
        delete nv;
      }
    }
  }

  // if (distance < a->max) {
  sparseVector *nv = a->Step(v, 1);
  if (nv->len > 0) {
    dfaNode *dfn = getCache(cache, nv);
    if (dfn) {
      fallback = dfn;
    } else {
      int dist = nv->entries[nv->len - 1].val;
      // printf("DEFAULT EDGE! edge %s - dist %d\n", a->string, dist);
      fallback = new dfaNode(dist, nv);
      fallback->putCache(cache);
      fallback->build(a, cache);
      return;
    }
  }

  //}
}

//-----------------------------------------------------------------------------

/* Create a new DFA filter  using a Levenshtein automaton, for the given string  and maximum
* distance. If prefixMode is 1, we match prefixes within the given distance, and then continue
* onwards to all suffixes. */

DFAFilter::DFAFilter(rune *str, size_t len, int maxDist, int prefixMode) {
  Vector *cache = new Vector<dfaNode *>(8);

  SparseAutomaton *a_ = new SparseAutomaton(str, len, maxDist);

  sparseVector *v = a_->Start();
  dfaNode *dr = new dfaNode(0, v);
  dr->putCache(cache);
  dr->build(a_, cache);

  cache = cache;
  stack = new Vector<dfaNode *>(8);
  distStack = new Vector<int>(8);
  a = &a_;
  prefixMode = prefixMode;
  stack->Push(dr);
  distStack->Push(maxDist + 1);
}

//-----------------------------------------------------------------------------

/* Free the underlying data of the DFA Filter. Note that since DFAFilter is created on the stack, it
* is not freed by itself. */

DFAFilter::~DFAFilter() {
  for (int i = 0; i < cache->Size(); i++) {
    dfaNode *dn;
    cache->Get(i, &dn);

    if (dn) delete dn;
  }

  delete cache;
  delete stack;
  delete distStack;
}

//-----------------------------------------------------------------------------

FilterCode FilterFunc(rune b, void *ctx, int *matched, void *matchCtx) {
  DFAFilter *fc = ctx;
  dfaNode *dn;
  int minDist;

  fc->stack->Get(fc->stack->Size() - 1, &dn);
  fc->distStack->Get(fc->distStack->Size() - 1, &minDist);

  // a null node means we're in prefix mode, and we're done matching our prefix
  if (dn == NULL) {
    *matched = 1;
    fc->stack->Push(NULL);
    fc->distStack->Push(minDist);
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
  dfaNode *next = dn->getEdge(foldedRune);
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
    fc->stack->Push(next);
    fc->distStack->Push(MIN(next->distance, minDist));
    return F_CONTINUE;
  } else if (fc->prefixMode && *matched) {
    fc->stack->Push(NULL);
    fc->distStack->Push(minDist);
    return F_CONTINUE;
  }

  return F_STOP;
}

//-----------------------------------------------------------------------------

void StackPop(void *ctx, int numLevels) {
  DFAFilter *fc = ctx;

  for (int i = 0; i < numLevels; i++) {
    fc->stack->Pop(NULL);
    fc->distStack->Pop(NULL);
  }
}
