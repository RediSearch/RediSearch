#include "levenshtein.h"
#include "rune_util.h"
#include "rmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <string.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Start initializes the automaton's state vector and returns it for further iteration

SparseVector SparseAutomaton::Start() {
  int vals[max + 1];
  for (int i = 0; i <= max; ++i) {
    vals[i] = i;
  }

  return SparseVector(vals, max + 1);
}

//---------------------------------------------------------------------------------------------

// Step from a given state of an automaton to the next step given a specific character

SparseVector SparseAutomaton::Step(SparseVector &state, rune c) {
  SparseVector newVec(state.size());

  if (state.size()) {
    SparseVectorEntry e = state[0];
    if (e.idx == 0 && e.val < max) {
      newVec.append(0, e.val + 1);
    }
  }

  for (int j = 0; j < state.size(); j++) {
    SparseVectorEntry &entry = state[j];

    if (entry.idx == runes.len()) {
      break;
    }

    int val = state[j].val;
    // increase the cost by 1
    if (runes[entry.idx] != c) ++val;

    if (newVec.size() && newVec.back().idx == entry.idx) {
      val = MIN(val, newVec.back().val + 1);
    }

    if (j + 1 < state.size() && state[j + 1].idx == entry.idx + 1) {
      val = MIN(val, state[j + 1].val + 1);
    }

    if (val <= max) {
      newVec.append(entry.idx + 1, val);
    }
  }
  return newVec;
}

//---------------------------------------------------------------------------------------------

// IsMatch returns true if the current state vector represents a string that is
// within the max edit distance from the initial automaton string

bool SparseAutomaton::IsMatch(const SparseVector &v) const {
  return v.size() && v.back().idx == runes.len();
}

//---------------------------------------------------------------------------------------------

// CanMatch returns true if there is a possibility that feeding the automaton
// with more steps will yield a match.
// Once CanMatch is false there is no point in continuing iteration.

bool SparseAutomaton::CanMatch(const SparseVector &v) const {
  return v.size() > 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

DFANode::DFANode(int distance, SparseVector&& state) :
  distance(distance), fallback(NULL), v(std::move(state)) {
}

///////////////////////////////////////////////////////////////////////////////////////////////

DFANode *DFACache::find(const SparseVector &v) const {
  for (auto &dfn: *this) {
    if (v == dfn->v) {
      return &*dfn;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

void DFACache::put(DFANode *node) {
  push_back(std::unique_ptr<DFANode>(node));
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Get a node associated with an edge given the next rune

DFANode *DFANode::getEdgeNode(rune r) {
  for (auto &edge: edges) {
    if (edge.r == r) {
      return edge.n;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

void DFANode::addEdge(rune r, DFANode *child) {
  edges.emplace_back(child, r);
}

//---------------------------------------------------------------------------------------------

// Recusively build the DFA node and all its descendants

void DFANode::build(SparseAutomaton &a, DFACache &cache) {
  match = a.IsMatch(v);
  for (auto &ent: v) {
    if (ent.idx >= a.runes.len()) {
      continue;
    }
    rune c = a.runes[ent.idx];
    // printf("%c ---> ", c);
    DFANode *edge_dfn = getEdgeNode(c);
    if (edge_dfn != NULL) {
      continue;
    }
    SparseVector nv = a.Step(v, c);
    if (nv.empty()) {
      continue;
    }
    DFANode *dfn = cache.find(nv);
    if (dfn != NULL) {
      addEdge(c, dfn);
      continue;
    }
    int dist = nv.back().val;
    edge_dfn = new DFANode(dist, std::move(nv));
    addEdge(c, edge_dfn);
    cache.put(edge_dfn);
    edge_dfn->build(a, cache);
  }

  SparseVector nv = a.Step(v, 1);
  if (nv.empty()) {
    return;
  }

  DFANode *dfn = cache.find(nv);
  if (dfn) {
    fallback = dfn;
    return;
  }

  int dist = nv.back().val;
  // printf("DEFAULT EDGE! edge %s - dist %d\n", a.runes, dist);
  fallback = new DFANode(dist, std::move(nv));
  cache.put(fallback);
  fallback->build(a, cache);
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Create DFA filter using Levenshtein automaton, for the given string and maximum distance.
// If prefixMode is true, match prefixes within given distance, then continue onwards to all suffixes.

DFAFilter::DFAFilter(Runes &runes, int maxDist, bool prefixMode) :
    a(runes, maxDist), prefixMode(prefixMode) {

  cache.reserve(8);
  stack.reserve(8);
  distStack.reserve(8);

  SparseVector v = a.Start();
  DFANode *dr = new DFANode(0, std::move(v));
  cache.put(dr);
  dr->build(a, cache);

  stack.push_back(dr);
  distStack.push_back(maxDist + 1);
}

//---------------------------------------------------------------------------------------------

// A callback function for the DFA Filter, passed to the Trie iterator

FilterCode DFAFilter::Filter(rune b, int *matched, int *match) {
  DFANode *dn = stack.back();
  int minDist = distStack.back();

  // a null node means we're in prefix mode, and we're done matching our prefix
  if (dn == NULL) {
    *matched = 1;
    stack.push_back(NULL);
    distStack.push_back(minDist);
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
  DFANode *next = dn->getEdgeNode(foldedRune);
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
    stack.push_back(next);
    distStack.push_back(MIN(next->distance, minDist));
    return F_CONTINUE;
  } else if (prefixMode && *matched) {
    stack.push_back(NULL);
    distStack.push_back(minDist);
    return F_CONTINUE;
  }

  return F_STOP;
}

//---------------------------------------------------------------------------------------------

// A stack-pop callback, passed to the trie iterator.
// It's called when we reach a dead end and need to rewind the stack of the filter.

void DFAFilter::StackPop(int numLevels) {
  for (int i = 0; i < numLevels; ++i) {
    stack.pop_back();
    distStack.pop_back();
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
