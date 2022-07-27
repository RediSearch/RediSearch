#pragma once

#include "trie/rune_util.h"
#include "rmutil/vector.h"
#include "redisearch.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

///////////////////////////////////////////////////////////////////////////////////////////////

typedef uint16_t t_len;

#define TRIE_INITIAL_STRING_LEN 256
#define TRIE_MAX_PREFIX 100

#define TRIENODE_TERMINAL 0x1
#define TRIENODE_DELETED 0x2

#define TRIENODE_SORTED_NONE 0
#define TRIENODE_SORTED_SCORE 1
#define TRIENODE_SORTED_LEX 2

//---------------------------------------------------------------------------------------------

#define ITERSTATE_SELF 0
#define ITERSTATE_CHILDREN 1
#define ITERSTATE_MATCH 2

//---------------------------------------------------------------------------------------------

#pragma pack(1)
struct TriePayload : Object {
  uint32_t len;  // 4G payload is more than enough
  char data[];   // data will not take an extra pointer

  TriePayload(const char *payload, uint32_t plen);
};
#pragma pack()

//---------------------------------------------------------------------------------------------

// The byte size of a TriePayload, based on its internal data length
size_t __triePayload_Sizeof(uint32_t len);

typedef void(TrieRangeCallback)(const rune *, size_t, void *);

//---------------------------------------------------------------------------------------------

struct RangeCtx {
  rune *buf;
  TrieRangeCallback *callback;
  void *cbctx;
  bool includeMin;
  bool includeMax;
};

//---------------------------------------------------------------------------------------------

enum TrieAddOp {
  ADD_REPLACE,
  ADD_INCR,
};

//---------------------------------------------------------------------------------------------

struct TrieNode;

//---------------------------------------------------------------------------------------------

// trie iterator stack node. for internal use only
struct StackNode {
  int state;
  TrieNode *n;
  t_len stringOffset;
  t_len childOffset;
  bool skipped;

  StackNode(bool skipped, TrieNode *node) : state(ITERSTATE_SELF), n(node), childOffset(0),
    stringOffset(0), skipped(skipped) {}
};

//---------------------------------------------------------------------------------------------

enum FilterCode { F_CONTINUE = 0, F_STOP = 1 };

// A callback for an automaton that receives the current state, evaluates the next byte,
// and returns the next state of the automaton. If we should not continue down, return F_STOP.
typedef FilterCode (*StepFilter)(rune b, void *ctx, int *match, void *matchCtx);

typedef void (*StackPopCallback)(void *ctx, int num);

//---------------------------------------------------------------------------------------------

#pragma pack(1)

// Opaque trie iterator type

template <class T>
struct TrieIterator : public Object {
  rune buf[TRIE_INITIAL_STRING_LEN + 1];
  t_len bufOffset;

  Vector<StackNode*> stack;
  StepFilter filter;
  float minScore;
  int nodesConsumed;
  int nodesSkipped;
  StackPopCallback popCallback;
  T ctx;

  TrieIterator(TrieNode *node, StepFilter f, StackPopCallback pf, T &&obj);
  ~TrieIterator();

  void Push(TrieNode *node, int skipped);
  void Pop();

  // current top of iterator stack
  StackNode &current() { return *stack.back(); }

  enum StepResult {
    __STEP_STOP = 0, // Stop the iteration
    __STEP_CONT = 1, // Continue to next node
    __STEP_MATCH = 3 // Match found, return the state to the user but continue afterwards
  };

  StepResult Step(void *matchCtx);
  bool Next(rune **ptr, t_len *len, RSPayload *payload, float *score, void *matchCtx);
};

//---------------------------------------------------------------------------------------------

// TrieNode represents a single node in a trie.
// The actual size of it is bigger, as the children are allocated after str[].
// Nonterminal nodes always have a score of 0, meaning nodes with score 0 cannot be inserted to the trie.

struct TrieNode : public Object {
  t_len _len; // string length of this node. can be 0

  uint8_t _flags : 2;
  uint8_t _sortmode : 2;

  float _score; // Nonterminal nodes always have a score of 0

  float _maxChildScore; // maximal score of any descendant of this node, used to optimize traversal

  TriePayload *_payload; // payload of terminal node. can be NULL if nonterminal.

  Runes _str; // string of current node
  Vector<TrieNode*> _children;

  TrieNode(rune *runes, t_len offset = 0, t_len new_len = 0, const char *new_payload = NULL, size_t plen = 0,
           t_len numChildren = 0, float score = 0, bool terminal = false);
  ~TrieNode();

  void Print(int idx, int depth); //@@ looks like nobody is using it
  TrieNode *Add(rune *runes, t_len len, RSPayload *payload, float score, TrieAddOp op);

  TrieNode *AddChild(rune *runes, t_len offset, t_len len, RSPayload *payload, float score);
  void SplitNode(t_len offset);

  float Find(rune *runes, t_len len) const;

  bool Delete(rune *runes, t_len len);

  template <class T>
  TrieIterator<T> Iterate(StepFilter f, StackPopCallback pf, T &&obj);

  TrieNode *RandomWalk(int minSteps, Runes &runes);
  void MergeWithSingleChild();

  void sortChildren();
  void optimizeChildren();

  void IterateRange(const rune *min, int minlen, bool includeMin, const rune *max,
                    int maxlen, bool includeMax, TrieRangeCallback callback, void *ctx);
  void rangeIterate(const rune *min, int nmin, const rune *max, int nmax, RangeCtx *r);
  void rangeIterateSubTree(RangeCtx *r);

  static int Cmp(const void *p1, const void *p2);

//  static size_t Size(t_len numChildren, t_len slen);

  bool isTerminal() const { return _flags & TRIENODE_TERMINAL; }
  bool isDeleted() const { return _flags & TRIENODE_DELETED; }
};
#pragma pack()

//---------------------------------------------------------------------------------------------

#include "trie_iter.hxx"

///////////////////////////////////////////////////////////////////////////////////////////////
