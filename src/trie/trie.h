#pragma once

#include "trie/rune_util.h"
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
  int isSkipped;
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

struct TrieIterator : public Object {
  rune buf[TRIE_INITIAL_STRING_LEN + 1];
  t_len bufOffset;

  StackNode stack[TRIE_INITIAL_STRING_LEN + 1];
  t_len stackOffset;
  StepFilter filter;
  float minScore;
  int nodesConsumed;
  int nodesSkipped;
  StackPopCallback popCallback;
  void *ctx;

  TrieIterator(TrieNode *node, StepFilter f, StackPopCallback pf, void *ctx);
  ~TrieIterator();

  void Push(TrieNode *node, int skipped);
  void Pop();

  // current top of iterator stack
  StackNode &current() { return stack[stackOffset - 1]; }

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
  t_len len; // string length of this node. can be 0
  t_len numChildren; // number of child nodes

  uint8_t flags : 2;
  uint8_t sortmode : 2;

  float score; // Nonterminal nodes always have a score of 0

  float maxChildScore; // maximal score of any descendant of this node, used to optimize traversal
  
  TriePayload *payload; // payload of terminal node. can be NULL if nonterminal.
  
  rune str[]; // string of current node
  // ... now come the children, to be accessed with children()

  TrieNode(Runes &runes, t_len offset, t_len len_, const char *payload_, size_t plen,
           t_len numChildren_, float score_, bool terminal);
  ~TrieNode();

  void Print(int idx, int depth); //@@ looks like nobody is using it
  TrieNode *Add(Runes &runes, t_len len_, RSPayload *payload, float score_, TrieAddOp op);

  TrieNode *AddChild(Runes &runes, t_len offset, t_len len_, RSPayload *payload, float score_);
  TrieNode *SplitNode(t_len offset);

  float Find(Runes *runes, t_len len_) const;

  bool Delete(rune *str_, t_len len_);

  TrieIterator Iterate(StepFilter f, StackPopCallback pf, void *ctx_);

  TrieNode *RandomWalk(int minSteps, rune **str, t_len *len);
  TrieNode *MergeWithSingleChild();

  void sortChildren();
  void optimizeChildren();

  void IterateRange(const rune *min, int minlen, bool includeMin, const rune *max,
                    int maxlen, bool includeMax, TrieRangeCallback callback, void *ctx);
  void rangeIterate(const rune *min, int nmin, const rune *max, int nmax, RangeCtx *r);
  void rangeIterateSubTree(RangeCtx *r);

  static int Cmp(const void *p1, const void *p2);

  static size_t Size(t_len numChildren, t_len slen);

  // Get pointer to children array of node. This is not an actual node member for memory saving reasons.
  TrieNode **getChildren() { return ((void *)this + sizeof(TrieNode) + (len + 1) * sizeof(rune)); }

  bool isTerminal() const { return flags & TRIENODE_TERMINAL; }
  bool isDeleted() const { return flags & TRIENODE_DELETED; }
};
#pragma pack()

//---------------------------------------------------------------------------------------------

#define ITERSTATE_SELF 0
#define ITERSTATE_CHILDREN 1
#define ITERSTATE_MATCH 2

///////////////////////////////////////////////////////////////////////////////////////////////
