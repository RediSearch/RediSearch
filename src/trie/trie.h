#pragma once

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include "rune_util.h"
#include "redisearch.h"

typedef uint16_t t_len;

#define TRIE_INITIAL_STRING_LEN 256
#define TRIE_MAX_PREFIX 100
#define TRIENODE_TERMINAL 0x1
#define TRIENODE_DELETED 0x2

#define TRIENODE_SORTED_NONE 0
#define TRIENODE_SORTED_SCORE 1
#define TRIENODE_SORTED_LEX 2

#pragma pack(1)
struct TriePayload : Object {
  uint32_t len;  // 4G payload is more than enough!!!!
  char data[];   // this means the data will not take an extra pointer.

  TriePayload(const char *payload, uint32_t plen);
};
#pragma pack()

/* The byte size of a TriePayload, based on its internal data length */
size_t __triePayload_Sizeof(uint32_t len);

typedef void(TrieRangeCallback)(const rune *, size_t, void *);

struct RangeCtx {
  rune *buf;
  TrieRangeCallback *callback;
  void *cbctx;
  bool includeMin;
  bool includeMax;
};

enum TrieAddOp {
  ADD_REPLACE,
  ADD_INCR,
};

#pragma pack(1)
/* TrieNode represents a single node in a trie. The actual size of it is bigger,
 * as the children are
 * allocated after str[].
 * Non terminal nodes always have a score of 0, meaning you can't insert nodes
 * with score 0 to the
 * trie.
 */
struct TrieNode : public Object {
  // the string length of this node. can be 0
  t_len len;
  // the number of child nodes
  t_len numChildren;

  uint8_t flags : 2;
  uint8_t sortmode : 2;

  // the node's score. Non termn
  float score;

  // the maximal score of any descendant of this node, used to optimize
  // traversal
  float maxChildScore;

  // the payload of terminal node. could be NULL if it's not terminal
  TriePayload *payload;

  // the string of the current node
  rune str[];
  // ... now come the children, to be accessed with children()

  TrieNode(rune *str_, t_len offset, t_len len_, const char *payload, size_t plen,
           t_len numChildren_, float score_, bool terminal);
  ~TrieNode();

  void Print(int idx, int depth); //@@ looks like nobody is uding it
  bool Add(rune *str_, t_len len_, RSPayload *payload, float score_, TrieAddOp op);

  TrieNode *AddChild(rune *str_, t_len offset, t_len len_, RSPayload *payload, float score_);
  TrieNode *SplitNode(t_len offset);

  float Find(rune *str_, t_len len_);

  int Delete(rune *str_, t_len len_);

  TrieIterator *Iterate(StepFilter f, StackPopCallback pf, void *ctx_);

  TrieNode *RandomWalk(int minSteps, rune **str, t_len *len);
  TrieNode *MergeWithSingleChild();

  void sortChildren();
  void optimizeChildren();

  void IterateRange(const rune *min, int minlen, bool includeMin, const rune *max,
                    int maxlen, bool includeMax, TrieRangeCallback callback, void *ctx);
  static void rangeIterate(const rune *min, int nmin, const rune *max, int nmax, RangeCtx *r);
  static void rangeIterateSubTree(RangeCtx *r);

  static int Cmp(const void *p1, const void *p2);

  /* Get a pointer to the children array of a node. This is not an actual member
  * of the node for
  * memory saving reasons */
  TrieNode **getChildren() { return ((void *)this + sizeof(TrieNode) + (len + 1) * sizeof(rune)); }

  bool isTerminal() { return flags & TRIENODE_TERMINAL; }

  bool isDeleted() { return flags & TRIENODE_DELETED; }
};
#pragma pack()


/* The byte size of a node, based on its internal string length and number of
 * children */
size_t __trieNode_Sizeof(t_len numChildren, t_len slen);

/* trie iterator stack node. for internal use only */
struct stackNode {
  int state;
  TrieNode *n;
  t_len stringOffset;
  t_len childOffset;
  int isSkipped;
};

enum FilterCode { F_CONTINUE = 0, F_STOP = 1 };

// A callback for an automaton that receives the current state, evaluates the
// next byte,
// and returns the next state of the automaton. If we should not continue down,
// return F_STOP
typedef FilterCode (*StepFilter)(rune b, void *ctx, int *match, void *matchCtx);

typedef void (*StackPopCallback)(void *ctx, int num);

#define ITERSTATE_SELF 0
#define ITERSTATE_CHILDREN 1
#define ITERSTATE_MATCH 2

/* Opaque trie iterator type */
// typedef struct TrieIterator TrieIterator;
struct TrieIterator : public Object {
  rune buf[TRIE_INITIAL_STRING_LEN + 1];
  t_len bufOffset;

  stackNode stack[TRIE_INITIAL_STRING_LEN + 1];
  t_len stackOffset;
  StepFilter filter;
  float minScore;
  int nodesConsumed;
  int nodesSkipped;
  StackPopCallback popCallback;
  void *ctx;

  ~TrieIterator();

  void Push(TrieNode *node, int skipped);
  void Pop();

  /* the current top of the iterator stack */
  #define current() &stack[stackOffset - 1]

  int step(void *matchCtx);
  bool Next(rune **ptr, t_len *len, RSPayload *payload, float *score, void *matchCtx);
};

/* Stop the iteration */
#define __STEP_STOP 0
/* Continue to next node  */
#define __STEP_CONT 1
/* We found a match, return the state to the user but continue afterwards */
#define __STEP_MATCH 3
