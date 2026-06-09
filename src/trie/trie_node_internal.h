/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/* Internal header for the trie node module.
 *
 * Exposes the full layout of TrieNode, TriePayload, TrieIterator, plus the
 * internal __newTrieNode / __ti_* helpers and the in-band flag bits. Intended
 * for inclusion by trie.c, trie_node.c, and whitebox unit tests under
 * tests/ctests/. Other translation units must include "trie_node.h" instead and
 * go through the public accessors. */

#ifndef __TRIE_NODE_INTERNAL_H__
#define __TRIE_NODE_INTERNAL_H__

#include "trie_node.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TRIENODE_TERMINAL 0x1
#define TRIENODE_DELETED 0x2

#pragma pack(1)
struct TriePayload {
  uint32_t len;  // 4G payload is more than enough!!!!
  char data[];   // this means the data will not take an extra pointer.
};
#pragma pack()

#pragma pack(1)
/* TrieNode represents a single node in a trie. The actual size of it is bigger,
 * as the children are allocated after str[].
 * Terminal nodes (real string ends) are identified by the TRIENODE_TERMINAL flag;
 * tombstoned terminals carry TRIENODE_DELETED.
 */
struct TrieNode {
  // the string length of this node. can be 0
  t_len len;
  // the number of child nodes
  t_len numChildren;

  uint8_t flags : 2;
  TrieSortMode sortMode : 1;

  // the node's score. Non termn
  float score;

  // max(self.score, max descendant subtreeMaxScore); upper-bound used to
  // prune branch-and-bound traversal in Trie_Search top-k
  float subtreeMaxScore;

  // the number of documents containing this key
  size_t numDocs;

  // the payload of terminal node. could be NULL if it's not terminal
  TriePayload *payload;

  // the string of the current node
  rune str[];
  // ... here come the first letters of each child childRunes[]
  // ... now come the children, to be accessed with TrieNode_Children
};
#pragma pack()

/* Create a new trie node. str is a string to be copied into the node, starting
 * from offset up until
 * len. numChildren is the initial number of allocated child nodes */
TrieNode *__newTrieNode(const rune *str, t_len offset, t_len len, const char *payload, size_t plen,
                        t_len numChildren, float score, int terminal, TrieSortMode sortMode,
                        size_t numDocs);

/* trie iterator stack node. for internal use only */
typedef struct {
  int state;
  TrieNode *n;
  t_len stringOffset;
  t_len childOffset;
  int isSkipped;
} stackNode;

#define ITERSTATE_SELF 0
#define ITERSTATE_CHILDREN 1
#define ITERSTATE_MATCH 2

struct TrieIterator {
  rune buf[TRIE_INITIAL_STRING_LEN + 1];
  t_len bufOffset;

  stackNode stack[TRIE_INITIAL_STRING_LEN + 1];
  t_len stackOffset;
  StepFilter filter;
  // kth-best score currently in the top-k heap; rises monotonically as the
  // heap fills with better candidates. Distinct from the scorer-layer
  // minScore filter floor (see QueryProcessingCtx::minScore).
  float kthBestScore;
  int nodesConsumed;
  int nodesSkipped;
  StackPopCallback popCallback;
  void *ctx;
};

/* push a new trie iterator stack node  */
void __ti_Push(TrieIterator *it, TrieNode *node, int skipped);

/* the current top of the iterator stack */
#define __ti_current(it) &it->stack[it->stackOffset - 1]

/* pop a node from the iterator's stcak */
void __ti_Pop(TrieIterator *it);

/* Step itearator return codes below: */

/* Stop the iteration */
#define __STEP_STOP 0
/* Continue to next node  */
#define __STEP_CONT 1
/* We found a match, return the state to the user but continue afterwards */
#define __STEP_MATCH 3

/* Single step iteration, feeding the given filter/automaton with the next
 * character */
int __ti_step(TrieIterator *it, void *matchCtx);

#ifdef __cplusplus
}
#endif
#endif
