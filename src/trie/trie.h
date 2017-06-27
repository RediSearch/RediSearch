#ifndef __TRIE_H__
#define __TRIE_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "rune_util.h"

typedef uint16_t t_len;

#define MAX_STRING_LEN 255

#define TRIENODE_SORTED 0x1
#define TRIENODE_TERMINAL 0x2
#define TRIENODE_DELETED 0x4

#pragma pack(1)
/* TrieNode represents a single node in a trie. The actual size of it is bigger,
 * as the children are
 * allocated after str[].
 * Non terminal nodes always have a score of 0, meaning you can't insert nodes
 * with score 0 to the
 * trie.
 */
typedef struct {
  // the string length of this node. can be 0
  t_len len;
  // the number of child nodes
  t_len numChildren;

  unsigned char flags;

  // the node's score. Non termn
  float score;

  // the maximal score of any descendant of this node, used to optimize
  // traversal
  float maxChildScore;

  // the payload of terminal node. could be NULL if it's not terminal
  char *payload;
  // the length of payload
  size_t payloadSize;

  // the string of the current node
  rune str[];
  // ... now come the children, to be accessed with __trieNode_children
} TrieNode;
#pragma pack()

void TrieNode_Print(TrieNode *n, int idx, int depth);

/* The byte size of a node, based on its internal string length and number of
 * children */
size_t __trieNode_Sizeof(t_len numChildren, t_len slen);

/* Create a new trie node. str is a string to be copied into the node, starting
 * from offset up until
 * len. numChildren is the initial number of allocated child nodes */
TrieNode *__newTrieNode(rune *str, t_len offset, t_len len, const char *payload, size_t plen, t_len numChildren, float score,
                        int terminal);

/* Get a pointer to the children array of a node. This is not an actual member
 * of the node for
 * memory saving reasons */
#define __trieNode_children(n) \
  ((TrieNode **)((void *)n + sizeof(TrieNode) + (n->len + 1) * sizeof(rune)))

#define __trieNode_isTerminal(n) (n->flags & TRIENODE_TERMINAL)

#define __trieNode_isDeleted(n) (n->flags & TRIENODE_DELETED)

/* Add a child node to the parent node n, with a string str starting at offset
up until len, and a
given score */
TrieNode *__trie_AddChild(TrieNode *n, rune *str, t_len offset, t_len len, const char *payload, size_t plen, float score);

/* Split node n at string offset n. This returns a new node which has a string
* up until offset, and
* a single child holding The old score of n, and its score */
TrieNode *__trie_SplitNode(TrieNode *n, t_len offset);

typedef enum {
  ADD_REPLACE,
  ADD_INCR,
} TrieAddOp;
/* Add a new string to a trie. Returns 1 if the string did not exist there, or 0
 * if we just replaced
 * the score. We pass a pointer to the node because it may actually change when
 * splitting */
int TrieNode_Add(TrieNode **n, rune *str, t_len len, const char *payload, size_t plen, float score, TrieAddOp op);

/* Find the entry with a given string and length, and return its score. Returns
* 0 if the entry was
* not found.
* Note that you cannot put entries with zero score */
float TrieNode_Find(TrieNode *n, rune *str, t_len len);

/* Mark a node as deleted. For simplicity for now we don't actually delete
* anything,
* but the node will not be persisted to disk, thus deleted after reload.
* Returns 1 if the node was indeed deleted, 0 otherwise */
int TrieNode_Delete(TrieNode *n, rune *str, t_len len);

/* Free the trie's root and all its children recursively */
void TrieNode_Free(TrieNode *n);

/* trie iterator stack node. for internal use only */
typedef struct {
  int state;
  TrieNode *n;
  t_len stringOffset;
  t_len childOffset;
  int isSkipped;
} stackNode;

typedef enum { F_CONTINUE = 0, F_STOP = 1 } FilterCode;

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
typedef struct TrieIterator {
  rune buf[MAX_STRING_LEN+1];
  t_len bufOffset;

  stackNode stack[MAX_STRING_LEN+1];
  t_len stackOffset;
  StepFilter filter;
  float minScore;
  int nodesConsumed;
  int nodesSkipped;
  StackPopCallback popCallback;
  void *ctx;
} TrieIterator;

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

/* Iterate the tree with a step filter, which tells the iterator whether to
 * continue down the trie
 * or not. This can be a levenshtein automaton, a regex automaton, etc. A NULL
 * filter means just
 * continue iterating the entire trie. ctx is the filter's context */
TrieIterator *TrieNode_Iterate(TrieNode *n, StepFilter f, StackPopCallback pf, void *ctx);

/* Free a trie iterator */
void TrieIterator_Free(TrieIterator *it);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done
 * and should exit */
int TrieIterator_Next(TrieIterator *it, rune **ptr, t_len *len, char **payload, size_t *plen, float *score, void *matchCtx);

#endif
