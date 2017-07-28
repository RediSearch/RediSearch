#ifndef __TRIE_H__
#define __TRIE_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "rune_util.h"
#include "redisearch.h"

typedef uint16_t t_len;

#define MAX_STRING_LEN 255

#define TRIENODE_SORTED 0x1
#define TRIENODE_TERMINAL 0x2
#define TRIENODE_DELETED 0x4
#define TRIENODE_OPAQUE_PAYLOAD 0x8

#pragma pack(1)
typedef struct {
  uint32_t len;  // 4G payload is more than enough!!!!
  char data[];   // this means the data will not take an extra pointer.
} TriePayload;
#pragma pack()

/* The byte size of a TriePayload, based on its internal data length */
size_t __triePayload_Sizeof(uint32_t len);

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
  union {
    TriePayload *managed;
    void *opaque;
  } uPayload;

  // the string of the current node
  rune str[];
  // ... now come the children, to be accessed with TrieNode_Children
} TrieNode;
#pragma pack()

void TrieNode_Print(TrieNode *n, int idx, int depth);

/* The byte size of a node, based on its internal string length and number of
 * children */
static inline size_t TrieNode_SizeOf(t_len numChildren, t_len slen) {
  return sizeof(TrieNode) + numChildren * sizeof(TrieNode *) + sizeof(rune) * (slen + 1);
}

/* Create a new trie node. str is a string to be copied into the node, starting
 * from offset up until
 * len. numChildren is the initial number of allocated child nodes */
TrieNode *NewTrieNodeEx(rune *str, t_len offset, t_len len, t_len numChildren, float score,
                        int terminal);

#define NewTrieNode(rs) NewTrieNodeEx(rs, 0, 0, 0, 0, 0)

/* Get a pointer to the children array of a node. This is not an actual member
 * of the node for
 * memory saving reasons */
#define TrieNode_Children(n) \
  ((TrieNode **)((void *)n + sizeof(TrieNode) + (n->len + 1) * sizeof(rune)))

#define TrieNode_IsTerminal(n) (n->flags & TRIENODE_TERMINAL)

#define TrieNode_IsDeleted(n) (n->flags & TRIENODE_DELETED)

/* Add a child node to the parent node n, with a string str starting at offset
up until len, and a
given score */
TrieNode *TrieNode_AddChild(TrieNode *n, rune *str, t_len offset, t_len len, TrieNode **entry,
                            float score);

/* Split node n at string offset n. This returns a new node which has a string
* up until offset, and
* a single child holding The old score of n, and its score */
TrieNode *TrieNode_Split(TrieNode *n, t_len offset);

typedef enum {
  ADD_REPLACE,
  ADD_INCR,
} TrieAddOp;
/* Add a new string to a trie. Returns 1 if the string did not exist there, or 0
 * if we just replaced
 * the score. We pass a pointer to the node because it may actually change when
 * splitting */
int TrieNode_Add(TrieNode **n, rune *str, t_len len, TrieNode **entry, float score, TrieAddOp op);

/**
 * Set a payload for a given node. The value of `data` depends on the options:
 *
 * if options contains TN_PAYLOAD_COPY then data is an RSPayload pointer whose
 * contents will be copied to the node. Otherwise the pointer is simply stored.
 * Note that you can (should) pass the free function later on, when deleting
 * and/or freeing nodes.
 *
 * Setting the payload to NULL will free the old payload, if managed.
 *
 */
void TrieNode_SetPayload(TrieNode *n, void *data, int options);
#define TN_PAYLOAD_COPY 0x1

/* Find the entry with a given string and length, Returns
* 0 if the entry was
* not found.
* Note that you cannot put entries with zero score */
TrieNode *TrieNode_Find(TrieNode *n, rune *str, t_len len);

static inline float TrieNode_FindScore(TrieNode *n, rune *str, t_len len) {
  n = TrieNode_Find(n, str, len);
  return n ? n->score : 0;
}

typedef void (*TrieNode_CleanFunc)(void *);

/* Mark a node as deleted. For simplicity for now we don't actually delete
* anything,
* but the node will not be persisted to disk, thus deleted after reload.
* Returns 1 if the node was indeed deleted, 0 otherwise */
int TrieNode_Delete(TrieNode *n, rune *str, t_len len, TrieNode_CleanFunc cleaner);

/* Free the trie's root and all its children recursively */
void TrieNode_Free(TrieNode *n, TrieNode_CleanFunc cleaner);

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
  rune buf[MAX_STRING_LEN + 1];
  t_len bufOffset;

  stackNode stack[MAX_STRING_LEN + 1];
  t_len stackOffset;
  StepFilter filter;
  float minScore;
  int nodesConsumed;
  int nodesSkipped;
  StackPopCallback popCallback;
  void *ctx;
} TrieIterator;

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
int TrieIterator_Next(TrieIterator *it, TrieNode **np, rune **s, t_len *slen, void *matchCtx);

/**
 * This only populates `payload` if it's managed.
 */
int TrieIterator_NextCompat(TrieIterator *iter, rune **rstr, t_len *slen, RSPayload *payload,
                            float *score, void *matchCtx);

#endif
