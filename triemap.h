#ifndef __TRIEMAP_H__
#define __TRIEMAP_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef u_int16_t tm_len_t;

#define TM_NODE_DELETED 0x01
#define TM_NODE_TERMINAL 0x02
#define TM_NODE_SORTED 0x04

/* This special pointer is returned when TrieMap_Find cannot find anything */
extern void *TRIEMAP_NOTFOUND;

#pragma pack(1)

/* TrieMapNode represents a single node in a trie. The actual size of it is
 * bigger, as the children are allocated after str[].
 * Non terminal nodes always have a score of 0, meaning you can't insert nodes
 * with score 0 to the trie. */
typedef struct {
  // the string length of this node. can be 0
  tm_len_t len;
  // the number of child nodes
  tm_len_t numChildren : 9;

  u_char flags : 7;

  void *value;

  // the string of the current node
  char str[];
  // ... here come the first letters of each child childChars[]
  // ... now come the children, to be accessed with __trieMapNode_children
} TrieMapNode;
#pragma pack()

typedef struct {
  TrieMapNode *root;
  size_t cardinality;
} TrieMap;

TrieMap *NewTrieMap();

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);
/* Add a new string to a trie. Returns 1 if the string did not exist there, or 0
 * if we just replaced
 * the score. We pass a pointer to the node because it may actually change when
 * splitting */
int TrieMap_Add(TrieMap *t, char *str, tm_len_t len, void *value,
                TrieMapReplaceFunc cb);

/* Find the entry with a given string and length, and return its score. Returns
* 0 if the entry was
* not found.
* Note that you cannot put entries with zero score */
void *TrieMap_Find(TrieMap *t, char *str, tm_len_t len);

/* Mark a node as deleted. For simplicity for now we don't actually delete
* anything,
* but the node will not be persisted to disk, thus deleted after reload.
* Returns 1 if the node was indeed deleted, 0 otherwise */
int TrieMap_Delete(TrieMap *t, char *str, tm_len_t len, void (*freeCB)(void *));

/* Free the trie's root and all its children recursively. If freeCB is given, we
 * call it to free
 * individual payload values. If not, free() is used instead. */
void TrieMap_Free(TrieMap *t, void (*freeCB)(void *));

size_t TrieMap_MemUsage(TrieMap *t);

/**************  Iterator API  - not ported from the textual trie yet
 * ***********/
/* trie iterator stack node. for internal use only */
typedef struct {
  int state;
  TrieMapNode *n;
  tm_len_t stringOffset;
  tm_len_t childOffset;
} __tmi_stackNode;

typedef struct {
  char *buf;
  tm_len_t bufLen;
  tm_len_t bufOffset;

  __tmi_stackNode *stack;
  tm_len_t stackOffset;
  tm_len_t stackCap;

  const char *prefix;
  tm_len_t prefixLen;
  int inSuffix;
} TrieMapIterator;

void __tmi_Push(TrieMapIterator *it, TrieMapNode *node);
void __tmi_Pop(TrieMapIterator *it);

/* Iterate the tree with a step filter, which tells the iterator whether to
 * continue down the trie
 * or not. This can be a levenshtein automaton, a regex automaton, etc. A NULL
 * filter means just
 * continue iterating the entire trie. ctx is the filter's context */
TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix,
                                 tm_len_t prefixLen);

/* Free a trie iterator */
void TrieMapIterator_Free(TrieMapIterator *it);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done
 * and should exit */
int TrieMapIterator_Next(TrieMapIterator *it, char **ptr, tm_len_t *len,
                         void **value);

#endif
