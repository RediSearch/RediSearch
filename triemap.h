#ifndef __TRIEMAP_H__
#define __TRIEMAP_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef u_int32_t tm_len_t;

#define TM_MAX_STRING_LEN 0xFFFF

#define TM_NODE_DELETED 0x01
#define TM_NODE_TERMINAL 0x02

#pragma pack(1)
/* TrieMapNode represents a single node in a trie. The actual size of it is bigger,
 * as the children are
 * allocated after str[].
 * Non terminal nodes always have a score of 0, meaning you can't insert nodes
 * with score 0 to the
 * trie.
 */
typedef struct {
  // the string length of this node. can be 0
  tm_len_t len;
  // the number of child nodes
  u_int8_t numChildren;

  unsigned char flags;

  void *value;

  // the string of the current node
  unsigned char str[];
  // ... now come the children, to be accessed with __trieMapNode_children
} TrieMapNode;
#pragma pack()

typedef TrieMapNode TrieMap;
TrieMap *NewTrieMap();

/* Print a trie node recursively. printval is a callback that prints the non-null values */
void TrieMapNode_Print(TrieMapNode *n, int idx, int depth, void (*printval)(void *));

/* The byte size of a node, based on its internal string length and number of
 * children */
size_t __trieMapNode_Sizeof(tm_len_t numChildren, tm_len_t slen);

/* Create a new trie node. str is a string to be copied into the node, starting
 * from offset up until
 * len. numChildren is the initial number of allocated child nodes */
TrieMapNode *__newTrieMapNode(unsigned char *str, tm_len_t offset, tm_len_t len,
                              tm_len_t numChildren, void *value, int terminal);

/* Get a pointer to the children array of a node. This is not an actual member
 * of the node for
 * memory saving reasons */
#define __trieMapNode_children(n) ((TrieMapNode **)((void *)n + sizeof(TrieMapNode) + (n->len + 1)))

#define __trieMapNode_isTerminal(n) (n->flags & TM_NODE_TERMINAL)

#define __trieMapNode_isDeleted(n) (n->flags & TM_NODE_DELETED)

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);
/* Add a new string to a trie. Returns 1 if the string did not exist there, or 0
 * if we just replaced
 * the score. We pass a pointer to the node because it may actually change when
 * splitting */
int TrieMapNode_Add(TrieMapNode **n, unsigned char *str, tm_len_t len, void *value,
                    TrieMapReplaceFunc cb);

/* Find the entry with a given string and length, and return its score. Returns
* 0 if the entry was
* not found.
* Note that you cannot put entries with zero score */
void *TrieMapNode_Find(TrieMapNode *n, unsigned char *str, tm_len_t len);

/* Mark a node as deleted. For simplicity for now we don't actually delete
* anything,
* but the node will not be persisted to disk, thus deleted after reload.
* Returns 1 if the node was indeed deleted, 0 otherwise */
int TrieMapNode_Delete(TrieMapNode *n, unsigned char *str, tm_len_t len, void (*freeCB)(void *));

/* Free the trie's root and all its children recursively. If freeCB is given, we call it to free
 * individual payload values. If not, free() is used instead. */
void TrieMapNode_Free(TrieMapNode *n, void (*freeCB)(void *));

size_t TrieMapNode_MemUsage(TrieMapNode *n);

/**************  Iterator API  - not ported from the textual trie yet ***********/
/* trie iterator stack node. for internal use only */
typedef struct {
  int state;
  TrieMapNode *n;
  tm_len_t stringOffset;
  tm_len_t childOffset;
  int isSkipped;
} __tmi_stackNode;


typedef struct {
  unsigned char buf[TM_MAX_STRING_LEN];
  tm_len_t bufOffset;
  __tmi_stackNode stack[TM_MAX_STRING_LEN];
  tm_len_t stackOffset;
  const char *prefix;
  tm_len_t prefixLen;
  int inSuffix;
} TrieMapIterator;


/* Iterate the tree with a step filter, which tells the iterator whether to
 * continue down the trie
 * or not. This can be a levenshtein automaton, a regex automaton, etc. A NULL
 * filter means just
 * continue iterating the entire trie. ctx is the filter's context */
TrieMapIterator *TrieMapNode_Iterate(TrieMapNode *n,  const char *prefix, tm_len_t prefixLen);

/* Free a trie iterator */
void TrieMapIterator_Free(TrieMapIterator *it);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done
 * and should exit */
int TrieMapIterator_Next(TrieMapIterator *it, unsigned char **ptr, tm_len_t *len, void **value);

#endif
