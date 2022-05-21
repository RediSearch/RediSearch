#ifndef __TRIEMAP_H__
#define __TRIEMAP_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef uint16_t tm_len_t;

#define TM_NODE_DELETED 0x01
#define TM_NODE_TERMINAL 0x02
#define TM_NODE_SORTED 0x04

/* This special pointer is returned when TrieMap_Find cannot find anything */
extern void *TRIEMAP_NOTFOUND;

#pragma pack(1)

/* TrieMapNode represents a single node in a trie. The actual size of it is
 * bigger, as the children are allocated after str[].
 * The value pointer is optional, and NULL can be used if you are just
 * interested in the triemap as a set for strings
 */
typedef struct {
  // the string length of this node. can be 0
  tm_len_t len;
  // the number of child nodes
  tm_len_t numChildren : 9;

  uint8_t flags : 7;

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

/* Add a new string to a trie. Returns 1 if the key is new to the trie or 0 if
 * it already existed.
 *
 * If value is given, it is saved as a pyaload inside the trie node.
 * If the key already exists, we replace the old value with the new value, using
 * free() to free the old value.
 *
 * If cb is given, instead of replacing and freeing, we call the callback with
 * the old and new value, and the function should return the value to set in the
 * node, and take care of freeing any unwanted pointers. The returned value
 * can be NULL and doesn't have to be either the old or new value.
 */
int TrieMap_Add(TrieMap *t, char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb);

/* Find the entry with a given string and length, and return its value, even if
 * that was NULL.
 *
 * NOTE: If the key does not exist in the trie, we return the special
 * constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
 * comparing to it, becase NULL can be a valid result.
 */
void *TrieMap_Find(TrieMap *t, char *str, tm_len_t len);

/* Mark a node as deleted. It also optimizes the trie by merging nodes if
 * needed. If freeCB is given, it will be used to free the value of the deleted
 * node. If it doesn't, we simply call free() */
int TrieMap_Delete(TrieMap *t, char *str, tm_len_t len, void (*freeCB)(void *));

/* Free the trie's root and all its children recursively. If freeCB is given, we
 * call it to free individual payload values. If not, free() is used instead. */
void TrieMap_Free(TrieMap *t, void (*freeCB)(void *));

/* Get a random key from the trie by doing a random walk down and up the tree
 * for a minimum number of steps. Returns 0 if the tree is empty and we couldn't
 * find a random node.
 * Assign's the key to str and saves its len (the key is NOT null terminated).
 * NOTE: It is the caller's responsibility to free the key string
 */
int TrieMap_RandomKey(TrieMap *t, char **str, tm_len_t *len, void **ptr);

/* Get the value of a random element under a specific prefix. NULL if the prefix was not found */
void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen);

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

/* Iterate the trie for all the suffixes of a given prefix. This returns an
 * iterator object even if the prefix was not found, and subsequent calls to
 * TrieMapIterator_Next are needed to get the results from the iteration. If the
 * prefix is not found, the first call to next will return 0 */
TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix, tm_len_t prefixLen);

/* Free a trie iterator */
void TrieMapIterator_Free(TrieMapIterator *it);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done and should exit */
int TrieMapIterator_Next(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);

typedef void(TrieMapRangeCallback)(const char *, size_t, void *, void *);

void TrieMap_IterateRange(TrieMap *trie, const char *min, int minlen, bool includeMin,
                          const char *max, int maxlen, bool includeMax,
                          TrieMapRangeCallback callback, void *ctx);

#endif
