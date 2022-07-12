#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef uint16_t tm_len_t;

#define TM_NODE_DELETED 0x01
#define TM_NODE_TERMINAL 0x02
#define TM_NODE_SORTED 0x04
#define TRIE_INITIAL_STRING_LEN 255

// This special pointer is returned when TrieMap::Find cannot find anything */
extern void *TRIEMAP_NOTFOUND;
typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);
typedef void(TrieMapRangeCallback)(const char *, size_t, void *, void *);

struct TrieMapRangeCtx {
  char *buf;
  TrieMapRangeCallback *callback;
  void *cbctx;
  bool includeMin;
  bool includeMax;
};

struct TrieMaprsbHelper {
  const char *r;
  int n;
};

#pragma pack(1)

/* TrieMapNode represents a single node in a trie. The actual size of it is
 * bigger, as the children are allocated after str[].
 * The value pointer is optional, and NULL can be used if you are just
 * interested in the triemap as a set for strings
 */
//@@ Make Template
struct TrieMapNode : public Object {
  // the string length of this node. can be 0
  tm_len_t len;
  // the number of child nodes
  tm_len_t numChildren : 9;

  uint8_t flags : 7;

  void *value;

  // the string of the current node
  char str[];

  TrieMapNode(char *str_, tm_len_t offset, tm_len_t len_, tm_len_t numChildren,
              void *value_, bool terminal);

  bool Add(char *str_, tm_len_t len_, void *value_, TrieMapReplaceFunc cb);
  void *Find(char *str_, tm_len_t len_);
  int Delete(char *str_, tm_len_t len_, void (*freeCB)(void *));
  TrieMapNode *RandomWalk(int minSteps, char **str_, tm_len_t *len_);
  TrieMapNode *FindNode(char *str_, tm_len_t len_, tm_len_t *poffset);
  size_t MemUsage();

  void RangeIterate(const char *min, int nmin, const char *max, int nmax,
                    TrieMapRangeCtx *r);

  // Get a pointer to the children array of a node. This is not an actual member
  // of the node for memory saving reasons
  TrieMapNode **children() {
    return ((void *)this + sizeof(TrieMapNode) + (len + 1) + numChildren);
  }

  char *childKey(tm_len_t c) {
    return (char *)this + sizeof(TrieMapNode) + len + 1 + c;
  }

  bool isTerminal() { return flags & TM_NODE_TERMINAL; }
  bool isDeleted() { return flags & TM_NODE_DELETED;}

  TrieMapNode *MergeWithSingleChild();
  void AddChild(char *str_, tm_len_t offset, tm_len_t len_, void *value_);
  void Split(tm_len_t offset);
  void sortChildren();
  void resizeChildren(int offset);
  void optimizeChildren(void (*freeCB)(void *));
  void rangeIterateSubTree(TrieMapRangeCtx *r);

  static size_t Sizeof(tm_len_t numChildren, tm_len_t slen);
  static int CompareCommon(const void *h, const void *e, bool prefix);
  static int CompareExact(const void *h, const void *e);
  static int ComparePrefix(const void *h, const void *e);

  static int Cmp(const void *p1, const void *p2);
};

/**************  Iterator API  - not ported from the textual trie yet
 * ***********/
/* trie iterator stack node. for internal use only */
struct __tmi_stackNode {
  int state;
  TrieMapNode *n;
  tm_len_t stringOffset;
  tm_len_t childOffset;
};

#pragma pack()

struct TrieMapIterator : public Object {
  char *buf;
  tm_len_t bufLen;
  tm_len_t bufOffset;

  __tmi_stackNode *stack;
  tm_len_t stackOffset;
  tm_len_t stackCap;

  const char *prefix;
  tm_len_t prefixLen;
  int inSuffix;

  void Push(TrieMapNode *node);
  void Pop();
  void Free();
  bool Next(char **ptr, tm_len_t *len, void **value);
  __tmi_stackNode *current() { return &stack[stackOffset - 1]; } // the current top of the iterator stack
};

struct TrieMap : public Object {
  TrieMapNode *root;
  size_t cardinality;

  TrieMap();
  ~TrieMap();

  bool Add(char *str_, tm_len_t len_, void *value, TrieMapReplaceFunc cb);
  void *Find(char *str_, tm_len_t len_);
  int Delete(char *str_, tm_len_t len_, void (*freeCB)(void *));
  void Free(void (*freeCB)(void *));
  int RandomKey(char **str, tm_len_t *len_, void **ptr);
  void *RandomValueByPrefix(const char *prefix, tm_len_t pflen);
  size_t MemUsage();

  void IterateRange(const char *min, int minlen, bool includeMin,
                    const char *max, int maxlen, bool includeMax,
                    TrieMapRangeCallback callback, void *ctx);
  TrieMapIterator *Iterate(const char *prefix, tm_len_t prefixLen);
};
