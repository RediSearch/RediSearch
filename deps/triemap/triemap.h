#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <functional>
#include <rmutil/vector.h>

typedef uint16_t tm_len_t;

#define TM_NODE_DELETED 0x01
#define TM_NODE_TERMINAL 0x02
#define TM_NODE_SORTED 0x04
#define TRIE_INITIAL_STRING_LEN 255

#define TM_ITERSTATE_SELF 0
#define TM_ITERSTATE_CHILDREN 1

// This special pointer is returned when TrieMap::Find cannot find anything
extern void *TRIEMAP_NOTFOUND;
typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);
//typedef void(TrieMapRangeCallback)(const char *, size_t, void *, void *);
typedef std::function<void(const char *min, size_t minlen, void *ctx, void *val)> TrieMapRangeCallback;

struct TrieMapRangeCtx {
  char *buf;
  TrieMapRangeCallback callback;
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
  uint8_t flags;
  void *value;
  std::string str;               // the string of the current node
  Vector<TrieMapNode*> _children;

  TrieMapNode(char *str_, tm_len_t offset, tm_len_t len_, tm_len_t numChildren,
              void *value_, bool terminal);

  bool Add(char *str_, tm_len_t len, void *value_, TrieMapReplaceFunc cb);
  void *Find(std::string str_, tm_len_t len);
  bool Delete(char *str_, tm_len_t len);
  TrieMapNode *RandomWalk(int minSteps, std::string &newstr);
  TrieMapNode *FindNode(char *str_, tm_len_t len_, tm_len_t *poffset);
  size_t MemUsage();

  void RangeIterate(const char *min, int nmin, const char *max, int nmax,
                    TrieMapRangeCtx *r);

  // Get a pointer to the children array of a node. This is not an actual member
  // of the node for memory saving reasons
  TrieMapNode **children() {
    return ((void *)this + sizeof(TrieMapNode) + (str.length() + 1) + _children.size());
  }

  //std::string childKey(tm_len_t c) { //@@ Is that the child or the str of the child?
  char *childKey(tm_len_t c) {
    return (char *)this + sizeof(TrieMapNode) + str.length() + 1 + c;
    //return _children[c]->str;
  }

  bool isTerminal() { return flags & TM_NODE_TERMINAL; }
  bool isDeleted() { return flags & TM_NODE_DELETED;}

  TrieMapNode *MergeWithSingleChild();
  void AddChild(char *str_, tm_len_t offset, tm_len_t len_, void *value_);
  void Split(tm_len_t offset);
  void sortChildren();
  void optimizeChildren();
  void rangeIterateSubTree(TrieMapRangeCtx *r);

  static size_t Sizeof(tm_len_t numChildren, tm_len_t slen);
  static int CompareCommon(const void *h, const void *e, bool prefix);
  static int CompareExact(const void *h, const void *e);
  static int ComparePrefix(const void *h, const void *e);

  static int Cmp(const void *p1, const void *p2);
};

// trie iterator stack node. for internal use only
struct stackNode {
  int state;
  TrieMapNode *n;
  tm_len_t stringOffset;
  tm_len_t childOffset;

  stackNode(TrieMapNode *node) :
    state(TM_ITERSTATE_SELF), n(node), stringOffset(0), childOffset(0) {}
};

#pragma pack()

struct TrieMapIterator : public Object {
  char *buf;
  tm_len_t bufLen;
  tm_len_t bufOffset;

  Vector<stackNode> stack;

  const char *prefix;
  tm_len_t prefixLen;
  int inSuffix;

  TrieMapIterator(TrieMapNode *node, const char *prefix, tm_len_t len) :
      bufLen(16), bufOffset(0), prefix(prefix), prefixLen(len), inSuffix(0) {
    Push(node);
  }

  void Push(TrieMapNode *node);
  void Pop();
  void Free();
  bool Next(char **ptr, tm_len_t *len, void **value);
  stackNode current() { return stack.back(); } // the current top of the iterator stack
};

struct TrieMap : public Object {
  TrieMapNode *root;
  size_t cardinality;

  TrieMap();
  ~TrieMap();

  bool Add(char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb);
  void *Find(char *str, tm_len_t len);
  int Delete(char *str, tm_len_t len);
  void Free(void (*freeCB)(void *));
  bool RandomKey(std::string str, tm_len_t *len_, void **ptr);
  void *RandomValueByPrefix(const char *prefix, tm_len_t pflen);
  size_t MemUsage();

  void IterateRange(const char *min, int minlen, bool includeMin,
                    const char *max, int maxlen, bool includeMax,
                    TrieMapRangeCallback callback, void *ctx);
  TrieMapIterator *Iterate(const char *prefix, tm_len_t prefixLen);
};
