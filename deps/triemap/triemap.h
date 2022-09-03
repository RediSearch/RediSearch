#pragma once

#include "rmutil/vector.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <functional>
#include <string_view>

///////////////////////////////////////////////////////////////////////////////////////////////

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

//---------------------------------------------------------------------------------------------

struct TrieMapRange {
  char *buf;
  TrieMapRangeCallback callback;
  void *cbctx;
  bool includeMin;
  bool includeMax;
};

//---------------------------------------------------------------------------------------------

struct TrieMaprsbHelper {
  const char *r;
  int n;
};

//---------------------------------------------------------------------------------------------

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
  std::string str; // the string of the current node
  Vector<TrieMapNode*> _children;
  Vector<char> _children_keys;

  TrieMapNode(std::string_view str, tm_len_t offset, void *value, bool terminal);

  bool Add(std::string_view str, void *value, TrieMapReplaceFunc cb);
  void *Find(std::string_view str);
  bool Delete(std::string_view str);
  TrieMapNode *RandomWalk(int minSteps, std::string &newstr);
  TrieMapNode *FindNode(std::string_view str, tm_len_t *poffset);
  size_t MemUsage() const;

  void RangeIterate(const char *min, int nmin, const char *max, int nmax, TrieMapRange *r);

  char childKey(tm_len_t i) const { return _children_keys[i]; }
  char &childKey(tm_len_t i) { return _children_keys[i]; }

  bool isTerminal() const { return !!(flags & TM_NODE_TERMINAL); }
  bool isDeleted() const { return !!(flags & TM_NODE_DELETED); }

  TrieMapNode *MergeWithSingleChild();
  void AddChild(std::string_view str, tm_len_t offset, void *value);
  void Split(tm_len_t offset);
  void sortChildren();
  void optimizeChildren();
  void rangeIterateSubTree(TrieMapRange *r);

  size_t Sizeof();
  static int CompareCommon(const void *h, const void *e, bool isPrefix);
  static int CompareExact(const void *h, const void *e);
  static int ComparePrefix(const void *h, const void *e);

  static int Cmp(const void *p1, const void *p2);
};

//---------------------------------------------------------------------------------------------

struct stackNode {
  int state;
  TrieMapNode *n;
  tm_len_t stringOffset;
  tm_len_t childOffset;

  stackNode(TrieMapNode *node) : state(TM_ITERSTATE_SELF), n(node), stringOffset(0), childOffset(0) {}
};

#pragma pack()

//---------------------------------------------------------------------------------------------

struct TrieMapIterator : public Object {
  char *buf;
  tm_len_t bufLen;
  tm_len_t bufOffset;

  Vector<stackNode> stack;

  String prefix;
  int inSuffix;

  TrieMapIterator(TrieMapNode *node, std::string_view prefix) :
      bufLen(16), bufOffset(0), prefix(prefix), inSuffix(0) {
    Push(node);
  }

  void Push(TrieMapNode *node);
  void Pop();

  bool Next(char **ptr, tm_len_t *len, void **value);
  stackNode current() { return stack.back(); } // the current top of the iterator stack
};

//---------------------------------------------------------------------------------------------

struct TrieMap : public Object {
  TrieMapNode *root;
  size_t cardinality;

  TrieMap();
  ~TrieMap();

  bool Add(std::string_view str, void *value, TrieMapReplaceFunc cb);
  void *Find(std::string_view str);
  int Delete(std::string_view str);
  void Free(void (*freeCB)(void *));
  bool RandomKey(std::string &str, void **ptr);
  void *RandomValueByPrefix(std::string_view prefix);
  size_t MemUsage() const;

  void IterateRange(const char *min, int minlen, bool includeMin, const char *max, int maxlen,
                    bool includeMax, TrieMapRangeCallback callback, void *ctx);
  TrieMapIterator *Iterate(std::string_view prefix);
};

///////////////////////////////////////////////////////////////////////////////////////////////
