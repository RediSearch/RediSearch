#include "triemap.h"
#include <math.h>
#include <sys/param.h>
#include <ctype.h>
#include "util/bsearch.h"
#include "util/arr.h"

void *TRIEMAP_NOTFOUND = "NOT FOUND";

void TrieMapNode_Free(TrieMapNode *n, void (*freeCB)(void *));

/* Get a pointer to the children array of a node. This is not an actual member
 * of the node for
 * memory saving reasons */
#define __trieMapNode_children(n) \
  ((TrieMapNode **)((void *)n + sizeof(TrieMapNode) + (n->len + 1) + n->numChildren))

#define __trieMapNode_childKey(n, c) (char *)((char *)n + sizeof(TrieMapNode) + n->len + 1 + c)

#define __trieMapNode_isTerminal(n) (n->flags & TM_NODE_TERMINAL)

#define __trieMapNode_isDeleted(n) (n->flags & TM_NODE_DELETED)

/* The byte size of a node, based on its internal string length and number of
 * children */
size_t __trieMapNode_Sizeof(tm_len_t numChildren, tm_len_t slen) {
  return (sizeof(TrieMapNode) + numChildren * sizeof(TrieMapNode *) + (slen + 1) + numChildren);
}

TrieMapNode *__trieMapNode_resizeChildren(TrieMapNode *n, int offset) {
  n = rm_realloc(n, __trieMapNode_Sizeof(n->numChildren + offset, n->len));
  TrieMapNode **children = __trieMapNode_children(n);

  // stretch or shrink the child key cache array
  memmove(((char *)children) + offset, (char *)children, sizeof(TrieMapNode *) * n->numChildren);
  n->numChildren += offset;
  return n;
}

/* Create a new trie node. str is a string to be copied into the node,
 * starting from offset up until len. numChildren is the initial number of
 * allocated child nodes */
TrieMapNode *__newTrieMapNode(char *str, tm_len_t offset, tm_len_t len, tm_len_t numChildren,
                              void *value, int terminal) {
  tm_len_t nlen = len - offset;
  TrieMapNode *n = rm_malloc(__trieMapNode_Sizeof(numChildren, nlen));
  n->len = nlen;
  n->numChildren = numChildren;
  n->value = value;
  n->flags = terminal ? TM_NODE_TERMINAL : 0;

  memcpy(n->str, str + offset, nlen);

  return n;
}

TrieMap *NewTrieMap() {
  TrieMap *tm = rm_malloc(sizeof(TrieMap));
  tm->cardinality = 0;
  tm->root = __newTrieMapNode((char *)"", 0, 0, 0, NULL, 0);
  return tm;
}

TrieMapNode *__trieMapNode_AddChild(TrieMapNode *n, char *str, tm_len_t offset, tm_len_t len,
                                    void *value) {
  // make room for another child
  n = __trieMapNode_resizeChildren(n, 1);

  // a newly added child must be a terminal node
  TrieMapNode *child = __newTrieMapNode(str, offset, len, 0, value, 1);
  *__trieMapNode_childKey(n, n->numChildren - 1) = str[offset];

  __trieMapNode_children(n)[n->numChildren - 1] = child;
  n->flags &= ~TM_NODE_SORTED;
  return n;
}

TrieMapNode *__trieMapNode_Split(TrieMapNode *n, tm_len_t offset) {
  // Copy the current node's data and children to a new child node
  TrieMapNode *newChild = __newTrieMapNode(n->str, offset, n->len, n->numChildren, n->value,
                                           __trieMapNode_isTerminal(n));
  newChild->flags = n->flags;

  TrieMapNode **children = __trieMapNode_children(n);
  TrieMapNode **newChildren = __trieMapNode_children(newChild);
  memcpy(newChildren, children, sizeof(TrieMapNode *) * n->numChildren);
  memcpy(__trieMapNode_childKey(newChild, 0), __trieMapNode_childKey(n, 0), n->numChildren);
  // reduce the node to be just one child long with no score
  n->numChildren = 1;
  n->len = offset;
  n->value = NULL;
  // the parent node is now non terminal and non sorted
  n->flags = 0;  //&= ~(TM_NODE_TERMINAL | TM_NODE_DELETED | TM_NODE_SORTED);

  n = rm_realloc(n, __trieMapNode_Sizeof(n->numChildren, n->len));
  __trieMapNode_children(n)[0] = newChild;
  *__trieMapNode_childKey(n, 0) = newChild->str[0];
  return n;
}

int TrieMapNode_Add(TrieMapNode **np, char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb) {
  TrieMapNode *n = *np;

  tm_len_t offset = 0;
  for (; offset < len && offset < n->len; offset++) {
    if (str[offset] != n->str[offset]) {
      break;
    }
  }
  // we broke off before the end of the string
  if (offset < n->len) {
    // split the node and create 2 child nodes:
    // 1. a child representing the new string from the diverted offset onwards
    // 2. a child representing the old node's suffix from the diverted offset
    // and the old children
    n = __trieMapNode_Split(n, offset);

    // the new string matches the split node exactly!
    // we simply turn the split node, which is now non terminal, into a
    // terminal node
    if (offset == len) {
      n->value = value;
      n->flags |= TM_NODE_TERMINAL;
    } else {
      // we add a child
      n = __trieMapNode_AddChild(n, str, offset, len, value);
    }
    *np = n;
    return 1;
  }

  // we're inserting in an existing node - just replace the value
  if (offset == len) {
    int term = __trieMapNode_isTerminal(n);
    int deleted = __trieMapNode_isDeleted(n);

    if (cb) {
      n->value = cb(n->value, value);
    } else {
      if (n->value) {
        rm_free(n->value);
      }
      n->value = value;
    }

    // set the node as terminal
    n->flags |= TM_NODE_TERMINAL;
    // if it was deleted, make sure it's not now
    n->flags &= ~TM_NODE_DELETED;
    n->flags &= ~TM_NODE_SORTED;
    *np = n;
    // if the node existed - we return 0, otherwise return 1 as it's a new
    // node
    return (term && !deleted) ? 0 : 1;
  }

  // proceed to the next child or add a new child for the current char
  for (tm_len_t i = 0; i < n->numChildren; i++) {
    TrieMapNode *child = __trieMapNode_children(n)[i];

    if (str[offset] == child->str[0]) {
      int rc = TrieMapNode_Add(&child, str + offset, len - offset, value, cb);
      __trieMapNode_children(n)[i] = child;
      //      *__trieMapNode_childKey(n, i) = child->str[0];

      return rc;
    }
  }

  *np = __trieMapNode_AddChild(n, str, offset, len, value);
  return 1;
}

int TrieMap_Add(TrieMap *t, char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb) {
  int rc = TrieMapNode_Add(&t->root, str, len, value, cb);
  t->cardinality += rc;
  return rc;
}

// comparator for node sorting by child max score
static int __cmp_nodes(const void *p1, const void *p2) {
  return (*(TrieMapNode **)p1)->str[0] - (*(TrieMapNode **)p2)->str[0];
}

static int __cmp_chars(const void *p1, const void *p2) {
  return *(char *)p1 - *(char *)p2;
}

/* Sort the children of a node by their first letter to allow binary search */
static inline void __trieNode_sortChildren(TrieMapNode *n) {
  if ((0 == (n->flags & TM_NODE_SORTED)) && n->numChildren > 3) {
    qsort(__trieMapNode_children(n), n->numChildren, sizeof(TrieMapNode *), __cmp_nodes);
    qsort(__trieMapNode_childKey(n, 0), n->numChildren, 1, __cmp_chars);
    n->flags |= TM_NODE_SORTED;
  }
}

void *TrieMapNode_Find(TrieMapNode *n, char *str, tm_len_t len) {
  tm_len_t offset = 0;
  while (n && (offset < len || len == 0)) {
    tm_len_t localOffset = 0;
    tm_len_t nlen = n->len;
    while (offset < len && localOffset < nlen) {
      if (str[offset] != n->str[localOffset]) {
        break;
      }
      offset++;
      localOffset++;
    }

    // we've reached the end of the node's string
    if (localOffset == nlen) {
      // we're at the end of both strings!
      if (offset == len) {
        // If this is a terminal, non deleted node
        if (__trieMapNode_isTerminal(n) && !__trieMapNode_isDeleted(n)) {
          return n->value;
        } else {
          return TRIEMAP_NOTFOUND;
        }
      }
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;
      // if (!(n->flags & TM_NODE_SORTED) && n->numChildren > 2) {
      //   qsort(__trieMapNode_children(n), n->numChildren, sizeof(TrieMapNode *), __cmp_nodes);
      //   qsort(__trieMapNode_childKey(n, 0), n->numChildren, 1, __cmp_chars);
      //   n->flags |= TM_NODE_SORTED;
      // } 
      char *childKeys = __trieMapNode_childKey(n, 0);
      char c = str[offset];
      // if (n->flags & TM_NODE_SORTED) {
      //   int bottom = 0, top = n->numChildren - 1;

      //   while (bottom <= top) {
      //     int mid = (bottom + top) / 2;

      //     char cc = *__trieMapNode_childKey(n, mid);
      //     if (c == cc) {
      //       nextChild = __trieMapNode_children(n)[mid];
      //       break;
      //     } else if (c < cc) {
      //       top = mid - 1;
      //     } else {
      //       bottom = mid + 1;
      //     }
      //   }

      // } else {      
      tm_len_t nc = n->numChildren;

      while (i < nc) {
        if (str[offset] == childKeys[i]) {
          nextChild = __trieMapNode_children(n)[i];
          break;
        }
        ++i;
      }
      n = nextChild;
    } else {
      return TRIEMAP_NOTFOUND;
    }
  }

  return TRIEMAP_NOTFOUND;
}

/* Find a node by string. Return the node matching the string even if it is not
 * terminal. Puts the node local offset in *offset */
TrieMapNode *TrieMapNode_FindNode(TrieMapNode *n, char *str, tm_len_t len, tm_len_t *poffset) {
  tm_len_t offset = 0;
  while (n && (offset < len || len == 0)) {
    tm_len_t localOffset = 0;
    tm_len_t nlen = n->len;
    while (offset < len && localOffset < nlen) {
      if (str[offset] != n->str[localOffset]) {
        break;
      }
      offset++;
      localOffset++;
    }

    // we've reached the end of the string - return the node even if it's not
    // temrinal
    if (offset == len) {
      // let the caller know the local offset
      if (poffset) {
        *poffset = localOffset;
      }
      return n;
    }

    // we've reached the end of the node's string
    if (localOffset == nlen) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;

      char *childKeys = __trieMapNode_childKey(n, 0);
      char c = str[offset];

      while (i < n->numChildren) {
        if (str[offset] == childKeys[i]) {
          nextChild = __trieMapNode_children(n)[i];
          break;
        }
        ++i;
      }

      // we couldn't find a matching child
      n = nextChild;
    } else {
      return NULL;
    }
  }

  return NULL;
}

void *TrieMap_Find(TrieMap *t, char *str, tm_len_t len) {
  return TrieMapNode_Find(t->root, str, len);
}

/* If a node has a single child after delete, we can merged them. This
 * deletes
 * the node and returns a newly allocated node */
TrieMapNode *__trieMapNode_MergeWithSingleChild(TrieMapNode *n) {
  // we do not merge terminal nodes
  if (__trieMapNode_isTerminal(n) || n->numChildren != 1) {
    return n;
  }

  TrieMapNode *ch = *__trieMapNode_children(n);

  // Copy the current node's data and children to a new child node
  char nstr[n->len + ch->len + 1];
  memcpy(nstr, n->str, sizeof(char) * n->len);
  memcpy(&nstr[n->len], ch->str, sizeof(char) * ch->len);
  TrieMapNode *merged = __newTrieMapNode(nstr, 0, n->len + ch->len, ch->numChildren, ch->value,
                                         __trieMapNode_isTerminal(ch));

  merged->numChildren = ch->numChildren;
  merged->flags = ch->flags;

  memcpy(__trieMapNode_children(merged), __trieMapNode_children(ch),
         sizeof(TrieMapNode *) * merged->numChildren);
  memcpy(__trieMapNode_childKey(merged, 0), __trieMapNode_childKey(ch, 0), merged->numChildren);
  rm_free(n);
  rm_free(ch);

  return merged;
}

/* Optimize the node and its children:
 *   1. If a child should be deleted - delete it and reduce the child count
 *   2. If a child has a single child - merge them
 */
void __trieMapNode_optimizeChildren(TrieMapNode *n, void (*freeCB)(void *)) {
  int i = 0;
  TrieMapNode **nodes = __trieMapNode_children(n);
  // free deleted terminal nodes
  while (i < n->numChildren) {
    // if this is a deleted node with no children - remove it
    if (nodes[i]->numChildren == 0 && __trieMapNode_isDeleted(nodes[i])) {
      TrieMapNode_Free(nodes[i], freeCB);

      nodes[i] = NULL;
      char *nk = __trieMapNode_childKey(n, i);
      // just "fill" the hole with the next node up
      while (i < n->numChildren - 1) {
        nodes[i] = nodes[i + 1];
        *nk = *(nk + 1);
        i++;
        nk++;
      }
      // reduce child count

      n->numChildren--;
      memmove(((char *)nodes) - 1, (char *)nodes, sizeof(TrieMapNode *) * n->numChildren);

    } else {
      // this node is ok!
      // if needed - merge this node with it its single child
      if (nodes[i] && nodes[i]->numChildren == 1) {
        nodes[i] = __trieMapNode_MergeWithSingleChild(nodes[i]);
      }
    }
    i++;
  }
}

int TrieMapNode_Delete(TrieMapNode *n, char *str, tm_len_t len, void (*freeCB)(void *)) {
  tm_len_t offset = 0;
  int stackCap = 8;
  TrieMapNode **stack = rm_calloc(stackCap, sizeof(TrieMapNode *));
  int stackPos = 0;
  int rc = 0;
  while (n && (offset < len || len == 0)) {
    stack[stackPos++] = n;
    if (stackPos == stackCap) {
      stackCap *= 2;
      stack = rm_realloc(stack, stackCap * sizeof(TrieMapNode *));
    }
    tm_len_t localOffset = 0;
    for (; offset < len && localOffset < n->len; offset++, localOffset++) {
      if (str[offset] != n->str[localOffset]) {
        break;
      }
    }

    if (offset == len) {
      // we're at the end of both strings!
      // this means we've found what we're looking for
      if (localOffset == n->len) {
        if (!(n->flags & TM_NODE_DELETED)) {
          n->flags |= TM_NODE_DELETED;
          n->flags &= ~TM_NODE_TERMINAL;

          if (n->value) {
            if (freeCB) {
              freeCB(n->value);
            } else {
              rm_free(n->value);
            }
            n->value = NULL;
          }

          rc = 1;
        }
        goto end;
      }
    } else if (localOffset == n->len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;
      for (; i < n->numChildren; i++) {

        if (str[offset] == *__trieMapNode_childKey(n, i)) {
          nextChild = __trieMapNode_children(n)[i];
          break;
        }
      }

      // we couldn't find a matching child
      n = nextChild;
    } else {
      goto end;
    }
  }

end:

  while (stackPos--) {
    __trieMapNode_optimizeChildren(stack[stackPos], freeCB);
  }
  rm_free(stack);
  return rc;
}

int TrieMap_Delete(TrieMap *t, char *str, tm_len_t len, void (*freeCB)(void *)) {
  int rc = TrieMapNode_Delete(t->root, str, len, freeCB);
  t->cardinality -= rc;
  return rc;
}

size_t TrieMapNode_MemUsage(TrieMapNode *n) {
  size_t ret = __trieMapNode_Sizeof(n->numChildren, n->len);
  for (tm_len_t i = 0; i < n->numChildren; i++) {
    TrieMapNode *child = __trieMapNode_children(n)[i];
    ret += TrieMapNode_MemUsage(child);
  }
  return ret;
}

size_t TrieMap_MemUsage(TrieMap *t) {
  return TrieMapNode_MemUsage(t->root);
}

void TrieMapNode_Free(TrieMapNode *n, void (*freeCB)(void *)) {
  for (tm_len_t i = 0; i < n->numChildren; i++) {
    TrieMapNode *child = __trieMapNode_children(n)[i];
    TrieMapNode_Free(child, freeCB);
  }
  if (n->value) {
    if (freeCB) {
      freeCB(n->value);
    } else {
      rm_free(n->value);
    }
  }

  rm_free(n);
}

/* the current top of the iterator stack */
#define __tmi_current(it) &it->stack[it->stackOffset - 1]

/* Step itearator return codes below: */

#define TM_ITERSTATE_SELF 0
#define TM_ITERSTATE_CHILDREN 1

/* Push a new trie node on the iterator's stack */
inline void __tmi_Push(TrieMapIterator *it, TrieMapNode *node) {
  if (it->stackOffset == it->stackCap) {
    // make sure we don't overflow stackCap
    if ((int)it->stackCap + 1024 > 0xFFFF) {
      it->stackCap = 0xFFFF;
    } else {
      it->stackCap += MIN(it->stackCap, 1024);
    }
    it->stack = rm_realloc(it->stack, it->stackCap * sizeof(__tmi_stackNode));
  }
  it->stack[it->stackOffset++] = (__tmi_stackNode){
      .childOffset = 0,
      .stringOffset = 0,
      .n = node,
      .state = TM_ITERSTATE_SELF,
  };
}

inline void __tmi_Pop(TrieMapIterator *it) {
  it->bufOffset -= (__tmi_current(it))->stringOffset;
  if (it->bufOffset < it->prefixLen) {
    it->inSuffix = 0;
  }
  --it->stackOffset;
}

TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix, tm_len_t len) {
  TrieMapIterator *it = rm_calloc(1, sizeof(TrieMapIterator));

  it->bufLen = 16;
  it->buf = rm_calloc(1, it->bufLen);
  it->stackCap = 8;
  it->stack = rm_calloc(it->stackCap, sizeof(__tmi_stackNode));
  it->bufOffset = 0;
  it->inSuffix = 0;
  it->prefix = prefix;
  it->prefixLen = len;

  __tmi_Push(it, t->root);

  return it;
}

void TrieMapIterator_Free(TrieMapIterator *it) {
  rm_free(it->buf);
  rm_free(it->stack);
  rm_free(it);
}

#define TRIE_INITIAL_STRING_LEN 255

typedef struct {
  char *buf;
  TrieMapRangeCallback *callback;
  void *cbctx;
  bool includeMin;
  bool includeMax;
} TrieMapRangeCtx;

typedef struct {
  const char *r;
  int n;
} TrieMaprsbHelper;

static int nodecmp(const char *sa, size_t na, const char *sb, size_t nb) {
  size_t minlen = MIN(na, nb);
  for (size_t ii = 0; ii < minlen; ++ii) {
    char a = tolower(sa[ii]), b = tolower(sb[ii]);
    int rc = a - b;
    if (rc == 0) {
      continue;
    }
    return rc;
  }

  // Both strings match up to this point
  if (na > nb) {
    // nb is a substring of na; na is greater
    return 1;
  } else if (nb > na) {
    // na is a substring of nb; nb is greater
    return -1;
  }
  // strings are the same
  return 0;
}

static int TrieMaprsbCompareCommon(const void *h, const void *e, int prefix) {
  const TrieMaprsbHelper *term = h;
  const TrieMapNode *elem = *(const TrieMapNode **)e;
  size_t ntmp;
  int rc;
  if (prefix) {
    size_t minLen = MIN(elem->len, term->n);
    rc = nodecmp(term->r, minLen, elem->str, minLen);
  } else {
    rc = nodecmp(term->r, term->n, elem->str, elem->len);
  }
  return rc;
}

static int TrieMaprsbCompareExact(const void *h, const void *e) {
  return TrieMaprsbCompareCommon(h, e, 0);
}

static int TrieMaprsbComparePrefix(const void *h, const void *e) {
  return TrieMaprsbCompareCommon(h, e, 1);
}

static void TrieMaprangeIterateSubTree(TrieMapNode *n, TrieMapRangeCtx *r) {
  r->buf = array_ensure_append(r->buf, n->str, n->len, char);

  if (__trieMapNode_isTerminal(n)) {
    r->callback(r->buf, array_len(r->buf), r->cbctx, n->value);
  }

  TrieMapNode **arr = __trieMapNode_children(n);

  for (int ii = 0; ii < n->numChildren; ++ii) {
    // printf("Descending to index %lu\n", ii);
    TrieMaprangeIterateSubTree(arr[ii], r);
  }

  array_trimm_len(r->buf, array_len(r->buf) - n->len);
}

/**
 * Try to place as many of the common arguments in rangectx, so that the stack
 * size is not negatively impacted and prone to attack.
 */
static void TrieMapRangeIterate(TrieMapNode *n, const char *min, int nmin, const char *max,
                                int nmax, TrieMapRangeCtx *r) {
  // Push string to stack
  r->buf = array_ensure_append(r->buf, n->str, n->len, char);

  if (__trieMapNode_isTerminal(n)) {
    // current node is a terminal.
    // if nmin or nmax is zero, it means that we find an exact match
    // we should fire the callback only if exact match requested
    if (r->includeMin && nmin == 0) {
      r->callback(r->buf, array_len(r->buf), r->cbctx, n->value);
    } else if (r->includeMax && nmax == 0) {
      r->callback(r->buf, array_len(r->buf), r->cbctx, n->value);
    }
  }

  TrieMapNode **arr = __trieMapNode_children(n);
  size_t arrlen = n->numChildren;
  if (!arrlen) {
    // no children, just return.
    goto clean_stack;
  }

  __trieNode_sortChildren(n);

  // Find the minimum range here..
  // Use binary search to find the beginning and end ranges:
  TrieMaprsbHelper h;

  int beginEqIdx = -1;
  if (nmin > 0) {
    // searching for node that matches the prefix of our min value
    h.r = min;
    h.n = nmin;
    beginEqIdx = rsb_eq(arr, arrlen, sizeof(*arr), &h, TrieMaprsbComparePrefix);
  }

  int endEqIdx = -1;
  if (nmax > 0) {
    // searching for node that matches the prefix of our max value
    h.r = max;
    h.n = nmax;
    endEqIdx = rsb_eq(arr, arrlen, sizeof(*arr), &h, TrieMaprsbComparePrefix);
  }

  if (beginEqIdx == endEqIdx && endEqIdx != -1) {
    // special case, min value and max value share a command prefix.
    // we need to call recursively with the child contains this prefix
    TrieMapNode *child = arr[beginEqIdx];

    const char *nextMin = min + child->len;
    int nNextMin = nmin - child->len;
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = NULL;
    }

    const char *nextMax = max + child->len;
    int nNextMax = nmax - child->len;
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = NULL;
    }

    TrieMapRangeIterate(child, nextMin, nNextMin, nextMax, nNextMax, r);
    goto clean_stack;
  }

  if (beginEqIdx != -1) {
    // we find a child that matches min prefix
    // we should continue the search on this child but at this point we should
    // not limit the max value
    TrieMapNode *child = arr[beginEqIdx];

    const char *nextMin = min + child->len;
    int nNextMin = nmin - child->len;
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = NULL;
    }

    TrieMapRangeIterate(child, nextMin, nNextMin, NULL, -1, r);
  }

  int beginIdx = 0;
  if (nmin > 0) {
    // search for the first element which are greater then our min value
    h.r = min;
    h.n = nmin;
    beginIdx = rsb_gt(arr, arrlen, sizeof(*arr), &h, TrieMaprsbCompareExact);
  }

  int endIdx = nmax ? arrlen - 1 : -1;
  if (nmax > 0) {
    // search for the first element which are less then our max value
    h.r = max;
    h.n = nmax;
    endIdx = rsb_lt(arr, arrlen, sizeof(*arr), &h, TrieMaprsbCompareExact);
  }

  // we need to iterate (without any checking) on all the subtree from beginIdx to endIdx
  for (int ii = beginIdx; ii <= endIdx; ++ii) {
    TrieMaprangeIterateSubTree(arr[ii], r);
  }

  if (endEqIdx != -1) {
    // we find a child that matches max prefix
    // we should continue the search on this child but at this point we should
    // not limit the min value
    TrieMapNode *child = arr[endEqIdx];

    const char *nextMax = max + child->len;
    int nNextMax = nmax - child->len;
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = NULL;
    }

    TrieMapRangeIterate(child, NULL, -1, nextMax, nNextMax, r);
  }

clean_stack:
  array_trimm_len(r->buf, array_len(r->buf) - n->len);
}

void TrieMap_IterateRange(TrieMap *trie, const char *min, int minlen, bool includeMin,
                          const char *max, int maxlen, bool includeMax,
                          TrieMapRangeCallback callback, void *ctx) {
  if (trie->root->numChildren == 0) {
    return;
  }

  if (min && max) {
    // min and max exists, lets compare them to make sure min < max
    int cmp = nodecmp(min, minlen, max, maxlen);
    if (cmp > 0) {
      // min > max, no reason to continue
      return;
    }

    if (cmp == 0) {
      // min = max, we should just search for min and check for its existence
      if (includeMin || includeMax) {
        void *val = TrieMapNode_Find(trie->root, (char *)min, minlen);
        if (val != TRIEMAP_NOTFOUND) {
          callback(min, minlen, ctx, val);
        }
      }
      return;
    }
  }

  TrieMapRangeCtx tmctx = {
      .callback = callback,
      .cbctx = ctx,
      .includeMin = includeMin,
      .includeMax = includeMax,
  };
  tmctx.buf = array_new(char, TRIE_INITIAL_STRING_LEN);
  TrieMapRangeIterate(trie->root, min, minlen, max, maxlen, &tmctx);
  array_free(tmctx.buf);
}

int TrieMapIterator_Next(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value) {
  while (it->stackOffset > 0) {
    __tmi_stackNode *current = __tmi_current(it);
    TrieMapNode *n = current->n;

    if (current->state == TM_ITERSTATE_SELF) {
      while (current->stringOffset < n->len) {
        char b = current->n->str[current->stringOffset];
        if (!it->inSuffix) {
          // end of iteration in prefix mode
          if (it->prefix[it->bufOffset] != b) {
            goto pop;
          }
          if (it->bufOffset == it->prefixLen - 1) {
            it->inSuffix = 1;
          }
        }

        // advance the buffer offset and character offset
        it->buf[it->bufOffset++] = b;
        current->stringOffset++;

        // if needed - increase the buffer on the heap
        if (it->bufOffset == it->bufLen) {
          if ((int)it->bufLen + 1024 > 0xFFFF) {
            it->bufLen = 0xFFFF;
          } else {
            it->bufLen += MIN(it->bufLen, 1024);
          }
          it->buf = rm_realloc(it->buf, it->bufLen);
        }
      }

      // this is required for an empty node to switch to suffix mode
      if (it->bufOffset == it->prefixLen) {
        it->inSuffix = 1;
      }
      // switch to "children mode"
      current->state = TM_ITERSTATE_CHILDREN;

      // we've reached
      if (__trieMapNode_isTerminal(n) && it->inSuffix) {
        *ptr = it->buf;
        *len = it->bufOffset;
        *value = n->value;
        return 1;
      }
    }

    if (current->state == TM_ITERSTATE_CHILDREN) {
      // push the next child that matches
      tm_len_t nch = current->n->numChildren;
      while (current->childOffset < nch) {
        if (it->inSuffix ||
            *__trieMapNode_childKey(n, current->childOffset) == it->prefix[it->bufOffset]) {
          TrieMapNode *ch = __trieMapNode_children(n)[current->childOffset++];

          // unless in suffix mode, no need to go back here after popping the
          // child, so we just set the child offset at the end
          if (!it->inSuffix) current->childOffset = nch;

          // Add the matching child to the stack
          __tmi_Push(it, ch);

          goto next;
        }
        // if the child doesn't match- just advance one
        current->childOffset++;
      }
    }
  pop:
    // at the end of the node - pop and go up
    __tmi_Pop(it);
  next:
    continue;
  }

  return 0;
}

void TrieMap_Free(TrieMap *t, void (*freeCB)(void *)) {
  TrieMapNode_Free(t->root, freeCB);
  rm_free(t);
}

TrieMapNode *TrieMapNode_RandomWalk(TrieMapNode *n, int minSteps, char **str, tm_len_t *len) {
  // create an iteration stack we walk up and down
  size_t stackCap = minSteps;
  size_t stackSz = 1;
  TrieMapNode **stack = rm_calloc(stackCap, sizeof(TrieMapNode *));
  stack[0] = n;

  if (stackSz == stackCap) {
    stackCap += minSteps;
    stack = rm_realloc(stack, stackCap * sizeof(TrieMapNode *));
  }

  size_t bufCap = n->len;

  int steps = 0;

  while (steps < minSteps || !__trieMapNode_isTerminal(stack[stackSz - 1])) {
    n = stack[stackSz - 1];

    /* select the next step - -1 means walk back up one level */
    int rnd = rand() % (n->numChildren + 1) - 1;
    if (rnd == -1) {
      /* we can't walk up the top level */
      if (stackSz > 1) {
        steps++;
        stackSz--;
        bufCap -= n->len;
      }
      continue;
    }

    /* Push a child on the stack */
    stack[stackSz++] = n = __trieMapNode_children(n)[rnd];
    steps++;
    if (stackSz == stackCap) {
      stackCap += minSteps;
      stack = rm_realloc(stack, stackCap * sizeof(TrieMapNode *));
    }

    bufCap += n->len;
  }

  /* Return the node at the top of the stack */
  n = stack[stackSz - 1];

  /* build the string by walking the stack and copying all node strings */
  char *buf = rm_malloc(bufCap + 1);
  buf[bufCap] = 0;
  tm_len_t bufSize = 0;
  for (size_t i = 0; i < stackSz; i++) {
    memcpy(buf + bufSize, stack[i]->str, stack[i]->len);
    bufSize += stack[i]->len;
  }
  *str = buf;
  *len = bufSize;
  rm_free(stack);
  return n;
}

void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen) {

  char *str;
  tm_len_t len;
  TrieMapNode *root = TrieMapNode_FindNode(t->root, (char *)prefix, pflen, NULL);
  if (!root) {
    return NULL;
  }

  TrieMapNode *n = TrieMapNode_RandomWalk(root, (int)round(log2(1 + t->cardinality)), &str, &len);
  if (n) {
    rm_free(str);
    return n->value;
  }
  return NULL;
}

int TrieMap_RandomKey(TrieMap *t, char **str, tm_len_t *len, void **ptr) {
  if (t->cardinality == 0) {
    return 0;
  }
  // TODO: deduce steps from cardinality properly
  TrieMapNode *n = TrieMapNode_RandomWalk(t->root, (int)round(log2(1 + t->cardinality)), str, len);
  *ptr = n->value;
  return 1;
}
