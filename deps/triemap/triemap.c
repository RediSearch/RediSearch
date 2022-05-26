#include "triemap.h"
#include <math.h>
#include <sys/param.h>
#include <ctype.h>
#include "util/bsearch.h"
#include "util/arr.h"

void *TRIEMAP_NOTFOUND = "NOT FOUND";

void TrieMapNode_Free(TrieMapNode *n, void (*freeCB)(void *));

/* The byte size of a node, based on its internal string length and number of
 * children */
static size_t TrieMapNode::Sizeof(tm_len_t numChildren, tm_len_t slen) {
  return (sizeof(TrieMapNode) + numChildren * sizeof(TrieMapNode *) + (slen + 1) + numChildren);
}

void TrieMapNode::resizeChildren(int offset) {
  TrieMapNode **children = children();

  // stretch or shrink the child key cache array
  memmove(((char *)children) + offset, (char *)children, sizeof(TrieMapNode *) * numChildren);
  numChildren += offset;
}

/* Create a new trie node. str is a string to be copied into the node,
 * starting from offset up until len. numChildren is the initial number of
 * allocated child nodes */
TrieMapNode::TrieMapNode(char *str_, tm_len_t offset, tm_len_t len_, tm_len_t numChildren,
                         void *value, int terminal) {
  tm_len_t nlen = len_ - offset;
  len = nlen;
  numChildren = numChildren;
  value = value;
  flags = terminal ? TM_NODE_TERMINAL : 0;

  memcpy(str, str_ + offset, nlen);
}

TrieMap::TrieMap() {
  cardinality = 0;
  root = new TrieMapNode((char *)"", 0, 0, 0, NULL, 0);
}

 void TrieMapNode::AddChild(char *str_, tm_len_t offset, tm_len_t len_, void *value) {
  // make room for another child
  resizeChildren(1);

  // a newly added child must be a terminal node
  TrieMapNode child = new TrieMapNode(str_, offset, len_, 0, value, 1);
  *childKey(numChildren - 1) = str_[offset];

  children()[numChildren - 1] = &child;
  flags &= ~TM_NODE_SORTED;
}

void TrieMapNode::Split(tm_len_t offset) {
  // Copy the current node's data and children to a new child node
  TrieMapNode *newChild = new TrieMapNode(str, offset, len, numChildren, value, isTerminal());
  newChild->flags = flags;

  TrieMapNode **children = children();
  TrieMapNode **newChildren = newChild->children();
  memcpy(newChildren, children, sizeof(TrieMapNode *) * numChildren);
  memcpy(newChild->childKey(0), childKey(0), numChildren);
  // reduce the node to be just one child long with no score
  numChildren = 1;
  len = offset;
  value = NULL;
  // the parent node is now non terminal and non sorted
  flags = 0;  //&= ~(TM_NODE_TERMINAL | TM_NODE_DELETED | TM_NODE_SORTED);

  children()[0] = newChild;
  *childKey(0) = newChild->str[0];
}

int TrieMapNode::Add(char *str_, tm_len_t len_, void *value, TrieMapReplaceFunc cb) {
  TrieMapNode n = this;

  tm_len_t offset = 0;
  for (; offset < len_ && offset < n.len; offset++) {
    if (str_[offset] != n.str[offset]) {
      break;
    }
  }
  // we broke off before the end of the string
  if (offset < n.len) {
    // split the node and create 2 child nodes:
    // 1. a child representing the new string from the diverted offset onwards
    // 2. a child representing the old node's suffix from the diverted offset
    // and the old children
    n.Split(offset);

    // the new string matches the split node exactly!
    // we simply turn the split node, which is now non terminal, into a
    // terminal node
    if (offset == len_) {
      n.value = value;
      n.flags |= TM_NODE_TERMINAL;
    } else {
      // we add a child
      n.AddChild(str_, offset, len_, value);
    }
    this = n;
    return 1;
  }

  // we're inserting in an existing node - just replace the value
  if (offset == len_) {
    int term = n.isTerminal();
    int deleted = n.isDeleted();

    if (cb) {
      n.value = cb(n.value, value);
    } else {
      if (n.value) {
        rm_free(n.value);
      }
      n.value = value;
    }

    // set the node as terminal
    n.flags |= TM_NODE_TERMINAL;
    // if it was deleted, make sure it's not now
    n.flags &= ~TM_NODE_DELETED;
    n.flags &= ~TM_NODE_SORTED;
    this = n;
    // if the node existed - we return 0, otherwise return 1 as it's a new
    // node
    return (term && !deleted) ? 0 : 1;
  }

  // proceed to the next child or add a new child for the current char
  for (tm_len_t i = 0; i < n.numChildren; i++) {
    TrieMapNode child = n.children()[i];

    if (str_[offset] == child.str[0]) {
      int rc = child.Add(str_ + offset, len_ - offset, value, cb);
      n.children()[i] = &child;
      //      *n.childKey(i) = child->str[0];

      return rc;
    }
  }

  n.AddChild(str_, offset, len_, value);
  return 1;
}

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
int TrieMap::Add(char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb) {
  int rc = &root->Add(str, len, value, cb);
  cardinality += rc;
  return rc;
}

// comparator for node sorting by child max score
static int __cmp_nodes(const void *p1, const void *p2) {
  return (*(TrieMapNode **)p1)->str[0] - (*(TrieMapNode **)p2)->str[0];
}

static int __cmp_chars(const void *p1, const void *p2) {
  return *(char *)p1 - *(char *)p2;
}

// Sort the children of a node by their first letter to allow binary search
inline void TrieMapNode::sortChildren() {
  if ((0 == (flags & TM_NODE_SORTED)) && numChildren > 3) {
    qsort(children(), numChildren, sizeof(TrieMapNode *), __cmp_nodes);
    qsort(childKey(0), numChildren, 1, __cmp_chars);
    flags |= TM_NODE_SORTED;
  }
}

void *TrieMapNode::Find(char *str_, tm_len_t len_) {
  tm_len_t offset = 0;
  while (this && (offset < len_ || len_ == 0)) {
    tm_len_t localOffset = 0;
    tm_len_t nlen = len;
    while (offset < len_ && localOffset < nlen) {
      if (str_[offset] != str[localOffset]) {
        break;
      }
      offset++;
      localOffset++;
    }

    // we've reached the end of the node's string
    if (localOffset == nlen) {
      // we're at the end of both strings!
      if (offset == len_) {
        // If this is a terminal, non deleted node
        if (isTerminal() && !isDeleted()) {
          return value;
        } else {
          return TRIEMAP_NOTFOUND;
        }
      }
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;
      // if (!(flags & TM_NODE_SORTED) && numChildren > 2) {
      //   qsort(n->children(), numChildren, sizeof(TrieMapNode *), __cmp_nodes);
      //   qsort(n->childKey(0), numChildren, 1, __cmp_chars);
      //   flags |= TM_NODE_SORTED;
      // }
      char *childKeys = childKey(0);
      char c = str_[offset];
      // if (flags & TM_NODE_SORTED) {
      //   int bottom = 0, top = numChildren - 1;

      //   while (bottom <= top) {
      //     int mid = (bottom + top) / 2;

      //     char cc = *n->childKey(mid);
      //     if (c == cc) {
      //       nextChild = n->children()[mid];
      //       break;
      //     } else if (c < cc) {
      //       top = mid - 1;
      //     } else {
      //       bottom = mid + 1;
      //     }
      //   }

      // } else {
      tm_len_t nc = numChildren;

      while (i < nc) {
        if (str_[offset] == childKeys[i]) {
          nextChild = n->children()[i];
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
TrieMapNode *TrieMapNode::FindNode(char *str_, tm_len_t len_, tm_len_t *poffset) {
  tm_len_t offset = 0;
  while (this && (offset < len_ || len_ == 0)) {
    tm_len_t localOffset = 0;
    tm_len_t nlen = len;
    while (offset < len_ && localOffset < nlen) {
      if (str_[offset] != str[localOffset]) {
        break;
      }
      offset++;
      localOffset++;
    }

    // we've reached the end of the string - return the node even if it's not
    // temrinal
    if (offset == len_) {
      // let the caller know the local offset
      if (poffset) {
        *poffset = localOffset;
      }
      return this;
    }

    // we've reached the end of the node's string
    if (localOffset == nlen) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;

      char *childKeys = childKey(0);
      char c = str_[offset];

      while (i < numChildren) {
        if (str_[offset] == childKeys[i]) {
          nextChild = children()[i];
          break;
        }
        ++i;
      }

      // we couldn't find a matching child
      this = nextChild;
    } else {
      return NULL;
    }
  }

  return NULL;
}

/* Find the entry with a given string and length, and return its value, even if
 * that was NULL.
 *
 * NOTE: If the key does not exist in the trie, we return the specialch->
 * constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
 * comparing to it, becase NULL can be a valid result.
 */
void *TrieMap::Find(char *str_, tm_len_t len_) {
  return root->Find(str_, len_);
}

/* If a node has a single child after delete, we can merged them. This
 * deletes
 * the node and returns a newly allocated node */
TrieMapNode *TrieMapNode::MergeWithSingleChild() {
  // we do not merge terminal nodes
  if (isTerminal() || numChildren != 1) {
    return this;
  }

  TrieMapNode ch = children();

  // Copy the current node's data and children to a new child node
  char nstr[len + ch.len + 1];
  memcpy(nstr, str, sizeof(char) * len);
  memcpy(&nstr[len], ch.str, sizeof(char) * ch.len);
  TrieMapNode merged = new TrieMapNode(nstr, 0, len + ch.len, ch.numChildren,
                                       ch.value, ch.isTerminal());

  merged.numChildren = ch.numChildren;
  merged.flags = ch.flags;

  memcpy(merged.children(), ch.children(),
         sizeof(TrieMapNode *) * merged.numChildren);
  memcpy(merged.childKey(0), ch.childKey(0), merged.numChildren);
  delete this;

  return merged;
}

/* Optimize the node and its children:
 *   1. If a child should be deleted - delete it and reduce the child count
 *   2. If a child has a single child - merge them
 */
void TrieMapNode::optimizeChildren(void (*freeCB)(void *)) {
  int i = 0;
  TrieMapNode **nodes = children();
  // free deleted terminal nodes
  while (i < numChildren) {
    // if this is a deleted node with no children - remove it
    if (nodes[i]->numChildren == 0 && nodes[i]->isDeleted()) {
      TrieMapNode_Free(nodes[i], freeCB);

      nodes[i] = NULL;
      char *nk = childKey(i);
      // just "fill" the hole with the next node up
      while (i < numChildren - 1) {
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
        nodes[i] = nodes[i]->MergeWithSingleChild();
      }
    }
    i++;
  }
}

int TrieMapNode::Delete(char *str_, tm_len_t len_, void (*freeCB)(void *)) {
  tm_len_t offset = 0;
  int stackCap = 8;
  // TrieMapNode **stack = rm_calloc(stackCap, sizeof(TrieMapNode *));
  TrieMapNode **stack = new TrieMapNode();
  int stackPos = 0;
  int rc = 0;
  while (this && (offset < len_ || len_ == 0)) {
    stack[stackPos++] = this;
    if (stackPos == stackCap) {
      stackCap *= 2;
      stack = rm_realloc(stack, stackCap * sizeof(TrieMapNode *));
    }
    tm_len_t localOffset = 0;
    for (; offset < len_ && localOffset < len; offset++, localOffset++) {
      if (str_[offset] != str[localOffset]) {
        break;
      }
    }

    if (offset == len_) {
      // we're at the end of both strings!
      // this means we've found what we're looking for
      if (localOffset == len) {
        if (!(flags & TM_NODE_DELETED)) {
          flags |= TM_NODE_DELETED;
          flags &= ~TM_NODE_TERMINAL;

          if (value) {
            if (freeCB) {
              freeCB(value);
            } else {
              rm_free(value);
            }
            value = NULL;
          }

          rc = 1;
        }
        goto end;
      }
    } else if (localOffset == len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;
      for (; i < numChildren; i++) {

        if (str[offset] == *childKey(i)) {
          nextChild = children()[i];
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
    stack[stackPos]->optimizeChildren(freeCB);
  }

  return rc;
}

/* Mark a node as deleted. It also optimizes the trie by merging nodes if
 * needed. If freeCB is given, it will be used to free the value of the deleted
 * node. If it doesn't, we simply call free() */
int TrieMap::Delete(char *str_, tm_len_t len_, void (*freeCB)(void *)) {
  int rc = root->Delete(str_, len_, freeCB);
  cardinality -= rc;
  return rc;
}

size_t TrieMapNode::MemUsage() {
  size_t ret = TrieMapNode::Sizeof(numChildren, len);
  for (tm_len_t i = 0; i < numChildren; i++) {
    TrieMapNode *child = children()[i];
    ret += child->MemUsage();
  }
  return ret;
}

size_t TrieMap::MemUsage() {
  return root->MemUsage();
}

void TrieMapNode::~TrieMapNode(void (*freeCB)(void *)) {
  for (tm_len_t i = 0; i < numChildren; i++) {
    TrieMapNode *child = children()[i];
    TrieMapNode_Free(child, freeCB);
  }
  if (value) {
    if (freeCB) {
      freeCB(value);
    } else {
      rm_free(value);
    }
  }
}

/* Step itearator return codes below: */

#define TM_ITERSTATE_SELF 0
#define TM_ITERSTATE_CHILDREN 1

// Push a new trie node on the iterator's stack
inline void TrieMapIterator::Push(TrieMapNode *node) {
  if (stackOffset == stackCap) {
    // make sure we don't overflow stackCap
    if ((int)stackCap + 1024 > 0xFFFF) {
      stackCap = 0xFFFF;
    } else {
      stackCap += MIN(stackCap, 1024);
    }
    stack = rm_realloc(stack, stackCap * sizeof(__tmi_stackNode));
  }
  stack[stackOffset++] = (__tmi_stackNode){
      state: TM_ITERSTATE_SELF,
      n: node,
      stringOffset: 0,
      childOffset: 0
  };
}

inline void TrieMapIterator::Pop() {
  bufOffset -= current()->stringOffset;
  if (bufOffset < prefixLen) {
    inSuffix = 0;
  }
  --stackOffset;
}

/* Iterate the trie for all the suffixes of a given prefix. This returns an
 * iterator object even if the prefix was not found, and subsequent calls to
 * TrieMapIterator::Next are needed to get the results from the iteration. If the
 * prefix is not found, the first call to next will return 0 */
TrieMapIterator *TrieMap::Iterate(const char *prefix, tm_len_t len) {
  TrieMapIterator *it = new TrieMapIterator();

  bufLen = 16;
  stackCap = 8;
  bufOffset = 0;
  inSuffix = 0;
  prefix = prefix;
  prefixLen = len;

  it->Push(root);

  return it;
}
#define TRIE_INITIAL_STRING_LEN 255

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

static int TrieMapNode::CompareCommon(const void *h, const void *e, bool prefix) {
  const TrieMaprsbHelper term = h;
  const TrieMapNode elem = *(const TrieMapNode **)e;
  size_t ntmp;
  int rc;
  if (prefix) {
    size_t minLen = MIN(elem.len, term.n);
    rc = nodecmp(term.r, minLen, elem.str, minLen);
  } else {
    rc = nodecmp(term.r, term.n, elem.str, elem.len);
  }
  return rc;
}

static int TrieMapNode::CompareExact(const void *h, const void *e) {
  return TrieMapNode::CompareCommon(h, e, false);
}

static int TrieMapNode::ComparePrefix(const void *h, const void *e) {
  return TrieMapNode::CompareCommon(h, e, true);
}

void TrieMapNode::rangeIterateSubTree(TrieMapRangeCtx *r) {
  r->buf = array_ensure_append(r->buf, str, len, char);

  if (isTerminal()) {
    r->callback(r->buf, array_len(r->buf), r->cbctx, value);
  }

  TrieMapNode **arr = children();

  for (int ii = 0; ii < numChildren; ++ii) {
    // printf("Descending to index %lu\n", ii);
    arr[ii]->rangeIterateSubTree(r);
  }

  array_trimm_len(r->buf, array_len(r->buf) - len);
}

//Try to place as many of the common arguments in rangectx, so that the stack
//size is not negatively impacted and prone to attack.
void TrieMapNode::RangeIterate(const char *min, int nmin, const char *max,
                               int nmax, TrieMapRangeCtx *r) {
  int beginIdx = 0, endIdx;
  int beginEqIdx, endEqIdx;

  // Push string to stack
  r->buf = array_ensure_append(r->buf, str, len, char);

  if (isTerminal()) {
    // current node is a terminal.
    // if nmin or nmax is zero, it means that we find an exact match
    // we should fire the callback only if exact match requested
    if (r->includeMin && nmin == 0) {
      r->callback(r->buf, array_len(r->buf), r->cbctx, value);
    } else if (r->includeMax && nmax == 0) {
      r->callback(r->buf, array_len(r->buf), r->cbctx, value);
    }
  }

  TrieMapNode **arr = children();
  size_t arrlen = numChildren;
  if (!arrlen) {
    // no children, just return.
    goto clean_stack;
  }

  sortChildren();

  // Find the minimum range here..
  // Use binary search to find the beginning and end ranges:
  TrieMaprsbHelper h;

  beginEqIdx = -1;
  if (nmin > 0) {
    // searching for node that matches the prefix of our min value
    h.r = min;
    h.n = nmin;
    beginEqIdx = rsb_eq(arr, arrlen, sizeof(*arr), &h, TrieMapNode::ComparePrefix);
  }

  endEqIdx = -1;
  if (nmax > 0) {
    // searching for node that matches the prefix of our max value
    h.r = max;
    h.n = nmax;
    endEqIdx = rsb_eq(arr, arrlen, sizeof(*arr), &h, TrieMapNode::ComparePrefix);
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

    child->RangeIterate(nextMin, nNextMin, nextMax, nNextMax, r);
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

    child->RangeIterate(nextMin, nNextMin, NULL, -1, r);
  }

  if (nmin > 0) {
    // search for the first element which are greater then our min value
    h.r = min;
    h.n = nmin;
    beginIdx = rsb_gt(arr, arrlen, sizeof(*arr), &h, TrieMapNode::CompareExact);
  }

  endIdx = nmax ? arrlen - 1 : -1;
  if (nmax > 0) {
    // search for the first element which are less then our max value
    h.r = max;
    h.n = nmax;
    endIdx = rsb_lt(arr, arrlen, sizeof(*arr), &h, TrieMapNode::CompareExact);
  }

  // we need to iterate (without any checking) on all the subtree from beginIdx to endIdx
  for (int ii = beginIdx; ii <= endIdx; ++ii) {
    arr[ii]->rangeIterateSubTree(r);
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

    child->RangeIterate(NULL, -1, nextMax, nNextMax, r);
  }

clean_stack:
  array_trimm_len(r->buf, array_len(r->buf) - len);
}

void TrieMap::IterateRange(const char *min, int minlen, bool includeMin,
                           const char *max, int maxlen, bool includeMax,
                           TrieMapRangeCallback callback, void *ctx) {
  if (root->numChildren == 0) {
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
        void *val = root->Find((char *)min, minlen);
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
  root->RangeIterate(min, minlen, max, maxlen, &tmctx);
  array_free(tmctx.buf);
}

// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
// or 0 if we're done and should exit
int TrieMapIterator::Next(char **ptr, tm_len_t *len, void **value) {
  while (stackOffset > 0) {
    __tmi_stackNode *current = current();
    TrieMapNode *n = current->n;

    if (current->state == TM_ITERSTATE_SELF) {
      while (current->stringOffset < n->len) {
        char b = current->n->str[current->stringOffset];
        if (!inSuffix) {
          // end of iteration in prefix mode
          if (prefix[bufOffset] != b) {
            goto pop;
          }
          if (bufOffset == prefixLen - 1) {
            inSuffix = 1;
          }
        }

        // advance the buffer offset and character offset
        buf[bufOffset++] = b;
        current->stringOffset++;

        // if needed - increase the buffer on the heap
        if (bufOffset == bufLen) {
          if ((int)bufLen + 1024 > 0xFFFF) {
            bufLen = 0xFFFF;
          } else {
            bufLen += MIN(bufLen, 1024);
          }
          buf = rm_realloc(buf, bufLen);
        }
      }

      // this is required for an empty node to switch to suffix mode
      if (bufOffset == prefixLen) {
        inSuffix = 1;
      }
      // switch to "children mode"
      current->state = TM_ITERSTATE_CHILDREN;

      // we've reached
      if (n->isTerminal() && inSuffix) {
        *ptr = buf;
        *len = bufOffset;
        *value = n->value;
        return 1;
      }
    }

    if (current->state == TM_ITERSTATE_CHILDREN) {
      // push the next child that matches
      tm_len_t nch = current->n->numChildren;
      while (current->childOffset < nch) {
        if (inSuffix ||
            *n->childKey(current->childOffset) == prefix[bufOffset]) {
          TrieMapNode *ch = n->children()[current->childOffset++];

          // unless in suffix mode, no need to go back here after popping the
          // child, so we just set the child offset at the end
          if (!inSuffix) current->childOffset = nch;

          // Add the matching child to the stack
          Push(ch);

          goto next;
        }
        // if the child doesn't match- just advance one
        current->childOffset++;
      }
    }
  pop:
    // at the end of the node - pop and go up
    Pop();
  next:
    continue;
  }

  return 0;
}

void TrieMap::~TrieMap(void (*freeCB)(void *)) {
  delete root;
}

TrieMapNode *TrieMapNode::RandomWalk(int minSteps, char **str_, tm_len_t *len_) {
  // create an iteration stack we walk up and down
  size_t stackCap = minSteps;
  size_t stackSz = 1;
  TrieMapNode **stack;
  stack[0] = this;

  if (stackSz == stackCap) {
    stackCap += minSteps;
    stack = rm_realloc(stack, stackCap * sizeof(TrieMapNode *));
  }

  size_t bufCap = len;

  int steps = 0;

  while (steps < minSteps || !stack[stackSz - 1]->isTerminal()) {
    this = stack[stackSz - 1];

    /* select the next step - -1 means walk back up one level */
    int rnd = rand() % (numChildren + 1) - 1;
    if (rnd == -1) {
      /* we can't walk up the top level */
      if (stackSz > 1) {
        steps++;
        stackSz--;
        bufCap -= len;
      }
      continue;
    }

    /* Push a child on the stack */
    stack[stackSz++] = this = children()[rnd];
    steps++;
    if (stackSz == stackCap) {
      stackCap += minSteps;
      stack = rm_realloc(stack, stackCap * sizeof(TrieMapNode *));
    }

    bufCap += len;
  }

  /* Return the node at the top of the stack */
  this = stack[stackSz - 1];

  /* build the string by walking the stack and copying all node strings */
  char *buf = rm_malloc(bufCap + 1);
  buf[bufCap] = 0;
  tm_len_t bufSize = 0;
  for (size_t i = 0; i < stackSz; i++) {
    memcpy(buf + bufSize, stack[i]->str, stack[i]->len_);
    bufSize += stack[i]->len_;
  }
  *str_ = buf;
  *len_ = bufSize;
  return this;
}

// Get the value of a random element under a specific prefix. NULL if the prefix was not found
void *TrieMap::RandomValueByPrefix(const char *prefix, tm_len_t pflen) {
  char *str;
  tm_len_t len;
  TrieMapNode *root_ = root->FindNode((char *)prefix, pflen, NULL);
  if (!root_) {
    return NULL;
  }

  TrieMapNode *n = root_->RandomWalk((int)round(log2(1 + t->cardinality)), &str, &len);
  if (n) {
    rm_free(str);
    return n->value;
  }
  return NULL;
}

/* Get a random key from the trie by doing a random walk down and up the tree
 * for a minimum number of steps. Returns 0 if the tree is empty and we couldn't
 * find a random node.
 * Assign's the key to str and saves its len (the key is NOT null terminated).
 * NOTE: It is the caller's responsibility to free the key string
 */
int TrieMap::RandomKey(char **str, tm_len_t *len, void **ptr) {
  if (cardinality == 0) {
    return 0;
  }
  // TODO: deduce steps from cardinality properly
  TrieMapNode *n = root->RandomWalk((int)round(log2(1 + t->cardinality)), str, len);
  *ptr = n->value;
  return 1;
}
