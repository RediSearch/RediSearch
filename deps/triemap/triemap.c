#include "triemap.h"
#include <math.h>
#include <sys/param.h>
#include <ctype.h>
#include "util/bsearch.h"
#include "util/arr.h"

///////////////////////////////////////////////////////////////////////////////////////////////

void *TRIEMAP_NOTFOUND = "NOT FOUND";

//---------------------------------------------------------------------------------------------

// The byte size of a node, based on its internal string length and number of childr

size_t TrieMapNode::Sizeof() {
  return sizeof(TrieMapNode) + _children.size() * (sizeof(TrieMapNode *) + 1) + str.length();
}

//---------------------------------------------------------------------------------------------

// Create a new trie node. str is a string to be copied into the node, starting from offset up until len.

TrieMapNode::TrieMapNode(
  std::string_view s, tm_len_t offset, void *val, bool terminal
) : str{s.data() + offset, s.length() - offset}
  , value{val}
  , flags{terminal ? TM_NODE_TERMINAL : 0}
{ }

//---------------------------------------------------------------------------------------------

TrieMap::TrieMap()
  : cardinality{0}
  , root{"", 0, nullptr, false}
{ }

//---------------------------------------------------------------------------------------------

void TrieMapNode::AddChild(std::string_view s, tm_len_t offset, void *val) {
  // a newly added child must be a terminal node
  TrieMapNode *child = new TrieMapNode(s, offset, val, true);
  _children.push_back(child);
  _children_keys.push_back(s[offset]);
  flags &= ~TM_NODE_SORTED;
}

//---------------------------------------------------------------------------------------------

void TrieMapNode::Split(tm_len_t offset) {
  TrieMapNode *newChild = new TrieMapNode(str, offset, value, isTerminal());
  newChild->flags = flags;

  // copy current node's data and children to a new child node
  _children.swap(newChild->_children);
  _children_keys.swap(newChild->_children_keys);
  // reduce the node to be just one child long with no score
  _children.push_back(newChild);
  _children_keys.push_back(newChild->str[0]);
  str.resize(offset);
  value = nullptr;
  // parent node is now non terminal and non sorted
  flags = 0;  //&= ~(TM_NODE_TERMINAL | TM_NODE_DELETED | TM_NODE_SORTED);
}

//---------------------------------------------------------------------------------------------

bool TrieMapNode::Add(std::string_view str_, void *value_, TrieMapReplaceFunc cb) {
  tm_len_t offset = 0;
  auto len_ = str_.length();
  for (; offset < len_ && offset < len_; offset++) {
    if (str_[offset] != str[offset]) {
      break;
    }
  }
  // we broke off before the end of the string
  if (offset < str.length()) {
    // split the node and create 2 child nodes:
    // 1. a child representing the new string from the diverted offset onwards
    // 2. a child representing the old node's suffix from the diverted offset
    // and the old children
    Split(offset);

    // the new string matches the split node exactly!
    // we simply turn the split node, which is now non terminal, into a
    // terminal node
    if (offset == len_) {
      value = value_;
      flags |= TM_NODE_TERMINAL;
    } else {
      // we add a child
      AddChild(str_, offset, value_);
    }

    return true;
  }

  // we're inserting in an existing node - just replace the value
  if (offset == len_) {
    bool term = isTerminal();
    bool deleted = isDeleted();

    if (cb) {
      value = cb(value, value_);
    } else {
      if (value) {
        rm_free(value);
      }
      value = value_;
    }

    // set the node as terminal
    flags |= TM_NODE_TERMINAL;
    // if it was deleted, make sure it's not now
    flags &= ~TM_NODE_DELETED;
    flags &= ~TM_NODE_SORTED;
    // if the node existed - we return 0, otherwise return 1 as it's a new node
    return !(term && !deleted);
  }

  // proceed to the next child or add a new child for the current char
  for (tm_len_t i = 0; i < _children.size(); i++) {
    TrieMapNode *child = _children[i];

    if (str_[offset] == child->str[0]) {
      int rc = child->Add(std::string_view{str_.data() + offset, len_ - offset}, value_, cb);
      _children[i] = child;
      childKey(i) = child->str[0];
      return rc;
    }
  }

  AddChild(str_, offset, value_);
  return true;
}

//---------------------------------------------------------------------------------------------

// Add a new string to a trie. Returns 1 if the key is new to the trie or 0 if
// it already existed.
//
// If value is given, it is saved as a payload inside the trie node.
// If the key already exists, we replace the old value with the new value, using
// free() to free the old value.
//
// If cb is given, instead of replacing and freeing, we call the callback with
// the old and new value, and the function should return the value to set in the
// node, and take care of freeing any unwanted pointers. The returned value
// can be nullptr and doesn't have to be either the old or new value.

bool TrieMap::Add(std::string_view str_, void *value, TrieMapReplaceFunc cb) {
  bool rc = root.Add(str_, value, cb);
#ifdef DEBUG_TRIE
  Print();
#endif
  cardinality += rc;
  return rc;
}

//---------------------------------------------------------------------------------------------

/*
// comparator for node sorting by child max score

static int TrieMapNode::Cmp(const void *p1, const void *p2) {
  return (*(TrieMapNode **)p1)->str[0] - (*(TrieMapNode **)p2)->str[0];
}

//---------------------------------------------------------------------------------------------

static int __cmp_chars(const void *p1, const void *p2) {
  return *(char *)p1 - *(char *)p2;
}
*/

//---------------------------------------------------------------------------------------------

// Sort the children of a node by their first letter to allow binary search

void TrieMapNode::sortChildren() {
  if (!(flags & TM_NODE_SORTED) && _children.size() > 3) {
    std::sort(_children.begin(), _children.end(), [&](const TrieMapNode *a, const TrieMapNode *b) {
        return a->str[0] - b->str[0];
      });
    _children_keys.clear();
    for (auto child: _children) {
      _children_keys.push_back(child->str[0]);
    }
    flags |= TM_NODE_SORTED;
  }
}

//---------------------------------------------------------------------------------------------

void *TrieMapNode::Find(std::string_view str_) {
  TrieMapNode *n = this;
  auto len = str_.length();
  tm_len_t offset = 0;
  while (n && (offset < len || len == 0)) {
    tm_len_t localOffset = 0;
    tm_len_t nlen = n->str.length();
    while (offset < len && localOffset < nlen) {
      if (str_[offset] != n->str[localOffset]) {
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
        if (n->isTerminal() && !n->isDeleted()) {
          return n->value;
        } else {
          return TRIEMAP_NOTFOUND;
        }
      }
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to.
      tm_len_t i = 0;
      TrieMapNode *nextChild = nullptr;
      tm_len_t nc = n->_children.size();
      while (i < nc) {
        if (str_[offset] == n->childKey(i)) {
          nextChild = n->_children[i];
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

//---------------------------------------------------------------------------------------------

// Find a node by string. Return the node matching the string even if it is not
// terminal. Puts the node local offset in *offset */

TrieMapNode *TrieMapNode::FindNode(std::string_view str_, tm_len_t *poffset) {
  TrieMapNode *n = this;
  tm_len_t offset = 0;
  auto len_ = str_.length();
  while (offset < len_ || len_ == 0) {
    tm_len_t localOffset = 0;
    tm_len_t nlen = n->str.length();
    while (offset < len_ && localOffset < nlen) {
      if (str_[offset] != n->str[localOffset]) {
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
      return n;
    }

    // we've reached the end of the node's string
    if (localOffset == nlen) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = nullptr;
      while (i < n->_children.size()) {
        if (str_[offset] == n->childKey(i)) {
          nextChild = n->_children[i];
          break;
        }
        ++i;
      }

      // we couldn't find a matching child
      n = nextChild;
    } else {
      return nullptr;
    }
  }

  return nullptr;
}

//---------------------------------------------------------------------------------------------

// Find the entry with a given string and length, and return its value, even if
// that was nullptr.
//
// NOTE: If the key does not exist in the trie, we return the specialch->
// constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
// comparing to it, becase nullptr can be a valid result.

void *TrieMap::Find(std::string_view str_) {
  return root.Find(str_);
}

//---------------------------------------------------------------------------------------------

// If a node has a single child after delete, we can merged them. This deletes
// the node and returns a newly allocated node

TrieMapNode *TrieMapNode::MergeWithSingleChild() {
  // we do not merge terminal nodes
  if (isTerminal() || _children.size() != 1) {
    return this;
  }

  TrieMapNode *ch = _children.front();

  // Copy the current node's data and children to a new child node
  str += ch->str;
  flags = ch->flags;
  _children.swap(ch->_children);
  _children_keys.swap(ch->_children_keys);

  delete ch;
  return this;
}

//---------------------------------------------------------------------------------------------

// Optimize the node and its children:
// 1. If a child should be deleted - delete it and reduce the child count
// 2. If a child has a single child - merge them

void TrieMapNode::optimizeChildren() {
  // free deleted terminal nodes
  auto key_i = _children_keys.begin();
  for (auto child_i = _children.begin(); child_i != _children.end(); ++child_i, ++key_i) {
    auto *child = *child_i;
    // if this is a deleted node with no children - remove it
    if (child->_children.empty() && child->isDeleted()) {
      child_i = _children.erase(child_i);
      key_i = _children_keys.erase(key_i);
      child = *child_i;
    } else {
      // this node is ok!
      // if needed - merge this node with its single child
      if (child->_children.size() == 1) {
        child->MergeWithSingleChild();
      }
    }
  }
}

//---------------------------------------------------------------------------------------------

bool TrieMapNode::Delete(std::string_view str_) {
  TrieMapNode *n = this;
  tm_len_t offset = 0;
  Vector<TrieMapNode *> stack;
  auto len = str_.length();
  bool rc = false;
  while (n && (offset < len || len == 0)) {
    stack.push_back(n);
    tm_len_t localOffset = 0;
    for (; offset < len && localOffset < str.length(); offset++, localOffset++) {
      if (str_[offset] != n->str[localOffset]) {
        break;
      }
    }

    if (offset == len) {
      // we're at the end of both strings!
      // this means we've found what we're looking for
      if (localOffset == n->str.length()) {
        if (!(n->flags & TM_NODE_DELETED)) {
          n->flags |= TM_NODE_DELETED;
          n->flags &= ~TM_NODE_TERMINAL;

          if (n->value) {
            delete n->value;
            value = nullptr;
          }

          rc = true;
        }
        goto end;
      }
    } else if (localOffset == n->str.length()) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      TrieMapNode *nextChild = nullptr;
      for (tm_len_t i = 0; i < n->_children.size(); i++) {
        if (str[offset] == n->childKey(i)) {
          nextChild = _children[i];
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
  for (auto n1 : stack) {
    n1->optimizeChildren();
  }

  return rc;
}

//---------------------------------------------------------------------------------------------

// Mark a node as deleted. It also optimizes the trie by merging nodes if
// needed. If freeCB is given, it will be used to free the value of the deleted
// node. If it doesn't, we simply call free() */

int TrieMap::Delete(std::string_view str) {
  int rc = root.Delete(str);
  cardinality -= rc;
  return rc;
}

//---------------------------------------------------------------------------------------------

size_t TrieMapNode::MemUsage() const {
  size_t ret = TrieMapNode::Sizeof();
  for (auto child: _children) {
    ret += child->MemUsage();
  }
  return ret;
}

size_t TrieMap::MemUsage() const {
  return root.MemUsage();
}

//---------------------------------------------------------------------------------------------

// Push a new trie node on the iterator's stack

void TrieMapIterator::Push(TrieMapNode *node) {
  stack.emplace_back(stackNode{node});
}

//---------------------------------------------------------------------------------------------

void TrieMapIterator::Pop() {
  bufOffset -= current().stringOffset;
  if (bufOffset < prefix.length()) {
    inSuffix = 0;
  }
  stack.pop_back();
}

//---------------------------------------------------------------------------------------------

// Iterate the trie for all the suffixes of a given prefix. This returns an
// iterator object even if the prefix was not found, and subsequent calls to
// TrieMapIterator::Next are needed to get the results from the iteration. If the
// prefix is not found, the first call to next will return 0 */

TrieMapIterator *TrieMap::Iterate(std::string_view prefix) {
  return new TrieMapIterator(&root, prefix);
}

//---------------------------------------------------------------------------------------------

static int nodecmp(const char *sa, size_t na, const char *sb, size_t nb) {
  size_t minlen = MIN(na, nb);
  for (size_t i = 0; i < minlen; ++i) {
    char a = tolower(sa[i]), b = tolower(sb[i]);
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

//---------------------------------------------------------------------------------------------

static int TrieMapNode::CompareCommon(const void *h, const void *e, bool isPrefix) {
  const TrieMaprsbHelper *term = (const TrieMaprsbHelper*) h;
  const TrieMapNode *elem = *(const TrieMapNode **) e;
  size_t ntmp;
  int rc;
  if (isPrefix) {
    size_t minLen = MIN(elem->str.length(), term->n);
    rc = nodecmp(term->r, minLen, elem->str.c_str(), minLen);
  } else {
    rc = nodecmp(term->r, term->n, elem->str.c_str(), elem->str.length());
  }
  return rc;
}

static int TrieMapNode::CompareExact(const void *h, const void *e) {
  return TrieMapNode::CompareCommon(h, e, false);
}

static int TrieMapNode::ComparePrefix(const void *h, const void *e) {
  return TrieMapNode::CompareCommon(h, e, true);
}

//---------------------------------------------------------------------------------------------

void TrieMapNode::rangeIterateSubTree(TrieMapRange *r) {
  r->buf = array_ensure_append(r->buf, str.c_str(), str.length(), char);

  if (isTerminal()) {
    r->callback(r->buf, array_len(r->buf), r->cbctx, value);
  }

  for (auto child: _children) {
    child->rangeIterateSubTree(r);
  }

  array_trimm_len(r->buf, array_len(r->buf) - str.length());
}

//---------------------------------------------------------------------------------------------

// Try to place as many of the common arguments in TrieMapRange, so that the stack
// size is not negatively impacted and prone to attack.

void TrieMapNode::RangeIterate(const char *min, int nmin, const char *max,
                               int nmax, TrieMapRange *r) {
  int beginIdx = 0, endIdx;
  int beginEqIdx, endEqIdx;

  // Push string to stack
  r->buf = array_ensure_append(r->buf, str.c_str(), str.length(), char);

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

  Vector<TrieMapNode *> arr = _children;
  size_t arrlen = _children.size();
  if (_children.empty()) {
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
    beginEqIdx = rsb_eq_vec(arr, &h, TrieMapNode::ComparePrefix);
  }

  endEqIdx = -1;
  if (nmax > 0) {
    // searching for node that matches the prefix of our max value
    h.r = max;
    h.n = nmax;
    endEqIdx = rsb_eq_vec(arr, &h, TrieMapNode::ComparePrefix);
  }

  if (beginEqIdx == endEqIdx && endEqIdx != -1) {
    // special case, min value and max value share a command prefix.
    // we need to call recursively with the child contains this prefix
    TrieMapNode *child = arr[beginEqIdx];

    const char *nextMin = min + child->str.length();
    int nNextMin = nmin - child->str.length();
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = nullptr;
    }

    const char *nextMax = max + child->str.length();
    int nNextMax = nmax - child->str.length();
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = nullptr;
    }

    child->RangeIterate(nextMin, nNextMin, nextMax, nNextMax, r);
    goto clean_stack;
  }

  if (beginEqIdx != -1) {
    // we find a child that matches min prefix
    // we should continue the search on this child but at this point we should
    // not limit the max value
    TrieMapNode *child = arr[beginEqIdx];

    const char *nextMin = min + child->str.length();
    int nNextMin = nmin - child->str.length();
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = nullptr;
    }

    child->RangeIterate(nextMin, nNextMin, nullptr, -1, r);
  }

  if (nmin > 0) {
    // search for the first element which are greater then our min value
    h.r = min;
    h.n = nmin;
    beginIdx = rsb_gt_vec(arr, &h, TrieMapNode::CompareExact);
  }

  endIdx = nmax ? arrlen - 1 : -1;
  if (nmax > 0) {
    // search for the first element which are less then our max value
    h.r = max;
    h.n = nmax;
    endIdx = rsb_lt_vec(arr, &h, TrieMapNode::CompareExact);
  }

  // we need to iterate (without any checking) on all the subtree from beginIdx to endIdx
  for (int i = beginIdx; i <= endIdx; ++i) {
    arr[i]->rangeIterateSubTree(r);
  }

  if (endEqIdx != -1) {
    // we find a child that matches max prefix
    // we should continue the search on this child but at this point we should
    // not limit the min value
    TrieMapNode *child = arr[endEqIdx];

    const char *nextMax = max + child->str.length();
    int nNextMax = nmax - child->str.length();
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = nullptr;
    }

    child->RangeIterate(nullptr, -1, nextMax, nNextMax, r);
  }

clean_stack:
  array_trimm_len(r->buf, array_len(r->buf) - str.length());
}

//---------------------------------------------------------------------------------------------

void TrieMap::IterateRange(const char *min, int minlen, bool includeMin,
                           const char *max, int maxlen, bool includeMax,
                           TrieMapRangeCallback callback, void *ctx) {
  if (root._children.empty()) {
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
        void *val = root.Find(std::string_view{(char *)min, minlen});
        if (val != TRIEMAP_NOTFOUND) {
          callback(min, minlen, ctx, val);
        }
      }
      return;
    }
  }

  TrieMapRange tmctx = {
      .callback = callback,
      .cbctx = ctx,
      .includeMin = includeMin,
      .includeMax = includeMax,
  };
  tmctx.buf = array_new(char, TRIE_INITIAL_STRING_LEN);
  root.RangeIterate(min, minlen, max, maxlen, &tmctx);
  array_free(tmctx.buf);
}

//---------------------------------------------------------------------------------------------

// Iterate to the next matching entry in the trie. Returns true if we can continue,
// or false if we're done and should exit

bool TrieMapIterator::Next(char **ptr, tm_len_t *len, void **value) {
  while (!stack.empty()) {
    stackNode curr = current();
    TrieMapNode *n = curr.n;

    if (curr.state == TM_ITERSTATE_SELF) {
      while (curr.stringOffset < n->str.length()) {
        char b = n->str[curr.stringOffset];
        if (!inSuffix) {
          // end of iteration in prefix mode
          if (prefix[bufOffset] != b) {
            goto pop;
          }
          if (bufOffset == prefix.length() - 1) {
            inSuffix = 1;
          }
        }

        // advance the buffer offset and character offset
        buf[bufOffset++] = b;
        curr.stringOffset++;

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
      if (bufOffset == prefix.length()) {
        inSuffix = 1;
      }
      // switch to "children mode"
      curr.state = TM_ITERSTATE_CHILDREN;

      // we've reached
      if (n->isTerminal() && inSuffix) {
        *ptr = buf;
        *len = bufOffset;
        *value = n->value;
        return true;
      }
    }

    if (curr.state == TM_ITERSTATE_CHILDREN) {
      // push the next child that matches
      tm_len_t nch = n->_children.size();
      while (curr.childOffset < nch) {
        if (inSuffix || n->childKey(curr.childOffset) == prefix[bufOffset]) {
          TrieMapNode *ch = n->_children[curr.childOffset++];

          // unless in suffix mode, no need to go back here after popping the
          // child, so we just set the child offset at the end
          if (!inSuffix) curr.childOffset = nch;

          // Add the matching child to the stack
          Push(ch);

          goto next;
        }
        // if the child doesn't match- just advance one
        curr.childOffset++;
      }
    }
  pop:
    // at the end of the node - pop and go up
    Pop();
  next:
    continue;
  }

  return false;
}

//---------------------------------------------------------------------------------------------

// TrieMap::~TrieMap() {
//   delete root;
// }

//---------------------------------------------------------------------------------------------

TrieMapNode *TrieMapNode::RandomWalk(int minSteps, std::string &newstr) {
  TrieMapNode *res = this;

  // create an iteration stack we walk up and down
  Vector<TrieMapNode*> stack;
  stack.push_back(this);

  int steps = 0;
  while (steps < minSteps || !stack.back()->isTerminal()) {
    res = stack.back();

    // select the next step - -1 means walk back up one level
	  // @@TODO: verify we're not in an infinite loop
    int rnd = rand() % (_children.size() + 1) - 1;
    if (rnd == -1) {
      // we can't walk up the top level
      if (stack.size() > 1) {
        steps++;
        stack.pop_back();
      }
      continue;
    }

    // Push a child on the stack
    auto child = res->_children[rnd];
    stack.push_back(child);
    res = child;
    steps++;
  }

  // Return the node at the top of the stack
  res = stack.back();

  // build the string by walking the stack and copying all node strings
  for (auto &n1: stack) {
    newstr += n1->str;
  }

  return res;
}

//---------------------------------------------------------------------------------------------

// Get the value of a random element under a specific prefix. nullptr if the prefix was not found

void *TrieMap::RandomValueByPrefix(std::string_view prefix) {
  TrieMapNode *root_ = root.FindNode(prefix, nullptr);
  if (!root_) {
    return nullptr;
  }

  std::string str;
  TrieMapNode *n = root_->RandomWalk((int)round(log2(1 + cardinality)), str);
  if (n) {
    return n->value;
  }
  return nullptr;
}

//---------------------------------------------------------------------------------------------

// Get a random key from the trie by doing a random walk down and up the tree
// for a minimum number of steps. Returns 0 if the tree is empty and we couldn't
// find a random node.
// Assign's the key to str and saves its len (the key is NOT null terminated).
// NOTE: It is the caller's responsibility to free the key string

bool TrieMap::RandomKey(std::string &str, void **ptr) {
  if (cardinality == 0) {
    return false;
  }
  // TODO: deduce steps from cardinality properly
  TrieMapNode *n = root.RandomWalk((int)round(log2(1 + cardinality)), str);
  *ptr = n->value;
  return true;
}

//---------------------------------------------------------------------------------------------

void TrieMapNode::Print(int idx, int depth) const {
  for (int i = 0; i < depth; ++i) {
    printf("  ");
  }
  printf("%d) '%s' flags %d\n", idx, str.c_str(), flags);
  int i = 0;
  ++depth;
  for (auto const &child: _children) {
    child->Print(i++, depth);
  }
}

//---------------------------------------------------------------------------------------------

void TrieMap::Print() const {
  root.Print(0, 0);
}

///////////////////////////////////////////////////////////////////////////////////////////////
