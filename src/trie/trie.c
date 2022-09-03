#include "trie/trie.h"
#include "util/bsearch.h"
#include "sparse_vector.h"
#include "redisearch.h"
#include "util/arr.h"

#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Allocate a new trie payload struct

TriePayload::TriePayload(const char *payload, uint32_t plen) {
  len = plen;
  memcpy(data, payload, sizeof(char) * plen);
}

//---------------------------------------------------------------------------------------------

// Add a child node to the parent node n, with a string str starting at offset up until len, and a
// given score

TrieNode::TrieNode(const Runes &runes, t_len offset, const char *payload, size_t payload_size,
                   t_len numChildren, float score_, bool terminal) : _runes(runes[offset]) {
  _children.reserve(numChildren);
  _score = score_;
  _flags = 0 | (terminal ? TRIENODE_TERMINAL : 0);
  _maxChildScore = 0;
  _sortmode = TRIENODE_SORTED_NONE;
  if (payload != NULL && payload_size > 0) {
    _payload = new TriePayload(payload, payload_size);
  }
}

//---------------------------------------------------------------------------------------------

TrieNode *TrieNode::AddChild(const Runes &runes, t_len offset, RSPayload *payload, float score) {
  // a newly added child must be a terminal node
  _children.emplace_back(new TrieNode(runes, offset, payload ? payload->data : NULL,
    payload ? payload->len : 0, 0, score, true));
  _sortmode = TRIENODE_SORTED_NONE;
  return _children.back();
}

//---------------------------------------------------------------------------------------------

// Split node n at string offset n. This returns a new node which has a string
// up until offset, and a single child holding The old score of n, and its score.

void TrieNode::SplitNode(t_len offset) {
  // Copy the current node's data and children to a new child node
  TrieNode *newChild = new TrieNode(_runes, offset, _payload ? _payload->data : NULL,
    _payload ? _payload->len : 0, _children.size(), _score, isTerminal());
  newChild->_maxChildScore = _maxChildScore;
  newChild->_flags = _flags;
  _children.swap(newChild->_children);

  // reduce the node to be just one child long with no score
  _children.emplace_back(newChild);
  _len = offset;
  _score = 0;
  // the parent node is now non terminal and non sorted
  _flags &= ~(TRIENODE_TERMINAL | TRIENODE_DELETED);
  _sortmode = TRIENODE_SORTED_NONE;

  _maxChildScore = MAX(_maxChildScore, newChild->_score);
  if (_payload != NULL) {
    delete _payload;
    _payload = NULL;
  }
}

//---------------------------------------------------------------------------------------------

// If a node has a single child after delete, we can merged them.
// This deletes the node and returns a newly allocated node.

void TrieNode::MergeWithSingleChild() {
  if (isTerminal() || _children.size() != 1) {
    return this;
  }
  TrieNode *ch = _children.front();

  _runes.append(ch->_runes);

  // copy state from child
  _score = ch->_score;
  if (_payload != NULL) delete _payload;
  _payload = ch->_payload;
  _maxChildScore = ch->_maxChildScore;
  _flags = ch->_flags;
  _children.swap(ch->_children);

  delete ch;
}

//---------------------------------------------------------------------------------------------

void TrieNode::Print(int idx, int depth) {
  for (int i = 0; i < depth; ++i) {
    printf("  ");
  }
  printf("%d) Score %f, max ChildScore %f\n", idx, _score, _maxChildScore);
  int i = 0;
  ++depth;
  for (auto const &child: _children) {
    child->Print(i++, depth);
  }
}

//---------------------------------------------------------------------------------------------

// Add a new string to a trie and return the node.

TrieNode *TrieNode::Add(const Runes &runes, RSPayload *payload, float score, TrieAddOp op) {
  if (score == 0 || runes.empty()) {
    return NULL;
  }

  int offset = 0;
  for (; offset < runes.len() && offset < _len; ++offset) {
    if (runes[offset] != _runes[offset]) {
      break;
    }
  }
  // we broke off before the end of the string
  if (offset < _len) {
    // split the node and create 2 child nodes:
    // 1. a child representing the new string from the diverted offset onwards
    // 2. a child representing the old node's suffix from the diverted offset
    //    and the old children
    SplitNode(offset);
    // the new string matches the split node exactly!
    // we simply turn the splitted node, which is now nonterminal, into a terminal node
    if (offset == runes.len()) {
      _score = score;
      _flags |= TRIENODE_TERMINAL;
      TrieNode *newChild = _children.back();
      if (_payload != NULL) {
        delete _payload;
        _payload = NULL;
      }
      if (payload != NULL && payload->data != NULL && payload->len > 0) {
        _payload = new TriePayload(payload->data, payload->len);
      }

      _children.emplace_back(newChild);
    } else {
      // we add a child
      AddChild(runes, offset, payload, score);
      _maxChildScore = MAX(_maxChildScore, score);
    }
    return this;
  }

  _maxChildScore = MAX(_maxChildScore, score);

  // we're inserting in an existing node - just replace the value
  if (offset == runes.len()) {
    switch (op) {
      // in increment mode, just add the score to the node's score
      case ADD_INCR:
        _score += score;
        break;

      // by default we just replace the score
      case ADD_REPLACE:
      default:
        _score = score;
    }
    if (_payload != NULL) {
      delete _payload;
      _payload = NULL;
    }
    if (payload != NULL && payload->data != NULL && payload->len > 0) {
      _payload = new TriePayload(payload->data, payload->len);
    }

    _flags |= TRIENODE_TERMINAL; // set the node as terminal
    _flags &= ~TRIENODE_DELETED; // if it was deleted, make sure it's not now
    return this;
  }

  // proceed to the next child or add a new child for the current rune
  for (auto &child: _children) {
    if (runes[offset] == child->_runes[0]) {
      child = child->Add(runes[offset], payload, score, op);
      return this;
    }
  }

  return AddChild(runes, offset, payload, score);
}

//---------------------------------------------------------------------------------------------

// Find the entry with a given string and length, and return its score.
// Returns 0 if the entry was not found.
// Note that you cannot put entries with zero score.

float TrieNode::Find(const Runes &runes) const {
  const TrieNode *n = this;
  t_len offset = 0;
  while (n && offset < runes.len()) {
    t_len localOffset = 0;
    for (; offset < runes.len() && localOffset < runes.len(); offset++, localOffset++) {
      if (runes[offset] != n->_runes[localOffset]) {
        break;
      }
    }

    if (offset == runes.len()) {
      // we're at the end of both strings!
      if (localOffset == n->_len) return n->isDeleted() ? 0 : n->_score;
    } else if (localOffset == n->_len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      t_len i = 0;
      TrieNode *nextChild = NULL;
      for (auto child: n->_children) {
        if (&runes[offset] == child->_runes[0]) {
          nextChild = child;
          break;
        }
      }

      // we couldn't find a matching child
      n = nextChild;
    } else {
      return 0;
    }
  }

  return 0;
}

//---------------------------------------------------------------------------------------------

// Optimize the node and its children:
// 1. If a child should be deleted - delete it and reduce the child count
// 2. If a child has a single child - merge them
// 3. recalculate the max child score

void TrieNode::optimizeChildren() {
  _maxChildScore = _score;

  // free deleted terminal nodes
  for (auto child_i = _children.begin(); child_i != _children.end(); ++child_i) {
    auto *child = *child_i;
    // if this is a deleted node with no children - remove it
    if (child->_children.empty() && child->isDeleted()) {
      child_i = _children.erase(child_i);
      child = *child_i;

      // fix _maxChildScore
      for (auto child_j = child_i; child_j != _children.end(); ++child_j) {
        _maxChildScore = MAX(_maxChildScore, (*child_j)->_maxChildScore);
      }
    } else {
      // this node is ok!
      // if needed - merge this node with its single child
      if (child->_children.size() == 1) {
        child->MergeWithSingleChild();
      }
      _maxChildScore = MAX(_maxChildScore, child->_maxChildScore);
    }
  }

  sortChildren();
}

//---------------------------------------------------------------------------------------------

// Mark a node as deleted. For simplicity for now we don't actually delete anything,
// but the node will not be persisted to disk, thus deleted after reload.
// Returns true if the node was indeed deleted, false otherwise.

bool TrieNode::Delete(const Runes &runes) {
  TrieNode *n = this;
  t_len offset = 0;
  static TrieNode *stack[TRIE_INITIAL_STRING_LEN]; //@@ static?!
  int stackPos = 0;
  bool rc = false;
  while (n && offset < runes.len()) {
    stack[stackPos++] = n;
    t_len localOffset = 0;
    for (; offset < runes.len() && localOffset < runes.len(); offset++, localOffset++) {
      if (runes[offset] != n->_runes[localOffset]) {
        break;
      }
    }

    if (offset == runes.len()) {
      // we're at the end of both strings!
      // this means we've found what we're looking for
      if (localOffset == runes.len()) {
        if (!n->isDeleted() && n->isTerminal()) {
          n->_flags |= TRIENODE_DELETED;
          n->_flags &= ~TRIENODE_TERMINAL;
          n->_score = 0;
          rc = true;
        }
        goto end;
      }
    } else if (localOffset == n->_len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      t_len i = 0;
      TrieNode *nextChild = NULL;
      for (auto child: n->_children) {
        if (runes[offset] == child->_runes[0]) {
          nextChild = child;
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
    stack[stackPos]->optimizeChildren();
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

// Free the trie's root and all its children recursively

TrieNode::~TrieNode() {
  for (auto child: _children) {
    delete child;
  }
  delete _payload;
}

//---------------------------------------------------------------------------------------------

// Sort the children of a node by their maxChildScore

void TrieNode::sortChildren() {
  if (_sortmode == TRIENODE_SORTED_SCORE && _children.size() > 1) {
    std::sort(_children.begin(), _children.end(), [&](const TrieNode *a, const TrieNode *b) {
        return a->_maxChildScore < b->_maxChildScore;
      });
  }
  _sortmode = TRIENODE_SORTED_SCORE;
}

///////////////////////////////////////////////////////////////////////////////////////////////

TrieNode *TrieNode::RandomWalk(int minSteps, Runes &runes) {
  // create an iteration stack we walk up and down
  minSteps = MAX(minSteps, 4);

  // create an iteration stack we walk up and down
  Vector<TrieNode*> stack;
  stack.push_back(this);

  TrieNode *res;

  int steps = 0;
  while (steps < minSteps || !stack.back()->isTerminal()) {
    res = stack.back();

    // select the next step - -1 means walk back up one level
    int rnd = (rand() % (res->_children.size() + 1)) - 1;
    if (rnd == -1) {
      // we can't walk up the top level
      if (stack.size() > 1) {
        steps++;
        stack.pop_back();
      }
      continue;
    }

    // Push a child on the stack
    TrieNode *child = res->_children[rnd];
    stack.push_back(child);
    res = child;
    steps++;
  }

  // Return the node at the top of the stack
  res = stack.back();

  for (auto n1: stack) {
    runes.append(n1->_runes);
  }

  return res;
}

///////////////////////////////////////////////////////////////////////////////////////////////

struct rsbHelper {
  const rune *r;
  uint16_t n;
};

//---------------------------------------------------------------------------------------------

static int rsbCompareCommon(const void *h, const void *e, bool prefix) {
  const rsbHelper *term = (const rsbHelper *) h;
  const TrieNode *elem = *(const TrieNode **) e;
  int rc;

  if (prefix) {
    size_t minLen = MIN(elem->_len, term->n);
    rc = runecmp(term->r, minLen, &elem->_runes[0], minLen);
  } else {
    rc = runecmp(term->r, term->n, &elem->_runes[0], elem->_len);
  }

  return rc;
}

//---------------------------------------------------------------------------------------------

static int rsbCompareExact(const void *h, const void *e) {
  return rsbCompareCommon(h, e, false);
}

//---------------------------------------------------------------------------------------------

static int rsbComparePrefix(const void *h, const void *e) {
  return rsbCompareCommon(h, e, true);
}

///////////////////////////////////////////////////////////////////////////////////////////////

void TrieNode::rangeIterateSubTree(TrieRange *r) {
  // Push string to stack
  r->buf = array_ensure_append(r->buf, &_runes[0], _len, rune);

  if (isTerminal()) {
    r->callback(r->buf, array_len(r->buf), r->cbctx);
  }

  for (auto child: _children) {
    child->rangeIterateSubTree(r);
  }

  array_trimm_len(r->buf, array_len(r->buf) - _len);
}

//---------------------------------------------------------------------------------------------

// Try to place as many of the common arguments in TrieRange, so that the stack
// size is not negatively impacted and prone to attack.

void TrieNode::rangeIterate(const rune *min, int nmin, const rune *max, int nmax, TrieRange *r) {
  int endIdx, beginIdx = 0, endEqIdx = -1, beginEqIdx = -1;

  // Push string to stack
  r->buf = array_ensure_append(r->buf, &_runes[0], _len, rune);

  if (isTerminal()) {
    // current node is a termina.
    // if nmin or nmax is zero, it means that we find an exact match
    // we should fire the callback only if exact match requested
    if (r->includeMin && nmin == 0) {
      r->callback(r->buf, array_len(r->buf), r->cbctx);
    } else if (r->includeMax && nmax == 0) {
      r->callback(r->buf, array_len(r->buf), r->cbctx);
    }
  }

  size_t arrlen = _children.size();
  if (!_children.empty()) {
    // no children, just return
    goto clean_stack;
  }

  if (_sortmode != TRIENODE_SORTED_LEX) {
    // lex sorting the children array
    std::sort(_children.begin(), _children.end(), [&](const TrieNode *a, const TrieNode *b) {
        return a->_runes < b->_runes;
      });
    _sortmode = TRIENODE_SORTED_LEX;
  }

  // Find the minimum range here
  // Use binary search to find the beginning and end ranges
  rsbHelper h;

  if (nmin > 0) {
    // searching for node that matches the prefix of our min value
    h.r = min;
    h.n = nmin;
    beginEqIdx = rsb_eq_vec(_children, &h, rsbComparePrefix);
  }

  if (nmax > 0) {
    // searching for node that matches the prefix of our max value
    h.r = max;
    h.n = nmax;
    endEqIdx = rsb_eq_vec(_children, &h, rsbComparePrefix);
  }

  if (beginEqIdx == endEqIdx && endEqIdx != -1) {
    // special case, min value and max value share a command prefix.
    // we need to call recursively with the child contains this prefix
    TrieNode *child = _children[beginEqIdx];

    const rune *nextMin = min + child->_len;
    int nNextMin = nmin - child->_len;
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = NULL;
    }

    const rune *nextMax = max + child->_len;
    int nNextMax = nmax - child->_len;
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = NULL;
    }

    child->rangeIterate(nextMin, nNextMin, nextMax, nNextMax, r);
    goto clean_stack;
  }

  if (beginEqIdx != -1) {
    // we find a child that matches min prefix
    // we should continue the search on this child but at this point we should
    // not limit the max value
    TrieNode *child = _children[beginEqIdx];

    const rune *nextMin = min + child->_len;
    int nNextMin = nmin - child->_len;
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = NULL;
    }

    child->rangeIterate(nextMin, nNextMin, NULL, -1, r);
  }

  if (nmin > 0) {
    // search for the first element which are greater then our min value
    h.r = min;
    h.n = nmin;
    beginIdx = rsb_gt_vec(_children, &h, rsbCompareExact);
  }

  endIdx = nmax ? arrlen - 1 : -1;
  if (nmax > 0) {
    // search for the first element which are less then our max value
    h.r = max;
    h.n = nmax;
    endIdx = rsb_lt_vec(_children, &h, rsbCompareExact);
  }

  // we need to iterate (without any checking) on all the subtree from beginIdx to endIdx
  for (int ii = beginIdx; ii <= endIdx; ++ii) {
    _children[ii]->rangeIterateSubTree(r);
  }

  if (endEqIdx != -1) {
    // we find a child that matches max prefix
    // we should continue the search on this child but at this point we should
    // not limit the min value
    TrieNode *child = _children[endEqIdx];

    const rune *nextMax = max + child->_len;
    int nNextMax = nmax - child->_len;
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = NULL;
    }

    child->rangeIterate(NULL, -1, nextMax, nNextMax, r);
  }

clean_stack:
  array_trimm_len(r->buf, array_len(r->buf) - _len);
}

//---------------------------------------------------------------------------------------------

/**
 * Iterate all nodes within range.
 * @param n the node to iterateo
 * @param min the minimum lexical string to check from
 * @param minlen the length of min
 * @param max the maximum lexical string to check until
 * @param maxlen the maximum length of the max
 * @param callback the callback to invoke
 * @param ctx data to be passed to the callback
 */

void TrieNode::IterateRange(const Runes &min, bool includeMin, const Runes &max, bool includeMax,
    TrieRangeCallback callback, void *ctx) {
  if (min > max) {
    // min > max, no reason to continue
    return;
  }

  size_t nmin = min.len();
  size_t nmax = max.len();
  if (min == max) {
    // min = max, we should just search for min and check for its existence
    if (includeMin || includeMax) {
      if (Find(min) != 0) {
        callback(min._runes, nmin, ctx);
      }
    }
    return;
  }

  // min < max we should start the scan
  TrieRange r = {
      .callback = callback,
      .cbctx = ctx,
      .includeMin = includeMin,
      .includeMax = includeMax,
  };
  r.buf = array_new(rune, TRIE_INITIAL_STRING_LEN);
  rangeIterate(min._runes, nmin, max._runes, nmax, &r);
  array_free(r.buf);
}

///////////////////////////////////////////////////////////////////////////////////////////////
