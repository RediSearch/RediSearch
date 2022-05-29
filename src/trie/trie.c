#include <sys/param.h>
#include "trie.h"
#include "util/bsearch.h"
#include "sparse_vector.h"
#include "redisearch.h"
#include "util/arr.h"

size_t __trieNode_Sizeof(t_len numChildren, t_len slen) {
  return sizeof(TrieNode) + numChildren * sizeof(TrieNode *) + sizeof(rune) * (slen + 1);
}

TriePayload *triePayload_New(const char *payload, uint32_t plen);

// Allocate a new trie payload struct
inline TriePayload *triePayload_New(const char *payload, uint32_t plen) {

  TriePayload *p = rm_malloc(sizeof(TriePayload) + sizeof(char) * (plen + 1));
  p->len = plen;
  memcpy(p->data, payload, sizeof(char) * plen);
  return p;
}

/* Add a child node to the parent node n, with a string str starting at offset
up until len, and a
given score */

TrieNode::TrieNode(rune *str_, t_len offset, t_len len_, const char *payload, size_t plen,
                   t_len numChildren_, float score_, bool terminal) {
  len = len_ - offset;
  numChildren = numChildren_;
  score = score_;
  flags = 0 | (terminal ? TRIENODE_TERMINAL : 0);
  maxChildScore = 0;
  sortmode = TRIENODE_SORTED_NONE;
  memcpy(str, str_ + offset, sizeof(rune) * (len_ - offset));
  if (payload != NULL && plen > 0) {
    payload = triePayload_New(payload, plen);
  }
}

//@@ Can this be a void func?
TrieNode *TrieNode::AddChild(rune *str_, t_len offset, t_len len_, RSPayload *payload, float score_) {
  numChildren++;
  this = rm_realloc((void *)this, __trieNode_Sizeof(numChildren, len));
  // a newly added child must be a terminal node
  TrieNode *child(str_, offset, len_, payload ? payload->data : NULL,
                  payload ? payload->len : 0, 0, score_, 1);
  children()[numChildren - 1] = child;
  sortmode = TRIENODE_SORTED_NONE;

  return this;
}

/* Split node n at string offset n. This returns a new node which has a string
 * up until offset, and
 * a single child holding The old score of n, and its score */
//@@ Can this be a void func?
TrieNode *TrieNode::SplitNode(t_len offset) {
  // Copy the current node's data and children to a new child node
  TrieNode *newChild(str, offset, len, payload ? payload->data : NULL,
                     payload ? payload->len : 0, numChildren, score, isTerminal());
  newChild->maxChildScore = maxChildScore;
  newChild->flags = flags;
  TrieNode **children = children();
  TrieNode **newChildren = newChild->children();
  memcpy(newChildren, children, sizeof(TrieNode *) * numChildren);

  // reduce the node to be just one child long with no score
  numChildren = 1;
  len = offset;
  score = 0;
  // the parent node is now non terminal and non sorted
  flags &= ~(TRIENODE_TERMINAL | TRIENODE_DELETED);
  sortmode = TRIENODE_SORTED_NONE;

  maxChildScore = MAX(maxChildScore, newChild->score);
  if (payload != NULL) {
    delete payload;
  }
  //@@ should we realloc?
  this = rm_realloc(this, __trieNode_Sizeof(numChildren, len));
  children()[0] = newChild;

  return this;
}

/* If a node has a single child after delete, we can merged them. This deletes
 * the node and returns a newly allocated node */
TrieNode *TrieNode::MergeWithSingleChild() {
  if (isTerminal() || numChildren != 1) {
    return this;
  }
  TrieNode *ch = *children();

  // Copy the current node's data and children to a new child node
  rune nstr[len + ch->len + 1];
  memcpy(nstr, str, sizeof(rune) * len);
  memcpy(&nstr[len], ch->str, sizeof(rune) * ch->len);
  TrieNode *merged(
      nstr, 0, len + ch->len, ch->payload ? ch->payload->data : NULL,
      ch->payload ? ch->payload->len : 0, ch->numChildren, ch->score, ch->isTerminal());
  merged->maxChildScore = ch->maxChildScore;
  merged->numChildren = ch->numChildren;
  merged->flags = ch->flags;
  TrieNode **children = ch->children();
  TrieNode **newChildren = merged->children();
  memcpy(newChildren, children, sizeof(TrieNode *) * merged->numChildren);
  if (ch->payload) {
    delete ch->payload;
  }
  if (payload != NULL) {
    delete payload;
  }
  rm_free(this);
  rm_free(ch);

  return merged;
}

void TrieNode::Print(int idx, int depth) {
  for (int i = 0; i < depth; i++) {
    printf("  ");
  }
  printf("%d) Score %f, max ChildScore %f\n", idx, score, maxChildScore);
  for (int i = 0; i < numChildren; i++) {
    Print(children()[i], i, depth + 1);
  }
}

/* Add a new string to a trie. Returns 1 if the string did not exist there, or 0
 * if we just replaced
 * the score. We pass a pointer to the node because it may actually change when
 * splitting */

bool TrieNode::Add(rune *str_, t_len len_, RSPayload *payload, float score_, TrieAddOp op) {
  if (score_ == 0 || len_ == 0) {
    return false;
  }

  TrieNode *n = *this;

  int offset = 0;
  for (; offset < len_ && offset < n->len; offset++) {
    if (str_[offset] != n->str[offset]) {
      break;
    }
  }
  // we broke off before the end of the string
  if (offset < n->len) {
    // split the node and create 2 child nodes:
    // 1. a child representing the new string from the diverted offset onwards
    // 2. a child representing the old node's suffix from the diverted offset
    // and the old children
    n = n->SplitNode(offset);
    // the new string matches the split node exactly!
    // we simply turn the split node, which is now non terminal, into a terminal
    // node
    if (offset == len_) {
      n->score = score_;
      n->flags |= TRIENODE_TERMINAL;
      TrieNode *newChild = children()[0];
      n = rm_realloc(n, __trieNode_Sizeof(n->numChildren, n->len));
      if (n->payload != NULL) {
        deletet n->payload;
      }
      if (payload != NULL && payload->data != NULL && payload->len > 0) {
        n->payload = triePayload_New(payload->data, payload->len);
      }

      children()[0] = newChild;
    } else {
      // we add a child
      n = n->AddChild(str_, offset, len_, payload, score_);
      n->maxChildScore = MAX(n->maxChildScore, score_);
    }
    *this = n;
    return true;
  }

  n->maxChildScore = MAX(n->maxChildScore, score_);

  // we're inserting in an existing node - just replace the value
  if (offset == len_) {
    bool term = isTerminal();
    bool deleted = isDeleted();
    switch (op) {
      // in increment mode, just add the score to the node's score
      case ADD_INCR:
        n->score += score_;
        break;

      // by default we just replace the score
      case ADD_REPLACE:
      default:
        n->score = score_;
    }
    if (n->payload != NULL) {
      delete n->payload;
    }
    if (payload != NULL && payload->data != NULL && payload->len > 0) {
      n->payload = triePayload_New(payload->data, payload->len);
    }
    // set the node as terminal
    n->flags |= TRIENODE_TERMINAL;
    // if it was deleted, make sure it's not now
    n->flags &= ~TRIENODE_DELETED;
    *this = n;
    return (term && !deleted) ? false : true;
  }

  // proceed to the next child or add a new child for the current rune
  for (t_len i = 0; i < n->numChildren; i++) {
    TrieNode *child = n->children()[i];
    if (str_[offset] == child->str[0]) {
      int rc = child.Add(str_ + offset, len_ - offset, payload, score_, op);
      n->children()[i] = child;
      return rc;
    }
  }
  *this = n->AddChild(str_, offset, len_, payload, score_);
  return true;
}

/* Find the entry with a given string and length, and return its score. Returns
 * 0 if the entry was
 * not found.
 * Note that you cannot put entries with zero score */

float TrieNode::Find(rune *str_, t_len len_) {
  t_len offset = 0;
  while (offset < len_) {
    // printf("n %.*s offset %d, len %d\n", len, str, offset,
    // len);
    t_len localOffset = 0;
    for (; offset < len_ && localOffset < len; offset++, localOffset++) {
      if (str_[offset] != str[localOffset]) {
        break;
      }
    }

    if (offset == len_) {
      // we're at the end of both strings!
      if (localOffset == len) return isDeleted() ? 0 : score;

    } else if (localOffset == len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      t_len i = 0;
      TrieNode *nextChild;
      for (; i < numChildren; i++) {
        TrieNode *child = children()[i];

        if (str_[offset] == child->str[0]) {
          nextChild = child;
          break;
        }
      }

      // we couldn't find a matching child
      this = nextChild;

    } else {
      return 0;
    }
  }

  return 0;
}

/* Optimize the node and its children:
 *   1. If a child should be deleted - delete it and reduce the child count
 *   2. If a child has a single child - merge them
 *   3. recalculate the max child score
 */

void TrieNode::optimizeChildren() {
  int i = 0;
  TrieNode **nodes = children();
  maxChildScore = score;
  // free deleted terminal nodes
  while (i < numChildren) {

    // if this is a deleted node with no children - remove it
    if (nodes[i]->numChildren == 0 && nodes[i]->isDeleted()) {
      delete nodes[i];

      nodes[i] = NULL;
      // just "fill" the hole with the next node up
      while (i < numChildren - 1) {
        nodes[i] = nodes[i + 1];
        maxChildScore = MAX(maxChildScore, nodes[i]->maxChildScore);
        i++;
      }
      // reduce child count
      numChildren--;
    } else {

      // this node is ok!
      // if needed - merge this node with it its single child
      if (nodes[i] && nodes[i]->numChildren == 1) {
        nodes[i] = nodes[i]->MergeWithSingleChild();
      }
      maxChildScore = MAX(maxChildScore, nodes[i]->maxChildScore);
    }
    i++;
  }

  sortChildren();
}

/* Mark a node as deleted. For simplicity for now we don't actually delete
 * anything,
 * but the node will not be persisted to disk, thus deleted after reload.
 * Returns 1 if the node was indeed deleted, 0 otherwise */

int TrieNode::Delete(rune *str_, t_len len_) {
  t_len offset = 0;
  static TrieNode *stack[TRIE_INITIAL_STRING_LEN];
  int stackPos = 0;
  int rc = 0;
  while (offset < len_) {
    stack[stackPos++] = this;
    t_len localOffset = 0;
    for (; offset < len_ && localOffset < len; offset++, localOffset++) {
      if (str_[offset] != str[localOffset]) {
        break;
      }
    }

    if (offset == len_) {
      // we're at the end of both strings!
      // this means we've found what we're looking for
      if (localOffset == len) {
        if (!isDeleted() && isTerminal()) {

          flags |= TRIENODE_DELETED;
          flags &= ~TRIENODE_TERMINAL;
          score = 0;
          rc = 1;
        }
        goto end;
      }
    } else if (localOffset == len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      t_len i = 0;
      TrieNode *nextChild;
      for (; i < numChildren; i++) {
        TrieNode *child = children()[i];

        if (str_[offset] == child->str[0]) {
          nextChild = child;
          break;
        }
      }

      // we couldn't find a matching child
      this = nextChild;
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

// Free the trie's root and all its children recursively
TrieNode::~TrieNode() {
  for (t_len i = 0; i < numChildren; i++) {
    delete children()[i];
  }
  if (payload != NULL) {
    delete payload;
  }
}

// comparator for node sorting by child max score
static int TrieNode::Cmp(const void *p1, const void *p2) {
  TrieNode *n1 = *(TrieNode **)p1;
  TrieNode *n2 = *(TrieNode **)p2;

  if (n1->maxChildScore < n2->maxChildScore) {
    return 1;
  } else if (n1->maxChildScore > n2->maxChildScore) {
    return -1;
  }
  return 0;
}

// Sort the children of a node by their maxChildScore
void TrieNode::sortChildren() {
  if (!(sortmode != TRIENODE_SORTED_SCORE) && numChildren > 1) {
    qsort(children(), numChildren, sizeof(TrieNode *), Cmp);
  }
  sortmode = TRIENODE_SORTED_SCORE;
}

// Push a new trie node on the iterator's stack
inline void TrieIterator::Push(TrieNode *node, int skipped) {
  if (stackOffset < TRIE_INITIAL_STRING_LEN - 1) {
    stackNode *sn = &stack[stackOffset++];
    sn->childOffset = 0;
    sn->stringOffset = 0;
    sn->isSkipped = skipped;
    sn->n = node;
    sn->state = ITERSTATE_SELF;
  }
}

// pop a node from the iterator's stcak
inline void TrieIterator::Pop() {
  if (stackOffset > 0) {
    stackNode *current = current();
    if (popCallback) {
      popCallback(ctx, current->stringOffset);
    }

    bufOffset -= current->stringOffset;
    --stackOffset;
  }
}

/* Single step iteration, feeding the given filter/automaton with the next
 * character */

inline int TrieIterator::step(void *matchCtx) {
  if (stackOffset == 0) {
    return __STEP_STOP;
  }

  stackNode *current = current();

  int matched = 0;
  // printf("[%.*s]current %p (%.*s %f), state %d, string offset %d/%d, child
  // offset %d/%d\n",
  //        bufOffset, buf, current, current->n->len, current->n->str,
  //        current->n->score, current->state, current->stringOffset,
  //        current->n->len,
  //        current->childOffset, current->n->numChildren);
  switch (current->state) {
    case ITERSTATE_MATCH:
      Pop();
      goto next;

    case ITERSTATE_SELF:

      if (current->stringOffset < current->n->len) {
        // get the current rune to feed the filter
        rune b = current->n->str[current->stringOffset];

        if (filter) {
          // run the next character in the filter
          FilterCode rc = filter(b, ctx, &matched, matchCtx);

          // if we should stop...
          if (rc == F_STOP) {
            // match stop - change the state to MATCH and return
            if (matched) {
              current->state = ITERSTATE_MATCH;
              return __STEP_MATCH;
            }
            // normal stop - just pop and continue
            Pop();
            goto next;
          }
        }

        // advance the buffer offset and character offset
        buf[bufOffset++] = b;
        current->stringOffset++;

        // if we don't have a filter, a "match" is when we reach the end of the
        // node
        if (!filter) {
          if (current->n->len > 0 && current->stringOffset == current->n->len &&
              current->n->isTerminal() && !current->n->isDeleted()) {
            matched = 1;
          }
        }

        return matched ? __STEP_MATCH : __STEP_CONT;
      } else {
        // switch to "children mode"
        current->state = ITERSTATE_CHILDREN;
      }

    case ITERSTATE_CHILDREN:
    default:
      if (current->n->sortmode != TRIENODE_SORTED_SCORE) {
        current->n->sortChildren();
      }
      // push the next child
      if (current->childOffset < current->n->numChildren) {
        TrieNode *ch = current->n->children()[current->childOffset++];
        if (ch->maxChildScore >= minScore || ch->score >= minScore) {
          Push(ch, 0);
          nodesConsumed++;
        } else {
          //Push(ch, 1);
          nodesSkipped++;
        }
      } else {
        // at the end of the node - pop and go up
        Pop();
      }
  }

next:
  return __STEP_CONT;
}

/* Iterate the tree with a step filter, which tells the iterator whether to
 * continue down the trie
 * or not. This can be a levenshtein automaton, a regex automaton, etc. A NULL
 * filter means just
 * continue iterating the entire trie. ctx is the filter's context */

TrieIterator *TrieNode::Iterate(StepFilter f, StackPopCallback pf, void *ctx_) {
  TrieIterator *it;
  it->filter = f;
  it->popCallback = pf;
  it->minScore = 0;
  it->ctx = ctx_;
  it->Push(this, 0);

  return it;
}

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done
 * and should exit */

bool TrieIterator::Next(rune **ptr, t_len *len, RSPayload *payload, float *score, void *matchCtx) {
  int rc;
  while ((rc = step(matchCtx)) != __STEP_STOP) {
    if (rc == __STEP_MATCH) {
      stackNode *sn = current();

      if (sn->n->isTerminal() && sn->n->len == sn->stringOffset && !sn->n->isDeleted()) {
        *ptr = buf;
        *len = bufOffset;
        *score = sn->n->score;
        if (payload != NULL) {
          if (sn->n->payload != NULL) {
            payload->data = sn->n->payload->data;
            payload->len = sn->n->payload->len;
          } else {
            payload->data = NULL;
            payload->len = 0;
          }
        }
        return true;
      }
    }
  }

  return false;
}

TrieNode *TrieNode::RandomWalk(int minSteps, rune **str_, t_len *len_) {
  // create an iteration stack we walk up and down
  minSteps = MAX(minSteps, 4);

  size_t stackCap = minSteps;
  size_t stackSz = 1;
  TrieNode **stack;
  stack[0] = this;

  int bufCap = len;

  int steps = 0;

  while (steps < minSteps || !stack[stackSz-1]->isTerminal()) {

    this = stack[stackSz - 1];

    /* select the next step - -1 means walk back up one level */
    int rnd = (rand() % (numChildren + 1)) - 1;
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
    TrieNode *child = children()[rnd];
    stack[stackSz++] = child;

    steps++;
    if (stackSz == stackCap) {
      stackCap += minSteps;
      stack = rm_realloc(stack, stackCap * sizeof(TrieNode *));
    }

    bufCap += child->len;
  }

  /* Return the node at the top of the stack */

  this = stack[stackSz - 1];

  /* build the string by walking the stack and copying all node strings */
  rune *buf;

  t_len bufSize = 0;
  for (size_t i = 0; i < stackSz; i++) {
    memcpy(&buf[bufSize], stack[i]->str, sizeof(rune) * stack[i]->len);
    bufSize += stack[i]->len;
  }

  *str_ = buf;
  *len_ = bufSize;

  return this;
}

static int runecmp(const rune *sa, size_t na, const rune *sb, size_t nb) {
  size_t minlen = MIN(na, nb);
  for (size_t ii = 0; ii < minlen; ++ii) {
    int rc = sa[ii] - sb[ii];
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

static int cmpLexFull(const void *a, const void *b) {
  const TrieNode *na = *(const TrieNode **)a, *nb = *(const TrieNode **)b;
  return runecmp(na->str, na->len, nb->str, nb->len);
}

struct rsbHelper {
  const rune *r;
  uint16_t n;
};

static int rsbCompareCommon(const void *h, const void *e, int prefix) {
  const rsbHelper *term = h;
  const TrieNode *elem = *(const TrieNode **)e;
  int rc;

  if (prefix) {
    size_t minLen = MIN(elem->len, term->n);
    rc = runecmp(term->r, minLen, elem->str, minLen);
  } else {
    rc = runecmp(term->r, term->n, elem->str, elem->len);
  }

  return rc;
}

static int rsbCompareExact(const void *h, const void *e) {
  return rsbCompareCommon(h, e, 0);
}

static int rsbComparePrefix(const void *h, const void *e) {
  return rsbCompareCommon(h, e, 1);
}

inline void TrieNode::rangeIterateSubTree(RangeCtx *r) {
  // Push string to stack
  r->buf = array_ensure_append(r->buf, str, len, rune);

  if (isTerminal()) {
    r->callback(r->buf, array_len(r->buf), r->cbctx);
  }

  TrieNode **arr = children();

  for (size_t ii = 0; ii < numChildren; ++ii) {
    // printf("Descending to index %lu\n", ii);
    arr[ii]->rangeIterateSubTree(r);
  }

  array_trimm_len(r->buf, array_len(r->buf) - len);
}

/**
 * Try to place as many of the common arguments in rangectx, so that the stack
 * size is not negatively impacted and prone to attack.
 */
inline void TrieNode::rangeIterate(const rune *min, int nmin, const rune *max, int nmax, RangeCtx *r) {
  // Push string to stack
  r->buf = array_ensure_append(r->buf, str, len, rune);

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

  TrieNode **arr = children();
  size_t arrlen = numChildren;
  if (!arrlen) {
    // no children, just return.
    goto clean_stack;
  }

  if (sortmode != TRIENODE_SORTED_LEX) {
    // lex sorting the children array.
    qsort(arr, arrlen, sizeof(*arr), cmpLexFull);
    sortmode = TRIENODE_SORTED_LEX;
  }

  // Find the minimum range here..
  // Use binary search to find the beginning and end ranges:
  rsbHelper h;

  int beginEqIdx = -1;
  if (nmin > 0) {
    // searching for node that matches the prefix of our min value
    h.r = min;
    h.n = nmin;
    beginEqIdx = rsb_eq(arr, arrlen, sizeof(*arr), &h, rsbComparePrefix);
  }

  int endEqIdx = -1;
  if (nmax > 0) {
    // searching for node that matches the prefix of our max value
    h.r = max;
    h.n = nmax;
    endEqIdx = rsb_eq(arr, arrlen, sizeof(*arr), &h, rsbComparePrefix);
  }

  if (beginEqIdx == endEqIdx && endEqIdx != -1) {
    // special case, min value and max value share a command prefix.
    // we need to call recursively with the child contains this prefix
    TrieNode *child = arr[beginEqIdx];

    const rune *nextMin = min + child->len;
    int nNextMin = nmin - child->len;
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = NULL;
    }

    const rune *nextMax = max + child->len;
    int nNextMax = nmax - child->len;
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
    TrieNode *child = arr[beginEqIdx];

    const rune *nextMin = min + child->len;
    int nNextMin = nmin - child->len;
    if (nNextMin < 0) {
      nNextMin = 0;
      nextMin = NULL;
    }

    child->rangeIterate(nextMin, nNextMin, NULL, -1, r);
  }

  int beginIdx = 0;
  if (nmin > 0) {
    // search for the first element which are greater then our min value
    h.r = min;
    h.n = nmin;
    beginIdx = rsb_gt(arr, arrlen, sizeof(*arr), &h, rsbCompareExact);
  }

  int endIdx = nmax ? arrlen - 1 : -1;
  if (nmax > 0) {
    // search for the first element which are less then our max value
    h.r = max;
    h.n = nmax;
    endIdx = rsb_lt(arr, arrlen, sizeof(*arr), &h, rsbCompareExact);
  }

  // we need to iterate (without any checking) on all the subtree from beginIdx to endIdx
  for (int ii = beginIdx; ii <= endIdx; ++ii) {
    arr[ii]->rangeIterateSubTree(r);
  }

  if (endEqIdx != -1) {
    // we find a child that matches max prefix
    // we should continue the search on this child but at this point we should
    // not limit the min value
    TrieNode *child = arr[endEqIdx];

    const rune *nextMax = max + child->len;
    int nNextMax = nmax - child->len;
    if (nNextMax < 0) {
      nNextMax = 0;
      nextMax = NULL;
    }

    child->rangeIterate(NULL, -1, nextMax, nNextMax, r);
  }

clean_stack:
  array_trimm_len(r->buf, array_len(r->buf) - len);
}

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

void TrieNode::IterateRange(const rune *min, int nmin, bool includeMin, const rune *max,
                            int nmax, bool includeMax, TrieRangeCallback callback, void *ctx) {
  if (min && max) {
    // min and max exists, lets compare them to make sure min < max
    int cmp = runecmp(min, nmin, max, nmax);
    if (cmp > 0) {
      // min > max, no reason to continue
      return;
    }

    if (cmp == 0) {
      // min = max, we should just search for min and check for its existence
      if (includeMin || includeMax) {
        if (Find((rune *)min, nmin) != 0) {
          callback(min, nmin, ctx);
        }
      }
      return;
    }
  }

  // min < max we should start the scan
  RangeCtx r = {
      .callback = callback,
      .cbctx = ctx,
      .includeMin = includeMin,
      .includeMax = includeMax,
  };
  r.buf = array_new(rune, TRIE_INITIAL_STRING_LEN);
  rangeIterate(min, nmin, max, nmax, &r);
  array_free(r.buf);
}
