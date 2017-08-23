#include <sys/param.h>
#include "trie.h"
#include "sparse_vector.h"
#include "redisearch.h"

size_t __trieNode_Sizeof(t_len numChildren, t_len slen) {
  return sizeof(TrieNode) + numChildren * sizeof(TrieNode *) + sizeof(rune) * (slen + 1);
}

TriePayload *triePayload_New(const char *payload, uint32_t plen);

// Allocate a new trie payload struct
inline TriePayload *triePayload_New(const char *payload, uint32_t plen) {

  TriePayload *p = malloc(sizeof(TriePayload) + sizeof(char) * (plen + 1));
  p->len = plen;
  memcpy(p->data, payload, sizeof(char) * plen);
  return p;
}

TrieNode *__newTrieNode(rune *str, t_len offset, t_len len, const char *payload, size_t plen,
                        t_len numChildren, float score, int terminal) {
  TrieNode *n = calloc(1, __trieNode_Sizeof(numChildren, len - offset));
  n->len = len - offset;
  n->numChildren = numChildren;
  n->score = score;
  n->flags = 0 | (terminal ? TRIENODE_TERMINAL : 0);
  n->maxChildScore = 0;
  memcpy(n->str, str + offset, sizeof(rune) * (len - offset));
  if (payload != NULL && plen > 0) {
    n->payload = triePayload_New(payload, plen);
  }
  return n;
}

TrieNode *__trie_AddChild(TrieNode *n, rune *str, t_len offset, t_len len, RSPayload *payload,
                          float score) {
  n->numChildren++;
  n = realloc((void *)n, __trieNode_Sizeof(n->numChildren, n->len));
  // a newly added child must be a terminal node
  TrieNode *child = __newTrieNode(str, offset, len, payload ? payload->data : NULL,
                                  payload ? payload->len : 0, 0, score, 1);
  __trieNode_children(n)[n->numChildren - 1] = child;
  n->flags &= ~TRIENODE_SORTED;  // the node is now not sorted

  return n;
}

TrieNode *__trie_SplitNode(TrieNode *n, t_len offset) {
  // Copy the current node's data and children to a new child node
  TrieNode *newChild = __newTrieNode(n->str, offset, n->len, n->payload ? n->payload->data : NULL,
                                     n->payload ? n->payload->len : 0, n->numChildren, n->score,
                                     __trieNode_isTerminal(n));
  newChild->maxChildScore = n->maxChildScore;
  newChild->flags = n->flags;
  TrieNode **children = __trieNode_children(n);
  TrieNode **newChildren = __trieNode_children(newChild);
  memcpy(newChildren, children, sizeof(TrieNode *) * n->numChildren);

  // reduce the node to be just one child long with no score
  n->numChildren = 1;
  n->len = offset;
  n->score = 0;
  // the parent node is now non terminal and non sorted
  n->flags &= ~(TRIENODE_SORTED | TRIENODE_TERMINAL | TRIENODE_DELETED);

  n->maxChildScore = MAX(n->maxChildScore, newChild->score);
  if (n->payload != NULL) {
    free(n->payload);
    n->payload = NULL;
  }
  n = realloc(n, __trieNode_Sizeof(n->numChildren, n->len));
  __trieNode_children(n)[0] = newChild;

  return n;
}

/* If a node has a single child after delete, we can merged them. This deletes
 * the node and returns a newly allocated node */
TrieNode *__trieNode_MergeWithSingleChild(TrieNode *n) {

  if (__trieNode_isTerminal(n) || n->numChildren != 1) {
    return n;
  }
  TrieNode *ch = *__trieNode_children(n);

  // Copy the current node's data and children to a new child node
  rune nstr[n->len + ch->len + 1];
  memcpy(nstr, n->str, sizeof(rune) * n->len);
  memcpy(&nstr[n->len], ch->str, sizeof(rune) * ch->len);
  TrieNode *merged = __newTrieNode(
      nstr, 0, n->len + ch->len, ch->payload ? ch->payload->data : NULL,
      ch->payload ? ch->payload->len : 0, ch->numChildren, ch->score, __trieNode_isTerminal(ch));
  merged->maxChildScore = ch->maxChildScore;
  merged->numChildren = ch->numChildren;
  merged->flags = ch->flags;
  TrieNode **children = __trieNode_children(ch);
  TrieNode **newChildren = __trieNode_children(merged);
  memcpy(newChildren, children, sizeof(TrieNode *) * merged->numChildren);
  if (ch->payload) {
    free(ch->payload);
    ch->payload = NULL;
  }
  if (n->payload != NULL) {
    free(n->payload);
    n->payload = NULL;
  }
  free(n);
  free(ch);

  return merged;
}

void TrieNode_Print(TrieNode *n, int idx, int depth) {
  for (int i = 0; i < depth; i++) {
    printf("  ");
  }
  printf("%d) Score %f, max ChildScore %f\n", idx, n->score, n->maxChildScore);
  for (int i = 0; i < n->numChildren; i++) {
    TrieNode_Print(__trieNode_children(n)[i], i, depth + 1);
  }
}

int TrieNode_Add(TrieNode **np, rune *str, t_len len, RSPayload *payload, float score,
                 TrieAddOp op) {
  if (score == 0 || len == 0) {
    return 0;
  }

  TrieNode *n = *np;

  int offset = 0;
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
    n = __trie_SplitNode(n, offset);
    // the new string matches the split node exactly!
    // we simply turn the split node, which is now non terminal, into a terminal
    // node
    if (offset == len) {
      n->score = score;
      n->flags |= TRIENODE_TERMINAL;
      TrieNode *newChild = __trieNode_children(n)[0];
      n = realloc(n, __trieNode_Sizeof(n->numChildren, n->len));
      if (n->payload != NULL) {
        free(n->payload);
        n->payload = NULL;
      }
      if (payload != NULL && payload->data != NULL && payload->len > 0) {
        n->payload = triePayload_New(payload->data, payload->len);
      }

      __trieNode_children(n)[0] = newChild;
    } else {
      // we add a child
      n = __trie_AddChild(n, str, offset, len, payload, score);
      n->maxChildScore = MAX(n->maxChildScore, score);
    }
    *np = n;
    return 1;
  }

  n->maxChildScore = MAX(n->maxChildScore, score);

  // we're inserting in an existing node - just replace the value
  if (offset == len) {
    int term = __trieNode_isTerminal(n);
    int deleted = __trieNode_isDeleted(n);
    switch (op) {
      // in increment mode, just add the score to the node's score
      case ADD_INCR:
        n->score += score;
        break;

      // by default we just replace the score
      case ADD_REPLACE:
      default:
        n->score = score;
    }
    if (n->payload != NULL) {
      free(n->payload);
      n->payload = NULL;
    }
    if (payload != NULL && payload->data != NULL && payload->len > 0) {
      n->payload = triePayload_New(payload->data, payload->len);
    }
    // set the node as terminal
    n->flags |= TRIENODE_TERMINAL;
    // if it was deleted, make sure it's not now
    n->flags &= ~TRIENODE_DELETED;
    *np = n;
    return (term && !deleted) ? 0 : 1;
  }

  // proceed to the next child or add a new child for the current rune
  for (t_len i = 0; i < n->numChildren; i++) {
    TrieNode *child = __trieNode_children(n)[i];
    if (str[offset] == child->str[0]) {
      int rc = TrieNode_Add(&child, str + offset, len - offset, payload, score, op);
      __trieNode_children(n)[i] = child;
      return rc;
    }
  }
  *np = __trie_AddChild(n, str, offset, len, payload, score);
  return 1;
}

float TrieNode_Find(TrieNode *n, rune *str, t_len len) {
  t_len offset = 0;
  while (n && offset < len) {
    // printf("n %.*s offset %d, len %d\n", n->len, n->str, offset,
    // len);
    t_len localOffset = 0;
    for (; offset < len && localOffset < n->len; offset++, localOffset++) {
      if (str[offset] != n->str[localOffset]) {
        break;
      }
    }

    if (offset == len) {
      // we're at the end of both strings!
      if (localOffset == n->len) return __trieNode_isDeleted(n) ? 0 : n->score;

    } else if (localOffset == n->len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      t_len i = 0;
      TrieNode *nextChild = NULL;
      for (; i < n->numChildren; i++) {
        TrieNode *child = __trieNode_children(n)[i];

        if (str[offset] == child->str[0]) {
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

void __trieNode_sortChildren(TrieNode *n);

/* Optimize the node and its children:
*   1. If a child should be deleted - delete it and reduce the child count
*   2. If a child has a single child - merge them
*   3. recalculate the max child score
*/
void __trieNode_optimizeChildren(TrieNode *n) {

  int i = 0;
  TrieNode **nodes = __trieNode_children(n);
  n->maxChildScore = n->score;
  // free deleted terminal nodes
  while (i < n->numChildren) {

    // if this is a deleted node with no children - remove it
    if (nodes[i]->numChildren == 0 && __trieNode_isDeleted(nodes[i])) {
      TrieNode_Free(nodes[i]);

      nodes[i] = NULL;
      // just "fill" the hole with the next node up
      while (i < n->numChildren - 1) {
        nodes[i] = nodes[i + 1];
        n->maxChildScore = MAX(n->maxChildScore, nodes[i]->maxChildScore);
        i++;
      }
      // reduce child count
      n->numChildren--;
    } else {

      // this node is ok!
      // if needed - merge this node with it its single child
      if (nodes[i] && nodes[i]->numChildren == 1) {
        nodes[i] = __trieNode_MergeWithSingleChild(nodes[i]);
      }
      n->maxChildScore = MAX(n->maxChildScore, nodes[i]->maxChildScore);
    }
    i++;
  }

  __trieNode_sortChildren(n);
}

int TrieNode_Delete(TrieNode *n, rune *str, t_len len) {
  t_len offset = 0;
  static TrieNode *stack[MAX_STRING_LEN];
  int stackPos = 0;
  int rc = 0;
  while (n && offset < len) {
    stack[stackPos++] = n;
    t_len localOffset = 0;
    for (; offset < len && localOffset < n->len; offset++, localOffset++) {
      if (str[offset] != n->str[localOffset]) {
        break;
      }
    }

    if (offset == len) {
      // we're at the end of both strings!
      // this means we've found what we're looking for
      if (localOffset == n->len) {
        if (!(n->flags & TRIENODE_DELETED)) {

          n->flags |= TRIENODE_DELETED;
          n->flags &= ~TRIENODE_TERMINAL;
          n->score = 0;
          rc = 1;
        }
        goto end;
      }

    } else if (localOffset == n->len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      t_len i = 0;
      TrieNode *nextChild = NULL;
      for (; i < n->numChildren; i++) {
        TrieNode *child = __trieNode_children(n)[i];

        if (str[offset] == child->str[0]) {
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
    __trieNode_optimizeChildren(stack[stackPos]);
  }
  return rc;
}

void TrieNode_Free(TrieNode *n) {
  for (t_len i = 0; i < n->numChildren; i++) {
    TrieNode *child = __trieNode_children(n)[i];
    TrieNode_Free(child);
  }
  if (n->payload != NULL) {
    free(n->payload);
    n->payload = NULL;
  }

  free(n);
}

// comparator for node sorting by child max score
static int __trieNode_Cmp(const void *p1, const void *p2) {
  TrieNode *n1 = *(TrieNode **)p1;
  TrieNode *n2 = *(TrieNode **)p2;

  if (n1->maxChildScore < n2->maxChildScore) {
    return 1;
  } else if (n1->maxChildScore > n2->maxChildScore) {
    return -1;
  }
  return 0;
}

/* Sort the children of a node by their maxChildScore */
void __trieNode_sortChildren(TrieNode *n) {
  if (!(n->flags & TRIENODE_SORTED) && n->numChildren > 1) {
    qsort(__trieNode_children(n), n->numChildren, sizeof(TrieNode *), __trieNode_Cmp);
  }
  n->flags |= TRIENODE_SORTED;
}

/* Push a new trie node on the iterator's stack */
inline void __ti_Push(TrieIterator *it, TrieNode *node, int skipped) {
  if (it->stackOffset < MAX_STRING_LEN - 1) {
    stackNode *sn = &it->stack[it->stackOffset++];
    sn->childOffset = 0;
    sn->stringOffset = 0;
    sn->isSkipped = skipped;
    sn->n = node;
    sn->state = ITERSTATE_SELF;
  }
}

inline void __ti_Pop(TrieIterator *it) {
  if (it->stackOffset > 0) {
    stackNode *current = __ti_current(it);
    if (it->popCallback) {
      it->popCallback(it->ctx, current->stringOffset);
    }

    it->bufOffset -= current->stringOffset;
    --it->stackOffset;
  }
}

inline int __ti_step(TrieIterator *it, void *matchCtx) {
  if (it->stackOffset == 0) {
    return __STEP_STOP;
  }

  stackNode *current = __ti_current(it);

  int matched = 0;
  // printf("[%.*s]current %p (%.*s %f), state %d, string offset %d/%d, child
  // offset %d/%d\n",
  //        it->bufOffset, it->buf, current, current->n->len, current->n->str,
  //        current->n->score, current->state, current->stringOffset,
  //        current->n->len,
  //        current->childOffset, current->n->numChildren);
  switch (current->state) {
    case ITERSTATE_MATCH:
      __ti_Pop(it);
      goto next;

    case ITERSTATE_SELF:

      if (current->stringOffset < current->n->len) {
        // get the current rune to feed the filter
        rune b = current->n->str[current->stringOffset];

        if (it->filter) {
          // run the next character in the filter
          FilterCode rc = it->filter(b, it->ctx, &matched, matchCtx);

          // if we should stop...
          if (rc == F_STOP) {
            // match stop - change the state to MATCH and return
            if (matched) {
              current->state = ITERSTATE_MATCH;
              return __STEP_MATCH;
            }
            // normal stop - just pop and continue
            __ti_Pop(it);
            goto next;
          }
        }

        // advance the buffer offset and character offset
        it->buf[it->bufOffset++] = b;
        current->stringOffset++;

        // if we don't have a filter, a "match" is when we reach the end of the
        // node
        if (!it->filter) {
          if (current->n->len > 0 && current->stringOffset == current->n->len &&
              __trieNode_isTerminal(current->n) && !__trieNode_isDeleted(current->n)) {
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
      if (!(current->n->flags & TRIENODE_SORTED)) {
        __trieNode_sortChildren(current->n);
      }
      // push the next child
      if (current->childOffset < current->n->numChildren) {
        TrieNode *ch = __trieNode_children(current->n)[current->childOffset++];
        if (ch->maxChildScore >= it->minScore || ch->score >= it->minScore) {
          __ti_Push(it, ch, 0);
          it->nodesConsumed++;
        } else {
          //__ti_Push(it, ch, 1);
          it->nodesSkipped++;
        }
      } else {
        // at the end of the node - pop and go up
        __ti_Pop(it);
      }
  }

next:
  return __STEP_CONT;
}

TrieIterator *TrieNode_Iterate(TrieNode *n, StepFilter f, StackPopCallback pf, void *ctx) {
  TrieIterator *it = calloc(1, sizeof(TrieIterator));
  it->filter = f;
  it->popCallback = pf;
  it->minScore = 0;
  it->ctx = ctx;
  __ti_Push(it, n, 0);

  return it;
}

void TrieIterator_Free(TrieIterator *it) {
  free(it);
}

int TrieIterator_Next(TrieIterator *it, rune **ptr, t_len *len, RSPayload *payload, float *score,
                      void *matchCtx) {
  int rc;
  while ((rc = __ti_step(it, matchCtx)) != __STEP_STOP) {
    if (rc == __STEP_MATCH) {
      stackNode *sn = __ti_current(it);

      if (__trieNode_isTerminal(sn->n) && sn->n->len == sn->stringOffset &&
          !__trieNode_isDeleted(sn->n)) {
        *ptr = it->buf;
        *len = it->bufOffset;
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
        return 1;
      }
    }
  }

  return 0;
}

TrieNode *TrieNode_RandomWalk(TrieNode *n, int minSteps, rune **str, t_len *len) {
  // create an iteration stack we walk up and down
  minSteps = MAX(minSteps, 4);

  size_t stackCap = minSteps;
  size_t stackSz = 1;
  TrieNode **stack = calloc(stackCap, sizeof(TrieNode *));
  stack[0] = n;

  int bufCap = n->len;

  int steps = 0;

  while (steps < minSteps || !__trieNode_isTerminal(stack[stackSz - 1])) {

    n = stack[stackSz - 1];

    /* select the next step - -1 means walk back up one level */
    int rnd = (rand() % (n->numChildren + 1)) - 1;
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
    TrieNode *child = __trieNode_children(n)[rnd];
    stack[stackSz++] = child;

    steps++;
    if (stackSz == stackCap) {
      stackCap += minSteps;
      stack = realloc(stack, stackCap * sizeof(TrieNode *));
    }

    bufCap += child->len;
  }

  /* Return the node at the top of the stack */

  n = stack[stackSz - 1];

  /* build the string by walking the stack and copying all node strings */
  rune *buf = calloc(bufCap + 1, sizeof(rune));

  t_len bufSize = 0;
  for (size_t i = 0; i < stackSz; i++) {
    memcpy(&buf[bufSize], stack[i]->str, sizeof(rune) * stack[i]->len);
    bufSize += stack[i]->len;
  }

  *str = buf;
  *len = bufSize;
  //(*str)[bufSize] = '\0';
  free(stack);
  return n;
}
