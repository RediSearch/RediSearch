#include "triemap.h"
#include <sys/param.h>
#include "../rmutil/alloc.h"

size_t __trieMapNode_Sizeof(tm_len_t numChildren, tm_len_t slen) {
  return sizeof(TrieMapNode) + numChildren * sizeof(TrieMapNode *) + (slen + 1);
}

TrieMapNode *__newTrieMapNode(unsigned char *str, tm_len_t offset, tm_len_t len,
                              tm_len_t numChildren, void *value, int terminal) {
  TrieMapNode *n = calloc(1, __trieMapNode_Sizeof(numChildren, len - offset));
  n->len = len - offset;
  n->numChildren = numChildren;
  n->value = value;

  n->flags = terminal ? TM_NODE_TERMINAL : 0;

  memcpy(n->str, str + offset, (len - offset));

  return n;
}

TrieMap *NewTrieMap() {
  return __newTrieMapNode((unsigned char *)"", 0, 0, 0, NULL, 0);
}

TrieMapNode *__trieMapNode_AddChild(TrieMapNode *n, unsigned char *str, tm_len_t offset,
                                    tm_len_t len, void *value) {
  n->numChildren++;
  n = realloc((void *)n, __trieMapNode_Sizeof(n->numChildren, n->len));
  // a newly added child must be a terminal node
  TrieMapNode *child = __newTrieMapNode(str, offset, len, 0, value, 1);
  __trieMapNode_children(n)[n->numChildren - 1] = child;

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

  // reduce the node to be just one child long with no score
  n->numChildren = 1;
  n->len = offset;
  n->value = NULL;
  // the parent node is now non terminal and non sorted
  n->flags &= ~(TM_NODE_TERMINAL | TM_NODE_DELETED);

  n = realloc(n, __trieMapNode_Sizeof(n->numChildren, n->len));
  __trieMapNode_children(n)[0] = newChild;

  return n;
}

/* If a node has a single child after delete, we can merged them. This deletes
 * the node and returns a newly allocated node */
TrieMapNode *__trieMapNode_MergeWithSingleChild(TrieMapNode *n) {

  if (__trieMapNode_isTerminal(n) || n->numChildren != 1) {
    return n;
  }
  TrieMapNode *ch = *__trieMapNode_children(n);

  // Copy the current node's data and children to a new child node
  unsigned char nstr[n->len + ch->len + 1];
  memcpy(nstr, n->str, sizeof(unsigned char) * n->len);
  memcpy(&nstr[n->len], ch->str, sizeof(unsigned char) * ch->len);
  TrieMapNode *merged = __newTrieMapNode(nstr, 0, n->len + ch->len, ch->numChildren, ch->value,
                                         __trieMapNode_isTerminal(ch));

  merged->numChildren = ch->numChildren;
  merged->flags = ch->flags;
  TrieMapNode **children = __trieMapNode_children(ch);
  TrieMapNode **newChildren = __trieMapNode_children(merged);
  memcpy(newChildren, children, sizeof(TrieMapNode *) * merged->numChildren);

  free(n);
  free(ch);

  return merged;
}

void TrieMapNode_Print(TrieMapNode *n, int idx, int depth, void (*printval)(void *)) {
  for (int i = 0; i < depth; i++) {
    printf("  ");
  }
  printf("%d) Value :", idx);
  printval(n->value);
  printf("\n");
  for (int i = 0; i < n->numChildren; i++) {
    TrieMapNode_Print(__trieMapNode_children(n)[i], i, depth + 1, printval);
  }
}

int TrieMapNode_Add(TrieMapNode **np, unsigned char *str, tm_len_t len, void *value,
                    TrieMapReplaceFunc cb) {
  if (len == 0) {
    return 0;
  }

  TrieMapNode *n = *np;

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
    n = __trieMapNode_Split(n, offset);

    // the new string matches the split node exactly!
    // we simply turn the split node, which is now non terminal, into a terminal
    // node
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
      n->value = value;
    }

    // set the node as terminal
    n->flags |= TM_NODE_TERMINAL;
    // if it was deleted, make sure it's not now
    n->flags &= ~TM_NODE_DELETED;
    *np = n;
    // if the node existed - we return 0, otherwise return 1 as it's a new node
    return (term && !deleted) ? 0 : 1;
  }

  // proceed to the next child or add a new child for the current unsigned char
  for (tm_len_t i = 0; i < n->numChildren; i++) {
    TrieMapNode *child = __trieMapNode_children(n)[i];

    if (str[offset] == child->str[0]) {
      int rc = TrieMapNode_Add(&child, str + offset, len - offset, value, cb);
      __trieMapNode_children(n)[i] = child;
      return rc;
    }
  }

  *np = __trieMapNode_AddChild(n, str, offset, len, value);
  return 1;
}

void *TrieMapNode_Find(TrieMapNode *n, unsigned char *str, tm_len_t len) {
  tm_len_t offset = 0;
  while (n && offset < len) {
    // printf("n %.*s offset %d, len %d\n", n->len, n->str, offset,
    // len);
    tm_len_t localOffset = 0;
    for (; offset < len && localOffset < n->len; offset++, localOffset++) {
      if (str[offset] != n->str[localOffset]) {
        break;
      }
    }

    if (offset == len) {
      // we're at the end of both strings!
      if (localOffset == n->len) return __trieMapNode_isDeleted(n) ? NULL : n->value;

    } else if (localOffset == n->len) {
      // we've reached the end of the node's string but not the search string
      // let's find a child to continue to
      tm_len_t i = 0;
      TrieMapNode *nextChild = NULL;
      for (; i < n->numChildren; i++) {
        TrieMapNode *child = __trieMapNode_children(n)[i];

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
      // just "fill" the hole with the next node up
      while (i < n->numChildren - 1) {
        nodes[i] = nodes[i + 1];
        i++;
      }
      // reduce child count
      n->numChildren--;
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

int TrieMapNode_Delete(TrieMapNode *n, unsigned char *str, tm_len_t len, void (*freeCB)(void *)) {
  tm_len_t offset = 0;
  static TrieMapNode *stack[TM_MAX_STRING_LEN];
  int stackPos = 0;
  int rc = 0;
  while (n && offset < len) {
    stack[stackPos++] = n;
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
              free(n->value);
            }
          }
          n->value = NULL;
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
        TrieMapNode *child = __trieMapNode_children(n)[i];

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
    __trieMapNode_optimizeChildren(stack[stackPos], freeCB);
  }
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

void TrieMapNode_Free(TrieMapNode *n, void (*freeCB)(void *)) {
  for (tm_len_t i = 0; i < n->numChildren; i++) {
    TrieMapNode *child = __trieMapNode_children(n)[i];
    TrieMapNode_Free(child, freeCB);
  }
  if (n->value) {
    if (freeCB) {
      freeCB(n->value);
    } else {
      free(n->value);
    }
  }

  free(n);
}

// #define TM_ITERSTATE_SELF 0
// #define TM_ITERSTATE_CHILDREN 1
// #define TM_ITERSTATE_MATCH 2

// /* Push a new trie node on the iterator's stack */
// inline void __tmi_Push(TrieMapIterator *it, TrieMapNode *node, int skipped) {
//   if (it->stackOffset < TM_MAX_STRING_LEN - 1) {
//     __tmi_stackNode *sn = &it->stack[it->stackOffset++];
//     sn->childOffset = 0;
//     sn->stringOffset = 0;
//     sn->isSkipped = skipped;
//     sn->n = node;
//     sn->state = TM_ITERSTATE_SELF;
//   }
// }

// inline void __tmi_Pop(TrieMapIterator *it) {
//   if (it->stackOffset) {
//     __tmi_stackNode *current = __tmi_current(it);
//     // if (it->popCallback) {
//     //   it->popCallback(it->ctx, current->stringOffset);
//     // }

//     it->bufOffset -= current->stringOffset;
//     --it->stackOffset;
//   }
// }

// inline int __tmi_step(TrieMapIterator *it, void *matchCtx) {
//   if (it->stackOffset == 0) {
//     return __TM_STEP_STOP;
//   }

//   __tmi_stackNode *current = __tmi_current(it);

//   int matched = 0;
//   // printf("[%.*s]current %p (%.*s %f), state %d, string offset %d/%d, child
//   // offset %d/%d\n",
//   //        it->bufOffset, it->buf, current, current->n->len, current->n->str,
//   //        current->n->score, current->state, current->stringOffset,
//   //        current->n->len,
//   //        current->childOffset, current->n->numChildren);
//   switch (current->state) {
//     case TM_ITERSTATE_MATCH:
//       __tmi_Pop(it);
//       goto next;

//     case TM_ITERSTATE_SELF:

//       if (current->stringOffset < current->n->len) {
//         // get the current unsigned char to feed the filter
//         unsigned char b = current->n->str[current->stringOffset];

//         // if (it->filter) {
//         //   // run the next character in the filter
//         //   FilterCode rc = it->filter(b, it->ctx, &matched, matchCtx);

//         //   // if we should stop...
//         //   if (rc == F_STOP) {
//         //     // match stop - change the state to MATCH and return
//         //     if (matched) {
//         //       current->state = ITERSTATE_MATCH;
//         //       return __STEP_MATCH;
//         //     }
//         //     // normal stop - just pop and continue
//         //     __ti_Pop(it);
//         //     goto next;
//         //   }
//         // }

//         // advance the buffer offset and character offset
//         it->buf[it->bufOffset++] = b;
//         current->stringOffset++;

//         // if we don't have a filter, a "match" is when we reach the end of the
//         // node
//         // if (!it->filter) {
//         if (current->stringOffset == current->n->len && __trieMapNode_isTerminal(current->n) &&
//             !__trieMapNode_isDeleted(current->n)) {
//           matched = 1;
//         }
//         //}

//         return matched ? __TM_STEP_MATCH : __TM_STEP_CONT;
//       } else {
//         // switch to "children mode"
//         current->state = TM_ITERSTATE_CHILDREN;
//       }

//     case TM_ITERSTATE_CHILDREN:
//     default:

//       // push the next child
//       if (current->childOffset < current->n->numChildren) {
//         TrieMapNode *ch = __trieMapNode_children(current->n)[current->childOffset++];
//         __tmi_Push(it, ch, 0);

//       } else {
//         // at the end of the node - pop and go up
//         __tmi_Pop(it);
//       }
//   }

// next:
//   return __TM_STEP_CONT;
// }

// TrieMapIterator *TrieMapNode_Iterate(TrieMapNode *n, void *ctx) {
//   TrieMapIterator *it = calloc(1, sizeof(TrieMapIterator));
//   // it->filter = f;
//   // it->popCallback = pf;

//   it->ctx = ctx;
//   __tmi_Push(it, n, 0);

//   return it;
// }

// void TrieMapIterator_Free(TrieMapIterator *it) {
//   free(it);
// }

// int TrieMapIterator_Next(TrieMapIterator *it, unsigned char **ptr, tm_len_t *len, void **value,
//                          void *matchCtx) {
//   int rc;
//   while ((rc = __tmi_step(it, matchCtx)) != __TM_STEP_STOP) {
//     if (rc == __TM_STEP_MATCH) {
//       __tmi_stackNode *sn = __tmi_current(it);

//       if (__trieMapNode_isTerminal(sn->n) && sn->n->len == sn->stringOffset &&
//           !__trieMapNode_isDeleted(sn->n)) {
//         *ptr = it->buf;
//         *len = it->bufOffset;
//         *value = sn->n->value;
//         return 1;
//       }
//     }
//   }

//   return 0;
// }
