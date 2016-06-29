#include "trie.h"
#include "sparse_vector.h"

size_t __trieNode_Sizeof(t_len numChildren, t_len slen) {
    return sizeof(nodeHeader) + numChildren * sizeof(TrieNode *) + slen + 1;
}

TrieNode *__newTrieNode(char *str, t_len offset, t_len len, t_len numChildren, float score) {
    TrieNode *n = calloc(1, __trieNode_Sizeof(numChildren, len - offset));
    n->node.len = len - offset;
    n->node.numChildren = numChildren;
    n->node.score = score;
    strncpy(n->node.str, str + offset, len - offset);
    return n;
}

TrieNode *__trie_AddChild(TrieNode *n, char *str, t_len offset, t_len len, float score) {
    n->node.numChildren++;
    n = realloc((void *)n, __trieNode_Sizeof(n->node.numChildren, n->node.len));
    TrieNode *child = __newTrieNode(str, offset, len, 0, score);

    __trieNode_children(n)[n->node.numChildren - 1] = child;

    return n;
}

TrieNode *__trie_SplitNode(TrieNode *n, t_len offset) {
    // Copy the current node's data and children to a new child node
    TrieNode *newChild =
        __newTrieNode(n->node.str, offset, n->node.len, n->node.numChildren, n->node.score);
    TrieNode **children = __trieNode_children(n);
    TrieNode **newChildren = __trieNode_children(newChild);
    memcpy(newChildren, children, sizeof(TrieNode *) * n->node.numChildren);

    // reduce the node to be just one child long with no score
    n->node.numChildren = 1;
    n->node.len = offset;
    n->node.score = 0;
    n = realloc(n, __trieNode_Sizeof(n->node.numChildren, n->node.len));
    __trieNode_children(n)[0] = newChild;

    return n;
}

TrieNode *Trie_Add(TrieNode *n, char *str, t_len len, float score) {
    int offset = 0, localOffset = 0;
    for (; offset < len && localOffset < n->node.len; offset++, localOffset++) {
        if (str[offset] != n->node.str[localOffset]) {
            break;
        }
    }
    // we broke off before the end of the string
    if (localOffset < n->node.len) {
        // split the node and create 2 child nodes:
        // 1. a child representing the new string from the diverted offset onwards
        // 2. a child representing the old node's suffix from the diverted offset
        // and the old children
        n = __trie_SplitNode(n, localOffset);
        n = __trie_AddChild(n, str, offset, len, score);
        return n;
    }

    // we're inserting in an existing node - just replace the value
    if (offset == len) {
        n->node.score = score;
        return n;
    }

    // proceed to the next child or add a new child for the current character
    t_len i = 0;
    for (; i < n->node.numChildren; i++) {
        TrieNode *child = __trieNode_children(n)[i];

        if (str[offset] == child->node.str[0]) {
            __trieNode_children(n)[i] = Trie_Add(child, str + offset, len - offset, score);
            return n;
        }
    }

    n = __trie_AddChild(n, str, offset, len, score);
    return n;
}

float Trie_Find(TrieNode *n, char *str, t_len len) {
    t_len offset = 0;
    while (n && offset < len) {
        // printf("n %.*s offset %d, len %d\n", n->node.len, n->node.str, offset,
        // len);
        t_len localOffset = 0;
        for (; offset < len && localOffset < n->node.len; offset++, localOffset++) {
            if (str[offset] != n->node.str[localOffset]) {
                break;
            }
        }

        if (offset == len) {
            // we're at the end of both strings!
            if (localOffset == n->node.len) return n->node.score;

        } else if (localOffset == n->node.len) {
            // we've reached the end of the node's string but not the search string
            // let's find a child to continue to
            t_len i = 0;
            TrieNode *nextChild = NULL;
            for (; i < n->node.numChildren; i++) {
                TrieNode *child = __trieNode_children(n)[i];

                if (str[offset] == child->node.str[0]) {
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

void Trie_Free(TrieNode *n) {
    for (t_len i = 0; i < n->node.numChildren; i++) {
        TrieNode *child = __trieNode_children(n)[i];
        Trie_Free(child);
    }

    free(n);
}

/* Push a new trie node on the iterator's stack */
inline void __ti_Push(TrieIterator *it, TrieNode *node) {
    if (it->stackOffset < MAX_STRING_LEN - 1) {
        stackNode *sn = &it->stack[it->stackOffset++];
        sn->childOffset = 0;
        sn->stringOffset = 0;

        sn->n = node;
        sn->state = ITERSTATE_SELF;
    }
}

inline void __ti_Pop(TrieIterator *it) {
    if (it->stackOffset) {
        stackNode *current = __ti_current(it);
        for (int i = 0; i < current->stringOffset; i++) {
            it->filter(FILTER_STACK_POP, it->ctx, NULL);
        }
        it->bufOffset -= current->stringOffset;
        --it->stackOffset;
    }
}

inline int __ti_step(TrieIterator *it) {
    if (it->stackOffset == 0) {
        return 0;
    }

    stackNode *current = __ti_current(it);
    int matched = 0;

    // printf("[%.*s]current %p (%.*s %f), state %d, string offset %d/%d, child offset %d/%d\n",
    //        it->bufOffset, it->buf, current, current->n->node.len, current->n->node.str,
    //        current->n->node.score, current->state, current->stringOffset, current->n->node.len,
    //        current->childOffset, current->n->node.numChildren);
    switch (current->state) {
        case ITERSTATE_MATCH:
            __ti_Pop(it);
            //__ti_Pop(it);
            goto next;

        case ITERSTATE_SELF:
            if (current->stringOffset < current->n->node.len) {
                unsigned char b = current->n->node.str[current->stringOffset];
                if (it->filter) {
                    FilterCode rc = it->filter(b, it->ctx, &matched);

                    if (rc == F_STOP) {
                        if (matched) {
                            current->state = ITERSTATE_MATCH;
                            return 3;
                        }
                        __ti_Pop(it);
                        goto next;
                    }
                }

                it->buf[it->bufOffset++] = b;
                current->stringOffset++;
                return matched ? 3 : 1;
            } else {
                // switch to "children mode"
                current->state = ITERSTATE_CHILDREN;
            }

        case ITERSTATE_CHILDREN:
        default:
            if (current->childOffset < current->n->node.numChildren) {
                __ti_Push(it, __trieNode_children(current->n)[current->childOffset++]);
            } else {
                __ti_Pop(it);
            }
    }

next:
    return 2;
}

TrieIterator *Trie_Iterate(TrieNode *n, StepFilter f, void *ctx) {
    TrieIterator *it = calloc(1, sizeof(TrieIterator));
    it->filter = f;
    it->ctx = ctx;
    __ti_Push(it, n);

    return it;
}

void TrieIterator_Free(TrieIterator *it) { free(it); }

int TrieIterator_Next(TrieIterator *it, char **ptr, t_len *len, float *score) {
    int rc;
    while ((rc = __ti_step(it)) != 0) {
        if (rc == 2) continue;

        if (rc == 3) {
            stackNode *sn = __ti_current(it);

            if (sn->n->node.score && sn->n->node.len == sn->stringOffset) {
                *ptr = it->buf;
                *len = it->bufOffset;
                *score = sn->n->node.score;
                printf("%p %.*s (%d) %f\n", sn, *len, *ptr, *len, sn->n->node.score);
                return 1;
            }
        }
    }

    return 0;
}
