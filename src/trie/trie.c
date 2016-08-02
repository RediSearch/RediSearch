#include <sys/param.h>
#include "trie.h"
#include "sparse_vector.h"

size_t __trieNode_Sizeof(t_len numChildren, t_len slen) {
    return sizeof(TrieNode) + numChildren * sizeof(TrieNode *) + slen + 1;
}

TrieNode *__newTrieNode(char *str, t_len offset, t_len len, t_len numChildren, float score) {
    TrieNode *n = calloc(1, __trieNode_Sizeof(numChildren, len - offset));
    n->len = len - offset;
    n->numChildren = numChildren;
    n->score = score;
    n->maxChildScore = 0;
    strncpy(n->str, str + offset, len - offset);
    return n;
}

TrieNode *__trie_AddChild(TrieNode *n, char *str, t_len offset, t_len len, float score) {
    n->numChildren++;
    n = realloc((void *)n, __trieNode_Sizeof(n->numChildren, n->len));
    TrieNode *child = __newTrieNode(str, offset, len, 0, score);
    __trieNode_children(n)[n->numChildren - 1] = child;

    return n;
}

TrieNode *__trie_SplitNode(TrieNode *n, t_len offset) {
    // Copy the current node's data and children to a new child node
    TrieNode *newChild = __newTrieNode(n->str, offset, n->len, n->numChildren, n->score);
    newChild->maxChildScore = n->maxChildScore;
    TrieNode **children = __trieNode_children(n);
    TrieNode **newChildren = __trieNode_children(newChild);
    memcpy(newChildren, children, sizeof(TrieNode *) * n->numChildren);

    // reduce the node to be just one child long with no score
    n->numChildren = 1;
    n->len = offset;
    n->score = 0;
    n->maxChildScore = MAX(n->maxChildScore, newChild->score);
    n = realloc(n, __trieNode_Sizeof(n->numChildren, n->len));
    __trieNode_children(n)[0] = newChild;

    return n;
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
int TrieNode_Add(TrieNode **np, char *str, t_len len, float score, TrieAddOp op) {
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
        // we simply turn the split node, which is now non terminal, into a terminal node
        if (offset == len) {
            n->score = score;
        } else {
            // we add a child
            n = __trie_AddChild(n, str, offset, len, score);
            n->maxChildScore = MAX(n->maxChildScore, score);
        }
        *np = n;
        return 1;
    }

    n->maxChildScore = MAX(n->maxChildScore, score);

    // we're inserting in an existing node - just replace the value
    if (offset == len) {
        int term = __trieNode_isTerminal(n);
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
        *np = n;
        return term ? 0 : 1;
    }

    // proceed to the next child or add a new child for the current character

    for (t_len i = 0; i < n->numChildren; i++) {
        TrieNode *child = __trieNode_children(n)[i];

        if (str[offset] == child->str[0]) {
            int rc = TrieNode_Add(&child, str + offset, len - offset, score, op);
            __trieNode_children(n)[i] = child;
            return rc;
        }
    }

    *np = __trie_AddChild(n, str, offset, len, score);
    return 1;
}

float TrieNode_Find(TrieNode *n, char *str, t_len len) {
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
            if (localOffset == n->len) return n->score;

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

void TrieNode_Free(TrieNode *n) {
    for (t_len i = 0; i < n->numChildren; i++) {
        TrieNode *child = __trieNode_children(n)[i];
        TrieNode_Free(child);
    }

    free(n);
}

// internal definition of trie iterator

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
    if (it->stackOffset) {
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
    // printf("[%.*s]current %p (%.*s %f), state %d, string offset %d/%d, child offset %d/%d\n",
    //        it->bufOffset, it->buf, current, current->n->len, current->n->str,
    //        current->n->score, current->state, current->stringOffset, current->n->len,
    //        current->childOffset, current->n->numChildren);
    switch (current->state) {
        case ITERSTATE_MATCH:
            __ti_Pop(it);
            goto next;

        case ITERSTATE_SELF:

            if (current->stringOffset < current->n->len) {
                // get the current character to feed the filter
                unsigned char b = current->n->str[current->stringOffset];

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

                // if we don't have a filter, a "match" is when we reach the end of the node
                if (!it->filter) {
                    if (current->stringOffset == current->n->len &&
                        __trieNode_isTerminal(current->n)) {
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

void TrieIterator_Free(TrieIterator *it) { free(it); }

int TrieIterator_Next(TrieIterator *it, char **ptr, t_len *len, float *score, void *matchCtx) {
    int rc;
    while ((rc = __ti_step(it, matchCtx)) != __STEP_STOP) {
        if (rc == __STEP_MATCH) {
            stackNode *sn = __ti_current(it);

            if (__trieNode_isTerminal(sn->n) && sn->n->len == sn->stringOffset) {
                *ptr = it->buf;
                *len = it->bufOffset;
                *score = sn->n->score;
                return 1;
            }
        }
    }

    return 0;
}
