/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

#include "skiplist.h"
#include "rmalloc.h"
#include <assert.h> // assert
#include <math.h>   // random
#include <stdlib.h>

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API
 *----------------------------------------------------------------------------*/

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */
static skiplistNode *slCreateNode(int level, void *ele) {
    skiplistNode *zn =
        rm_malloc(sizeof(*zn)+level*sizeof(struct skiplistLevel));
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist. */
skiplist *slCreate(slCmpFunc cmp, slDestroyFunc dtor) {
    int j;
    skiplist *sl;

    sl = rm_malloc(sizeof(*sl));
    sl->cmp = cmp;
    sl->dtor = dtor;
    sl->level = 1;
    sl->length = 0;
    sl->header = slCreateNode(SKIPLIST_MAXLEVEL,NULL);
    for (j = 0; j < SKIPLIST_MAXLEVEL; j++) {
        sl->header->level[j].forward = NULL;
        sl->header->level[j].span = 0;
    }
    sl->header->backward = NULL;
    sl->tail = NULL;
    return sl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
static void slFreeNode(skiplistNode *node, slDestroyFunc dtor) {
    if (dtor && node->ele) dtor(node->ele);
    rm_free(node);
}

/* Free a whole skiplist. */
void slFree(skiplist *sl) {
    skiplistNode *node = sl->header, *next;

    while(node) {
        next = node->level[0].forward;
        slFreeNode(node, sl->dtor);
        node = next;
    }
    rm_free(sl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and SKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
static int slRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (SKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<SKIPLIST_MAXLEVEL) ? level : SKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
skiplistNode *slInsert(skiplist *sl, void *ele) {
    skiplistNode *update[SKIPLIST_MAXLEVEL], *x;
    unsigned int rank[SKIPLIST_MAXLEVEL];
    int i, level;

    assert(ele);
    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (sl->level-1) ? 0 : rank[i+1];
        while (x->level[i].forward && sl->cmp(x->level[i].forward->ele,ele) < 0) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of slInsert() should test in the hash table if the element is
     * already inside or not. */
    level = slRandomLevel();
    if (level > sl->level) {
        for (i = sl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = sl->header;
            update[i]->level[i].span = sl->length;
        }
        sl->level = level;
    }
    x = slCreateNode(level,ele);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    for (i = level; i < sl->level; i++) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == sl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        sl->tail = x;
    sl->length++;
    return x;
}

/* Internal function used by slDelete, slDeleteRangeByScore and
 * slDeleteRangeByRank. */
void slDeleteNode(skiplist *sl, skiplistNode *x, skiplistNode **update) {
    int i;
    for (i = 0; i < sl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        sl->tail = x->backward;
    }
    while(sl->level > 1 && sl->header->level[sl->level-1].forward == NULL)
        sl->level--;
    sl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by slFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
int slDelete(skiplist *sl, void *eleIn, void **eleOut) {
    skiplistNode *update[SKIPLIST_MAXLEVEL], *x;
    int i;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        while (x->level[i].forward && sl->cmp(x->level[i].forward->ele,eleIn) < 0) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    x = x->level[0].forward;
    if (x && sl->cmp(x->ele,eleIn) == 0) {
        slDeleteNode(sl, x, update);
        if (eleOut)
            *eleOut = x->ele;
        else if (sl->dtor)
            sl->dtor(x->ele);
        rm_free(x);
        return 1;
    }
    return 0; /* not found */
}

/*-----------------------------------------------------------------------------
 * Skiplist iterator implementation of the low level API
 *----------------------------------------------------------------------------*/

static skiplistNode *_slFind(skiplist *sl, void *ele);

skiplistIterator *slIteratorCreate(skiplist *sl, void *start) {
    skiplistNode *node;


    // If no element is provided, we return the first element
    if (!start) {
        node = sl->header->level[0].forward;
    } else if (sl->cmp(sl->tail->ele, start) == -1) {
        node = sl->tail;
    } else {
        node = _slFind(sl, start);
    }
    
    skiplistIterator *slIter = rm_malloc(sizeof(*slIter));
    slIter->sl = sl;
    slIter->cur = node;

    return slIter;
}

void slIteratorDestroy(skiplistIterator *iter) {
    rm_free(iter);
}

void *slIteratorNext(skiplistIterator *iter) {
    // end of skiplist
    if(!iter || !iter->cur) {
        return NULL;
    }

    void *ele = iter->cur->ele;

    // If we iterator is at the tail, `forward` is NULL
    iter->cur = iter->cur->level[0].forward;

    return ele;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
static skiplistNode *_slFind(skiplist *sl, void *ele) {
    skiplistNode *x = sl->header;
    for (int i = sl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward && sl->cmp(x->level[i].forward->ele, ele) <= 0)
                x = x->level[i].forward;
    }
    if (x == sl->header) {
        x = sl->header->level[0].forward;
    }

    return x;//->level[0].forward;
}

void *slFind(skiplist *sl, void *key) {
    skiplistNode *node = _slFind(sl, key);
    if (!node) {
        return NULL;
    }
    return node->ele;
}

/* Find an element by `key`.
 * `key` must be compatible with the comparison function.
 * Returns NULL when the element cannot be found, pointer otherwise.
 *  */
void *slGet(skiplist *sl, void *key) {
    skiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        while (x->level[i].forward && sl->cmp(x->level[i].forward->ele,key) <= 0) {
            x = x->level[i].forward;
        }

        /* x might be equal to sl->header, so test if obj is non-NULL */
        if (x->ele && sl->cmp(x->ele,key) == 0) {
            return x->ele;
        }
    }
    return NULL;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of sl->header to the
 * first element. */
unsigned long slGetRank(skiplist *sl, void *ele) {
    skiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        while (x->level[i].forward && sl->cmp(x->level[i].forward->ele,ele) <= 0) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to sl->header, so test if obj is non-NULL */
        if (x->ele && sl->cmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
skiplistNode* slGetElementByRank(skiplist *sl, unsigned long rank) {
    skiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = sl->header;
    for (i = sl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

unsigned long slGetLength(skiplist *sl) {
    return sl->length;
}