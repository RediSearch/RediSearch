#pragma once

/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2020, Ariel Shtul <ariel.shtul at redislabs dot com>
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
 * Alternative to Balanced Trees", modified in two ways:
 * a) the comparison is not just by a compare function.
 * b) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head. */

#define SKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define SKIPLIST_P 0.25      /* Skiplist P = 1/4 */

typedef int(*slCmpFunc)(void *a, void *b);
typedef void(*slDestroyFunc)(void *a);

/* Skiplists */
typedef struct skiplistNode {
  void *ele;
  struct skiplistNode *backward;
  struct skiplistLevel {
    struct skiplistNode *forward;
    unsigned long span;
  } level[];
} skiplistNode;

typedef struct skiplist {
  struct skiplistNode *header, *tail;
  unsigned long length;
  int level;
  slCmpFunc cmp;
  slDestroyFunc dtor;
} skiplist;

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API
 *----------------------------------------------------------------------------*/

/* Create a new skiplist. */
skiplist *slCreate(slCmpFunc cmp, slDestroyFunc dtor);

/* Free a whole skiplist. */
void slFree(skiplist *sl);

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
skiplistNode *slInsert(skiplist *sl, void *ele);

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by slFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
int slDelete(skiplist *sl, void *eleIn, void **eleOut);

void *slGet(skiplist *sl, void *key);
unsigned long slGetRank(skiplist *sl, void *ele);
unsigned long slGetLength(skiplist *sl);

// TODO: return void *ele
skiplistNode* slGetElementByRank(skiplist *sl, unsigned long rank);

/*-----------------------------------------------------------------------------
 * Skiplist Iterator implementation of the low level API
 *----------------------------------------------------------------------------*/

typedef struct skiplistIterator {
  skiplist *sl;
  skiplistNode *cur;
  skiplistNode *end;
} skiplistIterator;

/* Return an iterator at or after the `start` element.
 * 
 */
skiplistIterator *slIteratorCreate(skiplist *sl, void *start, void *end);


void slIteratorDestroy(skiplistIterator *iter);


void *slIteratorNext(skiplistIterator *iter);

