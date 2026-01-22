/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "reply_pool.h"
#include "rmalloc.h"
#include "hiredis/read.h"
#include "hiredis/hiredis.h"

#include <string.h>

ReplyPool *ReplyPool_New(void) {
    ReplyPool *pool = rm_malloc(sizeof(ReplyPool));
    ReplyPoolBlock *block = rm_malloc(sizeof(ReplyPoolBlock) + REPLY_POOL_BLOCK_SIZE);
    block->next = NULL;
    block->used = 0;

    pool->head = block;
    pool->current = block;
    pool->block_size = REPLY_POOL_BLOCK_SIZE;
    return pool;
}

void *ReplyPool_Alloc(ReplyPool *pool, size_t size) {
    size = (size + 7) & ~7;  // 8-byte alignment

    if (size > pool->block_size) {
        // Oversized: allocate dedicated block, insert after current
        ReplyPoolBlock *big = rm_malloc(sizeof(ReplyPoolBlock) + size);
        big->used = size;
        big->next = pool->current->next;
        pool->current->next = big;
        return big->data;
    }

    if (pool->current->used + size > pool->block_size) {
        // Current block full: allocate new block
        ReplyPoolBlock *new_block = rm_malloc(sizeof(ReplyPoolBlock) + pool->block_size);
        new_block->next = NULL;
        new_block->used = 0;
        pool->current->next = new_block;
        pool->current = new_block;
    }

    void *ptr = pool->current->data + pool->current->used;
    pool->current->used += size;
    return ptr;
}

void ReplyPool_Free(ReplyPool *pool) {
    if (!pool) return;
    ReplyPoolBlock *block = pool->head;
    while (block) {
        ReplyPoolBlock *next = block->next;
        rm_free(block);
        block = next;
    }
    rm_free(pool);
}

void PooledReply_Free(PooledReply *pr) {
    if (!pr) return;
    ReplyPool_Free(pr->pool);
    rm_free(pr);
}

// Thread-local storage for the current pool being used for parsing
// This is set before parsing starts and cleared after the reply is complete
static __thread ReplyPool *tls_current_pool = NULL;

// Get or create the pool for the current reply being parsed
static ReplyPool *getOrCreatePool(void) {
    if (!tls_current_pool) {
        tls_current_pool = ReplyPool_New();
    }
    return tls_current_pool;
}

// Called after a reply is complete to get and clear the current pool
ReplyPool *ReplyPool_TakeCurrentPool(void) {
    ReplyPool *pool = tls_current_pool;
    tls_current_pool = NULL;
    return pool;
}

// Convert hiredis type to MRReply type (they are the same values)
static inline int cycleType(int type) {
    return type;  // MR_REPLY_* values match REDIS_REPLY_* values
}

static void *pooledCreateString(const redisReadTask *task, char *str, size_t len) {
    (void)task->privdata;  // We use TLS instead
    ReplyPool *pool = getOrCreatePool();
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);
    r->str = ReplyPool_Alloc(pool, len + 1);
    memcpy(r->str, str, len);
    r->str[len] = '\0';
    r->len = len;

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void *pooledCreateArray(const redisReadTask *task, size_t elements) {
    (void)task->privdata;  // We use TLS instead
    ReplyPool *pool = getOrCreatePool();
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);
    if (elements > 0) {
        r->element = ReplyPool_Alloc(pool, elements * sizeof(MRReply*));
        memset(r->element, 0, elements * sizeof(MRReply*));
    }
    r->elements = elements;

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void *pooledCreateInteger(const redisReadTask *task, long long value) {
    (void)task->privdata;  // We use TLS instead
    ReplyPool *pool = getOrCreatePool();
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);
    r->integer = value;

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void *pooledCreateDouble(const redisReadTask *task, double value, char *str, size_t len) {
    (void)task->privdata;  // We use TLS instead
    ReplyPool *pool = getOrCreatePool();
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);
    r->dval = value;
    if (str && len > 0) {
        r->str = ReplyPool_Alloc(pool, len + 1);
        memcpy(r->str, str, len);
        r->str[len] = '\0';
        r->len = len;
    }

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void *pooledCreateNil(const redisReadTask *task) {
    (void)task->privdata;  // We use TLS instead
    ReplyPool *pool = getOrCreatePool();
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void *pooledCreateBool(const redisReadTask *task, int bval) {
    (void)task->privdata;  // We use TLS instead
    ReplyPool *pool = getOrCreatePool();
    MRReply *r = ReplyPool_Alloc(pool, sizeof(MRReply));
    memset(r, 0, sizeof(*r));
    r->type = cycleType(task->type);
    r->integer = bval;

    if (task->parent) {
        MRReply *parent = task->parent->obj;
        parent->element[task->idx] = r;
    }
    return r;
}

static void pooledFreeObject(void *obj) {
    // No-op: pool frees everything at once
    (void)obj;
}

static redisReplyObjectFunctions pooledReplyFunctions = {
    .createString = pooledCreateString,
    .createArray = pooledCreateArray,
    .createInteger = pooledCreateInteger,
    .createDouble = pooledCreateDouble,
    .createNil = pooledCreateNil,
    .createBool = pooledCreateBool,
    .freeObject = pooledFreeObject,
};

redisReplyObjectFunctions *ReplyPool_GetFunctions(void) {
    return &pooledReplyFunctions;
}

// Deep copy an MRReply subtree using rm_malloc (not pool)
MRReply *MRReply_DeepCopy(const MRReply *src) {
    if (!src) return NULL;

    MRReply *dst = rm_calloc(1, sizeof(MRReply));
    dst->type = src->type;
    dst->integer = src->integer;
    dst->dval = src->dval;
    dst->len = src->len;

    // Copy string if present
    if (src->str) {
        dst->str = rm_malloc(src->len + 1);
        memcpy(dst->str, src->str, src->len);
        dst->str[src->len] = '\0';
    }

    // Copy vtype for VERB replies
    memcpy(dst->vtype, src->vtype, sizeof(dst->vtype));

    // Recursively copy elements for arrays/maps/sets
    if (src->elements > 0 && src->element) {
        dst->elements = src->elements;
        dst->element = rm_calloc(src->elements, sizeof(MRReply*));
        for (size_t i = 0; i < src->elements; i++) {
            dst->element[i] = MRReply_DeepCopy(src->element[i]);
        }
    }

    return dst;
}

