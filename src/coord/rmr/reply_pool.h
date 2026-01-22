/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "reply.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define REPLY_POOL_BLOCK_SIZE (64 * 1024)  // 64KB blocks

typedef struct ReplyPoolBlock {
    struct ReplyPoolBlock *next;
    size_t used;
    char data[];  // Flexible array member
} ReplyPoolBlock;

typedef struct ReplyPool {
    ReplyPoolBlock *head;      // First block (for freeing)
    ReplyPoolBlock *current;   // Current block for allocation
    size_t block_size;         // Size of each block's data area
} ReplyPool;

// Wrapper that pairs a reply with its memory pool
typedef struct PooledReply {
    MRReply *reply;
    ReplyPool *pool;
} PooledReply;

// Create a new reply pool
ReplyPool *ReplyPool_New(void);

// Allocate memory from the pool (8-byte aligned)
void *ReplyPool_Alloc(ReplyPool *pool, size_t size);

// Free the entire pool (all blocks)
void ReplyPool_Free(ReplyPool *pool);

// Free a PooledReply (frees the pool, not individual nodes)
void PooledReply_Free(PooledReply *pr);

// Deep copy an MRReply subtree using rm_malloc (not pool)
// The returned MRReply can be freed with MRReply_Free
MRReply *MRReply_DeepCopy(const MRReply *src);

// Get the pooled reply object functions for use with hiredis reader
struct redisReplyObjectFunctions *ReplyPool_GetFunctions(void);

// Take the current pool from thread-local storage (called after parsing a reply)
// Returns the pool and clears the TLS slot. Caller owns the returned pool.
ReplyPool *ReplyPool_TakeCurrentPool(void);

#ifdef __cplusplus
}
#endif

