/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef DEFERRED_INDEX_H__
#define DEFERRED_INDEX_H__

#include <stdbool.h>
#include <stddef.h>

#include "doc_types.h"

struct IndexSpec;
struct RedisModuleString;

/* The queue holds operations, not keys: a deletion applied ahead of an update
 * already queued for the same key would leave the index disagreeing with the
 * keyspace, so both kinds are ordered together. */
typedef enum {
  DEFER_OP_ADD = 0,
  DEFER_OP_DEL,
} DeferOp;

/* Queue `key` for indexing into `sp` on a later event-loop iteration.
 *
 * Only for a caller that has just failed to take the spec write lock without
 * blocking. Ordering is the caller's responsibility: the synchronous fast path
 * must be taken only while the queue is empty. */
void DeferredIndex_Enqueue(struct IndexSpec *sp, struct RedisModuleString *key, DocumentType type,
                           DeferOp op);

/* Entries waiting to be applied. Zero means the synchronous fast path is
 * available; non-zero means it must not be taken, to preserve ordering. */
size_t DeferredIndex_PendingCount(void);

/* True when the queue is at its cap. The caller should park on the lock rather
 * than defer, so sustained overload degrades to blocking rather than to
 * unbounded index lag. */
bool DeferredIndex_ShouldBlock(void);

#endif
