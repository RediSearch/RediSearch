/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "rmr/reply.h"
#include "score_explain.h"

#ifdef __cplusplus
extern "C" {
#endif

// Arity of a non-leaf wire node emitted by recExplainReply in score_explain.c.
#define SE_REPLY_NODE_ARITY 2

// Inverse of SEReply: reconstruct an RSScoreExplain tree from a shard reply.
// The returned root is heap-owned by the caller — release with SEDestroy.
// Any shape that SEReply could not have produced aborts.
RSScoreExplain *SE_FromMRReply(const MRReply *r);

#ifdef __cplusplus
}
#endif
