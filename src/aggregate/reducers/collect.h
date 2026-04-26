/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_REDUCERS_COLLECT_H_
#define RS_REDUCERS_COLLECT_H_

/**
 * Wire keyword used to plumb the per-shard COLLECT payload into the
 * coordinator-side COLLECT reducer.
 */
#define COLLECT_SOURCE_KEY "__SOURCE__"

#endif  // RS_REDUCERS_COLLECT_H_
