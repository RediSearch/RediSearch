/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef FORK_CYCLE_H__
#define FORK_CYCLE_H__

#include "index_gc.h"

// One collection cycle of the forking policy: fork a child that walks the index and streams
// deltas over the pipe, apply them in the parent as they arrive, then reap the child. Consumes
// `early_check`, the caller's strong reference to the spec, which the child must inherit
// alive. Returns false only when the index was dropped, so the GC should not reschedule.
bool ForkCycle_Run(IndexGC *gc, GCForcedRun *forced, const IndexGCHook *hook, StrongRef early_check);

#endif  // FORK_CYCLE_H__
