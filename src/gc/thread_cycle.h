/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef THREAD_CYCLE_H__
#define THREAD_CYCLE_H__

#include "index_gc.h"

// One collection cycle of the THREAD policy: the scanners run on the calling GC pool thread under
// the spec's own read/write lock. Consumes `spec_ref`, keeping the spec alive to the end; the
// driver still promotes the weak reference for every lock it takes, so a drop mid-cycle ends the
// cycle. Returns
// false only when the index was dropped. A failed cycle is recorded on `forced`, if any.
bool ThreadCycle_Run(IndexGC *gc, GCForcedRun *forced, StrongRef spec_ref);

#endif  // THREAD_CYCLE_H__
