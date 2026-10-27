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

// One collection cycle of the in-process policy: the scanners run on the calling GC pool thread
// under the spec's own read/write lock, through the Rust driver, instead of in a forked child.
// Consumes `spec_ref`; the driver promotes the weak reference again for every lock it takes, so
// a drop mid-cycle just ends the cycle. Returns false only when the index was dropped.
bool ThreadCycle_Run(IndexGC *gc, StrongRef spec_ref);

#endif  // THREAD_CYCLE_H__
