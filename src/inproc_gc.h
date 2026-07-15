/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef SRC_INPROC_GC_H_
#define SRC_INPROC_GC_H_

#include "gc.h"
#include "redismodule.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

/* In-process (fork-less) garbage collector.
 *
 * A distinct GC policy (GCPolicy_InProc), structured like the disk GC: its own
 * self-contained implementation behind the shared GCCallbacks vtable, with no
 * fork() and no pipe. Instead of scanning a copy-on-write child snapshot and
 * streaming a delta back to the parent, it scans and applies on the GC thread,
 * relying on the Arc-refcounted inverted-index blocks for reader safety.
 *
 * Coverage note (experimental): this first version collects inverted-index
 * (text) terms only. */
typedef struct InProcGC InProcGC;

InProcGC *InProcGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

#ifdef __cplusplus
}
#endif

#endif /* SRC_INPROC_GC_H_ */
