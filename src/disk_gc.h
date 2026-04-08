/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef SRC_DISK_GC_H_
#define SRC_DISK_GC_H_

#include <stddef.h>           // for size_t

#include "redismodule.h"
#include "gc.h"               // for GCCallbacks
#include "util/references.h"  // for StrongRef, WeakRef

#ifdef __cplusplus
#include <atomic>

#define RS_Atomic(T) std::atomic<T>
extern "C" {
#else
#define RS_Atomic(T) _Atomic(T)
#include <stdatomic.h>
#endif

/* Internal definition of the disk GC context (each disk index has one).
 * Stats are maintained in disk info; we do not duplicate them here. */
typedef struct DiskGC {
  WeakRef index;
  RS_Atomic(size_t) intervalSec;
  // Tracks only writes since last GC run (no updates)
  RS_Atomic(size_t) writesFromLastRun;
  // Tracks only deletes for global stats (no updates)
  RS_Atomic(size_t) deletesFromLastRun;
  // Tracks only updates for global stats (no pure writes or deletes)
  RS_Atomic(size_t) updatesFromLastRun;
} DiskGC;

DiskGC *DiskGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

#ifdef __cplusplus
}
#endif

#endif /* SRC_DISK_GC_H_ */
