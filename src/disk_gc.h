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

#include "redismodule.h"
#include "gc.h"
#include "util/references.h"
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Internal definition of the disk GC context (each disk index has one).
 * Stats are maintained in disk info; we do not duplicate them here. */
typedef struct DiskGC {
  WeakRef index;
  atomic_size_t intervalSec;
  atomic_size_t deletedDocsFromLastRun;
} DiskGC;

DiskGC *DiskGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

#ifdef __cplusplus
}
#endif

#endif /* SRC_DISK_GC_H_ */
