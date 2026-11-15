/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef SRC_GC_INDEX_GC_SETTINGS_H_
#define SRC_GC_INDEX_GC_SETTINGS_H_

#include <stdbool.h>
#include <stddef.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// A per-index override of a global GC setting. An unset override inherits the global value as
// it is when read, so a later CONFIG SET reaches the index. 0 is a valid value for some
// settings, which is why unset is a flag of its own. A zeroed override is unset.
typedef struct {
  bool isSet;
  size_t value;
} IndexGCOverride;

// The GC settings an index may override. Written only on the main thread under the GIL. The
// interval is read only there; the threshold reaches the GC thread only through the copy the
// GC takes when it queues a run, so neither needs a lock.
typedef struct {
  IndexGCOverride runIntervalSec;  // overrides search-fork-gc-run-interval
  IndexGCOverride cleanThreshold;  // overrides search-fork-gc-clean-threshold
} IndexGCSettings;

// Ids of the settings in the RDB override block. Fixed: never renumber or reuse one, since an
// older binary skips ids it does not know and a newer one must not misread an older RDB.
typedef enum {
  INDEX_GC_SETTING_RUN_INTERVAL = 1,
  INDEX_GC_SETTING_CLEAN_THRESHOLD = 2,
} IndexGCSettingId;

// Leaves every setting unset.
void IndexGCSettings_Init(IndexGCSettings *settings);

// Whether `value` is in range for `id`, the same range as the matching global config.
// False for an id this binary does not know.
bool IndexGCSettings_IsValid(IndexGCSettingId id, size_t value);

// The value the index uses: its override if set, otherwise the global value.
size_t IndexGCSettings_RunIntervalSec(const IndexGCSettings *settings);
size_t IndexGCSettings_CleanThreshold(const IndexGCSettings *settings);

// The override block: a count, then one (setting id, value) pair per set override. Load
// skips ids it does not know, so a later setting needs no encoding version bump, and fails
// on a known id with a value out of range or given twice.
void IndexGCSettings_RdbSave(RedisModuleIO *rdb, const IndexGCSettings *settings);
int IndexGCSettings_RdbLoad(RedisModuleIO *rdb, IndexGCSettings *settings);

#ifdef __cplusplus
}
#endif
#endif /* SRC_GC_INDEX_GC_SETTINGS_H_ */
