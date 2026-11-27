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
#include "rmutil/args.h"

#ifdef __cplusplus
extern "C" {
#endif

struct QueryError;

// A per-index override of a global GC setting. An unset override inherits the global value as
// it is when read, so a later CONFIG SET reaches the index. 0 is a valid value for some
// settings, which is why unset is a flag of its own. A zeroed override is unset.
typedef struct {
  bool isSet;
  size_t value;
} IndexGCOverride;

// The GC settings an index may override. Written only on the main thread under the GIL. The
// interval is read only there; the threshold and the ratio reach the GC thread only through the
// copy the GC takes when it queues a run, so none needs a lock.
typedef struct {
  IndexGCOverride runIntervalSec;  // overrides the active policy's run interval
  IndexGCOverride cleanThreshold;  // overrides the active policy's absolute clean threshold
  IndexGCOverride cleanRatioPpm;   // overrides the active policy's clean ratio
} IndexGCSettings;

// Ids of the settings in the RDB override block. Fixed: never renumber or reuse one, since an
// older binary skips ids it does not know and a newer one must not misread an older RDB.
typedef enum {
  INDEX_GC_SETTING_RUN_INTERVAL = 1,
  INDEX_GC_SETTING_CLEAN_THRESHOLD = 2,
  INDEX_GC_SETTING_CLEAN_RATIO_PPM = 3,
} IndexGCSettingId;

// Leaves every setting unset.
void IndexGCSettings_Init(IndexGCSettings *settings);

// Whether `value` is in range for `id`. A run interval goes up to GC_MAX_INTERVAL_SEC, the longest
// a timer arms, so no accepted value is silently capped; a clean threshold and a clean ratio take
// the range of their global configs. False for an id this binary does not know.
bool IndexGCSettings_IsValid(IndexGCSettingId id, size_t value);

// The value word that clears an override, so the setting inherits the global value again.
#define INDEX_GC_SETTING_DEFAULT_STR "DEFAULT"

// Parses the settings that follow `GC SET`, `<name> <value|DEFAULT>` pairs to the end of `ac`,
// and applies them to `settings`. All or nothing: on an error `settings` is left as it was. It
// takes at least one setting, each at most once, in any order, and leaves the settings it is
// not given as they are. `*setsRunInterval` tells whether RUN_INTERVAL was given, with a value
// or DEFAULT, since only that setting affects a waiting timer.
int IndexGCSettings_ParseSet(ArgsCursor *ac, IndexGCSettings *settings, bool *setsRunInterval,
                             struct QueryError *status);

// The global value an unset override inherits: the search-thread-gc-* config under the THREAD
// policy, the search-fork-gc-* one otherwise. The THREAD policy's clean ratio is
// search-gc-clean-ratio-ppm, which predates the per-policy names.
size_t IndexGCSettings_GlobalRunIntervalSec(void);
size_t IndexGCSettings_GlobalCleanThreshold(void);
size_t IndexGCSettings_GlobalCleanRatioPpm(void);

// The value the index uses: its override if set, otherwise the global value above.
size_t IndexGCSettings_RunIntervalSec(const IndexGCSettings *settings);
size_t IndexGCSettings_CleanThreshold(const IndexGCSettings *settings);
size_t IndexGCSettings_CleanRatioPpm(const IndexGCSettings *settings);

// The override block: a count, then one (setting id, value) pair per set override, the value
// saved as a string buffer of any length. Load skips ids it does not know, whatever their
// value, so a later setting needs no encoding version bump; it fails on a known id whose value
// is malformed, out of range or given twice. Skipped ids are counted until the next
// IndexGCSettings_ReportSkipped.
void IndexGCSettings_RdbSave(RedisModuleIO *rdb, const IndexGCSettings *settings);
int IndexGCSettings_RdbLoad(RedisModuleIO *rdb, IndexGCSettings *settings);

// Logs once how many settings the loads since the last call skipped, if any. Called when a
// whole RDB load or a single serialized spec load ends, so a load logs one line however many
// indexes it holds.
void IndexGCSettings_ReportSkipped(void);

#ifdef __cplusplus
}
#endif
#endif /* SRC_GC_INDEX_GC_SETTINGS_H_ */
