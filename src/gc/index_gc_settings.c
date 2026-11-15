/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "index_gc_settings.h"

#include <limits.h>

#include "config.h"
#include "module.h"
#include "rdb.h"

void IndexGCSettings_Init(IndexGCSettings *settings) {
  *settings = (IndexGCSettings){0};
}

bool IndexGCSettings_IsValid(IndexGCSettingId id, size_t value) {
  switch (id) {
    case INDEX_GC_SETTING_RUN_INTERVAL:
      return value >= 1 && value <= LLONG_MAX;
    case INDEX_GC_SETTING_CLEAN_THRESHOLD:
      return value <= LLONG_MAX;
  }
  return false;
}

static size_t effective(const IndexGCOverride *override, size_t global) {
  return override->isSet ? override->value : global;
}

size_t IndexGCSettings_RunIntervalSec(const IndexGCSettings *settings) {
  return effective(&settings->runIntervalSec,
                   RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec);
}

size_t IndexGCSettings_CleanThreshold(const IndexGCSettings *settings) {
  return effective(&settings->cleanThreshold,
                   RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold);
}

// The override for `id`, or NULL for an id this binary does not know.
static IndexGCOverride *overrideFor(IndexGCSettings *settings, uint64_t id) {
  switch (id) {
    case INDEX_GC_SETTING_RUN_INTERVAL:
      return &settings->runIntervalSec;
    case INDEX_GC_SETTING_CLEAN_THRESHOLD:
      return &settings->cleanThreshold;
  }
  return NULL;
}

void IndexGCSettings_RdbSave(RedisModuleIO *rdb, const IndexGCSettings *settings) {
  const struct {
    IndexGCSettingId id;
    const IndexGCOverride *override;
  } all[] = {
      {INDEX_GC_SETTING_RUN_INTERVAL, &settings->runIntervalSec},
      {INDEX_GC_SETTING_CLEAN_THRESHOLD, &settings->cleanThreshold},
  };
  uint64_t count = 0;
  for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); ++i) {
    count += all[i].override->isSet;
  }
  RedisModule_SaveUnsigned(rdb, count);
  for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); ++i) {
    if (all[i].override->isSet) {
      RedisModule_SaveUnsigned(rdb, all[i].id);
      RedisModule_SaveUnsigned(rdb, all[i].override->value);
    }
  }
}

int IndexGCSettings_RdbLoad(RedisModuleIO *rdb, IndexGCSettings *settings) {
  const uint64_t count = LoadUnsigned_IOError(rdb, return REDISMODULE_ERR);
  uint64_t skipped = 0;
  for (uint64_t i = 0; i < count; ++i) {
    const uint64_t id = LoadUnsigned_IOError(rdb, return REDISMODULE_ERR);
    const uint64_t value = LoadUnsigned_IOError(rdb, return REDISMODULE_ERR);
    IndexGCOverride *override = overrideFor(settings, id);
    if (!override) {
      // Written by a newer binary. Every GC setting only tunes when garbage is collected, never
      // what a query returns, so loading without it is safe.
      ++skipped;
      continue;
    }
    if (override->isSet || !IndexGCSettings_IsValid((IndexGCSettingId)id, value)) {
      RedisModule_LogIOError(rdb, "warning",
                             "RDB Load: invalid GC setting %llu (value %llu)",
                             (unsigned long long)id, (unsigned long long)value);
      return REDISMODULE_ERR;
    }
    *override = (IndexGCOverride){.isSet = true, .value = value};
  }
  if (skipped) {
    RedisModule_Log(RSDummyContext, "notice",
                    "RDB Load: ignored %llu GC setting(s) unknown to this version",
                    (unsigned long long)skipped);
  }
  return REDISMODULE_OK;
}
