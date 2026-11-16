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
#include "gc.h"
#include "module.h"
#include "rdb.h"

void IndexGCSettings_Init(IndexGCSettings *settings) {
  *settings = (IndexGCSettings){0};
}

bool IndexGCSettings_IsValid(IndexGCSettingId id, size_t value) {
  switch (id) {
    case INDEX_GC_SETTING_RUN_INTERVAL:
      return value >= 1 && value <= GC_MAX_INTERVAL_SEC;
    case INDEX_GC_SETTING_CLEAN_THRESHOLD:
      return value <= LLONG_MAX;
  }
  return false;
}

static size_t effective(const IndexGCOverride *override, size_t global) {
  return override->isSet ? override->value : global;
}

static bool threadPolicy(void) {
  return RSGlobalConfig.gcConfigParams.gcPolicy == GCPolicy_Thread;
}

size_t IndexGCSettings_GlobalRunIntervalSec(void) {
  const GCSettings *global = &RSGlobalConfig.gcConfigParams.gcSettings;
  return threadPolicy() ? global->threadGcRunIntervalSec : global->forkGcRunIntervalSec;
}

size_t IndexGCSettings_GlobalCleanThreshold(void) {
  const GCSettings *global = &RSGlobalConfig.gcConfigParams.gcSettings;
  return threadPolicy() ? global->threadGcCleanThreshold : global->forkGcCleanThreshold;
}

size_t IndexGCSettings_RunIntervalSec(const IndexGCSettings *settings) {
  return effective(&settings->runIntervalSec, IndexGCSettings_GlobalRunIntervalSec());
}

size_t IndexGCSettings_CleanThreshold(const IndexGCSettings *settings) {
  return effective(&settings->cleanThreshold, IndexGCSettings_GlobalCleanThreshold());
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

// Each value is saved as a string buffer, so a loader can skip a setting it does not know
// whatever the value holds. The numeric settings so far hold a little-endian unsigned 64-bit
// number, written byte by byte so the encoding does not depend on the host.
#define NUMERIC_VALUE_LEN 8

static void saveNumericValue(RedisModuleIO *rdb, uint64_t value) {
  char buf[NUMERIC_VALUE_LEN];
  for (size_t i = 0; i < NUMERIC_VALUE_LEN; ++i) {
    buf[i] = (char)(value >> (8 * i));
  }
  RedisModule_SaveStringBuffer(rdb, buf, NUMERIC_VALUE_LEN);
}

static bool decodeNumericValue(const char *buf, size_t len, uint64_t *value) {
  if (len != NUMERIC_VALUE_LEN) {
    return false;
  }
  *value = 0;
  for (size_t i = 0; i < NUMERIC_VALUE_LEN; ++i) {
    *value |= (uint64_t)(unsigned char)buf[i] << (8 * i);
  }
  return true;
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
      saveNumericValue(rdb, all[i].override->value);
    }
  }
}

// Settings skipped as unknown since the last report. Loads run on the main thread.
static uint64_t skippedSinceReport_g = 0;

int IndexGCSettings_RdbLoad(RedisModuleIO *rdb, IndexGCSettings *settings) {
  const uint64_t count = LoadUnsigned_IOError(rdb, return REDISMODULE_ERR);
  for (uint64_t i = 0; i < count; ++i) {
    const uint64_t id = LoadUnsigned_IOError(rdb, return REDISMODULE_ERR);
    size_t len = 0;
    char *buf = LoadStringBuffer_IOError(rdb, &len, return REDISMODULE_ERR);
    IndexGCOverride *override = overrideFor(settings, id);
    if (!override) {
      // Written by a newer binary. Every GC setting only tunes when garbage is collected, never
      // what a query returns, so loading without it is safe.
      RedisModule_Free(buf);
      ++skippedSinceReport_g;
      continue;
    }
    uint64_t value = 0;
    const bool decoded = decodeNumericValue(buf, len, &value);
    RedisModule_Free(buf);
    if (!decoded || override->isSet || !IndexGCSettings_IsValid((IndexGCSettingId)id, value)) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: invalid GC setting %llu",
                             (unsigned long long)id);
      return REDISMODULE_ERR;
    }
    *override = (IndexGCOverride){.isSet = true, .value = value};
  }
  return REDISMODULE_OK;
}

void IndexGCSettings_ReportSkipped(void) {
  if (skippedSinceReport_g) {
    RedisModule_Log(RSDummyContext, "notice",
                    "RDB Load: ignored %llu GC setting(s) unknown to this version",
                    (unsigned long long)skippedSinceReport_g);
    skippedSinceReport_g = 0;
  }
}
