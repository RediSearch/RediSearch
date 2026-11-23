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
#include <stdint.h>

#include "config.h"
#include "gc.h"
#include "module.h"
#include "query_error_ffi.h"
#include "rdb.h"
#include "util/strconv.h"

void IndexGCSettings_Init(IndexGCSettings *settings) {
  *settings = (IndexGCSettings){0};
}

// The accepted range of `id`, or false for an id this binary does not know.
static bool settingRange(IndexGCSettingId id, size_t *min, size_t *max) {
  switch (id) {
    case INDEX_GC_SETTING_RUN_INTERVAL:
      *min = 1;
      *max = GC_MAX_INTERVAL_SEC;
      return true;
    case INDEX_GC_SETTING_CLEAN_THRESHOLD:
      *min = 0;
      *max = LLONG_MAX;
      return true;
  }
  return false;
}

bool IndexGCSettings_IsValid(IndexGCSettingId id, size_t value) {
  size_t min, max;
  return settingRange(id, &min, &max) && value >= min && value <= max;
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

// The settings `GC SET` takes, by keyword. A new setting needs only an entry here, an id and an
// override field.
static const struct {
  const char *name;
  IndexGCSettingId id;
} settingNames[] = {
    {"RUN_INTERVAL", INDEX_GC_SETTING_RUN_INTERVAL},
    {"CLEAN_THRESHOLD", INDEX_GC_SETTING_CLEAN_THRESHOLD},
};

// A non-negative decimal integer, digits only: no sign, fraction or exponent, so "-1" and "1.5"
// are rejected rather than wrapped or truncated.
static bool parseUnsigned(const char *str, size_t len, size_t *out) {
  if (len == 0) {
    return false;
  }
  size_t value = 0;
  for (size_t i = 0; i < len; ++i) {
    if (str[i] < '0' || str[i] > '9') {
      return false;
    }
    const size_t digit = (size_t)(str[i] - '0');
    if (value > (SIZE_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  *out = value;
  return true;
}

int IndexGCSettings_ParseSet(ArgsCursor *ac, IndexGCSettings *settings, bool *setsRunInterval,
                             QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "GC SET requires at least one setting");
    return REDISMODULE_ERR;
  }
  IndexGCSettings updated = *settings;
  bool given[sizeof(settingNames) / sizeof(settingNames[0])] = {0};
  while (!AC_IsAtEnd(ac)) {
    const char *name;
    size_t nameLen;
    AC_GetString(ac, &name, &nameLen, 0);
    size_t i = 0;
    while (i < sizeof(settingNames) / sizeof(settingNames[0]) &&
           !STR_EQCASE(name, nameLen, settingNames[i].name)) {
      ++i;
    }
    if (i == sizeof(settingNames) / sizeof(settingNames[0])) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown GC setting",
                                    " `%.*s`", (int)nameLen, name);
      return REDISMODULE_ERR;
    }
    const char *settingName = settingNames[i].name;
    const IndexGCSettingId id = settingNames[i].id;
    if (given[i]) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                       "GC setting %s given more than once", settingName);
      return REDISMODULE_ERR;
    }
    given[i] = true;

    const char *value;
    size_t valueLen;
    if (AC_GetString(ac, &value, &valueLen, 0) != AC_OK) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                       "Missing value for GC setting %s", settingName);
      return REDISMODULE_ERR;
    }
    IndexGCOverride *override = overrideFor(&updated, id);
    if (STR_EQCASE(value, valueLen, INDEX_GC_SETTING_DEFAULT_STR)) {
      *override = (IndexGCOverride){0};
      continue;
    }
    size_t parsed = 0;
    if (!parseUnsigned(value, valueLen, &parsed) || !IndexGCSettings_IsValid(id, parsed)) {
      size_t min = 0, max = 0;
      settingRange(id, &min, &max);
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                       "Invalid value for GC setting %s: expected an integer from "
                                       "%zu to %zu, or " INDEX_GC_SETTING_DEFAULT_STR,
                                       settingName, min, max);
      return REDISMODULE_ERR;
    }
    *override = (IndexGCOverride){.isSet = true, .value = parsed};
  }
  *settings = updated;
  *setsRunInterval = false;
  for (size_t i = 0; i < sizeof(settingNames) / sizeof(settingNames[0]); ++i) {
    if (given[i] && settingNames[i].id == INDEX_GC_SETTING_RUN_INTERVAL) {
      *setsRunInterval = true;
    }
  }
  return REDISMODULE_OK;
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
