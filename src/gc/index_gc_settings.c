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
#include <stddef.h>
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

// Every setting an index may override, in the order the RDB block saves them. Parsing and
// persistence read only this table, so a new setting is a row here, an IndexGCSettingId and an
// override field.
typedef struct {
  IndexGCSettingId id;
  const char *name;  // the keyword GC SET takes
  size_t min, max;
  size_t offset;  // of its IndexGCOverride in IndexGCSettings
} SettingDesc;

static const SettingDesc settingDescs[] = {
    {INDEX_GC_SETTING_RUN_INTERVAL, "RUN_INTERVAL", 1, GC_MAX_INTERVAL_SEC,
     offsetof(IndexGCSettings, runIntervalSec)},
    {INDEX_GC_SETTING_CLEAN_THRESHOLD, "CLEAN_THRESHOLD", 0, LLONG_MAX,
     offsetof(IndexGCSettings, cleanThreshold)},
};
#define NUM_SETTINGS (sizeof(settingDescs) / sizeof(settingDescs[0]))

// The descriptor of `id`, or NULL for an id this binary does not know.
static const SettingDesc *descFor(uint64_t id) {
  for (size_t i = 0; i < NUM_SETTINGS; ++i) {
    if ((uint64_t)settingDescs[i].id == id) {
      return &settingDescs[i];
    }
  }
  return NULL;
}

static IndexGCOverride *overrideOf(IndexGCSettings *settings, const SettingDesc *desc) {
  return (IndexGCOverride *)((char *)settings + desc->offset);
}

static const IndexGCOverride *constOverrideOf(const IndexGCSettings *settings,
                                              const SettingDesc *desc) {
  return (const IndexGCOverride *)((const char *)settings + desc->offset);
}

static bool inRange(const SettingDesc *desc, size_t value) {
  return value >= desc->min && value <= desc->max;
}

bool IndexGCSettings_IsValid(IndexGCSettingId id, size_t value) {
  const SettingDesc *desc = descFor(id);
  return desc && inRange(desc, value);
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
  bool given[NUM_SETTINGS] = {0};
  while (!AC_IsAtEnd(ac)) {
    const char *name;
    size_t nameLen;
    AC_GetString(ac, &name, &nameLen, 0);
    size_t i = 0;
    while (i < NUM_SETTINGS && !STR_EQCASE(name, nameLen, settingDescs[i].name)) {
      ++i;
    }
    if (i == NUM_SETTINGS) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown GC setting",
                                    " `%.*s`", (int)nameLen, name);
      return REDISMODULE_ERR;
    }
    const SettingDesc *desc = &settingDescs[i];
    const char *settingName = desc->name;
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
    IndexGCOverride *override = overrideOf(&updated, desc);
    if (STR_EQCASE(value, valueLen, INDEX_GC_SETTING_DEFAULT_STR)) {
      *override = (IndexGCOverride){0};
      continue;
    }
    size_t parsed = 0;
    if (!parseUnsigned(value, valueLen, &parsed) || !inRange(desc, parsed)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                       "Invalid value for GC setting %s: expected an integer from "
                                       "%zu to %zu, or " INDEX_GC_SETTING_DEFAULT_STR,
                                       settingName, desc->min, desc->max);
      return REDISMODULE_ERR;
    }
    *override = (IndexGCOverride){.isSet = true, .value = parsed};
  }
  *settings = updated;
  *setsRunInterval = false;
  for (size_t i = 0; i < NUM_SETTINGS; ++i) {
    if (given[i] && settingDescs[i].id == INDEX_GC_SETTING_RUN_INTERVAL) {
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
  uint64_t count = 0;
  for (size_t i = 0; i < NUM_SETTINGS; ++i) {
    count += constOverrideOf(settings, &settingDescs[i])->isSet;
  }
  RedisModule_SaveUnsigned(rdb, count);
  for (size_t i = 0; i < NUM_SETTINGS; ++i) {
    const IndexGCOverride *override = constOverrideOf(settings, &settingDescs[i]);
    if (override->isSet) {
      RedisModule_SaveUnsigned(rdb, settingDescs[i].id);
      saveNumericValue(rdb, override->value);
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
    const SettingDesc *desc = descFor(id);
    if (!desc) {
      // Written by a newer binary. Every GC setting only tunes when garbage is collected, never
      // what a query returns, so loading without it is safe.
      RedisModule_Free(buf);
      ++skippedSinceReport_g;
      continue;
    }
    uint64_t value = 0;
    const bool decoded = decodeNumericValue(buf, len, &value);
    RedisModule_Free(buf);
    IndexGCOverride *override = overrideOf(settings, desc);
    if (!decoded || override->isSet || !inRange(desc, value)) {
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
