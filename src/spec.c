/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"

#include <math.h>
#include <ctype.h>

#include "triemap.h"
#include "util/logging.h"
#include "util/misc.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "rmutil/rm_assert.h"
#include "trie/trie_type.h"
#include "rmalloc.h"
#include "config.h"
#include "cursor.h"
#include "tag_index.h"
#include "redis_index.h"
#include "indexer.h"
#include "suffix.h"
#include "alias.h"
#include "module.h"
#include "aggregate/expr/expression.h"
#include "rules.h"
#include "dictionary.h"
#include "doc_types.h"
#include "rdb.h"
#include "commands.h"
#include "obfuscation/obfuscation_api.h"
#include "util/workers.h"
#include "info/global_stats.h"
#include "debug_commands.h"
#include "info/info_redis/threads/current_thread.h"
#include "obfuscation/obfuscation_api.h"
#include "util/hash/hash.h"
#include "reply_macros.h"
#include "notifications.h"
#include "info/field_spec_info.h"
#include "rs_wall_clock.h"
#include "util/redis_mem_info.h"
#include "search_disk.h"

#define INITIAL_DOC_TABLE_SIZE 1000

///////////////////////////////////////////////////////////////////////////////////////////////

const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;

dict *specDict_g;
IndexesScanner *global_spec_scanner = NULL;
size_t pending_global_indexing_ops = 0;
dict *legacySpecDict;
dict *legacySpecRules;

// Pending or in-progress index drops
uint16_t pendingIndexDropCount_g = 0;

Version redisVersion;
Version rlecVersion;
bool isCrdt;
bool should_filter_slots = false;
bool isTrimming = false;
bool isFlex = false;

// Default values make no limits.
size_t memoryLimit = -1;
size_t used_memory = 0;

static redisearch_thpool_t *cleanPool = NULL;

extern DebugCTX globalDebugCtx;

const char *DEBUG_INDEX_SCANNER_STATUS_STRS[] = {
    "NEW", "SCANNING", "DONE", "CANCELLED", "PAUSED", "RESUMED", "PAUSED_ON_OOM", "PAUSED_BEFORE_OOM_RETRY",
};

// Static assertion to ensure array size matches the number of statuses
static_assert(
    (sizeof(DEBUG_INDEX_SCANNER_STATUS_STRS) / sizeof(char*)) == DEBUG_INDEX_SCANNER_CODE_COUNT,
    "Mismatch between DebugIndexScannerCode enum and DEBUG_INDEX_SCANNER_STATUS_STRS array"
);

// Debug scanner functions
static DebugIndexesScanner *DebugIndexesScanner_New(StrongRef global_ref);
static void DebugIndexesScanner_Free(DebugIndexesScanner *dScanner);
static void DebugIndexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             DebugIndexesScanner *dScanner);
static void DebugIndexesScanner_pauseCheck(DebugIndexesScanner* dScanner, RedisModuleCtx *ctx, bool pauseField, DebugIndexScannerCode code);

//---------------------------------------------------------------------------------------------

// This function should be called after the first background scan OOM error
// It will wait for resource manager to allocate more memory to the process if possible
// and after the function returns, the scan will continue
static inline void threadSleepByConfigTime(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  // Thread sleep based on the config
  uint32_t sleepTime = RSGlobalConfig.bgIndexingOomPauseTimeBeforeRetry;
  RedisModule_Log(ctx, "notice", "Scanning index %s in background: paused for %u seconds due to OOM, waiting for memory allocation",
                  scanner->spec_name_for_logs, sleepTime);

  RedisModule_ThreadSafeContextUnlock(ctx);
  sleep(sleepTime);
  RedisModule_ThreadSafeContextLock(ctx);
}

// This function should be called after the second background scan OOM error
// It will stop the background scan process
static inline void scanStopAfterOOM(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  char* error;
  rm_asprintf(&error, "Used memory is more than %u percent of max memory, cancelling the scan",RSGlobalConfig.indexingMemoryLimit);
  RedisModule_Log(ctx, "warning", "%s", error);

    // We need to report the error message besides the log, so we can show it in FT.INFO
  if(!scanner->global) {
    scanner->cancelled = true;
    StrongRef curr_run_ref = WeakRef_Promote(scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(curr_run_ref);
    if (sp) {
      sp->scan_failed_OOM = true;
      // Error message does not contain user data
      IndexError_AddError(&sp->stats.indexError, error, error, scanner->OOMkey);
      IndexError_RaiseBackgroundIndexFailureFlag(&sp->stats.indexError);
      StrongRef_Release(curr_run_ref);
    } else {
      // spec was deleted
      RedisModule_Log(ctx, "notice", "Scanning index %s in background: cancelled due to OOM and index was dropped",
                    scanner->spec_name_for_logs);
      }
    }
    rm_free(error);
}


static void setMemoryInfo(RedisModuleCtx *ctx) {
  #define MIN_NOT_0(a,b) (((a)&&(b))?MIN((a),(b)):MAX((a),(b)))
  RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "memory");

  size_t maxmemory = RedisModule_ServerInfoGetFieldUnsigned(info, "maxmemory", NULL);
  size_t max_process_mem = RedisModule_ServerInfoGetFieldUnsigned(info, "max_process_mem", NULL); // Enterprise limit
  maxmemory = MIN_NOT_0(maxmemory, max_process_mem);

  size_t total_system_memory = RedisModule_ServerInfoGetFieldUnsigned(info, "total_system_memory", NULL);
  memoryLimit = MIN_NOT_0(maxmemory, total_system_memory);

  used_memory = RedisModule_ServerInfoGetFieldUnsigned(info, "used_memory", NULL);

  RedisModule_FreeServerInfo(ctx, info);
}

// Return true if used_memory exceeds (indexingMemoryLimit % × memoryLimit); false if within bounds or limit is 0.
static inline bool isBgIndexingMemoryOverLimit(RedisModuleCtx *ctx) {
  // if memory limit is set to 0, we don't need to check for memory usage
  if(RSGlobalConfig.indexingMemoryLimit == 0) {
    return false;
  }

  float used_memory_ratio = RedisMemory_GetUsedMemoryRatioUnified(ctx);
  float memory_limit_ratio = (float)RSGlobalConfig.indexingMemoryLimit / 100;

  return (used_memory_ratio > memory_limit_ratio) ;
}
/*
 * Initialize the spec's fields that are related to the cursors.
 */

static void Cursors_initSpec(IndexSpec *spec) {
  spec->activeCursors = 0;
}

/*
 * Get a field spec by field name. Case sensitive!
 * Return the field spec if found, NULL if not.
 * Assuming the spec is properly locked before calling this function.
 */
const FieldSpec *IndexSpec_GetFieldWithLength(const IndexSpec *spec, const char *name, size_t len) {
  for (size_t i = 0; i < spec->numFields; i++) {
    const FieldSpec *fs = spec->fields + i;
    if (!HiddenString_CompareC(fs->fieldName, name, len)) {
      return fs;
    }
  }
  return NULL;
}

const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const HiddenString *name) {
  for (size_t i = 0; i < spec->numFields; i++) {
    const FieldSpec *fs = spec->fields + i;
    if (!HiddenString_Compare(fs->fieldName, name)) {
      return fs;
    }
  }
  return NULL;
}

// Assuming the spec is properly locked before calling this function.
t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, name, len);
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_FULLTEXT) || !FieldSpec_IsIndexable(fs)) return 0;

  return FIELD_BIT(fs);
}

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CheckPhoneticEnabled(const IndexSpec *sp, t_fieldMask fm) {
  if (!(sp->flags & Index_HasPhonetic)) {
    return 0;
  }

  if (fm == 0 || fm == (t_fieldMask)-1) {
    // No fields -- implicit phonetic match!
    return 1;
  }

  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec *fs = sp->fields + ii;
      if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && (FieldSpec_IsPhonetics(fs))) {
        return 1;
      }
    }
  }
  return 0;
}

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CheckAllowSlopAndInorder(const IndexSpec *spec, t_fieldMask fm, QueryError *status) {
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec *fs = spec->fields + ii;
      if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && (FieldSpec_IsUndefinedOrder(fs))) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_BAD_ORDER_OPTION,
                               "slop/inorder are not supported for field with undefined ordering", " `%s`", HiddenString_GetUnsafe(fs->fieldName, NULL));
        return 0;
      }
    }
  }
  return 1;
}

// Assuming the spec is properly locked before calling this function.
const FieldSpec *IndexSpec_GetFieldBySortingIndex(const IndexSpec *sp, uint16_t idx) {
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (sp->fields[ii].options & FieldSpec_Sortable && sp->fields[ii].sortIdx == idx) {
      return sp->fields + ii;
    }
  }
  return NULL;
}

// Assuming the spec is properly locked before calling this function.
const char *IndexSpec_GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (FIELD_BIT(&sp->fields[i]) == id && FIELD_IS(&sp->fields[i], INDEXFLD_T_FULLTEXT) &&
      FieldSpec_IsIndexable(&sp->fields[i])) {
      return HiddenString_GetUnsafe(sp->fields[i].fieldName, NULL);
    }
  }
  return NULL;
}

// Get the field spec by the field mask.
const FieldSpec *IndexSpec_GetFieldByBit(const IndexSpec *sp, t_fieldMask id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (FIELD_BIT(&sp->fields[i]) == id && FIELD_IS(&sp->fields[i], INDEXFLD_T_FULLTEXT) &&
        FieldSpec_IsIndexable(&sp->fields[i])) {
      return &sp->fields[i];
    }
  }
  return NULL;
}

// Get the field specs that match a field mask.
arrayof(FieldSpec *) IndexSpec_GetFieldsByMask(const IndexSpec *sp, t_fieldMask mask) {
  arrayof(FieldSpec *) res = array_new(FieldSpec *, 2);
  for (int i = 0; i < sp->numFields; i++) {
    if (mask & FIELD_BIT(sp->fields + i) && FIELD_IS(sp->fields + i, INDEXFLD_T_FULLTEXT)) {
      array_append(res, sp->fields + i);
    }
  }
  return res;
}

//---------------------------------------------------------------------------------------------

/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relevant part of argv.
*
* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS] [NOFREQS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
*/
StrongRef IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, const HiddenString *name,
                                    RedisModuleString **argv, int argc, QueryError *status) {

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  return IndexSpec_Parse(name, args, argc, status);
}

arrayof(FieldSpec *) getFieldsByType(IndexSpec *spec, FieldType type) {
#define FIELDS_ARRAY_CAP 2
  arrayof(FieldSpec *) fields = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (FIELD_IS(spec->fields + i, type)) {
      array_append(fields, &(spec->fields[i]));
    }
  }
  return fields;
}

/* Check if Redis is currently loading from RDB. Our thread starts before RDB loading is finished */
int isRdbLoading(RedisModuleCtx *ctx) {
  long long isLoading = 0;
  RMUtilInfo *info = RMUtil_GetRedisInfo(ctx);
  if (!info) {
    return 0;
  }

  if (!RMUtilInfo_GetInt(info, "loading", &isLoading)) {
    isLoading = 0;
  }

  RMUtilRedisInfo_Free(info);
  return isLoading == 1;
}

//---------------------------------------------------------------------------------------------

void IndexSpec_LegacyFree(void *spec) {
  // free legacy index do nothing, it will be called only
  // when the index key will be deleted and we keep the legacy
  // index pointer in the legacySpecDict so we will free it when needed
}

static void IndexSpec_TimedOutProc(RedisModuleCtx *ctx, WeakRef w_ref) {
  // we need to delete the spec from the specDict_g, as far as the user see it,
  // this spec was deleted and its memory will be freed in a background thread.

  // attempt to promote the weak ref to a strong ref
  StrongRef spec_ref = WeakRef_Promote(w_ref);
  WeakRef_Release(w_ref);

  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    // the spec was already deleted, nothing to do here
    return;
  }
  const char* name = IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog);
  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "Freeing index %s by timer", name);

  sp->isTimerSet = false;
  if (RS_IsMock) {
    IndexSpec_Free(sp);
  } else {
    // called on master shard for temporary indexes and deletes all documents by defaults
    // pass FT.DROPINDEX with "DD" flag to self.
    RedisModuleCallReply *rep = RedisModule_Call(RSDummyContext, RS_DROP_INDEX_CMD, "cc!", HiddenString_GetUnsafe(sp->specName, NULL), "DD");
    if (rep) {
      RedisModule_FreeCallReply(rep);
    }
  }

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "Freeing index '%s' by timer: done", name);
  StrongRef_Release(spec_ref);
}

// Assuming the GIL is held.
// This can be done without locking the spec for write, since the timer is not modified or read by any other thread.
static void IndexSpec_SetTimeoutTimer(IndexSpec *sp, WeakRef spec_ref) {
  if (sp->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, sp->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
  }
  sp->timerId = RedisModule_CreateTimer(RSDummyContext, sp->timeout,
                                        (RedisModuleTimerProc)IndexSpec_TimedOutProc, spec_ref.rm);
  sp->isTimerSet = true;
}

// Assuming the spec is properly guarded before calling this function (GIL or write lock).
static void IndexSpec_ResetTimeoutTimer(IndexSpec *sp) {
  if (sp->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, sp->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
  }
  sp->timerId = 0;
  sp->isTimerSet = false;
}

// Assuming the GIL is locked before calling this function.
void Indexes_SetTempSpecsTimers(TimerOp op) {
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    if (sp->flags & Index_Temporary) {
      switch (op) {
        case TimerOp_Add: IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref)); break;
        case TimerOp_Del: IndexSpec_ResetTimeoutTimer(sp);    break;
      }
    }
  }
  dictReleaseIterator(iter);
}

//---------------------------------------------------------------------------------------------

double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp) {
  if (scanner || sp->scan_in_progress) {
    if (scanner) {
      size_t totalKeys = RedisModule_DbSize(ctx);
      return totalKeys > 0 ? (double)scanner->scannedKeys / totalKeys : 0;
    } else {
      return 0;
    }
  } else {
    return 1.0;
  }
}

size_t IndexSpec_collect_numeric_overhead(IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the numeric tree index
  size_t overhead = 0;
  for (size_t i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC) || FIELD_IS(fs, INDEXFLD_T_GEO)) {
      RedisModuleString *keyName = IndexSpec_GetFormattedKey(sp, fs, fs->types);
      NumericRangeTree *rt = openNumericKeysDict(sp, keyName, DONT_CREATE_INDEX);
      // Numeric index was not initialized yet
      if (!rt) {
        continue;
      }

      overhead += sizeof(NumericRangeTree);
    }
  }
  return overhead;
}

size_t IndexSpec_collect_tags_overhead(const IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the tags
  size_t overhead = 0;
  for (size_t i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      overhead += TagIndex_GetOverhead(sp, fs);
    }
  }
  return overhead;
}

size_t IndexSpec_collect_text_overhead(const IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the text suffixes
  size_t overhead = 0;
  // Collect overhead from sp->terms
  overhead += TrieType_MemUsage(sp->terms);
  // Collect overhead from sp->suffix
  if (sp->suffix) {
    // TODO: Count the values' memory as well
    overhead += TrieType_MemUsage(sp->suffix);
  }
  return overhead;
}

size_t IndexSpec_TotalMemUsage(IndexSpec *sp, size_t doctable_tm_size, size_t tags_overhead,
  size_t text_overhead, size_t vector_overhead) {
  size_t res = 0;
  res += sp->docs.memsize;
  res += sp->docs.sortablesSize;
  res += doctable_tm_size ? doctable_tm_size : TrieMap_MemUsage(sp->docs.dim.tm);
  res += text_overhead ? text_overhead :  IndexSpec_collect_text_overhead(sp);
  res += tags_overhead ? tags_overhead : IndexSpec_collect_tags_overhead(sp);
  res += IndexSpec_collect_numeric_overhead(sp);
  res += sp->stats.invertedSize;
  res += sp->stats.offsetVecsSize;
  res += sp->stats.termsSize;
  res += vector_overhead;
  return res;
}

const char *IndexSpec_FormatName(const IndexSpec *sp, bool obfuscate) {
    return obfuscate ? sp->obfuscatedName : HiddenString_GetUnsafe(sp->specName, NULL);
}

char *IndexSpec_FormatObfuscatedName(const HiddenString *specName) {
  Sha1 sha1;
  size_t len;
  const char* value = HiddenString_GetUnsafe(specName, &len);
  Sha1_Compute(value, len, &sha1);
  char buffer[MAX_OBFUSCATED_INDEX_NAME];
  Obfuscate_Index(&sha1, buffer);
  return rm_strdup(buffer);
}

static bool checkIfSpecExists(const char *rawSpecName) {
  bool found = false;
  HiddenString* specName = NewHiddenString(rawSpecName, strlen(rawSpecName), false);
  found = dictFetchValue(specDict_g, specName);
  HiddenString_Free(specName, false);
  return found;
}

//---------------------------------------------------------------------------------------------

/* Create a new index spec from a redis command */
// TODO: multithreaded: use global metadata locks to protect global data structures
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status) {
  const char *rawSpecName = RedisModule_StringPtrLen(argv[1], NULL);
  setMemoryInfo(ctx);
  if (checkIfSpecExists(rawSpecName)) {
    QueryError_SetCode(status, QUERY_ERROR_CODE_INDEX_EXISTS);
    return NULL;
  }
  size_t nameLen;
  const char *rawName = RedisModule_StringPtrLen(argv[1], &nameLen);
  HiddenString *name = NewHiddenString(rawName, nameLen, true);
  // Create the IndexSpec, along with its corresponding weak\strong refs
  StrongRef spec_ref = IndexSpec_ParseRedisArgs(ctx, name, &argv[2], argc - 2, status);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (sp == NULL) {
    return NULL;
  }

  // Add the spec to the global spec dictionary
  if (dictAdd(specDict_g, name, spec_ref.rm) != DICT_OK) {
    RedisModule_Log(ctx, "warning", "Failed adding index to global dictionary");
    StrongRef_Release(spec_ref);
    RS_ABORT("dictAdd shouldn't fail here - index shouldn't exists in the dictionary");
    return NULL;
  }
  // Start the garbage collector
  IndexSpec_StartGC(spec_ref, sp);

  Cursors_initSpec(sp);

  // set timeout for temporary index on master
  if ((sp->flags & Index_Temporary) && IsMaster()) {
    IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref));
  }

  // (Lazily) Subscribe to keyspace notifications, now that we have at least one
  // spec
  Initialize_KeyspaceNotifications();

  if (!(sp->flags & Index_SkipInitialScan)) {
    IndexSpec_ScanAndReindex(ctx, spec_ref);
  }
  return sp;
}

//---------------------------------------------------------------------------------------------

static bool checkPhoneticAlgorithmAndLang(const char *matcher) {
  if (strlen(matcher) != 5) {
    return false;
  }
  if (matcher[0] != 'd' || matcher[1] != 'm' || matcher[2] != ':') {
    return false;
  }

#define LANGUAGES_SIZE 4
  char *languages[] = {"en", "pt", "fr", "es"};

  bool langauge_found = false;
  for (int i = 0; i < LANGUAGES_SIZE; ++i) {
    if (matcher[3] == languages[i][0] && matcher[4] == languages[i][1]) {
      langauge_found = true;
    }
  }

  return langauge_found;
}

// Tries to get vector data type from ac. This function need to stay updated with
// the supported vector data types list of VecSim.
static int parseVectorField_GetType(ArgsCursor *ac, VecSimType *type) {
  const char *typeStr;
  size_t len;
  int rc;
  if ((rc = AC_GetString(ac, &typeStr, &len, 0)) != AC_OK) {
    return rc;
  }
  // Uncomment these when support for other type is added.
  if (STR_EQCASE(typeStr, len, VECSIM_TYPE_FLOAT32))
    *type = VecSimType_FLOAT32;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_FLOAT64))
    *type = VecSimType_FLOAT64;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_FLOAT16))
    *type = VecSimType_FLOAT16;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_BFLOAT16))
    *type = VecSimType_BFLOAT16;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_UINT8))
    *type = VecSimType_UINT8;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_INT8))
    *type = VecSimType_INT8;
  // else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_INT32))
  //   *type = VecSimType_INT32;
  // else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_INT64))
  //   *type = VecSimType_INT64;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// Tries to get distance metric from ac. This function need to stay updated with
// the supported distance metric functions list of VecSim.
static int parseVectorField_GetMetric(ArgsCursor *ac, VecSimMetric *metric) {
  const char *metricStr;
  int rc;
  if ((rc = AC_GetString(ac, &metricStr, NULL, 0)) != AC_OK) {
    return rc;
  }
  if (!strcasecmp(VECSIM_METRIC_IP, metricStr))
    *metric = VecSimMetric_IP;
  else if (!strcasecmp(VECSIM_METRIC_L2, metricStr))
    *metric = VecSimMetric_L2;
  else if (!strcasecmp(VECSIM_METRIC_COSINE, metricStr))
    *metric = VecSimMetric_Cosine;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// Parsing for Quantization parameter in SVS algorithm
static int parseVectorField_GetQuantBits(ArgsCursor *ac, VecSimSvsQuantBits *quantBits) {
  const char *quantBitsStr;
  size_t len;
  int rc;
  if ((rc = AC_GetString(ac, &quantBitsStr, &len, 0)) != AC_OK) {
    return rc;
  }
  if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_8))
    *quantBits = VecSimSvsQuant_8;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_4))
    *quantBits = VecSimSvsQuant_4;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_4X4))
    *quantBits = VecSimSvsQuant_4x4;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_4X8))
    *quantBits = VecSimSvsQuant_4x8;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LEANVEC_4X8))
    *quantBits = VecSimSvsQuant_4x8_LeanVec;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LEANVEC_8X8))
    *quantBits = VecSimSvsQuant_8x8_LeanVec;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// memoryLimit / 10 - default is 10% of global memory limit
#define ACTUAL_MEMORY_LIMIT ((memoryLimit == 0) ? SIZE_MAX : memoryLimit)
#define BLOCK_MEMORY_LIMIT ((RSGlobalConfig.vssMaxResize) ? RSGlobalConfig.vssMaxResize : ACTUAL_MEMORY_LIMIT / 10)

static int parseVectorField_validate_hnsw(VecSimParams *params, QueryError *status) {
  // BLOCK_SIZE is deprecated and not respected when set by user as of INDEX_VECSIM_SVS_VAMANA_VERSION.
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  params->algoParams.hnswParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);
  if (params->algoParams.hnswParams.blockSize == 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Vector index element size",
      " %zu exceeded maximum size allowed by server limit which is %zu", elementSize, maxBlockSize);
    return 0;
  }
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.hnswParams.blockSize;

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_NOTICE,
    "Creating vector index of type HNSW. Required memory for a block of %zu vectors: %zuB",
    params->algoParams.hnswParams.blockSize,  index_size_estimation);
  return 1;
}

static int parseVectorField_validate_flat(VecSimParams *params, QueryError *status) {
  // BLOCK_SIZE is deprecated and not respected when set by user as of INDEX_VECSIM_SVS_VAMANA_VERSION.
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  params->algoParams.bfParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);
  if (params->algoParams.bfParams.blockSize == 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Vector index element size",
      " %zu exceeded maximum size allowed by server limit which is %zu", elementSize, maxBlockSize);
    return 0;
  }
  // Calculating index size estimation, after first vector block was allocated.
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.bfParams.blockSize;

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_NOTICE,
    "Creating vector index of type FLAT. Required memory for a block of %zu vectors: %zuB",
    params->algoParams.bfParams.blockSize, index_size_estimation);
  return 1;
}

static int parseVectorField_validate_svs(VecSimParams *params, QueryError *status) {
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  // Block size should be min(maxBlockSize, DEFAULT_BLOCK_SIZE)
  params->algoParams.svsParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);

  // Calculating index size estimation, after first vector block was allocated.
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.svsParams.blockSize;
  if (params->algoParams.svsParams.blockSize == 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Vector index element size",
      " %zu exceeded maximum size allowed by server limit which is %zu", elementSize, maxBlockSize);
    return 0;
  }
  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_NOTICE,
    "Creating vector index of type SVS-VAMANA. Required memory for a block of %zu vectors: %zuB",
    params->algoParams.svsParams.blockSize,  index_size_estimation);
  return 1;
}

int VecSimIndex_validate_params(RedisModuleCtx *ctx, VecSimParams *params, QueryError *status) {
  setMemoryInfo(ctx);
  bool valid = false;
  if (VecSimAlgo_HNSWLIB == params->algo) {
    valid = parseVectorField_validate_hnsw(params, status);
  } else if (VecSimAlgo_BF == params->algo) {
    valid = parseVectorField_validate_flat(params, status);
  } else if (VecSimAlgo_SVS == params->algo) {
    valid = parseVectorField_validate_svs(params, status);
  } else if (VecSimAlgo_TIERED == params->algo) {
    return VecSimIndex_validate_params(ctx, params->algoParams.tieredParams.primaryIndexParams, status);
  }
  return valid ? REDISMODULE_OK : REDISMODULE_ERR;
}

#define VECSIM_ALGO_PARAM_MSG(algo, param) "vector similarity " algo " index `" param "`"

static int parseVectorField_hnsw(FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status) {
  int rc;

  // HNSW mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;

  // Get number of parameters
  size_t expNumParam, numParam = 0;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity number of parameters: %s", AC_Strerror(rc));
    return 0;
  } else if (expNumParam % 2) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Bad number of arguments for vector similarity index: got %d but expected even number (as algorithm parameters should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.hnswParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_TYPE), rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.hnswParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status,  VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_INITIAL_CAP), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_M)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.M, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_M), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EFCONSTRUCTION)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.efConstruction, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EFCONSTRUCTION), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EFRUNTIME)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.efRuntime, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EFRUNTIME), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EPSILON)) {
      if ((rc = AC_GetDouble(ac, &params->algoParams.hnswParams.epsilon, AC_F_GE0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EPSILON), rc);
        return 0;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_HNSW, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
    return 0;
  }
  if (!mandtype) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_HNSW, VECSIM_TYPE);
    return 0;
  }
  if (!mandsize) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_HNSW, VECSIM_DIM);
    return 0;
  }
  if (!mandmetric) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_HNSW, VECSIM_DISTANCE_METRIC);
    return 0;
  }
  // Calculating expected blob size of a vector in bytes.
  fs->vectorOpts.expBlobSize = params->algoParams.hnswParams.dim * VecSimType_sizeof(params->algoParams.hnswParams.type);

  return parseVectorField_validate_hnsw(params, status);
}

static int parseVectorField_flat(FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status) {
  int rc;

  // BF mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;

  // Get number of parameters
  size_t expNumParam, numParam = 0;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity number of parameters: %s", AC_Strerror(rc));
    return 0;
  } else if (expNumParam % 2) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad number of arguments for vector similarity index", ": got %d but expected even number as algorithm parameters (should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.bfParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_TYPE), rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.bfParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_INITIAL_CAP), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_BLOCKSIZE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.blockSize, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_BLOCKSIZE), rc);
        return 0;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_BF, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
    return 0;
  }
  if (!mandtype) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_BF, VECSIM_TYPE);
    return 0;
  }
  if (!mandsize) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_BF, VECSIM_DIM);
    return 0;
  }
  if (!mandmetric) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_BF, VECSIM_DISTANCE_METRIC);
    return 0;
  }
  // Calculating expected blob size of a vector in bytes.
  fs->vectorOpts.expBlobSize = params->algoParams.bfParams.dim * VecSimType_sizeof(params->algoParams.bfParams.type);

  return parseVectorField_validate_flat(&fs->vectorOpts.vecSimParams, status);
}

static int parseVectorField_svs(FieldSpec *fs, TieredIndexParams *tieredParams, ArgsCursor *ac, QueryError *status) {
  int rc;

  // SVS-VAMANA mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;

  VecSimParams *params = tieredParams->primaryIndexParams;

  // Get number of parameters
  size_t expNumParam, numParam = 0;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "vector similarity number of parameters", rc);
    return 0;
  } else if (expNumParam % 2) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad number of arguments for vector similarity index:", " got %d but expected even number as algorithm parameters (should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.svsParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_TYPE), rc);
        return 0;
      } else if (params->algoParams.svsParams.type != VecSimType_FLOAT16 &&
                 params->algoParams.svsParams.type != VecSimType_FLOAT32){
            QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Not supported data type is given. ", "Expected: FLOAT16, FLOAT32");
            return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.svsParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_GRAPH_DEGREE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.graph_max_degree, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_GRAPH_DEGREE), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_WINDOW_SIZE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.construction_window_size, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_WINDOW_SIZE), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_COMPRESSION)) {
      if ((rc = parseVectorField_GetQuantBits(ac, &params->algoParams.svsParams.quantBits)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_COMPRESSION), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_WSSEARCH)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.search_window_size, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_WSSEARCH), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EPSILON)) {
      if ((rc = AC_GetDouble(ac, &params->algoParams.svsParams.epsilon, AC_F_GE0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_EPSILON), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_REDUCED_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.leanvec_dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_REDUCED_DIM), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_TRAINING_THRESHOLD)) {
      if ((rc = AC_GetSize(ac, &tieredParams->specificParams.tieredSVSParams.trainingTriggerThreshold, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_TRAINING_THRESHOLD), rc);
        return 0;
      } else if (tieredParams->specificParams.tieredSVSParams.trainingTriggerThreshold < DEFAULT_BLOCK_SIZE) {
           QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid TRAINING_THRESHOLD: cannot be lower than DEFAULT_BLOCK_SIZE ", "(%d)", DEFAULT_BLOCK_SIZE);
          return 0;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_SVS, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
    return 0;
  }
  if (!mandtype) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_SVS, VECSIM_TYPE);
    return 0;
  }
  if (!mandsize) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_SVS, VECSIM_DIM);
    return 0;
  }
  if (!mandmetric) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_SVS, VECSIM_DISTANCE_METRIC);
    return 0;
  }
  if (params->algoParams.svsParams.quantBits == 0 && tieredParams->specificParams.tieredSVSParams.trainingTriggerThreshold > 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "TRAINING_THRESHOLD is irrelevant when compression was not requested", "");
    return 0;
  }
  if (!VecSim_IsLeanVecCompressionType(params->algoParams.svsParams.quantBits) && params->algoParams.svsParams.leanvec_dim > 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "REDUCE is irrelevant when compression is not of type LeanVec", "");
    return 0;
  }
  // Calculating expected blob size of a vector in bytes.
  fs->vectorOpts.expBlobSize = params->algoParams.svsParams.dim * VecSimType_sizeof(params->algoParams.svsParams.type);

  return parseVectorField_validate_svs(params, status);
}

// Parse the arguments of a TEXT field
static int parseTextField(FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  int rc;
  fs->types |= INDEXFLD_T_FULLTEXT;

  // this is a text field
  // init default weight and type
  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_NOSTEM_STR)) {
      fs->options |= FieldSpec_NoStemming;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = AC_GetDouble(ac, &d, 0)) != AC_OK) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for weight: %s", AC_Strerror(rc));
        return 0;
      }
      fs->ftWeight = d;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_PHONETIC_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SPEC_PHONETIC_STR " requires an argument");
        return 0;
      }

      const char *matcher = AC_GetStringNC(ac, NULL);
      // try and parse the matcher
      // currently we just make sure algorithm is double metaphone (dm)
      // and language is one of the following : English (en), French (fr), Portuguese (pt) and
      // Spanish (es)
      // in the future we will support more algorithms and more languages
      if (!checkPhoneticAlgorithmAndLang(matcher)) {
        QueryError_SetError(
            status, QUERY_ERROR_CODE_INVAL,
            "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
            "double metaphone (dm). Supported languages: English (en), French (fr), "
            "Portuguese (pt) and Spanish (es)");
        return 0;
      }
      fs->options |= FieldSpec_Phonetics;
      continue;
    } else if (AC_AdvanceIfMatch(ac, SPEC_WITHSUFFIXTRIE_STR)) {
      fs->options |= FieldSpec_WithSuffixTrie;
    } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXEMPTY_STR)) {
      fs->options |= FieldSpec_IndexEmpty;
    } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    } else {
      break;
    }
  }
  return 1;
}

// Parse the arguments of a TAG field
static int parseTagField(FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
    int rc = 1;
    fs->types |= INDEXFLD_T_TAG;

    while (!AC_IsAtEnd(ac)) {
      if (AC_AdvanceIfMatch(ac, SPEC_TAG_SEPARATOR_STR)) {
        if (AC_IsAtEnd(ac)) {
          QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SPEC_TAG_SEPARATOR_STR " requires an argument");
          rc = 0;
          break;
        }
        const char *sep = AC_GetStringNC(ac, NULL);
        if (strlen(sep) != 1) {
          QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                "Tag separator must be a single character. Got `%s`", sep);
          rc = 0;
          break;
        }
        fs->tagOpts.tagSep = *sep;
      } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_CASE_SENSITIVE_STR)) {
        fs->tagOpts.tagFlags |= TagField_CaseSensitive;
      } else if (AC_AdvanceIfMatch(ac, SPEC_WITHSUFFIXTRIE_STR)) {
        fs->options |= FieldSpec_WithSuffixTrie;
      } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXEMPTY_STR)) {
        fs->options |= FieldSpec_IndexEmpty;
      } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
        fs->options |= FieldSpec_IndexMissing;
      } else {
        break;
      }
    }

  return rc;
}

static int parseVectorField(IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  // this is a vector field
  // init default type, size, distance metric and algorithm

  fs->types |= INDEXFLD_T_VECTOR;
  sp->flags |= Index_HasVecSim;

  memset(&fs->vectorOpts.vecSimParams, 0, sizeof(VecSimParams));

  // If the index is on JSON and the given path is dynamic, create a multi-value index.
  bool multi = false;
  if (isSpecJson(sp)) {
    RedisModuleString *err_msg;
    JSONPath jsonPath = pathParse(fs->fieldPath, &err_msg);
    if (!jsonPath) {
      if (err_msg) {
        JSONParse_error(status, err_msg, fs->fieldPath, fs->fieldName, sp->specName);
      }
      return 0;
    }
    multi = !(japi->pathIsSingle(jsonPath));
    japi->pathFree(jsonPath);
  }

  // parse algorithm
  const char *algStr;
  size_t len;
  int rc;
  int result;
  if ((rc = AC_GetString(ac, &algStr, &len, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity algorithm: %s", AC_Strerror(rc));
    return 0;
  }
  VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
  logCtx->index_field_name = HiddenString_GetUnsafe(fs->fieldName, NULL);
  fs->vectorOpts.vecSimParams.logCtx = logCtx;

  if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_BF)) {
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_BF;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.initialCapacity = SIZE_MAX;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.blockSize = 0;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.multi = multi;
    result = parseVectorField_flat(fs, &fs->vectorOpts.vecSimParams, ac, status);
  } else if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_HNSW)) {
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
    VecSim_TieredParams_Init(&fs->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);
    fs->vectorOpts.vecSimParams.algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = 0; // Will be set to default value.

    VecSimParams *params = fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams;
    params->algo = VecSimAlgo_HNSWLIB;
    params->algoParams.hnswParams.initialCapacity = SIZE_MAX;
    params->algoParams.hnswParams.blockSize = 0;
    params->algoParams.hnswParams.M = HNSW_DEFAULT_M;
    params->algoParams.hnswParams.efConstruction = HNSW_DEFAULT_EF_C;
    params->algoParams.hnswParams.efRuntime = HNSW_DEFAULT_EF_RT;
    params->algoParams.hnswParams.multi = multi;
    // Point to the same logCtx as the external wrapping VecSimParams object, which is the owner.
    params->logCtx = logCtx;
    result = parseVectorField_hnsw(fs, params, ac, status);
  } else if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_SVS)) {
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
    VecSim_TieredParams_Init(&fs->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);

    // primary index params allocated in VecSim_TieredParams_Init()
    TieredIndexParams *params = &fs->vectorOpts.vecSimParams.algoParams.tieredParams;
    // TODO: FT.INFO currently displays index attributes from this struct instead of
    // querying VecSim runtime info. Once vecsim provides runtime info for FT.INFO,
    // remove this duplication and pass 0 to let VecSim apply its own defaults.
    params->specificParams.tieredSVSParams.trainingTriggerThreshold = 0;  // will be set to default value if not specified by user.
    params->primaryIndexParams->algo = VecSimAlgo_SVS;
    params->primaryIndexParams->algoParams.svsParams.quantBits = VecSimSvsQuant_NONE;
    params->primaryIndexParams->algoParams.svsParams.graph_max_degree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE;
    params->primaryIndexParams->algoParams.svsParams.construction_window_size = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE;
    params->primaryIndexParams->algoParams.svsParams.multi = multi;
    params->primaryIndexParams->algoParams.svsParams.num_threads = workersThreadPool_NumThreads();
    params->primaryIndexParams->algoParams.svsParams.leanvec_dim = SVS_VAMANA_DEFAULT_LEANVEC_DIM;
    params->primaryIndexParams->logCtx = logCtx;
    result = parseVectorField_svs(fs, params, ac, status);
    if (!(params->primaryIndexParams->algoParams.svsParams.quantBits == VecSimSvsQuant_NONE)
      && (params->specificParams.tieredSVSParams.trainingTriggerThreshold == 0)) {
        params->specificParams.tieredSVSParams.trainingTriggerThreshold = SVS_VAMANA_DEFAULT_TRAINING_THRESHOLD;
    }
    if (VecSim_IsLeanVecCompressionType(params->primaryIndexParams->algoParams.svsParams.quantBits) &&
        params->primaryIndexParams->algoParams.svsParams.leanvec_dim == 0) {
      params->primaryIndexParams->algoParams.svsParams.leanvec_dim =
        params->primaryIndexParams->algoParams.svsParams.dim / 2;  // default value
    }

  } else {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity algorithm: %s", AC_Strerror(AC_ERR_ENOENT));
    return 0;
  }

  if(result != 0) {
    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }
    return result;
  } else {
    return 0;
  }
}

static int parseGeometryField(IndexSpec *sp, FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  fs->types |= INDEXFLD_T_GEOMETRY;
  sp->flags |= Index_HasGeometry;

    if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_FLAT_STR)) {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Cartesian;
    } else if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_SPHERE_STR)) {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Geographic;
    } else {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Geographic;
    }

    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }

  return 1;
}

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static int parseFieldSpec(ArgsCursor *ac, IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, QueryError *status) {
  if (AC_IsAtEnd(ac)) {

    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Field", " `%s` does not have a type", HiddenString_GetUnsafe(fs->fieldName, NULL));
    return 0;
  }

  if (AC_AdvanceIfMatch(ac, SPEC_TEXT_STR)) {  // text field
    if (!parseTextField(fs, ac, status)) goto error;
    if (!FieldSpec_IndexesEmpty(fs)) {
      sp->flags |= Index_HasNonEmpty;
    }
  } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_STR)) {  // tag field
    if (!parseTagField(fs, ac, status)) goto error;
    if (!FieldSpec_IndexesEmpty(fs)) {
      sp->flags |= Index_HasNonEmpty;
    }
  } else if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_STR)) {  // geometry field
    if (!parseGeometryField(sp, fs, ac, status)) goto error;
  } else if (AC_AdvanceIfMatch(ac, SPEC_VECTOR_STR)) {  // vector field
    if (!parseVectorField(sp, sp_ref, fs, ac, status)) goto error;
    // Skip SORTABLE and NOINDEX options
    return 1;
  } else if (AC_AdvanceIfMatch(ac, SPEC_NUMERIC_STR)) {  // numeric field
    fs->types |= INDEXFLD_T_NUMERIC;
    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }
  } else if (AC_AdvanceIfMatch(ac, SPEC_GEO_STR)) {  // geo field
    fs->types |= INDEXFLD_T_GEO;
    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }
  } else {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid field type for field", " `%s`", HiddenString_GetUnsafe(fs->fieldName, NULL));
    goto error;
  }

  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(fs);
      if (AC_AdvanceIfMatch(ac, SPEC_UNF_STR) ||      // Explicitly requested UNF
          FIELD_IS(fs, INDEXFLD_T_NUMERIC) ||         // We don't normalize numeric fields. Implicit UNF
          TAG_FIELD_IS(fs, TagField_CaseSensitive)) { // We don't normalize case sensitive tags. Implicit UNF
        fs->options |= FieldSpec_UNF;
      }
      continue;
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      fs->options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  // We don't allow both NOINDEX and INDEXMISSING, since the missing values will
  // not contribute and thus this doesn't make sense.
  if (!FieldSpec_IsIndexable(fs) && FieldSpec_IndexesMissing(fs)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "'Field cannot be defined with both `NOINDEX` and `INDEXMISSING`", " `%s` '", HiddenString_GetUnsafe(fs->fieldName, NULL));
    goto error;
  }
  return 1;

error:
  if (!QueryError_HasError(status)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Could not parse schema for field", " `%s`", HiddenString_GetUnsafe(fs->fieldName, NULL));
  }
  return 0;
}

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CreateTextId(IndexSpec *sp, t_fieldIndex index) {
  size_t length = array_len(sp->fieldIdToIndex);
  if (length >= SPEC_MAX_FIELD_ID) {
    return -1;
  }

  array_ensure_append_1(sp->fieldIdToIndex, index);
  return length;
}

static IndexSpecCache *IndexSpec_BuildSpecCache(const IndexSpec *spec);

/**
 * Add fields to an existing (or newly created) index. If the addition fails,
 */
static int IndexSpec_AddFieldsInternal(IndexSpec *sp, StrongRef spec_ref, ArgsCursor *ac,
                                       QueryError *status, int isNew) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Fields arguments are missing");
    return 0;
  }

  const size_t prevNumFields = sp->numFields;
  const size_t prevSortLen = sp->numSortableFields;
  const IndexFlags prevFlags = sp->flags;

  while (!AC_IsAtEnd(ac)) {
    if (sp->numFields == SPEC_MAX_FIELDS) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is limited to %d fields",
                             SPEC_MAX_FIELDS);
      goto reset;
    }

    // Parse path and name of field
    size_t pathlen, namelen;
    const char *fieldPath = AC_GetStringNC(ac, &pathlen);
    const char *fieldName = fieldPath;
    if (AC_AdvanceIfMatch(ac, SPEC_AS_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SPEC_AS_STR " requires an argument");
        goto reset;
      }
      fieldName = AC_GetStringNC(ac, &namelen);
      sp->flags |= Index_HasFieldAlias;
    } else {
      // if `AS` is not used, set the path as name
      namelen = pathlen;
      fieldPath = NULL;
    }

    if (IndexSpec_GetFieldWithLength(sp, fieldName, namelen)) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Duplicate field in schema", " - %s", fieldName);
      goto reset;
    }

    FieldSpec *fs = IndexSpec_CreateField(sp, fieldName, fieldPath);
    if (!fs) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is currently limited", " to %d fields",
                             sp->numFields);
      goto reset;
    }
    if (!parseFieldSpec(ac, sp, spec_ref, fs, status)) {
      goto reset;
    }

    if (sp->diskSpec)
    {
      if (!FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support non-TEXT fields");
        goto reset;
      }
      if (fs->options & FieldSpec_NotIndexable) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support NOINDEX fields");
        goto reset;
      }
      if (fs->options & FieldSpec_Sortable) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support SORTABLE fields");
        goto reset;
      }
      if (fs->options & FieldSpec_IndexMissing) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support INDEXMISSING fields");
        goto reset;
      }
      if (fs->options & FieldSpec_IndexEmpty) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support INDEXEMPTY fields");
        goto reset;
      }
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_IsIndexable(fs)) {
      int textId = IndexSpec_CreateTextId(sp, fs->index);
      if (textId < 0) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is limited to %d TEXT fields",
                               SPEC_MAX_FIELD_ID);
        goto reset;
      }

      // If we need to store field flags and we have over 32 fields, we need to switch to wide
      // schema encoding
      if (textId >= SPEC_WIDEFIELD_THRESHOLD && (sp->flags & Index_StoreFieldFlags)) {
        if (isNew) {
          sp->flags |= Index_WideSchema;
        } else if ((sp->flags & Index_WideSchema) == 0) {
          QueryError_SetError(
              status, QUERY_ERROR_CODE_LIMIT,
              "Cannot add more fields. Declare index with wide fields to allow adding "
              "unlimited fields");
          goto reset;
        }
      }
      fs->ftId = textId;
      if isSpecJson(sp) {
        if ((sp->flags & Index_HasFieldAlias) && (sp->flags & Index_StoreTermOffsets)) {
          RedisModuleString *err_msg;
          JSONPath jsonPath = pathParse(fs->fieldPath, &err_msg);
          if (jsonPath && japi->pathHasDefinedOrder(jsonPath)) {
            // Ordering is well defined
            fs->options &= ~FieldSpec_UndefinedOrder;
          } else {
            // Mark FieldSpec
            fs->options |= FieldSpec_UndefinedOrder;
            // Mark IndexSpec
            sp->flags |= Index_HasUndefinedOrder;
          }
          if (jsonPath) {
            japi->pathFree(jsonPath);
          } else if (err_msg) {
            JSONParse_error(status, err_msg, fs->fieldPath, fs->fieldName, sp->specName);
            goto reset;
          } /* else {
            RedisModule_Log(RSDummyContext, "notice",
                            "missing RedisJSON API to parse JSONPath '%s' in attribute '%s' in index '%s', assuming undefined ordering",
                            fs->path, fs->name, sp->name);
          } */
        }
      }
    }

    if (FieldSpec_IsSortable(fs)) {
      if (isSpecJson(sp)) {
        // SORTABLE JSON field is always UNF
        fs->options |= FieldSpec_UNF;
      }

      if (fs->options & FieldSpec_Dynamic) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_BAD_OPTION,
                               "Cannot set dynamic field to sortable - %s", fieldName);
        goto reset;
      }

      fs->sortIdx = sp->numSortableFields++;
      if (fs->sortIdx == -1) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is limited to %d Sortable fields",
                               SPEC_MAX_FIELDS);
        goto reset;
      }
    } else {
      fs->sortIdx = -1;
    }
    if (FieldSpec_IsPhonetics(fs)) {
      sp->flags |= Index_HasPhonetic;
    }
    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_HasSuffixTrie(fs)) {
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->flags |= Index_HasSuffixTrie;
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
      }
    }
  }

  // If we successfully modified the schema, we need to update the spec cache
  IndexSpecCache_Decref(sp->spcache);
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  for (size_t ii = prevNumFields; ii < sp->numFields; ++ii) {
    FieldsGlobalStats_UpdateStats(sp->fields + ii, 1);
  }

  return 1;

reset:
  for (size_t ii = prevNumFields; ii < sp->numFields; ++ii) {
    IndexError_Clear(sp->fields[ii].indexError);
    FieldSpec_Cleanup(&sp->fields[ii]);
  }

  sp->numFields = prevNumFields;
  sp->numSortableFields = prevSortLen;
  // TODO: Why is this masking performed?
  sp->flags = prevFlags | (sp->flags & Index_HasSuffixTrie);
  return 0;
}

// Assumes the spec is locked for write
int IndexSpec_AddFields(StrongRef spec_ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status) {
  setMemoryInfo(ctx);

  int rc = IndexSpec_AddFieldsInternal(sp, spec_ref, ac, status, 0);
  if (rc && initialScan) {
    IndexSpec_ScanAndReindex(ctx, spec_ref);
  }

  return rc;
}

bool IndexSpec_IsCoherent(IndexSpec *spec, sds* prefixes, size_t n_prefixes) {
  if (!spec || !spec->rule) {
    return false;
  }
  arrayof(HiddenUnicodeString*) spec_prefixes = spec->rule->prefixes;
  if (n_prefixes != array_len(spec_prefixes)) {
    return false;
  }

  // Validate that the prefixes in the arguments are the same as the ones in the
  // index (also in the same order)
  for (size_t i = 0; i < n_prefixes; i++) {
    sds arg = prefixes[i];
    if (HiddenUnicodeString_CompareC(spec_prefixes[i], arg) != 0) {
      // Unmatching prefixes
      return false;
    }
  }

  return true;
}

inline static bool isSpecOnDisk(const IndexSpec *sp) {
  return isFlex;
}


/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
StrongRef IndexSpec_Parse(const HiddenString *name, const char **argv, int argc, QueryError *status) {
  IndexSpec *spec = NewIndexSpec(name);
  StrongRef spec_ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  spec->own_ref = spec_ref;

  IndexSpec_MakeKeyless(spec);

  ArgsCursor ac = {0};
  ArgsCursor acStopwords = {0};

  ArgsCursor_InitCString(&ac, argv, argc);
  long long timeout = -1;
  int dummy;
  size_t dummy2;
  SchemaRuleArgs rule_args = {0};
  ArgsCursor rule_prefixes = {0};

  ACArgSpec argopts[] = {
      {AC_MKUNFLAG(SPEC_NOOFFSETS_STR, &spec->flags,
                   Index_StoreTermOffsets | Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOHL_STR, &spec->flags, Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOFIELDS_STR, &spec->flags, Index_StoreFieldFlags)},
      {AC_MKUNFLAG(SPEC_NOFREQS_STR, &spec->flags, Index_StoreFreqs)},
      {AC_MKBITFLAG(SPEC_SCHEMA_EXPANDABLE_STR, &spec->flags, Index_WideSchema)},
      {AC_MKBITFLAG(SPEC_ASYNC_STR, &spec->flags, Index_Async)},
      {AC_MKBITFLAG(SPEC_SKIPINITIALSCAN_STR, &spec->flags, Index_SkipInitialScan)},

      // For compatibility
      {.name = "NOSCOREIDX", .target = &dummy, .type = AC_ARGTYPE_BOOLFLAG},
      {.name = "ON", .target = &rule_args.type, .len = &dummy2, .type = AC_ARGTYPE_STRING},
      SPEC_FOLLOW_HASH_ARGS_DEF(&rule_args)
      {.name = SPEC_TEMPORARY_STR, .target = &timeout, .type = AC_ARGTYPE_LLONG},
      {.name = SPEC_STOPWORDS_STR, .target = &acStopwords, .type = AC_ARGTYPE_SUBARGS},
      {.name = NULL}};

  ACArgSpec *errarg = NULL;
  int rc = AC_ParseArgSpec(&ac, argopts, &errarg);
  if (rc != AC_OK) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      goto failure;
    }
  }

  if (timeout != -1) {
    spec->flags |= Index_Temporary;
  }
  spec->timeout = timeout * 1000;  // convert to ms

  if (rule_prefixes.argc > 0) {
    rule_args.nprefixes = rule_prefixes.argc;
    rule_args.prefixes = (const char **)rule_prefixes.objs;
  } else {
    rule_args.nprefixes = 1;
    static const char *empty_prefix[] = {""};
    rule_args.prefixes = empty_prefix;
  }

  spec->rule = SchemaRule_Create(&rule_args, spec_ref, status);
  if (!spec->rule) {
    goto failure;
  }

  if (AC_IsInitialized(&acStopwords)) {
    if (spec->stopwords) {
      StopWordList_Unref(spec->stopwords);
    }
    spec->stopwords = NewStopWordListCStr((const char **)acStopwords.objs, acStopwords.argc);
    spec->flags |= Index_HasCustomStopwords;
  }

  if (!AC_AdvanceIfMatch(&ac, SPEC_SCHEMA_STR)) {
    if (AC_NumRemaining(&ac)) {
      const char *badarg = AC_GetStringNC(&ac, NULL);
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown argument", " `%s`", badarg);
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "No schema found");
    }
    goto failure;
  }

  if (!IndexSpec_AddFieldsInternal(spec, spec_ref, &ac, status, 1)) {
    goto failure;
  }

  if (spec->rule->filter_exp) {
    SchemaRule_FilterFields(spec);
  }

  // Store on disk if we're on Flex and we don't force RAM
  if (isSpecOnDisk(spec)) {
    RS_ASSERT(disk_db);
    spec->diskSpec = SearchDisk_OpenIndex(HiddenString_GetUnsafe(spec->specName, NULL), spec->rule->type);
    RS_LOG_ASSERT(spec->diskSpec, "Failed to open disk spec")
  }

  return spec_ref;

failure:  // on failure free the spec fields array and return an error
  spec->flags &= ~Index_Temporary;
  IndexSpec_RemoveFromGlobals(spec_ref, false);
  return INVALID_STRONG_REF;
}

StrongRef IndexSpec_ParseC(const char *name, const char **argv, int argc, QueryError *status) {
  HiddenString *hidden = NewHiddenString(name, strlen(name), true);
  return IndexSpec_Parse(hidden, argv, argc, status);
}

/* Initialize some index stats that might be useful for scoring functions */
// Assuming the spec is properly locked before calling this function
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats) {
  stats->numDocs = sp->stats.numDocuments;
  stats->numTerms = sp->stats.numTerms;
  stats->avgDocLen =
      stats->numDocs ? (double)sp->stats.totalDocsLen / (double)sp->stats.numDocuments : 0;
}

size_t IndexSpec_GetIndexErrorCount(const IndexSpec *sp) {
  return IndexError_ErrorCount(&sp->stats.indexError);
}

// Assuming the spec is properly locked for writing before calling this function.
void IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  int isNew = Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL);
  if (isNew) {
    sp->stats.numTerms++;
    sp->stats.termsSize += len;
  }
}

// For testing purposes only
void Spec_AddToDict(RefManager *rm) {
  IndexSpec* spec = ((IndexSpec*)__RefManager_Get_Object(rm));
  dictAdd(specDict_g, (void*)spec->specName, (void *)rm);
}

static void IndexSpecCache_Free(IndexSpecCache *c) {
  for (size_t ii = 0; ii < c->nfields; ++ii) {
    if (c->fields[ii].fieldName != c->fields[ii].fieldPath) {
      HiddenString_Free(c->fields[ii].fieldName, true);
    }
    HiddenString_Free(c->fields[ii].fieldPath, true);
  }
  rm_free(c->fields);
  rm_free(c);
}

// The value of the refcount can get to 0 only if the index spec itself does not point to it anymore,
// and at this point the refcount only gets decremented so there is no wory of some thread increasing the
// refcount while we are freeing the cache.
void IndexSpecCache_Decref(IndexSpecCache *c) {
  if (c && !__atomic_sub_fetch(&c->refcount, 1, __ATOMIC_RELAXED)) {
    IndexSpecCache_Free(c);
  }
}

// Assuming the spec is properly locked before calling this function.
static IndexSpecCache *IndexSpec_BuildSpecCache(const IndexSpec *spec) {
  IndexSpecCache *ret = rm_calloc(1, sizeof(*ret));
  ret->nfields = spec->numFields;
  ret->fields = rm_malloc(sizeof(*ret->fields) * ret->nfields);
  ret->refcount = 1;
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    const FieldSpec* fs = spec->fields + ii;
    FieldSpec* field = ret->fields + ii;
    *field = *fs;
    field->fieldName = HiddenString_Duplicate(fs->fieldName);
    // if name & path are pointing to the same string, copy only pointer
    if (fs->fieldName != fs->fieldPath) {
      field->fieldPath = HiddenString_Duplicate(fs->fieldPath);
    } else {
      // use the same pointer for both name and path
      field->fieldPath = field->fieldName;
    }
  }
  return ret;
}

IndexSpecCache *IndexSpec_GetSpecCache(const IndexSpec *spec) {
  RS_LOG_ASSERT(spec->spcache, "Index spec cache is NULL");
  __atomic_fetch_add(&spec->spcache->refcount, 1, __ATOMIC_RELAXED);
  return spec->spcache;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void CleanPool_ThreadPoolStart() {
  if (!cleanPool) {
    cleanPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "cleanPool");
  }
}

void CleanPool_ThreadPoolDestroy() {
  if (cleanPool) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(cleanPool);
    cleanPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

uint16_t getPendingIndexDrop() {
  return __atomic_load_n(&pendingIndexDropCount_g, __ATOMIC_RELAXED);
}

void addPendingIndexDrop() {
  __atomic_add_fetch(&pendingIndexDropCount_g, 1, __ATOMIC_RELAXED);
}

void removePendingIndexDrop() {
  __atomic_sub_fetch(&pendingIndexDropCount_g, 1, __ATOMIC_RELAXED);
}

size_t CleanInProgressOrPending() {
  return getPendingIndexDrop();
}

/*
 * Free resources of unlinked index spec
 */
static void IndexSpec_FreeUnlinkedData(IndexSpec *spec) {

  // Free all documents metadata
  DocTable_Free(&spec->docs);
  // Free TEXT field trie and inverted indexes
  if (spec->terms) {
    TrieType_Free(spec->terms);
  }
  // Free TEXT TAG NUMERIC VECTOR and GEOSHAPE fields trie and inverted indexes
  if (spec->keysDict) {
    dictRelease(spec->keysDict);
  }
  // Free missingFieldDict
  if (spec->missingFieldDict) {
    dictRelease(spec->missingFieldDict);
  }
  // Free existing docs inverted index
  if (spec->existingDocs) {
    InvertedIndex_Free(spec->existingDocs);
  }
  // Free synonym data
  if (spec->smap) {
    SynonymMap_Free(spec->smap);
  }
  // Destroy spec rule
  if (spec->rule) {
    SchemaRule_Free(spec->rule);
    spec->rule = NULL;
  }
  // Free fields cache data
  IndexSpecCache_Decref(spec->spcache);
  spec->spcache = NULL;

  array_free(spec->fieldIdToIndex);
  spec->fieldIdToIndex = NULL;

  // Free fields formatted names
  if (spec->indexStrs) {
    for (size_t ii = 0; ii < spec->numFields; ++ii) {
      IndexSpecFmtStrings *fmts = spec->indexStrs + ii;
      for (size_t jj = 0; jj < INDEXFLD_NUM_TYPES; ++jj) {
        if (fmts->types[jj]) {
          RedisModule_FreeString(RSDummyContext, fmts->types[jj]);
        }
      }
    }
    rm_free(spec->indexStrs);
  }
  // Free fields data
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      FieldSpec_Cleanup(&spec->fields[i]);
    }
    rm_free(spec->fields);
  }
  // Free spec name
  HiddenString_Free(spec->specName, true);
  rm_free(spec->obfuscatedName);
  // Free suffix trie
  if (spec->suffix) {
    TrieType_Free(spec->suffix);
  }

  // Destroy the spec's lock
  pthread_rwlock_destroy(&spec->rwlock);

  if (spec->diskSpec) SearchDisk_CloseIndex(spec->diskSpec);

  // Free spec struct
  rm_free(spec);

  removePendingIndexDrop();
}

/*
 * This function unlinks the index spec from any global structures and frees
 * all struct that requires acquiring the GIL.
 * Other resources are freed using IndexSpec_FreeData.
 */
void IndexSpec_Free(IndexSpec *spec) {
  // Stop scanner
  // Scanner has a weak reference to the spec, so at this point it will cancel itself and free
  // next time it will try to acquire the spec.

  // For temporary index
  // This function might be called from any thread, and we cannot deal with timers without the GIL.
  // At this point we should have already stopped the timer.
  RS_ASSERT(!spec->isTimerSet);
  // Stop and destroy garbage collector
  // We can't free it now, because it either runs at the moment or has a timer set which we can't
  // deal with without the GIL.
  // It will free itself when it discovers that the index was freed.
  // On the worst case, it just finishes the current run and will schedule another run soon.
  // In this case the GC will be freed on the next run, in `forkGcRunIntervalSec` seconds.
  if (RS_IsMock && spec->gc) {
    GCContext_StopMock(spec->gc);
  }

  // Free stopwords list (might use global pointer to default list)
  if (spec->stopwords) {
    StopWordList_Unref(spec->stopwords);
    spec->stopwords = NULL;
  }

  IndexError_Clear(spec->stats.indexError);
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      IndexError_Clear((spec->fields[i]).indexError);
    }
  }

  // Free unlinked index spec on a second thread
  if (RSGlobalConfig.freeResourcesThread == false) {
    IndexSpec_FreeUnlinkedData(spec);
  } else {
    redisearch_thpool_add_work(cleanPool, (redisearch_thpool_proc)IndexSpec_FreeUnlinkedData, spec, THPOOL_PRIORITY_HIGH);
  }
}

//---------------------------------------------------------------------------------------------

// Assumes this is called from the main thread with no competing threads
// Also assumes that the spec is existing in the global dictionary, so
// we use the global reference as our guard and access the spec directly.
// This function consumes the Strong reference it gets
void IndexSpec_RemoveFromGlobals(StrongRef spec_ref, bool removeActive) {
  IndexSpec *spec = StrongRef_Get(spec_ref);

  // Remove spec from global index list
  dictDelete(specDict_g, (void*)spec->specName);

  if (!spec->isDuplicate) {
    // Remove spec from global aliases list
    IndexSpec_ClearAliases(spec_ref);
  }

  SchemaPrefixes_RemoveSpec(spec_ref);

  // For temporary index
  // We are dropping the index from the mainthread, but the freeing process might happen later from
  // another thread. We cannot deal with timers from other threads, so we need to stop the timer
  // now. We don't need it anymore anyway.
  if (spec->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, spec->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
    spec->isTimerSet = false;
  }

  // Remove spec's fields from global statistics
  for (size_t i = 0; i < spec->numFields; i++) {
    FieldSpec *field = spec->fields + i;
    FieldsGlobalStats_UpdateStats(field, -1);
    FieldsGlobalStats_UpdateIndexError(field->types, -FieldSpec_GetIndexErrorCount(field));
  }

  // Mark there are pending index drops.
  // if ref count is > 1, the actual cleanup will be done only when StrongRefs are released.
  addPendingIndexDrop();

  // Nullify the spec's quick access to the strong ref. (doesn't decrement references count).
  spec->own_ref = (StrongRef){0};

  if (removeActive) {
    // Remove thread from active-threads container
    CurrentThread_ClearIndexSpec();
  }

  // mark the spec as deleted and decrement the ref counts owned by the global dictionaries
  StrongRef_Invalidate(spec_ref);
  StrongRef_Release(spec_ref);
}

void Indexes_Free(dict *d) {
  // free the schema dictionary this way avoid iterating over it for each combination of
  // spec<-->prefix
  SchemaPrefixes_Free(SchemaPrefixes_g);
  SchemaPrefixes_Create();

  CursorList_Empty(&g_CursorsListCoord);
  // cursor list is iterating through the list as well and consuming a lot of CPU
  CursorList_Empty(&g_CursorsList);

  arrayof(StrongRef) specs = array_new(StrongRef, dictSize(d));
  dictIterator *iter = dictGetIterator(d);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    array_append(specs, spec_ref);
  }
  dictReleaseIterator(iter);

  for (size_t i = 0; i < array_len(specs); ++i) {
    IndexSpec_RemoveFromGlobals(specs[i], false);
  }
  array_free(specs);
}


//---------------------------------------- atomic updates ---------------------------------------

// atomic update of usage counter
inline static void IndexSpec_IncreasCounter(IndexSpec *sp) {
  __atomic_fetch_add(&sp->counter , 1, __ATOMIC_RELAXED);
}


///////////////////////////////////////////////////////////////////////////////////////////////

StrongRef IndexSpec_LoadUnsafe(const char *name) {
  IndexLoadOptions lopts = {.nameC = name};
  return IndexSpec_LoadUnsafeEx(&lopts);
}

StrongRef IndexSpec_LoadUnsafeEx(IndexLoadOptions *options) {
  const char *ixname = NULL;
  if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
    ixname = RedisModule_StringPtrLen(options->nameR, NULL);
  } else {
    ixname = options->nameC;
  }

  HiddenString *specNameOrAlias = NewHiddenString(ixname, strlen(ixname), false);
  StrongRef spec_ref = {dictFetchValue(specDict_g, specNameOrAlias)};
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp && !(options->flags & INDEXSPEC_LOAD_NOALIAS)) {
    spec_ref = IndexAlias_Get(specNameOrAlias);
    sp = StrongRef_Get(spec_ref);
  }
  HiddenString_Free(specNameOrAlias, false);
  if (!sp) {
    return spec_ref;
  }

  if (!(options->flags & INDEXSPEC_LOAD_NOCOUNTERINC)){
    // Increment the number of uses.
    IndexSpec_IncreasCounter(sp);
  }

  if (!RS_IsMock && (sp->flags & Index_Temporary) && !(options->flags & INDEXSPEC_LOAD_NOTIMERUPDATE)) {
    IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref));
  }
  return spec_ref;
}

StrongRef IndexSpec_GetStrongRefUnsafe(const IndexSpec *spec) {
  return spec->own_ref;
}

// Assuming the spec is properly locked before calling this function.
RedisModuleString *IndexSpec_GetFormattedKey(IndexSpec *sp, const FieldSpec *fs,
                                             FieldType forType) {
  if (!sp->indexStrs) {
    sp->indexStrs = rm_calloc(SPEC_MAX_FIELDS, sizeof(*sp->indexStrs));
  }

  size_t typeix = INDEXTYPE_TO_POS(forType);

  RedisModuleString *ret = sp->indexStrs[fs->index].types[typeix];
  if (!ret) {
    RedisSearchCtx sctx = {.redisCtx = RSDummyContext, .spec = sp};
    switch (forType) {
      case INDEXFLD_T_NUMERIC:
      case INDEXFLD_T_GEO:  // TODO?? change the name
        ret = fmtRedisNumericIndexKey(&sctx, fs->fieldName);
        break;
      case INDEXFLD_T_TAG:
        ret = TagIndex_FormatName(sctx.spec, fs->fieldName);
        break;
      case INDEXFLD_T_VECTOR:
        ret = HiddenString_CreateRedisModuleString(fs->fieldName, sctx.redisCtx);
        break;
      case INDEXFLD_T_GEOMETRY:
        ret = fmtRedisGeometryIndexKey(&sctx, fs->fieldName);
        break;
      case INDEXFLD_T_FULLTEXT:  // Text fields don't get a per-field index
      default:
        ret = NULL;
        abort();
        break;
    }
    RS_LOG_ASSERT(ret, "Failed to create index string");
    sp->indexStrs[fs->index].types[typeix] = ret;
  }
  return ret;
}

// Assuming the spec is properly locked before calling this function.
RedisModuleString *IndexSpec_GetFormattedKeyByName(IndexSpec *sp, const char *s,
                                                   FieldType forType) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sp, s, strlen(s));
  if (!fs) {
    return NULL;
  }
  return IndexSpec_GetFormattedKey(sp, fs, forType);
}

// Assuming the spec is properly locked before calling this function.
void IndexSpec_InitializeSynonym(IndexSpec *sp) {
  if (!sp->smap) {
    sp->smap = SynonymMap_New(false);
    sp->flags |= Index_HasSmap;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void IndexSpec_InitLock(IndexSpec *sp) {
  int res = 0;
  pthread_rwlockattr_t attr;
  res = pthread_rwlockattr_init(&attr);
  RS_ASSERT(res == 0);
#if !defined(__APPLE__) && !defined(__FreeBSD__) && defined(__GLIBC__)
  int pref = PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP;
  res = pthread_rwlockattr_setkind_np(&attr, pref);
  RS_ASSERT(res == 0);
#endif

  pthread_rwlock_init(&sp->rwlock, &attr);
}

// Helper function for initializing a field spec
static void initializeFieldSpec(FieldSpec *fs, t_fieldIndex index) {
  fs->index = index;
  fs->indexError = IndexError_Init();
}

// Helper function for initializing an index spec
// Solves issues where a field is initialized in index creation but not when loading from RDB
static void initializeIndexSpec(IndexSpec *sp, const HiddenString *name, IndexFlags flags,
                                int16_t numFields) {
  sp->flags = flags;
  sp->numFields = numFields;
  sp->fields = rm_calloc(numFields, sizeof(FieldSpec));
  sp->specName = name;
  sp->obfuscatedName = IndexSpec_FormatObfuscatedName(name);
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->suffix = NULL;
  sp->suffixMask = (t_fieldMask)0;
  sp->keysDict = NULL;
  sp->getValue = NULL;
  sp->getValueCtx = NULL;
  sp->timeout = 0;
  sp->isTimerSet = false;
  sp->timerId = 0;

  sp->scanner = NULL;
  sp->scan_in_progress = false;
  sp->monitorDocumentExpiration = true;
  sp->monitorFieldExpiration = RedisModule_HashFieldMinExpire != NULL;
  sp->used_dialects = 0;

  memset(&sp->stats, 0, sizeof(sp->stats));
  sp->stats.indexError = IndexError_Init();

  sp->fieldIdToIndex = array_new(t_fieldIndex, 0);
  sp->terms = NewTrie(NULL, Trie_Sort_Lex);

  IndexSpec_InitLock(sp);
  // First, initialise fields IndexError for every field
  // In the RDB flow if some fields are not loaded correctly, we will free the spec and attempt to cleanup all the fields.
  for (t_fieldIndex i = 0; i < sp->numFields; i++) {
    initializeFieldSpec(sp->fields + i, i);
  }
}

IndexSpec *NewIndexSpec(const HiddenString *name) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  initializeIndexSpec(sp, name, INDEX_DEFAULT_FLAGS, 0);
  sp->stopwords = DefaultStopWordList();
  return sp;
}

// Assuming the spec is properly locked before calling this function.
FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path) {
  FieldSpec* fields = sp->fields;
  fields = rm_realloc(fields, sizeof(*fields) * (sp->numFields + 1));
  RS_LOG_ASSERT_FMT(fields, "Failed to allocate memory for %d fields", sp->numFields + 1);
  sp->fields = fields;
  FieldSpec *fs = sp->fields + sp->numFields;
  memset(fs, 0, sizeof(*fs));
  initializeFieldSpec(fs, sp->numFields);
  ++sp->numFields;
  fs->fieldName = NewHiddenString(name, strlen(name), true);
  fs->fieldPath = (path) ? NewHiddenString(path, strlen(path), true) : fs->fieldName;
  fs->ftId = RS_INVALID_FIELD_ID;
  fs->ftWeight = 1.0;
  fs->sortIdx = -1;
  fs->tagOpts.tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  if (!(sp->flags & Index_FromLLAPI)) {
    RS_LOG_ASSERT((sp->rule), "index w/o a rule?");
    switch (sp->rule->type) {
      case DocumentType_Hash:
        fs->tagOpts.tagSep = TAG_FIELD_DEFAULT_HASH_SEP; break;
      case DocumentType_Json:
        fs->tagOpts.tagSep = TAG_FIELD_DEFAULT_JSON_SEP; break;
      case DocumentType_Unsupported:
        RS_ABORT("shouldn't get here");
        break;
    }
  }
  return fs;
}

static dictType invidxDictType = {0};

static void valFreeCb(void *unused, void *p) {
  KeysDictValue *kdv = p;
  if (kdv->dtor) {
    kdv->dtor(kdv->p);
  }
  rm_free(kdv);
}

static void valIIFreeCb(void *unused, void *p) {
  InvertedIndex *ii = p;
  if(ii) {
    InvertedIndex_Free(ii);
  }
}

static dictType missingFieldDictType = {
        .hashFunction = hiddenNameHashFunction,
        .keyDup = hiddenNameKeyDup,
        .valDup = NULL,
        .keyCompare = hiddenNameKeyCompare,
        .keyDestructor = hiddenNameKeyDestructor,
        .valDestructor = valIIFreeCb,
};

// Only used on new specs so it's thread safe
void IndexSpec_MakeKeyless(IndexSpec *sp) {
  // Initialize only once:
  if (!invidxDictType.valDestructor) {
    invidxDictType = dictTypeHeapRedisStrings;
    invidxDictType.valDestructor = valFreeCb;
  }
  sp->keysDict = dictCreate(&invidxDictType, NULL);
  sp->missingFieldDict = dictCreate(&missingFieldDictType, NULL);
}

// Only used on new specs so it's thread safe
void IndexSpec_StartGCFromSpec(StrongRef global, IndexSpec *sp, uint32_t gcPolicy) {
  sp->gc = GCContext_CreateGC(global, gcPolicy);
  GCContext_Start(sp->gc);
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
// Only used on new specs so it's thread safe
void IndexSpec_StartGC(StrongRef global, IndexSpec *sp) {
  RS_LOG_ASSERT(!sp->gc, "GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.gcConfigParams.enableGC && !(sp->flags & Index_Temporary)) {
    sp->gc = GCContext_CreateGC(global, RSGlobalConfig.gcConfigParams.gcPolicy);
    GCContext_Start(sp->gc);

    const char* name = IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog);
    RedisModule_Log(RSDummyContext, "verbose", "Starting GC for %s", name);
    RedisModule_Log(RSDummyContext, "debug", "Starting GC %p for %s", sp->gc, name);
  }
}

// given a field mask with one bit lit, it returns its offset
int bit(t_fieldMask id) {
  for (int i = 0; i < sizeof(t_fieldMask) * 8; i++) {
    if (((id >> i) & 1) == 1) {
      return i;
    }
  }
  return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Backwards compat version of load for rdbs with version < 8
static int FieldSpec_RdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver) {
  char* name = NULL;
  size_t len = 0;
  LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
  f->fieldName = NewHiddenString(name, len, true);
  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->ftId = bit(LoadUnsigned_IOError(rdb, goto fail));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
  }
  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  f->tagOpts.tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagOpts.tagSep = TAG_FIELD_DEFAULT_HASH_SEP;
  if (encver >= 4) {
    f->options = LoadUnsigned_IOError(rdb, goto fail);
    f->sortIdx = LoadSigned_IOError(rdb, goto fail);
  }
  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  HiddenString_SaveToRdb(f->fieldName, rdb);
  if (HiddenString_Compare(f->fieldPath, f->fieldName) != 0) {
    RedisModule_SaveUnsigned(rdb, 1);
    HiddenString_SaveToRdb(f->fieldPath, rdb);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  RedisModule_SaveUnsigned(rdb, f->types);
  RedisModule_SaveUnsigned(rdb, f->options);
  RedisModule_SaveSigned(rdb, f->sortIdx);
  // Save text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->ftId);
    RedisModule_SaveDouble(rdb, f->ftWeight);
  }
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->tagOpts.tagFlags);
    RedisModule_SaveStringBuffer(rdb, &f->tagOpts.tagSep, 1);
  }
  if (FIELD_IS(f, INDEXFLD_T_VECTOR)) {
    RedisModule_SaveUnsigned(rdb, f->vectorOpts.expBlobSize);
    VecSim_RdbSave(rdb, &f->vectorOpts.vecSimParams);
  }
  if (FIELD_IS(f, INDEXFLD_T_GEOMETRY) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->geometryOpts.geometryCoords);
  }
}

static const FieldType fieldTypeMap[] = {[IDXFLD_LEGACY_FULLTEXT] = INDEXFLD_T_FULLTEXT,
                                         [IDXFLD_LEGACY_NUMERIC] = INDEXFLD_T_NUMERIC,
                                         [IDXFLD_LEGACY_GEO] = INDEXFLD_T_GEO,
                                         [IDXFLD_LEGACY_TAG] = INDEXFLD_T_TAG};
                                         // CHECKED: Not related to new data types - legacy code

static int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, StrongRef sp_ref, int encver) {

  f->ftId = RS_INVALID_FIELD_ID;
  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  char* name = NULL;
  size_t len = 0;
  LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
  f->fieldName = NewHiddenString(name, len, false);
  f->fieldPath = f->fieldName;
  if (encver >= INDEX_JSON_VERSION) {
    if (LoadUnsigned_IOError(rdb, goto fail) == 1) {
      LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
      f->fieldPath = NewHiddenString(name, len, false);
    }
  }

  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->options = LoadUnsigned_IOError(rdb, goto fail);
  f->sortIdx = LoadSigned_IOError(rdb, goto fail);

  if (encver < INDEX_MIN_MULTITYPE_VERSION) {
    RS_LOG_ASSERT(f->types <= IDXFLD_LEGACY_MAX, "field type should be string or numeric");
    f->types = fieldTypeMap[f->types];
  }

  // Load text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
    f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  }
  // Load tag specific options
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagOpts.tagFlags = LoadUnsigned_IOError(rdb, goto fail);
    // Load the separator
    size_t l;
    char *s = LoadStringBuffer_IOError(rdb, &l, goto fail);
    RS_LOG_ASSERT(l == 1, "buffer length should be 1");
    f->tagOpts.tagSep = *s;
    RedisModule_Free(s);
  }
  // Load vector specific options
  if (encver >= INDEX_VECSIM_VERSION && FIELD_IS(f, INDEXFLD_T_VECTOR)) {
    if (encver >= INDEX_VECSIM_2_VERSION) {
      f->vectorOpts.expBlobSize = LoadUnsigned_IOError(rdb, goto fail);
    }
    if (encver >= INDEX_VECSIM_SVS_VAMANA_VERSION) {
      if (VecSim_RdbLoad_v4(rdb, &f->vectorOpts.vecSimParams, sp_ref, HiddenString_GetUnsafe(f->fieldName, NULL)) != REDISMODULE_OK) {
        goto fail;
      }
    } else if (encver >= INDEX_VECSIM_TIERED_VERSION) {
      if (VecSim_RdbLoad_v3(rdb, &f->vectorOpts.vecSimParams, sp_ref, HiddenString_GetUnsafe(f->fieldName, NULL)) != REDISMODULE_OK) {
        goto fail;
      }
    } else {
      if (encver >= INDEX_VECSIM_MULTI_VERSION) {
        if (VecSim_RdbLoad_v2(rdb, &f->vectorOpts.vecSimParams) != REDISMODULE_OK) {
          goto fail;
        }
      } else {
        if (VecSim_RdbLoad(rdb, &f->vectorOpts.vecSimParams) != REDISMODULE_OK) {
          goto fail;
        }
      }
      // If we're loading an old (< INDEX_VECSIM_TIERED_VERSION) rdb, we need to convert an HNSW
      // index to a tiered index.
      VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
      logCtx->index_field_name = HiddenString_GetUnsafe(f->fieldName, NULL);
      f->vectorOpts.vecSimParams.logCtx = logCtx;
      if (f->vectorOpts.vecSimParams.algo == VecSimAlgo_HNSWLIB) {
        VecSimParams hnswParams = f->vectorOpts.vecSimParams;

        f->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
        VecSim_TieredParams_Init(&f->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);
        f->vectorOpts.vecSimParams.algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = 0;
        memcpy(f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams, &hnswParams, sizeof(VecSimParams));
      }
    }
    // Calculate blob size limitation on lower encvers.
    if (encver < INDEX_VECSIM_2_VERSION) {
      switch (f->vectorOpts.vecSimParams.algo) {
      case VecSimAlgo_HNSWLIB:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.hnswParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.hnswParams.type);
        break;
      case VecSimAlgo_BF:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.bfParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.bfParams.type);
        break;
      case VecSimAlgo_TIERED:
        if (f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
          f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams.type);
        } else if (f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_SVS) {
          goto fail;  // svs is not supported in old encvers
        }
        break;
      case VecSimAlgo_SVS:
        goto fail;  // svs is not supported in old encvers
      }
    }
  }

  // Load geometry specific options
  if (FIELD_IS(f, INDEXFLD_T_GEOMETRY) || (f->options & FieldSpec_Dynamic)) {
    if (encver >= INDEX_GEOMETRY_VERSION) {
      f->geometryOpts.geometryCoords = LoadUnsigned_IOError(rdb, goto fail);
    } else {
      // In RedisSearch RC (2.8.1 - 2.8.3) we supported default coordinate system which was not written to RDB
      f->geometryOpts.geometryCoords = GEOMETRY_COORDS_Cartesian;
    }
  }

  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

static void IndexStats_RdbLoad(RedisModuleIO *rdb, IndexStats *stats) {
  stats->numDocuments = RedisModule_LoadUnsigned(rdb);
  stats->numTerms = RedisModule_LoadUnsigned(rdb);
  stats->numRecords = RedisModule_LoadUnsigned(rdb);
  stats->invertedSize = RedisModule_LoadUnsigned(rdb);
  RedisModule_LoadUnsigned(rdb); // Consume `invertedCap`
  RedisModule_LoadUnsigned(rdb); // Consume `skipIndexesSize`
  RedisModule_LoadUnsigned(rdb); // Consume `scoreIndexesSize`
  stats->offsetVecsSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecRecords = RedisModule_LoadUnsigned(rdb);
  stats->termsSize = RedisModule_LoadUnsigned(rdb);
}

///////////////////////////////////////////////////////////////////////////////////////////////

static redisearch_thpool_t *reindexPool = NULL;

static IndexesScanner *IndexesScanner_NewGlobal() {
  if (global_spec_scanner) {
    return NULL;
  }

  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));
  scanner->global = true;
  scanner->scannedKeys = 0;

  global_spec_scanner = scanner;
  RedisModule_Log(RSDummyContext, "notice", "Global scanner created");

  return scanner;
}

static IndexesScanner *IndexesScanner_New(StrongRef global_ref) {

  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));

  scanner->spec_ref = StrongRef_Demote(global_ref);
  IndexSpec *spec = StrongRef_Get(global_ref);
  scanner->spec_name_for_logs = rm_strdup(IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog));
  scanner->isDebug = false;

  // scan already in progress?
  if (spec->scanner) {
    // cancel ongoing scan, keep on_progress indicator on
    IndexesScanner_Cancel(spec->scanner);
    const char* name = IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog);
    RedisModule_Log(RSDummyContext, "notice", "Scanning index %s in background: cancelled and restarted", name);
  }
  spec->scanner = scanner;
  spec->scan_in_progress = true;

  return scanner;
}

void IndexesScanner_Free(IndexesScanner *scanner) {
  rm_free(scanner->spec_name_for_logs);
  if (global_spec_scanner == scanner) {
    global_spec_scanner = NULL;
  } else {
    StrongRef tmp = WeakRef_Promote(scanner->spec_ref);
    IndexSpec *spec = StrongRef_Get(tmp);
    if (spec) {
      if (spec->scanner == scanner) {
        spec->scanner = NULL;
        spec->scan_in_progress = false;
      }
      StrongRef_Release(tmp);
    }
    WeakRef_Release(scanner->spec_ref);
  }
  // Free the last scanned key
  if (scanner->OOMkey) {
    RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
  }
  rm_free(scanner);
}

void IndexesScanner_Cancel(IndexesScanner *scanner) {
  scanner->cancelled = true;
}

void IndexesScanner_ResetProgression(IndexesScanner *scanner) {
  scanner-> scanFailedOnOOM = false;
  scanner-> scannedKeys = 0;
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_DoneIndexingCallabck(struct RSAddDocumentCtx *docCtx, RedisModuleCtx *ctx,
                                           void *pd) {
}

//---------------------------------------------------------------------------------------------

int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type);
static void Indexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             IndexesScanner *scanner) {

  if (scanner->cancelled) {
    return;
  }

  if (isBgIndexingMemoryOverLimit(ctx)){
    scanner->scanFailedOnOOM = true;
    if (scanner->OOMkey) {
      RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
    }
    // Hold the key that triggered OOM in case we need to attach an index error
    scanner->OOMkey = RedisModule_HoldString(RSDummyContext, keyname);
    return;
  }
  // RMKey it is provided as best effort but in some cases it might be NULL
  bool keyOpened = false;
  if (!key || isCrdt) {
    key = RedisModule_OpenKey(ctx, keyname, DOCUMENT_OPEN_KEY_INDEXING_FLAGS);
    keyOpened = true;
  }
  // Get the document type
  DocumentType type = getDocType(key);

  // Close the key if we opened it
  if (keyOpened) {
    RedisModule_CloseKey(key);
  }

  // Verify that the document type is supported and document is not empty
  if (type == DocumentType_Unsupported) {
    return;
  }

  if (scanner->global) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, keyname, type, NULL);
  } else {
    StrongRef curr_run_ref = IndexSpecRef_Promote(scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(curr_run_ref);
    if (sp) {
      // This check is performed without locking the spec, but it's ok since we locked the GIL
      // So the main thread is not running and the GC is not touching the relevant data
      if (SchemaRule_ShouldIndex(sp, keyname, type)) {
        IndexSpec_UpdateDoc(sp, ctx, keyname, type);
      }
      IndexSpecRef_Release(curr_run_ref);
    } else {
      // spec was deleted, cancel scan
      scanner->cancelled = true;
    }
  }
  ++scanner->scannedKeys;
}

//---------------------------------------------------------------------------------------------
// Define for neater code, first argument is the debug scanner flag field , second is the status code
#define IF_DEBUG_PAUSE_CHECK(scanner, ctx, status_bool, status_code) \
if (scanner->isDebug) { \
  DebugIndexesScanner *dScanner = (DebugIndexesScanner*)scanner;\
  DebugIndexesScanner_pauseCheck(dScanner, ctx, dScanner->status_bool, status_code); \
}
#define IF_DEBUG_PAUSE_CHECK_BEFORE_OOM_RETRY(scanner, ctx) IF_DEBUG_PAUSE_CHECK(scanner, ctx, pauseBeforeOOMRetry, DEBUG_INDEX_SCANNER_CODE_PAUSED_BEFORE_OOM_RETRY)
#define IF_DEBUG_PAUSE_CHECK_ON_OOM(scanner, ctx) IF_DEBUG_PAUSE_CHECK(scanner, ctx, pauseOnOOM, DEBUG_INDEX_SCANNER_CODE_PAUSED_ON_OOM)

static void Indexes_ScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  RedisModuleCtx *ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RedisModule_ThreadSafeContextLock(ctx);

  if (scanner->cancelled) {
    goto end;
  }
  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background");
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background", scanner->spec_name_for_logs);
  }

  size_t counter = 0;
  RedisModuleScanCB scanner_func = (RedisModuleScanCB)Indexes_ScanProc;
  if (globalDebugCtx.debugMode) {
    // If we are in debug mode, we need to use the debug scanner function
    scanner_func = (RedisModuleScanCB)DebugIndexes_ScanProc;

    // If background indexing paused, wait until it is resumed
    // Allow the redis server to acquire the GIL while we release it
    RedisModule_ThreadSafeContextUnlock(ctx);
    while (globalDebugCtx.bgIndexing.pause) { // volatile variable
      usleep(1000);
    }
    RedisModule_ThreadSafeContextLock(ctx);
  }

  while (RedisModule_Scan(ctx, cursor, scanner_func, scanner)) {
    RedisModule_ThreadSafeContextUnlock(ctx);
    counter++;
    if (counter % RSGlobalConfig.numBGIndexingIterationsBeforeSleep == 0) {
      // Sleep for one microsecond to allow redis server to acquire the GIL while we release it.
      // We do that periodically every X iterations (100 as default), otherwise we call
      // 'sched_yield()'. That is since 'sched_yield()' doesn't give up the processor for enough
      // time to ensure that other threads that are waiting for the GIL will actually have the
      // chance to take it.
      usleep(1);
    } else {
      sched_yield();
    }
    RedisModule_ThreadSafeContextLock(ctx);

    // Check if we need to handle OOM but must check if the scanner was cancelled for other reasons (i.e. FT. ALTER)
    if (scanner->scanFailedOnOOM && !scanner->cancelled) {

      // Check the config to see if we should wait for memory allocation
      if(RSGlobalConfig.bgIndexingOomPauseTimeBeforeRetry > 0) {
        IF_DEBUG_PAUSE_CHECK_BEFORE_OOM_RETRY(scanner, ctx);
        // Call the wait function
        threadSleepByConfigTime(ctx, scanner);
        if (!isBgIndexingMemoryOverLimit(ctx)) {
          // We can continue the scan
          RedisModule_Log(ctx, "notice", "Scanning index %s in background: resuming after OOM due to memory limit increase",
                          scanner->spec_name_for_logs);
          IndexesScanner_ResetProgression(scanner);
          RedisModule_ScanCursorRestart(cursor);
          continue;
        }
      }
      // At this point we either waited for memory allocation and failed
      // or the config is set to not wait for memory allocation after OOM
      scanStopAfterOOM(ctx, scanner);
      IF_DEBUG_PAUSE_CHECK_ON_OOM(scanner, ctx);
    }

    if (scanner->cancelled) {

      if (scanner->global) {
        RedisModule_Log(ctx, "notice", "Scanning indexes in background: cancelled (scanned=%ld)",
                        scanner->scannedKeys);
      } else {
        RedisModule_Log(ctx, "notice", "Scanning index %s in background: cancelled (scanned=%ld)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
        goto end;
      }
    }
  }

  if (scanner->isDebug) {
    DebugIndexesScanner* dScanner = (DebugIndexesScanner*)scanner;
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_DONE;
  }

  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%ld)",
                    scanner->scannedKeys);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background: done (scanned=%ld)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
  }

end:
  if (!scanner->cancelled && scanner->global) {
    Indexes_SetTempSpecsTimers(TimerOp_Add);
  }

  IndexesScanner_Free(scanner);

  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindexAsync(StrongRef spec_ref) {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "reindex");
  }
#ifdef _DEBUG
  IndexSpec* spec = (IndexSpec*)StrongRef_Get(spec_ref);
  const char* indexName = IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog);
  RedisModule_Log(RSDummyContext, "notice", "Register index %s for async scan", indexName);
#endif
  IndexesScanner *scanner;
  if (globalDebugCtx.debugMode) {
    // If we are in debug mode, we need to allocate a debug scanner
    scanner = (IndexesScanner*)DebugIndexesScanner_New(spec_ref);
    // If we need to pause before the scan, we set the pause flag
    if (globalDebugCtx.bgIndexing.pauseBeforeScan) {
      globalDebugCtx.bgIndexing.pause = true;
    }
  } else {
    scanner = IndexesScanner_New(spec_ref);
  }

  redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner, THPOOL_PRIORITY_HIGH);
}

void ReindexPool_ThreadPoolDestroy() {
  if (reindexPool != NULL) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(reindexPool);
    reindexPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

//---------------------------------------------------------------------------------------------

#ifdef FTINFO_FOR_INFO_MODULES
void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp) {
  char *temp = "info";
  char name[sp->nameLen + 4 + 2]; // 4 for info and 2 for null termination
  sprintf(name, "%s_%s", temp, sp->name);
  RedisModule_InfoAddSection(ctx, name);

  // Index flags
  if (sp->flags & ~(Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_StoreByteOffsets) || sp->flags & Index_WideSchema) {
    RedisModule_InfoBeginDictField(ctx, "index_options");
    if (!(sp->flags & (Index_StoreFreqs)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOFREQS_STR, "ON");
    if (!(sp->flags & (Index_StoreFieldFlags)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOFIELDS_STR, "ON");
    if (!(sp->flags & (Index_StoreTermOffsets | Index_StoreByteOffsets)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOOFFSETS_STR, "ON");
    if (!(sp->flags & (Index_StoreByteOffsets)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOHL_STR, "ON");
    if (sp->flags & Index_WideSchema)
      RedisModule_InfoAddFieldCString(ctx, SPEC_SCHEMA_EXPANDABLE_STR, "ON");
    RedisModule_InfoEndDictField(ctx);
  }

  // Index definition
  RedisModule_InfoBeginDictField(ctx, "index_definition");
  SchemaRule *rule = sp->rule;
  RedisModule_InfoAddFieldCString(ctx, "type", (char*)DocumentType_ToString(rule->type));
  if (rule->filter_exp_str)
    RedisModule_InfoAddFieldCString(ctx, "filter", rule->filter_exp_str);
  if (rule->lang_default)
    RedisModule_InfoAddFieldCString(ctx, "default_language", (char*)RSLanguage_ToString(rule->lang_default));
  if (rule->lang_field)
    RedisModule_InfoAddFieldCString(ctx, "language_field", rule->lang_field);
  if (rule->score_default)
    RedisModule_InfoAddFieldDouble(ctx, "default_score", rule->score_default);
  if (rule->score_field)
    RedisModule_InfoAddFieldCString(ctx, "score_field", rule->score_field);
  if (rule->payload_field)
    RedisModule_InfoAddFieldCString(ctx, "payload_field", rule->payload_field);
  // Prefixes
  int num_prefixes = array_len(rule->prefixes);
  if (num_prefixes && rule->prefixes[0][0] != '\0') {
    arrayof(char) prefixes = array_new(char, 512);
    for (int i = 0; i < num_prefixes; ++i) {
      prefixes = array_ensure_append_1(prefixes, "\"");
      prefixes = array_ensure_append_n(prefixes, rule->prefixes[i], strlen(rule->prefixes[i]));
      prefixes = array_ensure_append_n(prefixes, "\",", 2);
    }
    prefixes[array_len(prefixes)-1] = '\0';
    RedisModule_InfoAddFieldCString(ctx, "prefixes", prefixes);
    array_free(prefixes);
  }
  RedisModule_InfoEndDictField(ctx);

  // Attributes
  for (int i = 0; i < sp->numFields; i++) {
    const FieldSpec *fs = sp->fields + i;
    char title[28];
    sprintf(title, "%s_%d", "field", (i+1));
    RedisModule_InfoBeginDictField(ctx, title);

    RedisModule_InfoAddFieldCString(ctx, "identifier", fs->path);
    RedisModule_InfoAddFieldCString(ctx, "attribute", fs->name);

    if (fs->options & FieldSpec_Dynamic)
      RedisModule_InfoAddFieldCString(ctx, "type", "<DYNAMIC>");
    else
      RedisModule_InfoAddFieldCString(ctx, "type", (char*)FieldSpec_GetTypeNames(INDEXTYPE_TO_POS(fs->types)));

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT))
      RedisModule_InfoAddFieldDouble(ctx,  SPEC_WEIGHT_STR, fs->ftWeight);
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[4];
      sprintf(buf, "\"%c\"", fs->tagOpts.tagSep);
      RedisModule_InfoAddFieldCString(ctx, SPEC_TAG_SEPARATOR_STR, buf);
    }
    if (FieldSpec_IsSortable(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_SORTABLE_STR, "ON");
    if (FieldSpec_IsNoStem(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOSTEM_STR, "ON");
    if (!FieldSpec_IsIndexable(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOINDEX_STR, "ON");

    RedisModule_InfoEndDictField(ctx);
  }

  // More properties
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_docs", sp->stats.numDocuments);

  RedisModule_InfoBeginDictField(ctx, "index_properties");
  RedisModule_InfoAddFieldULongLong(ctx, "max_doc_id", sp->docs.maxDocId);
  RedisModule_InfoAddFieldLongLong(ctx, "num_terms", sp->stats.numTerms);
  RedisModule_InfoAddFieldLongLong(ctx, "num_records", sp->stats.numRecords);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_properties_in_mb");
  RedisModule_InfoAddFieldDouble(ctx, "inverted_size", sp->stats.invertedSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "vector_index_size", IndexSpec_VectorIndexesSize(sp) / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "offset_vectors_size", sp->stats.offsetVecsSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "doc_table_size", sp->docs.memsize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "sortable_values_size", sp->docs.sortablesSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "key_table_size", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  RedisModule_InfoAddFieldDouble("tag_overhead_size_mb", IndexSpec_collect_tags_overhead(sp) / (float)0x100000);
  RedisModule_InfoAddFieldDouble("text_overhead_size_mb", IndexSpec_collect_text_overhead(sp) / (float)0x100000);
  RedisModule_InfoAddFieldDouble("total_index_memory_sz_mb", IndexSpec_TotalMemUsage(sp) / (float)0x100000);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoAddFieldULongLong(ctx, "total_inverted_index_blocks", TotalIIBlocks());

  RedisModule_InfoBeginDictField(ctx, "index_properties_averages");
  RedisModule_InfoAddFieldDouble(ctx, "records_per_doc_avg",(float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  RedisModule_InfoAddFieldDouble(ctx, "bytes_per_record_avg",(float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  RedisModule_InfoAddFieldDouble(ctx, "offsets_per_term_avg",(float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  RedisModule_InfoAddFieldDouble(ctx, "offset_bits_per_record_avg",8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_failures");
  RedisModule_InfoAddFieldLongLong(ctx, "hash_indexing_failures", sp->stats.indexingFailures);
  RedisModule_InfoAddFieldLongLong(ctx, "indexing", !!global_spec_scanner || sp->scan_in_progress);
  IndexesScanner *scanner = global_spec_scanner ? global_spec_scanner : sp->scanner;
  double percent_indexed = IndexesScanner_IndexedPercent(ctx, scanner, sp);
  RedisModule_InfoAddFieldDouble(ctx, "percent_indexed", percent_indexed);
  RedisModule_InfoEndDictField(ctx);

  // Garbage collector
  if (sp->gc)
    GCContext_RenderStatsForInfo(sp->gc, ctx);

  // Cursor stat
  Cursors_RenderStatsForInfo(&g_CursorsList, &g_CursorsListCoord, sp, ctx);

  // Stop words
  if (sp->flags & Index_HasCustomStopwords)
    AddStopWordsListToInfo(ctx, sp->stopwords);
}
#endif // FTINFO_FOR_INFO_MODULES

// Assumes that the spec is in a safe state to set a scanner on it (write lock or main thread)
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef spec_ref) {
  size_t nkeys = RedisModule_DbSize(ctx);
  if (nkeys > 0) {
    IndexSpec_ScanAndReindexAsync(spec_ref);
  }
}

// only used on "RDB load finished" event (before the server is ready to accept commands)
// so it threadsafe
void IndexSpec_DropLegacyIndexFromKeySpace(IndexSpec *sp) {
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(RSDummyContext, sp);

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  TrieIterator *it = Trie_Iterate(ctx.spec->terms, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModuleString *keyName = fmtRedisTermKey(&ctx, res, strlen(res));
    Redis_DropScanHandler(ctx.redisCtx, keyName, &ctx);
    RedisModule_FreeString(ctx.redisCtx, keyName);
    rm_free(res);
  }
  TrieIterator_Free(it);

  // Delete the numeric, tag, and geo indexes which reside on separate keys
  for (size_t i = 0; i < ctx.spec->numFields; i++) {
    const FieldSpec *fs = ctx.spec->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
      Redis_DeleteKey(ctx.redisCtx, IndexSpec_GetFormattedKey(ctx.spec, fs, INDEXFLD_T_NUMERIC));
    }
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      Redis_DeleteKey(ctx.redisCtx, IndexSpec_GetFormattedKey(ctx.spec, fs, INDEXFLD_T_TAG));
    }
    if (FIELD_IS(fs, INDEXFLD_T_GEO)) {
      Redis_DeleteKey(ctx.redisCtx, IndexSpec_GetFormattedKey(ctx.spec, fs, INDEXFLD_T_GEO));
    }
  }
  HiddenString_DropFromKeySpace(ctx.redisCtx, INDEX_SPEC_KEY_FMT, sp->specName);
}

void Indexes_UpgradeLegacyIndexes() {
  dictIterator *iter = dictGetIterator(legacySpecDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    IndexSpec_DropLegacyIndexFromKeySpace(sp);

    // recreate the doctable
    DocTable_Free(&sp->docs);
    sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);

    // clear index stats
    memset(&sp->stats, 0, sizeof(sp->stats));
    // Init the index error
    sp->stats.indexError = IndexError_Init();

    // put the new index in the specDict_g with weak and strong references
    dictAdd(specDict_g, (void*)sp->specName, spec_ref.rm);
  }
  dictReleaseIterator(iter);
}

void Indexes_ScanAndReindex() {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "reindex");
  }

  RedisModule_Log(RSDummyContext, "notice", "Scanning all indexes");
  IndexesScanner *scanner = IndexesScanner_NewGlobal();
  // check no global scan is in progress
  if (scanner) {
    redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner, THPOOL_PRIORITY_HIGH);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec_RdbSave(RedisModuleIO *rdb, IndexSpec *sp) {
  // Save the name plus the null terminator
  HiddenString_SaveToRdb(sp->specName, rdb);
  RedisModule_SaveUnsigned(rdb, (uint64_t)sp->flags);
  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec_RdbSave(rdb, &sp->fields[i]);
  }

  SchemaRule_RdbSave(sp->rule, rdb);

  // If we have custom stopwords, save them
  if (sp->flags & Index_HasCustomStopwords) {
    StopWordList_RdbSave(rdb, sp->stopwords);
  }

  if (sp->flags & Index_HasSmap) {
    SynonymMap_RdbSave(rdb, sp->smap);
  }

  RedisModule_SaveUnsigned(rdb, sp->timeout);

  if (sp->aliases) {
    RedisModule_SaveUnsigned(rdb, array_len(sp->aliases));
    for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
      HiddenString_SaveToRdb(sp->aliases[ii], rdb);
    }
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
}

IndexSpec *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, QueryError *status) {
  char *rawName = LoadStringBuffer_IOError(rdb, NULL, goto cleanup_no_index);
  size_t len = strlen(rawName);
  HiddenString* specName = NewHiddenString(rawName, len, true);
  RedisModule_Free(rawName);

  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  StrongRef spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;


  // Note: indexError, fieldIdToIndex, docs, specName, obfuscatedName, terms, and monitor flags are already initialized in initializeIndexSpec
  IndexFlags flags = (IndexFlags)LoadUnsigned_IOError(rdb, goto cleanup);
  // Note: monitorDocumentExpiration and monitorFieldExpiration are already set in initializeIndexSpec
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    flags |= Index_StoreFreqs;
  }
  int16_t numFields = LoadUnsigned_IOError(rdb, goto cleanup);

  initializeIndexSpec(sp, specName, flags, numFields);

  IndexSpec_MakeKeyless(sp);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FieldSpec_RdbLoad(rdb, fs, spec_ref, encver) != REDISMODULE_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to load index field", " %d", i);
      goto cleanup;
    }
    if (fs->ftId != RS_INVALID_FIELD_ID) {
      // Prefer not to rely on the ordering of fields in the RDB file
      *array_ensure_at(&sp->fieldIdToIndex, fs->ftId, t_fieldIndex) = fs->index;
    }
    if (FieldSpec_IsSortable(fs)) {
      sp->numSortableFields++;
    }
    if (FieldSpec_HasSuffixTrie(fs) && FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      sp->flags |= Index_HasSuffixTrie;
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
      }
    }
  }
  // After loading all the fields, we can build the spec cache
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  if (SchemaRule_RdbLoad(spec_ref, rdb, encver, status) != REDISMODULE_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to load schema rule");
    goto cleanup;
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
    if (sp->stopwords == NULL)
      goto cleanup;
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
    if (sp->smap == NULL)
      goto cleanup;
  }

  sp->timeout = LoadUnsigned_IOError(rdb, goto cleanup);

  size_t narr = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t ii = 0; ii < narr; ++ii) {
    QueryError _status;
    char *s = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    HiddenString* alias = NewHiddenString(s, strlen(s), false);
    int rc = IndexAlias_Add(alias, spec_ref, 0, &_status);
    HiddenString_Free(alias, false);
    RedisModule_Free(s);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(RSDummyContext, "notice", "Loading existing alias failed");
    }
  }

  if (isFlex) {
    // TODO: Change to `if (isFlex && !(sp->flags & Index_StoreInRAM)) {` once
    // we add the `Index_StoreInRAM` flag to the rdb file.
    RS_ASSERT(disk_db);
    sp->diskSpec = SearchDisk_OpenIndex(HiddenString_GetUnsafe(sp->specName, NULL), sp->rule->type);
  }

  return sp;

cleanup:
  StrongRef_Release(spec_ref);
cleanup_no_index:
  QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "while reading an index");
  return NULL;
}

static int IndexSpec_StoreAfterRdbLoad(IndexSpec *sp) {

  if (!sp) {
    addPendingIndexDrop();
    return REDISMODULE_ERR;
  }

  StrongRef spec_ref = sp->own_ref;

  // setting isDuplicate to true will make sure index will not be removed from aliases container.
  const RefManager *oldSpec = dictFetchValue(specDict_g, sp->specName);
  sp->isDuplicate = oldSpec != NULL;
  if (sp->isDuplicate) {
    // spec already exists, however we need to finish consuming the rdb so redis won't issue an error(expecting an eof but seeing remaining data)
    // right now this can cause nasty side effects, to avoid them we will set isDuplicate to true
    RedisModule_Log(RSDummyContext, "notice", "Loading an already existing index, will just ignore.");
  }

  Cursors_initSpec(sp);

  if (sp->isDuplicate) {
    // spec already exists lets just free this one
    // Remove the new spec from the global prefixes dictionary.
    // This is the only global structure that we added the new spec to at this point
    SchemaPrefixes_RemoveSpec(spec_ref);
    addPendingIndexDrop();
    StrongRef_Release(spec_ref);
  } else {
    IndexSpec_StartGC(spec_ref, sp);
    dictAdd(specDict_g, (void*)sp->specName, spec_ref.rm);

    for (int i = 0; i < sp->numFields; i++) {
      FieldsGlobalStats_UpdateStats(sp->fields + i, 1);
    }
  }
  return REDISMODULE_OK;
}

static int IndexSpec_CreateFromRdb(RedisModuleIO *rdb, int encver, QueryError *status) {
  // Load the index spec using the new function
  IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, status);
  return IndexSpec_StoreAfterRdbLoad(sp);
}

void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < LEGACY_INDEX_MIN_VERSION || encver > LEGACY_INDEX_MAX_VERSION) {
    return NULL;
  }
  char *legacyName = RedisModule_LoadStringBuffer(rdb, NULL);

  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  IndexSpec_InitLock(sp);
  StrongRef spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;

  IndexSpec_MakeKeyless(sp);
  sp->numSortableFields = 0;
  sp->terms = NULL;
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);

  sp->specName = NewHiddenString(legacyName, strlen(legacyName), true);
  sp->obfuscatedName = IndexSpec_FormatObfuscatedName(sp->specName);
  RedisModule_Free(legacyName);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = RedisModule_LoadUnsigned(rdb);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    initializeFieldSpec(fs, i);
    FieldSpec_RdbLoad(rdb, fs, spec_ref, encver);
    if (FieldSpec_IsSortable(fs)) {
      sp->numSortableFields++;
    }
  }
  // After loading all the fields, we can build the spec cache
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  IndexStats_RdbLoad(rdb, &sp->stats);

  DocTable_LegacyRdbLoad(&sp->docs, rdb, encver);
  /* For version 3 or up - load the generic trie */
  if (encver >= 3) {
    sp->terms = TrieType_GenericLoad(rdb, 0);
  } else {
    sp->terms = NewTrie(NULL, Trie_Sort_Lex);
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->smap = NULL;
  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
  }
  if (encver < INDEX_MIN_EXPIRE_VERSION) {
    sp->timeout = -1;
  } else {
    sp->timeout = RedisModule_LoadUnsigned(rdb);
  }

  if (encver >= INDEX_MIN_ALIAS_VERSION) {
    size_t narr = RedisModule_LoadUnsigned(rdb);
    for (size_t ii = 0; ii < narr; ++ii) {
      QueryError status;
      size_t dummy;
      char *s = RedisModule_LoadStringBuffer(rdb, &dummy);
      HiddenString* alias = NewHiddenString(s, strlen(s), false);
      int rc = IndexAlias_Add(alias, spec_ref, 0, &status);
      HiddenString_Free(alias, false);
      RedisModule_Free(s);
      RS_ASSERT(rc == REDISMODULE_OK);
    }
  }

  const char *formattedIndexName = IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog);
  SchemaRuleArgs *rule_args = dictFetchValue(legacySpecRules, sp->specName);
  if (!rule_args) {
    RedisModule_LogIOError(rdb, "warning",
                           "Could not find upgrade definition for legacy index '%s'", formattedIndexName);
    StrongRef_Release(spec_ref);
    return NULL;
  }

  QueryError status;
  sp->rule = SchemaRule_Create(rule_args, spec_ref, &status);

  if (isFlex) {
    // TODO: Change to `if (isFlex && !(sp->flags & Index_StoreInRAM)) {` once
    // we add the `Index_StoreInRAM` flag to the rdb file.
    RS_ASSERT(disk_db);
    sp->diskSpec = SearchDisk_OpenIndex(HiddenString_GetUnsafe(sp->specName, NULL), sp->rule->type);
  }

  dictDelete(legacySpecRules, sp->specName);
  SchemaRuleArgs_Free(rule_args);

  if (!sp->rule) {
    RedisModule_LogIOError(rdb, "warning", "Failed creating rule for legacy index '%s', error='%s'",
                           formattedIndexName, QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
    StrongRef_Release(spec_ref);
    return NULL;
  }

  // start the gc and add the spec to the cursor list
  IndexSpec_StartGC(spec_ref, sp);
  Cursors_initSpec(sp);

  dictAdd(legacySpecDict, (void*)sp->specName, spec_ref.rm);
  // Subscribe to keyspace notifications
  Initialize_KeyspaceNotifications();

  return spec_ref.rm;
}

void IndexSpec_LegacyRdbSave(RedisModuleIO *rdb, void *value) {
  // we do not save legacy indexes
  return;
}

int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when) {

  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return REDISMODULE_ERR;
  }

  size_t nIndexes = LoadUnsigned_IOError(rdb, goto cleanup);
  QueryError status = QueryError_Default();
  for (size_t i = 0; i < nIndexes; ++i) {
    if (IndexSpec_CreateFromRdb(rdb, encver, &status) != REDISMODULE_OK) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
      QueryError_ClearError(&status);
      return REDISMODULE_ERR;
    }
  }

  // If we have indexes in the auxiliary data, we need to subscribe to the
  // keyspace notifications
  Initialize_KeyspaceNotifications();

  return REDISMODULE_OK;

cleanup:
  return REDISMODULE_ERR;
}

void Indexes_RdbSave(RedisModuleIO *rdb, int when) {

  RedisModule_SaveUnsigned(rdb, dictSize(specDict_g));

  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    IndexSpec_RdbSave(rdb, sp);
  }

  dictReleaseIterator(iter);
}

void Indexes_RdbSave2(RedisModuleIO *rdb, int when) {
  if (dictSize(specDict_g)) {
    Indexes_RdbSave(rdb, when);
  }
}

void *IndexSpec_RdbLoad_Logic(RedisModuleIO *rdb, int encver) {
  if (encver < INDEX_VECSIM_SVS_VAMANA_VERSION) {
    // Legacy index, loaded in order to upgrade from an old version
    return IndexSpec_LegacyRdbLoad(rdb, encver);
  } else {
    // New index, loaded normally.
    // Even though we don't actually load or save the index spec in the key space, this implementation is useful
    // because it allows us to serialize and deserialize the index spec in a clean way.
    QueryError status = QueryError_Default();
    IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, &status);
    if (!sp) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
      QueryError_ClearError(&status);
    }
    return sp;
  }
}

/**
 * Convert an IndexSpec to its RDB serialized form, by calling the `IndexSpecType` rdb_save function.
 * Note that the returned RedisModuleString* must be freed by the caller
 * using RedisModule_FreeString
*/
RedisModuleString * IndexSpec_Serialize(IndexSpec *sp) {
  return RedisModule_SaveDataTypeToString(NULL, sp, IndexSpecType);
}

/**
 * Deserialize an IndexSpec from its RDB serialized form, by calling the `IndexSpecType` rdb_load function.
 * Note that this function also stores the index spec in the global spec dictionary, as if it was loaded
 * from the RDB file.
 * Returns REDISMODULE_OK on success, REDISMODULE_ERR on failure.
 * Does not consume the serialized string, the caller is responsible for freeing it.
*/
int IndexSpec_Deserialize(const RedisModuleString *serialized, int encver) {
  IndexSpec *sp = RedisModule_LoadDataTypeFromStringEncver(serialized, IndexSpecType, encver);
  if (sp) Initialize_KeyspaceNotifications();
  return IndexSpec_StoreAfterRdbLoad(sp);
}

int CompareVersions(Version v1, Version v2) {
  if (v1.majorVersion < v2.majorVersion) {
    return -1;
  } else if (v1.majorVersion > v2.majorVersion) {
    return 1;
  }

  if (v1.minorVersion < v2.minorVersion) {
    return -1;
  } else if (v1.minorVersion > v2.minorVersion) {
    return 1;
  }

  if (v1.patchVersion < v2.patchVersion) {
    return -1;
  } else if (v1.patchVersion > v2.patchVersion) {
    return 1;
  }

  return 0;
}

void Indexes_Propagate(RedisModuleCtx *ctx) {
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    RS_ASSERT(sp != NULL);
    RedisModuleString *serialized = IndexSpec_Serialize(sp);
    RS_ASSERT(serialized != NULL);
    int rc = RedisModule_ClusterPropagateForSlotMigration(ctx, RS_RESTORE_IF_NX, "cls", SPEC_SCHEMA_STR, INDEX_CURRENT_VERSION, serialized);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "Failed to propagate index '%s' during slot migration. errno: %d", IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog), errno);
    }
    RedisModule_FreeString(NULL, serialized);
  }
  dictReleaseIterator(iter);
}

static void IndexSpec_RdbSave_Wrapper(RedisModuleIO *rdb, void *value) {
  IndexSpec_RdbSave(rdb, value);
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .rdb_load = IndexSpec_RdbLoad_Logic,    // We don't store the index spec in the key space,
      .rdb_save = IndexSpec_RdbSave_Wrapper,  // but these are useful for serialization/deserialization (and legacy loading)
      .aux_load = Indexes_RdbLoad,
      .aux_save = Indexes_RdbSave,
      .free = IndexSpec_LegacyFree,
      .aof_rewrite = GenericAofRewrite_DisabledHandler,
      .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
      .aux_save2 = Indexes_RdbSave2,
  };

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "warning", "Could not create index spec type");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  if (!spec->rule) {
    RedisModule_Log(ctx, "warning", "Index spec '%s': no rule found", IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog));
    return REDISMODULE_ERR;
  }

  QueryError status = QueryError_Default();

  if(spec->scan_failed_OOM) {
    QueryError_SetWithoutUserDataFmt(&status, QUERY_ERROR_CODE_INDEX_BG_OOM_FAIL, "Index background scan did not complete due to OOM. New documents will not be indexed.");
    IndexError_AddQueryError(&spec->stats.indexError, &status, key);
    QueryError_ClearError(&status);
    return REDISMODULE_ERR;
  }

  rs_wall_clock startDocTime;
  rs_wall_clock_init(&startDocTime);

  Document doc = {0};
  Document_Init(&doc, key, DEFAULT_SCORE, DEFAULT_LANGUAGE, type);
  // if a key does not exit, is not a hash or has no fields in index schema

  int rv = REDISMODULE_ERR;
  switch (type) {
  case DocumentType_Hash:
    rv = Document_LoadSchemaFieldHash(&doc, &sctx, &status);
    break;
  case DocumentType_Json:
    rv = Document_LoadSchemaFieldJson(&doc, &sctx, &status);
    break;
  case DocumentType_Unsupported:
    RS_ABORT("Should receive valid type");
    break;
  }

  if (rv != REDISMODULE_OK) {
    // we already unlocked the spec but we can increase this value atomically
    IndexError_AddQueryError(&spec->stats.indexError, &status, doc.docKey);

    // if a document did not load properly, it is deleted
    // to prevent mismatch of index and hash
    IndexSpec_DeleteDoc(spec, ctx, key);
    QueryError_ClearError(&status);
    Document_Free(&doc);
    return REDISMODULE_ERR;
  }

  unsigned int numOps = doc.numFields != 0 ? doc.numFields: 1;
  IndexerYieldWhileLoading(ctx, numOps, REDISMODULE_YIELD_FLAG_CLIENTS);
  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_IncrActiveWrites(spec);

  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(spec, &doc, &status);
  aCtx->stateFlags |= ACTX_F_NOFREEDOC;
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE);

  Document_Free(&doc);

  spec->stats.totalIndexTime += rs_wall_clock_elapsed_ns(&startDocTime);
  IndexSpec_DecrActiveWrites(spec);
  RedisSearchCtx_UnlockSpec(&sctx);
  return REDISMODULE_OK;
}

void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, t_docId id) {
  RSDocumentMetadata *md = DocTable_PopR(&spec->docs, key);
  if (md) {
    RS_LOG_ASSERT(spec->stats.numDocuments > 0, "numDocuments cannot be negative");
    spec->stats.numDocuments--;
    RS_LOG_ASSERT(spec->stats.totalDocsLen >= md->len, "totalDocsLen is smaller than dmd->len");
    spec->stats.totalDocsLen -= md->len;
    DMD_Return(md);

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (spec->gc) {
      GCContext_OnDelete(spec->gc);
    }
  }

  // VecSim fields clear deleted data on the fly
  if (spec->flags & Index_HasVecSim) {
    for (int i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
        RedisModuleString *rmskey = IndexSpec_GetFormattedKey(spec, spec->fields + i, INDEXFLD_T_VECTOR);
        VecSimIndex *vecsim = openVectorIndex(spec, rmskey, DONT_CREATE_INDEX);
        if(!vecsim)
          continue;
        VecSimIndex_DeleteVector(vecsim, id);
      }
    }
  }

  if (spec->flags & Index_HasGeometry) {
    GeometryIndex_RemoveId(spec, id);
  }
}

int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  // TODO: is this necessary?
  RedisSearchCtx_LockSpecRead(&sctx);
  // Get the doc ID
  t_docId id = DocTable_GetIdR(&spec->docs, key);
  RedisSearchCtx_UnlockSpec(&sctx);

  if (id == 0) {
    // ID does not exist.
    return REDISMODULE_ERR;
  }

  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_IncrActiveWrites(spec);
  IndexSpec_DeleteDoc_Unsafe(spec, ctx, key, id);
  IndexSpec_DecrActiveWrites(spec);
  RedisSearchCtx_UnlockSpec(&sctx);
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  Indexes_Free(specDict_g);
  workersThreadPool_Drain(ctx, 0);
  Dictionary_Clear();
  RSGlobalStats.totalStats.used_dialects = 0;
}

void Indexes_Init(RedisModuleCtx *ctx) {
  specDict_g = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
}

size_t Indexes_Count() {
  return dictSize(specDict_g);
}

SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   bool runFilters,
                                                   RedisModuleString *keyToReadData) {
  if (!keyToReadData) {
    keyToReadData = key;
  }
  SpecOpIndexingCtx *res = rm_malloc(sizeof(*res));
  res->specs = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  res->specsOps = array_new(SpecOpCtx, 10);
  if (dictSize(specDict_g) == 0) {
    return res;
  }
  dict *specs = res->specs;

#if defined(_DEBUG) && 0
  RLookupKey *k = RLookup_GetKey_LoadEx(&r->lk, UNDERSCORE_KEY, strlen(UNDERSCORE_KEY), UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
  RSValue *v = RLookup_GetItem(k, &r->row);
  const char *x = RSValue_StringPtrLen(v, NULL);
  RedisModule_Log(RSDummyContext, "notice", "Indexes_FindMatchingSchemaRules: x=%s", x);
  const char *f = "name";
  k = RLookup_GetKey_ReadEx(&r->lk, f, strlen(f), RLOOKUP_F_NOFLAGS);
  if (k) {
    v = RLookup_GetItem(k, &r->row);
    x = RSValue_StringPtrLen(v, NULL);
  }
#endif  // _DEBUG

  size_t n;
  const char *key_p = RedisModule_StringPtrLen(key, &n);
  // collect specs that their name is prefixed by the key name
  // `prefixes` includes list of arrays of specs, one for each prefix of key name
  TrieMapResultBuf prefixes = TrieMap_FindPrefixes(SchemaPrefixes_g, key_p, n);
  for (int i = 0; i < TrieMapResultBuf_Len(&prefixes); ++i) {
    SchemaPrefixNode *node = TrieMapResultBuf_GetByIndex(&prefixes, i);
    for (int j = 0; j < array_len(node->index_specs); ++j) {
      StrongRef global = node->index_specs[j];
      IndexSpec *spec = StrongRef_Get(global);
      if (spec && !dictFind(specs, spec->specName)) {
        SpecOpCtx specOp = {
            .spec = spec,
            .op = SpecOp_Add,
        };
        array_append(res->specsOps, specOp);
        dictEntry *entry = dictAddRaw(specs, (void*)spec->specName, NULL);
        // put the location on the specsOps array so we can get it
        // fast using index name
        entry->v.u64 = array_len(res->specsOps) - 1;
      }
    }
  }
  TrieMapResultBuf_Free(prefixes);

  if (runFilters) {

    EvalCtx *r = NULL;
    for (size_t i = 0; i < array_len(res->specsOps); ++i) {
      SpecOpCtx *specOp = res->specsOps + i;
      IndexSpec *spec = specOp->spec;
      if (!spec->rule->filter_exp) {
        continue;
      }

      // load hash only if required
      if (!r) r = EvalCtx_Create();
      RLookup_LoadRuleFields(ctx, &r->lk, &r->row, spec, key_p);

      if (EvalCtx_EvalExpr(r, spec->rule->filter_exp) == EXPR_EVAL_OK) {
        if (!RSValue_BoolTest(&r->res) && dictFind(specs, spec->specName)) {
          specOp->op = SpecOp_Del;
        }
      }
      QueryError_ClearError(r->ee.err);
    }

    if (r) {
      EvalCtx_Destroy(r);
    }
  }
  return res;
}

static bool hashFieldChanged(IndexSpec *spec, RedisModuleString **hashFields) {
  if (hashFields == NULL) {
    return true;
  }

  // TODO: improve implementation to avoid O(n^2)
  for (size_t i = 0; hashFields[i] != NULL; ++i) {
    size_t length = 0;
    const char *field = RedisModule_StringPtrLen(hashFields[i], &length);
    for (size_t j = 0; j < spec->numFields; ++j) {
      if (!HiddenString_CompareC(spec->fields[j].fieldName, field, length)) {
        return true;
      }
    }
    // optimize. change of score and payload fields just require an update of the doc table
    if ((spec->rule->lang_field && !strcmp(field, spec->rule->lang_field)) ||
        (spec->rule->score_field && !strcmp(field, spec->rule->score_field)) ||
        (spec->rule->payload_field && !strcmp(field, spec->rule->payload_field))) {
      return true;
    }
  }
  return false;
}

void Indexes_SpecOpsIndexingCtxFree(SpecOpIndexingCtx *specs) {
  dictRelease(specs->specs);
  array_free(specs->specsOps);
  rm_free(specs);
}

void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields) {
  if (type == DocumentType_Unsupported) {
    // COPY could overwrite a hash/json with other types so we must try and remove old doc
    Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
    return;
  }

  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, true, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;

    // skip if document type does not match the index type
    if (type != specOp->spec->rule->type) {
      continue;
    }

    if (hashFieldChanged(specOp->spec, hashFields)) {
      if (specOp->op == SpecOp_Add) {
        IndexSpec_UpdateDoc(specOp->spec, ctx, key, type);
      } else {
        IndexSpec_DeleteDoc(specOp->spec, ctx, key);
      }
    }
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           RedisModuleString **hashFields) {
  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, false, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;
    if (hashFieldChanged(specOp->spec, hashFields)) {
      IndexSpec_DeleteDoc(specOp->spec, ctx, key);
    }
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key) {
  DocumentType type = getDocTypeFromString(to_key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  SpecOpIndexingCtx *from_specs = Indexes_FindMatchingSchemaRules(ctx, from_key, true, to_key);
  SpecOpIndexingCtx *to_specs = Indexes_FindMatchingSchemaRules(ctx, to_key, true, NULL);

  size_t from_len, to_len;
  const char *from_str = RedisModule_StringPtrLen(from_key, &from_len);
  const char *to_str = RedisModule_StringPtrLen(to_key, &to_len);

  for (size_t i = 0; i < array_len(from_specs->specsOps); ++i) {
    SpecOpCtx *specOp = from_specs->specsOps + i;
    IndexSpec *spec = specOp->spec;
    if (specOp->op == SpecOp_Del) {
      // the document is not in the index from the first place
      continue;
    }
    dictEntry *entry = dictFind(to_specs->specs, spec->specName);
    if (entry) {
      RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
      RedisSearchCtx_LockSpecWrite(&sctx);
      DocTable_Replace(&spec->docs, from_str, from_len, to_str, to_len);
      RedisSearchCtx_UnlockSpec(&sctx);
      size_t index = entry->v.u64;
      dictDelete(to_specs->specs, spec->specName);
      array_del_fast(to_specs->specsOps, index);
    } else {
      IndexSpec_DeleteDoc(spec, ctx, from_key);
    }
  }

  // add to a different index
  for (size_t i = 0; i < array_len(to_specs->specsOps); ++i) {
    SpecOpCtx *specOp = to_specs->specsOps + i;
    if (specOp->op == SpecOp_Del) {
      // not need to index
      // also no need to delete because we know that the document is
      // not in the index because if it was there we would handle it
      // on the spec from section.
      continue;
    }
    IndexSpec_UpdateDoc(specOp->spec, ctx, to_key, type);
  }
  Indexes_SpecOpsIndexingCtxFree(from_specs);
  Indexes_SpecOpsIndexingCtxFree(to_specs);
}

void Indexes_List(RedisModule_Reply* reply, bool obfuscate) {
  RedisModule_Reply_Set(reply);
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(ref);
    CurrentThread_SetIndexSpec(ref);
    const char *specName = IndexSpec_FormatName(sp, obfuscate);
    REPLY_SIMPLE_SAFE(specName);
    CurrentThread_ClearIndexSpec();
  }
  dictReleaseIterator(iter);
  RedisModule_Reply_SetEnd(reply);
  RedisModule_EndReply(reply);
}
///////////////////////////////////////////////////////////////////////////////////////////////

// Debug Scanner Functions

static DebugIndexesScanner *DebugIndexesScanner_New(StrongRef global_ref) {

  DebugIndexesScanner *dScanner = rm_realloc(IndexesScanner_New(global_ref), sizeof(DebugIndexesScanner));

  dScanner->maxDocsTBscanned = globalDebugCtx.bgIndexing.maxDocsTBscanned;
  dScanner->maxDocsTBscannedPause = globalDebugCtx.bgIndexing.maxDocsTBscannedPause;
  dScanner->wasPaused = false;
  dScanner->status = DEBUG_INDEX_SCANNER_CODE_NEW;
  dScanner->base.isDebug = true;
  dScanner->pauseOnOOM = globalDebugCtx.bgIndexing.pauseOnOOM;
  dScanner->pauseBeforeOOMRetry = globalDebugCtx.bgIndexing.pauseBeforeOOMretry;

  IndexSpec *spec = StrongRef_Get(global_ref);
  spec->scanner = (IndexesScanner*)dScanner;

  return dScanner;
}

static void DebugIndexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             DebugIndexesScanner *dScanner) {

  IndexesScanner *scanner = &(dScanner->base);

  if (dScanner->status == DEBUG_INDEX_SCANNER_CODE_NEW) {
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_RUNNING;
  }

  if (dScanner->maxDocsTBscannedPause > 0 && (!dScanner->wasPaused) && scanner->scannedKeys == dScanner->maxDocsTBscannedPause) {
    globalDebugCtx.bgIndexing.pause = true;
    dScanner->wasPaused = true;
  }

  if ((dScanner->maxDocsTBscanned > 0) && (scanner->scannedKeys == dScanner->maxDocsTBscanned)) {
    scanner->cancelled = true;
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_CANCELLED;
  }

  // Check if we need to pause the scan before we release the GIL
  if (globalDebugCtx.bgIndexing.pause)
  {
      // Warning: This section is highly unsafe. RM_Scan does not permit the callback
      // function (i.e., this function) to release the GIL.
      // If the key currently being scanned is deleted after the GIL is released,
      // it can lead to a use-after-free and crash Redis.
      RedisModule_Log(ctx, "warning", "RM_Scan callback function is releasing the GIL, which is unsafe.");

      RedisModule_ThreadSafeContextUnlock(ctx);
      while (globalDebugCtx.bgIndexing.pause) { // volatile variable
        dScanner->status = DEBUG_INDEX_SCANNER_CODE_PAUSED;
        usleep(1000);
      }
      RedisModule_ThreadSafeContextLock(ctx);
  }

  if (dScanner->status == DEBUG_INDEX_SCANNER_CODE_PAUSED) {
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_RESUMED;
  }

  Indexes_ScanProc(ctx, keyname, key, &(dScanner->base));
}

StrongRef IndexSpecRef_Promote(WeakRef ref) {
  StrongRef strong = WeakRef_Promote(ref);
  IndexSpec *spec = StrongRef_Get(strong);
  if (spec) {
    CurrentThread_SetIndexSpec(strong);
  }
  return strong;
}

void IndexSpecRef_Release(StrongRef ref) {
  CurrentThread_ClearIndexSpec();
  StrongRef_Release(ref);
}

// If this function is called, it means that the scan did not complete due to OOM, should be verified by the caller
static inline void DebugIndexesScanner_pauseCheck(DebugIndexesScanner* dScanner, RedisModuleCtx *ctx, bool pauseField, DebugIndexScannerCode code) {
  if (!dScanner || !pauseField) {
    return;
  }
  globalDebugCtx.bgIndexing.pause = true;
  RedisModule_ThreadSafeContextUnlock(ctx);
  while (globalDebugCtx.bgIndexing.pause) { // volatile variable
    dScanner->status = code;
    usleep(1000);
  }
  dScanner->status = DEBUG_INDEX_SCANNER_CODE_RESUMED;
  RedisModule_ThreadSafeContextLock(ctx);
}

void Indexes_StartRDBLoadingEvent() {
  Indexes_Free(specDict_g);
  if (legacySpecDict) {
    dictEmpty(legacySpecDict, NULL);
  } else {
    legacySpecDict = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  }
  g_isLoading = true;
}

void Indexes_EndRDBLoadingEvent(RedisModuleCtx *ctx) {
  int hasLegacyIndexes = dictSize(legacySpecDict);
  Indexes_UpgradeLegacyIndexes();

  // we do not need the legacy dict specs anymore
  dictRelease(legacySpecDict);
  legacySpecDict = NULL;

  LegacySchemaRulesArgs_Free(ctx);

  if (hasLegacyIndexes) {
    Indexes_ScanAndReindex();
  }
}

void Indexes_EndLoading() {
  g_isLoading = false;
}
