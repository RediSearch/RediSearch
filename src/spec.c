/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"
#include "indexes.h"
#include "indexes_scan.h"
#include "document.h"
#include "inverted_index_ffi.h"
#include "numeric_range_tree_ffi.h"
#include "rlookup_load_document.h"

#include <math.h>
#include <ctype.h>
#include <limits.h>

#include "triemap_ffi.h"
#include "util/logging.h"
#include "util/likely.h"
#include "util/misc.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "rmutil/rm_assert.h"
#include "trie/trie.h"
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
#include "doc_id_meta.h"
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
#include "search_disk_utils.h"
#include "iterators_ffi.h"

///////////////////////////////////////////////////////////////////////////////////////////////

const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;

// The global index registry (specDict_g / specIdDict_g) is defined in indexes.c.
// Its extern declarations live in spec.h, since the IndexSpec lifecycle functions
// here read it as a data dependency.

// Maximum number of indexes that can be created.
// Can be modified via FT.DEBUG for testing.
uint32_t maxIndexes_g = DEFAULT_MAX_INDEXES;

dict *legacySpecDict;
dict *legacySpecRules;

// Pending or in-progress index drops
uint16_t pendingIndexDropCount_g = 0;

// Global monotonically increasing counter for unique spec incarnation IDs.
// Each new IndexSpec gets the next value, ensuring that even if an index is
// dropped and recreated with the same name, the new incarnation has a different ID.
static uint64_t nextSpecId_g = 1;

Version redisVersion;
Version rlecVersion;
bool isEnterprise = false;
bool isCrdt;
bool isTrimming = false;
bool isFlex = false;

// Default values make no limits.
size_t memoryLimit = -1;
size_t used_memory = 0;

static redisearch_thpool_t *cleanPool = NULL;

extern DebugCTX globalDebugCtx;


// Forward declaration for disk validation
inline static bool isSpecOnDiskForValidation(const IndexSpec *sp);

/**
 * Checks if SST persistence is enabled for the given RDB context.
 */
bool CheckRdbSstPersistence(RedisModuleCtx *ctx, const char* prefix) {
  const bool useSst = IS_SST_RDB_IN_PROCESS(ctx);
  RedisModule_Log(ctx, "notice", "%s, SST persistence: %s", prefix, useSst ? "true" : "false");
  return useSst;
}

//---------------------------------------------------------------------------------------------



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

// Forward declaration
static StrongRef IndexSpec_ParseFromArgCursor(RedisModuleCtx *ctx, const HiddenString *name,
                                           ArgsCursor *ac_in, QueryError *status);

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
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv, argc);
  StrongRef spec_ref = IndexSpec_ParseFromArgCursor(ctx, name, &ac, status);
  if (QueryError_HasError(status)) {
    // Parsing failed; the spec was never registered, so tear it down here.
    IndexSpec_Unlink(spec_ref, false);
    return INVALID_STRONG_REF;
  }
  return spec_ref;
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
    RedisModuleCallReply *rep = RedisModule_Call(RSDummyContext, CMD_FOR_ENV(RS_DROP_INDEX_CMD), "cc!", HiddenString_GetUnsafe(sp->specName, NULL), "DD");
    if (rep) {
      RedisModule_FreeCallReply(rep);
    }
  }

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "Freeing index '%s' by timer: done", name);
  StrongRef_Release(spec_ref);
}

// Assuming the GIL is held.
// This can be done without locking the spec for write, since the timer is not modified or read by any other thread.
void IndexSpec_SetTimeoutTimer(IndexSpec *sp, WeakRef spec_ref) {
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
void IndexSpec_ResetTimeoutTimer(IndexSpec *sp) {
  if (sp->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, sp->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
  }
  sp->timerId = 0;
  sp->isTimerSet = false;
}


//---------------------------------------------------------------------------------------------


size_t IndexSpec_collect_numeric_overhead(IndexSpec *sp) {
  // Traverse the fields and calculates the overhead of the numeric tree index
  size_t overhead = 0;
  for (size_t i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO)) {
      NumericRangeTree *rt = openNumericOrGeoIndex(sp, fs, DONT_CREATE_INDEX);
      // Numeric index was not initialized yet
      if (!rt) {
        continue;
      }

      overhead += NumericRangeTree_BaseSize();
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
      overhead += TagIndex_GetOverhead(fs);
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

  // For disk indexes, add storage + in-memory components.
  if (sp->diskSpec) {
    res += SearchDisk_CollectIndexMetrics(sp->diskSpec);
  }

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

/* Build a new IndexSpec from a redis command, wiring up its GC, cursors and
 * (for temporary indexes) timeout timer.
 *
 * This is the IndexSpec core: it does NOT publish the spec into the global
 * registry (specDict_g/specIdDict_g) and does NOT start the initial scan. The
 * caller-facing entry point Indexes_CreateNew (indexes.c) performs the
 * existence/limit checks, registers the spec, and schedules the scan around
 * this call. Keeping registry access out of spec.c preserves the one-way
 * indexes -> spec dependency. */
// TODO: multithreaded: use global metadata locks to protect global data structures
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status) {
  setMemoryInfo(ctx);
  size_t nameLen;
  const char *rawName = RedisModule_StringPtrLen(argv[1], &nameLen);
  HiddenString *name = NewHiddenString(rawName, nameLen, true);
  // Create the IndexSpec, along with its corresponding weak\strong refs
  StrongRef spec_ref = IndexSpec_ParseRedisArgs(ctx, name, &argv[2], argc - 2, status);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (sp == NULL) {
    return NULL;
  }

  // Start the garbage collector
  IndexSpec_StartGC(spec_ref, sp, sp->diskSpec ? GCPolicy_Disk : GCPolicy_Fork);

  Cursors_initSpec(sp);

  // set timeout for temporary index on master
  if ((sp->flags & Index_Temporary) && IsMaster()) {
    IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref));
  }

  // (Lazily) Subscribe to keyspace notifications, now that we have at least one
  // spec
  Initialize_KeyspaceNotifications();

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

static int parseVectorField_hnsw(IndexSpec *sp, FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status, bool *rerank) {
  int rc;

  // HNSW mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;
  // Disk-mode mandatory params (tracked here, validated later in parseVectorField)
  bool mandM = false;
  bool mandEfConstruction = false;
  bool mandEfRuntime = false;
  bool rerank_seen = false;

  // Get number of parameters and create a sub-cursor for them
  size_t expNumParam;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity number of parameters: %s", AC_Strerror(rc));
    return 0;
  }
  // Create a sub-cursor with exactly expNumParam arguments
  ArgsCursor subAc;
  if ((rc = AC_GetSlice(ac, &subAc, expNumParam)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity: not enough arguments");
    return 0;
  }

  while (!AC_IsAtEnd(&subAc)) {
    if (AC_AdvanceIfMatch(&subAc, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(&subAc, &params->algoParams.hnswParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_TYPE), rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_DIM)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(&subAc, &params->algoParams.hnswParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status,  VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_INITIAL_CAP), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_M)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.M, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_M), rc);
        return 0;
      }
      mandM = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_EFCONSTRUCTION)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.efConstruction, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EFCONSTRUCTION), rc);
        return 0;
      }
      mandEfConstruction = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_EFRUNTIME)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.efRuntime, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EFRUNTIME), rc);
        return 0;
      }
      mandEfRuntime = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_EPSILON)) {
      if ((rc = AC_GetDouble(&subAc, &params->algoParams.hnswParams.epsilon, AC_F_GE0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EPSILON), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_RERANK)) {
      if (rerank_seen) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
          "Duplicate RERANK parameter");
        return 0;
      }
      if (AC_IsAtEnd(&subAc)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, VECSIM_RERANK " requires an argument");
        return 0;
      }
      size_t rerank_len;
      const char *rerank_value = AC_GetStringNC(&subAc, &rerank_len);
      if (STR_EQCASE(rerank_value, rerank_len, "TRUE")) {
        *rerank = true;
      } else if (STR_EQCASE(rerank_value, rerank_len, "FALSE")) {
        *rerank = false;
      } else {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
          "Syntax error: RERANK value must be TRUE or FALSE");
        return 0;
      }
      rerank_seen = true;
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_HNSW, AC_GetStringNC(&subAc, NULL));
      return 0;
    }
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

  // Disk-mode validation: enforce mandatory parameters
  if (isSpecOnDiskForValidation(sp)) {
    if (params->algoParams.hnswParams.type != VecSimType_FLOAT32) {
      const char *typeName = VecSimType_ToString(params->algoParams.hnswParams.type);
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support %s vector type", typeName);
      return 0;
    }
    if (params->algoParams.hnswParams.multi) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support multi-value vectors");
      return 0;
    }
    if (!mandM) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires M parameter");
      return 0;
    }
    if (!mandEfConstruction) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires EF_CONSTRUCTION parameter");
      return 0;
    }
    if (!mandEfRuntime) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires EF_RUNTIME parameter");
      return 0;
    }
    if (!rerank_seen) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires RERANK parameter");
      return 0;
    }
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
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "TRAINING_THRESHOLD is irrelevant when compression was not requested");
    return 0;
  }
  if (!VecSim_IsLeanVecCompressionType(params->algoParams.svsParams.quantBits) && params->algoParams.svsParams.leanvec_dim > 0) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "REDUCE is irrelevant when compression is not of type LeanVec");
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
      if (!SearchDisk_MarkUnsupportedArgumentIfDiskEnabled(SPEC_WITHSUFFIXTRIE_STR, status)) {
        return 0;
      }
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
        if (!SearchDisk_MarkUnsupportedArgumentIfDiskEnabled(SPEC_WITHSUFFIXTRIE_STR, status)) {
          return 0;
        }
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
  memset(&fs->vectorOpts.diskCtx, 0, sizeof(VecSimDiskContext));

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
    // Disk mode does not support FLAT algorithm
    if (isSpecOnDiskForValidation(sp)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support FLAT algorithm");
      rm_free(logCtx);
      fs->vectorOpts.vecSimParams.logCtx = NULL;  // Prevent double-free in cleanup
      return 0;
    }
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
    bool rerank = false;
    result = parseVectorField_hnsw(sp, fs, params, ac, status, &rerank);
    // Build disk params if disk mode is enabled
    if (result && sp->diskSpec) {
      size_t nameLen;
      const char *namePtr = HiddenString_GetUnsafe(fs->fieldName, &nameLen);
      fs->vectorOpts.diskCtx = (VecSimDiskContext){
        .storage = sp->diskSpec,
        .indexName = rm_strndup(namePtr, nameLen),
        .indexNameLen = nameLen,
        .rerank = rerank,
      };
    }
  } else if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_SVS)) {
    // Disk mode does not support SVS algorithm
    if (isSpecOnDiskForValidation(sp)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support SVS algorithm");
      rm_free(logCtx);
      fs->vectorOpts.vecSimParams.logCtx = NULL;  // Prevent double-free in cleanup
      return 0;
    }
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
    // num_threads is deprecated — SVS indexes now share a global thread pool managed
    // via VecSim_UpdateThreadPoolSize(). Leave it at 0 (default) to avoid the deprecation warning.
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
    if (!SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_GEOMETRY_STR, fs, status)) goto error;
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
    if (!SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_GEO_STR, fs, status)) goto error;
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
 * Validate that a disk-backed JSON field uses a single-value JSONPath.
 * Returns `true` when the validation does not apply or the field path is valid.
 * On failure, sets `status` with the validation error and returns `false`.
 */
static bool validateDiskJsonSinglePath(const IndexSpec *sp, const FieldSpec *fs, QueryError *status) {
  if (!isSpecOnDiskForValidation(sp) || !isSpecJson(sp)) {
    return true;
  }

  RedisModuleString *err_msg = NULL;
  JSONPath jsonPath = pathParse(fs->fieldPath, &err_msg);
  if (!jsonPath) {
    if (err_msg) {
      JSONParse_error(status, err_msg, fs->fieldPath, fs->fieldName, sp->specName);
    }
    return false;
  }

  bool isSingle = japi->pathIsSingle(jsonPath);
  japi->pathFree(jsonPath);
  if (!isSingle) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
                                     "Disk JSON index supports only single-value JSONPath fields");
    return false;
  }

  return true;
}

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
    if (!validateDiskJsonSinglePath(sp, fs, status)) {
      goto reset;
    }
    if (!parseFieldSpec(ac, sp, spec_ref, fs, status)) {
      goto reset;
    }

    if (isSpecOnDiskForValidation(sp))
    {
      if (!FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && !FIELD_IS(fs, INDEXFLD_T_VECTOR) && !FIELD_IS(fs, INDEXFLD_T_TAG) && !FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support non-TEXT/VECTOR/TAG/NUMERIC fields");
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

// Assumes the spec is locked for write.
// NOTE: scheduling the post-alter background scan is the caller's responsibility
// (see CreateIndexAlterCommand in module.c), keeping spec.c free of a scanner edge.
int IndexSpec_AddFields(StrongRef spec_ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac,
                        QueryError *status) {
  setMemoryInfo(ctx);

  return IndexSpec_AddFieldsInternal(sp, spec_ref, ac, status, 0);
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
  return SearchDisk_IsEnabled();
}

inline static bool isSpecOnDiskForValidation(const IndexSpec *sp) {
  return SearchDisk_IsEnabledForValidation();
}

void handleBadArguments(IndexSpec *spec, const char *badarg, QueryError *status, ACArgSpec *non_flex_argopts) {
  if (isSpecOnDiskForValidation(spec)) {
    bool isKnownArg = false;
    for (int i = 0; non_flex_argopts[i].name; i++) {
      if (strcasecmp(badarg, non_flex_argopts[i].name) == 0) {
        isKnownArg = true;
        break;
      }
    }
    if (isKnownArg) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT,
        "Unsupported argument for Flex index:", " `%s`", badarg);
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument", " `%s`", badarg);
    }
  } else {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument", " `%s`", badarg);
  }
}

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */

static StrongRef IndexSpec_ParseFromArgCursor(RedisModuleCtx *ctx, const HiddenString *name,
                                           ArgsCursor *ac, QueryError *status) {
  IndexSpec *spec = NewIndexSpec(name);
  StrongRef spec_ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  spec->own_ref = spec_ref;

  IndexSpec_MakeKeyless(spec);

  ArgsCursor acStopwords = {0};

  long long timeout = -1;
  int dummy;
  size_t dummy2;
  SchemaRuleArgs rule_args = {0};
  ArgsCursor rule_prefixes = {0};
  int rc = AC_OK;
  ACArgSpec *errarg = NULL;
  ACArgSpec flex_argopts[] = {
    {.name = "ON", .target = &rule_args.type, .len = &dummy2, .type = AC_ARGTYPE_STRING},
    {.name = "PREFIX", .target = &rule_prefixes, .type = AC_ARGTYPE_SUBARGS},
    {.name = "FILTER", .target = &rule_args.filter_exp_str, .len = &dummy2, .type = AC_ARGTYPE_STRING},
    {.name = "LANGUAGE", .target = &rule_args.lang_default, .len = &dummy2, .type = AC_ARGTYPE_STRING},
    {.name = "LANGUAGE_FIELD", .target = &rule_args.lang_field, .len = &dummy2, .type = AC_ARGTYPE_STRING},
    {.name = "SCORE", .target = &rule_args.score_default, .len = &dummy2, .type = AC_ARGTYPE_STRING},
    {.name = "SCORE_FIELD", .target = &rule_args.score_field, .len = &dummy2, .type = AC_ARGTYPE_STRING},
    {.name = SPEC_STOPWORDS_STR, .target = &acStopwords, .type = AC_ARGTYPE_SUBARGS},
    {AC_MKBITFLAG(SPEC_SKIPINITIALSCAN_STR, &spec->flags, Index_SkipInitialScan)},
    {.name = NULL}
  };
  ACArgSpec non_flex_argopts[] = {
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
    {.name = NULL}
  };
  ACArgSpec *argopts = isSpecOnDiskForValidation(spec) ? flex_argopts : non_flex_argopts;
  rc = AC_ParseArgSpec(ac, argopts, &errarg);
  if (rc != AC_OK) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      goto failure;
    }
  }
  if ((spec->flags & Index_WideSchema) && !(spec->flags & Index_StoreFieldFlags)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_INVAL,
                        SPEC_SCHEMA_EXPANDABLE_STR " cannot be used with " SPEC_NOFIELDS_STR);
    goto failure;
  }

  if (timeout != -1) {
    // When disk validation is active, argopts is set to flex_argopts, which does not include SPEC_TEMPORARY_STR
    RS_ASSERT(!SearchDisk_IsEnabled());
    spec->flags |= Index_Temporary;
  }
  spec->timeout = timeout * 1000;  // convert to ms

  if (rule_prefixes.argc > 0) {
    spec->rule = SchemaRule_CreateWithPrefixesAC(&rule_args, &rule_prefixes, spec_ref, status);
  } else {
    rule_args.nprefixes = 1;
    static const char *empty_prefix[] = {""};
    rule_args.prefixes = empty_prefix;
    spec->rule = SchemaRule_Create(&rule_args, spec_ref, status);
  }
  if (!spec->rule) {
    goto failure;
  }

  // Store on disk if we're on Flex.
  // This must be done before IndexSpec_AddFieldsInternal so that sp->diskSpec
  // is available when parsing vector fields (for populating diskCtx).
  // For new indexes (FT.CREATE), we don't delete before open since there's nothing to delete.
  spec->diskSpec = NULL;
  if (isSpecOnDisk(spec)) {
    RS_ASSERT(disk_db);
    spec->diskSpec = SearchDisk_OpenIndex(ctx, spec->specName, spec->obfuscatedName, spec->rule->type, false, spec);
    RS_LOG_ASSERT(spec->diskSpec, "Failed to open disk spec")
    if (!spec->diskSpec) {
      QueryError_SetError(status, QUERY_ERROR_CODE_DISK_CREATION, "Could not open disk index");
      goto failure;
    }
    SearchDisk_RegisterIndex(ctx, spec);
  }

  if (AC_IsInitialized(&acStopwords)) {
    if (spec->stopwords) {
      StopWordList_Unref(spec->stopwords);
    }
    spec->stopwords = NewStopWordListAC(&acStopwords);
    spec->flags |= Index_HasCustomStopwords;
  }

  if (!AC_AdvanceIfMatch(ac, SPEC_SCHEMA_STR)) {
    if (AC_NumRemaining(ac)) {
      const char *badarg = AC_GetStringNC(ac, NULL);
      handleBadArguments(spec, badarg, status, non_flex_argopts);
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "No schema found");
    }
    goto failure;
  }

  if (!IndexSpec_AddFieldsInternal(spec, spec_ref, ac, status, 1)) {
    goto failure;
  }

  if (spec->rule->filter_exp) {
    SchemaRule_FilterFields(spec);
  }

  if (isSpecOnDiskForValidation(spec) && !(spec->flags & Index_SkipInitialScan)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_FLEX_SKIP_INITIAL_SCAN_MISSING_ARGUMENT, "Flex index requires SKIPINITIALSCAN argument");
    goto failure;
  }

  return spec_ref;

failure:  // on failure return the partially-built spec with the error set in `status`;
          // the caller is responsible for tearing it down (it was never registered).
  spec->flags &= ~Index_Temporary;
  if (spec->diskSpec) {
    SearchDisk_UnregisterIndex(ctx, spec);
  }
  return spec_ref;
}

StrongRef IndexSpec_ParseC(RedisModuleCtx *ctx, const char *name, const char **argv, int argc, QueryError *status) {
  HiddenString *hidden = NewHiddenString(name, strlen(name), true);
  ArgsCursor ac = {0};
  ArgsCursor_InitCString(&ac, argv, argc);
  StrongRef spec_ref = IndexSpec_ParseFromArgCursor(ctx, hidden, &ac, status);
  if (QueryError_HasError(status)) {
    // Parsing failed; the spec was never registered, so tear it down here.
    IndexSpec_Unlink(spec_ref, false);
    return INVALID_STRONG_REF;
  }
  return spec_ref;
}

static void RSIndexStats_FromScoringStats(const ScoringIndexStats *scoring, RSIndexStats *stats) {
  stats->numDocs = scoring->numDocuments;
  stats->numTerms = scoring->numTerms;
  stats->avgDocLen = stats->numDocs ? (double)scoring->totalDocsLen / (double)scoring->numDocuments : 0;
}

/* Initialize some index stats that might be useful for scoring functions */
// Assuming the spec is properly locked before calling this function
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats) {
  RSIndexStats_FromScoringStats(&sp->stats.scoring, stats);
}

size_t IndexSpec_GetIndexErrorCount(const IndexSpec *sp) {
  return IndexError_ErrorCount(&sp->stats.indexError);
}

// Assuming the spec is properly locked for writing before calling this function.
void IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  // Payload is NULL so TRIE_ERR_PAYLOAD_OVERFLOW cannot occur
  int isNew = Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL, 1);
  if (isNew == TRIE_OK_NEW) {
    sp->stats.scoring.numTerms++;
    sp->stats.termsSize += len;
  }
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

  // Free fields data first. For disk-backed vector fields,
  // IndexSpec_PopulateVectorDiskParams stored spec->diskSpec in each field's
  // diskCtx.storage, and FieldSpec_Cleanup routes those vector handles through
  // SearchDisk_FreeVectorIndex; freeing a VecSim disk index after its backing
  // storage handle was closed would be a use-after-free, so this must run
  // before the disk close below.
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      FieldSpec_Cleanup(&spec->fields[i]);
    }
    rm_free(spec->fields);
  }

  // Close the disk index right after the fields are cleaned up, while the rest
  // of the spec's state and locks are still fully alive. Background jobs
  // bound at open time hold this IndexSpec as their private data and may call
  // back into it (IndexSpec_AcquireWriteLock,
  // IndexSpec_DecrementTrieTermCount, ...) while the backend drains them during close.
  //  Closing here — before any of the trie/dict/lock
  // teardown below — keeps every field those callbacks might touch valid.
  if (spec->diskSpec) {
    SearchDisk_CloseIndex(spec->diskSpec);
    spec->diskSpec = NULL;
  }
  if (spec->pendingDiskRdbState) {
    SearchDisk_FreeRdbState(spec->pendingDiskRdbState);
    spec->pendingDiskRdbState = NULL;
  }

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

  // Free suffix trie
  if (spec->suffix) {
    TrieType_Free(spec->suffix);
  }

  // Free spec name
  HiddenString_Free(spec->specName, true);
  rm_free(spec->obfuscatedName);

  // Destroy the spec's lock. Safe now: the disk index was already closed above,
  // so no compaction listener can reference these locks.
  pthread_rwlock_destroy(&spec->rwlock);

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
  if (RSGlobalConfig.freeResourcesThread == false || SearchDisk_IsEnabled()) {
    IndexSpec_FreeUnlinkedData(spec);
  } else {
    redisearch_thpool_add_work(cleanPool, (redisearch_thpool_proc)IndexSpec_FreeUnlinkedData, spec, THPOOL_PRIORITY_HIGH);
  }
}

//---------------------------------------------------------------------------------------------

// Tear down a spec's non-registry global state: aliases, schema prefixes, the
// temporary-index timer, and global field statistics, then consume the strong
// reference it is given. Does NOT remove the spec from the global registry
// (specDict_g/specIdDict_g) - that is owned by indexes.c. Indexes_RemoveFromGlobals
// calls this after deleting the registry entries; the create/parse failure path
// calls it directly (the spec was never registered there).
// Assumes this is called from the main thread with no competing threads.
void IndexSpec_Unlink(StrongRef spec_ref, bool removeActive) {
  IndexSpec *spec = StrongRef_Get(spec_ref);

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

static RedisModuleString *fmtRedisNumericIndexKey(const RedisSearchCtx *ctx, const HiddenString *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, "nm:%s/%s", HiddenString_GetUnsafe(ctx->spec->specName, NULL), HiddenString_GetUnsafe(field, NULL));
}
/* Format the key name for a tag index */
static RedisModuleString *TagIndex_FormatName(const IndexSpec *spec, const HiddenString* field) {
  return RedisModule_CreateStringPrintf(RSDummyContext, "tag:%s/%s", HiddenString_GetUnsafe(spec->specName, NULL), HiddenString_GetUnsafe(field, NULL));
}

// Assuming the spec is properly locked before calling this function.
RedisModuleString *IndexSpec_LegacyGetFormattedKey(IndexSpec *sp, const FieldSpec *fs,
                                             FieldType forType) {

  size_t typeix = INDEXTYPE_TO_POS(forType);

  RedisModuleString *ret = NULL;

  RedisSearchCtx sctx = {.redisCtx = RSDummyContext, .spec = sp};
  switch (forType) {
    case INDEXFLD_T_NUMERIC:
    case INDEXFLD_T_GEO:
      ret = fmtRedisNumericIndexKey(&sctx, fs->fieldName);
      break;
    case INDEXFLD_T_TAG:
      ret = TagIndex_FormatName(sctx.spec, fs->fieldName);
      break;
    case INDEXFLD_T_VECTOR:    // Not in legacy
    case INDEXFLD_T_GEOMETRY:  // Not in legacy
    case INDEXFLD_T_FULLTEXT:  // Text fields don't get a per-field index
    default:
      RS_ABORT_ALWAYS("Unsupported field type for legacy formatted key");
      break;
  }
  return ret;
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
  // Writer-preferring: avoids writer starvation when readers arrive in a
  // steady stream.
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
                                uint16_t numFields) {
  sp->flags = flags;
  sp->numFields = numFields;
  sp->fields = rm_calloc(numFields, sizeof(FieldSpec));
  sp->specName = name;
  sp->obfuscatedName = IndexSpec_FormatObfuscatedName(name);
  // Assign a unique specId from the global counter. This ensures that even if
  // an index is dropped and recreated with the same name, the new incarnation
  // has a different ID. The specId is not persisted — on RDB load, each spec
  // gets a fresh sequential ID.
  sp->specId = nextSpecId_g++;
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->suffix = NULL;
  sp->suffixMask = (t_fieldMask)0;
  sp->keysDict = NULL;
  sp->timeout = 0;
  sp->isTimerSet = false;
  sp->timerId = 0;

  sp->scanner = NULL;
  sp->scan_in_progress = false;
  sp->diskSpec = NULL;
  sp->pendingDiskRdbState = NULL;
  sp->diskRegistered = false;
  sp->monitorDocumentExpiration = RSGlobalConfig.monitorExpiration;
  sp->monitorFieldExpiration = RSGlobalConfig.monitorExpiration &&
                               RedisModule_HashFieldMinExpire != NULL;
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
  return fs;
}

uint64_t CharBuf_HashFunction(const void *key) {
  const CharBuf *cb = key;
  return RS_dictGenHashFunction(cb->buf, cb->len);
}

void *CharBuf_KeyDup(void *privdata, const void *key) {
  const CharBuf *cb = key;
  CharBuf *newcb = rm_malloc(sizeof(*newcb));
  newcb->len = cb->len;
  newcb->buf = rm_malloc(cb->len);
  memcpy(newcb->buf, cb->buf, cb->len);
  return newcb;
}

int CharBuf_KeyCompare(void *privdata, const void *key1, const void *key2) {
  const CharBuf *cb1 = key1;
  const CharBuf *cb2 = key2;
  if (cb1->len != cb2->len) {
    return 0;
  }
  return (memcmp(cb1->buf, cb2->buf, cb1->len) == 0);
}

void CharBuf_KeyDestructor(void *privdata, void *key) {
  CharBuf *cb = key;
  rm_free(cb->buf);
  rm_free(cb);
}

void InvIndFreeCb(void *privdata, void *val) {
  InvertedIndex *idx = val;
  InvertedIndex_Free(idx);
}

static dictType invIdxDictType = {
  .hashFunction = CharBuf_HashFunction,
  .keyDup = CharBuf_KeyDup,
  .valDup = NULL, // Taking and owning the InvertedIndex pointer
  .keyCompare = CharBuf_KeyCompare,
  .keyDestructor = CharBuf_KeyDestructor,
  .valDestructor = InvIndFreeCb,
};

static dictType missingFieldDictType = {
        .hashFunction = hiddenNameHashFunction,
        .keyDup = hiddenNameKeyDup,
        .valDup = NULL,
        .keyCompare = hiddenNameKeyCompare,
        .keyDestructor = hiddenNameKeyDestructor,
        .valDestructor = InvIndFreeCb,
};

// Only used on new specs so it's thread safe
void IndexSpec_MakeKeyless(IndexSpec *sp) {
  sp->keysDict = dictCreate(&invIdxDictType, NULL);
  sp->missingFieldDict = dictCreate(&missingFieldDictType, NULL);
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
// Only used on new specs so it's thread safe
void IndexSpec_StartGC(StrongRef global, IndexSpec *sp, GCPolicy gcPolicy) {
  RS_LOG_ASSERT(!sp->gc, "GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.gcConfigParams.enableGC && !(sp->flags & Index_Temporary)) {
    sp->gc = GCContext_CreateGC(global, gcPolicy);
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

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f, int contextFlags) {
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
    // RERANK applies to HNSW (TIERED+HNSWLIB) only — BF/SVS streams stay
    // unchanged. The byte is written for every HNSW field regardless of disk
    // mode so the config survives RDB save/load uniformly.
    if (f->vectorOpts.vecSimParams.algo == VecSimAlgo_TIERED &&
        f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
      RedisModule_SaveUnsigned(rdb, f->vectorOpts.diskCtx.rerank ? 1 : 0);
    }
    // Disk-backed vector fields ride their in-memory state inline with the field's RDB encoding so the
    // load path can deserialize it directly into an unbound VecSimIndex and
    // bind storage later. Only emit the payload during SST
    // replication saves — regular RDB save does not include disk state.
    const bool storeDiskRdbData = contextFlags & REDISMODULE_CTX_FLAGS_SST_RDB;
    if (storeDiskRdbData) {
      RS_ASSERT(SearchDisk_IsEnabled());
      RS_ASSERT(f->vectorOpts.diskCtx.storage);

      const bool vecSimWithData = f->vectorOpts.vecSimIndex &&
                               VecSimIndex_IndexSize(f->vectorOpts.vecSimIndex) > 0;
      RedisModule_SaveUnsigned(rdb, vecSimWithData ? 1 : 0);
      if (vecSimWithData) {
        bool ok = SearchDisk_SaveVectorIndexToRDB(f->vectorOpts.vecSimIndex, rdb);
        RS_LOG_ASSERT_ALWAYS(ok, "Failed to stream vector index to RDB");
      }
    }
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

static int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, StrongRef sp_ref, int encver, bool useSst) {

  f->ftId = RS_INVALID_FIELD_ID;
  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  char* name = NULL;
  size_t len = 0;
  LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
  if (memchr(name, '\0', len)) {
    rm_free(name);
    goto fail;
  }
  f->fieldName = NewHiddenString(name, len, false);
  f->fieldPath = f->fieldName;
  if (encver >= INDEX_JSON_VERSION) {
    if (LoadUnsigned_IOError(rdb, goto fail) == 1) {
      LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
      if (memchr(name, '\0', len)) {
        rm_free(name);
        goto fail;
      }
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
    // RERANK byte was added in INDEX_VECTOR_RERANK_VERSION for every
    // TIERED+HNSWLIB field. Older RDBs and non-HNSW fields default to TRUE.
    if (encver >= INDEX_VECTOR_RERANK_VERSION &&
        f->vectorOpts.vecSimParams.algo == VecSimAlgo_TIERED &&
        f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
      f->vectorOpts.diskCtx.rerank = LoadUnsigned_IOError(rdb, goto fail) != 0;
    } else {
      f->vectorOpts.diskCtx.rerank = true;
    }
    // Disk-backed vector field's in-memory state rides inline with the
    // field's RDB encoding. We deserialize directly into a freshly-created
    // unbound VecSimIndex. The resulting handle is stored on
    // vectorOpts.vecSimIndex immediately; IndexSpec_SSTRdbOpenAndApply
    // binds storage to it.
    //
    if (useSst) {
      RS_ASSERT(SearchDisk_IsEnabled());
      RS_ASSERT(encver >= INDEX_DISK_VERSION);
      const bool vecSimWithData = LoadUnsigned_IOError(rdb, goto fail) != 0;
      if (vecSimWithData) {
        // Populate diskCtx.indexName early so cleanup uses the disk free
        // path. storage is NULL until IndexSpec_SSTRdbOpenAndApply runs
        // PopulateVectorDiskParams (which frees and reallocates indexName
        // before binding storage). diskCtx.rerank was already set above
        // from the persisted byte.
        size_t nameLen = 0;
        const char *namePtr = HiddenString_GetUnsafe(f->fieldName, &nameLen);
        f->vectorOpts.diskCtx.storage = NULL;
        f->vectorOpts.diskCtx.indexName = rm_strndup(namePtr, nameLen);
        f->vectorOpts.diskCtx.indexNameLen = nameLen;

        VecSimParamsDisk paramsDisk = {
            .indexParams = &f->vectorOpts.vecSimParams,
            .diskContext = &f->vectorOpts.diskCtx,
        };
        f->vectorOpts.vecSimIndex = (VecSimIndex *)SearchDisk_CreateUnboundVectorIndex(&paramsDisk);
        if (!f->vectorOpts.vecSimIndex) {
          RedisModule_Log(RSDummyContext, "warning",
                          "Failed to create unbound vector index for RDB load");
          goto fail;
        }
        if (!SearchDisk_LoadVectorIndexFromRDB(f->vectorOpts.vecSimIndex , rdb)) {
          // vecSimIndex (with diskCtx.indexName set) will be torn down by
          // FieldSpec_Cleanup via the disk free path on the abort flow.
          goto fail;
        }
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

static void IndexScoringStats_RdbLoad(RedisModuleIO *rdb, ScoringIndexStats *stats, int encver) {
  stats->numDocuments = RedisModule_LoadUnsigned(rdb);
  stats->numTerms = RedisModule_LoadUnsigned(rdb);
  if (encver >= INDEX_DISK_VERSION) {
    stats->totalDocsLen = RedisModule_LoadUnsigned(rdb);
  } else {
    stats->totalDocsLen = 0;
  }
}

static void IndexScoringStats_RdbSave(RedisModuleIO *rdb, ScoringIndexStats *stats) {
  RedisModule_SaveUnsigned(rdb, stats->numDocuments);
  RedisModule_SaveUnsigned(rdb, stats->numTerms);
  RedisModule_SaveUnsigned(rdb, stats->totalDocsLen);
}

static void IndexStats_RdbLoad(RedisModuleIO *rdb, IndexStats *stats, int encver) {
  IndexScoringStats_RdbLoad(rdb, &stats->scoring, encver);
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


//---------------------------------------------------------------------------------------------

void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp, bool obfuscate, bool skip_unsafe_ops) {
  const char* indexName = IndexSpec_FormatName(sp, obfuscate);
  RedisModule_InfoAddSection(ctx, indexName);

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
  if (rule->filter_exp_str) {
    const char *filter = HiddenString_GetUnsafe(rule->filter_exp_str, NULL);
    if (obfuscate) {
      RedisModule_InfoAddFieldCString(ctx, "filter", Obfuscate_Text(filter));
    } else {
      RedisModule_InfoAddFieldCString(ctx, "filter", filter);
    }
  }
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
  if (num_prefixes && !skip_unsafe_ops) {
    const char *first_prefix = HiddenUnicodeString_GetUnsafe(rule->prefixes[0], NULL);
    if (first_prefix && first_prefix[0] != '\0') {
      // Skip when unsafe operations should be avoided (e.g., in signal handler) due to memory allocations
      arrayof(char) prefixes = array_new(char, 512);
      for (int i = 0; i < num_prefixes; ++i) {
        const char *prefix = HiddenUnicodeString_GetUnsafe(rule->prefixes[i], NULL);
        const char *prefix_to_use = obfuscate ? Obfuscate_Prefix(prefix) : prefix;
        prefixes = array_ensure_append_1(prefixes, "\"");
        prefixes = array_ensure_append_n(prefixes, prefix_to_use, strlen(prefix_to_use));
        prefixes = array_ensure_append_n(prefixes, "\",", 2);
      }
      prefixes[array_len(prefixes)-1] = '\0';
      RedisModule_InfoAddFieldCString(ctx, "prefixes", prefixes);
      array_free(prefixes);
    }
  }
  RedisModule_InfoEndDictField(ctx);

  // Attributes
  for (int i = 0; i < sp->numFields; i++) {
    const FieldSpec *fs = sp->fields + i;
    char title[28];
    snprintf(title, sizeof(title), "%s_%d", "field", (i+1));
    RedisModule_InfoBeginDictField(ctx, title);

    // if we can't perform allocation then use a local buffer to format the field name
    if (skip_unsafe_ops) {
      char path[MAX_OBFUSCATED_PATH_NAME];
      char name[MAX_OBFUSCATED_FIELD_NAME];
      Obfuscate_FieldPath(fs->index, path);
      Obfuscate_Field(fs->index, name);
      RedisModule_InfoAddFieldCString(ctx, "identifier", path);
      RedisModule_InfoAddFieldCString(ctx, "attribute", name);
    } else {
      const char *path = FieldSpec_FormatPath(fs, obfuscate);
      const char *name = FieldSpec_FormatName(fs, obfuscate);
      RedisModule_InfoAddFieldCString(ctx, "identifier", path);
      RedisModule_InfoAddFieldCString(ctx, "attribute", name);
      rm_free((void*)path);
      rm_free((void*)name);
    }

    if (fs->options & FieldSpec_Dynamic)
      RedisModule_InfoAddFieldCString(ctx, "type", "<DYNAMIC>");
    else
      RedisModule_InfoAddFieldCString(ctx, "type", (char*)FieldSpec_GetTypeNames(INDEXTYPE_TO_POS(fs->types)));

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT))
      RedisModule_InfoAddFieldDouble(ctx,  SPEC_WEIGHT_STR, fs->ftWeight);
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[4];
      snprintf(buf, sizeof(buf), "\"%c\"", fs->tagOpts.tagSep);
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
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_docs", sp->stats.scoring.numDocuments);

  const bool isDisk = sp->diskSpec != NULL;
  size_t num_records = isDisk ? SearchDisk_GetNumRecords(sp->diskSpec) : sp->stats.numRecords;
  size_t inverted_size = isDisk ? SearchDisk_GetInvertedIndexTotalMemory(sp->diskSpec) :
    sp->stats.invertedSize;
  size_t doc_table_size = isDisk ? SearchDisk_GetDocTableTotalMemory(sp->diskSpec) :
    sp->docs.memsize;

  RedisModule_InfoBeginDictField(ctx, "index_properties");
  RedisModule_InfoAddFieldULongLong(ctx, "max_doc_id", sp->docs.maxDocId);
  RedisModule_InfoAddFieldLongLong(ctx, "num_terms", sp->stats.scoring.numTerms);
  RedisModule_InfoAddFieldLongLong(ctx, "num_records", num_records);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_properties_in_mb");
  RedisModule_InfoAddFieldDouble(ctx, "inverted_size", inverted_size / (float)0x100000);
  if (!skip_unsafe_ops) {
    // Skip when unsafe - calls dictFetchValue which can trigger dict rehashing with rm_free
    RedisModule_InfoAddFieldDouble(ctx, "vector_index_size", IndexSpec_VectorIndexesSize(sp) / (float)0x100000);
  }
  RedisModule_InfoAddFieldDouble(ctx, "offset_vectors_size", sp->stats.offsetVecsSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "doc_table_size", doc_table_size / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "sortable_values_size", sp->docs.sortablesSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "key_table_size", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  if (!skip_unsafe_ops) {
    // Skip when unsafe - tag overhead calls dictFetchValue which can trigger dict rehashing with rm_free
    RedisModule_InfoAddFieldDouble(ctx, "tag_overhead_size_mb", IndexSpec_collect_tags_overhead(sp) / (float)0x100000);
    RedisModule_InfoAddFieldDouble(ctx, "text_overhead_size_mb", IndexSpec_collect_text_overhead(sp) / (float)0x100000);
    RedisModule_InfoAddFieldDouble(ctx, "total_index_memory_sz_mb", IndexSpec_TotalMemUsage(sp, 0, 0, 0, 0) / (float)0x100000);
  }
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoAddFieldULongLong(ctx, "total_inverted_index_blocks", sp->stats.totalInvertedIndexBlocks);

  RedisModule_InfoBeginDictField(ctx, "index_properties_averages");
  RedisModule_InfoAddFieldDouble(ctx, "records_per_doc_avg",(float)num_records / (float)sp->stats.scoring.numDocuments);
  double bytes_per_record_avg = num_records ?
    (float)inverted_size / (float)num_records : NAN;
  RedisModule_InfoAddFieldDouble(ctx, "bytes_per_record_avg", bytes_per_record_avg);
  // Disk indexes don't track offset record counts/sizes; report NaN so the
  // metrics aren't misread as meaningful zeros.
  size_t offset_vec_records = isDisk ? 0 : sp->stats.offsetVecRecords;
  size_t offset_vecs_size = isDisk ? 0 : sp->stats.offsetVecsSize;
  double offsets_per_term_avg = (isDisk || !num_records) ? NAN :
    (float)offset_vec_records / (float)num_records;
  double offset_bits_per_record_avg = (isDisk || !offset_vec_records) ? NAN :
    (float)CHAR_BIT * (float)offset_vecs_size / (float)offset_vec_records;
  RedisModule_InfoAddFieldDouble(ctx, "offsets_per_term_avg", offsets_per_term_avg);
  RedisModule_InfoAddFieldDouble(ctx, "offset_bits_per_record_avg", offset_bits_per_record_avg);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_failures");
  RedisModule_InfoAddFieldLongLong(ctx, "hash_indexing_failures", sp->stats.indexError.error_count);
  RedisModule_InfoAddFieldLongLong(ctx, "indexing", !!global_spec_scanner || sp->scan_in_progress);
  RedisModule_InfoEndDictField(ctx);

  // Garbage collector - safe to call, just reads struct fields
  if (sp->gc) {
    GCContext_RenderStatsForInfo(sp->gc, ctx);
  }

  // Cursor stats - safe to call, uses trylock and won't deadlock
  Cursors_RenderStatsForInfo(&g_CursorsList, &g_CursorsListCoord, sp, ctx);

  // Stop words
  if (!skip_unsafe_ops && (sp->flags & Index_HasCustomStopwords)) {
    // Skip when unsafe operations should be avoided - AddStopWordsListToInfo allocates memory
    AddStopWordsListToInfo(ctx, sp->stopwords);
  }
}



///////////////////////////////////////////////////////////////////////////////////////////////


// Populate diskCtx for all HNSW vector fields in the spec.
// This must be called after sp->diskSpec is set.
static void IndexSpec_PopulateVectorDiskParams(IndexSpec *sp) {
  if (!sp->diskSpec) return;

  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = &sp->fields[i];
    if (!FIELD_IS(fs, INDEXFLD_T_VECTOR)) continue;

    // Only HNSW indexes support disk mode (tiered with HNSW primary)
    VecSimParams *params = &fs->vectorOpts.vecSimParams;
    RS_ASSERT(params->algo == VecSimAlgo_TIERED);

    VecSimParams *primaryParams = params->algoParams.tieredParams.primaryIndexParams;
    RS_ASSERT(primaryParams && primaryParams->algo == VecSimAlgo_HNSWLIB);

    size_t nameLen;
    const char *namePtr = HiddenString_GetUnsafe(fs->fieldName, &nameLen);

    // Free any existing indexName to avoid memory leak
    if (fs->vectorOpts.diskCtx.indexName) {
      rm_free((void*)fs->vectorOpts.diskCtx.indexName);
    }

    // Preserve rerank loaded by FieldSpec_RdbLoad — runtime fields below
    // are repopulated from the freshly opened disk handle.
    const bool rerank = fs->vectorOpts.diskCtx.rerank;
    fs->vectorOpts.diskCtx = (VecSimDiskContext){
      .storage = sp->diskSpec,
      .indexName = rm_strndup(namePtr, nameLen),
      .indexNameLen = nameLen,
      .rerank = rerank,
    };
  }
}

// Initialize spec->tagOpts.tagIndex for every TAG field after the disk index
// is opened.
static void IndexSpec_EnsureTagDiskIndexes(IndexSpec *sp) {
  if (!sp->diskSpec) return;

  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = &sp->fields[i];
    if (!FIELD_IS(fs, INDEXFLD_T_TAG)) continue;
    TagIndex_Ensure(fs, sp->diskSpec);
  }
}

// Opens sp->diskSpec from
// the pending RDB state, eagerly materializes each disk-backed VecSimIndex,
// and replays the in-memory blob stashed on the matching FieldSpec.
//
// Returns true on success (sp->diskSpec is set and ready to register).
// Returns false if SearchDisk_OpenIndexWithRdbState fails, or if any
// disk-backed vector field cannot be eagerly created. Note that the
// state (sp->pendingDiskRdbState) is consumed by SearchDisk_OpenIndexWithRdbState
// on both success and failure — we null the pointer here unconditionally.
// On the failure path, sp->diskSpec (if already created) is closed by the
// spec destructor; any unapplied per-field blobs and partially-created
// vecSimIndexes are released by FieldSpec_Cleanup when the spec is freed.
bool IndexSpec_SSTRdbOpenAndApply(RedisModuleCtx *ctx, IndexSpec *sp) {
  // The disk layer consumes pendingDiskRdbState by ownership: max_doc_id and
  // deleted_ids are moved into the spec. Per-field vector in-memory state was
  // already deserialized into an unbound VecSimIndex by FieldSpec_RdbLoad and
  // written directly to fs->vectorOpts.vecSimIndex; the loop below either
  // binds SpeedB storage to those handles or, for fields that were empty at
  // save time, falls back to the eager construction path.
  sp->diskSpec = SearchDisk_OpenIndexWithRdbState(ctx, sp->specName, sp->obfuscatedName, sp->rule->type, sp->pendingDiskRdbState, sp);
  sp->pendingDiskRdbState = NULL;  // consumed regardless of result
  if (!sp->diskSpec) {
    return false;
  }

  // Populate diskCtx for every HNSW-disk-backed vector field so we have the
  // storage context needed for both eager creation (cold fields) and the
  // bind-storage step on pending indexes (loaded inline from RDB).
  IndexSpec_PopulateVectorDiskParams(sp);

  // Materialize each disk-backed VecSimIndex up front so the HNSW graph
  // metadata + SQ8 vectors are available to readers immediately after RDB
  // load completes. Two paths:
  //   - vecSimIndex already set: FieldSpec_RdbLoad deserialized the
  //     in-memory state into an unbound index; bind storage in place.
  //   - vecSimIndex NULL: the save side flagged the VecSim as empty;
  //     fall back to today's eager construction.
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = &sp->fields[i];
    if (!FIELD_IS(fs, INDEXFLD_T_VECTOR)) continue;
    RS_ASSERT(fs->vectorOpts.diskCtx.storage);

    if (fs->vectorOpts.vecSimIndex) {
      VecSimParamsDisk paramsDisk = {
          .indexParams = &fs->vectorOpts.vecSimParams,
          .diskContext = &fs->vectorOpts.diskCtx,
      };
      if (!SearchDisk_BindVectorIndexStorage(ctx, sp->diskSpec,
                                             fs->vectorOpts.vecSimIndex,
                                             &paramsDisk)) {
        RedisModule_Log(RSDummyContext, "warning",
                        "Failed to bind storage to vector index for field %u; aborting spec load",
                        fs->index);
        return false;
      }
    } else if (!openVectorIndex(ctx, fs, CREATE_INDEX)) {
      // openVectorIndex assigns the result to fs->vectorOpts.vecSimIndex; we
      // only need to check for failure here.
      RedisModule_Log(RSDummyContext, "warning",
                      "Failed to eagerly create disk vector index for field %u during RDB load; aborting spec load",
                      fs->index);
      return false;
    }
  }

  // Make sure TagDiskIndex is created for every TAG field. In regular FT.CREATE the TagIndex is ensured lazily
  // in the first document insertion
  IndexSpec_EnsureTagDiskIndexes(sp);
  SearchDisk_RegisterIndex(ctx, sp);

  return true;
}

void IndexSpec_RdbSave(RedisModuleIO *rdb, IndexSpec *sp, int contextFlags) {
  // When saving disk-backed state from the main process, acquire the spec
  // read lock before serializing any field state. FieldSpec_RdbSave
  // serializes the in-memory VecSimIndex of disk-backed vector fields (gated
  // by REDISMODULE_CTX_FLAGS_SST_RDB), and background tiered-index threads
  // can mutate that index concurrently. The same lock also guards the
  // spec-level SST block below (terms trie, scoring stats, disk metadata).
  // In a forked child the memory is a snapshot and no lock is needed.
  const bool storeDiskRdbData = contextFlags & REDISMODULE_CTX_FLAGS_SST_RDB;
  const bool inMainProcess = !(contextFlags & REDISMODULE_CTX_FLAGS_IS_CHILD);
  const bool needLock = sp->diskSpec && storeDiskRdbData && inMainProcess;
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  if (needLock) {
    RedisSearchCtx_LockSpecRead(&sctx);
  }

  // Save the name plus the null terminator
  HiddenString_SaveToRdb(sp->specName, rdb);
  RedisModule_SaveUnsigned(rdb, (uint64_t)sp->flags);
  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec_RdbSave(rdb, &sp->fields[i], contextFlags);
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

  // Disk index
  // Check if we are using SST files with this RDB. If so, we save the disk-related
  // RAM-based data-structures to the RDB. Both save and load paths go through
  // IndexSpecRdbState as the single source of truth for serialization format:
  //
  // We assume symmetry w.r.t this context flag. I.e., If it is not set, we
  // assume it was not set in when the RDB will be loaded as well
  if (sp->diskSpec && storeDiskRdbData) {
    IndexScoringStats_RdbSave(rdb, &sp->stats.scoring);
    TrieType_GenericSave(rdb, sp->terms, false, true);
    SearchDisk_IndexSpecRdbSave(rdb, sp->diskSpec);
  }

  if (needLock) {
    RedisSearchCtx_UnlockSpec(&sctx);
  }
}

static void IndexSpec_NormalizeStorageFlagsOnLoad(IndexFlags *flags) {
  if ((*flags & Index_WideSchema) && !(*flags & Index_StoreFieldFlags)) {
    *flags &= ~Index_WideSchema;
    RedisModule_Log(RSDummyContext, "warning", "Ignoring %s because %s is set",
                    SPEC_SCHEMA_EXPANDABLE_STR, SPEC_NOFIELDS_STR);
  }
}

IndexSpec *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status) {
  IndexSpec *sp = NULL;
  StrongRef spec_ref = {0};
  IndexFlags flags = 0;
  int16_t numFields = 0;
  uint64_t numFields_u64 = 0;
  size_t narr = 0;
  char *rawName = NULL;
  size_t len = 0;
  HiddenString* specName = NULL;
  RedisModuleCtx* ctx = RedisModule_GetContextFromIO(rdb);

  rawName = LoadStringBuffer_IOError(rdb, NULL, goto cleanup_no_index);
  len = strlen(rawName);
  specName = NewHiddenString(rawName, len, true);
  RedisModule_Free(rawName);

  sp = rm_calloc(1, sizeof(IndexSpec));
  spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;

  // Note: indexError, fieldIdToIndex, docs, specName, obfuscatedName, terms, and monitor flags are already initialized in initializeIndexSpec
  flags = (IndexFlags)LoadUnsigned_IOError(rdb, goto cleanup);
  // Note: monitorDocumentExpiration and monitorFieldExpiration are already set in initializeIndexSpec
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    flags |= Index_StoreFreqs;
  }
  IndexSpec_NormalizeStorageFlagsOnLoad(&flags);
  numFields_u64 = LoadUnsigned_IOError(rdb, goto cleanup);

  if (unlikely(numFields_u64 > SPEC_MAX_FIELDS)) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
                           "RDB Load: Schema is limited to %d fields",
                           SPEC_MAX_FIELDS);
    goto cleanup;
  }

  initializeIndexSpec(sp, specName, flags, numFields_u64);

  sp->isDuplicate = dictFetchValue(specDict_g, sp->specName) != NULL;

  IndexSpec_MakeKeyless(sp);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FieldSpec_RdbLoad(rdb, fs, spec_ref, encver, useSst) != REDISMODULE_OK) {
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

  narr = LoadUnsigned_IOError(rdb, goto cleanup);
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

  if (isSpecOnDisk(sp) && useSst) {
    // Load the disk-related index data if we are on disk and the save flow used
    // sst-files. We load it into a temporary in-memory object first, then use it
    // to open the index with the RDB state applied.
    // We must always consume the RDB data to avoid corrupting the stream,
    // even for duplicates. We just won't use it in the duplicate case.
    RS_ASSERT(encver >= INDEX_DISK_VERSION);
    RS_ASSERT(disk_db);
    IndexScoringStats_RdbLoad(rdb, &sp->stats.scoring, encver);
    if (sp->terms) {
      TrieType_Free(sp->terms);
    }
    sp->terms = TrieType_GenericLoad(rdb, false, true, Trie_Sort_Lex);
    RS_LOG_ASSERT(sp->terms, "Failed to load terms trie");

    // Load disk metadata (max_doc_id, deleted_ids) into the spec. Stashed
    // here directly; consumed at LOADING_ENDED (SST flow) or freed by
    // the spec destructor (duplicate / cleanup / abort).
    sp->pendingDiskRdbState = SearchDisk_LoadRdbToTempObject(rdb);
    if (!sp->pendingDiskRdbState) {
      goto cleanup;
    }
  } else if (isSpecOnDisk(sp) && !sp->isDuplicate) {
    // If the regular RDB method is used, just open an Index without any populated data.
    RS_ASSERT(!useSst);
    sp->diskSpec = SearchDisk_OpenIndex(ctx, sp->specName, sp->obfuscatedName, sp->rule->type, false, sp);
    if (!sp->diskSpec) {
      goto cleanup;
    }
    IndexSpec_PopulateVectorDiskParams(sp);
    IndexSpec_EnsureTagDiskIndexes(sp);
    SearchDisk_RegisterIndex(ctx, sp);
  }

  return sp;

cleanup:
  if (sp && sp->diskSpec) {
    // Idempotent — no-op if registration never happened on this path.
    SearchDisk_UnregisterIndex(ctx, sp);
  }
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

  Cursors_initSpec(sp);

  // setting isDuplicate to true will make sure index will not be removed from aliases container.
  // It may have already been set.
  if (!sp->isDuplicate && dictFetchValue(specDict_g, sp->specName) != NULL) {
    sp->isDuplicate = true;
  }

  if (sp->isDuplicate) {
    // spec already exists, however we need to finish consuming the rdb so redis won't issue an error(expecting an eof but seeing remaining data)
    // right now this can cause nasty side effects, to avoid them we will set isDuplicate to true
    RedisModule_Log(RSDummyContext, "notice", "Loading an already existing index, will just ignore.");

    // spec already exists lets just free this one
    // Remove the new spec from the global prefixes dictionary.
    // This is the only global structure that we added the new spec to at this point
    SchemaPrefixes_RemoveSpec(spec_ref);
    addPendingIndexDrop();
    StrongRef_Release(spec_ref);
  } else {
    // In the SST replication path diskSpec is still NULL here — it's opened
    // later by Indexes_FinishSSTReplication, which also starts the Disk GC.
    // Start GC eagerly only when the spec is fully ready: memory mode, or a
    // disk spec whose diskSpec was opened during IndexSpec_RdbLoad (non-SST
    // RDB path).
    if (!SearchDisk_IsEnabled()) {
      IndexSpec_StartGC(spec_ref, sp, GCPolicy_Fork);
    } else if (sp->diskSpec) {
      RS_ASSERT(!IS_SST_RDB_IN_PROCESS(RSDummyContext));
      IndexSpec_StartGC(spec_ref, sp, GCPolicy_Disk);
    }
    dictAdd(specDict_g, (void*)sp->specName, spec_ref.rm);
    dictAdd(specIdDict_g, (void*)(uintptr_t)sp->specId, spec_ref.rm);

    for (int i = 0; i < sp->numFields; i++) {
      FieldsGlobalStats_UpdateStats(sp->fields + i, 1);
    }
  }
  return REDISMODULE_OK;
}

int IndexSpec_CreateFromRdb(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status) {
  // Load the index spec using the new function
  IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, useSst, status);
  return IndexSpec_StoreAfterRdbLoad(sp);
}

void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < LEGACY_INDEX_MIN_VERSION || encver > LEGACY_INDEX_MAX_VERSION) {
    return NULL;
  }
  RS_LOG_ASSERT(!SearchDisk_IsEnabled(), "Legacy indexes are not supported on disk");
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
  IndexSpec_NormalizeStorageFlagsOnLoad(&sp->flags);

  uint64_t numFields_u64 = RedisModule_LoadUnsigned(rdb);

  if (unlikely(numFields_u64 > SPEC_MAX_FIELDS)) {
    RedisModule_LogIOError(
        rdb, "warning", "RDB Load: Schema is limited to %d fields",
        SPEC_MAX_FIELDS);
    StrongRef_Release(spec_ref);
    return NULL;
  }

  sp->numFields = (uint16_t)numFields_u64;
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    initializeFieldSpec(fs, i);
    if (FieldSpec_RdbLoad(rdb, fs, spec_ref, encver, false) != REDISMODULE_OK) {
      StrongRef_Release(spec_ref);
      return NULL;
    }
    if (FieldSpec_IsSortable(fs)) {
      sp->numSortableFields++;
    }
  }
  // After loading all the fields, we can build the spec cache
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  IndexStats_RdbLoad(rdb, &sp->stats, encver);

  if (DocTable_LegacyRdbLoad(&sp->docs, rdb, encver) != REDISMODULE_OK) {
    StrongRef_Release(spec_ref);
    return NULL;
  }
  /* For version 3 or up - load the generic trie */
  if (encver >= 3) {
    sp->terms = TrieType_GenericLoad(rdb, false, false, Trie_Sort_Lex);
    if (sp->terms == NULL) {
      StrongRef_Release(spec_ref);
      return NULL;
    }
  } else {
    sp->terms = NewTrie(NULL, Trie_Sort_Lex);
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
    if (sp->stopwords == NULL) {
      StrongRef_Release(spec_ref);
      return NULL;
    }
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->smap = NULL;
  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
    if (sp->smap == NULL) {
      StrongRef_Release(spec_ref);
      return NULL;
    }
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

  dictDelete(legacySpecRules, sp->specName);
  SchemaRuleArgs_Free(rule_args);

  if (!sp->rule) {
    RedisModule_LogIOError(rdb, "warning", "Failed creating rule for legacy index '%s', error='%s'",
                           formattedIndexName, QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
    StrongRef_Release(spec_ref);
    return NULL;
  }

  IndexSpec_StartGC(spec_ref, sp, GCPolicy_Fork);
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





void *IndexSpec_RdbLoad_Logic(RedisModuleIO *rdb, int encver) {
  const bool useSst = CheckRdbSstPersistence(RedisModule_GetContextFromIO(rdb), "RDB Load Logic");
  if (encver <= LEGACY_INDEX_MAX_VERSION) {
    // Legacy index, loaded in order to upgrade from an old version
    return IndexSpec_LegacyRdbLoad(rdb, encver);
  } else {
    // New index, loaded normally.
    // Even though we don't actually load or save the index spec in the key space, this implementation is useful
    // because it allows us to serialize and deserialize the index spec in a clean way.
    // Required to support loading during ASM migration.
    RS_ASSERT(encver >= INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION);
    if (encver < INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: Unexpected encver %d found in RDB_Load, encver not expected to be lower than %d", encver, INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION);
      return NULL;
    }
    QueryError status = QueryError_Default();
    IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, useSst, &status);
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


static void IndexSpec_RdbSave_Wrapper(RedisModuleIO *rdb, void *value) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  const int contextFlags = RedisModule_GetContextFlags(ctx);
  IndexSpec_RdbSave(rdb, value, contextFlags);
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

// Shared helper: update stats and clean up auxiliary indexes after a document deletion.
// Caller must hold the spec write lock.
static void indexSpec_OnDocDeleted(IndexSpec *spec, t_docId docId, uint32_t docLen) {
  // Update the stats
  RS_LOG_ASSERT(spec->stats.scoring.totalDocsLen >= docLen, "totalDocsLen is smaller than docLen");
  spec->stats.scoring.totalDocsLen -= docLen;
  RS_LOG_ASSERT(spec->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
  spec->stats.scoring.numDocuments--;

  // Increment the index's garbage collector's scanning frequency after document deletions
  if (spec->gc) {
    GCContext_OnDelete(spec->gc);
  }

  // VecSim fields clear deleted data on the fly
  if (spec->flags & Index_HasVecSim) {
    for (int i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
        VecSimIndex *vecsim = openVectorIndex(NULL, spec->fields + i, DONT_CREATE_INDEX);
        if(!vecsim) continue;
        VecSimIndex_DeleteVector(vecsim, docId);
      }
    }
  }

  if (spec->flags & Index_HasGeometry) {
    GeometryIndex_RemoveId(spec, docId);
  }
}

void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  t_docId id = 0;
  uint32_t docLen = 0;
  if (SearchDisk_IsEnabled()) {
    RS_LOG_ASSERT(spec->diskSpec, "disk handle is unexpectedly NULL");

    // Look up docId from key metadata
    uint64_t docId = 0;
    if (DocIdMeta_Get(ctx, key, spec->specId, &docId) != REDISMODULE_OK || docId == 0) {
      // Nothing to delete
      return;
    }
    id = docId;

    // Delete the document by docId
    if (!SearchDisk_DeleteDocumentById(spec->diskSpec, id, &docLen)) {
      // Failed to delete
      return;
    }

    // Drop the key→docId mapping now that the document is gone from disk. This
    // keeps DocIdMeta authoritative as an "is this key indexed?" oracle: an
    // entry exists iff the document is currently indexed in this spec. Without
    // this, a key that survives in the keyspace but is de-indexed (e.g. an HSET
    // that makes it stop passing the index FILTER) would keep a stale mapping
    // pointing at an already-deleted docId.
    DocIdMeta_Delete(ctx, key, spec->specId);
  } else {
    RSDocumentMetadata *md = DocTable_PopR(&spec->docs, key);
    if (!md) {
      // Nothing to delete
      return;
    }

    id = md->id;
    docLen = md->docLen;

    DMD_Return(md);
  }

  indexSpec_OnDocDeleted(spec, id, docLen);
}

int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  IndexSpec_IncrActiveWrites(spec);
  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_DeleteDoc_Unsafe(spec, ctx, key);
  IndexSpec_DecrActiveWrites(spec);
  RedisSearchCtx_UnlockSpec(&sctx);

  return REDISMODULE_OK;
}

void IndexSpec_DeleteDocById(IndexSpec *spec, t_docId docId) {
  RS_ASSERT(isSpecOnDisk(spec));
  RS_LOG_ASSERT(spec->diskSpec, "disk handle is unexpectedly NULL");
  // Acquire the write lock
  IndexSpec_IncrActiveWrites(spec);
  pthread_rwlock_wrlock(&spec->rwlock);

  uint32_t docLen = 0;

  if (!SearchDisk_DeleteDocumentById(spec->diskSpec, docId, &docLen)) {
    // Document not found on disk
    IndexSpec_DecrActiveWrites(spec);
    pthread_rwlock_unlock(&spec->rwlock);
    return;
  }

  indexSpec_OnDocDeleted(spec, docId, docLen);

  IndexSpec_DecrActiveWrites(spec);
  pthread_rwlock_unlock(&spec->rwlock);
}

///////////////////////////////////////////////////////////////////////////////////////////////













///////////////////////////////////////////////////////////////////////////////////////////////


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







// =============================================================================
// Compaction FFI Functions (called by Rust during GC)
// =============================================================================

// Acquire IndexSpec write lock
void IndexSpec_AcquireWriteLock(IndexSpec* sp) {
  pthread_rwlock_wrlock(&sp->rwlock);
}

// Release IndexSpec write lock
void IndexSpec_ReleaseWriteLock(IndexSpec* sp) {
  pthread_rwlock_unlock(&sp->rwlock);
}


// Update a term's document count in the Serving Trie
// Note: term is NOT null-terminated; term_len specifies the length
// Returns true if the term was completely emptied and deleted from the trie.
bool IndexSpec_DecrementTrieTermCount(IndexSpec* sp, const char* term, size_t term_len,
                                  size_t doc_count_decrement) {
  if (!sp->terms || doc_count_decrement == 0) {
    return false;
  }
  // Decrement the numDocs count for this term in the trie
  // If numDocs reaches 0, the node will be deleted
  TrieDecrResult result = Trie_DecrementNumDocs(sp->terms, term, term_len, doc_count_decrement);
  RS_ASSERT(result != TRIE_DECR_NOT_FOUND);
  return result == TRIE_DECR_DELETED;
}

// Update IndexScoringStats based on compaction delta
// Note: num_docs and totalDocsLen are updated at delete time, NOT by GC.
// GC only updates numTerms (when terms become completely empty).
void IndexSpec_DecrementNumTerms(IndexSpec* sp, uint64_t num_terms_removed) {
  if (num_terms_removed == 0) {
    return;
  }

  RS_ASSERT(num_terms_removed <= sp->stats.scoring.numTerms);
  sp->stats.scoring.numTerms -= num_terms_removed;
}
