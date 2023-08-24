/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "spec.h"

#include <math.h>
#include <ctype.h>

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
#include "rmutil/cxx/chrono-clock.h"
#include "util/workers.h"

#define INITIAL_DOC_TABLE_SIZE 1000

///////////////////////////////////////////////////////////////////////////////////////////////


const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;
static uint64_t spec_unique_ids = 1;

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
bool isTrimming = false;

// Default values make no limits.
size_t memoryLimit = -1;
size_t used_memory = 0;

static redisearch_threadpool cleanPool = NULL;

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

static void Cursors_initSpec(IndexSpec *spec, size_t capacity) {
  spec->activeCursors = 0;
  spec->cursorsCap = capacity;
}

/*
 * Get a field spec by field name. Case sensetive!
 * Return the field spec if found, NULL if not.
 * Assuming the spec is properly locked before calling this function.
 */
const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const char *name, size_t len) {
  for (size_t i = 0; i < spec->numFields; i++) {
    if (len != strlen(spec->fields[i].name)) {
      continue;
    }
    const FieldSpec *fs = spec->fields + i;
    if (!strncmp(fs->name, name, len)) {
      return fs;
    }
  }
  return NULL;
}

// Assuming the spec is properly locked before calling this function.
t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  const FieldSpec *fs = IndexSpec_GetField(spec, name, len);
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
        QueryError_SetErrorFmt(status, QUERY_EBADORDEROPTION,
          "slop/inorder are not supported for field `%s` since it has undefined ordering", fs->name);
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
      return sp->fields[i].name;
    }
  }
  return NULL;
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
StrongRef IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, QueryError *status) {

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  return IndexSpec_Parse(RedisModule_StringPtrLen(name, NULL), args, argc, status);
}

arrayof(FieldSpec *) getFieldsByType(IndexSpec *spec, FieldType type) {
#define FIELDS_ARRAY_CAP 2
  arrayof(FieldSpec *) fields = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (FIELD_IS(spec->fields + i, type)) {
      fields = array_append(fields, &(spec->fields[i]));
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

// called on master shard for temporary indexes and deletes all documents by defaults
static void IndexSpec_FreeTask(char *specName) {
#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Freeing index %s in background", specName);
#endif
  RedisModule_ThreadSafeContextLock(RSDummyContext);

  // pass FT.DROPINDEX with "DD" flag to slef.
  RedisModuleCallReply *rep = RedisModule_Call(RSDummyContext, RS_DROP_INDEX_CMD, "cc!", specName, "DD");
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  RedisModule_ThreadSafeContextUnlock(RSDummyContext);

  rm_free(specName);
}

void IndexSpec_LegacyFree(void *spec) {
  // free legacy index do nothing, it will be called only
  // when the index key will be deleted and we keep the legacy
  // index pointer in the legacySpecDict so we will free it when needed
}

static void IndexSpec_TimedOut_Free(IndexSpec *spec) {
  if (RS_IsMock) {
    IndexSpec_Free(spec);
    return;
  }
  if (spec->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, spec->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
    spec->isTimerSet = false;
  }
  redisearch_thpool_add_work(cleanPool, (redisearch_thpool_proc)IndexSpec_FreeTask, rm_strdup(spec->name), THPOOL_PRIORITY_HIGH);
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
#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Freeing index %s by timer", sp->name);
#endif

  sp->isTimerSet = false;
  // This function will perform an index drop, and we will still have to return our references
  IndexSpec_TimedOut_Free(sp);

  StrongRef_Release(spec_ref);

#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Freeing index by timer: done");
#endif
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

double IndexesScanner_IndexedPercent(IndexesScanner *scanner, IndexSpec *sp) {
  if (scanner || sp->scan_in_progress) {
    if (scanner) {
      return scanner->totalKeys > 0 ? (double)scanner->scannedKeys / scanner->totalKeys : 0;
    } else {
      return 0;
    }
  } else {
    return 1.0;
  }
}

//---------------------------------------------------------------------------------------------

/* Create a new index spec from a redis command */
// TODO: multithreaded: use global metadata locks to protect global data structures
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status) {
  const char *specName = RedisModule_StringPtrLen(argv[1], NULL);
  setMemoryInfo(ctx);
  if (dictFetchValue(specDict_g, specName)) {
    QueryError_SetCode(status, QUERY_EINDEXEXISTS);
    return NULL;
  }
  StrongRef spec_ref = IndexSpec_ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, status);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (sp == NULL) {
    return NULL;
  }

  // Sets weak and strong references to the spec, then pass it to the spec dictionary

  dictAdd(specDict_g, (char *)specName, spec_ref.rm);

  sp->uniqueId = spec_unique_ids++;
  // Start the garbage collector
  IndexSpec_StartGC(ctx, spec_ref, sp);

  Cursors_initSpec(sp, RSCURSORS_DEFAULT_CAPACITY);

  // Create the indexer
  sp->indexer = NewIndexer(sp);

  // set timeout for temporary index on master
  if ((sp->flags & Index_Temporary) && IsMaster()) {
    IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref));
  }

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

static int parseTextField(FieldSpec *fs, ArgsCursor *ac, QueryError *status,
                          DelimiterList *indexDelimiters) {
  int rc;
  size_t len;
  // this is a text field
  // init default weight and type
  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_NOSTEM_STR)) {
      fs->options |= FieldSpec_NoStemming;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = AC_GetDouble(ac, &d, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "weight", rc);
        return 0;
      }
      fs->ftWeight = d;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_PHONETIC_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_PHONETIC_STR " requires an argument");
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
            status, QUERY_EINVAL,
            "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
            "double metaphone (dm). Supported languages: English (en), French (fr), "
            "Portuguese (pt) and Spanish (es)");
        return 0;
      }
      fs->options |= FieldSpec_Phonetics;
      continue;
    } else if(AC_AdvanceIfMatch(ac, SPEC_WITHSUFFIXTRIE_STR)) {
      fs->options |= FieldSpec_WithSuffixTrie;
    } else if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(fs);
      if (AC_AdvanceIfMatch(ac, SPEC_UNF_STR)) {      // Explicitly requested UNF
          fs->options |= FieldSpec_UNF;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      fs->options |= FieldSpec_NotIndexable;
    } else if (AC_AdvanceIfMatch(ac, SPEC_SET_DELIMITERS_STR)) {
      const char *separatorStr;
      if ((rc = AC_GetString(ac, &separatorStr, &len, 0)) != AC_OK) {
        return 0;
      }

      if(separatorStr != NULL) {
        if(fs->delimiters) {
          DelimiterList_Unref(fs->delimiters);
        }
        fs->delimiters = NewDelimiterListCStr(separatorStr);
        fs->options |= FieldSpec_WithCustomDelimiters;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_ADD_DELIMITERS_STR)) {
      const char *addSeparatorStr;
      if ((rc = AC_GetString(ac, &addSeparatorStr, &len, 0)) != AC_OK) {
        return 0;
      }

      if(addSeparatorStr != NULL) {
        if(fs->delimiters == NULL && indexDelimiters != NULL) {
          fs->delimiters = NewDelimiterListCStr(indexDelimiters->delimiterString);
        }
        fs->delimiters = AddDelimiterListCStr(addSeparatorStr, fs->delimiters);
        fs->options |= FieldSpec_WithCustomDelimiters;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_DEL_DELIMITERS_STR)) {
      const char *delSeparatorStr;
      if ((rc = AC_GetString(ac, &delSeparatorStr, &len, 0)) != AC_OK) {
        return 0;
      }

      if(delSeparatorStr != NULL) {
        if(fs->delimiters == NULL && indexDelimiters != NULL) {
          fs->delimiters = NewDelimiterListCStr(indexDelimiters->delimiterString);
        }
        fs->delimiters = RemoveDelimiterListCStr(delSeparatorStr, fs->delimiters);
        fs->options |= FieldSpec_WithCustomDelimiters;
      }
    } else {
      break;
    }
  }
  return 1;
}

static int parseTagField(FieldSpec *fs, ArgsCursor *ac, QueryError *status,
                          DelimiterList *indexDelimiters) {
  int rc;
  size_t len;
  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_TAG_SEPARATOR_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_TAG_SEPARATOR_STR " requires an argument");
        return 0;
      }
      const char *sep = AC_GetStringNC(ac, NULL);
      if (strlen(sep) != 1) {
        QueryError_SetErrorFmt(status, QUERY_EPARSEARGS,
                              "Tag separator must be a single character. Got `%s`", sep);
        return 0;
      }
      fs->tagOpts.tagSep = *sep;
    } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_CASE_SENSITIVE_STR)) {
      fs->tagOpts.tagFlags |= TagField_CaseSensitive;
    } else if (AC_AdvanceIfMatch(ac, SPEC_WITHSUFFIXTRIE_STR)) {
      fs->options |= FieldSpec_WithSuffixTrie;
    } else if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(fs);
      if (AC_AdvanceIfMatch(ac, SPEC_UNF_STR) ||      // Explicitly requested UNF
          TAG_FIELD_IS(fs, TagField_CaseSensitive)) { // We don't normalize case sensitive tags. Implicit UNF
        fs->options |= FieldSpec_UNF;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      fs->options |= FieldSpec_NotIndexable;
    } else if (AC_AdvanceIfMatch(ac, SPEC_SET_DELIMITERS_STR)) {
      const char *separatorStr;
      if ((rc = AC_GetString(ac, &separatorStr, &len, 0)) != AC_OK) {
        return 0;
      }

      if(separatorStr != NULL) {
        if(fs->delimiters) {
          DelimiterList_Unref(fs->delimiters);
        }
        fs->delimiters = NewDelimiterListCStr(separatorStr);
        fs->options |= FieldSpec_WithCustomDelimiters;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_ADD_DELIMITERS_STR)) {
      const char *addSeparatorStr;
      if ((rc = AC_GetString(ac, &addSeparatorStr, &len, 0)) != AC_OK) {
        return 0;
      }

      if(addSeparatorStr != NULL) {
        if(fs->delimiters == NULL && indexDelimiters != NULL) {
          fs->delimiters = NewDelimiterListCStr(indexDelimiters->delimiterString);
        }
        fs->delimiters = AddDelimiterListCStr(addSeparatorStr, fs->delimiters);
        fs->options |= FieldSpec_WithCustomDelimiters;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_DEL_DELIMITERS_STR)) {
      const char *delSeparatorStr;
      if ((rc = AC_GetString(ac, &delSeparatorStr, &len, 0)) != AC_OK) {
        return 0;
      }

      if(delSeparatorStr != NULL) {
        if(fs->delimiters == NULL && indexDelimiters != NULL) {
          fs->delimiters = NewDelimiterListCStr(indexDelimiters->delimiterString);
        }
        fs->delimiters = RemoveDelimiterListCStr(delSeparatorStr, fs->delimiters);
        fs->options |= FieldSpec_WithCustomDelimiters;
      }
    } else {
      break;
    }
  }
  return 1;
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
  if (!strncasecmp(VECSIM_TYPE_FLOAT32, typeStr, len))
    *type = VecSimType_FLOAT32;
  else if (!strncasecmp(VECSIM_TYPE_FLOAT64, typeStr, len))
    *type = VecSimType_FLOAT64;
  // else if (!strncasecmp(VECSIM_TYPE_INT32, typeStr, len))
  //   *type = VecSimType_INT32;
  // else if (!strncasecmp(VECSIM_TYPE_INT64, typeStr, len))
  //   *type = VecSimType_INT64;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// Tries to get distance metric from ac. This function need to stay updated with
// the supported distance metric functions list of VecSim.
static int parseVectorField_GetMetric(ArgsCursor *ac, VecSimMetric *metric) {
  const char *metricStr;
  size_t len;
  int rc;
  if ((rc = AC_GetString(ac, &metricStr, &len, 0)) != AC_OK) {
    return rc;
  }
  if (!strncasecmp(VECSIM_METRIC_IP, metricStr, len))
    *metric = VecSimMetric_IP;
  else if (!strncasecmp(VECSIM_METRIC_L2, metricStr, len))
    *metric = VecSimMetric_L2;
  else if (!strncasecmp(VECSIM_METRIC_COSINE, metricStr, len))
    *metric = VecSimMetric_Cosine;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// memoryLimit / 10 - default is 10% of global memory limit
#define BLOCK_MEMORY_LIMIT ((RSGlobalConfig.vssMaxResize) ? RSGlobalConfig.vssMaxResize : memoryLimit / 10)

static int parseVectorField_validate_hnsw(VecSimParams *params, QueryError *status) {
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / VecSimIndex_EstimateElementSize(params);
  // if Block size was not set by user, sets the default to min(maxBlockSize, DEFAULT_BLOCK_SIZE)
  if (params->algoParams.hnswParams.blockSize == 0) { // indicates that block size was not set by the user
    params->algoParams.hnswParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);
  }
  if (params->algoParams.hnswParams.initialCapacity == SIZE_MAX) { // indicates that initial capacity was not set by the user
    params->algoParams.hnswParams.initialCapacity = params->algoParams.hnswParams.blockSize;
  }
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  size_t free_memory = memoryLimit - used_memory;
  if (params->algoParams.hnswParams.initialCapacity > maxBlockSize) {
    QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Vector index initial capacity %zu exceeded server limit (%zu with the given parameters)", params->algoParams.hnswParams.initialCapacity, maxBlockSize);
    return 0;
  }
  if (params->algoParams.hnswParams.blockSize > maxBlockSize) {
    // TODO: uncomment when BLOCK_SIZE is added to FT.CREATE on HNSW
    // QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Vector index block size %zu exceeded server limit (%zu with the given parameters)", fs->vectorOpts.vecSimParams.bfParams.blockSize, maxBlockSize);
    // return 0;
  }
  RedisModule_Log(RSDummyContext, "warning", "creating vector index. Server memory limit: %zuB, required memory: %zuB, available memory: %zuB", memoryLimit, index_size_estimation, free_memory);
  return 1;
}

static int parseVectorField_validate_flat(VecSimParams *params, QueryError *status) {
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  // if Block size was not set by user, sets the default to min(maxBlockSize, DEFAULT_BLOCK_SIZE)
  if (params->algoParams.bfParams.blockSize == 0) { // indicates that block size was not set by the user
    params->algoParams.bfParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);
  }
  if (params->algoParams.bfParams.initialCapacity == SIZE_MAX) { // indicates that initial capacity was not set by the user
    params->algoParams.bfParams.initialCapacity = params->algoParams.bfParams.blockSize;
  }
  // Calculating index size estimation, after first vector block was allocated.
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.bfParams.blockSize;
  size_t free_memory = memoryLimit - used_memory;
  if (params->algoParams.bfParams.initialCapacity > maxBlockSize) {
    QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Vector index initial capacity %zu exceeded server limit (%zu with the given parameters)", params->algoParams.bfParams.initialCapacity, maxBlockSize);
    return 0;
  }
  if (params->algoParams.bfParams.blockSize > maxBlockSize) {
    QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Vector index block size %zu exceeded server limit (%zu with the given parameters)", params->algoParams.bfParams.blockSize, maxBlockSize);
    return 0;
  }
  RedisModule_Log(RSDummyContext, "warning", "creating vector index. Server memory limit: %zuB, required memory: %zuB, available memory: %zuB", memoryLimit, index_size_estimation, free_memory);
  return 1;
}

int VecSimIndex_validate_params(RedisModuleCtx *ctx, VecSimParams *params, QueryError *status) {
  setMemoryInfo(ctx);
  bool valid = false;
  if (VecSimAlgo_HNSWLIB == params->algo) {
    valid = parseVectorField_validate_hnsw(params, status);
  } else if (VecSimAlgo_BF == params->algo) {
    valid = parseVectorField_validate_flat(params, status);
  } else if (VecSimAlgo_TIERED == params->algo) {
    return VecSimIndex_validate_params(ctx, params->algoParams.tieredParams.primaryIndexParams, status);
  }
  return valid ? REDISMODULE_OK : REDISMODULE_ERR;
}

static int parseVectorField_hnsw(FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status) {
  int rc;

  // HNSW mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;

  // Get number of parameters
  size_t expNumParam, numParam = 0;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "vector similarity number of parameters", rc);
    return 0;
  } else if (expNumParam % 2) {
    QERR_MKBADARGS_FMT(status, "Bad number of arguments for vector similarity index: got %d but expected even number (as algorithm parameters should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.hnswParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index type", rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index dim", rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.hnswParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index metric", rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index initial cap", rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_M)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.M, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index m", rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EFCONSTRUCTION)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.efConstruction, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index efConstruction", rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EFRUNTIME)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.hnswParams.efRuntime, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index efRuntime", rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EPSILON)) {
      if ((rc = AC_GetDouble(ac, &params->algoParams.hnswParams.epsilon, AC_F_GE0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity HNSW index epsilon", rc);
        return 0;
      }
    } else {
      QERR_MKBADARGS_FMT(status, "Bad arguments for algorithm %s: %s", VECSIM_ALGORITHM_HNSW, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QERR_MKBADARGS_FMT(status, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
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
    QERR_MKBADARGS_AC(status, "vector similarity number of parameters", rc);
    return 0;
  } else if (expNumParam % 2) {
    QERR_MKBADARGS_FMT(status, "Bad number of arguments for vector similarity index: got %d but expected even number as algorithm parameters (should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.bfParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity FLAT index type", rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity FLAT index dim", rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.bfParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity FLAT index metric", rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity FLAT index initial cap", rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_BLOCKSIZE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.blockSize, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "vector similarity FLAT index blocksize", rc);
        return 0;
      }
    } else {
      QERR_MKBADARGS_FMT(status, "Bad arguments for algorithm %s: %s", VECSIM_ALGORITHM_BF, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QERR_MKBADARGS_FMT(status, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
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

static int parseVectorField(IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  // this is a vector field
  // init default type, size, distance metric and algorithm

  memset(&fs->vectorOpts.vecSimParams, 0, sizeof(VecSimParams));

  // If the index is on JSON and the given path is dynamic, create a multi-value index.
  bool multi = false;
  if (isSpecJson(sp)) {
    RedisModuleString *err_msg;
    JSONPath jsonPath = pathParse(fs->path, &err_msg);
    if (!jsonPath) {
      if (err_msg) {
        JSONParse_error(status, err_msg, fs->path, fs->name, sp->name);
      }
      return 0;
    }
    multi = !(pathIsSingle(jsonPath));
    pathFree(jsonPath);
  }

  // parse algorithm
  const char *algStr;
  size_t len;
  int rc;
  if ((rc = AC_GetString(ac, &algStr, &len, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "vector similarity algorithm", rc);
    return 0;
  }
  VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
  logCtx->index_field_name = fs->name;
  fs->vectorOpts.vecSimParams.logCtx = logCtx;

  if (!strncasecmp(VECSIM_ALGORITHM_BF, algStr, len)) {
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_BF;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.initialCapacity = SIZE_MAX;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.blockSize = 0;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.multi = multi;
    return parseVectorField_flat(fs, &fs->vectorOpts.vecSimParams, ac, status);
  } else if (!strncasecmp(VECSIM_ALGORITHM_HNSW, algStr, len)) {
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

    return parseVectorField_hnsw(fs, params, ac, status);
  } else {
    QERR_MKBADARGS_AC(status, "vector similarity algorithm", AC_ERR_ENOENT);
    return 0;
  }
}

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static int parseFieldSpec(ArgsCursor *ac, IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Field `%s` does not have a type", fs->name);
    return 0;
  }

  if (AC_AdvanceIfMatch(ac, SPEC_TEXT_STR)) {  // text field
    fs->types |= INDEXFLD_T_FULLTEXT;
    if (!parseTextField(fs, ac, status, sp->delimiters)) {
      goto error;
    }
    return 1;
  } else if (AC_AdvanceIfMatch(ac, SPEC_NUMERIC_STR)) {  // numeric field
    fs->types |= INDEXFLD_T_NUMERIC;
  } else if (AC_AdvanceIfMatch(ac, SPEC_GEO_STR)) {  // geo field
    fs->types |= INDEXFLD_T_GEO;
  } else if (AC_AdvanceIfMatch(ac, SPEC_VECTOR_STR)) {  // vector field
    sp->flags |= Index_HasVecSim;
    fs->types |= INDEXFLD_T_VECTOR;
    if (!parseVectorField(sp, sp_ref, fs, ac, status)) {
      goto error;
    }
    return 1;
  } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_STR)) {  // tag field
    fs->types |= INDEXFLD_T_TAG;
    if (!parseTagField(fs, ac, status, sp->delimiters)) {
      goto error;
    }
    return 1;
  } else if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_STR)) {  // geometry field
    sp->flags |= Index_HasGeometry;
    fs->types |= INDEXFLD_T_GEOMETRY;
    if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_FLAT_STR)) {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Cartesian;
    } else if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_SPHERE_STR)) {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Geographic;
    } else {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Geographic;
    }
  } else {  // nothing more supported currently
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid field type for field `%s`", fs->name);
    goto error;
  }

  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      if(FIELD_IS(fs, INDEXFLD_T_NUMERIC) || FIELD_IS(fs, INDEXFLD_T_GEO)) {
        FieldSpec_SetSortable(fs);
        if (AC_AdvanceIfMatch(ac, SPEC_UNF_STR) ||      // Explicitly requested UNF
            FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {         // We don't normalize numeric fields. Implicit UNF
          fs->options |= FieldSpec_UNF;
        }
        continue;
      } else {
        QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Field `%s` can't have the option SORTABLE", fs->name);
        goto error;
      }
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      fs->options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  return 1;

error:
  if (!QueryError_HasError(status)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Could not parse schema for field `%s`",
                           fs->name);
  }
  return 0;
}

// Assuming the spec is properly locked before calling this function.
size_t IndexSpec_VectorIndexSize(IndexSpec *sp) {
  size_t total_memory = 0;
  for (size_t i = 0; i < sp->numFields; ++i) {
    const FieldSpec *fs = sp->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
      RedisModuleString *vecsim_name = IndexSpec_GetFormattedKey(sp, fs, INDEXFLD_T_VECTOR);
      VecSimIndex *vecsim = OpenVectorIndex(sp, vecsim_name);
      total_memory += VecSimIndex_Info(vecsim).commonInfo.memory;
    }
  }
  return total_memory;
}

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CreateTextId(const IndexSpec *sp) {
  int maxId = -1;
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    const FieldSpec *fs = sp->fields + ii;
    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      if (fs->ftId == (t_fieldId)-1) {
        // ignore
        continue;
      }
      maxId = MAX(fs->ftId, maxId);
    }
  }

  if (maxId + 1 >= SPEC_MAX_FIELD_ID) {
    return -1;
  }
  return maxId + 1;
}

static IndexSpecCache *IndexSpec_BuildSpecCache(const IndexSpec *spec);

/**
 * Add fields to an existing (or newly created) index. If the addition fails,
 */
static int IndexSpec_AddFieldsInternal(IndexSpec *sp, StrongRef spec_ref, ArgsCursor *ac,
                                       QueryError *status, int isNew) {
  if (ac->offset == ac->argc) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Fields arguments are missing");
    return 0;
  }

  const size_t prevNumFields = sp->numFields;
  const size_t prevSortLen = sp->sortables->len;
  const IndexFlags prevFlags = sp->flags;

  while (!AC_IsAtEnd(ac)) {
    if (sp->numFields == SPEC_MAX_FIELDS) {
      QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Schema is limited to %d fields",
                             SPEC_MAX_FIELDS);
      goto reset;
    }

    // Parse path and name of field
    size_t pathlen, namelen;
    const char *fieldPath = AC_GetStringNC(ac, &pathlen);
    const char *fieldName = fieldPath;
    if (AC_AdvanceIfMatch(ac, SPEC_AS_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_AS_STR " requires an argument");
        goto reset;
      }
      fieldName = AC_GetStringNC(ac, &namelen);
      sp->flags |= Index_HasFieldAlias;
    } else {
      // if `AS` is not used, set the path as name
      namelen = pathlen;
      fieldPath = NULL;
    }

    if (IndexSpec_GetField(sp, fieldName, namelen)) {
      QueryError_SetErrorFmt(status, QUERY_EINVAL, "Duplicate field in schema - %s", fieldName);
      goto reset;
    }

    FieldSpec *fs = IndexSpec_CreateField(sp, fieldName, fieldPath);
    if (!parseFieldSpec(ac, sp, spec_ref, fs, status)) {
      goto reset;
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_IsIndexable(fs)) {
      int textId = IndexSpec_CreateTextId(sp);
      if (textId < 0) {
        QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Schema is limited to %d TEXT fields",
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
              status, QUERY_ELIMIT,
              "Cannot add more fields. Declare index with wide fields to allow adding "
              "unlimited fields");
          goto reset;
        }
      }
      fs->ftId = textId;
      if isSpecJson (sp) {
        if ((sp->flags & Index_HasFieldAlias) && (sp->flags & Index_StoreTermOffsets)) {
          RedisModuleString *err_msg;
          JSONPath jsonPath = pathParse(fs->path, &err_msg);
          if (jsonPath && pathHasDefinedOrder(jsonPath)) {
            // Ordering is well defined
            fs->options &= ~FieldSpec_UndefinedOrder;
          } else {
            // Mark FieldSpec
            fs->options |= FieldSpec_UndefinedOrder;
            // Mark IndexSpec
            sp->flags |= Index_HasUndefinedOrder;
          }
          if (jsonPath) {
            pathFree(jsonPath);
          } else if (err_msg) {
            JSONParse_error(status, err_msg, fs->path, fs->name, sp->name);
            goto reset;
          } /* else {
            RedisModule_Log(RSDummyContext, "info",
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
        QueryError_SetErrorFmt(status, QUERY_EBADOPTION,
                               "Cannot set dynamic field to sortable - %s", fieldName);
        goto reset;
      }

      fs->sortIdx = RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
      if (fs->sortIdx == -1) {
        QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Schema is limited to %d Sortable fields",
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

  return 1;

reset:
  for (size_t ii = prevNumFields; ii < sp->numFields; ++ii) {
    FieldSpec_Cleanup(&sp->fields[ii]);
  }

  sp->numFields = prevNumFields;
  sp->sortables->len = prevSortLen;
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

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
StrongRef IndexSpec_Parse(const char *name, const char **argv, int argc, QueryError *status) {
  IndexSpec *spec = NewIndexSpec(name);
  StrongRef spec_ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  spec->own_ref = spec_ref;

  IndexSpec_MakeKeyless(spec);

  ArgsCursor ac = {0};
  ArgsCursor acStopwords = {0};
  char *setdelimiters = NULL;
  char *adddelimiters = NULL;
  char *deldelimiters = NULL;

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
      SPEC_FOLLOW_HASH_ARGS_DEF(&rule_args){
          .name = SPEC_TEMPORARY_STR, .target = &timeout, .type = AC_ARGTYPE_LLONG},
      {.name = SPEC_STOPWORDS_STR, .target = &acStopwords, .type = AC_ARGTYPE_SUBARGS},
      {.name = SPEC_SET_DELIMITERS_STR, .target = &setdelimiters, .type = AC_ARGTYPE_STRING},
      {.name = SPEC_ADD_DELIMITERS_STR, .target = &adddelimiters, .type = AC_ARGTYPE_STRING},
      {.name = SPEC_DEL_DELIMITERS_STR, .target = &deldelimiters, .type = AC_ARGTYPE_STRING},
      {.name = NULL}
  };

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

  if (setdelimiters != NULL) {
    if(spec->delimiters) {
      DelimiterList_Unref(spec->delimiters);
    }
    spec->delimiters = NewDelimiterListCStr((const char *)setdelimiters);
    spec->flags |= Index_HasCustomDelimiters;
  }

  if (adddelimiters != NULL) {
    spec->delimiters = AddDelimiterListCStr((const char *)adddelimiters, spec->delimiters);
    spec->flags |= Index_HasCustomDelimiters;
  }

  if (deldelimiters != NULL) {
    spec->delimiters = RemoveDelimiterListCStr((const char *)deldelimiters, spec->delimiters);
    spec->flags |= Index_HasCustomDelimiters;
  }

  if (!AC_AdvanceIfMatch(&ac, SPEC_SCHEMA_STR)) {
    if (AC_NumRemaining(&ac)) {
      const char *badarg = AC_GetStringNC(&ac, NULL);
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Unknown argument `%s`", badarg);
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "No schema found");
    }
    goto failure;
  }

  if (!IndexSpec_AddFieldsInternal(spec, spec_ref, &ac, status, 1)) {
    goto failure;
  }

  if (spec->rule->filter_exp) {
    SchemaRule_FilterFields(spec);
  }

  for (int i = 0; i < spec->numFields; i++) {
    FieldsGlobalStats_UpdateStats(spec->fields + i, 1);
  }

  return spec_ref;

failure:  // on failure free the spec fields array and return an error
  spec->flags &= ~Index_Temporary;
  IndexSpec_RemoveFromGlobals(spec_ref);
  return INVALID_STRONG_REF;
}

/* Initialize some index stats that might be useful for scoring functions */
// Assuming the spec is properly locked before calling this function
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats) {
  stats->numDocs = sp->stats.numDocuments;
  stats->numTerms = sp->stats.numTerms;
  stats->avgDocLen =
      stats->numDocs ? (double)sp->stats.numRecords / (double)sp->stats.numDocuments : 0;
}

// Assuming the spec is properly locked for writing before calling this function.
int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  int isNew = Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL);
  if (isNew) {
    sp->stats.numTerms++;
    sp->stats.termsSize += len;
  }
  return isNew;
}

// For testing purposes only
void Spec_AddToDict(RefManager *rm) {
  dictAdd(specDict_g, ((IndexSpec*)__RefManager_Get_Object(rm))->name, (void *)rm);
}

static void IndexSpecCache_Free(IndexSpecCache *c) {
  for (size_t ii = 0; ii < c->nfields; ++ii) {
    if (c->fields[ii].name != c->fields[ii].path) {
      rm_free(c->fields[ii].name);
    }
    rm_free(c->fields[ii].path);
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
    ret->fields[ii] = spec->fields[ii];
    ret->fields[ii].name = rm_strdup(spec->fields[ii].name);
    ret->fields[ii].delimiters = spec->fields[ii].delimiters;
    // if name & path are pointing to the same string, copy pointer
    if (ret->fields[ii].path && (spec->fields[ii].name != spec->fields[ii].path)) {
      ret->fields[ii].path = rm_strdup(spec->fields[ii].path);
    } else {
      // use the same pointer for both name and path
      ret->fields[ii].path = ret->fields[ii].name;
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
    cleanPool = redisearch_thpool_create(1, DEFAULT_PRIVILEGED_THREADS_NUM);
    redisearch_thpool_init(cleanPool, LogCallback);
  }
}

void CleanPool_ThreadPoolDestroy() {
  if (cleanPool) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    if (RSGlobalConfig.freeResourcesThread) {
      redisearch_thpool_wait(cleanPool);
    }
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
  rm_free(spec->name);
  // Free sortable list
  if (spec->sortables) {
    SortingTable_Free(spec->sortables);
    spec->sortables = NULL;
  }
  // Free suffix trie
  if (spec->suffix) {
    TrieType_Free(spec->suffix);
  }

  // Destroy the spec's lock
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
  if (spec->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, spec->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
    spec->isTimerSet = false;
  }
  // Stop and destroy indexer
  if (spec->indexer) {
    Indexer_Free(spec->indexer);
  }
  // Stop and destroy garbage collector
  if (spec->gc) {
    GCContext_Stop(spec->gc);
  }

  // Free stopwords list (might use global pointer to default list)
  if (spec->stopwords) {
    StopWordList_Unref(spec->stopwords);
    spec->stopwords = NULL;
  }

  // Free delimiter list
  if (spec->delimiters) {
    DelimiterList_Unref(spec->delimiters);
    spec->delimiters = NULL;
  }

  // Reset fields stats
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      FieldsGlobalStats_UpdateStats(spec->fields + i, -1);
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
// we use the global reference as our guard and access the spec dierctly.
// This function consumes the Strong reference it gets
void IndexSpec_RemoveFromGlobals(StrongRef spec_ref) {
  IndexSpec *spec = StrongRef_Get(spec_ref);

  // Remove spec from global index list
  dictDelete(specDict_g, spec->name);

  // Remove spec from global aliases list
  if (spec->uniqueId) {
    // If uniqueid is 0, it means the index was not initialized
    // and is being freed now during an error.
    IndexSpec_ClearAliases(spec_ref);
  }

  SchemaPrefixes_RemoveSpec(spec_ref);

  // Mark there are pending index drops.
  // if ref count is > 1, the actual cleanup will be done only when StrongRefs are released.
  addPendingIndexDrop();

  // Nullify the spec's quick access to the strong ref. (doesn't decrements refrences count).
  spec->own_ref = (StrongRef){0};

  // mark the spec as deleted and decrement the ref counts owned by the global dictionaries
  StrongRef_Invalidate(spec_ref);
  StrongRef_Release(spec_ref);
}

void Indexes_Free(dict *d) {
  // free the schema dictionary this way avoid iterating over it for each combination of
  // spec<-->prefix
  SchemaPrefixes_Free(ScemaPrefixes_g);
  SchemaPrefixes_Create();

  // cursor list is iterating through the list as well and consuming a lot of CPU
  CursorList_Empty(&g_CursorsList);
  CursorList_Empty(&g_CursorsListCoord);

  arrayof(StrongRef) specs = array_new(StrongRef, dictSize(d));
  dictIterator *iter = dictGetIterator(d);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    specs = array_append(specs, spec_ref);
  }
  dictReleaseIterator(iter);

  for (size_t i = 0; i < array_len(specs); ++i) {
    IndexSpec_RemoveFromGlobals(specs[i]);
  }
  array_free(specs);
}


//---------------------------------------- atomic updates ---------------------------------------

// atomic update of usage counter
inline static void IndexSpec_IncreasCounter(IndexSpec *sp) {
  __atomic_fetch_add(&sp->counter , 1, __ATOMIC_RELAXED);
}


///////////////////////////////////////////////////////////////////////////////////////////////

StrongRef IndexSpec_LoadUnsafe(RedisModuleCtx *ctx, const char *name, int openWrite) {
  IndexLoadOptions lopts = {.flags = openWrite ? INDEXSPEC_LOAD_WRITEABLE : 0,
                            .name = {.cstring = name}};
  lopts.flags |= INDEXSPEC_LOAD_KEYLESS;
  return IndexSpec_LoadUnsafeEx(ctx, &lopts);
}

StrongRef IndexSpec_LoadUnsafeEx(RedisModuleCtx *ctx, IndexLoadOptions *options) {
  const char *ixname = NULL;
  if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
    ixname = RedisModule_StringPtrLen(options->name.rstring, NULL);
  } else {
    ixname = options->name.cstring;
  }

  StrongRef spec_ref = {dictFetchValue(specDict_g, ixname)};
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    if (!(options->flags & INDEXSPEC_LOAD_NOALIAS)) {
      spec_ref = IndexAlias_Get(ixname);
      sp = StrongRef_Get(spec_ref);
    }
    if (!sp) {
      return spec_ref;
    }
  }

  // Increament the number of uses.
  IndexSpec_IncreasCounter(sp);

  if (!RS_IsMock && (sp->flags & Index_Temporary) && !(options->flags & INDEXSPEC_LOAD_NOTIMERUPDATE)) {
    if (sp->isTimerSet) {
      WeakRef old_timer_ref;
      if (RedisModule_StopTimer(RSDummyContext, sp->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
        WeakRef_Release(old_timer_ref);
      }
    }
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
        ret = fmtRedisNumericIndexKey(&sctx, fs->name);
        break;
      case INDEXFLD_T_TAG:
        ret = TagIndex_FormatName(&sctx, fs->name);
        break;
      case INDEXFLD_T_VECTOR:
        // TODO: remove the whole thing
        // NOT NECESSARY ANYMORE - used when field were in keyspace
        ret = RedisModule_CreateString(sctx.redisCtx, fs->name, strlen(fs->name));
        break;
      case INDEXFLD_T_GEOMETRY:
        ret = fmtRedisGeometryIndexKey(&sctx, fs->name);
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
  const FieldSpec *fs = IndexSpec_GetField(sp, s, strlen(s));
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

IndexSpec *NewIndexSpec(const char *name) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  sp->fields = rm_calloc(sizeof(FieldSpec), SPEC_MAX_FIELDS);
  sp->sortables = NewSortingTable();
  sp->flags = INDEX_DEFAULT_FLAGS;
  sp->name = rm_strdup(name);
  sp->nameLen = strlen(name);
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->stopwords = DefaultStopWordList();
  sp->delimiters = DefaultDelimiterList();
  sp->terms = NewTrie(NULL, Trie_Sort_Lex);
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
  sp->used_dialects = 0;

  memset(&sp->stats, 0, sizeof(sp->stats));

  int res = 0;
  pthread_rwlockattr_t attr;
  res = pthread_rwlockattr_init(&attr);
  RedisModule_Assert(res == 0);
#if !defined(__APPLE__) && !defined(__FreeBSD__)
  int pref = PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP;
  res = pthread_rwlockattr_setkind_np(&attr, pref);
  RedisModule_Assert(res == 0);
#endif

  pthread_rwlock_init(&sp->rwlock, &attr);

  return sp;
}

// Assuming the spec is properly locked before calling this function.
FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path) {
  sp->fields = rm_realloc(sp->fields, sizeof(*sp->fields) * (sp->numFields + 1));
  FieldSpec *fs = sp->fields + sp->numFields;
  memset(fs, 0, sizeof(*fs));
  fs->index = sp->numFields++;
  fs->name = rm_strdup(name);
  fs->path = (path) ? rm_strdup(path) : fs->name;
  fs->ftId = (t_fieldId)-1;
  fs->ftWeight = 1.0;
  fs->sortIdx = -1;
  fs->delimiters = DefaultDelimiterList();
  fs->tagOpts.tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  if (!(sp->flags & Index_FromLLAPI)) {
    RS_LOG_ASSERT((sp->rule), "index w/o a rule?");
    switch (sp->rule->type) {
      case DocumentType_Hash:
        fs->tagOpts.tagSep = TAG_FIELD_DEFAULT_HASH_SEP; break;
      case DocumentType_Json:
        fs->tagOpts.tagSep = TAG_FIELD_DEFAULT_JSON_SEP; break;
      case DocumentType_Unsupported:
        RS_LOG_ASSERT(0, "shouldn't get here");
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

// Only used on new specs so it's thread safe
void IndexSpec_MakeKeyless(IndexSpec *sp) {
  // Initialize only once:
  if (!invidxDictType.valDestructor) {
    invidxDictType = dictTypeHeapRedisStrings;
    invidxDictType.valDestructor = valFreeCb;
  }
  sp->keysDict = dictCreate(&invidxDictType, NULL);
}

// Only used on new specs so it's thread safe
void IndexSpec_StartGCFromSpec(StrongRef global, IndexSpec *sp, uint32_t gcPolicy) {
  sp->gc = GCContext_CreateGC(global, gcPolicy);
  GCContext_Start(sp->gc);
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
// Only used on new specs so it's thread safe
void IndexSpec_StartGC(RedisModuleCtx *ctx, StrongRef global, IndexSpec *sp) {
  RS_LOG_ASSERT(!sp->gc, "GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.gcConfigParams.enableGC && !(sp->flags & Index_Temporary)) {
    sp->gc = GCContext_CreateGC(global, RSGlobalConfig.gcConfigParams.gcPolicy);
    GCContext_Start(sp->gc);
    RedisModule_Log(ctx, "verbose", "Starting GC for index %s", sp->name);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void IndexStats_RdbLoad(RedisModuleIO *rdb, IndexStats *stats) {
  stats->numDocuments = RedisModule_LoadUnsigned(rdb);
  stats->numTerms = RedisModule_LoadUnsigned(rdb);
  stats->numRecords = RedisModule_LoadUnsigned(rdb);
  stats->invertedSize = RedisModule_LoadUnsigned(rdb);
  stats->invertedCap = RedisModule_LoadUnsigned(rdb);
  stats->skipIndexesSize = RedisModule_LoadUnsigned(rdb);
  stats->scoreIndexesSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecsSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecRecords = RedisModule_LoadUnsigned(rdb);
  stats->termsSize = RedisModule_LoadUnsigned(rdb);
}

static void IndexStats_RdbSave(RedisModuleIO *rdb, IndexStats *stats) {
  RedisModule_SaveUnsigned(rdb, stats->numDocuments);
  RedisModule_SaveUnsigned(rdb, stats->numTerms);
  RedisModule_SaveUnsigned(rdb, stats->numRecords);
  RedisModule_SaveUnsigned(rdb, stats->invertedSize);
  RedisModule_SaveUnsigned(rdb, stats->invertedCap);
  RedisModule_SaveUnsigned(rdb, stats->skipIndexesSize);
  RedisModule_SaveUnsigned(rdb, stats->scoreIndexesSize);
  RedisModule_SaveUnsigned(rdb, stats->offsetVecsSize);
  RedisModule_SaveUnsigned(rdb, stats->offsetVecRecords);
  RedisModule_SaveUnsigned(rdb, stats->termsSize);
}

///////////////////////////////////////////////////////////////////////////////////////////////

static redisearch_threadpool reindexPool = NULL;

static IndexesScanner *IndexesScanner_NewGlobal() {
  if (global_spec_scanner) {
    return NULL;
  }

  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));
  scanner->global = true;
  scanner->scannedKeys = 0;
  scanner->totalKeys = RedisModule_DbSize(RSDummyContext);

  global_spec_scanner = scanner;
  RedisModule_Log(RSDummyContext, "notice", "Global scanner created");

  return scanner;
}

static IndexesScanner *IndexesScanner_New(StrongRef global_ref) {

  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));
  scanner->global = false;
  scanner->scannedKeys = 0;
  scanner->totalKeys = RedisModule_DbSize(RSDummyContext);

  scanner->spec_ref = StrongRef_Demote(global_ref);
  IndexSpec *spec = StrongRef_Get(global_ref);
  scanner->spec_name = rm_strndup(spec->name, spec->nameLen);

  // scan already in progress?
  if (spec->scanner) {
    // cancel ongoing scan, keep on_progress indicator on
    IndexesScanner_Cancel(spec->scanner);
    RedisModule_Log(RSDummyContext, "notice", "Scanning index %s in background: cancelled and restarted",
                    spec->name);
  }
  spec->scanner = scanner;
  spec->scan_in_progress = true;

  return scanner;
}

void IndexesScanner_Free(IndexesScanner *scanner) {
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
  if (scanner->spec_name) rm_free(scanner->spec_name);
  rm_free(scanner);
}

void IndexesScanner_Cancel(IndexesScanner *scanner) {
  scanner->cancelled = true;
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
  // RMKey it is provided as best effort but in some cases it might be NULL
  bool keyOpened = false;
  if (!key) {
    key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ);
    keyOpened = true;
  }

  // check type of document is support and document is not empty
  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  if (keyOpened) {
    RedisModule_CloseKey(key);
  }

  if (scanner->global) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, keyname, type, NULL);
  } else {
    StrongRef curr_run_ref = WeakRef_Promote(scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(curr_run_ref);
    if (sp) {
      // This check is performed without locking the spec, but it's ok since we locked the GIL
      // So the main thread is not running and the GC is not touching the relevant data
      if (SchemaRule_ShouldIndex(sp, keyname, type)) {
        IndexSpec_UpdateDoc(sp, ctx, keyname, type);
      }
      StrongRef_Release(curr_run_ref);
    } else {
      // spec was deleted, cancel scan
      scanner->cancelled = true;
    }
  }
  ++scanner->scannedKeys;
}

//---------------------------------------------------------------------------------------------

static void Indexes_ScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RedisModule_ThreadSafeContextLock(ctx);

  if (scanner->cancelled) {
    goto end;
  }
  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background");
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background", scanner->spec_name);
  }

  size_t counter = 0;
  while (RedisModule_Scan(ctx, cursor, (RedisModuleScanCB)Indexes_ScanProc, scanner)) {
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

    if (scanner->cancelled) {
      RedisModule_Log(ctx, "notice", "Scanning indexes in background: cancelled (scanned=%ld)",
                  scanner->totalKeys);
      goto end;
    }
  }

  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%ld)",
                    scanner->totalKeys);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background: done (scanned=%ld)",
                    scanner->spec_name, scanner->totalKeys);
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
    reindexPool = redisearch_thpool_create(1, DEFAULT_PRIVILEGED_THREADS_NUM);
    redisearch_thpool_init(reindexPool, LogCallback);
  }
#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Register index %s for async scan", ((IndexSpec*)StrongRef_Get(spec_ref))->name);
#endif
  IndexesScanner *scanner = IndexesScanner_New(spec_ref);
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

  // Index defenition
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
  RedisModule_InfoAddFieldDouble(ctx, "vector_index_size", IndexSpec_VectorIndexSize(sp) / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "offset_vectors_size", sp->stats.offsetVecsSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "doc_table_size", sp->docs.memsize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "sortable_values_size", sp->docs.sortablesSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "key_table_size", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoAddFieldULongLong(ctx, "total_inverted_index_blocks", TotalIIBlocks);

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
  double percent_indexed = IndexesScanner_IndexedPercent(scanner, sp);
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

  // Separators
  if (sp->flags & Index_HasCustomSeparators) {
    AddDelimiterListToInfo(ctx, sp->separators);
  }
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
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(ctx.redisCtx, INDEX_SPEC_KEY_FMT, ctx.spec->name);
  Redis_DeleteKey(ctx.redisCtx, str);
  RedisModule_FreeString(ctx.redisCtx, str);
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

    // put the new index in the specDict_g with weak and strong references
    dictAdd(specDict_g, sp->name, spec_ref.rm);
  }
  dictReleaseIterator(iter);
}

void Indexes_ScanAndReindex() {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_create(1, DEFAULT_PRIVILEGED_THREADS_NUM);
    redisearch_thpool_init(reindexPool, LogCallback);
  }

  RedisModule_Log(NULL, "notice", "Scanning all indexes");
  IndexesScanner *scanner = IndexesScanner_NewGlobal();
  // check no global scan is in progress
  if (scanner) {
    redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner, THPOOL_PRIORITY_HIGH);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

int IndexSpec_CreateFromRdb(RedisModuleCtx *ctx, RedisModuleIO *rdb, int encver,
                                       QueryError *status) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  StrongRef spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;

  IndexSpec_MakeKeyless(sp);

  sp->sortables = NewSortingTable();
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->name = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
  sp->nameLen = strlen(sp->name);
  char *tmpName = rm_strdup(sp->name);
  RedisModule_Free(sp->name);
  sp->name = tmpName;
  sp->flags = (IndexFlags)LoadUnsigned_IOError(rdb, goto cleanup);
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = LoadUnsigned_IOError(rdb, goto cleanup);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FieldSpec_RdbLoad(rdb, fs, spec_ref, encver) != REDISMODULE_OK) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Failed to load index field");
      goto cleanup;
    }
    sp->fields[i].index = i;
    if (FieldSpec_IsSortable(fs)) {
      RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
    }
    if (FieldSpec_HasSuffixTrie(fs)) {
      sp->flags |= Index_HasSuffixTrie;
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
      }
    }
  }
  // After loading all the fields, we can build the spec cache
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  //    IndexStats_RdbLoad(rdb, &sp->stats);

  if (SchemaRule_RdbLoad(spec_ref, rdb, encver) != REDISMODULE_OK) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Failed to load schema rule");
    goto cleanup;
  }


  //    DocTable_RdbLoad(&sp->docs, rdb, encver);
  sp->terms = NewTrie(NULL, Trie_Sort_Lex);
  /* For version 3 or up - load the generic trie */
  //  if (encver >= 3) {
  //    sp->terms = TrieType_GenericLoad(rdb, 0);
  //  } else {
  //    sp->terms = NewTrie(NULL);
  //  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
    if (sp->stopwords == NULL)
      goto cleanup;
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  if (encver >= INDEX_DELIMITERS_VERSION) {
    if (sp->flags & Index_HasCustomDelimiters) {
      sp->delimiters = DelimiterList_RdbLoad(rdb);
      if (sp->delimiters == NULL) {
        goto cleanup;
      }
    } else {
      sp->delimiters = DefaultDelimiterList();
    }
  }

  sp->uniqueId = spec_unique_ids++;

  IndexSpec_StartGC(ctx, spec_ref, sp);
  Cursors_initSpec(sp, RSCURSORS_DEFAULT_CAPACITY);

  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
    if (sp->smap == NULL)
      goto cleanup;
  }

  sp->timeout = LoadUnsigned_IOError(rdb, goto cleanup);

  size_t narr = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t ii = 0; ii < narr; ++ii) {
    QueryError _status;
    size_t dummy;
    char *s = LoadStringBuffer_IOError(rdb, &dummy, goto cleanup);
    int rc = IndexAlias_Add(s, spec_ref, 0, &_status);
    RedisModule_Free(s);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(NULL, "notice", "Loading existing alias failed");
    }
  }

  sp->indexer = NewIndexer(sp);

  sp->scan_in_progress = false;

  RefManager *oldSpec = dictFetchValue(specDict_g, sp->name);
  if (oldSpec) {
    // spec already exists lets just free this one
    RedisModule_Log(NULL, "notice", "Loading an already existing index, will just ignore.");
    // setting unique id to zero will make sure index will not be removed from global
    // cursor map and aliases.
    sp->uniqueId = 0;
    // Remove the new spec from the global prefixes dictionary.
    // This is the only global structure that we added the new spec to at this point
    SchemaPrefixes_RemoveSpec(spec_ref);
    addPendingIndexDrop();
    StrongRef_Release(spec_ref);
    spec_ref = (StrongRef){oldSpec};
  } else {
    dictAdd(specDict_g, sp->name, spec_ref.rm);
  }

  for (int i = 0; i < sp->numFields; i++) {
    FieldsGlobalStats_UpdateStats(sp->fields + i, 1);
  }

  return REDISMODULE_OK;

cleanup:
  addPendingIndexDrop();
  StrongRef_Release(spec_ref);
  QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "while reading an index");
  return REDISMODULE_ERR;
}

void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < LEGACY_INDEX_MIN_VERSION || encver > LEGACY_INDEX_MAX_VERSION) {
    return NULL;
  }
  char *name = RedisModule_LoadStringBuffer(rdb, NULL);

  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  StrongRef spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;

  IndexSpec_MakeKeyless(sp);
  sp->sortables = NewSortingTable();
  sp->terms = NULL;
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->name = rm_strdup(name);
  sp->nameLen = strlen(sp->name);
  RedisModule_Free(name);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = RedisModule_LoadUnsigned(rdb);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    FieldSpec_RdbLoad(rdb, fs, spec_ref, encver);
    sp->fields[i].index = i;
    if (FieldSpec_IsSortable(fs)) {
      RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
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

  sp->uniqueId = spec_unique_ids++;

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
      int rc = IndexAlias_Add(s, spec_ref, 0, &status);
      RedisModule_Free(s);
      assert(rc == REDISMODULE_OK);
    }
  }
  sp->indexer = NewIndexer(sp);

  SchemaRuleArgs *rule_args = dictFetchValue(legacySpecRules, sp->name);
  if (!rule_args) {
    RedisModule_LogIOError(rdb, "warning",
                           "Could not find upgrade definition for legacy index '%s'", sp->name);
    StrongRef_Release(spec_ref);
    return NULL;
  }

  QueryError status;
  sp->rule = SchemaRule_Create(rule_args, spec_ref, &status);

  dictDelete(legacySpecRules, sp->name);
  SchemaRuleArgs_Free(rule_args);

  if (!sp->rule) {
    RedisModule_LogIOError(rdb, "warning", "Failed creating rule for legacy index '%s', error='%s'",
                           sp->name, QueryError_GetError(&status));
    StrongRef_Release(spec_ref);
    return NULL;
  }

  // start the gc and add the spec to the cursor list
  IndexSpec_StartGC(RSDummyContext, spec_ref, sp);
  Cursors_initSpec(sp, RSCURSORS_DEFAULT_CAPACITY);

  dictAdd(legacySpecDict, sp->name, spec_ref.rm);
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
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  QueryError status = {0};
  for (size_t i = 0; i < nIndexes; ++i) {
    if (IndexSpec_CreateFromRdb(ctx, rdb, encver, &status) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "error", "RDB Load: %s",
                      status.detail ? status.detail : "general failure");
      return REDISMODULE_ERR;
    }
  }
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
    // we save the name plus the null terminator
    RedisModule_SaveStringBuffer(rdb, sp->name, sp->nameLen + 1);
    RedisModule_SaveUnsigned(rdb, (uint64_t)sp->flags);
    RedisModule_SaveUnsigned(rdb, sp->numFields);
    for (int i = 0; i < sp->numFields; i++) {
      FieldSpec_RdbSave(rdb, &sp->fields[i]);
    }

    SchemaRule_RdbSave(sp->rule, rdb);

    //    IndexStats_RdbSave(rdb, &sp->stats);
    //    DocTable_RdbSave(&sp->docs, rdb);
    //    // save trie of terms
    //    TrieType_GenericSave(rdb, sp->terms, 0);

    // If we have custom stopwords, save them
    if (sp->flags & Index_HasCustomStopwords) {
      StopWordList_RdbSave(rdb, sp->stopwords);
    }

    // If we have custom separators, save them
    if (sp->flags & Index_HasCustomDelimiters) {
      DelimiterList_RdbSave(rdb, sp->delimiters);
    }

    if (sp->flags & Index_HasSmap) {
      SynonymMap_RdbSave(rdb, sp->smap);
    }

    RedisModule_SaveUnsigned(rdb, sp->timeout);

    if (sp->aliases) {
      RedisModule_SaveUnsigned(rdb, array_len(sp->aliases));
      for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
        RedisModule_SaveStringBuffer(rdb, sp->aliases[ii], strlen(sp->aliases[ii]) + 1);
      }
    } else {
      RedisModule_SaveUnsigned(rdb, 0);
    }
  }

  dictReleaseIterator(iter);
}

void IndexSpec_Digest(RedisModuleDigest *digest, void *value) {
}

// from this version we will have the loaded notification which means that scan
// will no longer be needed
Version noScanVersion = {
    .majorVersion = 6,
    .minorVersion = 0,
    .patchVersion = 7,
};

int CompareVestions(Version v1, Version v2) {
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
// This funciton is called in case the server is started or
// when the replica is loading the RDB file from the master.
static void Indexes_LoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent,
                                 void *data) {
  if (subevent == REDISMODULE_SUBEVENT_LOADING_RDB_START ||
      subevent == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
      subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START) {
    Indexes_Free(specDict_g);
    if (legacySpecDict) {
      dictEmpty(legacySpecDict, NULL);
    } else {
      legacySpecDict = dictCreate(&dictTypeHeapStrings, NULL);
    }
    RedisModule_Log(RSDummyContext, "notice", "Loading event starts");
#ifdef MT_BUILD
    workersThreadPool_InitIfRequired();
#endif
  } else if (subevent == REDISMODULE_SUBEVENT_LOADING_ENDED) {
    int hasLegacyIndexes = dictSize(legacySpecDict);
    Indexes_UpgradeLegacyIndexes();

    // we do not need the legacy dict specs anymore
    dictRelease(legacySpecDict);
    legacySpecDict = NULL;

    LegacySchemaRulesArgs_Free(ctx);

    if (hasLegacyIndexes || CompareVestions(redisVersion, noScanVersion) < 0) {
      Indexes_ScanAndReindex();
    } else {
      RedisModule_Log(ctx, "warning",
                      "Skip background reindex scan, redis version contains loaded event.");
    }
#ifdef MT_BUILD
    workersThreadPool_waitAndTerminate(ctx);
#endif
    RedisModule_Log(RSDummyContext, "notice", "Loading event ends");
  }
}

#ifdef MT_BUILD
static void LoadingProgressCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent,
                                 void *data) {
  RedisModule_Log(RSDummyContext, "debug", "Waiting for background jobs to be executed while"
                  " loading is in progress (pregress is %d)",
                  ((RedisModuleLoadingProgress *)data)->progress);
  workersThreadPool_Drain(ctx, 100);
}
#endif

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .rdb_load = IndexSpec_LegacyRdbLoad,
      .rdb_save = IndexSpec_LegacyRdbSave,
      .aux_load = Indexes_RdbLoad,
      .aux_save = Indexes_RdbSave,
      .free = IndexSpec_LegacyFree,
      .aof_rewrite = GenericAofRewrite_DisabledHandler,
      .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
  };

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, Indexes_LoadingEvent);
#ifdef MT_BUILD
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_LoadingProgress, LoadingProgressCallback);
#endif
  return REDISMODULE_OK;
}

int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx);

int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  if (!spec->rule) {
    RedisModule_Log(ctx, "warning", "Index spec %s: no rule found", spec->name);
    return REDISMODULE_ERR;
  }

  hires_clock_t t0;
  hires_clock_get(&t0);

  Document doc = {0};
  Document_Init(&doc, key, DEFAULT_SCORE, DEFAULT_LANGUAGE, type);
  // if a key does not exit, is not a hash or has no fields in index schema

  int rv = REDISMODULE_ERR;
  switch (type) {
  case DocumentType_Hash:
    rv = Document_LoadSchemaFieldHash(&doc, &sctx);
    break;
  case DocumentType_Json:
    rv = Document_LoadSchemaFieldJson(&doc, &sctx);
    break;
  case DocumentType_Unsupported:
    RS_LOG_ASSERT(0, "Should receieve valid type");
  }

  if (rv != REDISMODULE_OK) {
    // we already unlocked the spec but we can increase this value atomically
    __atomic_add_fetch(&spec->stats.indexingFailures, 1, __ATOMIC_RELAXED);

    // if a document did not load properly, it is deleted
    // to prevent mismatch of index and hash
    IndexSpec_DeleteDoc(spec, ctx, key);
    Document_Free(&doc);
    return REDISMODULE_ERR;
  }

  RedisSearchCtx_LockSpecWrite(&sctx);

  QueryError status = {0};
  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(spec, &doc, &status);
  aCtx->stateFlags |= ACTX_F_NOBLOCK | ACTX_F_NOFREEDOC;
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE);

  Document_Free(&doc);

  spec->stats.totalIndexTime += hires_clock_since_usec(&t0);
  RedisSearchCtx_UnlockSpec(&sctx);
  return REDISMODULE_OK;
}

void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, t_docId id) {

  if (DocTable_DeleteR(&spec->docs, key)) {
    spec->stats.numDocuments--;

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (spec->gc) {
      GCContext_OnDelete(spec->gc);
    }
  }

  // VecSim fields clear deleted data on the fly
  if (spec->flags & Index_HasVecSim) {
    for (int i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
        RedisModuleString *rmskey = RedisModule_CreateString(ctx, spec->fields[i].name, strlen(spec->fields[i].name));
        KeysDictValue *kdv = dictFetchValue(spec->keysDict, rmskey);
        RedisModule_FreeString(ctx, rmskey);

        if (!kdv) {
          continue;
        }
        VecSimIndex *vecsim = kdv->p;
        VecSimIndex_DeleteVector(vecsim, id);
      }
    }
  }

  if (spec->flags & Index_HasGeometry) {
    GeometryIndex_RemoveId(ctx, spec, id);
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
  IndexSpec_DeleteDoc_Unsafe(spec, ctx, key, id);
  RedisSearchCtx_UnlockSpec(&sctx);
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  Indexes_Free(specDict_g);
  Dictionary_Clear();
  RSGlobalConfig.used_dialects = 0;
}

void Indexes_Init(RedisModuleCtx *ctx) {
  specDict_g = dictCreate(&dictTypeHeapStrings, NULL);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
}

SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   bool runFilters,
                                                   RedisModuleString *keyToReadData) {
  if (!keyToReadData) {
    keyToReadData = key;
  }
  SpecOpIndexingCtx *res = rm_malloc(sizeof(*res));
  res->specs = dictCreate(&dictTypeHeapStrings, NULL);
  res->specsOps = array_new(SpecOpCtx, 10);
  if (dictSize(specDict_g) == 0) {
    return res;
  }
  dict *specs = res->specs;

#if defined(_DEBUG) && 0
  RLookupKey *k = RLookup_GetKey_LoadEx(&r->lk, UNDERSCORE_KEY, strlen(UNDERSCORE_KEY), UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
  RSValue *v = RLookup_GetItem(k, &r->row);
  const char *x = RSValue_StringPtrLen(v, NULL);
  RedisModule_Log(NULL, "notice", "Indexes_FindMatchingSchemaRules: x=%s", x);
  const char *f = "name";
  k = RLookup_GetKeyEx(&r->lk, f, strlen(f), RLOOKUP_M_READ, RLOOKUP_F_NOFLAGS);
  if (k) {
    v = RLookup_GetItem(k, &r->row);
    x = RSValue_StringPtrLen(v, NULL);
  }
#endif  // _DEBUG

  size_t n;
  const char *key_p = RedisModule_StringPtrLen(key, &n);
  arrayof(SchemaPrefixNode *) prefixes = array_new(SchemaPrefixNode *, 1);
  // collect specs that their name is prefixed by the key name
  // `prefixes` includes list of arrays of specs, one for each prefix of key name
  int nprefixes = TrieMap_FindPrefixes(ScemaPrefixes_g, key_p, n, (arrayof(void *) *)&prefixes);
  for (int i = 0; i < array_len(prefixes); ++i) {
    SchemaPrefixNode *node = prefixes[i];
    for (int j = 0; j < array_len(node->index_specs); ++j) {
      StrongRef global = node->index_specs[j];
      IndexSpec *spec = StrongRef_Get(global);
      if (spec && !dictFind(specs, spec->name)) {
        SpecOpCtx specOp = {
            .spec = spec,
            .op = SpecOp_Add,
        };
        res->specsOps = array_append(res->specsOps, specOp);
        dictEntry *entry = dictAddRaw(specs, spec->name, NULL);
        // put the location on the specsOps array so we can get it
        // fast using index name
        entry->v.u64 = array_len(res->specsOps) - 1;
      }
    }
  }
  array_free(prefixes);

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
        if (!RSValue_BoolTest(&r->res) && dictFind(specs, spec->name)) {
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
    const char *field = RedisModule_StringPtrLen(hashFields[i], NULL);
    for (size_t j = 0; j < spec->numFields; ++j) {
      if (!strcmp(field, spec->fields[j].name)) {
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
    dictEntry *entry = dictFind(to_specs->specs, spec->name);
    if (entry) {
      RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
      RedisSearchCtx_LockSpecWrite(&sctx);
      DocTable_Replace(&spec->docs, from_str, from_len, to_str, to_len);
      RedisSearchCtx_UnlockSpec(&sctx);
      size_t index = entry->v.u64;
      dictDelete(to_specs->specs, spec->name);
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
///////////////////////////////////////////////////////////////////////////////////////////////
