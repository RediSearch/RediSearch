/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"
#include "spec_field_parse.h"
#include "rlookup_load_document.h"

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
#include "search_disk_utils.h"

#define INITIAL_DOC_TABLE_SIZE 1000

///////////////////////////////////////////////////////////////////////////////////////////////

const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;

dict *specDict_g = NULL;
// global_spec_scanner moved to spec_scanner.c
size_t pending_global_indexing_ops = 0;
dict *legacySpecDict;
dict *legacySpecRules;

// Pending or in-progress index drops
uint16_t pendingIndexDropCount_g = 0;

Version redisVersion;
Version rlecVersion;
bool isCrdt;
bool isTrimming = false;
bool isFlex = false;

// Default values make no limits.
size_t memoryLimit = -1;
size_t used_memory = 0;

static redisearch_thpool_t *cleanPool = NULL;

extern DebugCTX globalDebugCtx;

// DEBUG_INDEX_SCANNER_STATUS_STRS, debug scanner functions moved to spec_scanner.c

// Forward declaration for disk validation
inline static bool isSpecOnDiskForValidation(const IndexSpec *sp);

// CheckRdbSstPersistence moved to spec_rdb.c

//---------------------------------------------------------------------------------------------

// threadSleepByConfigTime, scanStopAfterOOM, setMemoryInfo, isBgIndexingMemoryOverLimit
// moved to spec_scanner.c
/*
 * Initialize the spec's fields that are related to the cursors.
 */

void Cursors_initSpec(IndexSpec *spec) {
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

  return IndexSpec_Parse(ctx, name, args, argc, status);
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

// IndexSpec_LegacyFree moved to spec_rdb.c

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

// Stats/info functions moved to spec_info.c

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
  IndexSpec_StartGC(spec_ref, sp, sp->diskSpec ? GCPolicy_Disk : GCPolicy_Fork);

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

// checkPhoneticAlgorithmAndLang, parseVectorField_*, parseTextField, parseTagField,
// parseGeometryField, parseFieldSpec, IndexSpec_AddFieldsInternal, IndexSpec_AddFields,
// VecSimIndex_validate_params moved to spec_field_parse.c

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CreateTextId(IndexSpec *sp, t_fieldIndex index) {
  size_t length = array_len(sp->fieldIdToIndex);
  if (length >= SPEC_MAX_FIELD_ID) {
    return -1;
  }

  array_ensure_append_1(sp->fieldIdToIndex, index);
  return length;
}

// IndexSpec_BuildSpecCache is in spec_cache.c

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

// IndexSpec_PopulateVectorDiskParams moved to spec_rdb.c

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
StrongRef IndexSpec_Parse(RedisModuleCtx *ctx, const HiddenString *name, const char **argv, int argc, QueryError *status) {
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
  int rc = AC_OK;
  ACArgSpec *errarg = NULL;
  bool invalid_flex_on_type = false;
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
  rc = AC_ParseArgSpec(&ac, argopts, &errarg);
  invalid_flex_on_type = isSpecOnDiskForValidation(spec) && rule_args.type && (strcasecmp(rule_args.type, RULE_TYPE_HASH) != 0);
  if (rc != AC_OK) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      goto failure;
    }
  }
  if (invalid_flex_on_type) {
    QueryError_SetError(status, QUERY_ERROR_CODE_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT, "Only HASH is supported as index data type for Flex indexes");
    goto failure;
  }

  if (timeout != -1) {
    // When disk validation is active, argopts is set to flex_argopts, which does not include SPEC_TEMPORARY_STR
    RS_ASSERT(!SearchDisk_IsEnabled());
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

  // Store on disk if we're on Flex.
  // This must be done before IndexSpec_AddFieldsInternal so that sp->diskSpec
  // is available when parsing vector fields (for populating diskCtx).
  // For new indexes (FT.CREATE), we don't delete before open since there's nothing to delete.
  spec->diskSpec = NULL;
  if (isSpecOnDisk(spec)) {
    RS_ASSERT(disk_db);
    size_t len;
    const char* name = HiddenString_GetUnsafe(spec->specName, &len);

    spec->diskSpec = SearchDisk_OpenIndex(ctx, name, len, spec->rule->type, false);
    RS_LOG_ASSERT(spec->diskSpec, "Failed to open disk spec")
    if (!spec->diskSpec) {
      QueryError_SetError(status, QUERY_ERROR_CODE_DISK_CREATION, "Could not open disk index");
      goto failure;
    }
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
      handleBadArguments(spec, badarg, status, non_flex_argopts);
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

  if (isSpecOnDiskForValidation(spec) && !(spec->flags & Index_SkipInitialScan)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_FLEX_SKIP_INITIAL_SCAN_MISSING_ARGUMENT, "Flex index requires SKIPINITIALSCAN argument");
    goto failure;
  }

  return spec_ref;

failure:  // on failure free the spec fields array and return an error
  spec->flags &= ~Index_Temporary;
  IndexSpec_RemoveFromGlobals(spec_ref, false);
  return INVALID_STRONG_REF;
}

StrongRef IndexSpec_ParseC(RedisModuleCtx *ctx, const char *name, const char **argv, int argc, QueryError *status) {
  HiddenString *hidden = NewHiddenString(name, strlen(name), true);
  return IndexSpec_Parse(ctx, hidden, argv, argc, status);
}

// IndexSpec_GetStats, IndexSpec_GetIndexErrorCount moved to spec_info.c

// Assuming the spec is properly locked for writing before calling this function.
void IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  int isNew = Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL, 1);
  if (isNew) {
    sp->stats.scoring.numTerms++;
    sp->stats.termsSize += len;
  }
}

// For testing purposes only
void Spec_AddToDict(RefManager *rm) {
  IndexSpec* spec = ((IndexSpec*)__RefManager_Get_Object(rm));
  dictAdd(specDict_g, (void*)spec->specName, (void *)rm);
}

// IndexSpecCache functions moved to spec_cache.c

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

  // Free fields data
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      FieldSpec_Cleanup(&spec->fields[i]);
    }
    rm_free(spec->fields);
  }
  // Free suffix trie
  if (spec->suffix) {
    TrieType_Free(spec->suffix);
  }

  // Close disk index before freeing spec name (needs the name for tracking)
  if (spec->diskSpec) SearchDisk_CloseIndex(NULL, spec->diskSpec);

  // Free spec name (after disk close, which needs the name)
  HiddenString_Free(spec->specName, true);
  rm_free(spec->obfuscatedName);

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

void Indexes_Free(dict *d, bool deleteDiskData) {
  // free the schema dictionary this way avoid iterating over it for each combination of
  // spec<-->prefix
  SchemaPrefixes_Free(SchemaPrefixes_g);
  SchemaPrefixes_g = NULL;
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
    // Delete disk index before removing from globals
    IndexSpec *spec = StrongRef_Get(specs[i]);
    if (deleteDiskData && spec && spec->diskSpec) {
      SearchDisk_MarkIndexForDeletion(spec->diskSpec);
    }
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

// fmtRedisNumericIndexKey, TagIndex_FormatName, IndexSpec_LegacyGetFormattedKey
// moved to spec_rdb.c

// Assuming the spec is properly locked before calling this function.
void IndexSpec_InitializeSynonym(IndexSpec *sp) {
  if (!sp->smap) {
    sp->smap = SynonymMap_New(false);
    sp->flags |= Index_HasSmap;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec_InitLock(IndexSpec *sp) {
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

// initializeFieldSpec moved to spec_field_parse.c

// Helper function for initializing an index spec
// Solves issues where a field is initialized in index creation but not when loading from RDB
void initializeIndexSpec(IndexSpec *sp, const HiddenString *name, IndexFlags flags,
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

// Only used on new specs so it's thread safe
void IndexSpec_StartGCFromSpec(StrongRef global, IndexSpec *sp, uint32_t gcPolicy) {
  sp->gc = GCContext_CreateGC(global, gcPolicy);
  GCContext_Start(sp->gc);
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
// bit, FieldSpec_RdbLoadCompat8, FieldSpec_RdbSave, fieldTypeMap, FieldSpec_RdbLoad,
// IndexScoringStats_RdbLoad, IndexScoringStats_RdbSave, IndexStats_RdbLoad,
// IndexSpec_DropLegacyIndexFromKeySpace, Indexes_UpgradeLegacyIndexes
// moved to spec_rdb.c

// Scanner lifecycle, scan execution, ScanAndReindex, ReindexPool moved to spec_scanner.c
// Indexes_ScanAndReindex moved to spec_scanner.c

///////////////////////////////////////////////////////////////////////////////////////////////

// IndexSpec_RdbSave, IndexSpec_RdbLoad, IndexSpec_StoreAfterRdbLoad,
// IndexSpec_CreateFromRdb, IndexSpec_LegacyRdbLoad, IndexSpec_LegacyRdbSave,
// Indexes_RdbLoad, Indexes_RdbSave, Indexes_RdbSave2, IndexSpec_RdbLoad_Logic,
// IndexSpec_Serialize, IndexSpec_Deserialize moved to spec_rdb.c

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

// Indexes_Propagate, IndexSpec_RdbSave_Wrapper, IndexSpec_RegisterType
// moved to spec_rdb.c

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

void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  t_docId id = 0;
  uint32_t docLen = 0;
  if (isSpecOnDisk(spec)) {
    RS_LOG_ASSERT(spec->diskSpec, "disk handle is unexpectedly NULL");
    size_t len;
    const char *keyStr = RedisModule_StringPtrLen(key, &len);

    // Delete the document
    SearchDisk_DeleteDocument(spec->diskSpec, keyStr, len, &docLen, &id);

    if (id == 0) {
      // Nothing to delete
      return;
    }
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
        // ctx is NULL because we don't create the index here
        VecSimIndex *vecsim = openVectorIndex(NULL, spec->fields + i, DONT_CREATE_INDEX);
        if(!vecsim) continue;
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

  IndexSpec_IncrActiveWrites(spec);
  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_DeleteDoc_Unsafe(spec, ctx, key);
  IndexSpec_DecrActiveWrites(spec);
  RedisSearchCtx_UnlockSpec(&sctx);

  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  if (specDict_g) {
    Indexes_Free(specDict_g, true);
    // specDict_g itself is not actually freed
  }
  Dictionary_Clear();
  RSGlobalStats.totalStats.used_dialects = 0;
}

void Indexes_Init(RedisModuleCtx *ctx) {
  if (!specDict_g) {
    specDict_g = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  }
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
}

size_t Indexes_Count() {
  return dictSize(specDict_g);
}

SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   DocumentType type, bool runFilters,
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
        // skip if document type does not match the index type
        // The unsupported type is needed for crdt empty keys (deleted)
        if (type != DocumentType_Unsupported && type != spec->rule->type) {
          continue;
        }

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
    // We load the data from the `keyToReadData` key, which is the key the old
    // key was changed to, since the old key is already deleted.
    key_p = RedisModule_StringPtrLen(keyToReadData, NULL);

    EvalCtx *r = NULL;
    for (size_t i = 0; i < array_len(res->specsOps); ++i) {
      SpecOpCtx *specOp = res->specsOps + i;
      IndexSpec *spec = specOp->spec;
      if (!spec->rule->filter_exp) {
        continue;
      }

      // load document only if required
      if (!r) {
        r = EvalCtx_Create(EVAL_MODE_INDEX);
      }

      RedisSearchCtx sctx = { .redisCtx = ctx };
      QueryError status = QueryError_Default();
      RLookup_LoadRuleFields(&sctx, &r->lk, &r->row, spec, key_p, &status);
      QueryError_ClearError(&status); // TODO: report errors

      if (!SchemaRule_FilterPasses(r, spec->rule->filter_exp)) {
        if (dictFind(specs, spec->specName)) {
          specOp->op = SpecOp_Del;
        }
      }
      // Clean up state between iterations (indexes)
      QueryError_ClearError(&r->status);
      RLookup_Cleanup(&r->lk);
      r->lk = RLookup_New();
      RLookupRow_Reset(&r->row);
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
    Indexes_DeleteMatchingWithSchemaRules(ctx, key, type, hashFields);
    return;
  }

  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, type, true, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;

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
                                           DocumentType type,
                                           RedisModuleString **hashFields) {
  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, type, false, NULL);

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

  SpecOpIndexingCtx *from_specs = Indexes_FindMatchingSchemaRules(ctx, from_key, type, true, to_key);
  SpecOpIndexingCtx *to_specs = Indexes_FindMatchingSchemaRules(ctx, to_key, type, true, NULL);

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
// Debug scanner functions moved to spec_scanner.c

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

// DebugIndexesScanner_pauseCheck, Indexes_StartRDBLoadingEvent,
// Indexes_EndRDBLoadingEvent, Indexes_EndLoading moved to spec_scanner.c

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
