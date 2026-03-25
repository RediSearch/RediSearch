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
#include "spec_registry.h"

#include "rmutil/util.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include "rules.h"
#include "search_disk.h"
#include "search_disk_utils.h"

//---------------------------------------------------------------------------------------------

inline static bool isSpecOnDisk(const IndexSpec *sp) {
  return SearchDisk_IsEnabled();
}

inline static bool isSpecOnDiskForValidation(const IndexSpec *sp) {
  return SearchDisk_IsEnabledForValidation();
}

static void handleBadArguments(IndexSpec *spec, const char *badarg, QueryError *status, ACArgSpec *non_flex_argopts) {
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
