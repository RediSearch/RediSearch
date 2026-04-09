/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>
#include "util/arg_parser.h"
#include "util/arr_rm_alloc.h"
#include "spec.h"
#include "config.h"
#include <string.h>
#include <limits.h>

#define COLLECT_MAX_SORT_KEYS SORTASCMAP_MAXFIELDS
#define SORT_DIR_ASC "ASC"
#define SORT_DIR_DESC "DESC"

typedef struct {
  Reducer base;

  arrayof(const RLookupKey *) field_keys;
  bool has_wildcard;

  arrayof(const RLookupKey *) sort_keys;
  uint64_t sortAscMap;

  bool has_limit;
  uint64_t limit_offset;
  uint64_t limit_count;
} CollectReducer;

typedef struct {
  CollectReducer *cr;
  const ReducerOptions *options;
} CollectParseCtx;

// --- Stub vtable functions (to be implemented in later tasks) ---

static void *collectNewInstance(Reducer *rbase) {
  return NULL;
}

static int collectAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  return 1;
}

static RSValue *collectFinalize(Reducer *parent, void *ctx) {
  return RSValue_NullStatic();
}

static void collectFreeInstance(Reducer *parent, void *p) {
}

static void collectFree(Reducer *r) {
  CollectReducer *cr = (CollectReducer *)r;
  array_free(cr->field_keys);
  array_free(cr->sort_keys);
  Reducer_GenericFree(r);
}

// --- ArgParser callbacks ---

// Parses: FIELDS nargs <@field | *> [<@field | *> ...]
//   nargs: 1..SPEC_MAX_FIELDS
static void handleCollectFields(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectReducer *cr = pctx->cr;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  int count = AC_NumRemaining(ac);
  ReducerOptions sub_opts = *opts;
  sub_opts.args = ac;
  sub_opts.name = "FIELDS";

  RS_ASSERT(count >= 1 && count <= SPEC_MAX_FIELDS);
  cr->field_keys = array_new(const RLookupKey *, count);

  for (int i = 0; i < count; i++) {
    if (AC_AdvanceIfMatch(ac, "*")) {
      if (cr->has_wildcard) {
        QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
          "Wildcard `*` can only appear once in FIELDS");
        return;
      }
      cr->has_wildcard = true;
    } else {
      const RLookupKey *key = NULL;
      if (!ReducerOpts_GetKey(&sub_opts, &key)) {
        return;
      }
      array_append(cr->field_keys, key);
    }
  }
}

// Parses: SORTBY nargs <@field [ASC|DESC]> [<@field [ASC|DESC]> ...]
//   nargs: 1..COLLECT_MAX_SORT_KEYS*2
//   Direction defaults to ASC when omitted.
static void handleCollectSortBy(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectReducer *cr = pctx->cr;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  cr->sort_keys = array_new(const RLookupKey *, 4);
  cr->sortAscMap = SORTASCMAP_INIT;

  ReducerOptions key_opts = *opts;
  key_opts.args = ac;
  key_opts.name = "SORTBY";

  while (!AC_IsAtEnd(ac)) {
    const char *s = AC_StringArg(ac, ac->offset);
    if (!s) {
      QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
        "SORTBY: unexpected null argument");
      return;
    }

    if (s[0] != '@') {
      QueryError_SetWithUserDataFmt(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
        "MISSING ASC or DESC after sort field", " (%s)", s);
      return;
    }

    if (array_len(cr->sort_keys) >= COLLECT_MAX_SORT_KEYS) {
      QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
        "SORTBY exceeds maximum of %d fields", COLLECT_MAX_SORT_KEYS);
      return;
    }
    const RLookupKey *key = NULL;
    if (!ReducerOpts_GetKey(&key_opts, &key)) {
      return;
    }
    array_append(cr->sort_keys, key);

    if (AC_AdvanceIfMatch(ac, SORT_DIR_ASC)) {
      // ASC is the default; nothing to do.
    } else if (AC_AdvanceIfMatch(ac, SORT_DIR_DESC)) {
      SORTASCMAP_SETDESC(cr->sortAscMap, array_len(cr->sort_keys) - 1);
    }
  }

  if (array_len(cr->sort_keys) == 0) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "SORTBY requires at least one sort field");
    return;
  }
}

// Parses: LIMIT <offset> <count>
//   Both values must be non-negative integers <= MAX_AGGREGATE_REQUEST_RESULTS.
static void handleCollectLimit(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectReducer *cr = pctx->cr;
  QueryError *status = pctx->options->status;
  ArgsCursor *ac = (ArgsCursor *)value;

  uint64_t offset = 0, count = 0;
  if (AC_GetU64(ac, &offset, AC_F_GE0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "LIMIT offset must be a non-negative integer");
    return;
  }
  // LIMIT count must be at least 1; use REDUCER COUNT to count results without collecting them.
  if (AC_GetU64(ac, &count, AC_F_GE1) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "LIMIT count must be a positive integer");
    return;
  }
  if (offset > MAX_AGGREGATE_REQUEST_RESULTS) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
      "LIMIT offset exceeds maximum of %llu", MAX_AGGREGATE_REQUEST_RESULTS);
    return;
  }
  if (count > MAX_AGGREGATE_REQUEST_RESULTS) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
      "LIMIT count exceeds maximum of %llu", MAX_AGGREGATE_REQUEST_RESULTS);
    return;
  }
  if (offset > LLONG_MAX - count) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Invalid LIMIT offset + count value");
    return;
  }

  cr->has_limit = true;
  cr->limit_offset = offset;
  cr->limit_count = count;
}

// --- Factory function ---

Reducer *RDCRCollect_New(const ReducerOptions *options) {
  if (!RSGlobalConfig.enableUnstableFeatures) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_INVAL,
      "`COLLECT` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off. "
      "Enable it with `CONFIG SET search-enable-unstable-features yes`");
    return NULL;
  }

  CollectReducer *cr = rm_calloc(1, sizeof(*cr));
  cr->sortAscMap = SORTASCMAP_INIT;

  Reducer *rbase = &cr->base;
  rbase->NewInstance = collectNewInstance;
  rbase->Add = collectAdd;
  rbase->Finalize = collectFinalize;
  rbase->FreeInstance = collectFreeInstance;
  rbase->Free = collectFree;

  CollectParseCtx pctx = {.cr = cr, .options = options};

  ArgsCursor *ac = options->args;
  ArgParser *parser = ArgParser_New(ac, NULL);
  if (!parser) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Failed to create argument parser for COLLECT");
    rbase->Free(rbase);
    return NULL;
  }

  ArgsCursor subArgs = {0};

  ArgParser_AddSubArgsV(parser, "FIELDS", "Projected fields",
    &subArgs, 1, SPEC_MAX_FIELDS,
    ARG_OPT_REQUIRED,
    ARG_OPT_POSITION, 1,
    ARG_OPT_CALLBACK, handleCollectFields, &pctx,
    ARG_OPT_END);

  ArgParser_AddSubArgsV(parser, "SORTBY", "In-group sort keys",
    &subArgs, 1, COLLECT_MAX_SORT_KEYS * 2,
    ARG_OPT_OPTIONAL,
    ARG_OPT_CALLBACK, handleCollectSortBy, &pctx,
    ARG_OPT_END);

  ArgParser_AddSubArgsV(parser, "LIMIT", "Per-group limit",
    &subArgs, 2, 2,
    ARG_OPT_OPTIONAL,
    ARG_OPT_CALLBACK, handleCollectLimit, &pctx,
    ARG_OPT_END);

  ArgParseResult result = ArgParser_Parse(parser);

  if (QueryError_HasError(options->status)) {
    ArgParser_Free(parser);
    rbase->Free(rbase);
    return NULL;
  }

  if (!result.success) {
    QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Bad arguments for COLLECT", ": %s", ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    rbase->Free(rbase);
    return NULL;
  }

  ArgParser_Free(parser);

  return rbase;
}
