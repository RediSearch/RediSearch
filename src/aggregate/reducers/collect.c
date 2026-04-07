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
#include "spec.h"
#include "config.h"
#include <string.h>
#include <limits.h>

#define COLLECT_MAX_SORT_KEYS SORTASCMAP_MAXFIELDS
#define SORT_DIR_ASC "ASC"
#define SORT_DIR_DESC "DESC"

typedef struct {
  Reducer base;

  int num_fields;
  const RLookupKey **field_keys;
  bool has_wildcard;

  int num_sort_keys;
  const RLookupKey **sort_keys;
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
  rm_free(cr->field_keys);
  rm_free(cr->sort_keys);
  BlkAlloc_FreeAll(&r->alloc, NULL, 0, 0);
  rm_free(cr);
}

// --- ArgParser callbacks ---

static void handleCollectFields(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectReducer *cr = pctx->cr;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  int count = AC_NumRemaining(ac);
  if (count < 1) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "FIELDS requires at least 1 field");
    return;
  }
  if (count > SPEC_MAX_FIELDS) {
    QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
      "FIELDS count exceeds maximum of %d", SPEC_MAX_FIELDS);
    return;
  }

  ReducerOptions sub_opts = *opts;
  sub_opts.args = ac;
  sub_opts.name = "FIELDS";

  cr->field_keys = rm_calloc(count, sizeof(const RLookupKey *));
  int field_idx = 0;

  for (int i = 0; i < count; i++) {
    if (strcmp(AC_StringArg(ac, ac->offset), "*") == 0) {
      if (cr->has_wildcard) {
        QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
          "Wildcard `*` can only appear once in FIELDS");
        return;
      }
      cr->has_wildcard = true;
      ac->offset++;
    } else if (!ReducerOpts_GetKey(&sub_opts, &cr->field_keys[field_idx++])) {
      return;
    }
  }
  cr->num_fields = field_idx;
}

static void handleCollectSortBy(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectReducer *cr = pctx->cr;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  cr->sort_keys = rm_calloc(COLLECT_MAX_SORT_KEYS, sizeof(const RLookupKey *));
  cr->sortAscMap = SORTASCMAP_INIT;
  cr->num_sort_keys = 0;

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

    if (s[0] == '@') {
      if (cr->num_sort_keys >= COLLECT_MAX_SORT_KEYS) {
        QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
          "SORTBY exceeds maximum of %d fields", COLLECT_MAX_SORT_KEYS);
        return;
      }
      if (!ReducerOpts_GetKey(&key_opts, &cr->sort_keys[cr->num_sort_keys])) {
        return;
      }
      cr->num_sort_keys++;
    } else if (strcasecmp(s, SORT_DIR_ASC) == 0) {
      ac->offset++;
      if (cr->num_sort_keys > 0) {
        SORTASCMAP_SETASC(cr->sortAscMap, cr->num_sort_keys - 1);
      }
    } else if (strcasecmp(s, SORT_DIR_DESC) == 0) {
      ac->offset++;
      if (cr->num_sort_keys > 0) {
        SORTASCMAP_SETDESC(cr->sortAscMap, cr->num_sort_keys - 1);
      }
    } else {
      QueryError_SetWithUserDataFmt(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
        "MISSING ASC or DESC after sort field", " (%s)", s);
      return;
    }
  }

  if (cr->num_sort_keys == 0) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "SORTBY requires at least one sort field");
    return;
  }
}

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
  if (AC_GetU64(ac, &count, AC_F_GE0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "LIMIT count must be a non-negative integer");
    return;
  }
  if (count > MAX_AGGREGATE_REQUEST_RESULTS) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
      "LIMIT count exceeds maximum of %llu", MAX_AGGREGATE_REQUEST_RESULTS);
    return;
  }
  if (offset > LLONG_MAX - count) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "LIMIT offset + count overflow");
    return;
  }

  cr->has_limit = true;
  cr->limit_offset = offset;
  cr->limit_count = count;
}

// --- Factory function ---

Reducer *RDCRCollect_New(const ReducerOptions *options) {
  CollectReducer *cr = rm_calloc(1, sizeof(*cr));
  cr->sortAscMap = SORTASCMAP_INIT;
  CollectParseCtx pctx = {.cr = cr, .options = options};

  ArgsCursor *ac = options->args;
  ArgParser *parser = ArgParser_New(ac, NULL);
  if (!parser) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Failed to create argument parser for COLLECT");
    rm_free(cr);
    return NULL;
  }

  ArgsCursor subArgs = {0};

  ArgParser_AddSubArgsV(parser, "FIELDS", "Projected fields",
    &subArgs, 1, -1,
    ARG_OPT_REQUIRED,
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
    collectFree(&cr->base);
    return NULL;
  }

  if (!result.success) {
    QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Bad arguments for COLLECT", ": %s", ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    collectFree(&cr->base);
    return NULL;
  }

  ArgParser_Free(parser);

  Reducer *rbase = &cr->base;
  rbase->NewInstance = collectNewInstance;
  rbase->Add = collectAdd;
  rbase->Finalize = collectFinalize;
  rbase->FreeInstance = collectFreeInstance;
  rbase->Free = collectFree;

  return rbase;
}
