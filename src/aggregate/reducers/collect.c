/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>
#include "aggregate/reducers/collect_parse.h"
#include "util/arg_parser.h"
#include "util/arr_rm_alloc.h"
#include "util/misc.h"
#include "spec.h"
#include "config.h"
#include "reducers_ffi.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define COLLECT_MAX_SORT_KEYS SORTASCMAP_MAXFIELDS
#define COLLECT_MAX_SORT_TOKENS (COLLECT_MAX_SORT_KEYS * 2)  // each key may have a direction
#define COLLECT_MAX_FIELD_ARGS  (SPEC_MAX_FIELDS + 1)
#define SORT_DIR_ASC "ASC"
#define SORT_DIR_DESC "DESC"

typedef struct {
  CollectArgs *args;
  const ReducerOptions *options;
} CollectParseCtx;

// Validates a `@`-prefixed name argument and returns the name with the leading
// `@` stripped. On error, sets `status` and returns NULL.
//
// Caller guarantees `s` is NUL-terminated and `len` reflects strlen(s).
static const char *parseAtPrefixedName(const char *s, size_t len, QueryError *status) {
  if (len == 0 || s[0] != '@') {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Missing prefix: name requires '@' prefix", " (%s)", s ? s : "");
    return NULL;
  }
  return s + 1;
}

// ===== ArgParser callbacks (pure: populate CollectArgs only) =====

// ----- FIELDS -----

// Parses: FIELDS ( * | <num_fields> @field [@field ...] )
//   <num_fields>: 1..COLLECT_MAX_FIELD_ARGS
//
// The first token after `FIELDS` is consumed by `ArgParser_AddStringV` and
// passed via `value`; the remainder is read directly from the parser's
// underlying cursor.
static void handleCollectFields(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectArgs *args = pctx->args;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = parser->cursor;
  const char *firstArg = *(const char **)value;

  // Load-all branch: `*` consumes nothing else from FIELDS.
  if (strcmp(firstArg, "*") == 0) {
    args->load_all = true;
    return;
  }

  // Count branch: validate <num_fields> then carve a slice of that size and
  // strip the `@` prefix from each name.
  char *end;
  errno = 0;
  long long count = strtoll(firstArg, &end, 10);
  if (errno != 0 || end == firstArg || *end != '\0') {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Expected number of fields or `*` after FIELDS");
    return;
  }
  if (count < 1 || count > COLLECT_MAX_FIELD_ARGS) {
    QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
      "FIELDS count must be in [1, %d]", COLLECT_MAX_FIELD_ARGS);
    return;
  }
  ArgsCursor sub = {0};
  if (AC_GetSlice(ac, &sub, (size_t)count) != AC_OK) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Not enough arguments were provided based on argument count for FIELDS");
    return;
  }

  args->field_names = array_new(const char *, (size_t)count);
  while (!AC_IsAtEnd(&sub)) {
    const char *s;
    size_t len;
    int rv = AC_GetString(&sub, &s, &len, 0);
    // The slice is sized exactly to `<num_fields>`, so every iteration succeeds.
    RS_ASSERT(rv == AC_OK);
    const char *name = ExtractKeyName(s, &len, opts->status, opts->strictPrefix, "FIELDS");
    if (!name) return;
    array_append(args->field_names, name);
  }
}

// ----- SORTBY -----

static void handleCollectSortDirection(ArgsCursor *ac, uint64_t *sortAscMap, size_t dir_idx) {
  if (AC_AdvanceIfMatch(ac, SORT_DIR_ASC)) {
    // ASC is the default; nothing to do.
  } else if (AC_AdvanceIfMatch(ac, SORT_DIR_DESC)) {
    SORTASCMAP_SETDESC(*sortAscMap, dir_idx);
  }
}

// Parses: SORTBY nargs <@field [ASC|DESC]> [<@field [ASC|DESC]> ...]
//   nargs: 1..COLLECT_MAX_SORT_KEYS*2
//   Direction defaults to ASC when omitted.
static void handleCollectSortBy(const ArgParser *parser, const void *value, void *user_data) {
  (void)parser;
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectArgs *args = pctx->args;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  args->sort_names = array_new(const char *, 4);
  args->sortAscMap = SORTASCMAP_INIT;

  while (!AC_IsAtEnd(ac)) {
    const char *s;
    size_t len;

    int rv = AC_GetString(ac, &s, &len, 0);
    // ArgParser already validated `count` and provided a sub-cursor with
    // exactly `count` tokens, so each iteration is guaranteed to succeed.
    RS_ASSERT(rv == AC_OK);

    const char *name = parseAtPrefixedName(s, len, opts->status);
    if (!name) return;

    if (array_len(args->sort_names) >= COLLECT_MAX_SORT_KEYS) {
      QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
        "SORTBY exceeds maximum of %d fields", COLLECT_MAX_SORT_KEYS);
      return;
    }

    array_append(args->sort_names, name);

    size_t dir_idx = array_len(args->sort_names) - 1;
    handleCollectSortDirection(ac, &args->sortAscMap, dir_idx);
  }

  if (array_len(args->sort_names) == 0) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "SORTBY requires at least one sort field");
    return;
  }
}

// ----- LIMIT -----

// Parses: LIMIT <offset> <count>
static void handleCollectLimit(const ArgParser *parser, const void *value, void *user_data) {
  (void)parser;
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectArgs *args = pctx->args;
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
  // Overflow guard for `offset + count`. Redundant given the individual bounds
  // above and a sane MAX, but cheap insurance against future MAX changes.
  if (offset > LLONG_MAX - count) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Invalid LIMIT offset + count value");
    return;
  }

  args->has_limit = true;
  args->limit_offset = offset;
  args->limit_count = count;
}

// ===== Pure parser API =====

bool CollectArgs_Parse(const ReducerOptions *options, CollectArgs *out) {
  RS_ASSERT(options && out);
  out->sortAscMap = SORTASCMAP_INIT;

  if (!RSGlobalConfig.enableUnstableFeatures) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_INVAL,
      "`COLLECT` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off. "
      "Enable it with `CONFIG SET search-enable-unstable-features yes`");
    return false;
  }

  CollectParseCtx pctx = {.args = out, .options = options};

  ArgsCursor *ac = options->args;
  ArgParser *parser = ArgParser_New(ac, NULL);
  if (!parser) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Failed to create argument parser for COLLECT");
    return false;
  }

  ArgsCursor subArgs = {0};

  // FIELDS accepts either `*` or `<num_fields> @field [@field ...]`. The first
  // token is consumed as a string; `handleCollectFields` branches on `*` vs.
  // count.
  const char *fieldsTarget = NULL;
  ArgParser_AddStringV(parser, "FIELDS", "Projected fields",
    &fieldsTarget,
    ARG_OPT_REQUIRED,
    ARG_OPT_POSITION, 1,
    ARG_OPT_CALLBACK, handleCollectFields, &pctx,
    ARG_OPT_END);

  ArgParser_AddSubArgsV(parser, "SORTBY", "In-group sort keys",
    &subArgs, 1, COLLECT_MAX_SORT_TOKENS,
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
    return false;
  }

  if (!result.success) {
    QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Bad arguments for COLLECT", ": %s", ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return false;
  }

  ArgParser_Free(parser);
  return true;
}

void CollectArgs_Free(CollectArgs *args) {
  if (!args) return;
  array_free(args->field_names);
  args->field_names = NULL;
  array_free(args->sort_names);
  args->sort_names = NULL;
}

// ===== Post-parse key resolution (side-effectful: opens RLookupKeys) =====

// Resolves stripped names into a freshly-allocated `arrayof(const RLookupKey *)`.
// Caller owns the result and must `array_free` it.
// On failure, frees the partial array and returns false.
static bool resolveKeyNames(const ReducerOptions *options,
                            arrayof(const char *) names,
                            arrayof(const RLookupKey *) *out_keys) {
  const size_t n = array_len(names);
  *out_keys = array_new(const RLookupKey *, n);
  for (size_t i = 0; i < n; i++) {
    const RLookupKey *key = NULL;
    if (!ReducerOpts_ResolveKey(options, names[i], &key)) {
      array_free(*out_keys);
      *out_keys = NULL;
      return false;
    }
    array_append(*out_keys, key);
  }
  return true;
}

static bool resolveFieldKeys(const ReducerOptions *options, const CollectArgs *args,
                             arrayof(const RLookupKey *) *out_keys) {
  return resolveKeyNames(options, args->field_names, out_keys);
}

static bool resolveSortKeys(const ReducerOptions *options, const CollectArgs *args,
                            arrayof(const RLookupKey *) *out_keys) {
  return resolveKeyNames(options, args->sort_names, out_keys);
}

// ===== Factory =====

Reducer *RDCRCollect_New(const ReducerOptions *options) {
  CollectArgs args = {0};
  if (!CollectArgs_Parse(options, &args)) {
    CollectArgs_Free(&args);
    return NULL;
  }

  Reducer *rbase = NULL;

  if (options->is_local) {
    RS_ASSERT(options->input_key);
    rbase = CollectReducer_CreateLocal(
      options->input_key,
      (const char *const *)args.field_names,
      array_len(args.field_names),
      args.load_all,
      (const char *const *)args.sort_names,
      array_len(args.sort_names),
      args.sortAscMap,
      args.has_limit,
      args.limit_offset,
      args.limit_count
    );
  } else {
    arrayof(const RLookupKey *) field_keys = NULL;
    arrayof(const RLookupKey *) sort_keys = NULL;

    if (!args.load_all) {
      RS_ASSERT(array_len(args.field_names) > 0);
      if (!resolveFieldKeys(options, &args, &field_keys)) {
        CollectArgs_Free(&args);
        return NULL;
      }
    }

    if (args.sort_names && array_len(args.sort_names) > 0) {
      if (!resolveSortKeys(options, &args, &sort_keys)) {
        array_free(field_keys);
        CollectArgs_Free(&args);
        return NULL;
      }
    }

    rbase = CollectReducer_CreateRemote(
      field_keys,
      field_keys ? array_len(field_keys) : 0,
      args.load_all ? options->srclookup : NULL,
      sort_keys,
      sort_keys ? array_len(sort_keys) : 0,
      args.sortAscMap,
      args.has_limit,
      args.limit_offset,
      args.limit_count,
      ReducerOpts_IsInternal(options)
    );
    array_free(field_keys);
    array_free(sort_keys);
  }

  CollectArgs_Free(&args);
  return rbase;
}
