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
#include "util/misc.h"
#include "spec.h"
#include "config.h"
#include "reducers_rs.h"
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define COLLECT_MAX_SORT_KEYS SORTASCMAP_MAXFIELDS
#define COLLECT_MAX_SORT_TOKENS (COLLECT_MAX_SORT_KEYS * 2)  // each key may have a direction
#define COLLECT_MAX_FIELD_ARGS  (SPEC_MAX_FIELDS + 1)
#define SORT_DIR_ASC "ASC"
#define SORT_DIR_DESC "DESC"

// Temporary storage for parsed COLLECT arguments. The data is handed off to
// Rust via the appropriate `CollectReducer_Create*` factory once parsing
// succeeds. Remote reducers keep `RLookupKey *`; local reducers keep raw names
// because they cannot resolve fields through remote lookup tables.
//
// Exactly one population pattern is used per parse, selected by
// `options->is_local`:
//   - is_local == true  : `field_names`, `sort_names`, `input_key` are set;
//                          `field_keys`, `sort_keys`, `load_all` are not.
//   - is_local == false : `field_keys`, `sort_keys`, `load_all` are set;
//                          `field_names`, `sort_names`, `input_key` are not.
// `sortAscMap` and the `limit_*` triple are shared across both modes.
typedef struct {
  arrayof(const RLookupKey *) field_keys;   // remote-only
  arrayof(const RLookupKey *) sort_keys;    // remote-only
  // Coord-mode names alias `options->args` and omit the leading `@`.
  arrayof(const char *) field_names;        // local-only
  arrayof(const char *) sort_names;         // local-only
  const RLookupKey *input_key;              // local-only

  bool load_all;                            // shared
  uint64_t sortAscMap;                      // shared

  bool has_limit;                           // shared
  uint64_t limit_offset;                    // shared
  uint64_t limit_count;                     // shared
} CollectParseData;

typedef struct {
  CollectParseData *data;
  const ReducerOptions *options;
} CollectParseCtx;

static void CollectParseData_Free(CollectParseData *data) {
  array_free(data->field_keys);
  array_free(data->sort_keys);
  array_free(data->field_names);
  array_free(data->sort_names);
}

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

// ===== ArgParser callbacks =====

// ----- FIELDS -----

// Drains `<num_fields>` `@field` tokens into `data->field_names`.
// The local reducer strips `@` and later matches against map keys carried by
// the remote payload.
static void handleCollectFieldsLocal(ArgsCursor *ac, CollectParseData *data,
                                     const ReducerOptions *opts) {
  data->field_names = array_new(const char *, AC_NumRemaining(ac));
  while (!AC_IsAtEnd(ac)) {
    const char *s;
    size_t len;
    int rv = AC_GetString(ac, &s, &len, 0);
    // The slice is sized exactly to `<num_fields>`, so every iteration succeeds.
    RS_ASSERT(rv == AC_OK);
    // `s` aliases the original argv and is NUL-terminated.
    const char *name = ExtractKeyName(s, &len, opts->status, opts->strictPrefix, "FIELDS");
    if (!name) return;
    array_append(data->field_names, name);
  }
}

// Drains `<num_fields>` `@field` tokens into `data->field_keys`. Remote (shard)
// mode resolves each field against the source lookup via `ReducerOpts_GetKey`.
static void handleCollectFieldsRemote(ArgsCursor *ac, CollectParseData *data,
                                      const ReducerOptions *opts) {
  ReducerOptions sub_opts = *opts;
  sub_opts.args = ac;
  sub_opts.name = "FIELDS";

  data->field_keys = array_new(const RLookupKey *, AC_NumRemaining(ac));
  while (!AC_IsAtEnd(ac)) {
    const RLookupKey *key = NULL;
    if (!ReducerOpts_GetKey(&sub_opts, &key)) {
      return;
    }
    array_append(data->field_keys, key);
  }
}

// Parses: FIELDS ( * | <num_fields> @field [@field ...] )
//   <num_fields>: 1..COLLECT_MAX_FIELD_ARGS
//
// The first token after `FIELDS` is consumed by `ArgParser_AddStringV` and
// passed in via `value`; the remainder is read directly from the parser's
// underlying cursor. On load-all the callback returns immediately; otherwise
// it slices `<num_fields>` tokens and dispatches to the per-mode drainer.
static void handleCollectFields(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectParseData *data = pctx->data;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = parser->cursor;
  const char *firstArg = *(const char **)value;

  // Load-all branch: `*` consumes nothing else from FIELDS. If the next token
  // begins with `@` or `$` it's a stray field reference and we reject it here;
  // other tokens (SORTBY, LIMIT, ...) are left for the outer parser to dispatch.
  if (strcmp(firstArg, "*") == 0) {
    data->load_all = true;
    return;
  }

  // Count branch: validate <num_fields> then carve a slice of that size and
  // hand it off to the mode-specific drain. `firstArg` was already extracted
  // by `ArgParser_AddStringV` and is NUL-terminated, so parse it directly.
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

  if (opts->is_local) {
    handleCollectFieldsLocal(&sub, data, opts);
  } else {
    handleCollectFieldsRemote(&sub, data, opts);
  }
}

// Parses: SORTBY nargs <@field [ASC|DESC]> [<@field [ASC|DESC]> ...]
//   nargs: 1..COLLECT_MAX_SORT_KEYS*2
//   Direction defaults to ASC when omitted.
//
static void handleCollectSortDirection(ArgsCursor *ac, uint64_t *sortAscMap, size_t dir_idx) {
  if (AC_AdvanceIfMatch(ac, SORT_DIR_ASC)) {
    // ASC is the default; nothing to do.
  } else if (AC_AdvanceIfMatch(ac, SORT_DIR_DESC)) {
    SORTASCMAP_SETDESC(*sortAscMap, dir_idx);
  }
}

// The local reducer stores raw names that match remote payload map keys.
static void handleCollectSortByLocal(ArgParser *parser, const void *value, void *user_data) {
  (void)parser;
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectParseData *data = pctx->data;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  data->sort_names = array_new(const char *, 4);
  data->sortAscMap = SORTASCMAP_INIT;

  while (!AC_IsAtEnd(ac)) {
    const char *s;
    size_t len;

    int rv = AC_GetString(ac, &s, &len, 0);
    // ArgParser already validated `count` and provided a sub-cursor with
    // exactly `count` so each iteration is guaranteed to succeed.
    RS_ASSERT(rv == AC_OK);

    const char *name = parseAtPrefixedName(s, len, opts->status);
    if (!name) return;

    if (array_len(data->sort_names) >= COLLECT_MAX_SORT_KEYS) {
      QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
        "SORTBY exceeds maximum of %d fields", COLLECT_MAX_SORT_KEYS);
      return;
    }

    // Store the raw name alias, then expose the optional ASC/DESC token.
    array_append(data->sort_names, name);

    size_t dir_idx = array_len(data->sort_names) - 1;
    handleCollectSortDirection(ac, &data->sortAscMap, dir_idx);
  }

  if (array_len(data->sort_names) == 0) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "SORTBY requires at least one sort field");
    return;
  }
}

// Shards resolve keys against the source lookup.
static void handleCollectSortByRemote(ArgParser *parser, const void *value, void *user_data) {
  (void)parser;
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectParseData *data = pctx->data;
  const ReducerOptions *opts = pctx->options;
  ArgsCursor *ac = (ArgsCursor *)value;

  data->sort_keys = array_new(const RLookupKey *, 4);
  data->sortAscMap = SORTASCMAP_INIT;

  ReducerOptions key_opts = *opts;
  key_opts.args = ac;
  key_opts.name = "SORTBY";

  while (!AC_IsAtEnd(ac)) {
    // Peek-only: `ReducerOpts_GetKey` below consumes this arg via its own
    // `AC_GetString`. Pass AC_F_NOADVANCE so we don't double-advance the
    // cursor. The loop guard makes AC_ERR_NOARG unreachable.
    const char *s;
    size_t len;
    int rv = AC_GetString(ac, &s, &len, AC_F_NOADVANCE);
    RS_ASSERT(rv == AC_OK);
    if (!parseAtPrefixedName(s, len, opts->status)) return;

    if (array_len(data->sort_keys) >= COLLECT_MAX_SORT_KEYS) {
      QueryError_SetWithoutUserDataFmt(opts->status, QUERY_ERROR_CODE_LIMIT,
        "SORTBY exceeds maximum of %d fields", COLLECT_MAX_SORT_KEYS);
      return;
    }

    const RLookupKey *key = NULL;
    if (!ReducerOpts_GetKey(&key_opts, &key)) {
      return;
    }
    array_append(data->sort_keys, key);

    size_t dir_idx = array_len(data->sort_keys) - 1;
    handleCollectSortDirection(ac, &data->sortAscMap, dir_idx);
  }

  if (array_len(data->sort_keys) == 0) {
    QueryError_SetError(opts->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "SORTBY requires at least one sort field");
    return;
  }
}

// Parses: LIMIT <offset> <count>
//   Both values must be non-negative integers <= MAX_AGGREGATE_REQUEST_RESULTS.
static void handleCollectLimit(ArgParser *parser, const void *value, void *user_data) {
  CollectParseCtx *pctx = (CollectParseCtx *)user_data;
  CollectParseData *data = pctx->data;
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

  data->has_limit = true;
  data->limit_offset = offset;
  data->limit_count = count;
}

// ===== Factory =====

Reducer *RDCRCollect_New(const ReducerOptions *options) {
  if (!RSGlobalConfig.enableUnstableFeatures) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_INVAL,
      "`COLLECT` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off. "
      "Enable it with `CONFIG SET search-enable-unstable-features yes`");
    return NULL;
  }

  CollectParseData data = {0};
  data.sortAscMap = SORTASCMAP_INIT;

  CollectParseCtx pctx = {.data = &data, .options = options};

  ArgsCursor *ac = options->args;
  ArgParser *parser = ArgParser_New(ac, NULL);
  if (!parser) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Failed to create argument parser for COLLECT");
    return NULL;
  }

  ArgsCursor subArgs = {0};
  ArgCallback handleSortBy =
    options->is_local ? handleCollectSortByLocal : handleCollectSortByRemote;

  // FIELDS accepts either `*` or `<num_fields> @field [@field ...]`. The first
  // token is consumed as a string; `handleCollectFields` branches on `*` vs.
  // count and dispatches to the mode-specific drain.
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
    ARG_OPT_CALLBACK, handleSortBy, &pctx,
    ARG_OPT_END);

  ArgParser_AddSubArgsV(parser, "LIMIT", "Per-group limit",
    &subArgs, 2, 2,
    ARG_OPT_OPTIONAL,
    ARG_OPT_CALLBACK, handleCollectLimit, &pctx,
    ARG_OPT_END);

  ArgParseResult result = ArgParser_Parse(parser);

  if (QueryError_HasError(options->status)) {
    ArgParser_Free(parser);
    CollectParseData_Free(&data);
    return NULL;
  }

  if (!result.success) {
    QueryError_SetWithUserDataFmt(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "Bad arguments for COLLECT", ": %s", ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    CollectParseData_Free(&data);
    return NULL;
  }

  ArgParser_Free(parser);

  if (options->is_local) {
    if (!options->input_key) {
      QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
        "COLLECT input key was not provided");
      CollectParseData_Free(&data);
      return NULL;
    }
    data.input_key = options->input_key;
  }

  if (data.load_all) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_PARSE_ARGS,
      "COLLECT does not yet support `*` in FIELDS");
    CollectParseData_Free(&data);
    return NULL;
  }

  // Rust copies the mode-specific parsed data and wires the vtable.
  Reducer *rbase;
  if (options->is_local) {
    rbase = CollectReducer_CreateLocal(
      data.input_key,
      (const char *const *)data.field_names,
      data.field_names ? array_len(data.field_names) : 0,
      data.load_all,
      (const char *const *)data.sort_names,
      data.sort_names ? array_len(data.sort_names) : 0,
      data.sortAscMap,
      data.has_limit,
      data.limit_offset,
      data.limit_count
    );
  } else {
    rbase = CollectReducer_CreateRemote(
      data.field_keys,
      data.field_keys ? array_len(data.field_keys) : 0,
      data.load_all,
      data.sort_keys,
      data.sort_keys ? array_len(data.sort_keys) : 0,
      data.sortAscMap,
      data.has_limit,
      data.limit_offset,
      data.limit_count,
      ReducerOpts_IsInternal(options)
    );
  }

  // Free the C arrays; Rust has copied the pointer values.
  CollectParseData_Free(&data);

  return rbase;
}
