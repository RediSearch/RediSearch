/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "hybrid_callbacks.h"
#include "config.h"
#include "param.h"
#include "util/arr.h"
#include "result_processor.h"
#include <string.h>
#include <limits.h>

// Helper function to append a sort entry - extracted from original code
static void appendSortEntry(PLN_ArrangeStep *arng, const char *field, bool ascending) {
    // Initialize sortKeys array if not already done
    if (!arng->sortKeys) {
        arng->sortKeys = array_new(const char*, 1);
        arng->sortAscMap = SORTASCMAP_INIT;
    }

    // Add the field to the sortKeys array
    array_append(arng->sortKeys, field);

    // Set the ascending/descending bit in the sortAscMap
    size_t index = array_len(arng->sortKeys) - 1;
    if (ascending) {
        SORTASCMAP_SETASC(arng->sortAscMap, index);
    } else {
        SORTASCMAP_SETDESC(arng->sortAscMap, index);
    }
}

// LIMIT callback - implements EXACT original logic from lines 259-296
void handleLimit(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;
    ctx->specifiedArgs |= SPECIFIED_ARG_LIMIT;

    if (AC_NumRemaining(ac) < 2) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT requires 2 arguments");
        return;
    }

    uint64_t offset = 0;
    uint64_t num = 0;
    if (AC_GetU64(ac, &offset, AC_F_GE0) != AC_OK) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT offset must be a non-negative integer");
        return;
    }
    if (AC_GetU64(ac, &num, AC_F_GE0) != AC_OK) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT count must be a non-negative integer");
        return;
    }

    if (num == 0 && offset != 0) {
        QueryError_SetError(status, QUERY_ELIMIT, "The `offset` of the LIMIT must be 0 when `num` is 0");
        return;
    }

    if (num == 0) {
        // LIMIT 0 0 - only count
        REQFLAGS_AddFlags(ctx->reqFlags, QEXEC_F_NOROWS);
        REQFLAGS_AddFlags(ctx->reqFlags, QEXEC_F_SEND_NOFIELDS);
        // TODO: unify if when req holds only maxResults according to the query type.
        //(SEARCH / AGGREGATE)
    } else if (num > *ctx->maxResults) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ELIMIT, "LIMIT exceeds maximum of %llu",
                             *ctx->maxResults);
        return;
    } else if (offset > LLONG_MAX - num) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT offset + count overflow");
        return;
    }

    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(ctx->plan);
    arng->isLimited = 1;
    arng->offset = offset;
    arng->limit = num;
}

#define ASC_BY_DEFAULT true

void handleSortBy(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;
    ctx->specifiedArgs |= SPECIFIED_ARG_SORTBY;

    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(ctx->plan);
    // Parse field/direction pairs
    while (!AC_IsAtEnd(ac)) {
        const char *field = AC_GetStringNC(ac, NULL);
        if (!field) {
            QueryError_SetError(status, QUERY_EPARSEARGS, "Missing field name in SORTBY");
            return;
        }

        // Remove '@' prefix if present (same logic as parseSortby)
        if (*field == '@') {
            field++;  // Skip the '@' prefix
        }

        // Default to ascending
        bool ascending = ASC_BY_DEFAULT;

        // Check for optional direction
        if (!AC_IsAtEnd(ac)) {
            const char *direction = NULL;
            if (AC_GetString(ac, &direction, NULL, AC_F_NOADVANCE) == AC_OK) {
                if (strcasecmp(direction, "ASC") == 0) {
                    AC_Advance(ac);  // Consume the direction
                    ascending = true;
                } else if (strcasecmp(direction, "DESC") == 0) {
                    AC_Advance(ac);  // Consume the direction
                    ascending = false;
                }
                // If it's not ASC/DESC, leave it for the next field
            }
        }

        appendSortEntry(arng, field, ascending);
    }

    return;
}

// WITHCURSOR callback - parses cursor settings directly
void handleWithCursor(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = parser->cursor;
    QueryError *status = ctx->status;
    ctx->specifiedArgs |= SPECIFIED_ARG_WITHCURSOR;

    // Parse cursor settings inline (merged from parseCursorSettings)
    ACArgSpec specs[] = {{.name = "MAXIDLE",
                          .type = AC_ARGTYPE_UINT,
                          .target = &ctx->cursorConfig->maxIdle,
                          .intflags = AC_F_GE1},
                         {.name = "COUNT",
                          .type = AC_ARGTYPE_UINT,
                          .target = &ctx->cursorConfig->chunkSize,
                          .intflags = AC_F_GE1},
                         {NULL}};

    int rv;
    ACArgSpec *errArg = NULL;
    if ((rv = AC_ParseArgSpec(ac, specs, &errArg)) != AC_OK && rv != AC_ERR_ENOENT) {
        QERR_MKBADARGS_AC(status, errArg->name, rv);
        return;
    }

    if (ctx->cursorConfig->maxIdle == 0 || ctx->cursorConfig->maxIdle > RSGlobalConfig.cursorMaxIdle) {
        ctx->cursorConfig->maxIdle = RSGlobalConfig.cursorMaxIdle;
    }
}

// PARAMS callback - improved with error handling macro and early validation
void handleParams(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *paramsArgs = (ArgsCursor*)value;
    QueryError *status = ctx->status;
    ctx->specifiedArgs |= SPECIFIED_ARG_PARAMS;

    // Early validation checks
    if (ctx->searchopts->params) {
        QueryError_SetError(status, QUERY_EADDARGS, "Multiple PARAMS are not allowed. Parameters can be defined only once");
        return;
    }
    // Validate argument count (must be even for key-value pairs)
    if (paramsArgs->argc == 0 || paramsArgs->argc % 2) {
        QueryError_SetError(status, QUERY_EADDARGS, "Parameters must be specified in PARAM VALUE pairs");
        return;
    }

    // Create parameter dictionary and populate
    dict *params = Param_DictCreate();
    if (!params) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create parameter dictionary");
        return;
    }

    size_t value_len;
    int n = AC_NumArgs(paramsArgs);
    for (int i = 0; i < n; i += 2) {
        const char *param = AC_GetStringNC(paramsArgs, NULL);
        const char *value = AC_GetStringNC(paramsArgs, &value_len);
        if (DICT_ERR == Param_DictAdd(params, param, value, value_len, status)) {
            Param_DictFree(params);  // Cleanup on error
            return;
        }
    }

    ctx->searchopts->params = params;
}

// DIALECT callback - implements EXACT original logic from lines 341-349
void handleDialect(ArgParser *parser, const void *value, void *user_data) {
  HybridParseContext *ctx = (HybridParseContext*)user_data;
  QueryError *status = ctx->status;
  QueryError_SetWithoutUserDataFmt(status, QUERY_EPARSEARGS, DIALECT_ERROR_MSG);
}

// FORMAT callback - implements EXACT original logic from lines 359-366
void handleFormat(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_FORMAT;
    const char *format = *(char**)value;
    QueryError *status = ctx->status;
    if (strcasecmp(format, "STRING") == 0) {
        *ctx->reqFlags &= ~QEXEC_FORMAT_EXPAND;
    } else if (strcasecmp(format, "EXPAND") == 0) {
        *ctx->reqFlags |= QEXEC_FORMAT_EXPAND;
    }
}

// Helper function to ensure extended mode for aggregation operations
static int ensureExtendedMode(uint32_t *reqflags, const char *name, QueryError *status) {
    if (*reqflags & QEXEC_F_IS_SEARCH) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_EINVAL,
                               "option `%s` is mutually exclusive with simple (i.e. search) options",
                               name);
        return 0;
    }
    REQFLAGS_AddFlags(reqflags, QEXEC_F_IS_AGGREGATE);
    return 1;
}

// GROUPBY callback - implements EXACT original logic from parseGroupby
void handleGroupby(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_GROUPBY;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;

    if (!ensureExtendedMode(ctx->reqFlags, "GROUPBY", status)) {
        return;
    }

    const long long nproperties = AC_NumRemaining(ac);
    if (nproperties <= 0) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments for GROUPBY: Expected at least one property", ", got: %d", nproperties);
        return;
    }
    const char **properties = array_newlen(const char *, nproperties);
    for (size_t i = 0; i < nproperties; ++i) {
        const char *property;
        size_t propertyLen;
        int rv = AC_GetString(ac, &property, &propertyLen, 0);
        if (rv != AC_OK) {
            const size_t oneBasedIndex = i + 1;
            QueryError_SetWithoutUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments for GROUPBY: Failed to parse property %zu, %s", oneBasedIndex, AC_Strerror(rv));
            array_free(properties);
            return;
        }
        if (property[0] != '@') {
            QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments for GROUPBY", ": Unknown property `%s`. Did you mean `@%s`?",
                               property, property);
            array_free(properties);
            return;
        }
        properties[i] = property;
    }

    // Number of fields.. now let's see the reducers
    StrongRef properties_ref = StrongRef_New((void *)properties, (RefManager_Free)array_free);
    PLN_GroupStep *gstp = PLNGroupStep_New(properties_ref);
    AGPLN_AddStep(ctx->plan, &gstp->base);

    ArgsCursor *reduce = parser->cursor;
    while (AC_AdvanceIfMatch(reduce, "REDUCE")) {
        const char *name;
        const int rv = AC_GetString(reduce, &name, NULL, 0);
        if (rv != AC_OK) {
            QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments for REDUCE", ": %s", AC_Strerror(rv));
            return;
        }
        if (PLNGroupStep_AddReducer(gstp, name, reduce, status) != REDISMODULE_OK) {
            return;
        }
    }
}

// APPLY callback - implements EXACT original logic from handleApplyOrFilter with isApply=1
void handleApply(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_APPLY;
    ArgsCursor *ac = parser->cursor;  // Get remaining args from parser cursor
    QueryError *status = ctx->status;

    // Get the expression from the string value
    const char *expr = *(const char**)value;
    size_t exprLen = strlen(expr);

    HiddenString* expression = NewHiddenString(expr, exprLen, false);
    PLN_MapFilterStep *stp = PLNMapFilterStep_New(expression, PLN_T_APPLY);
    HiddenString_Free(expression, false);
    AGPLN_AddStep(ctx->plan, &stp->base);

    // Check for optional AS alias in remaining arguments
    if (AC_AdvanceIfMatch(ac, "AS")) {
        const char *alias;
        size_t aliasLen;
        if (AC_GetString(ac, &alias, &aliasLen, 0) != AC_OK) {
            QueryError_SetError(status, QUERY_EPARSEARGS, "AS needs argument");
            goto error;
        }
        stp->base.alias = rm_strndup(alias, aliasLen);
    } else {
        stp->base.alias = rm_strndup(expr, exprLen);
    }
    return;

error:
    if (stp) {
        AGPLN_PopStep(&stp->base);
        stp->base.dtor(&stp->base);
    }
}

// LOAD callback - implements EXACT original logic from handleLoad
void handleLoad(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_LOAD;
    ArgsCursor *ac = parser->cursor;  // Get remaining args from parser cursor
    QueryError *status = ctx->status;

    // Get the first argument from the string value
    const char *firstArg = *(const char**)value;
    ArgsCursor loadfields = {0};
    if (strcmp(firstArg, "*") == 0) {
        // Successfully got a '*', load all fields
        REQFLAGS_AddFlags(ctx->reqFlags, QEXEC_AGG_LOAD_ALL);
    } else {
        // Try to parse the first argument as a number of fields to load
        char *end = NULL;
        long long numFields = strtoll(firstArg, &end, 10);
        if (*end != '\0' || numFields <= 0) {
            QueryError_SetError(status, QUERY_EPARSEARGS, "Bad arguments for LOAD: Expected number of fields or `*`");
            return;
        }
        // Successfully got a number, slice that many fields
        if (AC_GetSlice(ac, &loadfields, numFields) != AC_OK) {
            QueryError_SetError(status, QUERY_EPARSEARGS, "Not enough arguments for LOAD");
            return;
        }
    }

    PLN_LoadStep *lstp = rm_calloc(1, sizeof(*lstp));
    lstp->base.type = PLN_T_LOAD;
    lstp->base.dtor = loadDtor;
    if (loadfields.argc > 0) {
        lstp->args = loadfields;
        lstp->keys = rm_calloc(loadfields.argc, sizeof(*lstp->keys));
    }

    if (*ctx->reqFlags & QEXEC_AGG_LOAD_ALL) {
        lstp->base.flags |= PLN_F_LOAD_ALL;
    }

    AGPLN_AddStep(ctx->plan, &lstp->base);
}

// FILTER callback - implements EXACT original logic from handleApplyOrFilter with isApply=0
void handleFilter(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    QueryError *status = ctx->status;
    ctx->specifiedArgs |= SPECIFIED_ARG_FILTER;

    // Get the expression from the string value
    const char *expr = *(const char**)value;
    size_t exprLen = strlen(expr);

    HiddenString* expression = NewHiddenString(expr, exprLen, false);
    PLN_MapFilterStep *stp = PLNMapFilterStep_New(expression, PLN_T_FILTER);
    HiddenString_Free(expression, false);
    AGPLN_AddStep(ctx->plan, &stp->base);
}

// TIMEOUT callback - implements EXACT original logic from handleTimeout
void handleTimeout(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_TIMEOUT;
}

// WITHSCORES callback - implements EXACT original logic from handleWithScores
void handleWithScores(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_WITHSCORES;
}

// EXPLAINSCORE callback - implements EXACT original logic from handleExplainScore
void handleExplainScore(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_EXPLAINSCORE;
}

// _NUM_SSTRING callback - implements EXACT original logic from handleNumSString
void handleNumSString(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ctx->specifiedArgs |= SPECIFIED_ARG_NUM_SSTRING;
}

// _INDEX_PREFIXES callback - implements EXACT original logic from handleIndexPrefixes
void handleIndexPrefixes(ArgParser *parser, const void *value, void *user_data) {
  HybridParseContext *ctx = (HybridParseContext*)user_data;
  ArgsCursor *paramsArgs = (ArgsCursor*)value;
  QueryError *status = ctx->status;
  while (!AC_IsAtEnd(paramsArgs)) {
    const char *prefix;
    if (AC_GetString(paramsArgs, &prefix, NULL, 0) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Bad arguments for _INDEX_PREFIXES");
      return;
    }
    array_ensure_append_1(*ctx->prefixes, prefix);
  }
}
