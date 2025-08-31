#include "hybrid_callbacks.h"
#include "config.h"
#include "param.h"
#include "util/arr.h"
#include "result_processor.h"
#include <string.h>
#include <limits.h>

// Helper function to append a sort entry - extracted from original code
static void appendSortEntry(PLN_ArrangeStep *arng, const char *field, int ascending) {
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

    // Replicate exact original logic: lines 260-262
    if (AC_NumRemaining(ac) < 2) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT requires 2 arguments");
        return;
    }

    // Replicate exact original logic: lines 264-270
    long long offset, num;
    if (AC_GetLongLong(ac, &offset, AC_F_GE0) != AC_OK) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT offset must be a non-negative integer");
        return;
    }
    if (AC_GetLongLong(ac, &num, AC_F_GE0) != AC_OK) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT count must be a non-negative integer");
        return;
    }

    // Replicate exact original logic: lines 272-296
    if (ctx->maxResults) {
        if (offset > LLONG_MAX - num) {
            QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT offset + count overflow");
            return;
        }
        long long total = offset + num;
        if (total > *ctx->maxResults) {
            *ctx->maxResults = total;
        }
    }
    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(ctx->plan);
    arng->isLimited = 1;
    arng->offset = offset;
    arng->limit = num;
}

// SORTBY callback - implements EXACT original logic from lines 298-323
void handleSortBy(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;

    // Replicate exact original logic: lines 299-301
    if (AC_NumRemaining(ac) < 1) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "SORTBY requires at least 1 argument");
        return;
    }

    // Replicate exact original logic: lines 303-323
    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(ctx->plan);
    // Handle special case: SORTBY 0 (no sorting)
    if (AC_NumRemaining(ac) == 1) {
        const char *first = AC_GetStringNC(ac, NULL);
        if (strcmp(first, "0") == 0) {
            // SORTBY 0 means no sorting - just return success
            return;
        }
        // Reset cursor to parse normally
        ac->offset = 0;  // Reset cursor to beginning
    }

    // Parse field/direction pairs
    while (!AC_IsAtEnd(ac)) {
        const char *field = AC_GetStringNC(ac, NULL);
        if (!field) {
            QueryError_SetError(status, QUERY_EPARSEARGS, "Missing field name in SORTBY");
            return;
        }

        // Default to ascending
        int ascending = 1;
        
        // Check for optional direction
        if (!AC_IsAtEnd(ac)) {
            const char *direction = NULL;
            if (AC_GetString(ac, &direction, NULL, AC_F_NOADVANCE) == AC_OK) {
                if (strcasecmp(direction, "ASC") == 0) {
                    AC_Advance(ac);  // Consume the direction
                    ascending = 1;
                } else if (strcasecmp(direction, "DESC") == 0) {
                    AC_Advance(ac);  // Consume the direction
                    ascending = 0;
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
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;

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
    REQFLAGS_AddFlags(ctx->reqflags, QEXEC_F_IS_CURSOR);
}

// PARAMS callback - improved with error handling macro and early validation
void handleParams(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;
    dict **destParams = &(ctx->searchopts->params);

    // Early validation checks
    if (*destParams) {
        QueryError_SetError(status, QUERY_EADDARGS, "Multiple PARAMS are not allowed. Parameters can be defined only once");
        return;
    }

    // Parse parameter arguments
    ArgsCursor paramsArgs = {0};
    int rv = AC_GetVarArgs(ac, &paramsArgs);
    if (rv != AC_OK) {
        QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments", " for PARAMS: %s", AC_Strerror(rv));
        return;
    }
    
    // Validate argument count (must be even for key-value pairs)
    if (paramsArgs.argc == 0 || paramsArgs.argc % 2) {
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
    int n = AC_NumArgs(&paramsArgs);
    for (int i = 0; i < n; i += 2) {
        const char *param = AC_GetStringNC(&paramsArgs, NULL);
        const char *value = AC_GetStringNC(&paramsArgs, &value_len);
        
        if (DICT_ERR == Param_DictAdd(params, param, value, value_len, status)) {
            Param_DictFree(params);  // Cleanup on error
            return;
        }
    }
    
    *destParams = params;
}

// DIALECT callback - implements EXACT original logic from lines 341-349
void handleDialect(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;

    // Replicate exact original logic: lines 342-349
    long long dialect;
    if (AC_GetLongLong(ac, &dialect, AC_F_GE1) != AC_OK) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "DIALECT requires a positive integer");
        return;
    }
    ctx->reqConfig->requestConfigParams.dialectVersion = dialect;
    ctx->dialectSpecified = true;
}

// FORMAT callback - implements EXACT original logic from lines 359-366
void handleFormat(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;

    const char *fmt = AC_GetStringNC(ac, NULL);
    if (!fmt) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "FORMAT requires a format argument");
        return;
    }

    if (strcasecmp(fmt, "STRING") == 0) {
        *ctx->reqFlags |= QEXEC_F_SEND_NOFIELDS;
    } else {
        QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown format", " `%s`", fmt);
        return;
    }
}
