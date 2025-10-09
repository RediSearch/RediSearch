/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "hybrid_optional_args.h"
#include "util/arg_parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Callback handlers for common arguments in hybrid queries
 * These functions are used by the ArgParser framework to handle specific arguments
 */

/**
 * LIMIT callback - handles LIMIT offset count
 * Sets up PLN_ArrangeStep with limit configuration
 */
void handleLimit(ArgParser *parser, const void *value, void *user_data);

/**
 * SORTBY callback - handles SORTBY field [ASC|DESC] [field [ASC|DESC] ...]
 * Sets up PLN_ArrangeStep with sorting configuration
 */
void handleSortBy(ArgParser *parser, const void *value, void *user_data);

/**
 * WITHCURSOR callback - handles WITHCURSOR [COUNT count] [MAXIDLE maxidle]
 * Configures cursor settings and sets QEXEC_F_IS_CURSOR flag
 */
void handleWithCursor(ArgParser *parser, const void *value, void *user_data);

/**
 * PARAMS callback - handles PARAMS param value [param value ...]
 * Creates parameter dictionary for query parameterization
 */
void handleParams(ArgParser *parser, const void *value, void *user_data);

/**
 * DIALECT callback - handles DIALECT dialect
 * Sets the query dialect version
 */
void handleDialect(ArgParser *parser, const void *value, void *user_data);

/**
 * FORMAT callback - handles FORMAT format
 * Sets output format flags
 */
void handleFormat(ArgParser *parser, const void *value, void *user_data);

/**
 * COMBINE callback - handles COMBINE [RRF [K k] [WINDOW window]] | [LINEAR weight1 weight2 ...]
 * Configures hybrid scoring method and parameters
 */
void handleCombine(ArgParser *parser, const void *value, void *user_data);

/**
 * _NUM_SSTRING callback - handles _NUM_SSTRING
 * Sets QEXEC_F_TYPED flag to preserve numeric types in results
 */
void handleNumSString(ArgParser *parser, const void *value, void *user_data);

/**
 * GROUPBY callback - handles GROUPBY nproperties property [property ...] [REDUCE function nargs arg [arg ...] [AS alias]] [...]
 * Sets up PLN_GroupStep with grouping properties and reducers
 */
void handleGroupby(ArgParser *parser, const void *value, void *user_data);

/**
 * APPLY callback - handles APPLY expression [AS alias]
 * Sets up PLN_MapFilterStep with APPLY type for expression evaluation
 */
void handleApply(ArgParser *parser, const void *value, void *user_data);

/**
 * LOAD callback - handles LOAD nfields field [field ...] | LOAD *
 * Sets up PLN_LoadStep to load specified fields or all fields
 */
void handleLoad(ArgParser *parser, const void *value, void *user_data);

/**
 * FILTER callback - handles FILTER expression
 * Sets up PLN_MapFilterStep with FILTER type for result filtering
 */
void handleFilter(ArgParser *parser, const void *value, void *user_data);

/**
 * TIMEOUT callback - handles TIMEOUT timeout
 * Sets the query timeout in milliseconds
 */
void handleTimeout(ArgParser *parser, const void *value, void *user_data);

/**
 * WITHSCORES callback - handles WITHSCORES
 */
void handleWithScores(ArgParser *parser, const void *value, void *user_data);

/**
 * EXPLAINSCORE callback - handles EXPLAINSCORE
 */
void handleExplainScore(ArgParser *parser, const void *value, void *user_data);

/**
 * _INDEX_PREFIXES callback - handles _INDEX_PREFIXES prefix [prefix ...]
 * sets index prefix offset for later validation if needed
 */
void handleIndexPrefixes(ArgParser *parser, const void *value, void *user_data);

#ifdef __cplusplus
}
#endif
