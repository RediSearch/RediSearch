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

#ifdef __cplusplus
}
#endif
