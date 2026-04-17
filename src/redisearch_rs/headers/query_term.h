#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * A single term being evaluated at query time.
 *
 * Each term carries scoring metadata ([`idf`](RSQueryTerm::idf),
 * [`bm25_idf`](RSQueryTerm::bm25_idf)) and a unique
 * [`id`](RSQueryTerm::id) assigned during query parsing.
 *
 */
typedef struct RSQueryTerm RSQueryTerm;

/**
 * Flags associated with query tokens and terms.
 *
 * Extension-set token flags — up to 31 bits are available for extensions,
 * since 1 bit is reserved for the `expanded` flag on [`RSToken`].
 *
 * [`RSToken`]: https://github.com/RediSearch/RediSearch
 */
typedef uint32_t RSTokenFlags;
