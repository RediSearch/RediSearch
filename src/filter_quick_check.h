/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef FILTER_QUICK_CHECK_H__
#define FILTER_QUICK_CHECK_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Internal, opt-in building block; no query execution or indexing path installs it yet.
 * A rejection is a proof of nonmembership. MAYBE always requires the original exact filter.
 * In a graph search, rejection must not by itself prevent traversal through a vector. */
typedef enum {
  FilterQuickCheck_NO = 0,
  FilterQuickCheck_MAYBE = 1,
} FilterQuickCheckResult;

typedef enum {
  FilterQuickCheck_TAG,
  FilterQuickCheck_TEXT,
} FilterQuickCheckTermKind;

/* Both producers must use identical, fully normalized indexed bytes, stable schema field IDs,
 * and kind. TEXT expansion must already be represented in the predicate. Raw query strings,
 * raw document values, and SORTABLE values are not interchangeable with indexed terms. */
typedef struct {
  FilterQuickCheckTermKind kind;
  uint16_t fieldIndex;
  const char* value;
  size_t len;
} FilterQuickCheckTerm;

/* Zero-initialize before adding the terms of one complete document version. Do not publish a
 * partial signature or clear individual bits to remove terms: collisions make that unsafe.
 * This is a runtime representation, not a persistence or cross-version wire format. */
typedef struct {
  uint64_t words[4];
} FilterQuickCheckSignature;

void FilterQuickCheckSignature_Add(FilterQuickCheckSignature* signature,
                                   const FilterQuickCheckTerm* term);

typedef enum {
  FilterQuickCheckNumeric_UNKNOWN,
  FilterQuickCheckNumeric_EMPTY,
  FilterQuickCheckNumeric_BOUNDS,
} FilterQuickCheckNumericState;

/* One field of one complete document version, covering every indexed numeric value.
 * A zero-initialized summary is UNKNOWN, not proof that the field has no values.
 * BOUNDS may enclose gaps in multivalued fields; overlap still requires exact verification. */
typedef struct {
  FilterQuickCheckNumericState state;
  double min;
  double max;
} FilterQuickCheckNumericSummary;

/* Start a complete rebuild; publish only after adding every indexed value of the field.
 * NaN or invalid builder state makes the summary UNKNOWN until explicitly reinitialized. */
void FilterQuickCheckNumericSummary_Init(FilterQuickCheckNumericSummary* summary);
void FilterQuickCheckNumericSummary_Add(FilterQuickCheckNumericSummary* summary, double value);

/* Resolved NUMERIC bounds in the same double representation used by the exact filter.
 * Infinities and inclusive/exclusive endpoints are supported. NaN or reversed bounds
 * compile to UNKNOWN. Do not use numeric encodings of GEO fields as numeric predicates. */
typedef struct {
  uint16_t fieldIndex;
  double min;
  double max;
  bool minInclusive;
  bool maxInclusive;
} FilterQuickCheckNumericRange;

typedef enum {
  FilterQuickCheck_UNKNOWN,
  FilterQuickCheck_TERM,
  FilterQuickCheck_AND,
  FilterQuickCheck_OR,
  /* Negation is deliberately unsupported; even an apparently present term may be a collision. */
  FilterQuickCheck_NOT,
  FilterQuickCheck_NUMERIC,
} FilterQuickCheckPredicateType;

/* A caller builds this necessary-condition expression from its resolved query. Unsupported
 * predicates (geo, wildcard expansions, optional clauses, etc.) must be UNKNOWN. An AND
 * may retain supported necessary conditions; an OR must retain every alternative. */
typedef struct FilterQuickCheckPredicate {
  FilterQuickCheckPredicateType type;
  union {
    FilterQuickCheckTerm term;
    FilterQuickCheckNumericRange numeric;
    struct {
      const struct FilterQuickCheckPredicate* children;
      size_t len;
    } operands;
  };
} FilterQuickCheckPredicate;

typedef struct FilterQuickCheck FilterQuickCheck;

/* Copies the expression into an immutable, bounded program. The input may be freed afterwards.
 * NULL, unsupported nodes, or an expression exceeding the implementation budget are safe:
 * they compile to MAYBE. Returns NULL if the allocator reports failure; evaluation of NULL
 * is also MAYBE. */
FilterQuickCheck* FilterQuickCheck_Compile(const FilterQuickCheckPredicate* predicate);
/* Resolve each numeric field once while compiling. A false result makes that leaf UNKNOWN.
 * The numeric lookup receives the resolved slot instead of the original schema field ID. */
typedef bool (*FilterQuickCheckNumericResolver)(void* context, uint16_t fieldIndex, uint16_t* slot);
FilterQuickCheck* FilterQuickCheck_CompileWithNumericResolver(
    const FilterQuickCheckPredicate* predicate, FilterQuickCheckNumericResolver resolver,
    void* context);
void FilterQuickCheck_Free(FilterQuickCheck* check);

/* Term-signature evaluation only: numeric leaves are MAYBE. Use the context callbacks for
 * numeric checks. Allocation-free and read-only; NULL arguments return MAYBE. */
FilterQuickCheckResult FilterQuickCheck_Evaluate(const FilterQuickCheck* check,
                                                 const FilterQuickCheckSignature* signature);

/* Copy a complete signature for exactly this document version into out. Return false when
 * absent, incomplete, invalidated, or unavailable (including after reload). The provider owns
 * storage and synchronization; it must pin a query-consistent document/schema generation.
 * It must not enumerate all matches, advance a query iterator, or perform blocking disk I/O.
 * Disk-backed providers may serve resident summaries or return false on a cache miss. */
typedef bool (*FilterQuickCheckLookup)(void* context, uint64_t docId,
                                       FilterQuickCheckSignature* out);

/* Same generation, residency and synchronization contract as FilterQuickCheckLookup.
 * EMPTY certifies that this field has no indexed numeric values; false/UNKNOWN means
 * unavailable, including an uncovered field. All lookups must observe the same pinned
 * document/schema generation, including repeated predicates on the same field. */
typedef bool (*FilterQuickCheckNumericLookup)(void* context, uint64_t docId, uint16_t fieldIndex,
                                              FilterQuickCheckNumericSummary* out);

/* The check and lookup state must outlive every callback. Concurrent use requires thread-safe
 * lookups. Either provider may be NULL independently; its leaves then evaluate as MAYBE.
 * No global callback registration or hidden ownership. Zero-initialize unused members. */
typedef struct {
  const FilterQuickCheck* check;
  FilterQuickCheckLookup lookup;
  void* lookupContext;
  FilterQuickCheckNumericLookup numericLookup;
  void* numericLookupContext;
} FilterQuickCheckContext;

/* VecSim-facing callback shape: 0 means reject, 1 means retain for exact verification.
 * This API does not change VecSimQueryParams or install a VecSim consumer. */
int FilterQuickCheck_MayMatch(void* context, size_t label);

/* Input order is preserved, including repeated and non-monotonic IDs. Input and output arrays
 * must be valid for len elements and must not overlap. No allocations or retained pointers. */
void FilterQuickCheck_CheckBatch(const FilterQuickCheckContext* context, const uint64_t* docIds,
                                 size_t len, FilterQuickCheckResult* results);

#ifdef __cplusplus
}
#endif
#endif
