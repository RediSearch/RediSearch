/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "filter_quick_check.h"

#include "fnv_ffi.h"
#include "rmalloc.h"

#include <math.h>
#include <string.h>

/* Both compile recursion and per-candidate work are bounded independently of query size. */
#define QUICK_CHECK_MAX_NODES 256
#define QUICK_CHECK_MAX_DEPTH 32

typedef struct {
  FilterQuickCheckPredicateType type;
  size_t end;
  union {
    FilterQuickCheckSignature mask;
    FilterQuickCheckNumericRange numeric;
  };
} QuickCheckInstruction;

struct FilterQuickCheck {
  size_t len;
  bool needsSignature;
  QuickCheckInstruction instructions[];
};

static bool validNumericBounds(const FilterQuickCheckNumericSummary* summary) {
  return summary->state == FilterQuickCheckNumeric_BOUNDS && !isnan(summary->min) &&
         !isnan(summary->max) && summary->min <= summary->max;
}

void FilterQuickCheckNumericSummary_Init(FilterQuickCheckNumericSummary* summary) {
  if (summary) {
    *summary = (FilterQuickCheckNumericSummary){.state = FilterQuickCheckNumeric_EMPTY};
  }
}

void FilterQuickCheckNumericSummary_Add(FilterQuickCheckNumericSummary* summary, double value) {
  if (!summary) {
    return;
  }
  if (isnan(value) ||
      (summary->state != FilterQuickCheckNumeric_EMPTY && !validNumericBounds(summary))) {
    summary->state = FilterQuickCheckNumeric_UNKNOWN;
  } else if (summary->state == FilterQuickCheckNumeric_EMPTY) {
    *summary = (FilterQuickCheckNumericSummary){
        .state = FilterQuickCheckNumeric_BOUNDS, .min = value, .max = value};
  } else {
    if (value < summary->min) {
      summary->min = value;
    }
    if (value > summary->max) {
      summary->max = value;
    }
  }
}

static bool numericMayMatch(const FilterQuickCheckNumericRange* range,
                            const FilterQuickCheckContext* context, uint64_t docId) {
  FilterQuickCheckNumericSummary summary = {0};
  if (!context || !context->numericLookup ||
      !context->numericLookup(context->numericLookupContext, docId, range->fieldIndex, &summary)) {
    return true;
  }
  if (summary.state == FilterQuickCheckNumeric_EMPTY) {
    return false;
  }
  if (!validNumericBounds(&summary)) {
    return true;
  }
  if (range->min == range->max && (!range->minInclusive || !range->maxInclusive)) {
    return false;
  }
  return (summary.max > range->min || (range->minInclusive && summary.max == range->min)) &&
         (summary.min < range->max || (range->maxInclusive && summary.min == range->max));
}

static bool validTerm(const FilterQuickCheckTerm* term) {
  return term && term->value &&
         (term->kind == FilterQuickCheck_TAG || term->kind == FilterQuickCheck_TEXT);
}

void FilterQuickCheckSignature_Add(FilterQuickCheckSignature* signature,
                                   const FilterQuickCheckTerm* term) {
  if (!signature) {
    return;
  }
  if (!validTerm(term)) {
    /* Invalid producer input must never silently omit a term and cause a false rejection. */
    memset(signature, 0xff, sizeof(*signature));
    return;
  }
  const unsigned char domain[] = {(unsigned char)term->kind, (unsigned char)term->fieldIndex,
                                  (unsigned char)(term->fieldIndex >> 8)};
  uint64_t hash = fnv_64a_buf(domain, sizeof(domain), UINT64_C(14695981039346656037));
  hash = fnv_64a_buf(term->value, term->len, hash);
  /* Avalanche the field-qualified hash before selecting one bit in each independent word.
   * Collisions only increase exact-filter work; they must never change the final answer. */
  hash ^= hash >> 30;
  hash *= UINT64_C(0xbf58476d1ce4e5b9);
  hash ^= hash >> 27;
  hash *= UINT64_C(0x94d049bb133111eb);
  hash ^= hash >> 31;
  for (size_t i = 0; i < 4; ++i) {
    signature->words[i] |= UINT64_C(1) << ((hash >> (i * 16)) & 63);
  }
}

static bool compile(const FilterQuickCheckPredicate* predicate, QuickCheckInstruction* program,
                    size_t* len, size_t depth, FilterQuickCheckNumericResolver resolver,
                    void* context) {
  if (*len == QUICK_CHECK_MAX_NODES || depth == QUICK_CHECK_MAX_DEPTH) {
    return false;
  }
  const size_t position = (*len)++;
  QuickCheckInstruction instruction = {.type = FilterQuickCheck_UNKNOWN};
  if (predicate) {
    switch (predicate->type) {
      case FilterQuickCheck_TERM:
        if (validTerm(&predicate->term)) {
          instruction.type = FilterQuickCheck_TERM;
          FilterQuickCheckSignature_Add(&instruction.mask, &predicate->term);
        }
        break;
      case FilterQuickCheck_NUMERIC:
        if (!isnan(predicate->numeric.min) && !isnan(predicate->numeric.max) &&
            predicate->numeric.min <= predicate->numeric.max) {
          instruction.numeric = predicate->numeric;
          if (!resolver ||
              resolver(context, predicate->numeric.fieldIndex, &instruction.numeric.fieldIndex)) {
            instruction.type = FilterQuickCheck_NUMERIC;
          }
        }
        break;
      case FilterQuickCheck_AND:
      case FilterQuickCheck_OR:
        if (predicate->operands.len > QUICK_CHECK_MAX_NODES ||
            (predicate->operands.len && !predicate->operands.children)) {
          return false;
        }
        instruction.type = predicate->type;
        for (size_t i = 0; i < predicate->operands.len; ++i) {
          if (!compile(&predicate->operands.children[i], program, len, depth + 1, resolver,
                       context)) {
            return false;
          }
        }
        break;
      default:
        /* Never negate the result of a lossy membership test. */
        break;
    }
  }
  instruction.end = *len;
  program[position] = instruction;
  return true;
}

FilterQuickCheck* FilterQuickCheck_CompileWithNumericResolver(
    const FilterQuickCheckPredicate* predicate, FilterQuickCheckNumericResolver resolver,
    void* context) {
  QuickCheckInstruction program[QUICK_CHECK_MAX_NODES];
  size_t len = 0;
  if (!compile(predicate, program, &len, 0, resolver, context)) {
    /* Dropping a disjunct would create false negatives; discard the entire incomplete plan. */
    program[0] = (QuickCheckInstruction){.type = FilterQuickCheck_UNKNOWN, .end = 1};
    len = 1;
  }
  FilterQuickCheck* check = rm_malloc(sizeof(*check) + len * sizeof(program[0]));
  if (!check) {
    return NULL;
  }
  check->len = len;
  check->needsSignature = false;
  for (size_t i = 0; i < len; ++i) {
    check->needsSignature |= program[i].type == FilterQuickCheck_TERM;
  }
  memcpy(check->instructions, program, len * sizeof(program[0]));
  return check;
}

FilterQuickCheck* FilterQuickCheck_Compile(const FilterQuickCheckPredicate* predicate) {
  return FilterQuickCheck_CompileWithNumericResolver(predicate, NULL, NULL);
}

void FilterQuickCheck_Free(FilterQuickCheck* check) {
  rm_free(check);
}

static bool evaluateNode(const FilterQuickCheck* check, size_t position,
                         const FilterQuickCheckSignature* signature,
                         const FilterQuickCheckContext* context, uint64_t docId) {
  const QuickCheckInstruction* instruction = &check->instructions[position];
  switch (instruction->type) {
    case FilterQuickCheck_TERM:
      if (signature) {
        for (size_t w = 0; w < 4; ++w) {
          if ((signature->words[w] & instruction->mask.words[w]) != instruction->mask.words[w]) {
            return false;
          }
        }
      }
      return true;
    case FilterQuickCheck_NUMERIC:
      return numericMayMatch(&instruction->numeric, context, docId);
    case FilterQuickCheck_AND:
    case FilterQuickCheck_OR: {
      const bool identity = instruction->type == FilterQuickCheck_AND;
      for (size_t child = position + 1; child < instruction->end;
           child = check->instructions[child].end) {
        bool maybe = evaluateNode(check, child, signature, context, docId);
        if (maybe != identity) {
          return maybe;
        }
      }
      return identity;
    }
    default:
      return true;
  }
}

static FilterQuickCheckResult evaluate(const FilterQuickCheck* check,
                                       const FilterQuickCheckSignature* signature,
                                       const FilterQuickCheckContext* context, uint64_t docId) {
  return !check || evaluateNode(check, 0, signature, context, docId) ? FilterQuickCheck_MAYBE
                                                                     : FilterQuickCheck_NO;
}

FilterQuickCheckResult FilterQuickCheck_Evaluate(const FilterQuickCheck* check,
                                                 const FilterQuickCheckSignature* signature) {
  return signature ? evaluate(check, signature, NULL, 0) : FilterQuickCheck_MAYBE;
}

static FilterQuickCheckResult checkDocument(const FilterQuickCheckContext* context,
                                            uint64_t docId) {
  if (!context || !context->check) {
    return FilterQuickCheck_MAYBE;
  }
  if (context->check->len == 1 &&
      context->check->instructions[0].type == FilterQuickCheck_UNKNOWN) {
    return FilterQuickCheck_MAYBE;
  }
  FilterQuickCheckSignature signature;
  const FilterQuickCheckSignature* available = NULL;
  if (context->check->needsSignature && context->lookup &&
      context->lookup(context->lookupContext, docId, &signature)) {
    available = &signature;
  }
  return evaluate(context->check, available, context, docId);
}

int FilterQuickCheck_MayMatch(void* context, size_t label) {
  return checkDocument(context, label) != FilterQuickCheck_NO;
}

void FilterQuickCheck_CheckBatch(const FilterQuickCheckContext* context, const uint64_t* docIds,
                                 size_t len, FilterQuickCheckResult* results) {
  for (size_t i = 0; i < len; ++i) {
    results[i] = checkDocument(context, docIds[i]);
  }
}
