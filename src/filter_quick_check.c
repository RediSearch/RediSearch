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

#include <string.h>

/* Both compile recursion and per-candidate work are bounded independently of query size. */
#define QUICK_CHECK_MAX_NODES 256
#define QUICK_CHECK_MAX_DEPTH 32

typedef struct {
  FilterQuickCheckPredicateType type;
  size_t arity;
  FilterQuickCheckSignature mask;
} QuickCheckInstruction;

struct FilterQuickCheck {
  size_t len;
  QuickCheckInstruction instructions[];
};

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
                    size_t* len, size_t depth) {
  if (*len == QUICK_CHECK_MAX_NODES || depth == QUICK_CHECK_MAX_DEPTH) {
    return false;
  }
  QuickCheckInstruction instruction = {.type = FilterQuickCheck_UNKNOWN};
  if (predicate) {
    switch (predicate->type) {
      case FilterQuickCheck_TERM:
        if (validTerm(&predicate->term)) {
          instruction.type = FilterQuickCheck_TERM;
          FilterQuickCheckSignature_Add(&instruction.mask, &predicate->term);
        }
        break;
      case FilterQuickCheck_AND:
      case FilterQuickCheck_OR:
        if (predicate->operands.len > QUICK_CHECK_MAX_NODES ||
            (predicate->operands.len && !predicate->operands.children)) {
          return false;
        }
        instruction.type = predicate->type;
        instruction.arity = predicate->operands.len;
        for (size_t i = 0; i < instruction.arity; ++i) {
          if (!compile(&predicate->operands.children[i], program, len, depth + 1)) {
            return false;
          }
        }
        break;
      default:
        /* In particular, never negate the result of a lossy membership test. */
        break;
    }
  }
  if (*len == QUICK_CHECK_MAX_NODES) {
    return false;
  }
  program[(*len)++] = instruction;
  return true;
}

FilterQuickCheck* FilterQuickCheck_Compile(const FilterQuickCheckPredicate* predicate) {
  QuickCheckInstruction program[QUICK_CHECK_MAX_NODES];
  size_t len = 0;
  if (!compile(predicate, program, &len, 0)) {
    /* Dropping a disjunct would create false negatives; discard the entire incomplete plan. */
    program[0] = (QuickCheckInstruction){.type = FilterQuickCheck_UNKNOWN};
    len = 1;
  }
  FilterQuickCheck* check = rm_malloc(sizeof(*check) + len * sizeof(program[0]));
  if (!check) {
    return NULL;
  }
  check->len = len;
  memcpy(check->instructions, program, len * sizeof(program[0]));
  return check;
}

void FilterQuickCheck_Free(FilterQuickCheck* check) {
  rm_free(check);
}

FilterQuickCheckResult FilterQuickCheck_Evaluate(const FilterQuickCheck* check,
                                                 const FilterQuickCheckSignature* signature) {
  if (!check || !signature) {
    return FilterQuickCheck_MAYBE;
  }
  bool stack[QUICK_CHECK_MAX_NODES];
  size_t top = 0;
  for (size_t i = 0; i < check->len; ++i) {
    const QuickCheckInstruction* instruction = &check->instructions[i];
    bool maybe = true;
    switch (instruction->type) {
      case FilterQuickCheck_TERM:
        for (size_t w = 0; w < 4; ++w) {
          if ((signature->words[w] & instruction->mask.words[w]) != instruction->mask.words[w]) {
            maybe = false;
            break;
          }
        }
        break;
      case FilterQuickCheck_AND:
      case FilterQuickCheck_OR:
        maybe = instruction->type == FilterQuickCheck_AND;
        for (size_t j = 0; j < instruction->arity; ++j) {
          bool child = stack[--top];
          maybe = instruction->type == FilterQuickCheck_AND ? maybe && child : maybe || child;
        }
        break;
      default:
        break;
    }
    stack[top++] = maybe;
  }
  return stack[0] ? FilterQuickCheck_MAYBE : FilterQuickCheck_NO;
}

static FilterQuickCheckResult checkDocument(const FilterQuickCheckContext* context,
                                            uint64_t docId) {
  if (!context || !context->check || !context->lookup) {
    return FilterQuickCheck_MAYBE;
  }
  if (context->check->len == 1 &&
      context->check->instructions[0].type == FilterQuickCheck_UNKNOWN) {
    return FilterQuickCheck_MAYBE;
  }
  FilterQuickCheckSignature signature;
  if (!context->lookup(context->lookupContext, docId, &signature)) {
    return FilterQuickCheck_MAYBE;
  }
  return FilterQuickCheck_Evaluate(context->check, &signature);
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
