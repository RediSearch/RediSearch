/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <math.h>
#include <string.h>

#include <stdio.h>

#include "util/fnv.h"
#include "hll.h"

#include "rmalloc.h"

#define INVALID_CACHE_CARDINALITY SIZE_MAX

static inline uint8_t _hll_rank(uint32_t hash, uint8_t max) {
  uint8_t rank = hash ? __builtin_ctz(hash) : 32; // index of first set bit
  return (rank > max ? max : rank) + 1;
}

/*
 * @param bits: The number of bits to use for the register index.
 *              The expected error rate is 1.04 / sqrt(2^bits)
 */
int hll_init(struct HLL *hll, uint8_t bits) {
  if (bits < 4 || bits > 20) {
    return -1;
  }

  hll->bits = bits;
  hll->rank_bits = 32 - bits;
  hll->size = 1U << bits;
  hll->cachedCard = 0; // Initially the cardinality is 0
  hll->registers = rm_calloc(hll->size, sizeof(*hll->registers));

  return 0;
}

void hll_destroy(struct HLL *hll) {
  rm_free(hll->registers);
  hll->registers = NULL;
}

static inline void _hll_add_hash(struct HLL *hll, uint32_t hash) {
  uint32_t index = hash >> hll->rank_bits;
  uint8_t rank = _hll_rank(hash, hll->rank_bits);

  if (rank > hll->registers[index]) {
    hll->registers[index] = rank;
    // New max rank, invalidate the cached cardinality
    hll->cachedCard = INVALID_CACHE_CARDINALITY;
  }
}

void hll_add_hash(struct HLL *hll, uint32_t h) {
  _hll_add_hash(hll, h);
}

void hll_add(struct HLL *hll, const void *buf, size_t size) {
  uint32_t hash = rs_fnv_32a_buf(buf, size, 0x5f61767a);
  _hll_add_hash(hll, hash);
}

size_t hll_count(const struct HLL *hll) {
  // Return the cached cardinality if it's available
  if (INVALID_CACHE_CARDINALITY != hll->cachedCard) return hll->cachedCard;

  double alpha_mm;
  switch (hll->bits) {
    case 4:
      alpha_mm = 0.673;
      break;
    case 5:
      alpha_mm = 0.697;
      break;
    case 6:
      alpha_mm = 0.709;
      break;
    default:
      alpha_mm = 0.7213 / (1.0 + 1.079 / hll->size);
      break;
  }

  alpha_mm *= hll->size * hll->size;

  double sum = 0;
  for (uint32_t i = 0; i < hll->size; i++) {
    sum += 1.0 / (1 << hll->registers[i]);
  }

  double estimate = alpha_mm / sum;

  if (estimate <= 5.0 / 2.0 * hll->size) {
    int zeros = 0;

    for (uint32_t i = 0; i < hll->size; i++) if (hll->registers[i] == 0) zeros++;

    if (zeros) estimate = hll->size * log((double)hll->size / zeros);

  } else if (estimate > (1.0 / 30.0) * 4294967296.0) {
    estimate = -4294967296.0 * log(1.0 - (estimate / 4294967296.0));
  }

  ((struct HLL*)hll)->cachedCard = estimate; // cache the current estimate
  return estimate;
}

int hll_merge(struct HLL *dst, const struct HLL *src) {
  if (dst->size != src->size) {
    return -1;
  }

  for (uint32_t i = 0; i < src->size; i++) {
    if (dst->registers[i] < src->registers[i]) {
      dst->registers[i] = src->registers[i];
      // New max rank, invalidate the cached cardinality
      dst->cachedCard = INVALID_CACHE_CARDINALITY;
    }
  }
  return 0;
}

int hll_load(struct HLL *hll, const void *registers, uint32_t size) {
  if (__builtin_popcount(size) != 1) {
    return -1; // size must be a power of 2 - a single bit set
  }

  // Since `size` is a power of 2, the number of trailing zeros is the log2 of `size`
  if (hll_init(hll, __builtin_ctz(size)) == -1) return -1;

  memcpy(hll->registers, registers, size * sizeof(*hll->registers));

  return 0;
}

int hll_set_registers(struct HLL *hll, const void *registers, uint32_t size) {
  if (__builtin_popcount(size) != 1) {
    return -1; // size must be a power of 2 - a single bit set
  }

  if (hll->size != size) {
    hll_destroy(hll);
    if (hll_init(hll, __builtin_ctz(size)) == -1) return -1;
  }

  memcpy(hll->registers, registers, size * sizeof(*hll->registers));
  hll->cachedCard = INVALID_CACHE_CARDINALITY; // Invalidate the cached cardinality

  return 0;
}

void hll_clear(struct HLL *hll) {
  memset(hll->registers, 0, hll->size * sizeof(*hll->registers));
  hll->cachedCard = 0; // No elements, so the cardinality is 0
}
