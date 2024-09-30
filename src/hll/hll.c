/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <errno.h>
#include <math.h>
#include <stdbit.h>
#include <string.h>

#include <stdio.h>

#include "util/fnv.h"
#include "hll.h"

#include "rmalloc.h"

static inline uint8_t _hll_rank(uint32_t hash, uint8_t max) {
  uint8_t rank = hash ? stdc_trailing_zeros(hash) : 32; // index of first set bit
  return (rank > max ? max : rank) + 1;
}

int hll_init(struct HLL *hll, uint8_t bits) {
  if (bits < 4 || bits > 20) {
    errno = ERANGE;
    return -1;
  }

  hll->bits = bits;
  hll->rank_bits = 32 - bits;
  hll->size = 1ULL << bits;
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
  }
}

void hll_add_hash(struct HLL *hll, uint32_t h) {
  _hll_add_hash(hll, h);
}

void hll_add(struct HLL *hll, const void *buf, size_t size) {
  uint32_t hash = rs_fnv_32a_buf(buf, (uint32_t)size, 0x5f61767a);

  _hll_add_hash(hll, hash);
}

double hll_count(const struct HLL *hll) {
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
      alpha_mm = 0.7213 / (1.0 + 1.079 / (double)hll->size);
      break;
  }

  alpha_mm *= ((double)hll->size * (double)hll->size);

  double sum = 0;
  for (uint32_t i = 0; i < hll->size; i++) {
    sum += 1.0 / (1 << hll->registers[i]);
  }

  double estimate = alpha_mm / sum;

  if (estimate <= 5.0 / 2.0 * (double)hll->size) {
    int zeros = 0;

    for (uint32_t i = 0; i < hll->size; i++) zeros += (hll->registers[i] == 0);

    if (zeros) estimate = (double)hll->size * log((double)hll->size / zeros);

  } else if (estimate > (1.0 / 30.0) * 4294967296.0) {
    estimate = -4294967296.0 * log(1.0 - (estimate / 4294967296.0));
  }

  return estimate;
}

int hll_merge(struct HLL *dst, const struct HLL *src) {
  if (dst->bits != src->bits) {
    errno = EINVAL;
    return -1;
  }

  for (uint32_t i = 0; i < dst->size; i++) {
    if (src->registers[i] > dst->registers[i]) dst->registers[i] = src->registers[i];
  }

  return 0;
}

int hll_load(struct HLL *hll, const void *registers, size_t size) {
  if (!stdc_has_single_bit(size)) {
    errno = EINVAL; // size must be a power of 2 - a single bit set
    return -1;
  }

  // Since `size` is a power of 2, the number of training zeros is the log2 of `size`
  if (hll_init(hll, stdc_trailing_zeros(size)) == -1) return -1;

  memcpy(hll->registers, registers, size * sizeof(*hll->registers));

  return 0;
}
