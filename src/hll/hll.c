#include <stdlib.h>
#include <errno.h>
#include <math.h>
#include <string.h>

#include <stdio.h>

#include "util/fnv.h"
#include "hll.h"

#include "rmalloc.h"

static __inline uint8_t _hll_rank(uint32_t hash, uint8_t bits) {
  uint8_t i;

  for (i = 1; i <= 32 - bits; i++) {
    if (hash & 1) break;

    hash >>= 1;
  }

  return i;
}

bool HLL::ctor(uint8_t bits) {
  if (bits < 4 || bits > 20) {
    errno = ERANGE;
    return false;
  }

  bits = bits;
  size = (size_t)1 << bits;
  registers = rm_calloc(size, 1);
  return true;
}

HLL::~HLL() {
  rm_free(registers);
  registers = NULL;
}

void HLL::add_hash(uint32_t hash) {
  uint32_t index = hash >> (32 - bits);
  uint8_t rank = _hll_rank(hash, bits);

  if (rank > registers[index]) {
    registers[index] = rank;
  }
}

void HLL::add(const void *buf, size_t size) {
  uint32_t hash = rs_fnv_32a_buf((void *)buf, (uint32_t)size, 0x5f61767a);
  add_hash(hash);
}

double HLL::count() const {
  double alpha_mm;
  uint32_t i;

  switch (bits) {
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
      alpha_mm = 0.7213 / (1.0 + 1.079 / (double)size);
      break;
  }

  alpha_mm *= ((double)size * (double)size);

  double sum = 0;
  for (i = 0; i < size; i++) {
    sum += 1.0 / (1 << registers[i]);
  }

  double estimate = alpha_mm / sum;

  if (estimate <= 5.0 / 2.0 * (double)size) {
    int zeros = 0;

    for (i = 0; i < size; i++) zeros += (registers[i] == 0);

    if (zeros) estimate = (double)size * log((double)size / zeros);

  } else if (estimate > (1.0 / 30.0) * 4294967296.0) {
    estimate = -4294967296.0 * log(1.0 - (estimate / 4294967296.0));
  }

  return estimate;
}

bool HLL::merge(const HLL *src) {
  uint32_t i;

  if (bits != src->bits) {
    errno = EINVAL;
    return false;
  }

  for (i = 0; i < size; i++) {
    if (src->registers[i] > registers[i]) registers[i] = src->registers[i];
  }

  return true;
}

bool HLL::load(const void *registers_, size_t size) {
  uint8_t bits = 0;
  size_t s = size;

  while (s) {
    if (s & 1) break;

    bits++;

    s >>= 1;
  }

  if (!bits || ((size_t)1 << bits) != size) {
    errno = EINVAL;
    return false;
  }

  if (!ctor(bits)) return false;

  memcpy(registers, registers_, size);

  return true;
}

uint32_t HLL::hash() const {
  return rs_fnv_32a_buf(registers, (uint32_t)size, 0);
}
