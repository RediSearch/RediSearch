/*
 *  Copyright (c) 2012-2017, Jyri J. Virkki
 *  All rights reserved.
 *
 *  This file is under BSD license. See LICENSE file.
 */

/*
 * Refer to bloom.h for documentation on the public interfaces.
 */

#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "bloom.h"
#include "murmurhash2.h"

#define MAKESTRING(n) STRING(n)
#define STRING(n) #n

#ifndef BLOOM_CALLOC
#define BLOOM_CALLOC rm_calloc
#define BLOOM_FREE rm_free
#endif

#define MODE_READ 0
#define MODE_WRITE 1

inline static int test_bit_set_bit(unsigned char *buf, unsigned int x, int mode) {
  unsigned int byte = x >> 3;
  unsigned char c = buf[byte];  // expensive memory access
  unsigned int mask = 1 << (x % 8);

  if (c & mask) {
    return 1;
  } else {
    if (mode == MODE_WRITE) {
      buf[byte] = c | mask;
    }
    return 0;
  }
}

bloom_hashval bloom_calc_hash(const void *buffer, int len) {
  bloom_hashval rv;
  rv.a = murmurhash2(buffer, len, 0x9747b28c);
  rv.b = murmurhash2(buffer, len, rv.a);
  return rv;
}

// This function is defined as a macro because newer filters use a power of two
// for bit count, which is must faster to calculate. Older bloom filters don't
// use powers of two, so they are slower. Rather than calculating this inside
// the function itself, we provide two variants for this. The calling layer
// already knows which variant to call.
//
// modExp is the expression which will evaluate to the number of bits in the
// filter.
#define CHECK_ADD_FUNC(T, modExp)                \
  register unsigned int i;                       \
  int found_unset = 0;                           \
  const register T mod = modExp;                 \
  for (i = 0; i < bloom->hashes; i++) {          \
    T x = ((hashval.a + i * hashval.b)) % mod;   \
    if (!test_bit_set_bit(bloom->bf, x, mode)) { \
      if (mode == MODE_READ) {                   \
        return 0;                                \
      }                                          \
      found_unset = 1;                           \
    }                                            \
  }                                              \
  if (mode == MODE_READ) {                       \
    return 1;                                    \
  }                                              \
  return found_unset;

static int bloom_check_add32(struct bloom *bloom, bloom_hashval hashval, int mode) {
  CHECK_ADD_FUNC(uint32_t, (1 << bloom->n2));
}

static int bloom_check_add64(struct bloom *bloom, bloom_hashval hashval, int mode) {
  CHECK_ADD_FUNC(uint64_t, (1LLU << bloom->n2));
}

// This function is used for older bloom filters whose bit count was not
// 1 << X. This function is a bit slower, and isn't exposed in the API
// directly because it's deprecated
static int bloom_check_add_compat(struct bloom *bloom, bloom_hashval hashval, int mode) {
  CHECK_ADD_FUNC(uint64_t, bloom->bits)
}

static double calc_bpe(double error) {
  static const double denom = 0.480453013918201;  // ln(2)^2
  double num = log(error);

  double bpe = -(num / denom);
  if (bpe < 0) {
    bpe = -bpe;
  }
  return bpe;
}

size_t bloom_cap_for(double error, size_t maxbytes) {
  double bpe = calc_bpe(error);
  bpe *= maxbytes;
  return bpe / 8;
}

int bloom_init(struct bloom *bloom, unsigned entries, double error, unsigned options) {
  if (entries < 1 || error <= 0 || error > 1.0) {
    return 1;
  }

  bloom->error = error;
  bloom->bits = 0;
  bloom->entries = entries;
  bloom->bpe = calc_bpe(error);

  double dentries = (double)entries;
  uint64_t bits;

  if (options & BLOOM_OPT_ENTS_IS_BITS) {
    // Size is determined by the number of bits
    if (entries == 0 || entries > 64) {
      return 1;
    }

    bloom->n2 = entries;
    bits = 1LLU << bloom->n2;
    dentries = entries = bloom->entries = bits / bloom->bpe;

  } else if (options & BLOOM_OPT_NOROUND) {
    // Don't perform any rounding. Conserve memory instead
    bits = bloom->bits = (uint64_t)(dentries * bloom->bpe);
    bloom->n2 = 0;

  } else {
    double bn2 = logb(dentries * bloom->bpe);
    if (bn2 > 63 || bn2 == INFINITY) {
      return 1;
    }
    bloom->n2 = bn2 + 1;
    bits = 1LLU << bloom->n2;

    // Determine the number of extra bits available for more items. We rounded
    // up the number of bits to the next-highest power of two. This means we
    // might have up to 2x the bits available to us.
    size_t bitDiff = bits - (dentries * bloom->bpe);
    // The number of additional items we can store is the extra number of bits
    // divided by bits-per-element
    size_t itemDiff = bitDiff / bloom->bpe;
    bloom->entries += itemDiff;
  }

  if (bits % 8) {
    bloom->bytes = (bits / 8) + 1;
  } else {
    bloom->bytes = bits / 8;
  }

  bloom->hashes = (int)ceil(0.693147180559945 * bloom->bpe);  // ln(2)
  bloom->bf = (unsigned char *)BLOOM_CALLOC(bloom->bytes, sizeof(unsigned char));
  if (bloom->bf == NULL) {
    return 1;
  }

  return 0;
}

int bloom_check_h(const struct bloom *bloom, bloom_hashval hash) {
  if (bloom->n2 > 31) {
    return bloom_check_add64((void *)bloom, hash, MODE_READ);
  } else if (bloom->n2 > 0) {
    return bloom_check_add32((void *)bloom, hash, MODE_READ);
  } else {
    return bloom_check_add_compat((void *)bloom, hash, MODE_READ);
  }
}

int bloom_check(const struct bloom *bloom, const void *buffer, int len) {
  return bloom_check_h(bloom, bloom_calc_hash(buffer, len));
}

int bloom_add_h(struct bloom *bloom, bloom_hashval hash) {
  if (bloom->n2 > 31) {
    return !bloom_check_add64(bloom, hash, MODE_WRITE);
  } else if (bloom->n2) {
    return !bloom_check_add32(bloom, hash, MODE_WRITE);
  } else {
    return !bloom_check_add_compat(bloom, hash, MODE_WRITE);
  }
}

int bloom_add(struct bloom *bloom, const void *buffer, int len) {
  return bloom_add_h(bloom, bloom_calc_hash(buffer, len));
}

void bloom_free(struct bloom *bloom) {
  BLOOM_FREE(bloom->bf);
}

const char *bloom_version() {
  return MAKESTRING(BLOOM_VERSION);
}
