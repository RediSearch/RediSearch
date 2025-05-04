/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef AVZ_HLL_H
#define AVZ_HLL_H

#include <sys/types.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct HLL {
  uint8_t bits;       // number of bits used for the register index. 4 <= bits <= 20
  uint8_t rank_bits;  // number of bits used for the rank, and also the max rank. cached value of 32 - bits
  uint32_t size;      // number of registers (2^bits). bits <= 20 so this fits in 32 bits
  size_t cachedCard;  // cached cardinality from the last count operation. Invalidated when registers are modified.
  uint8_t *registers;
};

/* Initialise the HLL structure and resources. It has to be cleaned later with `hll_destroy` */
int hll_init(struct HLL *hll, uint8_t bits);
/* Destroy the HLL resources, after it was initialized with `hll_init` or `hll_load` */
void hll_destroy(struct HLL *hll);
/* Initialise the HLL registers from a buffer. The buffer must be of size 2^bits */
int hll_load(struct HLL *hll, const void *registers, uint32_t size);
/* Merge the registers of `src` into `dst`. Both HLLs must have the same number of registers */
int hll_merge(struct HLL *dst, const struct HLL *src);
/* Add an element to the HLL */
void hll_add(struct HLL *hll, const void *buf, size_t size);
/* Add a precomputed hash to the HLL */
void hll_add_hash(struct HLL *hll, uint32_t h);
/* Estimate the cardinality of the HLL */
size_t hll_count(const struct HLL *hll);
/* Load the registers from a buffer. The buffer must be of size 2^bits
   This function is similar to `hll_load`, but assumes the HLL is already initialized */
int hll_set_registers(struct HLL *hll, const void *registers, uint32_t size);
/* Clear the HLL registers, reset the cardinality to 0 */
void hll_clear(struct HLL *hll);

#ifdef __cplusplus
}
#endif
#endif /* AVZ_HLL_H */
