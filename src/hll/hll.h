#ifndef AVZ_HLL_H
#define AVZ_HLL_H

#include <sys/types.h>
#include <stdint.h>

struct HLL {
  uint8_t bits;

  size_t size;
  uint8_t *registers;
};

extern int hll_init(struct HLL *hll, uint8_t bits);
extern int hll_load(struct HLL *hll, const void *registers, size_t size);
extern void hll_destroy(struct HLL *hll);
extern int hll_merge(struct HLL *dst, const struct HLL *src);
extern void hll_add(struct HLL *hll, const void *buf, size_t size);
void hll_add_hash(struct HLL *hll, uint32_t h);
extern double hll_count(const struct HLL *hll);

extern uint32_t _hll_hash(const struct HLL *hll);

#endif /* AVZ_HLL_H */
