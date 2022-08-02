#pragma once

#include <sys/types.h>
#include <stdint.h>

#define HLL_PRECISION_BITS 8

struct HLL {
  uint8_t bits;

  size_t size;
  uint8_t *registers;

  bool ctor(uint8_t bits);
  HLL(uint8_t bits = HLL_PRECISION_BITS) { ctor(bits); }
  ~HLL();

  bool load(const void *registers, size_t size);
  void destroy();
  bool merge(const HLL *src);
  void add(const void *buf, size_t size);
  void add_hash(uint32_t h);
  double count() const;
  uint32_t hash() const;
};

