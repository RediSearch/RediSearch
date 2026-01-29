/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * wyhash - A fast, high-quality hash function.
 * Based on wyhash v4 (public domain): https://github.com/wangyi-fudan/wyhash
 */

#ifndef WYHASH_H
#define WYHASH_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline uint64_t _wyr8(const uint8_t *p) {
  uint64_t v;
  memcpy(&v, p, 8);
  return v;
}

static inline uint64_t _wyr4(const uint8_t *p) {
  uint32_t v;
  memcpy(&v, p, 4);
  return v;
}

static inline uint64_t _wymix(uint64_t a, uint64_t b) {
  __uint128_t r = (__uint128_t)a * b;
  return (uint64_t)(r ^ (r >> 64));
}

static inline uint64_t wyhash(const void *key, size_t len, uint64_t seed) {
  const uint8_t *p = (const uint8_t *)key;
  uint64_t a, b;
  if (len <= 16) {
    if (len >= 4) {
      a = (_wyr4(p) << 32) | _wyr4(p + ((len >> 3) << 2));
      b = (_wyr4(p + len - 4) << 32) | _wyr4(p + len - 4 - ((len >> 3) << 2));
    } else if (len > 0) {
      a = ((uint64_t)p[0] << 16) | ((uint64_t)p[len >> 1] << 8) | p[len - 1];
      b = 0;
    } else {
      a = b = 0;
    }
  } else {
    size_t i = len;
    if (i > 48) {
      uint64_t s1 = seed, s2 = seed;
      do {
        seed = _wymix(_wyr8(p) ^ 0xa0761d6478bd642fULL, _wyr8(p + 8) ^ seed);
        s1 = _wymix(_wyr8(p + 16) ^ 0xe7037ed1a0b428dbULL, _wyr8(p + 24) ^ s1);
        s2 = _wymix(_wyr8(p + 32) ^ 0x8ebc6af09c88c6e3ULL, _wyr8(p + 40) ^ s2);
        p += 48;
        i -= 48;
      } while (i > 48);
      seed ^= s1 ^ s2;
    }
    while (i > 16) {
      seed = _wymix(_wyr8(p) ^ 0xa0761d6478bd642fULL, _wyr8(p + 8) ^ seed);
      i -= 16;
      p += 16;
    }
    a = _wyr8(p + i - 16);
    b = _wyr8(p + i - 8);
  }
  return _wymix(0xa0761d6478bd642fULL ^ len, _wymix(a ^ 0xa0761d6478bd642fULL, b ^ seed));
}

#ifdef __cplusplus
}
#endif

#endif /* WYHASH_H */

