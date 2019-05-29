#ifndef API_STUBS_H
#define API_STUBS_H

/**
 * Include this file as well as the associated static library object
 * for any executable which utilizes redisearch, but does _not_
 * run within redis (or the mock).
 */

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k)
    __attribute__((visibility("default")));
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k)
    __attribute__((visibility("default")));

#ifdef __cplusplus
}
#endif
#endif