#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Returns the 32-bit [FNV-1a hash] of `buf` of length `len` using an [offset basis] `hval`.
 *
 * # Safety
 *
 * 1. `buf` must point to a valid region of memory of length `len`.
 *
 * [FNV-1a hash]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
 * [offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
 */
uint32_t rs_fnv_32a_buf(const void *buf, uintptr_t len, uint32_t hval);

/**
 * Returns the 64-bit [FNV-1a hash] of `buf` of length `len` using an [offset basis] `hval`.
 *
 * # Safety
 *
 * 1. `buf` must point to a valid region of memory of length `len`.
 *
 * [FNV-1a hash]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
 * [offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
 */
uint64_t fnv_64a_buf(const void *buf, uintptr_t len, uint64_t hval);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
