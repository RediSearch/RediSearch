/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __FT_FNV_H__
#define __FT_FNV_H__

#include <stdint.h>
#include <stdlib.h>

#define FNV_32_PRIME ((Fnv32_t)0x01000193)

uint32_t rs_fnv_32a_buf(const void *buf, size_t len, uint32_t hval);

uint64_t fnv_64a_buf(const void *buf, size_t len, uint64_t hval);

#endif
