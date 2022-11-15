/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <sys/types.h>
#define FNV_32_PRIME ((Fnv32_t)0x01000193)

u_int32_t fnv_32a_buf(void *buf, size_t len, u_int32_t hval);
