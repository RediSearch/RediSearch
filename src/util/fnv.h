/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __FT_FNV_H__
#define __FT_FNV_H__

#include <stdint.h>
#include <stdlib.h>

uint32_t rs_fnv_32a_buf(const void *buf, size_t len, uint32_t hval);

uint64_t fnv_64a_buf(const void *buf, size_t len, uint64_t hval);

#endif
