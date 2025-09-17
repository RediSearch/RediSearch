/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef FXHASH_H
#define FXHASH_H

#include <stdint.h>
#include <stdlib.h>

uint32_t fxhash_32_incremental(const void *buf, size_t len, uint32_t hval);
uint32_t fxhash_32(const void *buf, size_t len);

uint64_t fxhash_64_incremental(const void *buf, size_t len, uint64_t hval);
uint32_t fxhash_64(const void *buf, size_t len);

#endif
