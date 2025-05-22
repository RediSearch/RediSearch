/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>
#include "buffer.h"
#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Read an encoded integer from the buffer. It is assumed that the buffer will not overflow */
uint32_t ReadVarint(BufferReader *b);
t_fieldMask ReadVarintFieldMask(BufferReader *b);

size_t WriteVarint(uint32_t value, BufferWriter *w);

size_t WriteVarintFieldMask(t_fieldMask value, BufferWriter *w);

typedef struct VarintVectorWriter VarintVectorWriter;

VarintVectorWriter *NewVarintVectorWriter(size_t cap);
void VVW_Write(VarintVectorWriter *w, uint32_t i);
size_t VVW_Truncate(VarintVectorWriter *w);
void VVW_Free(VarintVectorWriter *w);

void VVW_Reset(VarintVectorWriter *w);

size_t VVW_GetCount(const VarintVectorWriter *w);
size_t VVW_GetByteLength(const VarintVectorWriter *w);
const char *VVW_GetByteData(const VarintVectorWriter *w);
char *VVW_TakeByteData(VarintVectorWriter *w, size_t *len);

#ifdef __cplusplus
}
#endif
#endif
