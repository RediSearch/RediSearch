/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef BYTE_OFFSETS_H
#define BYTE_OFFSETS_H

#include "varint_ffi.h"

// Collects the byte offset of each token of a document while it is indexed. The document's
// RSByteOffsets (byte_offsets_ffi.h) then takes them over with RSByteOffsets_TakeFromWriter.
typedef struct {
  VarintVectorWriter *vw;
} ByteOffsetWriter;

static inline void ByteOffsetWriter_Init(ByteOffsetWriter *w) {
  w->vw = NewVarintVectorWriter(16);
}

static inline void ByteOffsetWriter_Cleanup(ByteOffsetWriter *w) {
  if (w->vw != NULL) {
    VVW_Free(w->vw);
    w->vw = NULL;
  }
}

static inline void ByteOffsetWriter_Write(ByteOffsetWriter *w, uint32_t offset) {
  VVW_Write(w->vw, offset);
}

static inline uint32_t ByteOffsetWriter_Count(const ByteOffsetWriter *w) {
  return VVW_GetCount(w->vw);
}

#endif
