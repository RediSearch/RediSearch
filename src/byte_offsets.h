#ifndef BYTE_OFFSETS_H
#define BYTE_OFFSETS_H

#include "redisearch.h"
#include "varint.h"
#include "rmalloc.h"

typedef struct __attribute__((packed)) RSByteOffsetMap {
  // ID this belongs to.
  uint8_t fieldId;

  // The position of the first token for this field.
  uint32_t firstTokPos;

  // Position of last token for this field
  uint32_t lastTokPos;
} RSByteOffsetField;

typedef struct RSByteOffsets {
  // By-Byte offsets
  RSOffsetVector offsets;
  // List of field-id <-> position mapping
  RSByteOffsetField *fields;
  // How many fields
  uint8_t numFields;
} RSByteOffsets;

RSByteOffsets *NewByteOffsets();

void RSByteOffsets_Free(RSByteOffsets *offsets);

// Reserve memory for this many fields
void RSByteOffsets_ReserveFields(RSByteOffsets *offsets, size_t numFields);

// Add a field to the offset map. Note that you cannot add more fields than
// initially declared via ReserveFields
// The start position is the position of the first token in this field.
// The field info is returned, and the last position should be written to it
// when done.
RSByteOffsetField *RSByteOffsets_AddField(RSByteOffsets *offsets, uint32_t fieldId,
                                          uint32_t startPos);

void RSByteOffsets_Serialize(const RSByteOffsets *offsets, Buffer *b);
RSByteOffsets *LoadByteOffsets(Buffer *buf);

typedef VarintVectorWriter ByteOffsetWriter;

void ByteOffsetWriter_Move(ByteOffsetWriter *w, RSByteOffsets *offsets);

static inline void ByteOffsetWriter_Init(ByteOffsetWriter *w) {
  VVW_Init(w, 16);
}

static inline void ByteOffsetWriter_Cleanup(ByteOffsetWriter *w) {
  VVW_Cleanup(w);
}

static inline void ByteOffsetWriter_Write(ByteOffsetWriter *w, uint32_t offset) {
  VVW_Write(w, offset);
}

#endif