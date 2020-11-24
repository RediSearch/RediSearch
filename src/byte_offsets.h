
#pragma once

#include "redisearch.h"
#include "varint.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct __attribute__((packed)) RSByteOffsetField {
  // ID this belongs to
  uint16_t fieldId;

  // The position of the first token for this field
  uint32_t firstTokPos;

  // Position of last token for this field
  uint32_t lastTokPos;
};

//---------------------------------------------------------------------------------------------

struct RSByteOffsets {
  // By-Byte offsets
  RSOffsetVector offsets;
  // List of field-id <-> position mapping
  RSByteOffsetField *fields;
  // How many fields
  uint8_t numFields;

  RSByteOffsets();
  RSByteOffsets(const Buffer &buf);
  ~RSByteOffsets();

  // Reserve memory for this many fields
  void ReserveFields(size_t numFields);

  // Add a field to the offset map. Note that you cannot add more fields than
  // initially declared via ReserveFields
  // The start position is the position of the first token in this field.
  // The field info is returned, and the last position should be written to it
  // when done.
  RSByteOffsetField *AddField(uint32_t fieldId, uint32_t startPos);

  void Serialize(Buffer *b) const;
};

//---------------------------------------------------------------------------------------------

class ByteOffsetWriter : public VarintVectorWriter {
public:
  ByteOffsetWriter() : VarintVectorWriter(16) {}

  void Move(RSByteOffsets *offsets);
};

//---------------------------------------------------------------------------------------------

// Iterator which yields the byte offset for a given position
struct RSByteOffsetIterator {
  BufferReader rdr;
  Buffer buf;
  uint32_t lastValue;
  uint32_t curPos;
  uint32_t endPos;
  bool valid;

  // Begin iterating over the byte offsets for a given field. 
  // Returns REDISMODULE_ERR if the field does not exist in the current byte offset.

  RSByteOffsetIterator(const RSByteOffsets &offsets, uint32_t fieldId);

  // Returns the next byte offset for the given position. 
  // The current position can be obtained using the curPos variable.
  // Returns RSBYTEOFFSET_EOF when the iterator is at the end of the token stream.

  uint32_t Next();

  bool operator!() const { return valid; }
};

#define RSBYTEOFFSET_EOF ((uint32_t)-1)


///////////////////////////////////////////////////////////////////////////////////////////////
