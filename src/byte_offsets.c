
#include "byte_offsets.h"
#include <arpa/inet.h>

///////////////////////////////////////////////////////////////////////////////////////////////

RSByteOffsets::RSByteOffsets() {
  fields = NULL;
  numFields = 0;
}

//---------------------------------------------------------------------------------------------

RSByteOffsets::~RSByteOffsets() {
  rm_free(offsets.data);
  if (fields) rm_free(fields);
}

//---------------------------------------------------------------------------------------------

void RSByteOffsets::ReserveFields(size_t numFields) {
  fields = rm_realloc(fields, sizeof(*fields) * numFields);
}

//---------------------------------------------------------------------------------------------

RSByteOffsetField *RSByteOffsets::AddField(uint32_t fieldId, uint32_t startPos) {
  RSByteOffsetField *field = &(fields[numFields++]);
  field->fieldId = fieldId;
  field->firstTokPos = startPos;
  return field;
}

//---------------------------------------------------------------------------------------------

void ByteOffsetWriter::Move(RSByteOffsets *offsets) {
  offsets->offsets.data = buf.data;
  offsets->offsets.len = buf.offset;
  memset(&buf, 0, sizeof(buf));
}

//---------------------------------------------------------------------------------------------

void RSByteOffsets::Serialize(Buffer *b) const {
  BufferWriter w(b);

  w.WriteU8(numFields);

  for (size_t i = 0; i < numFields; ++i) {
    w.WriteU8(fields[i].fieldId);
    w.WriteU32(fields[i].firstTokPos);
    w.WriteU32(fields[i].lastTokPos);
  }

  w.WriteU32(offsets.len);
  w.Write(offsets.data, offsets.len);
}

//---------------------------------------------------------------------------------------------

RSByteOffsets::RSByteOffsets(const Buffer &buf) {
  BufferReader r(&buf);

  uint8_t numFields = r.ReadU8();
  ReserveFields(numFields);

  for (size_t i = 0; i < numFields; ++i) {
    uint8_t fieldId = r.ReadU8();
    uint32_t firstTok = r.ReadU32();
    uint32_t lastTok = r.ReadU32();
    RSByteOffsetField *fieldInfo = AddField(fieldId, firstTok);
    fieldInfo->lastTokPos = lastTok;
  }

  uint32_t offsetsLen = r.ReadU32();
  offsets.len = offsetsLen;
  if (offsetsLen) {
    offsets.data = rm_malloc(offsetsLen);
    r.Read(offsets.data, offsetsLen);
  } else {
    offsets.data = NULL;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSByteOffsetIterator::RSByteOffsetIterator(const RSByteOffsets &offsets, uint32_t fieldId) :
  rdr(&buf) {

  valid = false;

  const RSByteOffsetField *offField = NULL;
  for (size_t i = 0; i < offsets.numFields; ++i) {
    if (offsets.fields[i].fieldId == fieldId) {
      offField = &offsets.fields[i];
      break;
    }
  }
  if (!offField) {
    return;
  }

  // printf("Generating iterator for fieldId=%lu. BeginPos=%lu. EndPos=%lu\n", fieldId,
  //        offField->firstTokPos, offField->lastTokPos);

  buf.cap = 0;
  buf.data = offsets.offsets.data;
  buf.offset = offsets.offsets.len;
  rdr.Set(&buf);
  curPos = 1;
  endPos = offField->lastTokPos;

  lastValue = 0;
  while (curPos < offField->firstTokPos && !rdr.AtEnd()) {
    // printf("Seeking & incrementing\n");
    lastValue += ReadVarint(rdr);
    curPos++;
  }

  // printf("Iterator is now at %lu\n", iter->curPos);
  curPos--;
  valid = true;;
}

//---------------------------------------------------------------------------------------------

uint32_t RSByteOffsetIterator::Next() {
  if (!valid || rdr.AtEnd() || ++curPos > endPos) {
    return RSBYTEOFFSET_EOF;
  }

  lastValue += ReadVarint(rdr);
  return lastValue;
}

///////////////////////////////////////////////////////////////////////////////////////////////
