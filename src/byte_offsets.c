
#include "byte_offsets.h"
#include <arpa/inet.h>

///////////////////////////////////////////////////////////////////////////////////////////////

RSByteOffsets::~RSByteOffsets() {
  rm_free(offsets.data);
}

//---------------------------------------------------------------------------------------------

void RSByteOffsets::ReserveFields(size_t numFields) {
  fields.clear();
  fields.reserve(numFields);
}

//---------------------------------------------------------------------------------------------

void RSByteOffsets::AddField(uint32_t fieldId, uint32_t startPos, uint32_t lastTok) {
  RSByteOffsetField field;
  field.fieldId = fieldId;
  field.firstTokPos = startPos;
  field.lastTokPos = lastTok;
  fields.push_back(field);
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

  w.WriteU8(fields.size());

  for (auto field : fields) {
    w.WriteU8(field.fieldId);
    w.WriteU32(field.firstTokPos);
    w.WriteU32(field.lastTokPos);
  }

  w.WriteU32(offsets.len);
  w.Write(offsets.data, offsets.len);
}

//---------------------------------------------------------------------------------------------

RSByteOffsets::RSByteOffsets(const Buffer &buf) {
  BufferReader r(&buf);

  uint8_t numFields = r.ReadU8();
  fields.reserve(numFields);

  for (size_t i = 0; i < numFields; ++i) {
    uint8_t fieldId = r.ReadU8();
    uint32_t firstTok = r.ReadU32();
    uint32_t lastTok = r.ReadU32();
    AddField(fieldId, firstTok, lastTok);
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
  for (size_t i = 0; i < offsets.fields.size(); ++i) {
    if (offsets.fields[i].fieldId == fieldId) {
      offField = &offsets.fields[i];
      break;
    }
  }
  if (!offField) {
    return;
  }

  buf.cap = 0;
  buf.data = offsets.offsets.data;
  buf.offset = offsets.offsets.len;
  rdr.Set(&buf);
  curPos = 1;
  endPos = offField->lastTokPos;

  lastValue = 0;
  while (curPos < offField->firstTokPos && !rdr.AtEnd()) {
    lastValue += ReadVarint(rdr);
    curPos++;
  }

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
