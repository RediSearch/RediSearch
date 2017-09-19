#include "byte_offsets.h"

RSByteOffsets *NewByteOffsets() {
  RSByteOffsets *ret = rm_calloc(1, sizeof(*ret));
  return ret;
}

void RSByteOffsets_Free(RSByteOffsets *offsets) {
  rm_free(offsets->offsets.data);
  rm_free(offsets->fields);
  rm_free(offsets);
}

void RSByteOffsets_ReserveFields(RSByteOffsets *offsets, size_t numFields) {
  offsets->fields = rm_realloc(offsets->fields, sizeof(*offsets->fields) * numFields);
}

RSByteOffsetField *RSByteOffsets_AddField(RSByteOffsets *offsets, uint32_t fieldId,
                                          uint32_t startPos) {
  RSByteOffsetField *field = &(offsets->fields[offsets->numFields++]);
  field->fieldId = fieldId;
  field->firstTokPos = startPos;
  return field;
}

void ByteOffsetWriter_Move(ByteOffsetWriter *w, RSByteOffsets *offsets) {
  offsets->offsets.data = w->buf.data;
  offsets->offsets.len = w->buf.offset;
  memset(&w->buf, 0, sizeof w->buf);
}

static void write_u32(BufferWriter *w, uint32_t n) {
  n = htonl(n);
  Buffer_Write(w, &n, 4);
}

static uint32_t load_u32(BufferReader *r) {
  uint32_t n;
  Buffer_Read(r, &n, 4);
  return ntohl(n);
}

static uint8_t load_u8(BufferReader *r) {
  uint8_t n;
  Buffer_Read(r, &n, 1);
  return n;
}

static void write_u8(BufferWriter *w, uint8_t n) {
  Buffer_Write(w, &n, 1);
}

void RSByteOffsets_Serialize(const RSByteOffsets *offsets, Buffer *b) {
  // Create a buffer
  Buffer_Init(b, 16);
  BufferWriter w = NewBufferWriter(b);

  write_u8(&w, offsets->numFields);

  for (size_t ii = 0; ii < offsets->numFields; ++ii) {
    write_u8(&w, offsets->fields[ii].fieldId);
    write_u32(&w, offsets->fields[ii].firstTokPos);
    write_u32(&w, offsets->fields[ii].lastTokPos);
  }

  write_u32(&w, offsets->offsets.len);
  Buffer_Write(&w, offsets->offsets.data, offsets->offsets.len);
}

RSByteOffsets *LoadByteOffsets(Buffer *buf) {
  BufferReader r = NewBufferReader(buf);

  RSByteOffsets *offsets = NewByteOffsets();
  uint8_t numFields = load_u8(&r);
  RSByteOffsets_ReserveFields(offsets, numFields);

  for (size_t ii = 0; ii < numFields; ++ii) {
    uint8_t fieldId = load_u8(&r);
    uint32_t firstTok = load_u32(&r);
    uint32_t lastTok = load_u32(&r);
    RSByteOffsetField *fieldInfo = RSByteOffsets_AddField(offsets, fieldId, firstTok);
    fieldInfo->lastTokPos = lastTok;
  }

  uint32_t offsetsLen = load_u32(&r);
  offsets->offsets.len = offsetsLen;
  if (offsetsLen) {
    offsets->offsets.data = rm_malloc(offsetsLen);
    Buffer_Read(&r, offsets->offsets.data, offsetsLen);
  } else {
    offsets->offsets.data = NULL;
  }

  return offsets;
}
