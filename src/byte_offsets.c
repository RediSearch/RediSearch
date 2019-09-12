#include "byte_offsets.h"
#include <arpa/inet.h>

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

void RSByteOffsets_Serialize(const RSByteOffsets *offsets, Buffer *b) {
  BufferWriter w = NewBufferWriter(b);

  Buffer_WriteU8(&w, offsets->numFields);

  for (size_t ii = 0; ii < offsets->numFields; ++ii) {
    Buffer_WriteU8(&w, offsets->fields[ii].fieldId);
    Buffer_WriteU32(&w, offsets->fields[ii].firstTokPos);
    Buffer_WriteU32(&w, offsets->fields[ii].lastTokPos);
  }

  Buffer_WriteU32(&w, offsets->offsets.len);
  Buffer_Write(&w, offsets->offsets.data, offsets->offsets.len);
}

RSByteOffsets *LoadByteOffsets(Buffer *buf) {
  BufferReader r = NewBufferReader(buf);

  RSByteOffsets *offsets = NewByteOffsets();
  uint8_t numFields = Buffer_ReadU8(&r);
  RSByteOffsets_ReserveFields(offsets, numFields);

  for (size_t ii = 0; ii < numFields; ++ii) {
    uint8_t fieldId = Buffer_ReadU8(&r);
    uint32_t firstTok = Buffer_ReadU32(&r);
    uint32_t lastTok = Buffer_ReadU32(&r);
    RSByteOffsetField *fieldInfo = RSByteOffsets_AddField(offsets, fieldId, firstTok);
    fieldInfo->lastTokPos = lastTok;
  }

  uint32_t offsetsLen = Buffer_ReadU32(&r);
  offsets->offsets.len = offsetsLen;
  if (offsetsLen) {
    offsets->offsets.data = rm_malloc(offsetsLen);
    Buffer_Read(&r, offsets->offsets.data, offsetsLen);
  } else {
    offsets->offsets.data = NULL;
  }

  return offsets;
}

int RSByteOffset_Iterate(const RSByteOffsets *offsets, uint32_t fieldId,
                         RSByteOffsetIterator *iter) {
  const RSByteOffsetField *offField = NULL;
  for (size_t ii = 0; ii < offsets->numFields; ++ii) {
    if (offsets->fields[ii].fieldId == fieldId) {
      offField = offsets->fields + ii;
      break;
    }
  }
  if (!offField) {
    return REDISMODULE_ERR;
  }

  // printf("Generating iterator for fieldId=%lu. BeginPos=%lu. EndPos=%lu\n", fieldId,
  //        offField->firstTokPos, offField->lastTokPos);

  iter->buf.cap = 0;
  iter->buf.data = offsets->offsets.data;
  iter->buf.offset = offsets->offsets.len;
  iter->rdr = NewBufferReader(&iter->buf);
  iter->curPos = 1;
  iter->endPos = offField->lastTokPos;

  iter->lastValue = 0;

  while (iter->curPos < offField->firstTokPos && !BufferReader_AtEnd(&iter->rdr)) {
    // printf("Seeking & incrementing\n");
    iter->lastValue = ReadVarint(&iter->rdr) + iter->lastValue;
    iter->curPos++;
  }

  // printf("Iterator is now at %lu\n", iter->curPos);
  iter->curPos--;
  return REDISMODULE_OK;
}

uint32_t RSByteOffsetIterator_Next(RSByteOffsetIterator *iter) {
  if (BufferReader_AtEnd(&iter->rdr) || ++iter->curPos > iter->endPos) {
    return RSBYTEOFFSET_EOF;
  }

  iter->lastValue = ReadVarint(&iter->rdr) + iter->lastValue;
  return iter->lastValue;
}